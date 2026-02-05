package server

import (
	"encoding/json"
	"html/template"
	"hydrakv/envhandler"
	"hydrakv/hashMap"
	"hydrakv/restartcheck"
	"hydrakv/utils"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server represents a server instance with configuration, routing, validation, templates, and thread-safe operations.
type Server struct {
	port      int
	ip        string
	Server    *http.Server
	dbs       map[string]*hashMap.HashMap
	validate  *validator.Validate
	templates *template.Template
	mut       *sync.RWMutex
}

// DBObject represents a database object with its name, number of entries, and number of baskets.
type DBObject struct {
	Name    string
	Entries int64
	Baskets int
}

// kvLogic defines an interface for key-value storage logic with methods for managing databases and key-value pairs.
type kvLogic interface {
	NewDB(name string) (err error, exists bool, created bool, apikey string)
	Set(db string, key string, value string, ttl int64) bool
	SetNX(db string, key string, value string, ttl int64) bool
	Get(db, key string) (bool, string)
	Incr(db, key, amount string) bool
	Del(db, key string) bool
	DBExists(db string) bool
}

// NewServer initializes and returns a new Server instance configured with the provided port and IP address.
func NewServer(port int, ip string) *Server {

	// create the server
	server := &Server{port: port, ip: ip}

	// Load html templates
	templates := template.Must(template.ParseGlob("server/templates/*.html"))

	// Create the ServeMux and the RequestLimiter for HTTP
	publicMux := http.NewServeMux()
	privateMux := http.NewServeMux()

	limitWrapper := newRequestLimiter()

	rootHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Public routes
		if utils.U.IsPublicPath(r.URL.Path) {
			publicMux.ServeHTTP(w, r)
			return
		}

		// disabled APIKEY
		if !*envhandler.ENV.APIKEY_ENABLED {
			privateMux.ServeHTTP(w, r)
			return
		}

		// check API Key
		dbName := r.PathValue("dbname")
		if dbName == "" {
			parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
			if len(parts) >= 2 && parts[0] == "db" {
				dbName = parts[1]
			}
		}

		if utils.U.CheckDbName(dbName) == false {
			http.Error(w, "invalid db name", http.StatusBadRequest)
			return
		}

		key := r.Header.Get("X-API-Key")
		if key == "" || !utils.U.IsApiKeyValid(dbName, key) {
			http.Error(w, "invalid api key", http.StatusUnauthorized)
			return
		}
		privateMux.ServeHTTP(w, r)
	})

	server.dbs = make(map[string]*hashMap.HashMap)
	server.validate = validator.New()
	server.templates = templates
	server.mut = &sync.RWMutex{}
	server.Server = &http.Server{Addr: ip + ":" + strconv.Itoa(port),
		Handler:        limitWrapper.wrap(rootHandler),
		WriteTimeout:   time.Duration(*envhandler.ENV.WRITE_TIMEOUT) * time.Second,
		ReadTimeout:    time.Duration(*envhandler.ENV.READ_TIMEOUT) * time.Second,
		IdleTimeout:    time.Duration(*envhandler.ENV.IDLE_TIMEOUT) * time.Second,
		MaxHeaderBytes: *envhandler.ENV.MAX_HEADER_BATES,
	}

	// shows the startpage with some information
	publicMux.HandleFunc("GET /", server.Index)

	// Prometheus healthroute
	publicMux.HandleFunc("GET /health", server.HealthHandler)

	// Prometheus metrics route
	publicMux.Handle("GET /metrics", promhttp.Handler())

	// creates a new DB
	publicMux.HandleFunc("POST /create", server.CreateDB)

	// checks if a DB exists
	privateMux.HandleFunc("GET /db/{dbname}", server.DB)

	// Sets a value in a DB
	privateMux.HandleFunc("PUT /db/{dbname}", server.SetValue)

	// Sets a value in a DB if its key doesnt exists
	privateMux.HandleFunc("POST /db/{dbname}", server.SetValue)

	// Increments a value in a DB
	privateMux.HandleFunc("PATCH /db/{dbname}", server.SetValue)

	// Deletes a value from a DB
	privateMux.HandleFunc("DELETE /db/{dbname}/keys", server.DeleteValue)

	// Gets a value from a DB
	privateMux.HandleFunc("POST /db/{dbname}/keys", server.GetValue)

	// Changes a apikey for a existing DB
	privateMux.HandleFunc("UPDATE /db/{dbname}", server.ChangeApiKey)

	// DeleteDB route
	privateMux.HandleFunc("DELETE /db/{dbname}", server.DeleteDB)

	return server
}

// Handler returns the HTTP handler associated with the server.
func (s *Server) Handler() http.Handler {
	return s.Server.Handler
}

// DBExists checks if a database with the specified name exists, returning true if found, or false otherwise.
func (s *Server) DBExists(name string) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if _, ok := s.dbs[strings.ToUpper(name)]; ok {
		return true
	}
	return false
}

// NewDB initializes a new database with the given name if it does not already exist and may create a new API key.
func (s *Server) NewDB(name string) (error, bool, bool, string) {
	// if DB already exists...
	if s.DBExists(name) {
		return nil, true, false, ""
	}

	// Create new DB
	hm, err := hashMap.NewHashMap(name)
	if err != nil {
		return err, false, false, ""
	}
	s.mut.Lock()
	s.dbs[strings.ToUpper(name)] = hm
	s.mut.Unlock()

	// if there is an APIKEY enabled, create a new one
	var apikey string
	if *envhandler.ENV.APIKEY_ENABLED {
		if apikey, err = s.CreateApiKey(name); err != nil {
			return err, false, false, ""
		}
	}

	return nil, false, true, apikey
}

// CreateApiKey generates a new API key, stores its hash, and returns the API key. Returns an error if creation or storage fails.
func (s *Server) CreateApiKey(db string) (string, error) {
	s.mut.Lock()
	defer s.mut.Unlock()
	// Create a new APIKEY
	apikey, hash, err := utils.U.CreateRandomApiKey()
	if err != nil {
		return "", err
	}

	// Save the APIKEY
	err = utils.U.SaveApiKey(db, hash)
	if err != nil {
		return "", err
	}
	return apikey, nil
}

// Set stores a key-value pair with an optional TTL in the specified database, returning true on success or false on failure.
func (s *Server) Set(db, key, value string, ttl int64) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.CheckEntries(db) == false {
		return false
	}
	if hm, ok := s.dbs[strings.ToUpper(db)]; ok {
		return hm.Set(ttl, key, value)
	}
	return false
}

// Incr increments the value of a specified key in the given database by the specified amount. Returns true if successful.
func (s *Server) Incr(db, key, amount string) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()
	if hm, ok := s.dbs[strings.ToUpper(db)]; ok {
		return hm.Incr(0, key, amount)
	}
	return false
}

// Del removes the specified key from the given database and returns true if the operation is successful, otherwise false.
func (s *Server) Del(db, key string) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if hm, ok := s.dbs[strings.ToUpper(db)]; ok {
		return hm.Del(key)
	}
	return false
}

// Get retrieves the value associated with the given key from the specified database. Returns a boolean and the value.
func (s *Server) Get(db, key string) (bool, string) {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if hm, ok := s.dbs[strings.ToUpper(db)]; ok {
		return hm.Get(key)
	}
	return false, ""
}

// SetNX attempts to set a key with a value and TTL if the key does not already exist in the specified database.
func (s *Server) SetNX(db, key, value string, ttl int64) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if s.CheckEntries(db) == false {
		return false
	}
	if hm, ok := s.dbs[strings.ToUpper(db)]; ok {
		exists, _ := hm.Get(key)
		if exists {
			return false
		}
		return hm.Set(ttl, key, value)
	}
	return false
}

// readPayloadAndValidate reads JSON payload from the request body, validates it, and returns the error or the decoded payload.
func readPayloadAndValidate[T any](body io.ReadCloser, s *Server) (error, T) {
	var payload T

	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	err := decoder.Decode(&payload)

	// return error if this alread fails
	if err != nil {
		return err, payload
	}

	// validate the payload
	if err := s.validate.Struct(payload); err != nil {
		return err, payload
	}
	return nil, payload
}

// ReloadDb reloads the database connections and restores API keys if enabled.
func (s *Server) ReloadDb() error {
	dbs, err := restartcheck.RCheck.Check()
	if err != nil {
		return err
	}

	// if we are using APIKEYS - restore them
	if *envhandler.ENV.APIKEY_ENABLED {
		err := utils.U.RestoreApiKeys()
		if err != nil {
			return err
		}
	}

	for _, db := range dbs {
		err, _, _, _ := s.NewDB(db)
		if err != nil {
			log.Printf("Error recreating DB %s: %v", db, err)
		}
	}
	return nil
}

// Start initializes the server, attempts to reload the database, and begins listening for incoming HTTP connections.
func (s *Server) Start() {
	// lets check for existing bin files in the aof dir
	err := s.ReloadDb()
	if err != nil {
		log.Println(err)
	}

	log.Printf("Starting HTTPServer on %s:%d\n", s.ip, s.port)
	err = s.Server.ListenAndServe()
	if err != nil {
		log.Printf("ListenAndServe: %v", err)
	}
}

// CloseDbs releases all database resources managed by the server and logs any errors encountered during the process.
func (s *Server) CloseDbs() {
	s.mut.Lock()
	defer s.mut.Unlock()
	errors := make([]error, 0)
	for _, db := range s.dbs {
		errors = append(errors, db.Close())
	}

	// print possible errors
	for _, err := range errors {
		if err != nil {
			log.Println(err)
		}
	}
}

// CheckEntries checks if the number of entries in the database identified by name is below the maximum allowed limit.
func (s *Server) CheckEntries(name string) bool {
	s.mut.RLock()
	defer s.mut.RUnlock()

	if _, ok := s.dbs[strings.ToUpper(name)]; ok {
		return s.dbs[strings.ToUpper(name)].GetEntries() < int64(*envhandler.ENV.MAX_ENTRIES)
	}
	return false
}

// ListDBs returns a slice of pointers to DBObject, representing a detailed list of databases managed by the server.
func (s *Server) ListDBs() []*DBObject {
	s.mut.RLock()
	defer s.mut.RUnlock()

	dbs := make([]*DBObject, 0)

	for _, db := range s.dbs {
		entries := db.GetEntries()
		name := db.Name
		baskets := db.GetBasketNum()
		dbs = append(dbs, &DBObject{Name: name, Entries: entries, Baskets: baskets})
	}
	return dbs
}

// DBDelete deletes a database by name, closes its instance, removes its AOF file, and updates the server's database map.
func (s *Server) DBDelete(name string) {
	s.mut.Lock()
	defer s.mut.Unlock()

	// Close the DB
	err := s.dbs[strings.ToUpper(name)].Close()
	if err != nil {
		log.Println(err)
	}

	// Delete the AOF file
	err = os.Remove(s.dbs[strings.ToUpper(name)].Aof.FileName)
	if err != nil {
		log.Println(err)
	}

	// Delete the DB from the map
	delete(s.dbs, strings.ToUpper(name))
}
