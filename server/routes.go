package server

import (
	"encoding/json"
	"fmt"
	"hydrakv/envhandler"
	"hydrakv/utils"
	"log"
	"net/http"
	"strings"
)

// Index shows up a welcome page, listing all DBs created
func (s *Server) Index(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.URL.Path == "/" {
		data := struct {
			DBs           []*DBObject
			ApiKeyEnabled bool
		}{
			DBs:           s.ListDBs(),
			ApiKeyEnabled: *envhandler.ENV.APIKEY_ENABLED,
		}
		err := s.templates.ExecuteTemplate(w, "dbobjects", data)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// CreateDB creates a new DB
func (s *Server) CreateDB(w http.ResponseWriter, r *http.Request) {
	// secure request
	r.Body = http.MaxBytesReader(w, r.Body, int64(*envhandler.ENV.ENTRY_SIZE))
	// Close the Body on return
	defer r.Body.Close()

	// get the payload
	err, payload := readPayloadAndValidate[NewDB](r.Body, s)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// JSON Header
	w.Header().Set("Content-Type", "application/json")

	err, exists, created, apikey := s.NewDB(payload.Name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// return the response
	if exists {
		w.WriteHeader(http.StatusConflict)
	} else {
		w.WriteHeader(http.StatusCreated)
	}
	_ = json.NewEncoder(w).Encode(NewDBCreated{Name: strings.ToUpper(payload.Name), Created: created,
		Exists: exists, ApiKey: apikey})
}

// SetValue sets a value in a DB
func (s *Server) SetValue(w http.ResponseWriter, r *http.Request) {
	// Close the Body on return
	defer r.Body.Close()

	// bootstrap the request
	dbname, err := s.bootstrap(r, w)
	if err != nil {
		log.Println(err)
		return
	}

	err, payload := readPayloadAndValidate[Set](r.Body, s)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// set the value and return
	w.Header().Set("Content-Type", "application/json")

	var ok bool

	switch r.Method {
	case http.MethodPut:
		ok = s.Set(dbname, payload.Key, payload.Value, int64(payload.Ttl))
	case http.MethodPost:
		ok = s.SetNX(dbname, payload.Key, payload.Value, int64(payload.Ttl))
	case http.MethodPatch:
		ok = s.Incr(dbname, payload.Key, payload.Value)
	default:
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if !ok {
		w.WriteHeader(http.StatusConflict)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(OK{OK: ok})
}

// DeleteValue deletes a value from a DB
func (s *Server) DeleteValue(w http.ResponseWriter, r *http.Request) {
	// Close the Body on return
	defer r.Body.Close()

	// bootstrap the request
	dbname, err := s.bootstrap(r, w)
	if err != nil {
		log.Println(err)
		return
	}

	// Read the Payload
	err, payload := readPayloadAndValidate[Key](r.Body, s)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// del the value and return
	w.Header().Set("Content-Type", "application/json")
	ok := s.Del(dbname, payload.Key)

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(OK{OK: ok})
}

// GetValue gets a value from a DB
func (s *Server) GetValue(w http.ResponseWriter, r *http.Request) {
	// Close the Body on return
	defer r.Body.Close()

	// bootstrap the request
	dbname, err := s.bootstrap(r, w)
	if err != nil {
		log.Println(err)
		return
	}

	err, payload := readPayloadAndValidate[Key](r.Body, s)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// JSON Header
	w.Header().Set("Content-Type", "application/json")

	// Get the value and return
	ok, val := s.Get(dbname, payload.Key)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(Value{Found: ok, Value: val})
}

// DB checks if the DB exists
func (s *Server) DB(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	dbname := r.PathValue("dbname")

	if !utils.U.CheckDbName(dbname) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")

	ok := s.DBExists(dbname)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
	} else {
		w.WriteHeader(http.StatusOK)
	}
	_ = json.NewEncoder(w).Encode(ExistsResponse{Exists: ok})
}

func (s *Server) DeleteDB(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// bootstrap the request
	dbname, err := s.bootstrap(r, w)
	if err != nil {
		log.Println(err)
		return
	}

	// Delet the DB and return
	s.DBDelete(dbname)
	w.WriteHeader(http.StatusOK)
}

// ChangeApiKey creates a new API key for a existing DB
func (s *Server) ChangeApiKey(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()

	// just check if the *envhandler.APIKEY_ENABLED is true, otherwise return service temporary unavailable
	if !*envhandler.ENV.APIKEY_ENABLED {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	dbname, err := s.bootstrap(r, w)
	if err != nil {
		log.Println(err)
		return
	}

	apikey, err := s.CreateApiKey(dbname)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(NewDBCreated{Name: dbname, Created: false, Exists: true, ApiKey: apikey})
}

// HealthHandler returns 200 OK
func (s *Server) HealthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// bootstrap checks if the DB exists, sets MaxHeaderBytes to the entry size and checks the dbname
func (s *Server) bootstrap(r *http.Request, w http.ResponseWriter) (string, error) {
	// secure request
	r.Body = http.MaxBytesReader(w, r.Body, int64(*envhandler.ENV.ENTRY_SIZE))

	// get the path
	dbname := r.PathValue("dbname")
	if dbname == "" {
		parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		if len(parts) >= 2 && parts[0] == "db" {
			dbname = parts[1]
		}
	}

	if !utils.U.CheckDbName(dbname) {
		w.WriteHeader(http.StatusBadRequest)
		return "", fmt.Errorf("invalid db name")
	}

	if s.DBExists(dbname) == false {
		w.WriteHeader(http.StatusNotFound)
		return "", fmt.Errorf("DB %s does not exist", dbname)
	}
	return dbname, nil
}
