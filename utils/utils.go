package utils

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"hydrakv/envhandler"
	"os"
	"regexp"
	"strings"
	"sync"
)

type Utils struct {
	DbNameRegex *regexp.Regexp
	apiKeys     map[string][32]byte
	mu          sync.RWMutex
}

var U = &Utils{}

// init will init the Utils struct
func init() {
	// Same rule as: validate:"alphanum,min=1,max=100"
	U.DbNameRegex = regexp.MustCompile("^[a-zA-Z0-9]{1,100}$")
	U.apiKeys = map[string][32]byte{}
}

// CheckDbName checks if the given db name is valid
func (u *Utils) CheckDbName(name string) bool {
	return u.DbNameRegex.MatchString(name)
}

// IsPublicPath checks if the given path is public
func (u *Utils) IsPublicPath(path string) bool {
	return path == "/health" || path == "/metrics" || path == "/create" || path == "/"
}

// IsApiKeyValid checks if the given api key is valid
func (u *Utils) IsApiKeyValid(db, apiKey string) bool {
	db = strings.ToUpper(db)

	// apiKey arrives as a string (header/proto), so hash the string form.
	hash := sha256.Sum256([]byte(apiKey))

	u.mu.RLock()
	val, ok := u.apiKeys[db]
	u.mu.RUnlock()

	if ok {
		return subtle.ConstantTimeCompare(val[:], hash[:]) == 1
	}
	return false
}

// CreateRandomApiKey creates a random api key
func (u *Utils) CreateRandomApiKey() (string, [32]byte, error) {
	apiKey := make([]byte, 16)
	if _, err := rand.Read(apiKey); err != nil {
		return "", [32]byte{}, fmt.Errorf("rand.Read: %w", err)
	}

	apiKeyStr := hex.EncodeToString(apiKey)

	// store hash of the string form, because that's what clients will send back.
	hash := sha256.Sum256([]byte(apiKeyStr))

	return apiKeyStr, hash, nil
}

// SaveApiKey saves the given api key
func (u *Utils) SaveApiKey(db string, apiKey [32]byte) error {
	db = strings.ToUpper(db)

	u.mu.Lock()
	u.apiKeys[db] = apiKey
	u.mu.Unlock()

	// create or open the file in *envhandler
	file, err := os.OpenFile(*envhandler.ENV.DB_FOLDER+"/."+db+".apikey", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.Write(apiKey[:])
	if err != nil {
		return err
	}
	return nil
}

// RestoreApiKeys restores the api keys from the .apikey files
func (u *Utils) RestoreApiKeys() error {
	files, err := os.ReadDir(*envhandler.ENV.DB_FOLDER)
	if err != nil {
		return err
	}
	for _, file := range files {
		if !strings.HasSuffix(file.Name(), ".apikey") {
			continue
		}
		apiKey, err := u.ReadApiKey(strings.TrimSuffix(file.Name(), ".apikey"))
		if err != nil {
			return err
		}
		err = u.SaveApiKey(strings.TrimSuffix(file.Name(), ".apikey"), [32]byte(apiKey))
		if err != nil {
			return err
		}
	}
	return nil
}

// ReadApiKey reads the api key from the file
func (u *Utils) ReadApiKey(db string) ([]byte, error) {
	db = strings.ToUpper(db)

	// read the file
	apiKey, err := os.ReadFile(*envhandler.ENV.DB_FOLDER + "/." + db + ".apikey")
	if err != nil {
		return nil, err
	}
	return apiKey, nil
}
