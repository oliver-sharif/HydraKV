package restartcheck

import (
	"hydrakv/envhandler"
	"log"
	"os"
	"strings"
)

type RestartCheck struct {
}

var RCheck *RestartCheck

// init creates a new RestartCheck
func init() {
	RCheck = &RestartCheck{}
}

// Check Checks if in the AOF dir already *.bin data files exist
func (r *RestartCheck) Check() ([]string, error) {
	log.Println("Checking for existing bin files in aof dir...")
	d, err := os.ReadDir(*envhandler.ENV.DB_FOLDER)
	if err != nil {
		// Create the dir if it does not exist
		err := os.Mkdir(*envhandler.ENV.DB_FOLDER, 0755)
		log.Println("Creating aof dir... No DB directories and no DB files found.")
		return nil, err
	}
	var files []string
	for _, f := range d {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".bin") {
			continue
		}
		files = append(files, strings.Split(f.Name(), ".")[0])
	}
	log.Printf("Found %d bin files in aof dir", len(files))
	return files, nil
}
