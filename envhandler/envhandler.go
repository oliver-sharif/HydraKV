package envhandler

import (
	"flag"
	"log"
	"os"
	"reflect"
	"runtime"
	"strconv"
)

const (
	BIND_ADDRESS                = "HKV_BIND_ADDRESS"
	PORT                        = "HKV_PORT"
	DB_FOLDER                   = "HKV_DB_FOLDER"
	MAX_ENTRIES                 = "HKV_MAX_ENTRIES"
	WRITE_TIMEOUT               = "HKV_WRITE_TIMEOUT"
	APIKEY_ENABLED              = "HKV_APIKEY_ENABLED"
	READ_TIMEOUT                = "HKV_READ_TIMEOUT"
	IDLE_TIMEOUT                = "HKV_IDLE_TIMEOUT"
	METRICS                     = "HKV_METRICS_ENABLED"
	ENTRY_SIZE                  = "HKV_ENTRY_SIZE"
	MAX_HEADER_BYTES            = "HKV_MAX_HEADER_BYTES"
	XXHASH_SEED                 = "HKV_XXHASH_SEED"
	REQ_LIMIT                   = "HKV_REQUEST_LIMIT"
	GRPC_ENABLED                = "HKV_GRPC_ENABLED"
	GRPC_PORT                   = "HKV_GRPC_PORT"
	GRPC_BIND_ADDRESS           = "HKV_GRPC_BIND_ADDRESS"
	GRPC_REQ_LIMIT              = "HKV_GRPC_REQUEST_LIMIT"
	GRPC_MAX_DURATION           = "HKV_GRPC_MAX_DURATION"
	GRPC_MAX_CONCURRENT_STREAMS = "GRPC_MAX_CONCURRENT_STREAMS"
	CPU_MULTIPLIER              = "HKV_CPU_MULTIPLIER"
)

type EnvHandler struct {
	BIND_ADDRESS                *string `env:"BIND_ADDRESS"`
	PORT                        *int    `env:"PORT"`
	DB_FOLDER                   *string `env:"DB_FOLDER"`
	MAX_ENTRIES                 *int    `env:"MAX_ENTRIES"`
	WRITE_TIMEOUT               *int    `env:"WRITE_TIMEOUT"`
	APIKEY_ENABLED              *bool   `env:"APIKEY_ENABLED"`
	READ_TIMEOUT                *int    `env:"READ_TIMEOUT"`
	IDLE_TIMEOUT                *int    `env:"IDLE_TIMEOUT"`
	METRICS                     *bool   `env:"METRICS"`
	ENTRY_SIZE                  *int    `env:"ENTRY_SIZE"`
	MAX_HEADER_BATES            *int    `env:"MAX_HEADER_BYTES"`
	XXHASH_SEED                 *uint64 `env:"XXHASH_SEED"`
	REQ_LIMIT                   *int    `env:"REQUEST_LIMIT"`
	GRPC_ENABLED                *bool   `env:"GRPC_ENABLED"`
	GRPC_PORT                   *int    `env:"GRPC_PORT"`
	GRPC_BIND_ADDRESS           *string `env:"GRPC_BIND_ADDRESS"`
	GRPC_REQ_LIMIT              *int    `env:"GRPC_REQUEST_LIMIT"`
	GRPC_MAX_DURATION           *int    `env:"GRPC_MAX_DURATION"`
	GRPC_MAX_CONCURRENT_STREAMS *int    `env:"GRPC_MAX_CONCURRENT_STREAMS"`
	CPU_MULTIPLIER              *int    `env:"CPU_MULTIPLIER"`
}

// ENV is the global EnvHandler - its a singleton
var ENV *EnvHandler

// init creates a new EnvHandler
func init() {
	ENV = &EnvHandler{
		BIND_ADDRESS:                flag.String(BIND_ADDRESS, "0.0.0.0", "The address to bind to"),
		PORT:                        flag.Int(PORT, 9191, "The port to bind to"),
		DB_FOLDER:                   flag.String(DB_FOLDER, "./data", "The folder to store the DBs in"),
		MAX_ENTRIES:                 flag.Int(MAX_ENTRIES, 100000, "The maximum number of entries per DB"),
		WRITE_TIMEOUT:               flag.Int(WRITE_TIMEOUT, 20, "The maximum time in Seconds to wait for a write operation to complete"),
		APIKEY_ENABLED:              flag.Bool(APIKEY_ENABLED, false, "Enable API key authentication"),
		READ_TIMEOUT:                flag.Int(READ_TIMEOUT, 20, "The maximum time in Seconds to wait for a read operation to complete"),
		IDLE_TIMEOUT:                flag.Int(IDLE_TIMEOUT, 20, "The maximum time in Seconds to wait for a client to send a request"),
		METRICS:                     flag.Bool(METRICS, false, "Enable Prometheus metrics"),
		ENTRY_SIZE:                  flag.Int(ENTRY_SIZE, 2048, "The maximum size of a single entry in bytes"),
		MAX_HEADER_BATES:            flag.Int(MAX_HEADER_BYTES, 1024, "The maximum size of the header in bytes"),
		XXHASH_SEED:                 flag.Uint64(XXHASH_SEED, 0, "The seed for the xxhash algorithm"),
		REQ_LIMIT:                   flag.Int(REQ_LIMIT, 500, "The maximum number of requests per second"),
		GRPC_ENABLED:                flag.Bool(GRPC_ENABLED, true, "Enable gRPC server"),
		GRPC_PORT:                   flag.Int(GRPC_PORT, 9292, "The port to bind to for the gRPC server"),
		GRPC_BIND_ADDRESS:           flag.String(GRPC_BIND_ADDRESS, "0.0.0.0", "The address to bind to for the gRPC server"),
		GRPC_REQ_LIMIT:              flag.Int(GRPC_REQ_LIMIT, 1000, "The maximum number of requests per second for the gRPC server"),
		GRPC_MAX_DURATION:           flag.Int(GRPC_MAX_DURATION, 10, "The maximum duration in seconds for a gRPC call"),
		GRPC_MAX_CONCURRENT_STREAMS: flag.Int(GRPC_MAX_CONCURRENT_STREAMS, runtime.NumCPU()*4, "The maximum number of concurrent streams for a gRPC call"),
		CPU_MULTIPLIER:              flag.Int(CPU_MULTIPLIER, 16, "The multiplier to use for CPU usage"),
	}
}

// LoadENVs loads all ENV variables into the EnvHandler
func (e *EnvHandler) LoadENVs() {
	v := reflect.ValueOf(e).Elem()
	t := reflect.TypeOf(e).Elem()

	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldType := t.Field(i)

		// Get the environment variable key from the struct tag
		envKey := fieldType.Tag.Get("env")
		if envKey == "" {
			continue
		}

		// Map internal tag names to the actual HKV_* environment variable names
		actualEnvKey := ""
		switch envKey {
		case "BIND_ADDRESS":
			actualEnvKey = BIND_ADDRESS
		case "PORT":
			actualEnvKey = PORT
		case "DB_FOLDER":
			actualEnvKey = DB_FOLDER
		case "MAX_ENTRIES":
			actualEnvKey = MAX_ENTRIES
		case "WRITE_TIMEOUT":
			actualEnvKey = WRITE_TIMEOUT
		case "APIKEY_ENABLED":
			actualEnvKey = APIKEY_ENABLED
		case "READ_TIMEOUT":
			actualEnvKey = READ_TIMEOUT
		case "IDLE_TIMEOUT":
			actualEnvKey = IDLE_TIMEOUT
		case "METRICS":
			actualEnvKey = METRICS
		case "ENTRY_SIZE":
			actualEnvKey = ENTRY_SIZE
		case "MAX_HEADER_BYTES":
			actualEnvKey = MAX_HEADER_BYTES
		case "XXHASH_SEED":
			actualEnvKey = XXHASH_SEED
		case "REQUEST_LIMIT":
			actualEnvKey = REQ_LIMIT
		case "GRPC_ENABLED":
			actualEnvKey = GRPC_ENABLED
		case GRPC_PORT:
			actualEnvKey = GRPC_PORT
		case GRPC_BIND_ADDRESS:
			actualEnvKey = GRPC_BIND_ADDRESS
		case GRPC_REQ_LIMIT:
			actualEnvKey = GRPC_REQ_LIMIT
		case GRPC_MAX_DURATION:
			actualEnvKey = GRPC_MAX_DURATION
		case GRPC_MAX_CONCURRENT_STREAMS:
			actualEnvKey = GRPC_MAX_CONCURRENT_STREAMS
		case CPU_MULTIPLIER:
			actualEnvKey = CPU_MULTIPLIER
		default:
			continue
		}

		envVal, ok := os.LookupEnv(actualEnvKey)
		if !ok {
			continue
		}

		elem := field.Elem() // Field Value

		switch elem.Kind() { // switch on the kind of the field
		case reflect.String:
			elem.SetString(envVal)

		case reflect.Int:
			if i, err := strconv.Atoi(envVal); err == nil {
				elem.SetInt(int64(i))
			} else {
				log.Fatalf("Invalid int for %s", actualEnvKey)
			}

		case reflect.Bool:
			if b, err := strconv.ParseBool(envVal); err == nil {
				elem.SetBool(b)
			} else {
				log.Fatalf("Invalid bool for %s", actualEnvKey)
			}
		case reflect.Uint64:
			if i, err := strconv.ParseUint(envVal, 10, 64); err == nil {
				elem.SetUint(i)
			} else {
				log.Fatalf("Invalid uint for %s", actualEnvKey)
			}

		default:
			log.Fatalf("Unsupported type for %s", actualEnvKey)
		}
	}

	// warn the user when there is APIKey false
	if !*e.APIKEY_ENABLED {
		log.Println("WARNING: APIKEY_ENABLED is false, all requests will be accepted without authentication!")
	}
}
