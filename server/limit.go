package server

import (
	"encoding/json"
	"hydrakv/envhandler"
	"log"
	"net/http"
)

type requestLimiter struct {
	sem chan struct{}
}

// creates a new request limiter
func newRequestLimiter() *requestLimiter {
	return &requestLimiter{sem: make(chan struct{}, *envhandler.ENV.REQ_LIMIT)}
}

// wrap creates a new request limiter middleware
func (l *requestLimiter) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case l.sem <- struct{}{}:
			defer func() { <-l.sem }()
			next.ServeHTTP(w, r)
		default:
			log.Println("request limit reached - please check requestlimit!")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]interface{}{
				"error":       "rate_limit_exceeded",
				"message":     "Too many requests",
				"currentLoad": len(l.sem),
			})
		}
	})
}
