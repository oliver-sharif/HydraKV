package main

import (
	"context"
	"hydrakv/envhandler"
	server2 "hydrakv/server"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {

	// Create stop channel
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	// Create ENV Handler
	envhandler.ENV.LoadENVs()

	// Create Server
	server := server2.NewServer(*envhandler.ENV.PORT, *envhandler.ENV.BIND_ADDRESS)

	// if *envhandler.ENV.GRPC_ENABLED - we will start a GRPC Server as well
	grpcServer := server2.NewGRPCServer(server)

	// Only start GRPC Server if *envhandler.ENV.GRPC_ENABLED
	if *envhandler.ENV.GRPC_ENABLED {
		go grpcServer.Start(*envhandler.ENV.GRPC_BIND_ADDRESS, *envhandler.ENV.GRPC_PORT)
	}

	// Start the Server in its own goroutine
	go server.Start()

	// Wait for Signal to terminate
	<-stop
	log.Println("Received Signal - shutting down...")

	// Stop grpc if *envhandler.ENV.GRPC_ENABLED
	if *envhandler.ENV.GRPC_ENABLED {
		grpcServer.Stop()
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*envhandler.ENV.WRITE_TIMEOUT+5)*time.Second)
	defer cancel()

	// End the Serving
	if err := server.Server.Shutdown(ctx); err != nil {
		log.Println("Server Shutdown:", err)
	}

	// Close all DBs gracefully
	server.CloseDbs()

	log.Println(
		"Server stopped",
	)
}
