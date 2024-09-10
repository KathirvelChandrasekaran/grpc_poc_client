package main

import (
	"context"
	ride "github.com/KathirvelChandrasekaran/grpc_poc/ride_data"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"time"
)

func main() {
	conn, err := grpc.NewClient("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(
		grpc.MaxCallRecvMsgSize(50*1024*1024), // Increase receive size to 50MB
		grpc.MaxCallSendMsgSize(50*1024*1024), // Increase send size to 50MB
	))
	if err != nil {
		log.Fatalf("failed to connect to gRPC server at localhost:8080: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			log.Fatalf("failed to close gRPC connection: %v", err)
		}
	}(conn)
	c := ride.NewRideClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	response, err := c.Create(ctx, &ride.CreateRideRequest{})
	startTime := time.Now()
	if err != nil {
		log.Fatalf("error calling function Create: %v", err)
	}
	log.Printf("Client: Time taken to receive data: %v", time.Since(startTime))
	log.Printf("Received %d rides", len(response.CreatedRides))
}
