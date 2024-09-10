package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"time"

	ride "github.com/KathirvelChandrasekaran/grpc_poc/ride_data"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RideData defines the structure for the REST API data
type RideData struct {
	Key              string `json:"key"`
	FareAmount       string `json:"fare_amount"`
	PickupDatetime   string `json:"pickup_datetime"`
	PickupLongitude  string `json:"pickup_longitude"`
	PickupLatitude   string `json:"pickup_latitude"`
	DropoffLongitude string `json:"dropoff_longitude"`
	DropoffLatitude  string `json:"dropoff_latitude"`
	PassengerCount   string `json:"passenger_count"`
}

// Function to fetch data from REST API
func fetchFromRESTAPI() ([]RideData, error) {
	// Send an HTTP GET request to the REST API
	resp, err := http.Get("http://localhost:8080/rides")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, err
	}

	// Decode the JSON response
	var rides []RideData
	err = json.NewDecoder(resp.Body).Decode(&rides)
	if err != nil {
		return nil, err
	}

	return rides, nil
}

func main() {
	// Set up a connection to the gRPC server
	conn, err := grpc.Dial("localhost:8080", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(
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

	// Fetch data from the REST API
	log.Println("Fetching data from REST API...")
	restStartTime := time.Now()
	rides, err := fetchFromRESTAPI()
	if err != nil {
		log.Fatalf("Failed to fetch data from REST API: %v", err)
	}
	log.Printf("Client: Time taken to receive REST data: %v", time.Since(restStartTime))
	log.Printf("Received %d rides from REST API", len(rides))

	// Call the gRPC method after fetching data from REST API
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	grpcStartTime := time.Now()
	response, err := c.Create(ctx, &ride.CreateRideRequest{})
	if err != nil {
		log.Fatalf("error calling gRPC function Create: %v", err)
	}
	log.Printf("Client: Time taken to receive gRPC data: %v", time.Since(grpcStartTime))
	log.Printf("Received %d rides from gRPC", len(response.CreatedRides))
}
