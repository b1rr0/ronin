package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/ronin/services/coordinator/internal/brokers"
	"github.com/ronin/services/coordinator/internal/console"
	"github.com/ronin/services/coordinator/internal/metadata"
)

func main() {
	fmt.Println("Starting Kafka Analog Coordinator...")

	metadataStore := metadata.NewStore()
	brokerManager := brokers.NewManager(metadataStore)
	consoleManager := console.NewManager(brokerManager, metadataStore)

	if err := brokerManager.StartAll(); err != nil {
		log.Fatalf("Failed to start services: %v", err)
	}

	go consoleManager.Start()

	fmt.Println("Coordinator started successfully!")
	fmt.Println("All services are running:")
	fmt.Println("- 5 Brokers (ports 9092-9096)")
	fmt.Println("- Producer Service (port 9093)")
	fmt.Println("- Consumer Service (port 9094)")
	fmt.Println("- Console available on port 8081")

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	<-c

	fmt.Println("\nShutting down...")
	brokerManager.StopAll()
	consoleManager.Stop()
}