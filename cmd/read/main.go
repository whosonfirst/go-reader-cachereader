package main

import (
	"context"
	"log"

	_ "github.com/whosonfirst/go-reader-cachereader/v2"
	
	"github.com/whosonfirst/go-reader/v2/app/read"
)

func main() {

	ctx := context.Background()
	err := read.Run(ctx)

	if err != nil {
		log.Fatalf("Failed to read, %v", err)
	}
}
