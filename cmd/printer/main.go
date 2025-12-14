package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kmsec-uk/npm-replicate-follower/follower"
)

func main() {
	ctx := context.Background()
	fmt.Println("helllo from the printer")
	scraper := follower.NewFollower()
	for res := range scraper.Connect(ctx) {
		if err := res.Error; err != nil {
			log.Printf("error polling: %v", err)
		} else {
			log.Println(res.Change.ID)
		}
	}
}
