package main

import (
	"fmt"
	"os"

	"github.com/darianmavgo/banquet"
	"github.com/darianmavgo/banquet/sqlite"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Usage: bqsqlite <url>")
		os.Exit(1)
	}

	rawURL := os.Args[1]
	bq, err := banquet.ParseBanquet(rawURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error parsing URL: %v\n", err)
		os.Exit(1)
	}

	query := sqlite.Compose(bq)
	fmt.Println(query)
}
