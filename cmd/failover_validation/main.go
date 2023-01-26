package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	visibleNodes := flag.Int("visible-nodes", 0, "Total visible nodes from the perspective of the proposed leader")
	totalNodes := flag.Int("total-nodes", 0, "The total number of nodes registered")
	flag.Parse()

	if *visibleNodes == 0 || *visibleNodes < (*totalNodes/2+1) {
		fmt.Printf("Unable to perform failover as quorum can not be met. Total nodes: %d, Visible nodes: %d\n", *totalNodes, *visibleNodes)
		os.Exit(1)
	}

	os.Exit(0)
}
