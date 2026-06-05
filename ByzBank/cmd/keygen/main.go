// Command keygen generates ed25519 keypairs for every server in the topology.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
)

func main() {
	out := flag.String("out", "config/keys", "output directory for key JSON files")
	flag.Parse()

	topo := config.Load()
	dir, err := filepath.Abs(*out)
	if err != nil {
		log.Fatalf("resolve output dir: %v", err)
	}
	if err := crypto.GenerateAllKeys(topo, dir); err != nil {
		log.Fatalf("generate keys: %v", err)
	}
	fmt.Printf("generated %d keypairs in %s\n", topo.TotalServers(), dir)
	os.Exit(0)
}
