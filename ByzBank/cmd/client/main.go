// Command client is the single open-loop client driver and interactive menu.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/client"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/smallbank"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/testcase"
)

func main() {
	health := flag.Bool("healthcheck", false, "probe every server's /health endpoint and report")
	testfile := flag.String("testfile", "", "path to Lab4 CSV test file")
	benchmark := flag.String("benchmark", "", "benchmark to run (smallbank)")
	auto := flag.Bool("auto", false, "run all sets without interactive menu (for scripting)")
	verify := flag.String("verify", "", "path to Lab4 *_expected.json oracle; compare after each set")
	txns := flag.Int("txns", 1000, "SmallBank: number of transactions")
	skew := flag.Float64("skew", 0.9, "SmallBank: hot-access fraction (0..1)")
	amt := flag.Int64("amt", 2, "SmallBank: transfer amount per write txn")
	flag.Parse()

	topo := config.Load()

	if *health {
		os.Exit(runHealthcheck(topo))
	}

	if *benchmark == "smallbank" {
		os.Exit(runSmallBank(topo, smallbank.Config{
			Txns:              *txns,
			Amt:               *amt,
			Skew:              *skew,
			HotAccessFraction: *skew,
			Seed:              42,
			SettleTimeout:     300 * time.Second,
			MultiStepTimeout:  10 * time.Second,
			Pace:              25 * time.Millisecond,
			ShowProgress:      true,
		}))
	}

	if *testfile != "" {
		os.Exit(runTestfile(topo, *testfile, *auto, *verify))
	}

	fmt.Printf("2pcbyz client\n")
	fmt.Printf("topology: %d clusters x %d nodes = %d servers, f=%d, quorum=%d\n",
		topo.NumClusters, topo.ClusterSize, topo.TotalServers(), topo.F(), topo.Quorum())
	fmt.Printf("usage:\n")
	fmt.Printf("  --healthcheck\n")
	fmt.Printf("  --testfile test/Lab4_Testset_1_36node.csv\n")
	fmt.Printf("  --benchmark smallbank --txns 1000 --skew 0.9\n")
}

func runSmallBank(topo config.Topology, cfg smallbank.Config) int {
	remote := client.NewRemote(&topo)
	defer remote.Close()
	driver := smallbank.NewDriver(&topo, remote)
	ctx := context.Background()
	if err := driver.Run(ctx, cfg); err != nil {
		fmt.Fprintf(os.Stderr, "smallbank: %v\n", err)
		return 1
	}
	return 0
}

func runHealthcheck(topo config.Topology) int {
	client := &http.Client{Timeout: 2 * time.Second}
	up, down := 0, 0
	for _, s := range topo.Servers {
		url := fmt.Sprintf("http://%s:%d/health", s.Host, s.Port+10000)
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("  %-4s %-20s DOWN (%v)\n", s.ID, s.Addr(), err)
			down++
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			fmt.Printf("  %-4s %-20s UP\n", s.ID, s.Addr())
			up++
		} else {
			fmt.Printf("  %-4s %-20s BAD (%d)\n", s.ID, s.Addr(), resp.StatusCode)
			down++
		}
	}
	fmt.Printf("\nhealthcheck: %d up, %d down (of %d)\n", up, down, topo.TotalServers())
	if down > 0 {
		return 1
	}
	return 0
}

func runTestfile(topo config.Topology, path string, auto bool, verifyPath string) int {
	file, err := testcase.ParseFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "parse %s: %v\n", path, err)
		return 1
	}
	remote := client.NewRemote(&topo)
	defer remote.Close()
	runner := testcase.NewRunner(&topo, remote)
	ctx := context.Background()
	allItems := file.AllItems()

	if auto {
		var dumps []testcase.OracleDump
		for _, set := range file.Sets {
			fmt.Printf("=== Set %d ===\n", set.Number)
			if _, err := runner.RunSet(ctx, set); err != nil {
				fmt.Fprintf(os.Stderr, "set %d: %v\n", set.Number, err)
				return 1
			}
			fmt.Println(runner.Metrics.Performance())
			if verifyPath != "" {
				dump, err := runner.CollectOracleDump(ctx, set, allItems)
				if err != nil {
					fmt.Fprintf(os.Stderr, "oracle dump set %d: %v\n", set.Number, err)
					return 1
				}
				dumps = append(dumps, dump)
			}
		}
		if verifyPath != "" {
			rc, err := testcase.VerifyDumps(verifyPath, dumps)
			if err != nil {
				fmt.Fprintf(os.Stderr, "verify: %v\n", err)
				return 1
			}
			return rc
		}
		return 0
	}

	if err := testcase.RunInteractive(ctx, runner, file, os.Stdin); err != nil {
		fmt.Fprintf(os.Stderr, "runner: %v\n", err)
		return 1
	}
	return 0
}
