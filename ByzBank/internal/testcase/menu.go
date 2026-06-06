package testcase

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

// RunInteractive executes sets one at a time, pausing for menu commands between sets.
func RunInteractive(ctx context.Context, r *Runner, file *File, in io.Reader) error {
	reader := bufio.NewReader(in)
	for i, set := range file.Sets {
		fmt.Printf("\n=== Running Set %d (%d transactions) ===\n", set.Number, len(set.Txns))
		if _, err := r.RunSet(ctx, set); err != nil {
			return fmt.Errorf("set %d: %w", set.Number, err)
		}
		fmt.Printf("Set %d complete. Commands: PrintBalance <item> | PrintDatastore <server> | Performance | next | quit\n", set.Number)
		for {
			fmt.Print("> ")
			line, err := reader.ReadString('\n')
			if err != nil {
				return err
			}
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			parts := strings.Fields(line)
			switch parts[0] {
			case "next":
				if i == len(file.Sets)-1 {
					fmt.Println("No more sets.")
					return nil
				}
				goto nextSet
			case "quit", "exit":
				return nil
			case "Performance":
				fmt.Println(r.Metrics.Performance())
			case "PrintBalance":
				if len(parts) < 2 {
					fmt.Println("usage: PrintBalance <item>")
					continue
				}
				var item int
				if _, err := fmt.Sscanf(parts[1], "%d", &item); err != nil {
					fmt.Println("invalid item")
					continue
				}
				_ = r.PrintBalance(ctx, item)
			case "PrintDatastore":
				if len(parts) < 2 {
					fmt.Println("usage: PrintDatastore <server>")
					continue
				}
				id, err := config.ParseServerID(parts[1])
				if err != nil {
					fmt.Println(err)
					continue
				}
				_ = r.PrintDatastore(ctx, id)
			default:
				fmt.Println("unknown command")
			}
		}
	nextSet:
	}
	fmt.Println("All sets complete.")
	return nil
}
