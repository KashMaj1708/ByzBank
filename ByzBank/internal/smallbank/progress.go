package smallbank

import (
	"fmt"
	"io"
	"os"
	"strings"
)

const progressBarWidth = 40

// progressBar renders an in-place terminal progress indicator on stderr.
type progressBar struct {
	label  string
	total  int
	width  int
	out    io.Writer
	closed bool
}

func newProgressBar(label string, total int) *progressBar {
	if total < 1 {
		total = 1
	}
	return &progressBar{
		label: label,
		total: total,
		width: progressBarWidth,
		out:   os.Stderr,
	}
}

func (p *progressBar) update(done int, note string) {
	if p == nil || p.closed {
		return
	}
	if done > p.total {
		done = p.total
	}
	if done < 0 {
		done = 0
	}
	pct := float64(done) * 100 / float64(p.total)
	filled := int(float64(done) * float64(p.width) / float64(p.total))
	if filled > p.width {
		filled = p.width
	}
	bar := strings.Repeat("=", filled) + strings.Repeat("-", p.width-filled)
	line := fmt.Sprintf("SmallBank %-8s [%s] %d/%d (%3.0f%%)", p.label, bar, done, p.total, pct)
	if note != "" {
		line += " " + note
	}
	// Pad to overwrite prior longer lines (e.g. changing note text).
	if len(line) < 100 {
		line += strings.Repeat(" ", 100-len(line))
	}
	fmt.Fprintf(p.out, "\r%s", line)
}

func (p *progressBar) finish(note string) {
	if p == nil || p.closed {
		return
	}
	p.update(p.total, note)
	fmt.Fprintln(p.out)
	p.closed = true
}

func (p *progressBar) message(text string) {
	if p == nil {
		return
	}
	fmt.Fprintf(p.out, "\r%s\n", text)
}
