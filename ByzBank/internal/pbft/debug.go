package pbft

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// StatusCode returns the Lab-2 status letter for one sequence slot.
func (e *Engine) StatusCode(seq int64) string {
	e.mu.Lock()
	defer e.mu.Unlock()
	st, ok := e.log[seq]
	if !ok || !st.prePrepare {
		return "X"
	}
	if st.executed {
		return "E"
	}
	if st.commitCert != nil {
		return "C"
	}
	if st.prepareCert != nil || len(st.prepares) >= e.commitQuorum {
		return "P"
	}
	return "PP"
}

// PrintStatus formats status lines for seq (0 = all known sequences).
func (e *Engine) PrintStatus(seq int64) string {
	e.mu.Lock()
	seqs := e.statusSeqsLocked(seq)
	e.mu.Unlock()

	var b strings.Builder
	for _, s := range seqs {
		st, ok := e.log[s]
		req := Request{}
		if ok {
			req = st.req
		}
		fmt.Fprintf(&b, "v=%d s=%d %s %v\n", e.view, s, e.StatusCode(s), req)
	}
	return b.String()
}

func (e *Engine) statusSeqsLocked(seq int64) []int64 {
	if seq > 0 {
		return []int64{seq}
	}
	set := make(map[int64]struct{})
	for s := range e.log {
		set[s] = struct{}{}
	}
	out := make([]int64, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// PrintView formats all accepted NEW-VIEW messages.
func (e *Engine) PrintView() string {
	e.mu.Lock()
	views := append([]NewViewMsg(nil), e.newViewLog...)
	e.mu.Unlock()

	var b strings.Builder
	for _, nv := range views {
		fmt.Fprintf(&b, "NEW_VIEW v=%d pre_prepares=%d view_changes=%d\n", nv.NewView, len(nv.PrePrepares), len(nv.ViewChanges))
	}
	return b.String()
}

// PrintLog formats received VIEW-CHANGE messages (including Byzantine leader logging).
func (e *Engine) PrintLog() string {
	e.mu.Lock()
	vcs := append([]ViewChangeMsg(nil), e.viewChangeReceived...)
	e.mu.Unlock()

	var b strings.Builder
	for _, vc := range vcs {
		fmt.Fprintf(&b, "VIEW_CHANGE new_view=%d stable=%d prepared=%d pending=%d\n",
			vc.NewView, vc.LatestStable, len(vc.Prepared), len(vc.PendingReqs))
	}
	return b.String()
}

// View returns the current PBFT view number.
func (e *Engine) View() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.view
}

// SetViewChangeTimeout overrides the backup suspicion timer (tests).
func (e *Engine) SetViewChangeTimeout(d time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.viewChangeTimeout = d
	e.tunables.ViewChangeTimeout = d
}

// SetFault configures Byzantine/crash behaviour for this replica.
func (e *Engine) SetFault(fc FaultConfig) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !fc.Alive {
		fc.ByzantineLeader = false
		fc.ByzantineBackup = false
	}
	e.fault = fc
}
