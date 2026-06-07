package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// RegisterHTTP mounts grading query and control endpoints on mux.
func (r *Replica) RegisterHTTP(mux *http.ServeMux) {
	mux.HandleFunc("/balance", r.handleHTTPBalance)
	mux.HandleFunc("/datastore", r.handleHTTPDatastore)
	mux.HandleFunc("/datastore/oracle", r.handleHTTPDatastoreOracle)
	mux.HandleFunc("/fault", r.handleHTTPFault)
	mux.HandleFunc("/reply", r.handleHTTPReply)
	mux.HandleFunc("/drain", r.handleHTTPDrain)
	mux.HandleFunc("/reset_consensus", r.handleHTTPResetConsensus)
}

func (r *Replica) handleHTTPResetConsensus(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ResetConsensus(); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (r *Replica) handleHTTPBalance(w http.ResponseWriter, req *http.Request) {
	item, err := strconv.Atoi(req.URL.Query().Get("item"))
	if err != nil {
		http.Error(w, "item query param required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(r.Store.PrintBalance(item) + "\n"))
}

func (r *Replica) handleHTTPDatastore(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(r.Store.PrintDatastore()))
}

func (r *Replica) handleHTTPDatastoreOracle(w http.ResponseWriter, _ *http.Request) {
	entries, err := r.Store.Datastore()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.OracleString())
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (r *Replica) handleHTTPFault(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	var fc pbft.FaultConfig
	if err := json.NewDecoder(req.Body).Decode(&fc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	r.SetFault(fc)
	w.WriteHeader(http.StatusOK)
}

func (r *Replica) handleHTTPDrain(w http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}
	if r.PBFT == nil {
		http.Error(w, "no engine", http.StatusServiceUnavailable)
		return
	}
	timeout := pbft.DefaultTunables(*r.Topo).ReclaimDrainWait
	if ms := req.URL.Query().Get("timeout_ms"); ms != "" {
		if v, err := strconv.Atoi(ms); err == nil && v > 0 {
			timeout = time.Duration(v) * time.Millisecond
		}
	}
	executeOnly := req.URL.Query().Get("execute_only") == "1"
	if !executeOnly {
		r.PBFT.WaitReclaims(req.Context(), timeout)
	}
	ctx, cancel := context.WithTimeout(req.Context(), timeout)
	defer cancel()
	r.PBFT.DrainExecute(ctx)
	w.WriteHeader(http.StatusOK)
}

func (r *Replica) handleHTTPReply(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	ts, err := strconv.ParseInt(q.Get("ts"), 10, 64)
	if err != nil {
		http.Error(w, "ts required", http.StatusBadRequest)
		return
	}
	amt, err := strconv.ParseInt(q.Get("amt"), 10, 64)
	if err != nil {
		http.Error(w, "amt required", http.StatusBadRequest)
		return
	}
	x, err := strconv.Atoi(q.Get("x"))
	if err != nil {
		http.Error(w, "x required", http.StatusBadRequest)
		return
	}
	y, err := strconv.Atoi(q.Get("y"))
	if err != nil {
		http.Error(w, "y required", http.StatusBadRequest)
		return
	}
	pbftReq := pbft.Request{
		ClientID: q.Get("client_id"),
		TS:       ts,
		X:        x,
		Y:        y,
		Amt:      amt,
	}
	if r.PBFT == nil {
		http.Error(w, "no engine", http.StatusServiceUnavailable)
		return
	}
	reply, ok := r.PBFT.LookupReply(pbftReq)
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(reply)
}
