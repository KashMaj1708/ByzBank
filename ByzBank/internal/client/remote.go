package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Remote talks to running server processes over gRPC and HTTP.
type Remote struct {
	topo   *config.Topology
	http   *http.Client
	grpcMu sync.Mutex
	grpc   map[config.ServerID]pb.ReplicaTransportClient
	conns  map[config.ServerID]*grpc.ClientConn
}

// NewRemote constructs a client for the topology's server addresses.
func NewRemote(topo *config.Topology) *Remote {
	return &Remote{
		topo:  topo,
		http:  &http.Client{Timeout: 30 * time.Second},
		grpc:  make(map[config.ServerID]pb.ReplicaTransportClient),
		conns: make(map[config.ServerID]*grpc.ClientConn),
	}
}

// Close tears down gRPC connections.
func (r *Remote) Close() {
	r.grpcMu.Lock()
	defer r.grpcMu.Unlock()
	for id, conn := range r.conns {
		_ = conn.Close()
		delete(r.conns, id)
		delete(r.grpc, id)
	}
}

func (r *Remote) healthURL(id config.ServerID, path string) (string, error) {
	srv, ok := r.topo.ServerByID(id)
	if !ok {
		return "", fmt.Errorf("unknown server %s", id)
	}
	return fmt.Sprintf("http://%s:%d%s", srv.Host, srv.Port+10000, path), nil
}

func (r *Remote) grpcClient(ctx context.Context, id config.ServerID) (pb.ReplicaTransportClient, error) {
	r.grpcMu.Lock()
	defer r.grpcMu.Unlock()
	if c, ok := r.grpc[id]; ok {
		return c, nil
	}
	srv, ok := r.topo.ServerByID(id)
	if !ok {
		return nil, fmt.Errorf("unknown server %s", id)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", id, err)
	}
	client := pb.NewReplicaTransportClient(conn)
	r.conns[id] = conn
	r.grpc[id] = client
	return client, nil
}

// SendRequest delivers a client request to one contact server.
func (r *Remote) SendRequest(ctx context.Context, to config.ServerID, req pbft.Request) error {
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}
	env := &pb.Envelope{Type: transport.TypeClientRequest, Payload: payload}
	client, err := r.grpcClient(ctx, to)
	if err != nil {
		return err
	}
	ack, err := client.Send(ctx, env)
	if err != nil {
		return err
	}
	if ack == nil || !ack.Ok {
		msg := ""
		if ack != nil {
			msg = ack.Error
		}
		return fmt.Errorf("send to %s rejected: %s", to, msg)
	}
	return nil
}

// ResetConsensus clears volatile PBFT and 2PC state on one live replica.
func (r *Remote) ResetConsensus(ctx context.Context, id config.ServerID) error {
	url, err := r.healthURL(id, "/reset_consensus")
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("reset_consensus %s: %s", id, string(b))
	}
	return nil
}

// DrainReplica waits for pending seq reclaims and execution drain on one replica.
func (r *Remote) DrainReplica(ctx context.Context, id config.ServerID, timeout time.Duration, executeOnly bool) error {
	q := "timeout_ms=" + strconv.FormatInt(timeout.Milliseconds(), 10)
	if executeOnly {
		q += "&execute_only=1"
	}
	url, err := r.healthURL(id, "/drain?"+q)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("drain %s: %s", id, string(b))
	}
	return nil
}

// SetFault posts fault configuration to one server.
func (r *Remote) SetFault(ctx context.Context, id config.ServerID, fc pbft.FaultConfig) error {
	url, err := r.healthURL(id, "/fault")
	if err != nil {
		return err
	}
	body, err := json.Marshal(fc)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := r.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("set fault %s: %s", id, string(b))
	}
	return nil
}

// PrintBalance queries one server's balance for an item.
func (r *Remote) PrintBalance(ctx context.Context, id config.ServerID, item int) (string, error) {
	url, err := r.healthURL(id, "/balance?item="+strconv.Itoa(item))
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s balance query: %s", id, string(b))
	}
	return string(bytes.TrimSpace(b)), nil
}

// FetchDatastoreOracle returns committed entries in oracle string format.
func (r *Remote) FetchDatastoreOracle(ctx context.Context, id config.ServerID) ([]string, error) {
	url, err := r.healthURL(id, "/datastore/oracle")
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("%s datastore oracle: %s", id, string(b))
	}
	var out []string
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

// PrintDatastore queries one server's committed datastore log.
func (r *Remote) PrintDatastore(ctx context.Context, id config.ServerID) (string, error) {
	url, err := r.healthURL(id, "/datastore")
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}
	resp, err := r.http.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("%s datastore query: %s", id, string(b))
	}
	return string(b), nil
}

// LookupReply fetches a recorded client reply from one server, if present.
func (r *Remote) LookupReply(ctx context.Context, id config.ServerID, req pbft.Request) (pbft.Reply, bool, error) {
	url, err := r.healthURL(id, fmt.Sprintf("/reply?client_id=%s&ts=%d&x=%d&y=%d&amt=%d",
		req.ClientID, req.TS, req.X, req.Y, req.Amt))
	if err != nil {
		return pbft.Reply{}, false, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return pbft.Reply{}, false, err
	}
	resp, err := r.http.Do(httpReq)
	if err != nil {
		return pbft.Reply{}, false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return pbft.Reply{}, false, nil
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return pbft.Reply{}, false, fmt.Errorf("%s reply query: %s", id, string(b))
	}
	var reply pbft.Reply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return pbft.Reply{}, false, err
	}
	return reply, true, nil
}
