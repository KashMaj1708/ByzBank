// Package deps pins the project's core third-party dependencies so their
// versions are recorded in go.mod from Phase 0 and don't drift mid-project.
// These libraries are wired up properly in later phases:
//   - google.golang.org/grpc, google.golang.org/protobuf : signed RPC transport
//   - go.etcd.io/bbolt                                    : embedded KV store
//
// The blank imports keep `go mod tidy` from dropping the requirements before
// the packages are actually referenced.
package deps

import (
	_ "go.etcd.io/bbolt"
	_ "google.golang.org/grpc"
	_ "google.golang.org/protobuf/proto"
)
