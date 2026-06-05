// Package twopc implements the cross-shard two-phase-commit coordinator and
// participant state machines, layered on top of per-cluster PBFT. Each 2PC
// message crossing cluster boundaries carries a certificate of 2f+1 matching
// commit messages. Implemented in Phases 5-6.
package twopc
