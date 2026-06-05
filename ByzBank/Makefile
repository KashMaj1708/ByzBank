# Makefile for the 2pcbyz BFT sharded 2PC system (Unix/macOS).
# On Windows/PowerShell use scripts/run.ps1 with the same target names.

GO      ?= go
BIN     := bin
RUN     := run
PIDFILE := $(RUN)/pids.txt
LOGDIR  := $(RUN)/logs

# Mirror config.Default(): 3 clusters x 12 = 36 servers (override via env).
NUM_CLUSTERS ?= 3
CLUSTER_SIZE ?= 12
NSERVERS     := $(shell echo $$(( $(NUM_CLUSTERS) * $(CLUSTER_SIZE) )))

.PHONY: all build keys up down status test health clean

all: build

build:
	$(GO) build ./...
	mkdir -p $(BIN)
	$(GO) build -o $(BIN)/server ./cmd/server
	$(GO) build -o $(BIN)/client ./cmd/client
	@echo "build OK -> $(BIN)"

keys:
	$(GO) run ./cmd/keygen --out config/keys

up: build
	@mkdir -p $(RUN) $(LOGDIR)
	@if [ -f $(PIDFILE) ]; then echo "pids.txt exists; run 'make down' first."; exit 1; fi
	@for n in $$(seq 1 $(NSERVERS)); do \
		$(BIN)/server --id S$$n > $(LOGDIR)/S$$n.out.log 2> $(LOGDIR)/S$$n.err.log & \
		echo "S$$n $$!" >> $(PIDFILE); \
	done
	@echo "started $(NSERVERS) servers; pids in $(PIDFILE)"

down:
	@if [ ! -f $(PIDFILE) ]; then echo "no pids.txt; nothing to stop."; exit 0; fi
	@while read -r id pid; do \
		kill $$pid 2>/dev/null && echo "stopped $$id ($$pid)" || true; \
	done < $(PIDFILE)
	@rm -f $(PIDFILE)
	@echo "all servers stopped"

status:
	@if [ ! -f $(PIDFILE) ]; then echo "no pids.txt; servers not tracked."; exit 0; fi
	@while read -r id pid; do \
		if kill -0 $$pid 2>/dev/null; then echo "$$id ($$pid) alive"; else echo "$$id ($$pid) dead"; fi; \
	done < $(PIDFILE)

test:
	$(GO) test ./...

health: build
	$(BIN)/client --healthcheck

clean:
	rm -rf $(BIN) $(RUN)
	$(GO) clean
