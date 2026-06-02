# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
	GOBIN=$(shell go env GOPATH)/bin
else
	GOBIN=$(shell go env GOBIN)
endif

# if grc is installed, use `grc go` instead of just `go`
GRC := $(shell command -v grc 2> /dev/null)
ifdef GRC
	GO ?= grc go
else
	GO ?= go
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: run 

.PHONY: fmt
fmt: 
	$(GO) fmt ./...

.PHONY: vet
vet: 
	$(GO) vet ./...

.PHONY: run
run: fmt vet
	$(GO) run ./...

.PHONY: test
test: fmt vet
	if [[ -z "${TEST}" ]]; then $(GO) test ./... -v; else $(GO) test ./... -v -run "${TEST}"; fi

.PHONY: build
build: fmt vet
	$(GO) build -o bin/valkey-encoding-analyzer cmd/*.go