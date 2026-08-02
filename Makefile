BINARY  := occa
CGO     := CGO_ENABLED=0
LINT_VER := v2.12.2

.PHONY: build install test lint fmt vet deadcode check

build:
	$(CGO) go build -o $(BINARY) ./cmd/occa

install:
	$(CGO) go install ./cmd/occa

test:
	go test ./... -race -count=1

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(LINT_VER) run ./...

fmt:
	gofmt -l -w .

vet:
	go vet ./...

# relay/cli is an alternative backend not wired into the main binary — expected dead code.
deadcode:
	@echo "=== deadcode (relay/cli excluded) ==="
	go run golang.org/x/tools/cmd/deadcode@latest ./... | grep -v "relay/cli" || true

check: fmt vet lint test deadcode
