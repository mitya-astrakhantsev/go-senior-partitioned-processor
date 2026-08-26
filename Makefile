.PHONY: test test-race vet fmt-check verify

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; exit 1; }

verify: fmt-check vet test-race
