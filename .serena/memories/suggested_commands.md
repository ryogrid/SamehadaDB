# Suggested Commands

## Testing
```bash
# Run all unit tests (short mode, skips long-running tests)
cd /home/ryo/work/SamehadaDB/lib && go clean -testcache; go test ./... -short -v

# Run tests for a specific package
cd /home/ryo/work/SamehadaDB/lib && go test ./storage/access/... -short -v

# Run a specific test
cd /home/ryo/work/SamehadaDB/lib && go test ./samehada/samehada_test/... -run TestName -v
```

## Building
```bash
# Build the server binary
cd /home/ryo/work/SamehadaDB/server && go build -o samehada-db-server main.go
```

## Running
```bash
# Run the REST API server (listens on http://0.0.0.0:19999/Query)
cd /home/ryo/work/SamehadaDB/server && ./samehada-db-server
```

## Important Configuration
- `lib/common/config.go`: `EnableOnMemStorage` constant controls whether to use in-memory virtual storage (set to `false` for disk-based testing)
- CI changes this to `false` via sed before running tests

## System Utilities
- `git` - Version control
- `go` - Go toolchain (1.21+)
- Standard Linux: `ls`, `cd`, `grep`, `find`, etc.

## Notes
- No Makefile exists; use `go` commands directly
- No linter or formatter configured in CI; use standard `gofmt` / `go vet` as needed
- Two separate Go modules: run tests from `lib/` directory, build server from `server/` directory
