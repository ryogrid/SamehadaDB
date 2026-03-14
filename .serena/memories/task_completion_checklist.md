# Task Completion Checklist

When a task is completed, perform the following steps:

1. **Run tests**: Execute relevant unit tests to verify correctness
   ```bash
   cd /home/ryo/work/SamehadaDB/lib && go clean -testcache; go test ./... -short -v
   ```

2. **Build check**: Ensure the server still builds
   ```bash
   cd /home/ryo/work/SamehadaDB/server && go build -o samehada-db-server main.go
   ```

3. **Verify with disk storage**: If changes touch storage/buffer/access layer, also test with `EnableOnMemStorage = false`
   (CI does this automatically via sed)

4. **Check for race conditions**: If changes involve concurrency, consider running tests with `-race` flag
   ```bash
   cd /home/ryo/work/SamehadaDB/lib && go test ./... -short -race -v
   ```

5. **No linter/formatter in CI** but standard `gofmt` should be applied to modified files
