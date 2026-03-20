# go vet Results

## Summary

Ran `go vet ./...` on both `lib/` and `server/` modules. Results below are annotated by severity.

---

## Real Bugs

### 1. Self-assignment bug — `samehada_util.go:95`

**File:** `lib/samehada/samehada_util/samehada_util.go:95`
**go vet message:** `self-assignment of SlotNum to SlotNum`
**Severity:** Minor (dead code, no functional impact)

```go
SlotNum = binary.BigEndian.Uint32(buf)
SlotNum = SlotNum  // <- self-assignment, no-op
```

**Analysis:** The variable `SlotNum` is assigned to itself on line 95. This is a no-op — the value was already correctly assigned on line 94. The self-assignment appears to be a leftover from debugging or a copy-paste error. While it does not cause incorrect behavior (the correct value is already in `SlotNum`), it indicates sloppy code that could mask a real intent (e.g., was a transformation meant to be applied here?).

### 2. Unreachable code — `table_page.go:86`

**File:** `lib/storage/access/table_page.go:86`
**Severity:** Minor (dead code)

```go
if tuple.Size() <= 0 {
    panic("tuple size is illegal!!!")
    return nil, ErrEmptyTuple  // <- unreachable after panic
}
```

**Analysis:** The `return` statement on line 86 is unreachable because `panic()` on line 85 will always execute first. The `panic` means the process will crash rather than returning the `ErrEmptyTuple` error to the caller. This may or may not be intentional — if the intent was to return an error gracefully, the `panic` should be removed. If the intent was to crash on invalid input, the `return` is simply dead code.

**Note:** `go vet` did not explicitly flag this as "unreachable code" in the output, but static analysis confirms it is unreachable.

---

## Style-Only Warnings (Not Bugs)

### Unkeyed struct literals

Both `lib/` and `server/` modules have numerous warnings about unkeyed struct literal fields. These are in both production code and test files:

- `storage/page/skip_list_page/skip_list_block_page.go` (5 instances)
- `storage/page/skip_list_page/skip_list_header_page.go` (8 instances)
- `container/skip_list/skip_list.go` (multiple instances)
- `samehada/samehada_util/samehada_util_test.go` (14 instances — test file)
- `execution/executors/executor_test/executor_test.go` (many instances — test file)
- `server/main.go` (2 instances)
- Various other files

**Assessment:** These are not bugs. Unkeyed struct literals are a Go style issue — they are fragile if struct fields are reordered, but they are functionally correct. Since this review is bug-focused only, these are noted but not flagged as findings.
