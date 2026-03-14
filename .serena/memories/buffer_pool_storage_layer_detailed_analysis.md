# SamehadaDB Buffer Pool and Storage Layer - Detailed Analysis

## 1. Configuration (lib/common/config.go)

### Key Constants
- **PageSize = 4096 bytes** (4KB) — fixed page size for all pages
- **HeaderPageID = 0** — reserved page ID for database header
- **InvalidPageID = -1** — sentinel value for invalid references
- **InvalidTxnID = -1** — sentinel for invalid transaction IDs
- **BufferPoolMaxFrameNumForTest = 32** — frame count for test pool
- **LogBufferSize = (128 + 1) * 4096** — log buffer (131,072 bytes)

### Type Definitions
```go
type TxnID int32        // transaction ID type
type SlotOffset uintptr // slot offset for page layout
```

---

## 2. Page Structure (lib/storage/page/page.go)

### Page Struct Fields
```go
type Page struct {
    id              types.PageID              // Page identifier (4 bytes)
    pinCount        int32                     // Atomic counter for concurrent access
    isDirty         bool                      // Flag: modified but not flushed
    isDeallocated   bool                      // Flag: page deallocated
    data            *[4096]byte               // Raw 4KB data buffer
    rwLatch         common.ReaderWriterLatch  // RWLatch for concurrency control
    WLatchMap       map[int32]bool            // Debug: tracks write latch owners
    RLatchMap       map[int32]bool            // Debug: tracks read latch owners
}
```

### Page Header Layout (first 8 bytes)
- **Bytes 0-3**: PageID (serialized)
- **Bytes 4-7**: LSN (Log Sequence Number) for recovery

### Key Methods
- **IncPinCount()** / **DecPinCount()** — atomic pin count operations
- **PinCount()** — atomic read of pin count
- **GetPageID()** / **Data()** — accessors
- **SetIsDirty(bool)** / **IsDirty()** — dirty bit management
- **SetIsDeallocated(bool)** / **IsDeallocated()** — deallocation tracking
- **WLatch() / WUnlatch()** — exclusive latch acquire/release
- **RLatch() / RUnlatch()** — shared latch acquire/release
- **GetLSN() / SetLSN()** — recovery support

### Pin Count Semantics
- **Pin count > 0**: Page is in use, cannot be evicted from buffer pool
- **Pin count = 0**: Page is eligible for eviction (added to clock replacer)
- Operations are atomic using `atomic.AddInt32()`

---

## 3. Circular List (lib/storage/buffer/circular_list.go)

### Structure
```go
type circularList struct {
    head       *node
    tail       *node
    size       uint32
    capacity   uint32
    supportMap map[FrameID]*node  // O(1) lookup
}

type node struct {
    key   FrameID
    value bool          // reference bit for clock algorithm
    next  *node
    prev  *node
}
```

### Key Design Features
- **Doubly-linked circular list** — efficient removal and insertion
- **supportMap** — hash map for O(1) membership testing
- **Single-node case**: Head and tail point to self, both next/prev point to self

### Key Methods
- **insert(key, value)** — adds frame if not exists, updates value if exists
- **remove(key)** — removes frame from list and map
- **hasKey(key)** — O(1) membership test
- **isFull()** — check if capacity reached

---

## 4. Clock Replacer (lib/storage/buffer/clock_replacer.go)

### Structure
```go
type ClockReplacer struct {
    cList     *circularList
    clockHand **node              // Pointer to pointer (allows advancing through list)
    mutex     *sync.Mutex         // Not actively used for synchronization
}

const DeallocatedFrame FrameID = math.MaxUint32
```

### Algorithm: Clock Replacement (approximates LRU)
```
Victim():
  1. Check node pointed to by clock hand
  2. If value (reference bit) == true: set to false, advance hand, repeat
  3. If value == false: return this frame as victim, remove from list, advance hand
```

### Key Methods

#### **Victim() *FrameID**
- Returns frame eligible for eviction
- Advances clock hand with each check
- Panics if no victim available (should not happen with proper BPM logic)
- O(k) worst case where k is number of pinned frames

#### **Unpin(id FrameID)**
- Called when pin count drops to 0
- Adds frame to clock replacer with reference bit = true
- If first frame, initializes clock hand to it
- O(1) operation

#### **Pin(id FrameID)**
- Called when frame transitions from 0 to 1 pin count
- Removes frame from clock replacer (cannot evict pinned frame)
- Advances clock hand if it was pointing at this frame
- O(1) operation

#### **isContain(id FrameID)** → bool
- O(1) check via supportMap

---

## 5. Buffer Pool Manager (lib/storage/buffer/buffer_pool_manager.go)

### Structure
```go
type BufferPoolManager struct {
    diskManager       disk.DiskManager       // Disk I/O interface
    pages             []*page.Page           // Array of frames (index = FrameID)
    replacer          *ClockReplacer         // Replacement policy
    freeList          []FrameID              // Available frames (no page yet)
    reUsablePageList  []types.PageID         // Pages marked for deallocation
    pageTable         map[types.PageID]FrameID  // PageID -> FrameID mapping
    logManager        *recovery.LogManager   // For WAL
    mutex             *sync.Mutex            // Global BPM lock
}

var pool = &sync.Pool{}  // Object pool for byte arrays (page-sized buffers)
```

### Initialization
```go
NewBufferPoolManager(poolSize uint32, diskMgr, logMgr):
  - Allocates arrays of size poolSize
  - All frame IDs [0, poolSize) added to freeList
  - Creates ClockReplacer with poolSize capacity
  - All pages initially nil
```

### **FetchPage(pageID PageID) *Page** — Decision Tree

**Locked section (mutex acquired):**

1. **Check pageTable[pageID]:**
   - **Found:**
     - If frameID == DeallocatedFrame: unlock, return nil
     - Increment pin count (atomic)
     - Call replacer.Pin(frameID) to remove from victimization list
     - Increment pin count assert (single-thread mode)
     - Unlock, return page
   
2. **Not found:**
   - Call getFrameID() to obtain frame (from freeList or Victim)
   - If frameID == nil: unlock, return nil (pool full, no victim)

3. **If not from freeList (evicting a victim):**
   - Get current page at frame location
   - Assert pin count == 0 (victim rule)
   - **If victim is dirty:**
     - Flush log manager
     - Write victim's data to disk via diskManager.WritePage()
   - **If victim is deallocated:**
     - Add pageID to reUsablePageList for recycling
   - Remove victim from pageTable
   - Return victim's buffer to pool via ReturnBuffer()

4. **Read new page from disk:**
   - Allocate buffer from pool (or create new)
   - Call diskManager.ReadPage(pageID, buffer)
   - Handle DeallocatedPageErr: return nil
   - Create new Page struct with pinCount=1, isDirty=false
   - Add to pageTable[pageID] = frameID
   - Add to pages[frameID]
   - Unlock

**Return: page with pinCount=1, write-latchable**

### **UnpinPage(pageID PageID, isDirty bool) error**

**Locked section:**

1. Look up pageTable[pageID]
   - Not found: unlock, panic
   - Found: frameID ok

2. If frameID == DeallocatedFrame: unlock, return nil (no-op)

3. Get page = pages[frameID]

4. Decrement pin count (atomic)
   - Assert pin count >= 0

5. If pin count <= 0:
   - Call replacer.Unpin(frameID) to add to victimization list

6. Set dirty flag:
   - If isDirty or page.IsDirty(): set true
   - Else: set false

7. Unlock, return nil

### **NewPage() *Page**

**Locked section:**

1. Get frame ID via getFrameID()
   - If nil: unlock, return nil

2. If not from freeList (victim eviction):
   - Assert victim's pin count == 0
   - Flush log, write dirty victim to disk
   - Add deallocated victim to reUsablePageList
   - Remove victim from pageTable
   - Return victim's buffer to pool

3. **Allocate new page:**
   - **If reUsablePageList not empty:**
     - Pop pageID from reUsablePageList
     - Log a REUSEPAGE record
     - Flush log
   - **Else:**
     - Call diskManager.AllocatePage() to get new pageID
   
4. Create new Page: pinCount=1, isDirty=false
   - Add to pageTable[pageID] = frameID
   - Add to pages[frameID]

5. Unlock, return page (pinCount=1, write-latchable)

### **FlushPage(pageID PageID) bool**

1. Lookup pageTable[pageID]
   - Not found: return false
2. Get page, unlock mutex
3. Set isDirty = false
4. Call diskManager.WritePage(pageID, page.Data())
5. Return success/failure

### **DeallocatePage(pageID PageID, isNoWait bool)**

- If isNoWait=true:
  - Remove from pageTable
  - Add to reUsablePageList
- Log DEALLOCATEPAGE record
- Flush log

### **getFrameID() (*FrameID, bool)**

1. If freeList not empty:
   - Pop and return (frameID, true)
2. Else:
   - Call replacer.Victim()
   - If nil: panic, buffer exhausted
   - Return (frameID, false)

### **Memory Management**
- **GetBuffer()**: pop from sync.Pool or allocate new
- **ReturnBuffer()**: zero-fill and return to pool (prevents information leak)
- **memClr()**: fills byte array with zeros

### **Buffer State Methods**
- **GetPoolSize()** → int: number of pages in pageTable
- **FlushAllPages()**: flush all pages to disk
- **FlushAllDirtyPages()**: flush only dirty pages
- **PrintBufferUsageState()**: debug output of pinned pages

### Concurrency Model
- **Single global mutex** protects all state
- **Pin count is atomic** (can be read without lock)
- **Page latches (RWLatch)** protect page data separately
- **Lock hierarchy**: mutex > page latch > replacer mutex (not enforced)

---

## 6. Disk Manager Interface (lib/storage/disk/disk_manager.go)

```go
type DiskManager interface {
    ReadPage(pageID, []byte) error
    WritePage(pageID, []byte) error
    AllocatePage() types.PageID
    DeallocatePage(types.PageID)
    GetNumWrites() uint64
    ShutDown()
    Size() int64
    RemoveDBFile()
    RemoveLogFile()
    WriteLog([]byte) error
    ReadLog([]byte, int32, *uint32) bool
    GetLogFileSize() int64
    GCLogFile() error
}
```

Two implementations provided.

---

## 7. Real Disk Manager (lib/storage/disk/disk_manager_impl.go)

### Structure
```go
type DiskManagerImpl struct {
    db            *os.File
    fileName      string
    log           *os.File
    fileNameLog   string
    nextPageID    types.PageID    // monotonically increasing
    numWrites     uint64
    size          int64           // current file size
    flushLog      bool
    numFlushes    uint64
    dbFileMutex   *sync.Mutex
    logFileMutex  *sync.Mutex
}
```

### Initialization
- Opens database file (create if not exists)
- Opens log file (separate, derived from DB filename)
- Calculates nextPageID from file size: nPages = fileSize / PageSize
- Seeks to end of log file

### Page I/O

#### **ReadPage(pageID, buffer)**
- Seek to offset = pageID * PageSize
- Check offset <= fileSize
- Read 4096 bytes
- If read < 4096: zero-fill rest of buffer
- Return error

#### **WritePage(pageID, buffer)**
- Seek to offset = pageID * PageSize
- Write exactly 4096 bytes
- Update file size if extended
- No sync (relies on OS buffering or explicit flush)
- Return error

### Page Allocation
```go
AllocatePage():
  ret := nextPageID
  nextPageID++
  return ret
```

**Simple monotonic allocation** — no recycling.

### Log I/O

#### **WriteLog(data []byte)**
- Append to log file
- Sync file to ensure durability
- Return error

#### **ReadLog(buffer, offset, retReadBytes)**
- Seek to offset in log file
- Read into buffer
- Set retReadBytes to bytes read
- Return false if offset >= file size

### Cleanup
- **RemoveDBFile()**: delete DB file after shutdown
- **RemoveLogFile()**: delete log file
- **GCLogFile()**: truncate log file to empty

---

## 8. Virtual Disk Manager (lib/storage/disk/virtual_disk_manager_impl.go)

### Structure
```go
type VirtualDiskManagerImpl struct {
    db             *memfile.File             // In-memory file
    fileName       string
    log            *memfile.File             // In-memory log
    fileNameLog    string
    nextPageID     types.PageID
    numWrites      uint64
    size           int64
    flushLog       bool
    numFlushes     uint64
    dbFileMutex    *sync.Mutex
    logFileMutex   *sync.Mutex
    
    reusableSpceIDs []types.PageID           // Pages available for reuse
    spaceIDConvMap  map[types.PageID]types.PageID  // PageID -> actual storage location
    deallocedIDMap  map[types.PageID]bool    // Deallocated page tracking
}
```

### Page Space Recycling

**Problem:** Normal disk manager wastes space when pages deallocated.

**Solution:** Map logical pageID to physical spaceID:
```
AllocatePage():
  ret := nextPageID
  if reusableSpceIDs not empty:
    spaceID := pop(reusableSpceIDs)
    spaceIDConvMap[ret] = spaceID  // Map logical to physical
  nextPageID++
  return ret

convToSpaceID(pageID):
  if convMap[pageID] exists:
    return convMap[pageID]  // Use recycled space
  else:
    return pageID           // Use logical space
```

### Key Differences from Real DiskManager

1. **Uses memfile.File** (in-memory): no actual disk I/O
2. **Supports page recycling** via spaceIDConvMap
3. **Tracks deallocated pages** to detect invalid reads
4. **ReadPage() checks deallocedIDMap**: returns DeallocatedPageErr if marked deallocated
5. **No logging** (comment: "VirtualDisk can't test logging/recovery")
6. **No file cleanup** (ShutDown, RemoveDBFile, RemoveLogFile are no-ops)

### Use Case
- Unit testing buffer pool without disk I/O overhead
- Testing page recycling logic
- Fast in-memory database for benchmarks

---

## 9. Table Page: Slotted Page Format (lib/storage/access/table_page.go)

### Physical Layout (4096 bytes total)

```
┌─────────────────────────────────────────────────────┐
│ HEADER (24 bytes, offset 0-23)                      │
├─────────────────────────────────────────────────────┤
│ PageID (4)    LSN (4)   PrevPageID (4)              │
│ NextPageID (4) FreeSpacePointer (4) TupleCount (4) │
├─────────────────────────────────────────────────────┤
│ ... SLOT DIRECTORY (variable, offset 24+) ...      │
│ [Tuple0_offset(4)] [Tuple0_size(4)] ...            │
│ [TupleN_offset(4)] [TupleN_size(4)] ...            │
├─────────────────────────────────────────────────────┤
│ ... FREE SPACE ...                                  │
│ (gap between slot directory and tuple data)        │
├─────────────────────────────────────────────────────┤
│ ... INSERTED TUPLES (actual data) ...               │
│ (data grows downward from FreeSpacePointer)         │
└─────────────────────────────────────────────────────┘
  ↑                                         ↑
  offset 0                                  FreeSpacePointer
                                            (grows downward)
```

### Header Layout (offsets in bytes)

```
Offset  Size  Field
------  ----  -----
0       4     PageID (serialized)
4       4     LSN (Log Sequence Number)
8       4     PrevPageID (prev in linked list)
12      4     NextPageID (next in linked list)
16      4     FreeSpacePointer (points to end of free space)
20      4     TupleCount (number of slots in directory)
24+     8*N   Slot Directory: pairs of (offset, size) for each tuple
```

### Key Constants
```go
const (
    sizeTablePageHeader = 24      // Header size
    sizeTuple = 8                 // Each slot: 4B offset + 4B size
    offSetPrevPageID = 8
    offSetNextPageID = 12
    offsetFreeSpace = 16
    offSetTupleCount = 20
    offsetTupleOffset = 24        // Start of slot directory
    offsetTupleSize = 28
)
```

### Deletion Flag Encoding

**Unused space:** Most significant bit (bit 31) of size field encodes deletion status.

```go
const deleteMask = uint32(1 << 31)  // 0x80000000

IsDeleted(size):         size & 0x80000000 != 0
SetDeletedFlag(size):    size | 0x80000000
UnsetDeletedFlag(size):  size & 0x7FFFFFFF
```

### Tuple Access Methods

#### Insert Semantics
1. Find first free slot (GetTupleSize(slot) == 0)
2. Ensure free space >= tuple.Size() + 8 (slot entry)
3. Acquire exclusive lock on RID
4. Decrease FreeSpacePointer by tuple size
5. Write tuple data starting at FreeSpacePointer
6. Write slot entry at offset [24 + 8*slot]
7. If slot == TupleCount, increment TupleCount
8. Log INSERT record with tuple data

#### Delete Semantics (Two-phase)

**MarkDelete(rid):**
1. Set MSB of tuple size (mark as deleted)
2. Log MARKDELETE record
3. Page space NOT reclaimed

**ApplyDelete(rid):**
1. Copy tuple to log (for undo)
2. Shift tuples after deleted tuple upward
3. Update FreeSpacePointer
4. Zero out slot entry
5. Update offsets of all tuples above deleted
6. Log APPLYDELETE record

**RollbackDelete(rid):**
- Unset MSB of tuple size
- Log ROLLBACKDELETE record

#### Update Semantics

**UpdateTuple(newTuple, updateColIdxs, schema):**

1. Copy old tuple data for rollback
2. **If updateColIdxs != nil:** merge only specified columns from newTuple
3. Check space: newSize <= (freeSpace + oldSize)
4. **If newSize > oldSize and not rollback:** return ErrRollbackDifficult (prevents rollback issues)
5. If newSize >= oldSize:
   - Shift tuples down (make room)
   - Update FreeSpacePointer
   - Write new tuple
   - Update offset of affected tuples
6. If newSize < oldSize:
   - Shift tuples up (reclaim space)
   - Update FreeSpacePointer
7. Log UPDATE record

**Special handling for large updates:**
- If new tuple doesn't fit even after freeing old space: return ErrNotEnoughSpace
- Caller then does: DELETE old + INSERT new (in different page if needed)

### Free Space Calculation

```go
getFreeSpaceRemaining():
  return FreeSpacePointer - sizeTablePageHeader - (sizeTuple * TupleCount)
  
Available for next insert = FreeSpacePointer - 24 - 8*TupleCount
```

### LinkedList Navigation

```go
GetNextPageID() PageID     // Follow chain to next page
GetPrevPageID() PageID     // Follow chain to prev page (for scans)
```

### Type Casting

```go
CastPageAsTablePage(page *Page) *TablePage:
  return (*TablePage)(unsafe.Pointer(page))
```

**Why unsafe.Pointer?**
- TablePage embeds Page: `type TablePage struct { page.Page }`
- Casting preserves all Page data
- Safe because TablePage has no additional fields

### Logging Integration

Every operation creates log records:
- **INSERT**: LogRecordInsertDelete with tuple data
- **UPDATE**: LogRecordUpdate with old + new tuple
- **MARKDELETE**: LogRecordInsertDelete with empty tuple
- **APPLYDELETE**: LogRecordInsertDelete with tuple data
- **ROLLBACKDELETE**: LogRecordInsertDelete with empty tuple
- All set page LSN to returned LSN

---

## 10. Table Heap: Linked List of Pages (lib/storage/access/table_heap.go)

### Structure

```go
type TableHeap struct {
    bpm          *buffer.BufferPoolManager
    firstPageID  types.PageID               // Head of linked list
    logManager   *recovery.LogManager
    lockManager  *LockManager
    lastPageID   types.PageID               // Optimization: memorized last page for inserts
}
```

### Initialization

#### **NewTableHeap(bpm, logMgr, lockMgr, txn) *TableHeap**
1. Allocate new page via bpm.NewPage()
2. Cast to TablePage
3. Write latch it
4. Call Init(pageID, InvalidPageID, ...) to initialize header
5. Flush page to disk (for recovery)
6. Unpin page
7. Return TableHeap with firstPageID set

#### **InitTableHeap(bpm, existingPageID, ...) *TableHeap**
- Open existing table at given page ID
- No new page allocation

### Insert Strategy

**TableHeap.InsertTuple(tpl, txn, oid, isForUpdate):**

1. Fetch page at lastPageID (optimization: start from last used page)
2. Write latch current page
3. **Loop:**
   - Try InsertTuple() on current page
   - Success or ErrEmptyTuple: break
   - Page full: check GetNextPageID()
     - Exists: unpin current, move to next page
     - Doesn't exist: allocate new page, link via SetNextPageID()
4. Set lastPageID to inserted page
5. Unpin page (dirty=true)
6. Add to transaction write set

**Optimization:** lastPageID tracking avoids re-scanning first pages when inserting many tuples. Reset to firstPageID on delete/update to find space.

### Update Strategy

**TableHeap.UpdateTuple(tpl, updateColIdxs, schema, oid, rid, txn, isRollback):**

1. Fetch page containing RID
2. Write latch it
3. Call TablePage.UpdateTuple()
   - Returns: (isSuccess, error, needFollowTuple)
4. Unpin with dirty=isSuccess
5. **If update failed due to space:**
   - If ErrNotEnoughSpace or ErrRollbackDifficult:
     - Delete old tuple (MarkDelete or ApplyDelete)
     - Insert new tuple (may go to next page)
     - Return newRID != oldRID

### Delete Strategy

**TableHeap.MarkDelete(rid, oid, txn):**
1. Fetch page containing RID
2. Write latch
3. Call TablePage.MarkDelete()
4. Unpin (dirty=true)
5. Reset lastPageID to firstPageID (optimize next insert for space)

**TableHeap.ApplyDelete(rid, txn):**
1. Fetch and write latch page
2. Call TablePage.ApplyDelete()
3. Reset lastPageID to firstPageID
4. Unpin (dirty=true)

**TableHeap.RollbackDelete(rid, txn):**
1. Fetch and write latch page
2. Call TablePage.RollbackDelete()
3. Unpin (dirty=true)

### Retrieval

**TableHeap.GetTuple(rid, txn) (*Tuple, error):**

1. Acquire lock if not already held:
   - Try to get shared lock
   - If fails: abort transaction
2. Fetch page
3. Read latch page
4. Call TablePage.GetTuple(rid, ...)
5. Release read latch
6. Unpin (dirty=false)
7. Handle special cases:
   - ErrSelfDeletedCase: tuple deleted by current txn (expected)
   - ErrGeneral: tuple deleted by other txn (abort current)

### Iterator

**TableHeap.Iterator(txn) *TableHeapIterator:**
- Creates iterator starting at first valid tuple
- Returns TableHeapIterator struct

**TableHeap.GetFirstTuple(txn) *Tuple:**
1. Iterate through pages from firstPageID
2. Find first non-deleted tuple
3. GetTuple() to acquire locks
4. Return tuple with locks held

---

## 11. Table Heap Iterator (lib/storage/access/table_heap_iterator.go)

### Structure

```go
type TableHeapIterator struct {
    tableHeap    *TableHeap
    tuple        *tuple.Tuple
    lockManager  *LockManager
    txn          *Transaction
}
```

### State Machine

1. **NewTableHeapIterator**: Initialize at first valid tuple
2. **Current()**: Returns current tuple
3. **End()**: Returns true if tuple == nil
4. **Next()**: Advance to next valid tuple

### Next() Algorithm

```
1. Fetch current page
2. Read latch it
3. Get next RID in current page:
   - nextTupleRID = page.GetNextTupleRID(currentRID, false)
4. If nextTupleRID == nil (no more tuples in page):
   - Loop through next pages until find one with tuples
   - Unpin old page, fetch next page
   - Read latch new page
   - Find first tuple (isNextPage=true)
5. If found tuple:
   - GetTuple(nextRID, ...)
   - Handle ErrSelfDeletedCase: retry from start (goto start)
6. Unpin and unlatch page
7. Return tuple (or nil if end)
```

### Latch Management

- **Read latches only** (no write)
- **RLatch/RUnlatch** per page transition
- Pages unpinned before advancing
- Prevents deadlock on concurrent updates

### Handling Deleted Tuples

- TablePage.GetNextTupleRID() only returns non-deleted tuples
- If tuple marked deleted: GetTuple() may return ErrSelfDeletedCase
- Iterator restarts search from beginning (rare case)

---

## 12. RID: Record Identifier (lib/storage/page/rid.go)

```go
type RID struct {
    PageID  types.PageID
    SlotNum uint32
}
```

**Fields:**
- **PageID**: 4-byte page identifier
- **SlotNum**: 4-byte slot index in page's slot directory

**Methods:**
- **Set(pageID, slot)**: Set both fields
- **GetPageID()**: Return PageID
- **GetSlotNum()**: Return SlotNum
- **GetAsStr()**: Format as string for debugging

**Invariant:** (PageID, SlotNum) pair uniquely identifies a tuple in the entire table.

---

## 13. Integration: Key Data Flow

### Insert Flow

```
SQL INSERT
    ↓
Executor.Insert()
    ↓
TableHeap.InsertTuple(tuple)
    ↓
BPM.FetchPage(lastPageID)
  → Check pageTable, if not found: read from disk
  → Pin count++, add to clock replacer
    ↓
TablePage.InsertTuple(tuple)
  → Find free slot
  → Write tuple data at FreeSpacePointer
  → Write slot directory entry
  → Log INSERT record
    ↓
BPM.UnpinPage(pageID, isDirty=true)
  → Pin count--
  → If pin count == 0: add to clock replacer
    ↓
Log record buffered, flush on UnpinPage
    ↓
On eviction (clock replacer.Victim):
  → Flush log first
  → Write dirty page to disk
  → Return frame to pool
```

### Select/Scan Flow

```
SQL SELECT (sequential scan)
    ↓
Executor.SeqScan()
    ↓
TableHeapIterator.Next()
    ↓
For each page:
  BPM.FetchPage(pageID)
    → Pin count++
      ↓
    TablePage.GetNextTupleRID()
      → Scan slot directory, find next non-deleted
        ↓
    TablePage.GetTuple(rid)
      → Copy tuple data
      → Check deletion flag
        ↓
    BPM.UnpinPage(pageID, isDirty=false)
      → Pin count--
```

### Delete Flow

```
SQL DELETE
    ↓
Executor.Delete()
    ↓
TableHeap.MarkDelete(rid)
    ↓
BPM.FetchPage(rid.PageID)
  → Pin count++
    ↓
TablePage.MarkDelete(rid)
  → Set deletion flag bit in size
  → Log MARKDELETE record
    ↓
BPM.UnpinPage(pageID, isDirty=true)
  → Pin count--

[Later] Commit:
  ApplyDelete() removes tuple completely

[Or] Rollback:
  RollbackDelete() unsets deletion flag
```

### Buffer Pool Eviction Flow

```
BPM.FetchPage(newPageID) [when buffer full]
    ↓
getFrameID() needed
    ↓
ClockReplacer.Victim()
  → Scan circular list for frame with ref_bit=0
  → Clear ref_bits of scanned frames
  → Return victim frame
    ↓
Evict page at [victim_frameID]:
  ↓
  If dirty:
    logManager.Flush()  ← Ensure all logs on disk
    diskManager.WritePage(pageID, data)
  ↓
  If deallocated:
    reUsablePageList.append(pageID)
  ↓
  pageTable.delete(pageID)
  ReturnBuffer(page)  ← Zero-fill and return to pool
    ↓
Load new page:
  diskManager.ReadPage(newPageID, buffer)
    ↓
Create Page(newPageID, buffer), pinCount=1
```

---

## 14. Key Design Decisions

### 1. Pin Count vs Latches
- **Pin count**: Controls eviction eligibility (buffer-level)
- **RWLatch**: Controls concurrent access to page data (data-level)
- **Separation of concerns**: Multiple readers can hold pin without latch

### 2. Deletion Flag Encoding
- Uses MSB of size field instead of separate field
- Saves space (no extra slot directory entries)
- Fast: single bit check via mask

### 3. Two-Phase Deletion
- MarkDelete: marks logically deleted (prevents further access)
- ApplyDelete: physically reclaims space (safe at commit)
- RollbackDelete: undoes mark
- Enables rollback without space reorganization

### 4. Free Space Pointer Direction
- Points to end of used space (decreases as tuples inserted)
- Tuple data grows downward: [header] [directory] ↓ [tuples]
- Slot directory grows upward: [header] ↓ [directory] ... [tuples]
- Collision detection: FSP <= 24 + 8*TupleCount

### 5. Page Recycling in Virtual Disk
- spaceIDConvMap: logical pageID → physical storage location
- Allows testing without real deallocated page detection
- Real disk manager has simpler allocation (no recycling)

### 6. Clock Replacer Reference Bit
- Set to true on Unpin (page added to pool)
- Cleared on Victim() scan if encountered
- Removed from pool on Pin()
- Approximates LRU with lower overhead than true LRU

### 7. lastPageID Optimization
- Memorized last page for sequential inserts
- Reduces page fetches for bulk inserts
- Reset to firstPageID on delete/update (seek for space)

### 8. Unsafe Pointer Casting
- CastPageAsTablePage: embeds Page in TablePage struct
- unsafe.Pointer preserves memory layout
- Safe because no additional fields in TablePage

### 9. Global BPM Mutex
- Single lock protects all state (simple, but may serialize)
- Held briefly: only during frame allocation/eviction
- Page latches provide fine-grained concurrency

### 10. Write-Ahead Logging
- Log record created before page modified (Write-Ahead)
- LSN stored in page header (first 8 bytes)
- Log flushed before evicting dirty pages
- Ensures durability and recovery correctness

---

## 15. Concurrency & Isolation

### Lock Hierarchy
1. **Transaction Lock** (via LockManager): Transaction-level locks on RIDs
2. **Page Latch** (RWLatch): Reader/writer latches on page data
3. **BPM Mutex**: Global lock on frame allocation

### Multi-Version Concurrency
- No MVCC; uses **strict 2-phase locking** (2PL)
- Shared locks (read): acquired for SELECT
- Exclusive locks (write): acquired for INSERT/UPDATE/DELETE
- Locks held until transaction commit/abort

### Dirty Read Prevention
- MarkDelete prevents other txns from reading deleted tuples
- ApplyDelete only at commit
- Uncommitted modifications isolated to transaction

### Phantom Read Prevention
- Predicate locks on table/range scans
- Not visible in this code; likely in LockManager

---

## 16. Performance Characteristics

### Buffer Pool Operations
- **FetchPage**: O(1) pageTable lookup, O(k) worst-case victim search
- **NewPage**: O(1) frame allocation + disk I/O
- **UnpinPage**: O(1) frame return
- **Eviction**: Disk I/O + log flush (blocking)

### Table Heap Operations
- **Insert**: O(n) page traversal (n = num pages) + O(m) slot search (m = tuples/page)
- **Update**: O(n) + O(m) + O(m) (shift tuples)
- **Delete**: O(n) + O(m)
- **Select**: O(n*m) full scan

### Slotted Page Operations
- **Slot lookup**: O(1) via offset array
- **Tuple search**: O(m) linear scan of slots
- **Insertion**: O(m) space search + copy
- **Deletion**: O(m) offset updates

---

## 17. Testing & Debugging

### Test Constants
- **BufferPoolMaxFrameNumForTest = 32**: Small pool for test scenarios
- **VirtualDiskManagerImpl**: In-memory storage for unit tests
- **EnableDebug, EnableOnMemStorage**: Config for test modes

### Debug Features
- **WLatchMap, RLatchMap**: Track latch holders per page
- **PrintBufferUsageState()**: List all pinned pages
- **PrintReplacerInternalState()**: Show clock replacer list
- **Assertions**: PinCountAssert, DEBUGGING, RDBOpFuncCall

### Panic Conditions
- Pin count < 0: "pin count is less than 0"
- Victim pin count ≠ 0: "pin count of page to be cache out must be zero"
- Buffer exhausted: "getFrameID: Victime page is nil"
- Illegal pointer: "freeSpacePointer value to be set is illegal"

---

## 18. Future Improvements / Known Issues

### Comments in Code
1. **Direct I/O**: Commented-out directio calls for aligned I/O
2. **MVCC**: Not implemented; uses strict 2PL
3. **Bitmap allocation**: Header page should track free pages (currently not)
4. **Predicate locks**: Not visible; likely needed for phantom prevention
5. **Thread safety of clock hand**: Comments suggest mutex not actively used

### Edge Cases Mentioned
- Recovery phase: IsRecoveryPhase() bypasses locking
- Self-deleted tuples: handled with special return value
- Statistics updater: causes rare I/O errors (noted TODO)
- Deallocated pages: special error type DeallocatedPageErr

---

## Summary Table

| Component | Key Responsibility | Size | Thread-Safe |
|-----------|-------------------|------|------------|
| Page | Storage unit, pinning, latching | 4KB | Via RWLatch + atomic pinCount |
| CircularList | Frame tracking for replacer | Variable | No (used by replacer only) |
| ClockReplacer | Frame eviction policy | ≤ poolSize | Via mutex (not used) |
| BPM | Frame allocation, page caching | poolSize * 4KB | Via global mutex |
| DiskManager | I/O abstraction | Varies | Via file mutexes |
| TablePage | Slotted page format, tuple ops | 4KB | Via page latch |
| TableHeap | Table as linked pages | Variable | Via BPM + page latches |
| Iterator | Page-by-page traversal | Constant | Via page latches |

---

## Code Locations

- **Config**: `/lib/common/config.go`
- **Page**: `/lib/storage/page/page.go`, `/lib/storage/page/rid.go`
- **CircularList**: `/lib/storage/buffer/circular_list.go`
- **ClockReplacer**: `/lib/storage/buffer/clock_replacer.go`
- **BPM**: `/lib/storage/buffer/buffer_pool_manager.go`
- **DiskManager**: `/lib/storage/disk/disk_manager.go`, `disk_manager_impl.go`, `virtual_disk_manager_impl.go`
- **TablePage**: `/lib/storage/access/table_page.go`
- **TableHeap**: `/lib/storage/access/table_heap.go`
- **Iterator**: `/lib/storage/access/table_heap_iterator.go`

