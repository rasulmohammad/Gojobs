# Work-Back Plan: Durable, Effectively-Once Task Queue (Go)

My reference for what I'm building and in what order. I build every line; Claude coaches
with guiding questions. Each phase is a committable chunk. Check tasks off as I go.

## The one idea the whole project rests on
"Effectively-once" is not one mechanism. It is two:
1. **At-least-once delivery** — the queue never loses a task; if unsure, it redelivers.
2. **Idempotent / deduplicated processing** — the consumer (and enqueue path) collapses
   duplicates so redelivery does no harm.
Together they *look* exactly-once from the outside. Every phase builds one half or the other.

## Locked decisions
- **Backend:** write-ahead log, built from scratch (fsync, length+CRC record encoding, replay). No Postgres in the core path.
- **Interface:** in-process Go library (Enqueue / Dequeue(lease) / Ack / Nack). Network layer is a stretch goal only.
- **Go teaching depth:** heavy — each phase names the Go concepts, links one doc, offers a small warm-up.

## Durability, defined (the thing the project earns)
Once `Enqueue` returns success, the task survives a crash. That requires the record be
`fsync`'d (`file.Sync()`) to disk *before* the call returns — a buffered `write()` alone is
not durable. Durability = the write side (Phase 2). Recovery = replay on restart (Phase 3).
Need both.

## Architecture sketch (target end state)
```
producers --Enqueue(task, idempotencyKey)--+
                                           v
                          +------------------------------+
                          |            Queue             |
                          |  in-memory index (rebuilt    |
                          |  from WAL on startup):       |
                          |   - ready queue              |
                          |   - inflight (leased)+timers |
                          |   - dedup: idemKey -> state  |
                          +-------------+----------------+
                                        | every state change appended first
                                        v
                          +------------------------------+
                          |   WAL (append-only, fsync)   |
                          |   ENQUEUE/LEASE/ACK/RETRY/    |
                          |   DEAD records                |
                          |   length + CRC per record     |
                          +------------------------------+
                                        ^ replay on startup rebuilds index
consumers --Dequeue()->lease--work--Ack()/Nack()--+
```
Rule that makes it durable: **write-ahead**. Append + fsync to the WAL *before* the change
is committed in memory. Recovery = replay the log.

## Suggested project layout
Start flat (one package, several files) for Phases 1-3:
```
task_queue/
  go.mod                // module taskqueue
  task.go               // Task struct, State enum
  queue.go              // Queue, Enqueue/Dequeue/Ack/Nack
  wal.go                // WAL: Append, Sync, Replay
  record.go             // record encode/decode + CRC
  *_test.go
  notes/work-back-plan.md
```
Promote to packages around Phase 4 once boundaries are firm:
`internal/wal/` (opaque []byte records, knows nothing about tasks) and `internal/queue/`
(imports wal), plus `cmd/loadtest/main.go`. Dependency direction only ever
`cmd -> queue -> wal`, never backward. Keep `wal` ignorant of `Task`.

---

## Success criteria (definition of done)
- [ ] A consumer can crash at any point; on restart, no enqueued task is lost and no task's effect is applied twice.
- [ ] Duplicate enqueues (same idempotency key) -> exactly one logical task and one applied effect.
- [ ] Load test reports: throughput (records/sec), p95/p99 latency, correctness counters (produced == uniquely-processed; zero double-applies).
- [ ] A short writeup publishes those numbers with methodology.

---

## Phase 0 — Environment + Go fundamentals
_Commit: `chore: bootstrap go module and hello-world`_
- [ ] 0.1 Install Go (`brew install go`), confirm `go version`.
- [ ] 0.2 `go mod init taskqueue` -> `go.mod` exists.
- [ ] 0.3 hello-world `main.go`; run `go run .` and `go build`; understand the difference.
- [ ] 0.4 One trivial `_test.go`; `go test ./...` -> PASS.
- **Go concepts:** packages, modules, `func main`, `go run` vs `build` vs `test`.
- **Reading:** A Tour of Go (go.dev/tour) intro + Basics; "How to Write Go Code" (go.dev/doc/code).

## Phase 1 — Data model + in-memory queue (NO durability yet)
_Commit: `feat: in-memory queue with enqueue/dequeue/ack/nack`_
Goal: get semantics right with zero I/O. **Phase 1 has zero durability by design.**
- [ ] 1.1 `Task` struct: ID, IdempotencyKey, Payload []byte, State (ready/inflight/done/dead), Attempts, EnqueuedAt, LeaseUntil.
- [ ] 1.2 `Queue` type: in-memory index (map ID->*Task) + ready list.
- [ ] 1.3 `Enqueue(payload, idemKey) (id, error)` -> append to ready (no dedup yet).
- [ ] 1.4 `Dequeue() (*Task, bool)` -> pop ready, mark inflight; FIFO; empty returns false.
- [ ] 1.5 `Ack(id)` marks done; `Nack(id)` returns to ready. Test full paths.
- **Go concepts:** structs, methods/receivers, slices vs maps, `iota` enums, multi-return, `error`, zero values.
- **Reading:** Tour of Go — Methods and interfaces; Go by Example — Structs, Maps, Errors.

## Phase 2 — Write-ahead log: append + fsync + encoding
_Commit: `feat: write-ahead log with fsync and crc-framed records`_
Goal: durably record every state change. Heart of the "from scratch" part.
- [ ] 2.1 Decide record format on paper first: `[len uint32][crc32 uint32][type uint8][payload]`. Write it as a comment. Know why len AND crc exist.
- [ ] 2.2 `encodeRecord(type, payload) []byte` and `decodeRecord(reader)` via `encoding/binary`; round-trip test.
- [ ] 2.3 `WAL` type: opens a file; `Append(record) (offset, error)` writes then `file.Sync()` (fsync). Verify bytes are on disk (reopen + read).
- [ ] 2.4 Define op record types: ENQUEUE (id+idemKey+payload), LEASE, ACK, RETRY, DEAD; round-trip each.
- [ ] 2.5 Wire Phase 1 Queue so every mutation appends the WAL record *before* updating memory. Old tests still pass; WAL grows.
- **Go concepts:** `os.File`, `bufio.Writer`, `file.Sync()`, `encoding/binary`, `hash/crc32`, `defer file.Close()`.
- **Reading:** pkg.go.dev os / bufio / encoding/binary; Go by Example — Reading/Writing Files.
- **Guiding Q:** fsync every append or batch? (every-append first; batching is Phase 8). What is a torn write and how does CRC catch it?

## Phase 3 — Replay + crash recovery of state
_Commit: `feat: replay wal on startup to rebuild queue state`_
Goal: rebuild exact in-memory state from the log after restart.
- [ ] 3.1 `Replay()` reads records front-to-back and applies each. Enqueue 5 / ack 2, drop Queue, replay same file, assert 3 ready + 2 done.
- [ ] 3.2 Torn tail record: bad len/crc on last record -> stop replay cleanly, treat log as ending before it. Truncate mid-record in a test; recover last good state.
- [ ] 3.3 Inflight-on-recovery policy: a LEASE'd-but-never-ACK'd task returns to ready after replay (at-least-once). Test it.
- **Go concepts:** loops with `io.EOF`, error wrapping (`fmt.Errorf("...: %w", err)`), read-to-end, table-driven tests.
- **Reading:** Go blog — Error handling and Go; Tour of Go — errors.
- **Guiding Q:** simulate a crash without killing the process (fresh Queue over same file, no graceful shutdown). Why does at-least-once *require* redelivering un-acked leases?

## Phase 4 — Concurrency: leasing, visibility timeout, multiple consumers
_Commit: `feat: concurrent leasing with visibility timeout sweeper`_
Goal: many consumers pull safely in parallel; a dead consumer's task is redelivered after lease timeout.
- [ ] 4.1 Mutex on Queue; make Enqueue/Dequeue/Ack/Nack concurrency-safe. `go test -race` clean under many goroutines.
- [ ] 4.2 Dequeue becomes a *lease*: `LeaseUntil = now + visibilityTimeout`, log LEASE record.
- [ ] 4.3 Background sweeper goroutine returns expired leases to ready. Short timeout, no ack -> becomes ready again.
- [ ] 4.4 `context.Context` stops the sweeper cleanly on shutdown; no goroutine leak.
- **Go concepts:** goroutines, channels, `sync.Mutex`/`RWMutex`, `sync.WaitGroup`, `context.Context`, `time.Ticker`, `-race`, `select`.
- **Warm-up:** throwaway worker pool of 3 goroutines on a channel (Go by Example — Worker Pools) before the real sweeper.
- **Reading:** Tour of Go — Concurrency; Go by Example — Goroutines, Channels, Mutexes, WaitGroups, Timers, Context.
- **Guiding Q:** mutex-around-map vs channel-based queue, which fits and why? How do I prove there's no goroutine leak?

## Phase 5 — Retries, backoff, dead-letter
_Commit: `feat: bounded retries with exponential backoff and dead-letter`_
- [ ] 5.1 On Nack: increment Attempts, set next-eligible = now + backoff(Attempts) (exponential, capped), log RETRY. Not re-dequeued until backoff elapses.
- [ ] 5.2 `maxAttempts`: on exceed, mark `dead`, log DEAD instead of retrying. Always-nack task -> dead after exactly maxAttempts.
- [ ] 5.3 List/drain dead-letter tasks; they survive a replay.
- **Go concepts:** `time.Duration`, `time.Now()`, integer math for backoff, more enum states.
- **Reading:** pkg.go.dev/time; AWS Architecture blog — Exponential Backoff And Jitter (the why, incl. jitter).
- **Guiding Q:** do I need jitter for a single-node in-process queue? Should backoff state live in the WAL? (must survive recovery -> yes).

## Phase 6 — Idempotency keys + dedup (the effectively-once core)
_Commit: `feat: idempotent enqueue and consumer-side dedup`_
Goal: duplicates on enqueue collapse to one task; duplicates on processing do no harm. Mirrors the billing work.
- [ ] 6.1 Dedup index `idemKey -> taskID/state`, rebuilt during replay; matches tasks after replay.
- [ ] 6.2 Idempotent Enqueue: existing idemKey -> return existing ID, no second task, no second ENQUEUE record. Enqueue same key 100x -> exactly one task/record.
- [ ] 6.3 Consumer-side idempotency: `Ack(id, resultKey)` records the unit of work as applied so a redelivered-then-acked task can't double-apply. Tiny example effect (counter per idemKey). Force redelivery + second processing -> counter increments once.
- **Go concepts:** map-as-set, composite keys, invariants/assertions in tests.
- **Reading:** re-read my own mental model from the billing project; map it onto these APIs.
- **Guiding Q (bring to Claude):** where is the exactly-once boundary — queue, consumer, or effect store? Difference between dedup-at-enqueue vs dedup-at-apply, and do I need both?

## Phase 7 — Correctness under fault injection
_Commit: `test: fault-injection suite for duplicates and crashes`_
Goal: prove the two guarantees, don't just assert them.
- [ ] 7.1 Duplicate-injection: producers re-send a fraction of keys; unique logical tasks == unique keys, across many seeds.
- [ ] 7.2 Crash-during-append: WAL can "fail" after N bytes (test hook); trigger, replay, assert consistent state (no half-applied record accepted) across crash points.
- [ ] 7.3 End-to-end crash: run producers+consumers, drop/rebuild Queue at random points; produced == processed-at-least-once AND effects-applied == unique keys. Passes repeatedly.
- **Go concepts:** interfaces as test seams (swap a faulty `syncer`/`writer`), randomized/property testing, `testing.T` helpers, subtests.
- **Reading:** Go blog — Table-driven tests; pkg.go.dev/testing/quick (optional).
- **Guiding Q:** inject a crash at a precise point without real kills (interface seam that errors/panics on the Nth call). What invariant exactly am I asserting after each crash?

## Phase 8 — Load testing + metrics
_Commit: `feat: load harness with throughput and latency percentiles`_
Goal: produce the publishable numbers honestly.
- [ ] 8.1 Load harness `cmd/loadtest`: configurable producers, consumers, duration, payload size; prints task counts.
- [ ] 8.2 Throughput = total processed / wall-clock seconds; stable across runs.
- [ ] 8.3 Latency: record enqueue->ack per task; p50/p95/p99 (collect-into-slice-then-sort first; note memory cost).
- [ ] 8.4 fsync bottleneck: per-append fsync vs batched group-commit (fsync per N appends or per T ms). Report both; explain the durability/latency tradeoff chosen.
- **Go concepts:** `time` measurement, `sort`, `flag`, `sync/atomic` counters, benchmarks (`go test -bench`).
- **Reading:** pkg.go.dev flag / sort; Go by Example — Command-Line Flags.
- **Guiding Q:** does my latency include or exclude queue wait time, and which do I report? How do I keep the harness itself from being the bottleneck?

## Phase 9 — Writeup / publish the numbers
_Commit: `docs: readme with architecture, results, and limitations`_
- [ ] 9.1 README: what it is, the effectively-once framing, the architecture diagram.
- [ ] 9.2 Results: throughput, p50/p95/p99, hardware, methodology, the fsync tradeoff, correctness guarantees + how tested.
- [ ] 9.3 Honest limitations: single-node, in-process, no compaction yet, etc.
- [ ] A peer could read it and reproduce the numbers.

---

## Stretch goals (only after Success Criteria are met)
- [ ] Compaction / snapshots: WAL grows forever -> periodic snapshot + log truncation; replay = load snapshot then tail.
- [ ] Segment files: roll the WAL into multiple files.
- [ ] Pluggable backend: define a `Store` interface, add a Postgres impl (`SELECT ... FOR UPDATE SKIP LOCKED`); contrast with the WAL.
- [ ] Network layer: wrap the library in HTTP or gRPC with separate producer/consumer processes.

## How I work with Claude
- I build every line. Claude answers guiding questions, reviews designs, unblocks.
- When stuck, bring: what I expected, what happened, the smallest failing test.
- Full roadmap also at `~/.claude/plans/plan-this-project-out-cached-scone.md`.
