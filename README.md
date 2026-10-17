# Parallel Packet Firewall Simulator

A multi-threaded packet firewall simulator written in **C (GNU C99)** with **POSIX threads**. A producer reads packets from a file and publishes them into a bounded **ring buffer**; several consumer threads filter the packets in parallel and write the decisions to a log, **in timestamp order, while processing is still running**.

The point of the project is the synchronization: correct producer/consumer coordination with mutexes and condition variables (no busy-waiting), and an ordered output despite out-of-order, parallel processing.

---

## How it works

```
 input file ──► producer ──► ring buffer ──► consumer 1 ─┐
                (1 thread)   (mutex + 2     consumer 2 ─┼─► min-heap ──► log file
                              cond vars)    ...         ─┘   (ordered by sequence number)
```

### Ring buffer (`src/ring_buffer.c`)
* Fixed-size circular byte buffer, protected by one mutex and two condition variables (`not_full`, `not_empty`). The producer sleeps when the buffer is full; consumers sleep when it is empty. No spinning, no `sleep()`.
* Copies wrap around the end of the buffer (two `memcpy` segments).
* `ring_buffer_stop()` wakes every waiter; consumers drain what is left and then exit.
* `ring_buffer_dequeue_seq()` additionally returns the **sequence number** of the dequeued packet. The number is assigned under the same lock as the dequeue, so it is exactly the producer's send order, even with many consumers.

### Consumers and ordered logging (`src/consumer.c`)
Packet filtering (hashing + rule matching) is the expensive part and runs fully in parallel, with no lock held. The problem is that threads finish in a non-deterministic order, but the log must be sorted by timestamp.

* Every processed packet is pushed into a custom **min-heap** keyed by its sequence number (timestamps increase strictly in the same order, so sequence order = timestamp order).
* After each push, a thread checks whether the next packet to be written (`next_write_seq`) is at the top of the heap. If so, it pops the run of consecutive ready packets and writes them to the log.
* Only one thread flushes at a time. It **releases the heap lock while calling `write()`** (lines are batched in a 4 KB buffer), so the other consumers keep processing and inserting. A thread that finds a flusher already active just inserts and moves on; the flusher picks its entries up.
* The log is therefore written in ascending order **during** processing. Nothing is sorted afterwards, and nothing waits in a loop.
* The last consumer to finish closes the log file and frees the shared state.

---

## Build and run

Requirements: a C compiler, `make`, `pthread`; Python 3 for the checker.

```bash
cd src
make                       # builds ./firewall (parallel) and ./serial (reference)

./firewall <input-file> <output-file> <num-consumers 1-32>
./serial   <input-file> <output-file>
```

The output format is one line per packet: `PASS|DROP <hash as 16 hex digits> <timestamp>`.

## Tests

```bash
cd tests
make check                 # generates inputs + reference outputs, runs all cases
```

The checker (from the course) runs the firewall on 10 to 20,000 packets with 1, 2, 4 and 8 consumer threads. It compares the log with the serial reference, verifies that the log **grows while the program is running** (so it was not sorted at the end), and requires at least a **20% speedup** each time the thread count doubles.

Result on my machine (Apple Silicon, 4 performance + 4 efficiency cores): **90/100** in 7 of 9 runs, which is the maximum for correctness; the remaining 10 points are for code style, which I did not run through `checkpatch`/`cpplint`. In the other 2 runs a single 1,000-packet test scores 85/100 because the 4-thread speedup lands just under the 20% threshold; that is timing noise on a laptop with heterogeneous cores. Output correctness never failed.

Measured on 20,000 packets (best of 7):

| Threads | Time |
|---|---|
| serial | 291 ms |
| 1 | 294 ms |
| 2 | 159 ms |
| 4 | 127 ms |
| 8 | 92 ms |

The output is byte-identical to the serial implementation for 1, 2, 4, 8 and 32 consumers, and ThreadSanitizer reports no data races:

```bash
cd src
clang -fsanitize=thread -g -O1 -I../utils -o fw_tsan \
    ring_buffer.c producer.c consumer.c packet.c firewall.c ../utils/log/log.c -lpthread
./fw_tsan ../tests/in/test_10_000.in out.log 8
```

(Run `make check` once first so that `tests/in` exists.)

---

## Project structure

```
src/
├── firewall.c        # main: starts consumers, runs the producer, joins
├── producer.c        # reads packets from the input file into the ring buffer
├── consumer.c        # consumer threads + min-heap + ordered log writer
├── ring_buffer.c/.h  # bounded circular buffer (mutex + condition variables)
├── packet.c/.h       # packet format, hash and filtering rules
├── serial.c          # single-threaded reference implementation
└── Makefile
tests/                # course checker, packet generator, Makefile
utils/                # logging helpers (rxi/log.c) and DIE/ERR macros
```

## Credits

This was a course assignment ("Parallel Firewall", Operating Systems, POLITEHNICA University of Bucharest). The assignment skeleton, `packet.c`, `producer.c`, `serial.c`, and the checker come from the course materials (BSD-3-Clause). The ring buffer, consumers, min-heap and ordered logging are my implementation. See [LICENSE](LICENSE).
