# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Summary]

StormByte Buffer is the byte-buffer module of the StormByte C++ suite.

It depends on [StormByte Logger](https://github.com/StormBytePP/StormByte-Logger) and [StormByte System](https://github.com/StormBytePP/StormByte-System), which bring Base and String. This repository is not Base, Config, Crypto, Database, Logger, Multimedia, Network or System.

Public headers under `StormByte/buffer/` cover FIFO, SharedFIFO, Ring, Producer/Consumer, Hopper, Sink, Bridge, Pipeline and `StormByte::Buffer::IO` (buffered binary sources and sinks). Octet payloads are `StormByte::BinaryData`. Byte lengths are `StormByte::ByteSize`. `Hopper` and `Sink` count items with `StormByte::Size`.

If you landed here from a release link and have not read the tree:

- What this module is, how to build it, and short examples: [README.md](https://github.com/StormBytePP/StormByte-Buffer/blob/master/README.md)
- License: GNU Lesser General Public License version 3 or later, [LICENSE](https://github.com/StormBytePP/StormByte-Buffer/blob/master/LICENSE)

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

[Unreleased]: https://github.com/StormBytePP/StormByte-Buffer/compare/2.0.0...HEAD

## [2.0.0] - 2026-09-30

### Added

- `option(BUILD_SHARED_LIBS "Build shared libraries" ON)` in the project root. Shared is the default so a consumer can redistribute without triggering LGPL static-link obligations. Static is opt-in (`-DBUILD_SHARED_LIBS=OFF`). CI passes `-DBUILD_SHARED_LIBS=ON`. Third-party StormByte pins pass `ENABLE_TEST=OFF`.
- Nested `Parameters` on `BufferedReader`, `BufferedLocationReader`, `BufferedFileReader`, `BufferedWriter`, `BufferedLocationWriter` and `BufferedFileWriter`. Each level declares its own type (leaves inherit the parent and add nothing). Knobs are optional: omitted means the previous default; all device knobs omitted means `Setup()` probes. Brace-init and a named `Parameters` object are both valid. Variadic knobs resolve in the caller under `STORMBYTE_FORCE_INLINE`; the DLL sees only numbers and a probe flag.
- Knob types `ReadAhead`, `MaxMemory`, `MaxWait`, `WriteChunk`, `BackPressure` in `parameters.hxx`.
- `BufferedReader` page cache. Consumed bytes can stay in RAM up to `MaxMemory`. CollectGarbage evicts farthest from `Tell`. Readahead and the page map work together; a later `Seek` into a live page is served from cache.
- Reader logical seek. `Seek` updates `Tell` immediately. If the target is already cached, the origin is not moved. When the next `Read` runs off the cached range, one real `OriginSeek` resumes prefetch. Documented on the public reader: `Tell` never lies.
- `BufferedWriter` `MaxMemory` and a dirty page map. Writes are lazy until `MaxMemory`, `Flush` or `Close`. Typical case is a nearby backward correction plus continue-at-Tell. Far-future islands are supported while RAM lasts; eviction prefers the oldest dirty page behind the origin cursor (a real write, possibly with a real seek).
- Writer logical seek. Same idea as the reader: `Seek` is logical. A patch that lands on a dirty page does not touch the device. Materializing a page (evict / flush / close) is when the origin moves.
- Layered telemetry in `StormByte::Buffer` (`telemetry.hxx`) and `StormByte::Buffer::IO` (`io/telemetry.hxx`). Base `ReadTelemetry` / `WriteTelemetry` hold delivered/written bytes and `MeanRate`. IO types add cache/origin/seek/wait counters. `MeanRate` is the caller-visible effective rate (`ByteSize/s`), including cache hits; it is not device throughput. `DeltaOperation` updates rate without a mutex.
- `BufferedReader::Telemetry()` / `BufferedWriter::Telemetry()` return `const StormByte::Shared<…>`. Same object for the life of the office; the handle cannot be reseated. A leaf may widen the dynamic type via `CreateTelemetry()`. Accumulators do not reset on Close. Writer public `Flush` (and the Flush inside `Close`) counts toward `MeanRate`; internal worker / GC drains do not.
- Flatten: `operator StormByte::String::String` (out of line) and `STORMBYTE_FORCE_INLINE operator std::string()` so `Logger << *telemetry` stays on the caller TU.
- `BufferedReader::Available()`. Contiguous cached bytes at `Tell`. Does not call `OriginPull` / `OriginSeek` and does not wait for prefetch.
- `StormByte::Buffer::Pumper`. Takes a `Bridge` by move and runs `Passthrough` on a worker until EoF or failure. Starts in the constructor; the destructor joins. Nested `Parameters` with knobs `Chunk` and `HighWater`. `Chunk` `0` is automatic cycle size, not Bridge “current contents”. `HighWater` applies to the **input** only: omitted = `0` if the source is IO, otherwise the backend default (constexpr in the PIMPL `.cxx`); explicit `0` = no Pumper cap (intended when the IO source already limits itself). Non-IO sources are unbounded by design. `Toggle` pauses/resumes. `Cancel` is terminal (`Failed`, no restart). Telemetry is forwarded from the owned Bridge.
- `StormByte::Buffer::Pipe` (`pipe.hxx` / `pipe.cxx`). Abstract stream stage (`Clonable` + `Unique<Pipe>`). Copyable and movable (special members out of line). `Run(ReadOnly&, WriteOnly&, const Shared<Logger::Log>&)`. `Clone` and `Move` are public and must be implemented by the leaf with `Unique::MakePointer` in the leaf TU.

### Changed

- **Breaking:** `StormByte::Buffer::Data` is gone. Octet payloads are `StormByte::BinaryData` from Base. `data.hxx` / `data.cxx` and `DataTests` are removed.
- **Breaking:** byte counts are `StormByte::ByteSize` (`FIFO`, `Ring`, `SharedFIFO`, `Producer` / `Consumer`, `Bridge`, `Pipeline`, `IO`). `Hopper<T>` and `Sink<T>` count items with `StormByte::Size` (`Capacity`, `Size`, `Buckets`, `Select`).
- **Breaking:** `AvailableBytes()` is `Available()`. The return type is already `StormByte::ByteSize`.
- **Breaking:** `ExternalReader`, `ExternalWriter`, `ExternalBufferReader` and `ExternalBufferWriter` are removed. `Bridge` borrows non-IO `ReadOnly` / `WriteOnly` tips directly; those buffers must outlive it. IO leaves remain owned by move.
- **Breaking:** `Pipeline::PipeFunction` (`std::function` over External reader/writer types) is gone. A stage is a user leaf of `Pipe`. `Pipeline::Add(const Pipe&)` clones onto Base's heap and does not touch the caller; `Pipeline::Add(Pipe&&)` takes `Move()`. There is no boxing of callables and no public `Unique<Pipe>` add. `Pipeline` is stream buffers only (`ReadOnly` / `WriteOnly`); IO joins through `Bridge` / `Pumper`. `Process(Consumer, Shared<Logger::Log>, ExecutionMode)` — mode last.
- **Breaking:** `BufferedLocationReader` and `BufferedLocationWriter` sit between the engines and the file leaves. A location is file-like: named by `Location()` (`StormByte::String::String`, owned in this module), always seekable and sized. Path-only `Setup()` lives here.
  - `Device()` and the pure `OriginDevice()` return `StormByte::Shared<StormByte::System::Device>` instead of `System::Device` by value. A leaf may now hand out a `System::Device` subclass (for example a NIC device whose accessor is not a filesystem path) and the dynamic type survives, so overridden `Throughput()` / `Window()` are honoured and the caller can keep the object alive. Build the owner with `StormByte::Shared<StormByte::System::Device>::MakePointer<Leaf>(…)` so the object lives on Base's heap and crosses the DLL boundary safely.
  - New protected `virtual bool OriginDeviceUsable(const StormByte::Shared<StormByte::System::Device>&) const noexcept`. The default is the previous behaviour (non-empty owner and `operator bool()` true, which probes the stored path). A leaf whose identifier is not a filesystem path overrides it and never reaches the non-virtual path probe. An empty owner is always unusable.
  - `Setup()` stays `final`, calls `OriginDevice()` once, asks `OriginDeviceUsable()` and only then applies `Window()` on the dynamic object. When the device is not usable the per-leaf defaults (`ReadAhead`, or `WriteChunk` / `BackPressure` / `MaxMemory`) are kept. The device is never copied or sliced to the base type.
- **Breaking:** `BufferedFileReader` and `BufferedFileWriter` are `final`. `CreateDevice()` is gone. `Path()` (`const String&`) and `Location()` (`IO::Location`) are set on `BufferedReader` / `BufferedWriter` and do not change. A file leaf passes `Location::Local`. A socket on the lower layer can pass `Location::Remote`. The file leaves keep the plain `System::Device` and the real path probe, so their windows and defaults are unchanged.
- **Breaking:** IO constructors no longer take positional windows (`read_ahead`, `max_memory`, `write_chunk`, `back_pressure`, `max_wait`). One constructor per leaf: path plus that class’s `Parameters` (default `{}` = probe). Explicit zeros stay zeros; they do not probe.
- **Breaking:** `Buffer::Exception` uses `Exception::Path{"Buffer"}`. `what()` is `StormByte.Buffer: message`. `ReadError` is `StormByte.Buffer.Read`. `WriteError` is `StormByte.Buffer.Write`. `Component` is gone. Destructors are defined in this module.
- **Breaking:** `Bridge` is a manual transfer again, not a worker. Public `Passthrough(ByteSize, Operation)` is the only transfer; `Operation::{Blocking, NonBlocking}` applies to the **read** tip; write `TryAgain` is retried until that call completes. `n == 0` is current contents (`Available()`). Non-IO tips are `ReadOnly&` / `WriteOnly&`. IO tips are stolen by move as the concrete leaf. `Failed()` is sticky. Continuous pumping is `Pumper`.
- Reader `Seek` is no longer “always `OriginSeek`”. A cache hit is O(1) on the origin. A miss still costs a real seek plus whatever the device does.
- Writer `Seek` exists and is part of the public contract. It is not guaranteed O(1) when the target is not in the dirty map or when eviction must drain pages first.
- Writer contract: lazy write up to `MaxMemory`. More random access needs more `MaxMemory` or islands get evicted (a real write + seek).
- Nested `BufferedReader::Telemetry` / `BufferedWriter::Telemetry` structs are gone. Counters live on the Shared objects; getters, not public fields.
- `LockFreeRing::FrontSpan` returns a snapshot copied under the wait mutex so a concurrent `Grow` cannot invalidate the pointer the drain worker is pushing.
- Origin I/O on the writer (`OriginSeek` / `OriginPush` / `OriginFlush` / `OriginOpen` / `OriginClose` / `OriginTruncate`) is serialized against the drain worker. `Flush` waits until the ring is empty **and** the worker has published the origin cursor (`!m_drain_run`).
- Dual license layout: `LICENSE` is the short header text; `COPYING.LGPLv3` is the LGPL text.

### Fixed

- Writer drain vs `Grow`: `FrontSpan` no longer aliases `m_storage` while the producer reallocates (Mac `patev-ring-only` corruption).
- Writer `Flush` returning before `m_origin_pos` was stored, which let the next `EnsureOrigin` land a patch on the wrong offset.
- Concurrent `FILE*` / `ofstream` use from the writer thread and the drain worker.
- Origin cursor after `OriginFlush` treated as untrusted until the next `EnsureOrigin` (Darwin). Sequential drain after that first realign does not seek again.
- `LockFreeRing` `Close`, `SetError`, `Clean`, `Drop` and `Consume` publish under the wait mutex. A parallel pipeline stage waiting on an intermediate ring could miss the wake and leave `Process` spinning on `IsWritable()`.
- Doxygen: broken `\ref` on the public reader header; private storage types not listed as public API.
- Missing virtual destructors on `BufferedLocationReader` / `BufferedLocationWriter`. Leaves stay `final` and do not declare `virtual` on the destructor.

### Tests

- `BufferedFileReaderTests` / `BufferedFileWriterTests` / `BridgeTests` construct IO with `Parameters` / knobs (`ReadAhead`, `MaxMemory`, `WriteChunk`, `BackPressure`). Path-only still probes.
- Predictable hex fixture, integrity of every `Read` after logical and cold seeks, `Tell` during a logical seek, telemetry prints via `*Telemetry()`.
- Writer close/flush integrity on hex files, holes, far islands, eviction + patch, ring-only / pages / direct knobs.

[2.0.0]: https://github.com/StormBytePP/StormByte-Buffer/compare/1.4.0...2.0.0

## [1.4.0] - 2026-09-23

### Added

- `StormByte::Buffer::IO`. Buffered binary sources and sinks, separate from FIFO / Ring / Hopper. Public surface: `Status`, `State`, `Result`, `ToString`, `BufferedReader`, `BufferedWriter`, `BufferedFileReader`, `BufferedFileWriter`. `IO::Backend` is the PIMPL and is not a public include.
- `Status` / `Result` / `State`. `Ok`, `End`, `Error`, `Failed`, `TryAgain` plus a byte `count`. `TryAgain` is backpressure or a bounded `MaxWait`. `State`: `Idle`, `Missing`, `Directory`, `Permission`, `NotWritable`, `Fault`, `Unavailable`. `constexpr ToString` for `Status` and `State`.
- `BufferedReader`. Public base for a binary origin. Leaves implement `OriginOpen`, `OriginClose`, `OriginPull`, `OriginCanSeek`, `OriginSeek`, `OriginHasSize` and `OriginSize`. Optional `Setup()` runs once from `Open` before `OriginOpen`. Construction is `Unavailable`; a successful `Open` is `Idle`. `Close` is idempotent; `Open` is not. `operator bool` is Idle and not `EoF`.
- Reader `Read` / `Peek` into a `FIFO` or a writable `std::span<std::byte>`. Destination overwritten only on `Ok` / `End` with a non-zero count. Empty span is `{Ok, 0}`. FIFO `n == 0` serves the cached span at `Tell`. `MaxWait` `0ms` waits without limit.
- Reader `Seek` / `Tell` / `IsSeekable` / `IsSized` / `Size`. Absolute or relative only. End-relative is `Seek(*Size() + off, Absolute)` when sized. Seekable `Seek` always calls `OriginSeek`, including a cache hit. Non-seekable `Seek` is `Failed` and does not call the hook. Seek is not O(1). Cache is a map of owned spans; overlap merges; `MaxMemory` evicts farthest from `Tell`; `0` stores nothing and still serves from the origin. Prefetch stops on `Seek` and on move (`Rebind`); the next `Read` / `Peek` requests it again.
- `BufferedFileReader`. `ifstream` leaf, seekable and sized. Path-only constructor probes device throughput at `Setup` and sets `ReadAhead` (window clamped 16 KiB–1 MiB) with `MaxMemory` 1 MiB. Explicit `(path, read_ahead, max_memory)` keeps those knobs. Does not open in the constructor.
- `BufferedWriter`. Public base for a binary sink. Leaves implement `OriginOpen`, `OriginClose`, `OriginPush`, `OriginFlush` and `OriginTruncate`. Optional `Setup()` and `WillWrite`. No `Seek`. `Tell` is bytes accepted since `Open` or `Truncate`. `Close` flushes then closes; a flush failure is `Fault`.
- Writer `Write(const FIFO&)`, `Write(FIFO&)` and `Write(std::span<const std::byte>)`. Atomic. `WriteChunk` and `BackPressure` (in chunks): either knob `0` is direct; both `> 0` use an SPSC `LockFreeRing` capped at `BackPressure * WriteChunk` bytes. Overflow is `TryAgain`. `Dirty()` is unread ring bytes. `Flush()` drains the ring and calls `OriginFlush`.
- `BufferedFileWriter`. `ofstream` leaf, binary append. Creates the file when the parent exists (no `mkdir -p`). Path-only constructor probes the device at `Setup` and sets `WriteChunk` plus `BackPressure` 4. Explicit `(path, write_chunk, backpressure)` keeps those knobs. `Truncate` overwrites.
- Device throughput probe (private): Linux / Windows / macOS classification (HDD, SATA SSD, NVMe gen, USB, network at 80 % of NIC). Nominal rates, not a benchmark. Device knobs have no setters; `MaxMemory` and `MaxWait` stay settable.
- `LockFreeRing::FrontSpan`, `Consume` and `Write(std::span<const std::byte>)`.
- `ExternalWriter::Occupied`.
- `Bridge` pumps any `ExternalReader` / `IO` reader into any `ExternalWriter` / `IO` writer. `Drain` respects sink backpressure. Worker auto-drains; public `Passthrough` is gone. `high_water == 0` means no extra occupancy cap. The worker starts, including when `high_water` is 0. Pause is only `Drainer(Toggle)`.
- Two-argument `Bridge` constructors for `BufferedWriter` sinks:
  `Bridge(const IO::BufferedReader&, IO::BufferedWriter&)` and
  `Bridge(ExternalReader&, IO::BufferedWriter&)`. No occupancy cap at
  the Bridge layer. Same pump path as `high_water == 0`. Intended for
  `BufferedFileWriter` (WriteChunk / BackPressure already cap Dirty).
  Pairings into FIFO / SharedFIFO / Ring / Producer keep the
  three-argument constructor.

### Removed

- `Sink::Bind` and `Sink::Bind(int, Sink&)`. Wire with `To(key)` / `>>` / `<<`.

### Tests

- `BufferedFileReaderTests`. Fixtures under `test/files/`. Span `Read` / `Peek`, `Tell`, `Seek` (absolute, relative, end via `Size`, cache hit, `MaxMemory` 0), path-only vs explicit constructors, move with prefetch stopped.
- `BufferedFileWriterTests`. Temp files via `StormByte::System::TempFileName`. Direct `(path, 0, 0)`, path-only device knobs, Dirty / Flush / BackPressure / Truncate / move.
- `BufferedMeteredFileTests`. Selective override example (`BytesRead` / `BytesWritten`).
- Bridge coverage for pipe close-while-started, `high_water` 0, and the two-argument writer ctors (`test_io_uncapped_ctor`, `test_buf_to_io_uncapped_ctor`).

[1.4.0]: https://github.com/StormBytePP/StormByte-Buffer/compare/1.3.0...1.4.0

## [1.3.0] - 2026-09-20

### Added

- `Sink::Keys`, `Sink::Buckets`, `Sink::Contains`, `Sink::Empty(key)`, `Sink::EoF(key)` and `Sink::Ready(key)`. Query only. Missing key: `Empty` is true, `EoF`/`Ready`/`Contains` are false, `Buckets` is the wired count.
- `Sink::Pop(int key)`. Reads that hopper only. Waits until the key is wired or the Sink is closed. Empty hopper returns default `T` (same as `Hopper::Pop`). Does not interpret the key.
- `Hopper::Writers`, `Hopper::Ready` and `Hopper::Front`. `Front` copies the next item and does not dequeue; requires `std::copy_constructible<T>` (`shared_ptr`). Not a deep copy of the payload. `Writers` is the live writer count (starts at 1).

### Tests

- `test_hopper_front_peek`, `test_hopper_writers_and_ready`.
- `test_sink_pop_key`, `test_sink_query_unwired`, `test_sink_query_wired`.
- Hopper and Sink test files ordered by section name, then by test name.

[1.3.0]: https://github.com/StormBytePP/StormByte-Buffer/compare/1.2.0...1.3.0

## [1.2.0] - 2026-09-17

### Added

- `Hopper::Unnotify` and `Sink::Unnotify`. `Notify(cv&)` does not own the
  condition variable. After wiring, the Hopper outlives the consumer; the
  consumer must `Unnotify` before that CV is destroyed so a later producer
  `Eof` does not signal a freed object. `SignalConsumer` is a no-op when
  the pointer is null.
- `Sink::To(key)`, `Sink::operator>>` and `Sink::operator<<`. Same wiring
  as `Bind` / `Bind(key)` (writer, reader, co-writer). `Bind` stays as a
  `[[deprecated]]` wrapper for one or two releases.
- `Hopper::operator<<` / `Hopper::operator>>` and `item >> hopper`. Same
  as `Push` / `Pop`. Those methods stay.
- `Pipeline::Process` scopes a non-null logger with `Scope("Buffer/Pipeline")`
  before handing it to stages. `%c` identifies this module without using the
  thread-local component stack. Nested `log->Scope("Decode")` inside a stage
  becomes `Buffer/Pipeline/Decode` (or `Multimedia/Buffer/Pipeline/Decode`
  if the caller already scoped a parent). Pass the application or parent-module
  logger; do not pre-scope `Buffer/Pipeline`.

### Changed

- Optional Logger pin is [1.2.0](https://github.com/StormBytePP/StormByte-Logger/releases/tag/1.2.0)
  (`Scope` and hierarchical components).

### Tests

- `test_hopper_unnotify_before_cv_dies`, `test_hopper_notify_after_unnotify`,
  `test_hopper_stream_members`, `test_hopper_stream_item_into`.
- `test_sink_unnotify_before_cv_dies`, `test_sink_stream_operators`.
  Existing Sink tests use `To` / `>>` / `<<` instead of `Bind`.

[1.2.0]: https://github.com/StormBytePP/StormByte-Buffer/compare/1.1.2...1.2.0

## [1.1.2] - 2026-09-16

### Deprecated

- `Sink::Bind` (both overloads). See Unreleased TODO.

### Fixed

- `Sink::Bind(key)` of a hopper this Sink already holds attaches the other Sink as a co-writer. `Sink::Eof` then closes that hopper only when the last writer closes, so extra producers can still `Push`. No new public methods.
- `Sink::Bind(key)` does not add a writer if the other Sink already writes that hopper. A second Bind of the same producer no longer leaves the hopper open after one `Eof`. `test_sink_rebind_same_writer_eof` waits up to 1s for that EoF (the remuxer hang).

[1.1.2]: https://github.com/StormBytePP/StormByte-Buffer/compare/1.1.1...1.1.2

## [1.1.1] - 2026-09-15

### Changed

- Bundled StormByte Logger is [1.1.1](https://github.com/StormBytePP/StormByte-Logger/releases/tag/1.1.1) (and transitively [StormByte Base 1.1.1](https://github.com/StormBytePP/StormByte/releases/tag/1.1.1)). The declared requirement stays Logger [1.1.0](https://github.com/StormBytePP/StormByte-Logger/releases/tag/1.1.0) or newer.

[1.1.1]: https://github.com/StormBytePP/StormByte-Buffer/compare/1.1.0...1.1.1

## [1.1.0] - 2026-09-13

### Added

- `Hopper<T>` — single-producer single-consumer (SPSC) typed queue (`Type::MoveConstructible`) with optional capacity ceiling, non-blocking `Pop`, `Push` wait when full, `Eof` signaling, consumer condition variable notification (`Notify`), and automatic null item discarding for `Type::SmartPointer` types.
- `Sink<T>` — integer key map of `Hopper<T>` buckets supporting `Bind` hopper sharing, terminal producer `Drain` mode, `Ready` check, Round-Robin or custom `Select` index chooser popping, and per-key `Capacity`, `Size`, and `Full` queries.
- Header implementation files `hopper.txx` and `sink.txx` installed alongside public headers (`*.txx` in `cmake/install.cmake`).

### Changed

- Updated dependency requirement to [StormByte Logger 1.1.0](https://github.com/StormBytePP/StormByte-Logger/releases/tag/1.1.0) (and transitively [StormByte Base 1.1.0](https://github.com/StormBytePP/StormByte/releases/tag/1.1.0)).
- Routed range and iterator byte APIs through StormByte Base type concepts (`Type::ByteInputRange`, `Type::ByteInputIterator`, `Type::SentinelFor`, `Type::SameAs`) instead of local standard-library constraints.
- Limited `Hopper<T>` and `Sink<T>` null-item filtering to `Type::NullablePointer` so only pointer-like values that can be tested for emptiness use the `!item` path.
- Documented `Sink::EoF()` contract: with hoppers, evaluates `true` when all hoppers are empty and `Hopper::EoF()` is `true`, even if this `Sink` itself did not call `Eof()`; binding a new key after `EoF()` returned `true` may return `EoF()` to `false`.

### Fixed

- Ported Buffer exceptions to `StormByte::Component`, preventing format-string overload ambiguity and correctly prefixing `ReadError` and `WriteError` messages.
- Moved virtual override definitions and destructors to compiled translation units so shared-library builds export stable vtables, RTTI and non-virtual thunks for GCC consumers, with polymorphic consumer coverage in each owning class test.
- Closed `Sink` and marked hoppers `Eof` atomically under `m_mutex` in `Sink::Eof` and checked closed state and `consumer.m_consumer` under lock in `Sink::Bind` to prevent concurrent `Bind` calls from creating un-marked hoppers or skipping consumer `Notify`.
- Marked `Sink` closed in destructor to safely unblock threads waiting in `Push` and `Pop`.

[1.1.0]: https://github.com/StormBytePP/StormByte-Buffer/releases/tag/1.0.0..1.1.0

## [1.0.0] - 2026-09-05

Initial public release of StormByte Buffer.

### Added

- Hierarchical interfaces: `Generic`, `ReadOnly`, `WriteOnly`, `ReadWrite`
- `FIFO` — grow-on-demand byte buffer (single-threaded)
- `SharedFIFO` — thread-safe FIFO with blocking reads/extracts
- `Ring` — concurrent ring (`shared_mutex`, many-to-many)
- `Producer` / `Consumer` — write/read handles over `Ring`
- `ExternalReader` / `ExternalWriter` — I/O adapters
- `Bridge` — chunked passthrough with optional flush-on-destroy
- `Pipeline` with `ExecutionMode`: `Sync`, `Async`, `Parallel` (combinable)
- Private `LockFreeRing` — SPSC intermediates between pipeline stages
- Lifecycle: `Close()`, `SetError()`, `EoF()`, `IsReadable()`, `IsWritable()`
- Non-destructive `Read` / `Peek` and destructive `Extract`, plus `*UntilEoF`
- Seek, Drop, Clean, Clear, HexDump
- Unit tests (FIFO, SharedFIFO, Ring, Producer/Consumer, Bridge, Pipeline)
- Project version read from the `VERSION` file
- CMake 3.28 floor

### Notes

- `FIFO` is not thread-safe; use `SharedFIFO` or `Ring` for concurrent access.
- `LockFreeRing` is private and only safe under single-producer / single-consumer use.
- Pipeline stages must `out.Close()` or `out.SetError()` when finished.
- Needs a C++26 compiler and StormByte Base ≥ 1.0.0.

[1.0.0]: https://github.com/StormBytePP/StormByte-Buffer/releases/tag/1.0.0
