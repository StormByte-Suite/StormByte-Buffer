# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Summary]

StormByte Buffer is the byte-buffer module of the StormByte C++ suite.

It depends on [StormByte Logger](https://github.com/StormByte-Suite/StormByte-Logger) and [StormByte System](https://github.com/StormByte-Suite/StormByte-System), which bring Base. This repository is not Base, Config, Crypto, Database, Logger, Multimedia, Network or System.

Public headers under `StormByte/buffer/` cover FIFO, SharedFIFO, Ring, Producer/Consumer, Hopper, Sink, Bridge, Pipeline and `StormByte::Buffer::IO` (buffered binary sources and sinks). Octet payloads are `StormByte::Safe::Binary`. Byte lengths are `StormByte::ByteSize`. `Hopper` and `Sink` count items with `StormByte::Size`.

If you landed here from a release link and have not read the tree:

- What this module is, how to build it, and short examples: [README.md](https://github.com/StormByte-Suite/StormByte-Buffer/blob/master/README.md)
- License: GNU Lesser General Public License version 3 or later, [LICENSE](https://github.com/StormByte-Suite/StormByte-Buffer/blob/master/LICENSE)

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

[Unreleased]: https://github.com/StormByte-Suite/StormByte-Buffer/compare/2.0.0...HEAD

Reviso el changelog en `a2e7da4` contra lo que de verdad cambió. La fecha, si hay que tocarla, es 2026-10-08.

El summary no miente. El 2.0.0 sí: la fecha es de ayer, y sigue diciendo que la espera de `Sink` es `std::mutex`, `std::condition_variable` y `std::atomic` hasta que Base tenga primitiva. Eso ya no es cierto. Tampoco lo es el `Notify` del bloque Fixed.

## [2.0.0] - 2026-10-10

### Added

- `option(BUILD_SHARED_LIBS "Build shared libraries" ON)` in the project root. Shared is the default so a consumer can redistribute without triggering LGPL static-link obligations. Static is opt-in (`-DBUILD_SHARED_LIBS=OFF`). CI passes `-DBUILD_SHARED_LIBS=ON`. Third-party StormByte pins pass `ENABLE_TEST=OFF`.
- Nested `Parameters` on all buffered reader/writer levels, with knobs `ReadAhead`, `MaxMemory`, `MaxWait`, `WriteChunk` and `BackPressure`. Omitted knobs retain the previous defaults; omitting all device knobs makes `Setup()` probe. Brace-init and named `Parameters` are supported. Header-only bags use Safe-owned optional values; IO constructors resolve numbers, durations and probe flags. Pure knob and bag functions use ordinary inline definitions rather than `STORMBYTE_FORCE_INLINE`.
- `BufferedReader` page cache and logical seek. Consumed bytes remain in RAM up to `MaxMemory`; garbage collection evicts farthest from `Tell`. `Seek` updates `Tell` immediately, avoids moving the origin on a cache hit, and resumes prefetch with one `OriginSeek` when a later read leaves the cached range. `Tell` never lies.
- `BufferedWriter` dirty-page cache and logical seek. Writes are lazy until `MaxMemory`, `Flush` or `Close`; nearby corrections and far-future islands are supported while memory allows. Eviction prefers the oldest dirty page behind the origin cursor. `Seek` is logical; materializing a page (eviction, flush or close) moves the origin.
- Layered telemetry in `StormByte::Buffer` and `StormByte::Buffer::IO`. Read/write counters hold delivered/accepted bytes; IO telemetry adds cache/origin/seek/wait counters. `MeanRate` is the caller-visible effective rate (`ByteSize/s`), including cache hits, not device throughput. Telemetry handles are `const StormByte::Safe::Shared<…>`, stable for the life of the office; accumulators do not reset on close. Public writer `Flush` and the flush in `Close` count toward the rate; internal worker/GC drains do not. Flattening provides `operator StormByte::Safe::String` out of line and caller-side `STORMBYTE_FORCE_INLINE operator std::string()`. Writer `Cap` is the ring cap (`WriteChunk * BackPressure`, or 0 if the ring is off), published with the dirty snapshot. It is not `MaxMemory`.
- `BufferedReader::Available()`. Contiguous cached bytes at `Tell`. Does not call `OriginPull` / `OriginSeek` and does not wait for prefetch.
- `StormByte::Buffer::Pumper`. Takes a `Bridge` by move and runs `Passthrough` on a worker until EoF or failure. Starts in the constructor; the destructor joins. Nested `Parameters` with knobs `Chunk` and `HighWater`. `Chunk` `0` is automatic cycle size, not Bridge “current contents”. `HighWater` applies to the **input** only: omitted = `0` if the source is IO, otherwise the backend default (constexpr in the PIMPL `.cxx`); explicit `0` = no Pumper cap (intended when the IO source already limits itself). Non-IO sources are unbounded by design. `Toggle` pauses/resumes. `Cancel` is terminal (`Failed`, no restart). Telemetry is forwarded from the owned Bridge.
- `StormByte::Buffer::Pipe` (`pipe.hxx` / `pipe.cxx`). Nonpolymorphic, copyable callable stage owning `Safe::Function` with creator-context invoke, clone and release callbacks. `PipeInput` and `PipeOutput` are explicit synchronous endpoint borrows whose const functions forward stream operations; neither facades nor their copies may be retained after invocation. Text writes use length-aware views, including Base `Safe::String` conversions, without CString/WCString adapters. Callable state is allocated with `Safe::Heap::Allocate` and released by the creator callback. It is not a `std::unique_ptr`.

### Changed

- Doxygen external tagfiles now use HTTPS for downloads and cross-reference mappings; fix invalid references and comments reported during generation.
- **Breaking:** Port Buffer to StormByte Base 2.0.0. Owned text is now `StormByte::Safe::String` / `Safe::WString`; octet payloads are `StormByte::Safe::Binary`; pointer and clonable APIs use `StormByte::Safe`; Buffer telemetry derives from Base `StormByte::Telemetry` and measures operations with independent clock samples; Buffer exceptions accept Base-owned `Safe::String` messages.
- **Breaking:** Complete the DLL-boundary lifecycle contract for buffers, typed queues and worker handles:
  - `Producer` and `Consumer` keep the shared `Ring` behind `StormByte::Safe::Owner`; `Producer(std::shared_ptr<Ring>)` is removed. Copies share the Ring, whose retain/release callbacks remain in Buffer. Ring constructors, move operations and destruction keep private deque and mutex/condition-variable lifecycle in the provider; SharedFIFO construction and destruction of synchronization state are out of line. Caller-side range conversion delegates through Base-owned `Safe::Binary` rather than constructing provider storage in the caller.
  - `FIFO::HexDumpHeader()` and `Ring::HexDumpHeader()` return `StormByte::Safe::String` instead of `std::ostringstream`; custom overrides must update their return type. Formatting streams stay local to their implementation.
  - `BufferedReader::Size()` and virtual `OriginSize() const noexcept` return `StormByte::Safe::Optional<StormByte::ByteSize>`. Empty means unknown/unavailable, not a known zero-byte origin. File readers preserve an engaged zero; custom readers, including downstream Network overrides, must adopt the new signature in their owning repository.
  - IO parameter bags use `Safe::Optional<ByteSize>` for byte counts and `Safe::Optional<std::size_t>` for backpressure. Bag `MaxWait()` returns `const Safe::Optional<std::chrono::milliseconds::rep>&`; callers construct `std::chrono::milliseconds` from the signed count. Duration knobs and live IO getters remain unchanged. Absence, explicit zero, negative counts and the full millisecond representation range are preserved. IO and Pumper bag construction, copying and knob assignment may allocate and throw; copy operations are not `noexcept`, while moves and destruction remain nonthrowing. Safe callbacks retain storage lifecycle in its provider; unrelated private STL optionals remain private.
  - Pumper `Parameters::Chunk()` / `HighWater()` and its provider constructor use `Safe::Optional<ByteSize>` instead of `std::optional`. Omitted settings remain distinct from explicit zero: omitted/zero Chunk is automatic; omitted HighWater uses the input-kind default and explicit zero disables the cap.
  - `Sink<T>::Keys()` returns an independent ascending `Safe::Vector<int>` snapshot instead of `std::vector<int>`; copies remain independent after wiring changes or Sink destruction. The Sink coordinator is `Safe::Owner`. Wired hoppers are `Safe::Shared`. Buckets are `Safe::Map<int, Safe::Shared<Hopper<T>>>`, writers are `Safe::Set<Safe::Shared<Hopper<T>>>`, and the round-robin order is `Safe::Vector`. Hopper queue storage stays on Base's heap. Snapshot creator modules must remain loaded until release. The wait path is `Safe::Mutex`, `Safe::ConditionVariable` and `Safe::Atomic<std::size_t>`. A borrowed consumer wait is the same Safe pair. No public header exports `std::mutex`, `std::condition_variable`, `std::atomic` or `std::thread`.
  - `Hopper<T>` and `Sink<T>` require unqualified `Type::SafeValue` elements with nonthrowing default construction, move construction, move assignment and destruction, and `alignof(T) <= alignof(std::max_align_t)`. `Front` additionally requires nonthrowing copying. Arbitrary movable values, standard strings/containers/smart pointers and raw pointers are no longer admitted merely for being movable. Extended alignment is rejected at compile time; there is no standard-allocator fallback or shared-runtime exception.
  - `Sink<T>::Select` is `Safe::Function<Size(Size)>`, borrowed by `Pop(const Select&)` synchronously and never retained. Provider callbacks return `Safe::Status` and write their selected index through an output pointer. Only `Success` permits dequeue; missing context, failure or exceptions return default `T` without consuming items. Provider-owned captures are released by their provider, which must remain loaded through callback destruction. Caller-side callable adapters borrow the original mutable or move-only callable for that call only.
  - Exact-type `MaybeSafe` declarations cover the generic read/write interfaces, Producer/Consumer, Bridge/Pumper, Pipe/Pipeline, buffered IO bases and file leaves, telemetry and operation samples, IO Result/knobs and Pumper Parameters. `Hopper<T>` and `Sink<T>` are partial specializations of `IsMaybeSafe`, not the exact-type macro: the macro does not accept a class template. They are not unconditional `IsSafe` certification, are not inherited by derived types, and do not certify callback providers or arbitrary payloads. Buffer, Base, Logger where used, and every concrete/template/element/callback provider must remain loaded with compatible C++/STL ABI until release. Inline template callbacks are not automatically provider-local.
- **Breaking:** `StormByte::Buffer::Data` is gone. Octet payloads are `StormByte::Safe::Binary` from Base. `data.hxx` / `data.cxx` and `DataTests` are removed.
- **Breaking:** byte counts are `StormByte::ByteSize` (`FIFO`, `Ring`, `SharedFIFO`, `Producer` / `Consumer`, `Bridge`, `Pipeline`, `IO`). `Hopper<T>` and `Sink<T>` count items with `StormByte::Size` (`Capacity`, `Size`, `Buckets`, `Select`).
- **Breaking:** `AvailableBytes()` is `Available()`. The return type is already `StormByte::ByteSize`.
- **Breaking:** `ExternalReader`, `ExternalWriter`, `ExternalBufferReader` and `ExternalBufferWriter` are removed. `Bridge` borrows non-IO `ReadOnly` / `WriteOnly` tips directly; those buffers must outlive it. IO leaves remain owned by move.
- **Breaking:** `Pipeline::PipeFunction` and polymorphic `Pipe` inheritance with virtual `Run`/`Clone`/`Move` are removed. Construct `Pipe` from a copyable callable taking `const PipeInput&`, `const PipeOutput&` and `const Safe::Shared<Logger::Log>&`, or explicitly supply a `Pipe::Callback` provider. `Add(const Pipe&)` and Pipeline copies independently clone callable value captures; `Add(Pipe&&)` transfers context. Reference/shared captures still share their referents; move-only captures are rejected. Capture construction, cloning and destruction use creator callbacks and Base heap helpers; providers must remain loaded with a compatible ABI and template provider-locality must be audited. Buffers, threads and private synchronization remain in Buffer. `Pipeline` remains stream-only; IO joins through `Bridge` / `Pumper`, execution modes are unchanged, and `Process(Consumer, Shared<Logger::Log>, ExecutionMode)` keeps mode last.
- **Breaking:** `BufferedLocationReader` and `BufferedLocationWriter` sit between the engines and the file leaves. A location is file-like: named by `Location()` (`StormByte::Safe::String`, owned by Base), always seekable and sized. Path-only `Setup()` lives here.
  - `Device()` and the pure `OriginDevice()` return `StormByte::Safe::Shared<StormByte::System::Device>` instead of `System::Device` by value. A leaf may now hand out a `System::Device` subclass (for example a NIC device whose accessor is not a filesystem path) and the dynamic type survives, so overridden `Throughput()` / `Window()` are honoured and the caller can keep the object alive. Build the owner with `StormByte::Safe::Shared<StormByte::System::Device>::MakePointer<Leaf>(…)` so the object lives on Base's heap and crosses the DLL boundary safely.
  - New protected `virtual bool OriginDeviceUsable(const StormByte::Safe::Shared<StormByte::System::Device>&) const noexcept`. The default is the previous behaviour (non-empty owner and `operator bool()` true, which probes the stored path). A leaf whose identifier is not a filesystem path overrides it and never reaches the non-virtual path probe. An empty owner is always unusable.
  - `Setup()` stays `final`, calls `OriginDevice()` once, asks `OriginDeviceUsable()` and only then applies `Window()` on the dynamic object. When the device is not usable the per-leaf defaults (`ReadAhead`, or `WriteChunk` / `BackPressure` / `MaxMemory`) are kept. The device is never copied or sliced to the base type.
- **Breaking:** `BufferedFileReader` and `BufferedFileWriter` are `final`. `CreateDevice()` is gone. `Path()` (`const String&`) and `Location()` (`IO::Location`) are set on `BufferedReader` / `BufferedWriter` and do not change. A file leaf passes `Location::Local`. A socket on the lower layer can pass `Location::Remote`. The file leaves keep the plain `System::Device` and the real path probe, so their windows and defaults are unchanged.
- **Breaking:** IO constructors no longer take positional windows (`read_ahead`, `max_memory`, `write_chunk`, `back_pressure`, `max_wait`). One constructor per leaf: path plus that class’s `Parameters` (default `{}` = probe). Explicit zeros stay zeros; they do not probe.
- **Breaking:** `Buffer::Exception` uses `Exception::Path{"Buffer"}`. `what()` is `StormByte.Buffer: message`; `ReadError` and `WriteError` use `StormByte.Buffer.Read` and `StormByte.Buffer.Write`. Buffer-specific destructors are defined in this module. The message constructor takes `string_view`. A null `const char*` is not a message.
- **Breaking:** `Bridge` is a manual transfer again, not a worker. Public `Passthrough(ByteSize, Operation)` is the only transfer; `Operation::{Blocking, NonBlocking}` applies to the **read** tip; write `TryAgain` is retried until that call completes. `n == 0` is current contents (`Available()`). Non-IO tips are `ReadOnly&` / `WriteOnly&`. IO tips are stolen by move as the concrete leaf. `Failed()` is sticky. Continuous pumping is `Pumper`.
- Reader `Seek` is no longer “always `OriginSeek`”. A cache hit is O(1) on the origin. A miss still costs a real seek plus whatever the device does.
- Writer `Seek` exists and is part of the public contract. It is not guaranteed O(1) when the target is not in the dirty map or when eviction must drain pages first.
- Writer contract: lazy write up to `MaxMemory`. More random access needs more `MaxMemory` or islands get evicted (a real write + seek).
- Nested `BufferedReader::Telemetry` / `BufferedWriter::Telemetry` structs are gone. Counters live on the Shared objects; getters, not public fields.
- `LockFreeRing::FrontSpan` returns a snapshot copied under the wait mutex so a concurrent `Grow` cannot invalidate the pointer the drain worker is pushing.
- Origin I/O on the writer (`OriginSeek` / `OriginPush` / `OriginFlush` / `OriginOpen` / `OriginClose` / `OriginTruncate`) is serialized against the drain worker. `Flush` waits until the ring is empty **and** the worker has published the origin cursor (`!m_drain_run`).
- Dual license layout: `LICENSE` is the short header text; `COPYING.LGPLv3` is the LGPL text.

### Fixed

- Add opt-in stored notifications through `Hopper<T>::Notify(Safe::ConditionVariable&, Safe::Atomic<std::size_t>&)` and `Sink<T>::Notify(Safe::ConditionVariable&, Safe::Atomic<std::size_t>&)` for consumers whose wait mutex is independent of the queue mutex. Push, EOF, Sink closure (including an empty Sink) and wiring publish generation changes after state updates, preventing a notification between a false predicate check and atomic wait from being lost. Existing CV-only registrations still require external wait-mutex coordination. Registration propagates through current/new buckets, keyed/all wiring and fan-in; replacement and matching-owner removal synchronize both borrowed referents with in-flight notifications. Queue limits, blocking, writer accounting and Safe-owned element lifetimes are unchanged. All template providers and consumers must be rebuilt together because private Hopper/Sink implementation layouts changed; do not mix pre-fix and post-fix instantiations across DLLs.
- Export `Telemetry::OperationSample` from the Buffer DLL so consumers can use its out-of-line move, destruction and commit operations on Windows.
- Buffer operation telemetry no longer builds clock names from owner addresses, thread IDs or nesting depth. Each operation uses Base's independently timed `Clock::Sample`, allowing nested and concurrent measurements without name collisions.
- Exceptions from `Pipe::Run` and `Sink::Select` no longer escape `noexcept` execution paths: pipeline callback `Failure`, moved-from `Missing` and exceptions mark every output errored and wake waiters, while a failed selector returns default `T` without removing queued items. Pipeline copy assignment preserves the destination if callback cloning fails.
- Hopper observer replacement/unregistration synchronizes with in-flight notifications. Sink unregisters only its matching hopper registrations, so teardown does not clear a replacement observer. Borrowed condition variables must remain alive until `Unnotify()` returns; stop and join queue users before destruction. Allocation failure in nonthrowing queue operations terminates rather than weakening the lifetime contract.
- Exceptions from IO origin hooks are converted to `Status::Error`; `Open` reports setup failures as `false`, and `Device()` translates foreign exceptions to `StormByte::Buffer::Exception`. Reader and writer close results now include failures from their origin close hooks.
- Writer drain vs `Grow`: `FrontSpan` no longer aliases `m_storage` while the producer reallocates (Mac `patev-ring-only` corruption).
- Writer `Flush` returning before `m_origin_pos` was stored, which let the next `EnsureOrigin` land a patch on the wrong offset.
- Concurrent `FILE*` / `ofstream` use from the writer thread and the drain worker.
- Origin cursor after `OriginFlush` treated as untrusted until the next `EnsureOrigin` (Darwin). Sequential drain after that first realign does not seek again.
- `LockFreeRing` `Close`, `SetError`, `Clean`, `Drop` and `Consume` publish under the wait mutex. A parallel pipeline stage waiting on an intermediate ring could miss the wake and leave `Process` spinning on `IsWritable()`.
- Doxygen: broken `\ref` on the public reader header; private storage types not listed as public API.
- Exclude `Pipe` itself from its callable constructor before evaluating copyability, avoiding recursive concept satisfaction and consequent vector copy/move compilation failures with Clang/libc++.
- Missing virtual destructors on `BufferedLocationReader` / `BufferedLocationWriter`. Leaves stay `final` and do not declare `virtual` on the destructor.
- `BufferedReader` cache cost on large sequential reads (also through `BufferedLocationReader` / `BufferedFileReader`). Every contiguous pull rebuilt and copied the whole accumulated span, so a sequential scan was quadratic in the span size (a 182 MB file effectively stalled with the cache on). Spans are now extended and trimmed in place:
  - Contiguous or overlapping pulls reuse the span that opens the merged range and append only the new bytes.
  - Cache hits and span splits copy only the requested slice instead of the whole span.
  - Trimming the front of the span at `Tell` advances its read position and compacts lazily (amortized O(1) per evicted byte).
  - Prefetch no longer pulls past `MaxMemory` ahead of `Tell`, avoiding a tail rebuild on every refill.
  - Random-access semantics (overlaps, seeks, `MaxMemory`, eviction order, `ReadAhead`, telemetry, prefetch cancellation) are unchanged.

### Tests

- Stored-notification regressions force Push, final-writer EOF, empty Sink closure, explicit control wake and queued/closed wiring between a false readiness check and atomic wait. Additional Hopper/Sink tests cover registration before/after wiring, new buckets, all-bucket binding, fan-in, capacity preservation, counter replacement, legacy replacement, stale owner removal, rewiring and concurrent Push/EOF/wiring versus observer removal and borrowed-object destruction.
- `BufferedFileReaderTests` / `BufferedFileWriterTests` / `BridgeTests` construct IO with `Parameters` / knobs (`ReadAhead`, `MaxMemory`, `WriteChunk`, `BackPressure`). Path-only still probes.
- Predictable hex fixture, integrity of every `Read` after logical and cold seeks, `Tell` during a logical seek, telemetry prints via `*Telemetry()`.
- Writer close/flush integrity on hex files, holes, far islands, eviction + patch, ring-only / pages / direct knobs.
- Reader sequential regressions: many contiguous blocks merge into one replayable span (bytes and telemetry), capped sliding window, islands bridged by an overlapping sequential pass, and a 64 MiB sequential benchmark with cache / read-ahead off, cache only, windowed and full.
- Typed queue boundary regressions: negative/zero/positive key ordering, snapshot independence from later wiring, deep-copy/move behaviour and use after Sink destruction; shared hoppers retain queued items and EoF after producer destruction. Compile-time admission checks reject extended alignment, throwing construction and unsupported payloads while accepting supported Safe values and fundamental alignment. Borrowed mutable/move-only selectors preserve original captures; explicit Safe selectors cover status/output, missing context, exceptions and provider-owned release. Notification teardown covers matching observers and concurrent notification.
- Independent and nested telemetry samples, including moving a sample to another thread. Exact-type `MaybeSafe` assertions include rejection of inherited certification. Throwing pipeline stages set error in Sync, Async, Parallel and Async|Parallel modes.
- Throwing IO `OriginOpen`, `OriginPull` and `OriginDevice` hooks become IO result failures or Buffer-domain exceptions rather than leaking foreign exceptions.

[2.0.0]: https://github.com/StormByte-Suite/StormByte-Buffer/compare/1.4.0...2.0.0

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

[1.4.0]: https://github.com/StormByte-Suite/StormByte-Buffer/compare/1.3.0...1.4.0

## [1.3.0] - 2026-09-20

### Added

- `Sink::Keys`, `Sink::Buckets`, `Sink::Contains`, `Sink::Empty(key)`, `Sink::EoF(key)` and `Sink::Ready(key)`. Query only. Missing key: `Empty` is true, `EoF`/`Ready`/`Contains` are false, `Buckets` is the wired count.
- `Sink::Pop(int key)`. Reads that hopper only. Waits until the key is wired or the Sink is closed. Empty hopper returns default `T` (same as `Hopper::Pop`). Does not interpret the key.
- `Hopper::Writers`, `Hopper::Ready` and `Hopper::Front`. `Front` copies the next item and does not dequeue; requires `std::copy_constructible<T>` (`shared_ptr`). Not a deep copy of the payload. `Writers` is the live writer count (starts at 1).

### Tests

- `test_hopper_front_peek`, `test_hopper_writers_and_ready`.
- `test_sink_pop_key`, `test_sink_query_unwired`, `test_sink_query_wired`.
- Hopper and Sink test files ordered by section name, then by test name.

[1.3.0]: https://github.com/StormByte-Suite/StormByte-Buffer/compare/1.2.0...1.3.0

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

- Optional Logger pin is [1.2.0](https://github.com/StormByte-Suite/StormByte-Logger/releases/tag/1.2.0)
  (`Scope` and hierarchical components).

### Tests

- `test_hopper_unnotify_before_cv_dies`, `test_hopper_notify_after_unnotify`,
  `test_hopper_stream_members`, `test_hopper_stream_item_into`.
- `test_sink_unnotify_before_cv_dies`, `test_sink_stream_operators`.
  Existing Sink tests use `To` / `>>` / `<<` instead of `Bind`.

[1.2.0]: https://github.com/StormByte-Suite/StormByte-Buffer/compare/1.1.2...1.2.0

## [1.1.2] - 2026-09-16

### Deprecated

- `Sink::Bind` (both overloads). Removed in 2.0.0.

### Fixed

- `Sink::Bind(key)` of a hopper this Sink already holds attaches the other Sink as a co-writer. `Sink::Eof` then closes that hopper only when the last writer closes, so extra producers can still `Push`. No new public methods.
- `Sink::Bind(key)` does not add a writer if the other Sink already writes that hopper. A second Bind of the same producer no longer leaves the hopper open after one `Eof`. `test_sink_rebind_same_writer_eof` waits up to 1s for that EoF (the remuxer hang).

[1.1.2]: https://github.com/StormByte-Suite/StormByte-Buffer/compare/1.1.1...1.1.2

## [1.1.1] - 2026-09-15

### Changed

- Bundled StormByte Logger is [1.1.1](https://github.com/StormByte-Suite/StormByte-Logger/releases/tag/1.1.1) (and transitively [StormByte Base 1.1.1](https://github.com/StormByte-Suite/StormByte/releases/tag/1.1.1)). The declared requirement stays Logger [1.1.0](https://github.com/StormByte-Suite/StormByte-Logger/releases/tag/1.1.0) or newer.

[1.1.1]: https://github.com/StormByte-Suite/StormByte-Buffer/compare/1.1.0...1.1.1

## [1.1.0] - 2026-09-13

### Added

- `Hopper<T>` — single-producer single-consumer (SPSC) typed queue (`Type::MoveConstructible`) with optional capacity ceiling, non-blocking `Pop`, `Push` wait when full, `Eof` signaling, consumer condition variable notification (`Notify`), and automatic null item discarding for `Type::SmartPointer` types.
- `Sink<T>` — integer key map of `Hopper<T>` buckets supporting `Bind` hopper sharing, terminal producer `Drain` mode, `Ready` check, Round-Robin or custom `Select` index chooser popping, and per-key `Capacity`, `Size`, and `Full` queries.
- Header implementation files `hopper.txx` and `sink.txx` installed alongside public headers (`*.txx` in `cmake/install.cmake`).

### Changed

- Updated dependency requirement to [StormByte Logger 1.1.0](https://github.com/StormByte-Suite/StormByte-Logger/releases/tag/1.1.0) (and transitively [StormByte Base 1.1.0](https://github.com/StormByte-Suite/StormByte/releases/tag/1.1.0)).
- Routed range and iterator byte APIs through StormByte Base type concepts (`Type::ByteInputRange`, `Type::ByteInputIterator`, `Type::SentinelFor`, `Type::SameAs`) instead of local standard-library constraints.
- Limited `Hopper<T>` and `Sink<T>` null-item filtering to `Type::NullablePointer` so only pointer-like values that can be tested for emptiness use the `!item` path.
- Documented `Sink::EoF()` contract: with hoppers, evaluates `true` when all hoppers are empty and `Hopper::EoF()` is `true`, even if this `Sink` itself did not call `Eof()`; binding a new key after `EoF()` returned `true` may return `EoF()` to `false`.

### Fixed

- Ported Buffer exceptions to `StormByte::Component`, preventing format-string overload ambiguity and correctly prefixing `ReadError` and `WriteError` messages.
- Moved virtual override definitions and destructors to compiled translation units so shared-library builds export stable vtables, RTTI and non-virtual thunks for GCC consumers, with polymorphic consumer coverage in each owning class test.
- Closed `Sink` and marked hoppers `Eof` atomically under `m_mutex` in `Sink::Eof` and checked closed state and `consumer.m_consumer` under lock in `Sink::Bind` to prevent concurrent `Bind` calls from creating un-marked hoppers or skipping consumer `Notify`.
- Marked `Sink` closed in destructor to safely unblock threads waiting in `Push` and `Pop`.

[1.1.0]: https://github.com/StormByte-Suite/StormByte-Buffer/releases/tag/1.0.0..1.1.0

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

[1.0.0]: https://github.com/StormByte-Suite/StormByte-Buffer/releases/tag/1.0.0
