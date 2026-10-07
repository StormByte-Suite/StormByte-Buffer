# StormByte-Buffer

![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS-lightgrey)
![C++26](https://img.shields.io/badge/C%2B%2B-26-00599C?logo=c%2B%2B&logoColor=white)
![CMake](https://img.shields.io/badge/CMake-3.28+-064F8C?logo=cmake&logoColor=white)
![License: LGPL v3 or commercial](https://img.shields.io/badge/License-LGPL_v3_or_commercial-blue.svg)
[![CI](https://github.com/StormByte-Suite/StormByte-Buffer/actions/workflows/ci.yml/badge.svg)](https://github.com/StormByte-Suite/StormByte-Buffer/actions/workflows/ci.yml)
[![Sponsor](https://img.shields.io/badge/Sponsor-StormBytePP-ea4aaa?logo=githubsponsors)](https://github.com/sponsors/StormBytePP)

This repository is **StormByte Buffer**: byte streams, buffered I/O and typed item queues for the StormByte C++ suite. Those are three different things. A byte buffer does not carry a C++ object, and a Hopper does not carry octets.

It uses [StormByte Base 2.0.0](https://github.com/StormByte-Suite/StormByte/releases/tag/2.0.0) or newer through [StormByte-System 2.0.0](https://github.com/StormByte-Suite/StormByte-System/releases/tag/2.0.0) or newer, and uses [StormByte-Logger 2.0.0](https://github.com/StormByte-Suite/StormByte-Logger/releases/tag/2.0.0) or newer for pipeline pipes (`Scope`). Base provides the owned UTF-8 and wide text types, the octet payload, and the wait primitives. Public headers live under `StormByte/buffer/`.

The suite is split on purpose. Base, Config, Crypto, Database, Logger, Multimedia, Network and System are **other repositories**. This one does not implement them.

## Designed to interconnect

Pieces plug into each other only inside their own group.

- Byte tips share `ReadOnly` / `WriteOnly`. `Producer` yields a `Consumer` over the same `Ring`. `Pipeline` transforms those stream buffers only. `Bridge` moves octets from a `ReadOnly` or an IO reader into a `WriteOnly` or an IO writer. `Pumper` owns a `Bridge` and runs `Passthrough` until EoF or failure.
- IO leaves are also byte tips. `BufferedFileReader` *is* a `BufferedReader`. `BufferedFileWriter` *is* a `BufferedWriter`. A Bridge can steal a leaf by move. A Pipeline cannot take a leaf.
- `Hopper` and `Sink` take C++ items. They do not read or write `Safe::Binary`, and a byte buffer is not a Hopper.

Typical wires:

- `Producer` → `Consumer` (same ring).
- `Producer` → `Pipeline::Process` → `Consumer`.
- That `Consumer` → `Bridge::Passthrough` → file (or wrap the Bridge in a `Pumper`).
- `BufferedFileReader` / `BufferedFileWriter` for seekable files with a page map.
- `Sink::To(key) >> other` for a typed item, never for a byte chunk.

See [Bytes](#bytes), [IO](#io) and [Items](#items).

## What this module does

- **Bytes** — octet payloads are `StormByte::Safe::Binary`. Lengths are `StormByte::ByteSize`. FIFO, SharedFIFO, Ring, Producer/Consumer, Pipeline, Bridge and Pumper live here.
- **IO** — the buffered origin and sink hierarchy. `BufferedReader` / `BufferedWriter` own the cache. `BufferedLocationReader` / `BufferedLocationWriter` add a named, seekable, sized location. `BufferedFileReader` / `BufferedFileWriter` are the final file leaves.
- **Items** — `Hopper<T>` and `Sink<T>`. They count objects with `StormByte::Size`. `T` is a `SafeValue`, not an octet buffer.
- **Telemetry** — `ReadTelemetry` / `WriteTelemetry` as `const StormByte::Safe::Shared<…>`, derived from Base `StormByte::Telemetry`. Used by IO offices and by Bridge. `MeanRate` is caller rate, not disk rate. Writer `Cap` is the ring cap (`WriteChunk * BackPressure`, or 0 if the ring is off). It is not `MaxMemory`.
- **Lifecycle** — `Close()`, `SetError()`, `EoF()`, `IsReadable()`, `IsWritable()`.

## The rest of the suite

| Module | Role | API |
| --- | --- | --- |
| [Base](https://github.com/StormByte-Suite/StormByte) | Exceptions, Expected, serialization, UUID, Safe | [/StormByte](http://suite.stormbyte.org/StormByte) |
| **Buffer** | This repository | [/StormByte-Buffer](http://suite.stormbyte.org/StormByte-Buffer) |
| [Config](https://github.com/StormByte-Suite/StormByte-Config) | Human-readable text and versioned binary documents | [/StormByte-Config](http://suite.stormbyte.org/StormByte-Config) |
| [Crypto](https://github.com/StormByte-Suite/StormByte-Crypto) | Hash, compress, encrypt, sign — Crypto++ stays private | [/StormByte-Crypto](http://suite.stormbyte.org/StormByte-Crypto) |
| [Database](https://github.com/StormByte-Suite/StormByte-Database) | One API over SQLite, PostgreSQL and MariaDB | [/StormByte-Database](http://suite.stormbyte.org/StormByte-Database) |
| [Logger](https://github.com/StormByte-Suite/StormByte-Logger) | Stream logger with levels, headers, components and `Scope` | [/StormByte-Logger](http://suite.stormbyte.org/StormByte-Logger) |
| [Multimedia](https://github.com/StormByte-Suite/StormByte-Multimedia) | Decode, encode and containers without raw FFmpeg types | [/StormByte-Multimedia](http://suite.stormbyte.org/StormByte-Multimedia) |
| [Network](https://github.com/StormByte-Suite/StormByte-Network) | Framed packets, Client/Server, IPv4/IPv6 TCP | [/StormByte-Network](http://suite.stormbyte.org/StormByte-Network) |
| [System](https://github.com/StormByte-Suite/StormByte-System) | Processes, pipes, `Device`, host and environment | [/StormByte-System](http://suite.stormbyte.org/StormByte-System) |

## Table of Contents

- [Designed to interconnect](#designed-to-interconnect)
- [What this module does](#what-this-module-does)
- [The rest of the suite](#the-rest-of-the-suite)
- [Documentation](#documentation)
- [Installation](#installation)
- [Bytes](#bytes)
  - [FIFO](#fifo)
  - [Producer and Consumer](#producer-and-consumer)
  - [Pipeline](#pipeline)
  - [Bridge](#bridge)
  - [Pumper](#pumper)
- [IO](#io)
  - [Parameters](#parameters)
  - [Telemetry](#telemetry)
  - [BufferedReader](#bufferedreader)
  - [BufferedLocationReader](#bufferedlocationreader)
  - [BufferedFileReader](#bufferedfilereader)
  - [BufferedWriter](#bufferedwriter)
  - [BufferedLocationWriter](#bufferedlocationwriter)
  - [BufferedFileWriter](#bufferedfilewriter)
- [Items](#items)
  - [Hopper](#hopper)
  - [Sink](#sink)
- [Support](#support)
- [Contributing](#contributing)
- [License](#license)

## Documentation

- This README: how to build, ownership, examples.
- Doxygen class reference: [http://suite.stormbyte.org/StormByte-Buffer/](http://suite.stormbyte.org/StormByte-Buffer/).

### DLL Boundary Contract (2.0.0)

DLL safety is conditional, not a promise of compatibility between different compilers or STL ABIs. `MaybeSafe` registrations apply to exact types only: the `Generic` / `ReadOnly` / `WriteOnly` / `ReadWrite` interfaces, `Producer`, `Consumer`, `Bridge`, `Pumper`, `Pipe`, `Pipeline`, buffered IO bases and file leaves, telemetry types and operation samples, IO `Result` and knobs, and `Pumper::Parameters`. `Hopper<T>` and `Sink<T>` are partial specializations of `IsMaybeSafe`. The exact-type macro does not accept a class template. Derived types do not inherit certification; each concrete provider and its owned state need their own audit and declaration.

Keep Buffer, Base and every participating concrete, template, element and callback provider loaded until their objects, owners and callbacks are released. Logger must also remain loaded while pipeline logger handles are used. Inline template callbacks are not automatically provider-local. Stop and join all users before destroying queues or their borrowed state; allocation failure in nonthrowing operations terminates the process.

`Ring` constructs, moves and destroys its private deque and synchronization state in Buffer; `SharedFIFO` constructs and destroys its mutex / condition variable out of line. Range constructors convert to Base-owned `Safe::Binary` in the caller and delegate to the provider constructor. This keeps private storage lifecycle in its provider, without transferring STL container ownership or certifying derived buffers. Pipe callable state is allocated with `Safe::Heap::Allocate` and released by the creator callback. It is not a `std::unique_ptr`.

Protected `FIFO::HexDumpHeader()` and `Ring::HexDumpHeader()` return `StormByte::Safe::String`, not `std::ostringstream`. Custom overrides must use the new return type; stream formatting stays local to the implementation.

Public headers do not export `std::mutex`, `std::condition_variable`, `std::atomic` or `std::thread`. Hopper, Sink, SharedFIFO, Ring and the IO engines wait on `Safe::Mutex`, `Safe::ConditionVariable` and `Safe::Atomic`. `Notify` borrows the caller's `Safe::ConditionVariable` and, in the stored-event overload, a `Safe::Atomic<std::size_t>`. A worker created by this module is a `Safe::Thread`, created and joined here. A thread you start in your own `.cxx` is yours. Do not pass a `std` wait object into `Notify`.

## Installation

Needs a C++26 compiler, CMake 3.28 or newer, [StormByte Base 2.0.0](https://github.com/StormByte-Suite/StormByte/releases/tag/2.0.0) or newer via [StormByte-System 2.0.0](https://github.com/StormByte-Suite/StormByte-System/releases/tag/2.0.0) or newer, and [StormByte-Logger 2.0.0](https://github.com/StormByte-Suite/StormByte-Logger/releases/tag/2.0.0) for pipeline pipes.

```sh
git clone --recursive https://github.com/StormByte-Suite/StormByte-Buffer.git
cd StormByte-Buffer
cmake -S . -B build
cmake --build build
```

Shared vs static follows CMake `BUILD_SHARED_LIBS` (declared in `lib/`, default ON). A plain configure builds the shared library. `-DBUILD_SHARED_LIBS=OFF` builds a static archive; on Windows the headers then do not use `dllimport`. Vendored StormByte pins follow the same mode and are configured with `ENABLE_TEST=OFF`.

A shared build keeps this library as its own `.so` / `.dll`. Under the LGPL that is usually the simpler way to ship: the user can replace that file. A static archive is folded into your binary. The LGPL still applies to this code; you must give the recipient a way to relink your product with a different build of this library. If that does not fit how you distribute the final product, a commercial license is available from the copyright holder (see [License](#license)).

Link `StormByte-Buffer`. Headers: `#include <StormByte/buffer/….hxx>`.

## Bytes

These types move octets. The payload is `StormByte::Safe::Binary`. A length is `StormByte::ByteSize`. `FIFO` is the plain buffer. `SharedFIFO` and `Ring` add concurrency. `Producer` and `Consumer` are the write and read tips of one ring. `Pipeline` transforms those tips. `Bridge` copies octets from a read tip to a write tip, and can steal an IO leaf. `Pumper` owns that Bridge and runs it. None of them stores a `T`.

Namespace root is `StormByte::Buffer`.

### FIFO

```cpp
#include <StormByte/buffer/fifo.hxx>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Safe::Binary;

int main() {
	FIFO fifo;
	fifo.Write("Hello World");

	Binary data;
	fifo.Read(5, data); // "Hello", still in the buffer
	fifo.Seek(6, Position::Absolute);

	Binary extracted;
	fifo.Extract(5, extracted); // "World"
}
```

`FIFO` is not thread-safe. Concurrent ends use `SharedFIFO` or `Ring`. `Read` / `Peek` keep data; `Extract` consumes it.

### Producer and Consumer

A `Producer` yields a `Consumer` over the same ring. The `Consumer` is a `ReadOnly`; the `Producer` is a `WriteOnly`. Either tip can be passed to a [Bridge](#bridge). Ring ownership uses opaque `StormByte::Safe::Owner` callbacks implemented in Buffer, not `std::shared_ptr<Ring>` in the public ABI. The owner and any `Producer`/`Consumer` copy can outlive its handle, but the Buffer and Base modules must stay loaded until all handles are destroyed. This conditional contract does not certify arbitrary cross-module payloads or compiler/STL ABI compatibility.

The threads in this example are the caller's. They are not part of the Buffer ABI.

```cpp
#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/producer.hxx>
#include <thread>

using StormByte::Buffer::Consumer;
using StormByte::Buffer::Producer;
using StormByte::Safe::Binary;

int main() {
	Producer producer;
	Consumer consumer = producer.Consumer();

	std::thread writer([producer]() mutable {
		producer.Write("Data chunk 1");
		producer.Write("Data chunk 2");
		producer.Close();
	});

	std::thread reader([consumer]() mutable {
		while (!consumer.EoF()) {
			Binary data;
			if (consumer.Extract(0, data) && !data.empty()) {
				// process
			}
		}
	});

	writer.join();
	reader.join();
}
```

### Pipeline

`Pipeline` transforms **stream** buffers (`ReadOnly` / `WriteOnly`). It does not take IO leaves. A file or device is attached later with a [Bridge](#bridge).

A `Pipe` is a nonpolymorphic value owning a copyable `Safe::Function`. Construct it from a copyable lambda or callable taking `const PipeInput&`, `const PipeOutput&` and `const Safe::Shared<Logger::Log>&`. Mutable value captures are supported. `Add(const Pipe&)` deep-copies the callable context without changing the caller; `Add(Pipe&&)` transfers it. Copying or assigning a Pipeline independently copies its stages, not its buffers or workers. Reference captures and shared handles retain their normal sharing semantics. Do not copy a running pipeline while callbacks mutate their captures; externally serialize owner operations and callback-state access.

`PipeInput` borrows a live `ReadOnly`; `PipeOutput` borrows a live `WriteOnly`. Their const functions forward stream operations to those endpoints. `Read(count, bytes)` advances the input cursor, with zero meaning all currently available bytes; `Write(bytes)` copies or moves binary data and `Write(text)` uses a length-aware string view, including Base `Safe::String`'s view conversion. Close or `SetError` the output before returning. `Pipeline::Process(buffer, log, mode)` keeps mode last and supplies a scoped logger when present.

The endpoint facades, all their copies, and the logger reference are synchronous borrows valid only until invocation returns. Never retain them in captures, background work or handles. Their exact-type `MaybeSafe` declarations admit the explicit facade contract; they do not automatically certify the lifetime of an arbitrary raw pointer or underlying endpoint. Const forwarding is not a thread-safety guarantee. Pipeline keeps its endpoints alive for invocation, including on background workers. The callback provider, Buffer, Base and Logger must remain loaded with compatible C++/STL ABI until all callbacks and workers are released.

The callable factory constructs captures with `Safe::Heap::Allocate` in the creator module and attaches matching creator-side invoke, clone and release callbacks. For explicitly controlled DLL providers, construct `Pipe::Callback(context, invoke, clone, release)` and move it into `Pipe`. Clone must independently copy context in that provider and return null on failure; release must destroy captures there and free storage with `Safe::Heap::Free`. Template instantiations and externally defined callable types still require a provider-locality audit; this is not compatibility across arbitrary runtimes. `Run` returns `Safe::Status`: `Failure`, moved-from `Missing`, Safe exceptions and foreign exceptions cause Pipeline to mark every output errored and wake waiters. Nothing escapes `Process`'s `noexcept` boundary.

To migrate an old derived stage, replace its `Run` implementation with a callable using the facades, remove inheritance and virtual `Clone`/`Move`, and construct a `Pipe` from that callable. Replace endpoint `Extract` calls with a `Read` loop and `Write(count, bytes)` with `Write(bytes)` when writing the whole chunk. Move-only captures are intentionally rejected to preserve Pipeline copy semantics.

```cpp
#include <StormByte/buffer/pipe.hxx>
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/safe/binary.hxx>
#include <StormByte/safe/pointers.hxx>

using StormByte::Buffer::Consumer;
using StormByte::Buffer::ExecutionMode;
using StormByte::Buffer::Pipe;
using StormByte::Buffer::Pipeline;
using StormByte::Buffer::Producer;
using StormByte::Buffer::PipeInput;
using StormByte::Buffer::PipeOutput;
using StormByte::Safe::Binary;

Pipe MakeStripCrPipe() {
	return Pipe([](const PipeInput& in, const PipeOutput& out,
		const StormByte::Safe::Shared<StormByte::Logger::Log>&) {
		while (!in.EoF()) {
			Binary raw;
			if (!in.Read(0, raw))
				break;
			Binary unix_newlines;
			unix_newlines.reserve(raw.size());
			for (const std::byte octet : raw) {
				if (octet != std::byte{'\r'})
					unix_newlines.push_back(octet);
			}
			out.Write(std::move(unix_newlines));
		}
		out.Close();
	});
}

int main() {
	Producer src;
	src.Write("a\r\nb\r\n");
	src.Close();

	Pipe crlf = MakeStripCrPipe();
	Pipeline pipe;
	pipe.Add(crlf);

	Consumer result = pipe.Process(src.Consumer(), {}, ExecutionMode::Sync);
	(void)result;
}
```

`Sync` runs on the caller thread. `Async` returns at once. `Parallel` is one thread per pipe. Combine with `|`. The pipeline worker is a `Safe::Thread` created and joined in this module.

### Bridge

`Bridge` is a manual transfer. Bytes move only when you call `Bridge::Passthrough`. There is no worker and no occupancy cap here. Continuous transfer is [Pumper](#pumper).

- In-memory tips: `ReadOnly&` / `WriteOnly&`. Those buffers must outlive the Bridge.
- IO tips: stolen by move as the concrete leaf (`BufferedFileReader`, `BufferedFileWriter`, …).
- `Passthrough(n, Operation)` is atomic. Write `TryAgain` is retried until that call completes.
- `Operation` applies to the **read** tip only: `Blocking` waits for `n` or EoF; `NonBlocking` takes what is available now, up to `n`.
- `n == 0` is current contents (`Available()` on IO does not touch the origin).
- `State` is `Open`, `Closed` or `Failed`. `Failed()` is only a real tip fault. A moved-from Bridge is `Closed`, not `Failed`.
- `Close()` ends the session. Borrowed in-memory tips are detached and stolen IO leaves are released. After `Close`, a consumed source EoF, or a move-from, the instance is `Closed` and cannot be re-armed. Construct a new Bridge to transfer again. `Close()` is idempotent and does not set `Failed`.
- A NonBlocking `Passthrough` that returns `0` is not the end of the session.
- Telemetry: the Bridge always keeps a `Shared` copy of the read and write counters. Non-IO tips use a basic telemetry owned and updated by the Bridge. IO tips donate the leaf handle at attach; the leaf updates that object. After `Close` the same handles remain valid. A `Shared` you already copied stays valid when the Bridge dies.

On Windows an IO leaf keeps the origin handle until that leaf is destroyed. While the Bridge still owns a `BufferedFileReader` or a `BufferedFileWriter`, the path stays locked: `DeleteFile` / `std::filesystem::remove` fail. In-memory tips do not lock a file. Call `Bridge::Close()` when you are done so the stolen IO origin is closed and the path can be unlinked. The destructor does the same work; `Close` is for when `*this` must stay alive.

```cpp
#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>

#include <filesystem>

using StormByte::Buffer::Bridge;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::IO::BufferedFileWriter;

int main() {
	FIFO in;
	in.Write("payload");
	in.Close();

	BufferedFileWriter out("out.bin");
	out.Open();

	Bridge bridge(in, std::move(out));
	while (!bridge.EoF() && !bridge.Failed()) {
		if (bridge.Passthrough(64 * 1024) == 0 && !bridge.EoF())
			break;
	}
	auto read = bridge.ReadTelemetry();
	bridge.Close();
	(void)read;	// still valid
	std::filesystem::remove("out.bin");
}
```

A `Pipeline` is not a Bridge tip. `Pipeline::Process` is. It returns a `Consumer`, and a `Consumer` is `ReadOnly`, so it can be the source of a Bridge. The stage factory `MakeStripCrPipe` is the one from [Pipeline](#pipeline).

```cpp
#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/buffer/producer.hxx>

using StormByte::Buffer::Bridge;
using StormByte::Buffer::Consumer;
using StormByte::Buffer::ExecutionMode;
using StormByte::Buffer::Pipeline;
using StormByte::Buffer::Producer;
using StormByte::Buffer::IO::BufferedFileWriter;

int main() {
	Producer capture;
	capture.Write("line 1\r\nline 2\r\n");
	capture.Close();

	auto crlf = MakeStripCrPipe();
	Pipeline pipe;
	pipe.Add(crlf);

	Consumer normalized = pipe.Process(capture.Consumer(), {}, ExecutionMode::Sync);

	BufferedFileWriter out("session.log");
	out.Open();
	Bridge to_disk(normalized, std::move(out));
	while (!to_disk.EoF() && !to_disk.Failed()) {
		if (to_disk.Passthrough(4 * 1024) == 0 && !to_disk.EoF())
			break;
	}
	to_disk.Close();
}
```

`session.log` holds `line 1\nline 2\n`. For a long capture, wrap `to_disk` in a `Pumper` instead of the `Passthrough` loop. The file stays locked while that `Pumper` is alive; `~Pumper` joins and then releases the owned Bridge.

### Pumper

`Pumper` takes a `Bridge` by move and starts a worker immediately. The destructor **joins** and may block until the current cycle ends. It does **not** `Cancel`: it lets the worker finish. That worker is a `Safe::Thread`.

- `Chunk` — bytes asked of `Passthrough` each cycle. `0` is automatic chunking, **not** Bridge “current contents”.
- `HighWater` — input cap only. Omitted: `0` if the source is IO, otherwise a backend default (constexpr in the PIMPL). Explicit `0`: no Pumper cap. Use `0` when the IO source already limits itself. Non-IO sources are unbounded by design.
- `Toggle` pauses and resumes. No-op if `Failed` or `Canceled`.
- `Cancel` is terminal (`Canceled`, no restart). It does not set `Failed`. It `Close()`s the owned Bridge so Windows can unlink an IO path.
- `Failed()` is only a real Bridge fault. A moved-from Pumper is empty (`EoF`), not `Failed` and not `Canceled`.
- Telemetry handles are copied from the Bridge at construction. They stay valid after `Cancel`. A `Shared` you already copied stays valid when the Pumper dies.

`Parameters::Chunk()` and `Parameters::HighWater()` return `StormByte::Safe::Optional<StormByte::ByteSize>`. An empty optional means omitted, not an engaged zero. Omitted or zero `Chunk` selects automatic chunking; omitted `HighWater` selects the input-kind default, while an engaged zero explicitly disables the cap.

An IO path stays locked while the Pumper is alive. After `Cancel` (or after `~Pumper` joins and destroys the Bridge) the handle is released.

```cpp
#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/pumper.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>

using StormByte::Buffer::Bridge;
using StormByte::Buffer::Chunk;
using StormByte::Buffer::HighWater;
using StormByte::Buffer::Pumper;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::BufferedFileWriter;

int main() {
	BufferedFileReader in("in.bin");
	BufferedFileWriter out("out.bin");
	in.Open();
	out.Open();

	Pumper pump(Bridge(std::move(in), std::move(out)), { Chunk{1 << 20} });
	while (!pump.EoF() && !pump.Failed() && !pump.Canceled()) {
		// work elsewhere; pump runs on its thread
	}
	auto read = pump.ReadTelemetry();
	(void)read;	// still valid after ~Pumper
	// ~Pumper joins
}
```

`finish = !pump.Failed() && !pump.Canceled() && pump.EoF()`.
`active = !pump.Failed() && !pump.Canceled() && !pump.EoF()`.

## IO

IO is the buffered file-like hierarchy. It still moves octets, but the public type is not a FIFO. A leaf is a reader or a writer with a page cache, a logical cursor and an origin hook.

The chain is fixed:

- `BufferedReader` is the read engine. Cache, prefetch, logical `Seek`, `Tell` and telemetry live here. A custom origin implements the `Origin*` hooks.
- `BufferedLocationReader` sits on that engine. A location is named, always seekable and sized. `Device()` returns a `Safe::Shared<System::Device>` so a subclass is not sliced.
- `BufferedFileReader` is the final file leaf. It opens a path, probes the device and uses the location layer.
- `BufferedWriter`, `BufferedLocationWriter` and `BufferedFileWriter` are the write side of the same three levels.

A Pipeline cannot take one of these. A Bridge can, by move.

### Parameters

IO leaves and `Pumper` take a nested `Parameters` object. Omitted knobs keep the office default. Brace-init and a named bag are both valid. Bags are header-only and store `StormByte::Safe::Optional` values; ownership and release use the storage provider's callbacks, not an exposed STL optional. IO constructors resolve the knobs into numbers, durations and probe flags. Pure knob and bag functions use ordinary inline definitions.

IO byte-count getters return `const Safe::Optional<ByteSize>&`; `BackPressure()` returns `const Safe::Optional<std::size_t>&`. The bag's `MaxWait()` returns `const Safe::Optional<std::chrono::milliseconds::rep>&`, a signed millisecond count. Resolve it with `std::chrono::milliseconds{p.MaxWait().value_or(0)}`. `MaxWait{std::chrono::milliseconds{...}}` still accepts durations, and live reader/writer `MaxWait()` getters still return durations. Negative counts are preserved; an absent knob remains distinct from an engaged zero (unlimited wait).

Bag construction, copying and knob assignment may allocate and throw. The empty file-leaf `Parameters` constructor is not `noexcept`. Copy operations are not `noexcept`; moves and destruction remain nonthrowing and transfer or release Safe-owned state. Keep the storage provider loaded until all bags and copies are released.

```cpp
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::ReadAhead;
using StormByte::Buffer::IO::BufferedFileReader;

BufferedFileReader in("in.bin");                          // probe at Open
BufferedFileReader in2("in.bin", { ReadAhead{1 << 20} }); // one knob
BufferedFileReader::Parameters p{ ReadAhead{1 << 20}, MaxMemory{8 << 20} };
BufferedFileReader in3("in.bin", p);
```

Explicit `0` is `0`. It does not probe.

### Telemetry

Every IO office and every Bridge exposes `const StormByte::Safe::Shared<ReadTelemetry>` / `WriteTelemetry`. The handle is the same object for the life of the office. IO types add cache / origin / seek / wait counters. Non-IO Bridge tips use the basic type.

`MeanRate` is octets per second of **requested user operations**, including cache hits. It is not a disk benchmark. A cached write can look like GiB/s. Worker, GC and internal flushes enter the rate only when they delay the caller. Explicit `Flush` / `Close` Flush pull it back.

Each measured operation owns an independent Base clock sample, so concurrent and nested operations do not share a start/stop state. A sample borrows its `Telemetry` owner and must not outlive it.

Reader `Cap` is the `MaxMemory` snapshot. Writer `Cap` is the ring cap at that snapshot: `WriteChunk * BackPressure`, or 0 when the ring is off. It is not the page-map budget.

Flatten with `operator StormByte::Safe::String` or `operator std::string()` (the latter is `FORCE_INLINE` so the `std::string` lives in your TU):

```cpp
auto tel = reader.Telemetry();
if (tel)
	log << Level::Info << *tel << std::endl;
```

### BufferedReader

Public base for a binary origin. Leaves implement `OriginOpen`, `OriginClose`, `OriginPull`, `OriginCanSeek`, `OriginSeek`, `OriginHasSize`, `OriginSize`. Construction is `Unavailable`; a successful `Open` is `Idle`.

`Size()` and the virtual `OriginSize() const noexcept` return `StormByte::Safe::Optional<StormByte::ByteSize>`, not `std::optional`. Empty means unknown or unavailable size; an engaged `ByteSize{0}` means a known empty origin. Custom readers must update the override signature and preserve this distinction. Downstream Network reader overrides must be updated in the Network repository too; this repository does not implement that migration.

Exceptions from `OriginOpen`, `OriginClose`, `OriginPull` and `OriginSeek` are converted to `Status::Error`. Exceptions from `Setup` or `CreateTelemetry` make `Open` return `false`. The public `Device()` accessor rethrows StormByte exceptions and translates foreign exceptions to `StormByte::Buffer::Exception`.

`Available()` is cached bytes at `Tell`. It does not call the origin.

`Seek` is logical. A cache hit does not move the device. `Tell` never lies. Consumed bytes remain in RAM up to `MaxMemory`. Garbage collection evicts farthest from `Tell`.

### BufferedLocationReader

Sits between the engine and a file leaf. A location is file-like: named by `Location()` (`StormByte::Safe::String`, owned by Base), always seekable and sized. Path-only `Setup()` lives here.

`Device()` and the pure `OriginDevice()` return `StormByte::Safe::Shared<StormByte::System::Device>`, not `System::Device` by value. A leaf may hand out a subclass and the dynamic type survives, so overridden `Throughput()` / `Window()` are honoured. Build the owner with `StormByte::Safe::Shared<StormByte::System::Device>::MakePointer<Leaf>(…)`.

`OriginDeviceUsable(const Safe::Shared<System::Device>&) const noexcept` defaults to a non-empty owner whose `operator bool()` is true. A leaf whose identifier is not a filesystem path overrides it. An empty owner is always unusable. `Setup()` is `final`: it calls `OriginDevice()` once, asks `OriginDeviceUsable()` and only then applies `Window()`.

### BufferedFileReader

Final file leaf. It passes `Location::Local`, keeps the plain `System::Device` and the real path probe. `CreateDevice()` is gone. Path-only construction probes at `Setup`. Explicit knobs stay explicit. The message constructor of `Buffer::Exception` takes `string_view`. A null `const char*` is not a message.

```cpp
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/fifo.hxx>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::MaxMemory;
using StormByte::Buffer::IO::ReadAhead;

int main() {
	BufferedFileReader in("in.bin", { ReadAhead{1 << 20}, MaxMemory{8 << 20} });
	in.Open();

	FIFO dest;
	(void)in.Read(64 * 1024, dest);
	auto tel = in.Telemetry();
	in.Close();
}
```

### BufferedWriter

Public base for a binary sink. Leaves implement `OriginOpen`, `OriginClose`, `OriginPush`, `OriginFlush`, `OriginTruncate`. Writes are lazy up to `MaxMemory`. Public `Flush` and the Flush inside `Close` count toward `MeanRate`. Internal drains do not.

`Seek` is logical. Materializing a page (eviction, flush or close) moves the origin. Nearby corrections and far-future islands are supported while memory allows. Eviction prefers the oldest dirty page behind the origin cursor.

Exceptions from origin hooks are converted to `Status::Error`; `Open` returns `false` when setup or telemetry creation throws, and `Close` returns `false` when closing the origin fails.

### BufferedLocationWriter

The write-side location layer. Same contract as the reader: a named, seekable, sized location, `Device()` as `Safe::Shared<System::Device>`, and `OriginDeviceUsable` before the window is applied. Path-only `Setup()` lives here. A socket on a lower layer can pass `Location::Remote`. This repository's file leaf does not.

### BufferedFileWriter

Final file leaf. It passes `Location::Local` and probes the device at `Setup` when no knobs are set. Explicit `WriteChunk`, `BackPressure` and `MaxMemory` stay explicit. `Truncate` overwrites. The path stays locked on Windows until the leaf is destroyed.

## Items

Items are not byte buffers and not IO. `Hopper<T>` and `Sink<T>` carry C++ objects. `T` must be an unqualified `StormByte::Type::SafeValue`, default-constructible and movable, with nonthrowing default construction, move construction, move assignment and destruction. `alignof(T)` must not exceed `alignof(std::max_align_t)`. Counts are `StormByte::Size`. A `Safe::Binary`, a `std::string` or a raw pointer is not admitted just because it can be moved. Use `int`, `Safe::String`, `Safe::Shared<int>` or another value that already meets the contract.

The wait path is `Safe::Mutex`, `Safe::ConditionVariable` and `Safe::Atomic<std::size_t>`. It is not a public `std::mutex`, `std::condition_variable` or `std::atomic`.

### Hopper

`Hopper<T>` is an SPSC queue. Queue storage uses Base's heap. Extended alignment is rejected at compile time, with no standard-allocator fallback. `Front` additionally requires nonthrowing copying.

Capacity `0` is unbounded. `Push` blocks when a bounded hopper is full. `Eof()` ends production; queued items can still be drained. Nullable pointer items discard nulls.

`Notify(cv)` borrows a `Safe::ConditionVariable`. Replacing or unregistering an observer synchronizes with in-flight notifications; `Unnotify()` must return before that condition variable dies. This does not make concurrent destruction of the Hopper safe: stop and join its users first.

`Notify(cv)` does not store events: the caller must coordinate predicate checks, waits and producer operations through the consumer's wait mutex. For consumers with an independent mutex, use `Notify(cv, generation)`, where `generation` is a borrowed `Safe::Atomic<std::size_t>`. Push and EOF increment it with release ordering and call `notify_all()` after publishing queue state. Load the counter with acquire ordering **before** checking readiness; if false, call `generation.wait(captured)` and repeat. Both the condition variable and the counter must remain alive until `Unnotify()` returns. Explicit stop/failure events must publish their state and increment/notify this same counter; do not reset it while consumers can wait on it.

```cpp
#include <StormByte/buffer/hopper.hxx>
#include <StormByte/safe/pointers.hxx>
#include <thread>

using StormByte::Buffer::Hopper;

int main() {
	Hopper<StormByte::Safe::Shared<int>> hopper(5);

	std::thread producer([&hopper]() {
		for (int i = 0; i < 10; ++i)
			hopper << StormByte::Safe::Shared<int>::MakePointer<int>(i);
		hopper.Eof();
	});

	std::thread consumer([&hopper]() {
		while (!hopper.Empty() || !hopper.EoF()) {
			auto item = hopper.Pop();
			(void)item;
		}
	});

	producer.join();
	consumer.join();
}
```

### Sink

`Sink<T>` maps integer keys to `Hopper<T>` buckets. It takes the same items as Hopper. It does not take octets. Wire with `To(key) >> dest`, `dest << producer` or `dest << producer.To(key)`. `To` returns a lane. It does not take the destination.

`Keys()` returns an independent, ascending `StormByte::Safe::Vector<int>` snapshot that remains valid after the Sink is destroyed. Copies are independent; keep Base and the snapshot's creator module loaded until all copies are released.

The coordinator is `Safe::Owner`. Wired hoppers are `Safe::Shared`. Buckets are `Safe::Map<int, Safe::Shared<Hopper<T>>>`, writers are `Safe::Set` of that same shared hopper, and the round-robin order is `Safe::Vector`. Over-aligned payloads are rejected, not routed to a standard allocator. The wait path is the Safe mutex, condition variable and atomic named above.

`Select` is `StormByte::Safe::Function<StormByte::Size(StormByte::Size)>`. `Pop(const Select&)` borrows it synchronously and never retains it. The provider callback has the form `Safe::Status(void* context, Size* output, Size count)`: write the index through `output` and report `Success`. `Missing`, `Failure` or an exception returns default `T` without consuming queued items. The callback provider owns and releases its context and must remain loaded through callback destruction. The caller-side callable overload borrows the original callable, including mutable or move-only captures, only for that `Pop` call.

Conditions passed to `Notify` are borrowed `Safe::ConditionVariable` objects. `Unnotify` unregisters this Sink's matching hopper registrations and waits for in-flight notifications before returning; finish it before destroying the condition variable. Stop and join all queue users before destroying the Sink.

`Sink::Notify(cv, generation)` uses the same stored-event protocol as Hopper on current and future buckets, including keyed wiring, all-bucket wiring and fan-in. Wiring publishes an event after updating the consumer, so already queued data or EOF also wakes it. Sink closure publishes an event even with no buckets. Shared hoppers have one observer: the latest registration replaces its condition variable, counter and owner identity. An older Sink's `Unnotify()` cannot remove that newer registration.

```cpp
StormByte::Safe::ConditionVariable wake;
StormByte::Safe::Atomic<std::size_t> generation{0};
consumer_sink.Notify(wake, generation);
for (;;) {
	const auto captured = generation.load(StormByte::Safe::MemoryOrder::Acquire);
	if (!consumer_sink.Ready()) {
		generation.wait(captured);
		continue;
	}
	if (consumer_sink.EoF())
		break;
	(void)consumer_sink.Pop();
}
consumer_sink.Unnotify();
```

Stored notifications change the private Hopper/Sink template implementation layouts in 2.0.0. Rebuild all providers and consumers that instantiate them before replacing the library; do not mix older and newer instantiations across DLLs. The borrowed condition variable and counter are Safe types. Their providers must remain loaded until notification removal completes. The counters add no ownership callbacks or changes to queue limits, blocking or element release.

```cpp
#include <StormByte/buffer/sink.hxx>
#include <StormByte/safe/string.hxx>
#include <thread>

using StormByte::Buffer::Sink;

int main() {
	Sink<StormByte::Safe::String> producer_sink;
	Sink<StormByte::Safe::String> consumer_sink;

	producer_sink.To(1) >> consumer_sink;
	producer_sink.To(2) >> consumer_sink;

	std::thread writer([&producer_sink]() {
		producer_sink.Push(1, StormByte::Safe::String("ch1"));
		producer_sink.Push(2, StormByte::Safe::String("ch2"));
		producer_sink.Eof();
	});

	std::thread reader([&consumer_sink]() {
		while (!consumer_sink.EoF())
			(void)consumer_sink.Pop();
	});

	writer.join();
	reader.join();
}
```

## Support

Questions and bugs: GitHub issues on this repository. Sponsorship: [github.com/sponsors/StormBytePP](https://github.com/sponsors/StormBytePP).

## Contributing

Open an issue before large work. Conventional Commits. Public headers need Doxygen. Do not send patches that reintroduce raw `new` for StormByte pointer types.

## License

Dual license: GNU Lesser General Public License v3.0 or later, or a commercial license from the copyright holder. See [LICENSE](LICENSE), [COPYING.LGPLv3](COPYING.LGPLv3) and <https://www.gnu.org/licenses/lgpl-3.0.html>. Third-party trees under `thirdparty/` keep their own licenses.
