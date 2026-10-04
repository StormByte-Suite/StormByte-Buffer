# StormByte-Buffer

![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS-lightgrey)
![C++26](https://img.shields.io/badge/C%2B%2B-26-00599C?logo=c%2B%2B&logoColor=white)
![CMake](https://img.shields.io/badge/CMake-3.28+-064F8C?logo=cmake&logoColor=white)
![License: LGPL v3 or commercial](https://img.shields.io/badge/License-LGPL_v3_or_commercial-blue.svg)
[![CI](https://github.com/StormByte-Suite/StormByte-Buffer/actions/workflows/ci.yml/badge.svg)](https://github.com/StormByte-Suite/StormByte-Buffer/actions/workflows/ci.yml)
[![Sponsor](https://img.shields.io/badge/Sponsor-StormBytePP-ea4aaa?logo=githubsponsors)](https://github.com/sponsors/StormBytePP)

This repository is **StormByte Buffer**: FIFO, SharedFIFO, Ring, Producer/Consumer, Hopper, Sink, Bridge, Pumper, pipelines and buffered I/O for the StormByte C++ suite.

It uses [StormByte Base 2.0.0](https://github.com/StormByte-Suite/StormByte/releases/tag/2.0.0) or newer through [StormByte-System 2.0.0](https://github.com/StormByte-Suite/StormByte-System/releases/tag/2.0.0) or newer, and uses [StormByte-Logger 2.0.0](https://github.com/StormByte-Suite/StormByte-Logger/releases/tag/2.0.0) or newer for pipeline pipes (`Scope`). Base provides the owned UTF-8 and wide text types. Public headers live under `StormByte/buffer/`.

The suite is split on purpose. Base, Config, Crypto, Database, Logger, Multimedia, Network and System are **other repositories**. This one does not implement them.

## Designed to interconnect

Pieces plug into each other through `ReadOnly` / `WriteOnly` and through IO leaves.

- `Producer` yields a `Consumer` over the same `Ring`.
- `Pipeline` transforms stream buffers only (`ReadOnly` / `WriteOnly`). Pipes do not take IO leaves.
- `Bridge` moves bytes from a `ReadOnly` or an IO reader into a `WriteOnly` or an IO writer. IO tips are taken by move; in-memory tips stay referenced.
- `Pumper` owns a `Bridge` and runs `Passthrough` until EoF or failure.
- `BufferedFileReader` *is* a `BufferedReader`. `BufferedFileWriter` *is* a `BufferedWriter`. Leaves implement `Origin*`. Cache, prefetch, backpressure, delayed seek and telemetry live in the bases.

Typical wires:

- `Producer` → `Consumer` (same ring).
- `Producer` → `Pipeline::Process` → `Consumer`.
- That `Consumer` → `Bridge::Passthrough` → file (or wrap the Bridge in a `Pumper`).
- `BufferedFileReader` / `BufferedFileWriter` for seekable files with a page map.

See [Pipeline](#pipeline), [Bridge](#bridge), [Pumper](#pumper), [Telemetry](#telemetry), [IO::BufferedReader](#iobufferedreader) and [IO::BufferedWriter](#iobufferedwriter).

## What this module does

- **BinaryData** — octet payloads are `StormByte::BinaryData` (Base). Lengths of byte buffers are `StormByte::ByteSize`. `Hopper` and `Sink` count items with `StormByte::Size`.
- **FIFO** — grow-on-demand byte buffer. Not thread-safe. `Read` / `Peek` keep data; `Extract` consumes it.
- **SharedFIFO** — thread-safe FIFO. `Read` / `Extract` block until data or `Close` / `SetError`.
- **Ring** — concurrent ring (many-to-many).
- **Producer / Consumer** — write-only / read-only handles over a shared `Ring`.
- **Hopper / Sink** — SPSC typed items and a keyed map of hoppers.
- **Pipeline** — user leaves of `Pipe`. Stream buffers only. `Add` clones or moves.
- **Bridge** — manual transfer. `Passthrough(n, Operation)` only. No worker.
- **Pumper** — owns a Bridge and pumps until EoF or `Cancel`.
- **Telemetry** — `ReadTelemetry` / `WriteTelemetry` as `const StormByte::Safe::Shared<…>`, derived from Base `StormByte::Telemetry`. Named Base clocks measure operation rates. `MeanRate` is caller rate, not disk rate.
- **IO** — `BufferedReader` / `BufferedWriter` bases and file leaves. Nested `Parameters` and knobs.
- **Lifecycle** — `Close()`, `SetError()`, `EoF()`, `IsReadable()`, `IsWritable()`.

## The rest of the suite

| Module | Role | API |
| --- | --- | --- |
| [Base](https://github.com/StormByte-Suite/StormByte) | Exceptions, Expected, serialization, UUID, concepts | [/StormByte](http://suite.stormbyte.org/StormByte) |
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
- [Usage](#usage)
  - [FIFO](#fifo)
  - [Producer and Consumer](#producer-and-consumer)
  - [Hopper](#hopper)
  - [Sink](#sink)
  - [Parameters](#parameters)
  - [Telemetry](#telemetry)
  - [Pipeline](#pipeline)
  - [Bridge](#bridge)
  - [Pumper](#pumper)
  - [IO::BufferedReader](#iobufferedreader)
  - [IO::BufferedWriter](#iobufferedwriter)
  - [BufferedFileReader / BufferedFileWriter](#bufferedfilereader--bufferedfilewriter)
- [Support](#support)
- [Contributing](#contributing)
- [License](#license)

## Documentation

- This README: how to build, ownership, examples.
- Doxygen class reference: [http://suite.stormbyte.org/StormByte-Buffer/](http://suite.stormbyte.org/StormByte-Buffer/).

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

## Usage

Namespace root is `StormByte::Buffer`. I/O types live in `StormByte::Buffer::IO`. Octet payloads use `StormByte::BinaryData`. Byte lengths use `StormByte::ByteSize`.

### FIFO

```cpp
#include <StormByte/buffer/fifo.hxx>

using StormByte::BinaryData;
using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;

int main() {
	FIFO fifo;
	fifo.Write("Hello World");

	BinaryData data;
	fifo.Read(5, data); // "Hello", still in the buffer
	fifo.Seek(6, Position::Absolute);

	BinaryData extracted;
	fifo.Extract(5, extracted); // "World"
}
```

`FIFO` is not thread-safe. Concurrent ends use `SharedFIFO` or `Ring`.

### Producer and Consumer

A `Producer` yields a `Consumer` over the same ring. The `Consumer` is a `ReadOnly`; the `Producer` is a `WriteOnly`. Either tip can be passed to a [Bridge](#bridge).

```cpp
#include <StormByte/buffer/consumer.hxx>
#include <StormByte/buffer/producer.hxx>
#include <thread>

using StormByte::BinaryData;
using StormByte::Buffer::Consumer;
using StormByte::Buffer::Producer;

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
			BinaryData data;
			if (consumer.Extract(0, data) && !data.empty()) {
				// process
			}
		}
	});

	writer.join();
	reader.join();
}
```

### Hopper

`Hopper<T>` is an SPSC queue for `StormByte::Type::MoveConstructible` items. Capacity `0` is unbounded. `Push` blocks when a bounded hopper is full. `Eof()` ends production. Smart-pointer items discard nulls.

`Notify(cv)` stores a pointer the Hopper does **not** own. Call `Unnotify()` before that CV dies.

```cpp
#include <StormByte/buffer/hopper.hxx>
#include <memory>
#include <thread>

using StormByte::Buffer::Hopper;

int main() {
	Hopper<std::unique_ptr<int>> hopper(5);

	std::thread producer([&hopper]() {
		for (int i = 0; i < 10; ++i)
			hopper << std::make_unique<int>(i);
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

`Sink<T>` maps integer keys to `Hopper<T>` buckets. Wire with `To(key)` / `>>` / `<<`.

`Keys()` returns an independent, ascending `StormByte::Safe::Vector<int>` snapshot that remains valid after the Sink is destroyed. Copies are independent; keep Base and the snapshot's creator module loaded until all copies are released.

Sink coordinators, shared Hopper owners and internal collection storage use Base's heap. Hopper queue storage also uses Base's heap for normally aligned items; over-aligned items retain their standard allocator because Base's raw heap API does not provide extended alignment. This controls allocation and release across DLL boundaries, but does not make arbitrary item types or different STL/compiler ABIs compatible. Use boundary-safe payloads when wiring across modules; over-aligned payloads still require a shared allocation runtime. `Select` is a borrowed `std::function`, invoked synchronously and never retained; its caller and callee need a compatible ABI. Conditions passed to `Notify` are borrowed: call `Unnotify` before destroying them.

```cpp
#include <StormByte/buffer/sink.hxx>
#include <memory>
#include <string>
#include <thread>

using StormByte::Buffer::Sink;

int main() {
	Sink<std::shared_ptr<std::string>> producer_sink;
	Sink<std::shared_ptr<std::string>> consumer_sink;

	producer_sink.To(1, consumer_sink);
	producer_sink.To(2, consumer_sink);

	std::thread writer([&producer_sink]() {
		producer_sink.Push(1, std::make_shared<std::string>("ch1"));
		producer_sink.Push(2, std::make_shared<std::string>("ch2"));
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

### Parameters

IO leaves and `Pumper` take a nested `Parameters` object. Omitted knobs keep the office default. Brace-init and a named bag are both valid. The variadic list is applied in **your** TU (`STORMBYTE_FORCE_INLINE`); the DLL sees numbers.

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

Flatten with `operator StormByte::Safe::String` or `operator std::string()` (the latter is `FORCE_INLINE` so the `std::string` lives in your TU):

```cpp
auto tel = reader.Telemetry();
if (tel)
	log << Level::Info << *tel << std::endl;
```

### Pipeline

`Pipeline` transforms **stream** buffers (`ReadOnly` / `WriteOnly`). It does not take IO leaves. A file or device is attached later with a [Bridge](#bridge).

A pipe is a user leaf of `Pipe`. Implement `Run`, `Clone` and `Move`. `Add(const Pipe&)` clones onto Base's heap and does not touch the caller object. `Add(Pipe&&)` takes `Move()`.

`Pipe::Run(ReadOnly&, WriteOnly&, const Shared<Logger::Log>&)`. Close or `SetError` the sink before return. `Pipeline::Process(buffer, log, mode)` — mode last. When a logger is set, each pipe receives a scoped handle.

```cpp
#include <StormByte/buffer/pipe.hxx>
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/buffer/producer.hxx>
#include <StormByte/safe/pointers.hxx>

using StormByte::Buffer::Consumer;
using StormByte::Buffer::ExecutionMode;
using StormByte::Buffer::Pipe;
using StormByte::Buffer::Pipeline;
using StormByte::Buffer::Producer;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::WriteOnly;

class StripCrPipe final: public Pipe {
	public:
		StripCrPipe() = default;
		StripCrPipe(const StripCrPipe&) = default;
		StripCrPipe(StripCrPipe&&) noexcept = default;
		~StripCrPipe() noexcept override = default;
		StripCrPipe& operator=(const StripCrPipe&) = default;
		StripCrPipe& operator=(StripCrPipe&&) noexcept = default;

		void Run(ReadOnly& in, WriteOnly& out,
			const StormByte::Safe::Shared<StormByte::Logger::Log>&) override {
			StormByte::BinaryData raw;
			in.Extract(0, raw);
			StormByte::BinaryData unix_newlines;
			unix_newlines.reserve(raw.size());
			for (const std::byte octet : raw) {
				if (octet != std::byte{'\r'})
					unix_newlines.push_back(octet);
			}
			out.Write(unix_newlines.size(), std::move(unix_newlines));
			out.Close();
		}

		PointerType Clone() const noexcept override {
			return StormByte::Safe::Unique<Pipe>::MakePointer<StripCrPipe>(*this);
		}

		PointerType Move() noexcept override {
			return StormByte::Safe::Unique<Pipe>::MakePointer<StripCrPipe>(std::move(*this));
		}
};

int main() {
	Producer src;
	src.Write("a\r\nb\r\n");
	src.Close();

	StripCrPipe crlf;
	Pipeline pipe;
	pipe.Add(crlf);

	Consumer result = pipe.Process(src.Consumer(), {}, ExecutionMode::Sync);
	(void)result;
}
```

`Sync` runs on the caller thread. `Async` returns at once. `Parallel` is one thread per pipe. Combine with `|`.

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

A `Pipeline` is not a Bridge tip. `Pipeline::Process` is. It returns a `Consumer`, and a `Consumer` is `ReadOnly`, so it can be the source of a Bridge. The leaf `StripCrPipe` is the one from [Pipeline](#pipeline).

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

	StripCrPipe crlf;
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

`Pumper` takes a `Bridge` by move and starts a worker immediately. The destructor **joins** and may block until the current cycle ends. It does **not** `Cancel`: it lets the worker finish.

- `Chunk` — bytes asked of `Passthrough` each cycle. `0` is automatic chunking, **not** Bridge “current contents”.
- `HighWater` — input cap only. Omitted: `0` if the source is IO, otherwise a backend default (constexpr in the PIMPL). Explicit `0`: no Pumper cap. Use `0` when the IO source already limits itself. Non-IO sources are unbounded by design.
- `Toggle` pauses and resumes. No-op if `Failed` or `Canceled`.
- `Cancel` is terminal (`Canceled`, no restart). It does not set `Failed`. It `Close()`s the owned Bridge so Windows can unlink an IO path.
- `Failed()` is only a real Bridge fault. A moved-from Pumper is empty (`EoF`), not `Failed` and not `Canceled`.
- Telemetry handles are copied from the Bridge at construction. They stay valid after `Cancel`. A `Shared` you already copied stays valid when the Pumper dies.

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

### IO::BufferedReader

Public base for a binary origin. Leaves implement `OriginOpen`, `OriginClose`, `OriginPull`, `OriginCanSeek`, `OriginSeek`, `OriginHasSize`, `OriginSize`. Construction is `Unavailable`; a successful `Open` is `Idle`.

`Available()` is cached bytes at `Tell`. It does not call the origin.

`Seek` is logical. A cache hit does not move the device. `Tell` never lies.

### IO::BufferedWriter

Public base for a binary sink. Leaves implement `OriginOpen`, `OriginClose`, `OriginPush`, `OriginFlush`, `OriginTruncate`. Writes are lazy up to `MaxMemory`. Public `Flush` and the Flush inside `Close` count toward `MeanRate`. Internal drains do not.

### BufferedFileReader / BufferedFileWriter

Final file leaves. Path-only constructors probe the device at `Setup`. Explicit knobs stay explicit.

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

## Support

Questions and bugs: GitHub issues on this repository. Sponsorship: [github.com/sponsors/StormBytePP](https://github.com/sponsors/StormBytePP).

## Contributing

Open an issue before large work. Conventional Commits. Public headers need Doxygen. Do not send patches that reintroduce raw `new` for StormByte pointer types.

## License

Dual license: GNU Lesser General Public License v3.0 or later, or a commercial license from the copyright holder. See [LICENSE](LICENSE), [COPYING.LGPLv3](COPYING.LGPLv3) and <https://www.gnu.org/licenses/lgpl-3.0.html>. Third-party trees under `thirdparty/` keep their own licenses.
