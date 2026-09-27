# StormByte-Buffer

![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS-lightgrey)
![C++26](https://img.shields.io/badge/C%2B%2B-26-00599C?logo=c%2B%2B&logoColor=white)
![CMake](https://img.shields.io/badge/CMake-3.28+-064F8C?logo=cmake&logoColor=white)
![License: LGPL v3 or commercial](https://img.shields.io/badge/License-LGPL_v3_or_commercial-blue.svg)
[![CI](https://github.com/StormBytePP/StormByte-Buffer/actions/workflows/ci.yml/badge.svg)](https://github.com/StormBytePP/StormByte-Buffer/actions/workflows/ci.yml)
[![Sponsor](https://img.shields.io/badge/Sponsor-StormBytePP-ea4aaa?logo=githubsponsors)](https://github.com/sponsors/StormBytePP)

This repository is **StormByte Buffer**: FIFO, SharedFIFO, Ring, Producer/Consumer, Hopper, Sink, Bridge, Pumper, pipelines and buffered I/O for the StormByte C++ suite.

It depends on [StormByte-String 1.0.0](https://github.com/StormBytePP/StormByte-String/releases/tag/1.0.0) or newer, which vendors [StormByte Base 2.0.0](https://github.com/StormBytePP/StormByte/releases/tag/2.0.0) or newer, [StormByte-System 2.0.0](https://github.com/StormBytePP/StormByte-System/releases/tag/2.0.0) or newer, and optionally [StormByte-Logger 2.0.0](https://github.com/StormBytePP/StormByte-Logger/releases/tag/2.0.0) or newer for pipeline stages (`Scope`). Public headers live under `StormByte/buffer/`.

The suite is split on purpose. Base, Config, Crypto, Database, Logger, Multimedia, Network, String and System are **other repositories**. This one does not implement them.

## Designed to interconnect

Pieces plug into each other through `ReadOnly` / `WriteOnly` and through IO leaves.

- `Producer` yields a `Consumer` over the same `Ring`.
- `Bridge` moves bytes from a `ReadOnly` or an IO reader into a `WriteOnly` or an IO writer. IO tips are taken by move; in-memory tips stay referenced.
- `Pumper` owns a `Bridge` and runs `Passthrough` until EoF or failure.
- `BufferedFileReader` *is* a `BufferedReader`. `BufferedFileWriter` *is* a `BufferedWriter`. Leaves implement `Origin*`. Cache, prefetch, backpressure, delayed seek and telemetry live in the bases.
- `Pipeline` stages read a `ReadOnly` and write a `WriteOnly`. Intermediates are a private ring; the last stage writes a `Producer`.

Typical wires:

- `Producer` → `Consumer` (same ring).
- `Bridge(consumer, std::move(file_writer))` then `Passthrough`, or wrap that Bridge in a `Pumper`.
- `BufferedFileReader` — sequential or seekable reads with a page map.
- `BufferedFileWriter` — sequential or seekable writes with lazy dirty pages.

See [Bridge](#bridge), [Pumper](#pumper), [Telemetry](#telemetry), [IO::BufferedReader](#iobufferedreader), [IO::BufferedWriter](#iobufferedwriter) and [Pipeline](#pipeline).

## What this module does

- **BinaryData** — octet payloads are `StormByte::BinaryData` (Base). Lengths of byte buffers are `StormByte::ByteSize`. `Hopper` and `Sink` count items with `StormByte::Size`.
- **FIFO** — grow-on-demand byte buffer. Not thread-safe. `Read` / `Peek` keep data; `Extract` consumes it.
- **SharedFIFO** — thread-safe FIFO. `Read` / `Extract` block until data or `Close` / `SetError`.
- **Ring** — concurrent ring (many-to-many).
- **Producer / Consumer** — write-only / read-only handles over a shared `Ring`.
- **Hopper / Sink** — SPSC typed items and a keyed map of hoppers.
- **Bridge** — manual transfer. `Passthrough(n, Operation)` only. No worker.
- **Pumper** — owns a Bridge and pumps until EoF or `Cancel`.
- **Telemetry** — `ReadTelemetry` / `WriteTelemetry` as `const StormByte::Shared<…>`. `MeanRate` is caller rate, not disk rate.
- **IO** — `BufferedReader` / `BufferedWriter` bases and file leaves. Nested `Parameters` and knobs.
- **Pipeline** — owned `Pipeline::Stage` objects. Callables boxed in the caller TU.
- **Lifecycle** — `Close()`, `SetError()`, `EoF()`, `IsReadable()`, `IsWritable()`.

## The rest of the suite

| Module | Role | API |
| --- | --- | --- |
| [Base](https://github.com/StormBytePP/StormByte) | Exceptions, Expected, serialization, UUID, concepts | [/StormByte](https://dev.stormbyte.org/StormByte) |
| **Buffer** | This repository | [/StormByte-Buffer](https://dev.stormbyte.org/StormByte-Buffer) |
| [Config](https://github.com/StormBytePP/StormByte-Config) | Human-readable text and versioned binary documents | [/StormByte-Config](https://dev.stormbyte.org/StormByte-Config) |
| [Crypto](https://github.com/StormBytePP/StormByte-Crypto) | Hash, compress, encrypt, sign — Crypto++ stays private | [/StormByte-Crypto](https://dev.stormbyte.org/StormByte-Crypto) |
| [Database](https://github.com/StormBytePP/StormByte-Database) | One API over SQLite, PostgreSQL and MariaDB | [/StormByte-Database](https://dev.stormbyte.org/StormByte-Database) |
| [Logger](https://github.com/StormBytePP/StormByte-Logger) | Stream logger with levels, headers, components and `Scope` | [/StormByte-Logger](https://dev.stormbyte.org/StormByte-Logger) |
| [Multimedia](https://github.com/StormBytePP/StormByte-Multimedia) | Decode, encode and containers without raw FFmpeg types | [/StormByte-Multimedia](https://dev.stormbyte.org/StormByte-Multimedia) |
| [Network](https://github.com/StormBytePP/StormByte-Network) | Framed packets, Client/Server, IPv4/IPv6 TCP | [/StormByte-Network](https://dev.stormbyte.org/StormByte-Network) |
| [String](https://github.com/StormBytePP/StormByte-String) | Owned UTF-8 / wide text that can cross a DLL boundary | [/StormByte-String](https://dev.stormbyte.org/StormByte-String) |
| [System](https://github.com/StormBytePP/StormByte-System) | Processes, pipes, `Device`, host and environment | [/StormByte-System](https://dev.stormbyte.org/StormByte-System) |

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
  - [Bridge](#bridge)
  - [Pumper](#pumper)
  - [IO::BufferedReader](#iobufferedreader)
  - [IO::BufferedWriter](#iobufferedwriter)
  - [BufferedFileReader / BufferedFileWriter](#bufferedfilereader--bufferedfilewriter)
  - [Pipeline](#pipeline)
- [Support](#support)
- [Contributing](#contributing)
- [License](#license)

## Documentation

- This README: how to build, ownership, examples.
- Doxygen class reference: [https://dev.stormbyte.org/StormByte-Buffer/](https://dev.stormbyte.org/StormByte-Buffer/).

## Installation

Needs a C++26 compiler, CMake 3.28 or newer, [StormByte-String 1.0.0](https://github.com/StormBytePP/StormByte-String/releases/tag/1.0.0) or newer (vendors [StormByte Base 2.0.0](https://github.com/StormBytePP/StormByte/releases/tag/2.0.0)), [StormByte-System 2.0.0](https://github.com/StormBytePP/StormByte-System/releases/tag/2.0.0) or newer, and optionally [StormByte-Logger 2.0.0](https://github.com/StormBytePP/StormByte-Logger/releases/tag/2.0.0) when pipeline stages take a logger.

```sh
git clone --recursive https://github.com/StormBytePP/StormByte-Buffer.git
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

Every IO office and every Bridge exposes `const StormByte::Shared<ReadTelemetry>` / `WriteTelemetry`. The handle is the same object for the life of the office. IO types add cache / origin / seek / wait counters. Non-IO Bridge tips use the basic type.

`MeanRate` is octets per second of **requested user operations**, including cache hits. It is not a disk benchmark. A cached write can look like GiB/s. Worker, GC and internal flushes enter the rate only when they delay the caller. Explicit `Flush` / `Close` Flush pull it back.

Flatten with `operator StormByte::String::String` or `operator std::string()` (the latter is `FORCE_INLINE` so the `std::string` lives in your TU):

```cpp
auto tel = reader.Telemetry();
if (tel)
	log << Level::Info << *tel << std::endl;
```

### Bridge

`Bridge` is a manual transfer. Bytes move only when you call `Passthrough`. There is no worker and no occupancy cap here. Continuous transfer is [Pumper](#pumper).

- In-memory tips: `ReadOnly&` / `WriteOnly&`. Those buffers must outlive the Bridge.
- IO tips: stolen by move as the concrete leaf (`BufferedFileReader`, `BufferedFileWriter`, …).
- `Passthrough(n, Operation)` is atomic. Write `TryAgain` is retried until that call completes.
- `Operation` applies to the **read** tip only: `Blocking` waits for `n` or EoF; `NonBlocking` takes what is available now, up to `n`.
- `n == 0` is current contents (`Available()` on IO does not touch the origin).
- `Failed()` is sticky. A failed Bridge returns `0` and does nothing.
- Telemetry: IO tips forward their Shared handle; in-memory tips use a basic telemetry owned by the Bridge.

```cpp
#include <StormByte/buffer/bridge.hxx>
#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>

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
}
```

### Pumper

`Pumper` takes a `Bridge` by move and starts a worker immediately. The destructor **joins** and may block until the current cycle ends.

- `Chunk` — bytes asked of `Passthrough` each cycle. `0` is automatic chunking, **not** Bridge “current contents”.
- `HighWater` — input cap only. Omitted: `0` if the source is IO, otherwise a backend default (constexpr in the PIMPL). Explicit `0`: no Pumper cap. Use `0` when the IO source already limits itself. Non-IO sources are unbounded by design.
- `Toggle` pauses and resumes. `Cancel` is terminal (`Failed`, no restart).
- Telemetry is forwarded from the owned Bridge.

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
	while (!pump.EoF() && !pump.Failed()) {
		// work elsewhere; pump runs on its thread
	}
	// ~Pumper joins
}
```

`finish = !pump.Failed() && pump.EoF()`. `active = !pump.Failed() && !pump.EoF()`.

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

### Pipeline

Stages are owned `Pipeline::Stage` objects. Copy of a stage value is deleted. `Clone` is private (only Pipeline copy). `AddPipe(Unique<Stage>)` takes ownership. `AddPipe(F&&)` boxes a callable in **your** TU.

`Run(ReadOnly&, WriteOnly&, Logger::Log*)`. Close or `SetError` the sink before return. When a logger is passed to `Process`, each stage receives `Scope("Buffer/Pipeline")`.

```cpp
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/buffer/producer.hxx>

using StormByte::Buffer::Consumer;
using StormByte::Buffer::ExecutionMode;
using StormByte::Buffer::Pipeline;
using StormByte::Buffer::Producer;
using StormByte::Buffer::ReadOnly;
using StormByte::Buffer::WriteOnly;

int main() {
	Producer src;
	src.Write("abcdef");
	src.Close();

	Pipeline pipe;
	pipe.AddPipe([](ReadOnly& in, WriteOnly& out, StormByte::Logger::Log*) {
		StormByte::BinaryData chunk;
		in.Extract(0, chunk);
		out.Write(chunk.size(), std::move(chunk));
		out.Close();
	});

	Consumer result = pipe.Process(src.Consumer(), ExecutionMode::Sync, nullptr);
	(void)result;
}
```

`Sync` runs on the caller thread. `Async` returns at once. `Parallel` is one thread per stage. Combine with `|`.

## Support

Questions and bugs: GitHub issues on this repository. Sponsorship: [github.com/sponsors/StormBytePP](https://github.com/sponsors/StormBytePP).

## Contributing

Open an issue before large work. Conventional Commits. Public headers need Doxygen. Do not send patches that reintroduce raw `new` for StormByte pointer types.

## License

Dual license: GNU Lesser General Public License v3.0 or later, or a commercial license from the copyright holder. See [LICENSE](LICENSE), [COPYING.LGPLv3](COPYING.LGPLv3) and <https://www.gnu.org/licenses/lgpl-3.0.html>. Third-party trees under `thirdparty/` keep their own licenses.
