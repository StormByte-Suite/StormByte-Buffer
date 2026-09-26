# StormByte-Buffer

![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20Windows%20%7C%20macOS-lightgrey)
![C++26](https://img.shields.io/badge/C%2B%2B-26-00599C?logo=c%2B%2B&logoColor=white)
![CMake](https://img.shields.io/badge/CMake-3.28+-064F8C?logo=cmake&logoColor=white)
![License: LGPL v3 or commercial](https://img.shields.io/badge/License-LGPL_v3_or_commercial-blue.svg)
[![CI](https://github.com/StormBytePP/StormByte-Buffer/actions/workflows/ci.yml/badge.svg)](https://github.com/StormBytePP/StormByte-Buffer/actions/workflows/ci.yml)
[![Sponsor](https://img.shields.io/badge/Sponsor-StormBytePP-ea4aaa?logo=githubsponsors)](https://github.com/sponsors/StormBytePP)

This repository is **StormByte Buffer**: FIFO, SharedFIFO, Ring, Producer/Consumer, Hopper, Sink, pipelines, Bridge and buffered I/O for the StormByte C++ suite.

It depends on [StormByte-String 1.0.0](https://github.com/StormBytePP/StormByte-String/releases/tag/1.0.0) or newer, which vendors [StormByte Base 2.0.0](https://github.com/StormBytePP/StormByte/releases/tag/2.0.0) or newer, [StormByte-System 2.0.0](https://github.com/StormBytePP/StormByte-System/releases/tag/2.0.0) or newer, and optionally [StormByte-Logger 2.0.0](https://github.com/StormBytePP/StormByte-Logger/releases/tag/2.0.0) or newer for pipeline stages (`Scope`). Public headers live under `StormByte/buffer/`.

The suite is split on purpose. Base, Config, Crypto, Database, Logger, Multimedia, Network, String and System are **other repositories**. This one does not implement them.

## Designed to interconnect

The point of this library is that its pieces plug into each other. A `Producer` is a `Consumer`. A `Bridge` pulls from any `ExternalReader` and pushes into any `ExternalWriter`. A `BufferedFileReader` *is* a `BufferedReader`; a `BufferedFileWriter` *is* a `BufferedWriter`. Leaves only implement origin hooks. Caches, prefetch, backpressure, delayed seek and `Telemetry` live in the bases.

Typical wires:

- `Producer` → `Consumer` (same ring).
- `Bridge(Consumer, BufferedFileWriter)` — drain a ring to a file.
- `BufferedFileReader` as a `BufferedReader` — sequential or seekable reads with a page map and delayed seek.
- `BufferedFileWriter` as a `BufferedWriter` — sequential or seekable writes with lazy pages and delayed seek.
- Future leaves that act as a file inherit `BufferedLocationReader` / `BufferedLocationWriter`. A plain byte stream inherits `BufferedReader` / `BufferedWriter`. `BufferedFileReader` and `BufferedFileWriter` are final.

See [Bridge](#bridge), [IO::BufferedReader](#iobufferedreader), [IO::BufferedWriter](#iobufferedwriter) and [BufferedFileReader / BufferedFileWriter](#bufferedfilereader--bufferedfilewriter).

## What this module does

- **BinaryData** — octet payloads are `StormByte::BinaryData` (Base). Buffer does not define its own byte container. Lengths of byte buffers are `StormByte::ByteSize`. `Hopper` and `Sink` count items, so they stay on `StormByte::Size`. Do not pass `std::vector<std::byte>` across a DLL boundary.
- **FIFO** — grow-on-demand byte buffer. Not thread-safe. `Read` / `Peek` keep data; `Extract` consumes it.
- **SharedFIFO** — thread-safe FIFO. `Read` / `Extract` block until data or `Close` / `SetError`.
- **Ring** — concurrent ring (many-to-many).
- **Producer / Consumer** — write-only / read-only handles over a shared `Ring`. See [Producer and Consumer](#producer-and-consumer).
- **Hopper** — SPSC queue of typed items with optional capacity. See [Hopper](#hopper).
- **Sink** — map of integer keys to Hopper buckets. See [Sink](#sink).
- **Bridge** — chunked passthrough `ExternalReader` → `ExternalWriter`, with optional high-water. See [Bridge](#bridge).
- **IO::BufferedReader / IO::BufferedWriter** — session bases (`Open` / `Close` / `Tell` / `Seek` / `Telemetry`). Leaves implement `Origin*`. Page cache + delayed seek live here. See [IO::BufferedReader](#iobufferedreader) and [IO::BufferedWriter](#iobufferedwriter).
- **BufferedFileReader / BufferedFileWriter** — final file leaves of `BufferedLocationReader` / `BufferedLocationWriter`. Path-only constructors pick device-tuned windows at `Open` via `Device()`; explicit constructors keep the knobs you pass and forward them. See [BufferedFileReader / BufferedFileWriter](#bufferedfilereader--bufferedfilewriter).
- **Pipeline** — stages chained with `ExecutionMode`. See [Pipeline](#pipeline).
- **Lifecycle** — `Close()`, `SetError()`, `EoF()`, `IsReadable()`, `IsWritable()`.

## The rest of the suite

| Module | Role | API |
| --- | --- | --- |
| [Base](https://github.com/StormBytePP/StormByte) | Exceptions, Expected, serialization, UUID, concepts | [/StormByte](https://dev.stormbyte.org/StormByte) |
| **Buffer** | This repository | [/StormByte-Buffer](https://dev.stormbyte.org/StormByte-Buffer) |
| [Config](https://github.com/StormBytePP/StormByte-Config) | Human-readable text and versioned binary documents (groups, lists, raw bytes) | [/StormByte-Config](https://dev.stormbyte.org/StormByte-Config) |
| [Crypto](https://github.com/StormBytePP/StormByte-Crypto) | Hash, compress, encrypt, sign and key agreement — Crypto++ never leaves the public API | [/StormByte-Crypto](https://dev.stormbyte.org/StormByte-Crypto) |
| [Database](https://github.com/StormBytePP/StormByte-Database) | One API over SQLite, PostgreSQL and MariaDB | [/StormByte-Database](https://dev.stormbyte.org/StormByte-Database) |
| [Logger](https://github.com/StormBytePP/StormByte-Logger) | Stream logger with levels, headers, hierarchical components and `Scope` | [/StormByte-Logger](https://dev.stormbyte.org/StormByte-Logger) |
| [Multimedia](https://github.com/StormBytePP/StormByte-Multimedia) | Decode, encode and containers without raw FFmpeg types; codecs enabled only if present | [/StormByte-Multimedia](https://dev.stormbyte.org/StormByte-Multimedia) |
| [Network](https://github.com/StormBytePP/StormByte-Network) | Framed packets, Client/Server, IPv4/IPv6 TCP and Buffer pipelines (compress/encrypt) | [/StormByte-Network](https://dev.stormbyte.org/StormByte-Network) |
| [String](https://github.com/StormBytePP/StormByte-String) | Owned UTF-8 / wide text that can cross a DLL boundary (`String`, `WString`, `CString`, `WCString`) | [/StormByte-String](https://dev.stormbyte.org/StormByte-String) |
| [System](https://github.com/StormBytePP/StormByte-System) | Processes, pipes, `Device`, host and environment across Linux, Windows and macOS | [/StormByte-System](https://dev.stormbyte.org/StormByte-System) |

## Table of Contents

- [Designed to interconnect](#designed-to-interconnect)
- [What this module does](#what-this-module-does)
- [The rest of the suite](#the-rest-of-the-suite)
- [Installation](#installation)
- [Usage](#usage)
  - [FIFO](#fifo)
  - [Producer and Consumer](#producer-and-consumer)
  - [Hopper](#hopper)
  - [Sink](#sink)
  - [Bridge](#bridge)
  - [IO::BufferedReader](#iobufferedreader)
  - [IO::BufferedWriter](#iobufferedwriter)
  - [BufferedFileReader / BufferedFileWriter](#bufferedfilereader--bufferedfilewriter)
  - [Pipeline](#pipeline)
- [Support](#support)
- [Contributing](#contributing)
- [License](#license)

## Installation

Needs a C++26 compiler, CMake 3.28 or newer, [StormByte-String 1.0.0](https://github.com/StormBytePP/StormByte-String/releases/tag/1.0.0) or newer (which vendors [StormByte Base 2.0.0](https://github.com/StormBytePP/StormByte/releases/tag/2.0.0) or newer), [StormByte-System 2.0.0](https://github.com/StormBytePP/StormByte-System/releases/tag/2.0.0) or newer, and optionally [StormByte-Logger 2.0.0](https://github.com/StormBytePP/StormByte-Logger/releases/tag/2.0.0) or newer when pipeline stages take a logger.

```sh
git clone --recursive https://github.com/StormBytePP/StormByte-Buffer.git
cd StormByte-Buffer
cmake -S . -B build
cmake --build build
```

## Usage

Headers are `#include <StormByte/buffer/….hxx>`. Namespace root is `StormByte::Buffer`. I/O types live in `StormByte::Buffer::IO`.

Octet payloads use `StormByte::BinaryData`. `size()` is `StormByte::ByteSize`. Text that leaves the module (`HexDump`) is `StormByte::String::String`. `Hopper<T>` and `Sink<T>` count items with `StormByte::Size`.

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
	auto res = fifo.Read(5, data); // "Hello", still in the buffer
	fifo.Seek(6, Position::Absolute);

	BinaryData extracted;
	auto gone = fifo.Extract(5, extracted); // "World"
}
```

`FIFO` is not thread-safe. Concurrent writers/readers use `SharedFIFO` or `Ring`.

### Producer and Consumer

Prefer these over touching `SharedFIFO` / `Ring` by hand. A `Producer` yields a `Consumer` over the same ring; that `Consumer` is an `ExternalReader` and can feed a [Bridge](#bridge).

```cpp
#include <StormByte/buffer/producer.hxx>
#include <StormByte/buffer/consumer.hxx>
#include <thread>

using StormByte::Buffer::Consumer;
using StormByte::BinaryData;
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

`Hopper<T>` is a single-producer single-consumer (SPSC) queue for discrete typed items (`StormByte::Type::MoveConstructible T`). Capacity `0` is unbounded; `Push` blocks when a bounded hopper is full. `Eof()` ends production. Smart pointer types (`StormByte::Type::SmartPointer<T>`) discard null items on `Push`.

`Push` and `Pop` are the stable API. `hopper << item`, `hopper >> item` and `item >> hopper` do the same thing.

`Notify(cv)` stores a pointer to a condition variable the Hopper does **not** own. The Hopper outlives a typical consumer. Call `Unnotify()` before that CV is destroyed, otherwise a later producer `Eof` can signal a freed object.

```cpp
#include <StormByte/buffer/hopper.hxx>
#include <thread>
#include <memory>
#include <iostream>

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
			if (item)
				std::cout << "Popped: " << *item << "\n";
		}
	});

	producer.join();
	consumer.join();
}
```

### Sink

`Sink<T>` maps integer keys to `Hopper<T>` buckets. Wire a consumer with `To(key)` / `>>` / `<<`. `Bind` is the old name and is `[[deprecated]]`.

`Sink::EoF()` contract:

- **Zero hoppers:** `true` only if this `Sink` was closed (`Eof()` or destruction).
- **With hoppers:** `true` when every hopper is empty and `Hopper::EoF()` is `true`, even if this `Sink` did not call `Eof()` (the producer may have closed a shared hopper).
- **Dynamic wire:** attaching a new key after `EoF()` was `true` may make `EoF()` `false` again.
- **Meaning:** no items remain and none can enter the current buckets.

```cpp
#include <StormByte/buffer/sink.hxx>
#include <thread>
#include <memory>
#include <string>
#include <iostream>

using StormByte::Buffer::Sink;

int main() {
	Sink<std::shared_ptr<std::string>> producerSink;
	Sink<std::shared_ptr<std::string>> consumerSink;

	producerSink.To(1, consumerSink);
	producerSink.To(2, consumerSink);

	std::thread writer([&producerSink]() {
		producerSink.Push(1, std::make_shared<std::string>("Message on Channel 1"));
		producerSink.Push(2, std::make_shared<std::string>("Message on Channel 2"));
		producerSink.Eof();
	});

	std::thread reader([&consumerSink]() {
		while (!consumerSink.EoF()) {
			auto msg = consumerSink.Pop();
			if (msg)
				std::cout << "Received: " << *msg << "\n";
		}
	});

	writer.join();
	reader.join();
}
```

### Bridge

`Bridge` copies bytes from an `ExternalReader` to an `ExternalWriter` in chunks. Optional high-water (`0` = no extra cap; the writer may still apply its own backpressure). Use it when the source is a pipe or a ring and the sink is a file or another writer.

```cpp
#include <StormByte/buffer/io/bridge.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/buffer/producer.hxx>

using StormByte::Buffer::Producer;
using StormByte::Buffer::IO::Bridge;
using StormByte::Buffer::IO::BufferedFileWriter;

int main() {
	Producer producer;
	BufferedFileWriter out("out.bin");
	out.Open();

	Bridge bridge(producer.Consumer(), out, /*high_water=*/0);
	producer.Write("payload");
	producer.Close();
	(void)bridge.Pump();
	out.Close();
}
```

### IO::BufferedReader

Session over a byte origin. The public type owns the page map, prefetch, delayed seek, `Tell` and `Telemetry`. A leaf only implements `OriginOpen` / `OriginClose` / `OriginPull` and, if it can, `OriginCanSeek` / `OriginSeek` / `OriginHasSize` / `OriginSize`.

- `Open` is not idempotent. It calls the leaf `Setup()` once, then the origin.
- `Read` / `Peek` take a `FIFO` or a writable `std::span<std::byte>`.
- `Tell` is the public cursor. It never lies: after `Seek(off)` it is `off`, even when the device has not moved.
- **Delayed seek.** `Seek` updates `Tell` only. `OriginSeek` runs later, and only when a `Read` / `Peek` must fill a hole the page map does not cover. If the whole epoch is served from resident pages, the device never sees that seek.
- **Page cache.** Consumed ranges stay in the map until `MaxMemory` and GC evict the spans farthest from `Tell`. A later seek back into a resident page is a cache hit. `MaxMemory == 0` stores nothing; every miss hits the origin.
- Prefetch is gated while the logical cursor and the device cursor differ. It resumes when they meet again. Prefetch bytes are not `Delivered`.
- Seek is **not** guaranteed O(1). A jump inside resident pages is a map lookup. A cold miss is an origin seek plus a pull.
- Not seekable: one forward span; `Seek` returns `Failed` and does not call the hook.

`Telemetry()` returns a snapshot. Useful fields:

| Field | Meaning |
| --- | --- |
| `Delivered` | Bytes given to the caller (`Read`, not `Peek`, not prefetch). |
| `HitAhead` / `HitBack` | Delivered from the map, forward of / behind the previous origin cursor. |
| `Miss` | Delivered from the origin. `Delivered == HitAhead + HitBack + Miss`. |
| `Cached` / `CachedPeak` / `Cap` | Resident map and `MaxMemory`. |
| `SeekLogical` / `SeekOrigin` | Public seeks vs device seeks. |
| `SeekSavedFull` / `SeekSavedPartial` | Epoch closed with no origin seek / with a later origin seek. |
| `Evicted` | Pages dropped by GC. |

Give the reader enough `MaxMemory` for the working set you actually rewind into. Distant random seeks will miss; that is the contract, not a bug.

### IO::BufferedWriter

Session over a byte sink. The public type owns the page map, the optional drain ring, delayed seek, `Dirty`, `Flush`, `Truncate` and `Telemetry`. A leaf implements `OriginOpen` / `OriginClose` / `OriginPush` / `OriginSeek` and optionally `OriginFlush` / `OriginTruncate`.

- `Write` is atomic. `TryAgain` means backpressure; the source is left untouched.
- `WriteChunk == 0` and `BackPressure == 0` is direct: each `Write` hits the origin and `Dirty()` stays `0`.
- `WriteChunk > 0` and `BackPressure > 0` use an SPSC ring capped at `BackPressure * WriteChunk`. Overflow is `TryAgain`.
- `MaxMemory > 0` keeps dirty **pages**. Writes stay off the device until GC, `Flush` or `Close`. Overlapping and abutting spans coalesce. `MaxMemory == 0` stores no pages.
- `BackPressure` is in **chunks**, not bytes.
- **Delayed seek.** `Seek` updates `Tell` only. `OriginSeek` runs when a page is materialized or when the ring must align before a drain. Typical workload: local corrections (seek a short way back, patch, seek to the front). That path is meant to stay in RAM when `MaxMemory` allows it.
- **GC.** When dirty pages exceed `MaxMemory`, the farthest-past page is materialized first, then future islands. An eviction is a real origin seek + write. Far random writes need more `MaxMemory` or they become device seeks.
- **Holes.** Seeking past the durable end and writing an island leaves zeros in the gap when that range is materialized. That is the filesystem contract, not a fill loop in the caller.
- `Flush` and `Close` force every dirty page and the ring onto the origin. After either, `Dirty() == 0` and `Telemetry().Materialized == Telemetry().HighWater`.
- Seek is **not** guaranteed O(1). A jump that is still dirty in the map does not touch the device. Evicting a non-local island does.

`Telemetry()` snapshot:

| Field | Meaning |
| --- | --- |
| `Accepted` | Bytes taken by `Write` since `Open`. |
| `Behind` / `Direct` | Accepted into pages/ring vs written immediately. |
| `Origin` | Bytes actually pushed to the device (overwrites count again). |
| `Materialized` | Highest origin offset known to hold data (holes included once flushed past them). |
| `HighWater` | Highest logical cursor since `Open` / `Truncate`. |
| `HitAhead` / `HitBack` / `Miss` | Page-map classification of accepted bytes. Zero on the ring-only / direct paths. |
| `Dirty` / `DirtyPeak` / `Cap` | Pending bytes and ring cap. |
| `SeekLogical` / `SeekOrigin` / `SeekSavedFull` / `SeekSavedPartial` | Same epoch rules as the reader. |
| `Evicted` | Pages forced to the origin by GC. |

Durable progress is `Materialized / HighWater`, not `Accepted / HighWater` and not `Tell`. `Accepted` can run ahead while pages are still dirty. After `Close`, the two lengths match.

This cache is not magic. Local seeks and short rewinds with a budget that fits the dirty working set stay off the device. A seek several gigabytes ahead, a tiny write, and a seek back only stays cheap if that island still fits in `MaxMemory`. Document that to the caller: more random access needs more RAM.

### BufferedFileReader / BufferedFileWriter

File leaves of `BufferedLocationReader` / `BufferedLocationWriter`. Those are the file-like layer (`Device()`, always seekable and sized). The file classes are `final`: they only open, transfer and seek the filesystem file. Constructors take a `StormByte::String::String` and pass `Location::Local`. `Path()` is that local path (`const String&`). `Location()` is `Local`. Both are stored once on `BufferedReader` / `BufferedWriter` and do not change. A socket that inherits the lower layer can pass `Location::Remote` and a `socket://` or `http://` path.

| Constructor | What happens at `Open` |
| --- | --- |
| `BufferedFileReader(path)` / `BufferedFileWriter(path)` | `Setup()` on the location layer uses `Device()` and `Device::Window` to set `ReadAhead` (reader) or `WriteChunk` + `BackPressure` (writer). Path-only `MaxMemory` is 1 MiB. Path-only writer `BackPressure` is 4. |
| `BufferedFileReader(path, read_ahead, max_memory)` | Those values are forwarded and stay. `(path, 0, 0)` is no prefetch / no pages. |
| `BufferedFileWriter(path, write_chunk, back_pressure)` | Ring knobs are forwarded and stay. `MaxMemory` stays 0 (no pages). |
| `BufferedFileWriter(path, write_chunk, max_memory, back_pressure)` | Pages and ring together. Either ring knob `0` disables the ring. `MaxMemory == 0` disables pages. |

The device does not change after construction. `MaxMemory` and `MaxWait` stay settable: they are cache and wait policy, not device speed.

`Setup()` is sealed on the location layer and runs from `Open` before `OriginOpen`. An explicit constructor skips the probe.

`Open` on an existing file does not truncate. `Truncate` overwrites. A second `Open` after `Close` starts `Tell` at 0; seek to `Size()` to append.

```cpp
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>

using StormByte::Buffer::FIFO;
using StormByte::Buffer::Position;
using StormByte::Buffer::IO::BufferedFileReader;
using StormByte::Buffer::IO::BufferedFileWriter;

int main() {
	BufferedFileReader in("in.bin");
	in.Open();
	FIFO dest;
	(void)in.Read(16, dest);
	(void)in.Seek(0, Position::Absolute); // Tell is 0; origin moves only on miss
	(void)in.Read(16, dest);              // served from the page map when MaxMemory allows
	const struct BufferedFileReader::Telemetry rt = in.Telemetry();
	(void)rt.Delivered;

	BufferedFileWriter out("out.bin", StormByte::ByteSize{4096},
		StormByte::ByteSize{256ull * 1024ull}, 4);
	out.Open();
	(void)out.Write(dest);
	(void)out.Seek(0, Position::Absolute);
	(void)out.Write(dest); // patch stays dirty until Flush / Close / GC
	(void)out.Flush();
	const struct BufferedFileWriter::Telemetry wt = out.Telemetry();
	(void)wt.Materialized;
	out.Close();
}
```

### Pipeline

Stages receive `ExternalReader&`, `ExternalWriter&` and an optional `std::shared_ptr<Logger::Log>`. They must `out.Close()` or `out.SetError()`.

When `Process` gets a non-null logger it passes `log->Scope("Buffer/Pipeline")` to every stage. `%c` is then `Buffer/Pipeline`. Do not pre-scope `Buffer/Pipeline` on the argument. A stage that needs a leaf can `log->Scope("Decode")`.

```cpp
#include <StormByte/buffer/pipeline.hxx>
#include <StormByte/buffer/external.hxx>
#include <StormByte/logger/log.hxx>
#include <cctype>
#include <memory>

using StormByte::BinaryData;
using StormByte::Buffer::Pipeline;
using StormByte::Buffer::Producer;
using StormByte::Buffer::ExternalReader;
using StormByte::Buffer::ExternalWriter;
using StormByte::Logger::Log;
using StormByte::Logger::Level;

int main() {
	auto log = std::make_shared<Log>(std::cout, Level::Info, "[%L] %c");
	Pipeline pipeline;
	pipeline.AddPipe([](ExternalReader& in, ExternalWriter& out,
						std::shared_ptr<Log> log) {
		while (!in.EoF()) {
			BinaryData data;
			if (in.Extract(0, data) && !data.empty()) {
				std::string str(reinterpret_cast<const char*>(data.data()),
					static_cast<std::size_t>(data.size()));
				for (auto& c : str)
					c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
				(void)out.Write(str);
			}
		}
		out.Close();
	});

	Producer input;
	(void)input.Write("hello");
	input.Close();
	auto out = pipeline.Process(input.Consumer(),
		StormByte::Buffer::ExecutionMode::Sync, log);
}
```

`ExecutionMode`: `Sync` (caller thread), `Async` (background), `Parallel` (one thread per stage). Flags combine (`Async | Parallel`).

## Support

StormByte is developed in spare time. Sponsorship is optional and does not buy features, priority or support.

- [GitHub Sponsors](https://github.com/sponsors/StormBytePP)
- [PayPal](https://paypal.me/StormBytePP)

## Contributing

Issues only on this repository. Fork and open a pull request against `master`.

## License

Dual license: GNU Lesser General Public License v3.0 or later, or a commercial license from the copyright holder. See [LICENSE](LICENSE), [COPYING.LGPLv3](COPYING.LGPLv3) and <https://www.gnu.org/licenses/lgpl-3.0.html>. Third-party trees under `thirdparty/` keep their own licenses.
