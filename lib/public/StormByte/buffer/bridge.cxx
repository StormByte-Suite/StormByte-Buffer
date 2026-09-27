#include <StormByte/buffer/bridge.hxx>

#include <chrono>
#include <utility>

using namespace StormByte::Buffer;

namespace {
	StormByte::Shared<StormByte::Buffer::ReadTelemetry> MakeReadTelemetry() {
		return StormByte::Shared<StormByte::Buffer::ReadTelemetry>::MakePointer<StormByte::Buffer::ReadTelemetry>();
	}

	StormByte::Shared<StormByte::Buffer::WriteTelemetry> MakeWriteTelemetry() {
		return StormByte::Shared<StormByte::Buffer::WriteTelemetry>::MakePointer<StormByte::Buffer::WriteTelemetry>();
	}
}

Bridge::Bridge(ReadOnly& in, WriteOnly& out) noexcept:
	m_ext_in(std::make_unique<ExternalBufferReader>(in)),
	m_ext_out(std::make_unique<ExternalBufferWriter>(out)),
	m_owned_read(MakeReadTelemetry()),
	m_owned_write(MakeWriteTelemetry()) {}

Bridge::Bridge(Bridge&& other) noexcept {
	std::lock_guard lock(other.m_mutex);
	m_ext_in = std::move(other.m_ext_in);
	m_ext_out = std::move(other.m_ext_out);
	m_io_in = std::move(other.m_io_in);
	m_io_out = std::move(other.m_io_out);
	m_owned_read = std::move(other.m_owned_read);
	m_owned_write = std::move(other.m_owned_write);
	m_failed = other.m_failed;
	other.m_failed = true;
}

Bridge::~Bridge() noexcept = default;

Bridge& Bridge::operator=(Bridge&& other) noexcept {
	if (this == &other)
		return *this;
	std::scoped_lock lock(m_mutex, other.m_mutex);
	m_ext_in = std::move(other.m_ext_in);
	m_ext_out = std::move(other.m_ext_out);
	m_io_in = std::move(other.m_io_in);
	m_io_out = std::move(other.m_io_out);
	m_owned_read = std::move(other.m_owned_read);
	m_owned_write = std::move(other.m_owned_write);
	m_failed = other.m_failed;
	other.m_failed = true;
	return *this;
}

bool Bridge::EoF() const noexcept {
	std::lock_guard lock(m_mutex);
	if (m_ext_in)
		return m_ext_in->EoF();
	if (m_io_in)
		return m_io_in->EoF();
	return true;
}

bool Bridge::Failed() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_failed;
}

const StormByte::Shared<StormByte::Buffer::ReadTelemetry> Bridge::ReadTelemetry() const noexcept {
	std::lock_guard lock(m_mutex);
	if (m_io_in)
		return m_io_in->Telemetry();
	return m_owned_read;
}

const StormByte::Shared<StormByte::Buffer::WriteTelemetry> Bridge::WriteTelemetry() const noexcept {
	std::lock_guard lock(m_mutex);
	if (m_io_out)
		return m_io_out->Telemetry();
	return m_owned_write;
}

IO::Result Bridge::Pull(const StormByte::ByteSize n, FIFO& dest, const Operation operation) {
	dest.Clear();

	if (m_ext_in) {
		if (!m_ext_in->IsReadable() && m_ext_in->Available() == StormByte::ByteSize{0})
			return { IO::Status::Failed, 0 };

		StormByte::ByteSize want = n;
		if (want == StormByte::ByteSize{0} || operation == Operation::NonBlocking) {
			const StormByte::ByteSize now = m_ext_in->Available();
			if (want == StormByte::ByteSize{0})
				want = now;
			else if (now < want)
				want = now;
		}
		if (want == StormByte::ByteSize{0})
			return { m_ext_in->EoF() ? IO::Status::End : IO::Status::Ok, 0 };

		BinaryData chunk;
		if (!m_ext_in->Extract(want, chunk)) {
			if (m_ext_in->EoF())
				return { IO::Status::End, 0 };
			if (operation == Operation::NonBlocking)
				return { IO::Status::Ok, 0 };
			return { IO::Status::Failed, 0 };
		}
		if (chunk.empty())
			return { m_ext_in->EoF() ? IO::Status::End : IO::Status::Ok, 0 };
		if (!dest.Write(std::move(chunk)))
			return { IO::Status::Failed, 0 };
		return { IO::Status::Ok, dest.Available() };
	}

	if (m_io_in) {
		StormByte::ByteSize want = n;
		if (want == StormByte::ByteSize{0} || operation == Operation::NonBlocking) {
			const StormByte::ByteSize now = m_io_in->Available();
			if (want == StormByte::ByteSize{0})
				want = now;
			else if (now < want)
				want = now;
			if (want == StormByte::ByteSize{0})
				return { m_io_in->EoF() ? IO::Status::End : IO::Status::Ok, 0 };
		}
		return m_io_in->Read(want, dest);
	}

	return { IO::Status::Failed, 0 };
}

IO::Result Bridge::Push(FIFO& src) {
	const StormByte::ByteSize n = src.Available();
	if (n == StormByte::ByteSize{0})
		return { IO::Status::Ok, 0 };

	if (m_ext_out) {
		BinaryData chunk;
		if (!src.Extract(n, chunk))
			return { IO::Status::Failed, 0 };
		if (!m_ext_out->Write(std::move(chunk)))
			return { IO::Status::Failed, 0 };
		return { IO::Status::Ok, n };
	}

	if (m_io_out) {
		for (;;) {
			const IO::Result pushed = m_io_out->Write(src);
			if (pushed.status == IO::Status::TryAgain)
				continue;
			return pushed;
		}
	}

	return { IO::Status::Failed, 0 };
}

StormByte::ByteSize Bridge::Passthrough(const StormByte::ByteSize n, const Operation operation) {
	std::lock_guard lock(m_mutex);
	if (m_failed)
		return StormByte::ByteSize{0};

	const auto started = std::chrono::steady_clock::now();
	FIFO work;
	const IO::Result pulled = Pull(n, work, operation);
	if (pulled.status == IO::Status::Failed || pulled.status == IO::Status::Error) {
		m_failed = true;
		return StormByte::ByteSize{0};
	}

	const StormByte::ByteSize got = work.Available();
	if (got == StormByte::ByteSize{0})
		return StormByte::ByteSize{0};

	const IO::Result pushed = Push(work);
	if (pushed.status != IO::Status::Ok) {
		m_failed = true;
		return StormByte::ByteSize{0};
	}

	const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
		std::chrono::steady_clock::now() - started);
	if (m_owned_read)
		m_owned_read->DeltaOperation(got, elapsed);
	if (m_owned_write)
		m_owned_write->DeltaOperation(got, elapsed);
	return got;
}
