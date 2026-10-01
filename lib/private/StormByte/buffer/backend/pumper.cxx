#include <StormByte/buffer/backend/pumper.hxx>

using namespace StormByte::Buffer::Backend;

namespace {
	constexpr StormByte::ByteSize kAutoChunk{64 * 1024};
	constexpr StormByte::ByteSize kDefaultNonIoHighWater{1 * 1024 * 1024};
}

Pumper::Pumper(StormByte::Buffer::Bridge&& bridge, const StormByte::ByteSize chunk,
		const std::optional<StormByte::ByteSize> high_water):
	m_bridge(std::move(bridge)),
	m_chunk(chunk) {
	m_read = m_bridge.ReadTelemetry();
	m_write = m_bridge.WriteTelemetry();
	m_io_in_blocking = m_bridge.InputPullBlocking();
	if (high_water.has_value())
		m_high_water = *high_water;
	else
		m_high_water = m_bridge.InputIsIO() ? StormByte::ByteSize{0} : kDefaultNonIoHighWater;
	m_worker = std::thread(&Pumper::Worker, this);
}

Pumper::~Pumper() {
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

void Pumper::Cancel() noexcept {
	std::lock_guard lock(m_mutex);
	if (m_canceled)
		return;
	m_canceled = true;
	m_bridge.Close();
	m_cv.notify_all();
}

bool Pumper::Canceled() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_canceled;
}

bool Pumper::EoF() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_bridge.EoF();
}

bool Pumper::Failed() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_bridge.Failed();
}

void Pumper::Toggle() noexcept {
	std::lock_guard lock(m_mutex);
	if (m_canceled || m_bridge.Failed())
		return;
	m_paused = !m_paused;
	m_cv.notify_all();
}

const StormByte::Safe::Shared<StormByte::Buffer::ReadTelemetry> Pumper::ReadTelemetry() const noexcept {
	return m_read;
}

const StormByte::Safe::Shared<StormByte::Buffer::WriteTelemetry> Pumper::WriteTelemetry() const noexcept {
	return m_write;
}

StormByte::ByteSize Pumper::CycleRequest() const noexcept {
	StormByte::ByteSize want = m_chunk == StormByte::ByteSize{0} ? kAutoChunk : m_chunk;
	if (want == StormByte::ByteSize{0})
		want = kAutoChunk;
	if (m_high_water > StormByte::ByteSize{0} && m_high_water < want)
		want = m_high_water;
	return want;
}

void Pumper::Worker() {
	for (;;) {
		{
			std::unique_lock lock(m_mutex);
			m_cv.wait(lock, [this] {
				return m_canceled || !m_paused || m_bridge.Failed();
			});
			if (m_canceled || m_bridge.Failed())
				return;
			if (m_paused)
				continue;
		}

		if (m_canceled || m_bridge.Failed())
			return;

		const StormByte::ByteSize want = CycleRequest();
		const auto op = m_io_in_blocking
			? StormByte::Buffer::Bridge::Operation::Blocking
			: StormByte::Buffer::Bridge::Operation::NonBlocking;
		const StormByte::ByteSize got = m_bridge.Passthrough(want, op);
		if (m_canceled || m_bridge.Failed())
			return;
		if (m_bridge.EoF())
			return;
		if (got == StormByte::ByteSize{0}) {
			std::unique_lock lock(m_mutex);
			if (m_canceled || m_paused || m_bridge.Failed())
				continue;
			m_cv.wait_for(lock, std::chrono::milliseconds(1), [this] {
				return m_canceled || m_paused || m_bridge.Failed();
			});
		}
	}
}
