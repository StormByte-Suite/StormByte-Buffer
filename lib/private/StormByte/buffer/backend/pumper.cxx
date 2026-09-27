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
	if (high_water.has_value())
		m_high_water = *high_water;
	else
		m_high_water = m_bridge.InputIsIO() ? StormByte::ByteSize{0} : kDefaultNonIoHighWater;
	m_worker = std::thread(&Pumper::Worker, this);
}

Pumper::~Pumper() {
	m_stop.store(true);
	m_cv.notify_all();
	if (m_worker.joinable())
		m_worker.join();
}

bool Pumper::EoF() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_bridge.EoF();
}

bool Pumper::Failed() const noexcept {
	std::lock_guard lock(m_mutex);
	return m_failed || m_bridge.Failed();
}

void Pumper::Toggle() noexcept {
	std::lock_guard lock(m_mutex);
	if (m_failed || m_bridge.Failed())
		return;
	m_paused = !m_paused;
	m_cv.notify_all();
}

void Pumper::Cancel() noexcept {
	std::lock_guard lock(m_mutex);
	m_failed = true;
	m_stop.store(true);
	m_cv.notify_all();
}

const StormByte::Shared<StormByte::Buffer::ReadTelemetry> Pumper::ReadTelemetry() const noexcept {
	return m_bridge.ReadTelemetry();
}

const StormByte::Shared<StormByte::Buffer::WriteTelemetry> Pumper::WriteTelemetry() const noexcept {
	return m_bridge.WriteTelemetry();
}

StormByte::ByteSize Pumper::CycleRequest() const noexcept {
	StormByte::ByteSize want = m_chunk == StormByte::ByteSize{0} ? kAutoChunk : m_chunk;
	if (m_high_water > StormByte::ByteSize{0} && m_high_water < want)
		want = m_high_water;
	return want;
}

void Pumper::Worker() {
	for (;;) {
		{
			std::unique_lock lock(m_mutex);
			m_cv.wait(lock, [this] {
				return m_stop.load() || !m_paused || m_failed || m_bridge.Failed();
			});
			if (m_stop.load() || m_failed || m_bridge.Failed())
				return;
		}

		if (m_bridge.EoF() || m_bridge.Failed()) {
			std::lock_guard lock(m_mutex);
			if (m_bridge.Failed())
				m_failed = true;
			return;
		}

		const StormByte::ByteSize want = CycleRequest();
		const StormByte::ByteSize got = m_bridge.Passthrough(want, StormByte::Buffer::Bridge::Operation::Blocking);
		if (m_bridge.Failed()) {
			std::lock_guard lock(m_mutex);
			m_failed = true;
			return;
		}
		if (got == StormByte::ByteSize{0} && m_bridge.EoF())
			return;
	}
}
