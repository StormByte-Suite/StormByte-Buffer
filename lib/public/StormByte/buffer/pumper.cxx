#include <StormByte/buffer/backend/pumper.hxx>
#include <StormByte/buffer/pumper.hxx>

using namespace StormByte::Buffer;

Pumper::Pumper(Bridge&& bridge, const StormByte::ByteSize chunk,
		const std::optional<StormByte::ByteSize> high_water):
	m_backend(std::make_unique<Backend::Pumper>(std::move(bridge), chunk, high_water)) {}

Pumper::Pumper(Pumper&& other) noexcept:
	m_backend(std::move(other.m_backend)) {}

Pumper::~Pumper() noexcept = default;

Pumper& Pumper::operator=(Pumper&& other) noexcept {
	if (this != &other)
		m_backend = std::move(other.m_backend);
	return *this;
}

bool Pumper::EoF() const noexcept {
	return !m_backend || m_backend->EoF();
}

bool Pumper::Failed() const noexcept {
	return !m_backend || m_backend->Failed();
}

void Pumper::Toggle() noexcept {
	if (m_backend)
		m_backend->Toggle();
}

void Pumper::Cancel() noexcept {
	if (m_backend)
		m_backend->Cancel();
}

const StormByte::Shared<StormByte::Buffer::ReadTelemetry> Pumper::ReadTelemetry() const noexcept {
	if (!m_backend)
		return {};
	return m_backend->ReadTelemetry();
}

const StormByte::Shared<StormByte::Buffer::WriteTelemetry> Pumper::WriteTelemetry() const noexcept {
	if (!m_backend)
		return {};
	return m_backend->WriteTelemetry();
}
