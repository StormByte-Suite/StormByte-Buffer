#include <StormByte/buffer/pipe.hxx>

using namespace StormByte::Buffer;

Pipe::Pipe(const Pipe&) = default;

Pipe::Pipe(Pipe&&) noexcept = default;

Pipe::~Pipe() noexcept = default;

Pipe& Pipe::operator=(const Pipe&) = default;

Pipe& Pipe::operator=(Pipe&&) noexcept = default;
