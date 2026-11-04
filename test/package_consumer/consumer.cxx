// SPDX-License-Identifier: LGPL-3.0-or-later OR LicenseRef-StormByte-Commercial

#include <StormByte/buffer/fifo.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/system/host.hxx>

#include <climits>
#include <sstream>
#include <string>

int main() {
	StormByte::Buffer::FIFO fifo;
	if (!fifo.Write("hello") || fifo.Available() != StormByte::ByteSize{5})
		return 1;

	StormByte::Safe::Binary contents;
	if (!fifo.Extract(5, contents) || contents.size() != StormByte::ByteSize{5})
		return 2;

	std::ostringstream output;
	StormByte::Logger::Log log(output);
	log << "buffer consumer" << std::endl;
	if (output.str().find("buffer consumer") == std::string::npos)
		return 3;

	const StormByte::System::Host::CPU cpu;
	return cpu.Bitness() == sizeof(void*) * CHAR_BIT ? 0 : 4;
}
