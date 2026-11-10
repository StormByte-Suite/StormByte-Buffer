// SPDX-License-Identifier: LGPL-3.0-or-later OR LicenseRef-StormByte-Commercial

#include <StormByte/buffer/fifo.hxx>
#include <StormByte/buffer/io/buffered_file_reader.hxx>
#include <StormByte/buffer/io/buffered_file_writer.hxx>
#include <StormByte/logger/log.hxx>
#include <StormByte/system/host.hxx>

#include <climits>
#include <sstream>
#include <string>

int main() {
	using namespace StormByte::Buffer::IO;
	BufferedFileReader reader("unused", {ReadChunk(4096), ReadAhead(0), MaxMemory(0)});
	BufferedFileWriter writer("unused", {WriteTransfer(8192), WriteThreshold(16384), MaxMemory(32768)});
	if (reader.Transfers().transfer_limit != StormByte::ByteSize{4096}
			|| writer.Transfers().transfer_limit != StormByte::ByteSize{8192})
		return 5;
	auto configuration = writer.Configuration();
	configuration.write_transfer = StormByte::ByteSize{4096};
	if (writer.Configure(configuration).status != Status::Ok)
		return 6;
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
