option(ENABLE_TEST "Enable Unit Tests" OFF)
if(ENABLE_TEST)
	enable_testing()

	add_executable(BridgeTests bridge_test.cxx)
	target_link_libraries(BridgeTests StormByte-Buffer)
	add_test(NAME BridgeTests COMMAND BridgeTests)

	add_executable(BufferedFileReaderTests buffered_file_reader_test.cxx)
	target_link_libraries(BufferedFileReaderTests StormByte-Buffer)
	add_test(NAME BufferedFileReaderTests COMMAND BufferedFileReaderTests)

	add_executable(BufferedFileWriterTests buffered_file_writer_test.cxx)
	target_link_libraries(BufferedFileWriterTests StormByte-Buffer)
	add_test(NAME BufferedFileWriterTests COMMAND BufferedFileWriterTests)

	add_executable(ExceptionTests exception_test.cxx)
	target_link_libraries(ExceptionTests StormByte-Buffer)
	add_test(NAME ExceptionTests COMMAND ExceptionTests)

	add_executable(FIFOTests fifo_test.cxx)
	target_link_libraries(FIFOTests StormByte-Buffer)
	add_test(NAME FIFOTests COMMAND FIFOTests)

	add_executable(HopperTests hopper_test.cxx)
	target_link_libraries(HopperTests StormByte-Buffer)
	add_test(NAME HopperTests COMMAND HopperTests)

	add_executable(PipelineTests pipeline_test.cxx)
	target_link_libraries(PipelineTests StormByte-Buffer)
	add_test(NAME PipelineTests COMMAND PipelineTests)

	add_executable(ProducerConsumerTests producer_consumer_test.cxx)
	target_link_libraries(ProducerConsumerTests StormByte-Buffer)
	add_test(NAME ProducerConsumerTests COMMAND ProducerConsumerTests)

	add_executable(PumperTests pumper_test.cxx)
	target_link_libraries(PumperTests StormByte-Buffer)
	add_test(NAME PumperTests COMMAND PumperTests)

	add_executable(RingTests ring_test.cxx)
	target_link_libraries(RingTests StormByte-Buffer)
	add_test(NAME RingTests COMMAND RingTests)

	add_executable(SharedFIFOTests shared_fifo_test.cxx)
	target_link_libraries(SharedFIFOTests StormByte-Buffer)
	add_test(NAME SharedFIFOTests COMMAND SharedFIFOTests)

	add_executable(SinkTests sink_test.cxx)
	target_link_libraries(SinkTests StormByte-Buffer)
	add_test(NAME SinkTests COMMAND SinkTests)

	add_executable(TelemetryTests telemetry_test.cxx)
	target_link_libraries(TelemetryTests StormByte-Buffer)
	add_test(NAME TelemetryTests COMMAND TelemetryTests)
endif()
