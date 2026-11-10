option(ENABLE_TEST "Enable Unit Tests" OFF)
option(ENABLE_CI_FIXTURES "Enable CI-only package discovery fixtures" OFF)
if(ENABLE_TEST)
	enable_testing()

	add_test(NAME OptimizationTests
		COMMAND "${CMAKE_COMMAND}" -P "${CMAKE_CURRENT_SOURCE_DIR}/optimizations_test.cmake")
	set_tests_properties(OptimizationTests PROPERTIES TIMEOUT 15)

	add_executable(BridgeTests bridge_test.cxx)
	target_link_libraries(BridgeTests StormByte-Buffer)
	add_test(NAME BridgeTests COMMAND BridgeTests)

	add_executable(BufferedFileReaderTests buffered_file_reader_test.cxx)
	target_link_libraries(BufferedFileReaderTests StormByte-Buffer)
	add_test(NAME BufferedFileReaderTests COMMAND BufferedFileReaderTests)

	add_executable(BufferedFileWriterTests buffered_file_writer_test.cxx)
	target_link_libraries(BufferedFileWriterTests StormByte-Buffer)
	add_test(NAME BufferedFileWriterTests COMMAND BufferedFileWriterTests)

	add_executable(BufferedLocationDeviceTests buffered_location_device_test.cxx)
	target_link_libraries(BufferedLocationDeviceTests StormByte-Buffer)
	add_test(NAME BufferedLocationDeviceTests COMMAND BufferedLocationDeviceTests)
	set_tests_properties(BufferedLocationDeviceTests PROPERTIES TIMEOUT 20)

	add_executable(IOTransferTests io_transfer_test.cxx)
	target_link_libraries(IOTransferTests StormByte-Buffer)
	add_test(NAME IOTransferTests COMMAND IOTransferTests)
	set_tests_properties(IOTransferTests PROPERTIES TIMEOUT 45)

	add_executable(ByteBudgetTests byte_budget_test.cxx)
	target_link_libraries(ByteBudgetTests StormByte-Buffer)
	add_test(NAME ByteBudgetTests COMMAND ByteBudgetTests)
	set_tests_properties(ByteBudgetTests PROPERTIES TIMEOUT 30)

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

	add_executable(SinkAllocationFailureTests sink_allocation_failure_test.cxx)
	target_link_libraries(SinkAllocationFailureTests StormByte-Buffer)
	add_test(NAME SinkAllocationFailureTests COMMAND SinkAllocationFailureTests)
	set_tests_properties(SinkAllocationFailureTests PROPERTIES SKIP_RETURN_CODE 77)

	add_executable(TelemetryTests telemetry_test.cxx)
	target_link_libraries(TelemetryTests StormByte-Buffer)
	add_test(NAME TelemetryTests COMMAND TelemetryTests)

	if(ENABLE_CI_FIXTURES AND BUILD_SHARED_LIBS AND WITH_STORMBYTE STREQUAL "BUNDLED" AND NOT CMAKE_CROSSCOMPILING)
		set(_package_cxx_flags "${CMAKE_CXX_FLAGS}")
		set(_package_link_flags "${CMAKE_EXE_LINKER_FLAGS}")
		if(COMMAND _pgo_strip_profile_flags)
			_pgo_strip_profile_flags("${_package_cxx_flags}" _package_cxx_flags)
			_pgo_strip_profile_flags("${_package_link_flags}" _package_link_flags)
		endif()

		add_test(NAME PackageDiscoveryTests
			COMMAND "${CMAKE_COMMAND}"
				"-DBUFFER_BUILD_DIR=${PROJECT_BINARY_DIR}"
				"-DCONSUMER_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}/package_consumer"
				"-DDEPENDENCY_PREFIX=${BUILDMASTER_INSTALL_DIR}"
				"-DBUILD_CONFIG=$<CONFIG>"
				"-DGENERATOR=${CMAKE_GENERATOR}"
				"-DGENERATOR_PLATFORM=${CMAKE_GENERATOR_PLATFORM}"
				"-DGENERATOR_TOOLSET=${CMAKE_GENERATOR_TOOLSET}"
				"-DMAKE_PROGRAM=${CMAKE_MAKE_PROGRAM}"
				"-DCXX_COMPILER=${CMAKE_CXX_COMPILER}"
				"-DCXX_COMPILER_TARGET=${CMAKE_CXX_COMPILER_TARGET}"
				"-DCXX_FLAGS=${_package_cxx_flags}"
				"-DEXE_LINKER_FLAGS=${_package_link_flags}"
				"-DOSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}"
				"-DOSX_SYSROOT=${CMAKE_OSX_SYSROOT}"
				"-DEXECUTABLE_SUFFIX=${CMAKE_EXECUTABLE_SUFFIX}"
				-P "${CMAKE_CURRENT_SOURCE_DIR}/package_discovery_test.cmake")
		set_tests_properties(PackageDiscoveryTests PROPERTIES TIMEOUT 120)
	endif()
endif()
