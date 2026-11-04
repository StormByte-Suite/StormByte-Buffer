cmake_minimum_required(VERSION 3.28)
project(StormByteBufferPackageConsumer LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

list(PREPEND CMAKE_MODULE_PATH
	"${BUFFER_PREFIX}/share/cmake/Modules"
	"${DEPENDENCY_PREFIX}/share/cmake/Modules"
)
list(PREPEND CMAKE_PREFIX_PATH "${BUFFER_PREFIX}" "${DEPENDENCY_PREFIX}")
set(CMAKE_FIND_USE_CMAKE_ENVIRONMENT_PATH FALSE)
set(CMAKE_FIND_USE_SYSTEM_ENVIRONMENT_PATH FALSE)
set(CMAKE_FIND_USE_CMAKE_SYSTEM_PATH FALSE)
set(CMAKE_FIND_USE_PACKAGE_REGISTRY FALSE)
set(CMAKE_FIND_USE_SYSTEM_PACKAGE_REGISTRY FALSE)

find_package(StormByte REQUIRED COMPONENTS Buffer)
find_package(StormByte REQUIRED COMPONENTS Base Buffer Logger System)

foreach(path IN ITEMS "${STORMBYTE_Buffer_INCLUDE_DIR}" "${STORMBYTE_Buffer_LIBRARY}")
	cmake_path(IS_PREFIX BUFFER_PREFIX "${path}" NORMALIZE _in_buffer_prefix)
	if(NOT _in_buffer_prefix)
		message(FATAL_ERROR "Buffer was not found in ${BUFFER_PREFIX}: ${path}")
	endif()
endforeach()
foreach(path IN ITEMS
		"${STORMBYTE_LIBRARY}"
		"${STORMBYTE_Logger_LIBRARY}"
		"${STORMBYTE_System_LIBRARY}")
	cmake_path(IS_PREFIX DEPENDENCY_PREFIX "${path}" NORMALIZE _in_dependency_prefix)
	if(NOT _in_dependency_prefix)
		message(FATAL_ERROR "StormByte dependency was not found in ${DEPENDENCY_PREFIX}: ${path}")
	endif()
endforeach()

add_executable(buffer_package_consumer consumer.cxx)
target_link_libraries(buffer_package_consumer PRIVATE StormByte::Buffer)
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
	target_compile_options(buffer_package_consumer PRIVATE -Wno-changes-meaning)
endif()
set_target_properties(buffer_package_consumer PROPERTIES
	RUNTIME_OUTPUT_DIRECTORY "$<1:${CMAKE_BINARY_DIR}/bin>"
)
