include(GNUInstallDirs)

file(GLOB_RECURSE STORMBYTE_BUFFER_SOURCES CONFIGURE_DEPENDS "${CMAKE_CURRENT_LIST_DIR}/*.cxx")

if(BUILD_SHARED_LIBS)
	set(_mode shared)
else()
	set(_mode static)
endif()

buildmaster_component(
	StormByte-Buffer
	"StormByte Suite - Buffer"
	"${STORMBYTE_BUFFER_SOURCES}"
	""
	${_mode}
	"StormByte-Buffer"
	"BACKEND=host;ALIAS=StormByte::Buffer"
)

if(BUILD_SHARED_LIBS)
	target_compile_definitions(StormByte-Buffer
		PRIVATE StormByte_Buffer_EXPORTS
		PUBLIC STORMBYTE_BUFFER_SHARED
	)
endif()

set_target_properties(StormByte-Buffer PROPERTIES
	LINKER_LANGUAGE CXX
	SOVERSION ${CMAKE_PROJECT_VERSION}
	VERSION ${CMAKE_PROJECT_VERSION}
	POSITION_INDEPENDENT_CODE ON
	CXX_VISIBILITY_PRESET hidden
	VISIBILITY_INLINES_HIDDEN ON
)

if(MSVC)
	target_compile_options(StormByte-Buffer PRIVATE
		/EHsc
		$<$<CONFIG:Release>:/O2>
		$<$<CONFIG:Debug>:/Od>
	)
	target_compile_definitions(StormByte-Buffer PUBLIC UNICODE NOMINMAX)
else()
	target_compile_options(StormByte-Buffer PRIVATE
		$<$<CONFIG:Debug>:-pipe -g -ggdb -Wall -Wextra -Wnon-virtual-dtor -pedantic -pedantic-errors -O0>
	)
endif()

target_include_directories(StormByte-Buffer
	SYSTEM BEFORE PUBLIC
		"${CMAKE_CURRENT_LIST_DIR}/public"
		"${CMAKE_CURRENT_LIST_DIR}/private"
)
