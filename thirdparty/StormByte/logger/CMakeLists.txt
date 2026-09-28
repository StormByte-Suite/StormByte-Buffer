set(STORMBYTE_LOGGER_OPTSTR "ALIAS=StormByte::Logger")
if(MSVC)
	string(PREPEND STORMBYTE_LOGGER_OPTSTR "TOOLCHAIN=clang-cl;")
endif()

if(BUILD_SHARED_LIBS)
	set(STORMBYTE_LOGGER_MODE shared)
else()
	set(STORMBYTE_LOGGER_MODE static)
endif()

buildmaster_component(
	StormByte-Logger
	"StormByte Suite - Logger"
	"${CMAKE_CURRENT_LIST_DIR}/src"
	"WITH_STORMBYTE=BUNDLED;ENABLE_TEST=OFF"
	${STORMBYTE_LOGGER_MODE}
	"StormByte-Logger"
	"${STORMBYTE_LOGGER_OPTSTR}"
)
