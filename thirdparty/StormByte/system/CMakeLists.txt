set(STORMBYTE_SYSTEM_OPTSTR "ALIAS=StormByte::System")
if(MSVC)
	string(PREPEND STORMBYTE_SYSTEM_OPTSTR "TOOLCHAIN=clang-cl;")
endif()

if(BUILD_SHARED_LIBS)
	set(STORMBYTE_SYSTEM_MODE shared)
else()
	set(STORMBYTE_SYSTEM_MODE static)
endif()

buildmaster_component(
	StormByte-System
	"StormByte Suite - System"
	"${CMAKE_CURRENT_LIST_DIR}/src"
	"WITH_STORMBYTE=BUNDLED;ENABLE_TEST=OFF"
	${STORMBYTE_SYSTEM_MODE}
	"StormByte-System"
	"${STORMBYTE_SYSTEM_OPTSTR}"
)
