if(WIN32)
	set(WITH_STORMBYTE "BUNDLED" CACHE STRING "Use StormByte components (SYSTEM/BUNDLED)" FORCE)
else()
	set(WITH_STORMBYTE "BUNDLED" CACHE STRING "Use StormByte components (SYSTEM/BUNDLED)")
endif()

if(WITH_STORMBYTE STREQUAL "SYSTEM")
	find_package(StormByte REQUIRED Logger System)
else()
	add_subdirectory(logger)
	add_subdirectory(system)
endif()

buildmaster_link(StormByte-Buffer StormByte-Logger StormByte-System)
