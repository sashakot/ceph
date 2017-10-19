# - Find ucx
# Find the ucx library and includes
#
# UCX_INCLUDE_DIR - where to find ucp.h
# UCX_LIBS - List of libraries when using ucx 
# UCX_FOUND - True if ucx found.

# sanity check

message(STATUS "Looking for UCX in ${WITH_UCX}")
if (WITH_UCX AND EXISTS ${WITH_UCX})
    find_path(UCX_INCLUDE_DIR NAMES ucp/api/ucp.h PATHS "${WITH_UCX}/include")
    find_library(UCP_LIBRARY ucp HINTS "${WITH_UCX}/lib")
    find_library(UCS_LIBRARY ucs HINTS "${WITH_UCX}/lib")
endif ()

message (STATUS "ucp lib ${UCP_LIBRARY}")
message (STATUS "ucs lib ${UCS_LIBRARY}")

if (UCP_LIBRARY)
    set(UCX_LIBS
        ${UCP_LIBRARY}
        ${UCS_LIBRARY})
endif (UCP_LIBRARY)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ucx DEFAULT_MSG UCX_LIBS UCX_INCLUDE_DIR)
