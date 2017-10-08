# - Find ucx
# Find the ucx library and includes
#
# UCX_INCLUDE_DIR - where to find ucp.h
# UCX_LIBS - List of libraries when using ucx 
# UCX_FOUND - True if ucx found.

# sanity check
message(STATUS "Looking for UCX in ${WITH_UCX}")
find_path(UCX_INCLUDE_DIR NAMES ucp/api/ucp.h PATHS ${WITH_UCX}/include)
find_library(UCP_LIB NAMES libucp.a PATHS ${WITH_UCX}/lib)
message (STATUS "ucp lib ${UCP_LIB}")
if (UCP_LIB)
  set(UCX_LIBS "-L${WITH_UCX}/lib -lucp")
endif (UCP_LIB)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(UCX DEFAULT_MSG UCX_LIBS UCX_INCLUDE_DIR)

