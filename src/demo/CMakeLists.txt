#
# Create the demo executable
#
include_directories(BEFORE ../../include)
add_executable(hxhim-demo hxhim-demo.cxx ../comm/comm_mpi.cpp)
target_link_libraries(hxhim-demo)
