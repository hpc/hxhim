#
# Create the demo executable
#
include_directories(BEFORE ../../include)
add_executable(hxhim-demo hxhim-demo.cxx)
#target_link_libraries(hxhim-demo LINK_PUBLIC hxhim)
target_link_libraries(hxhim-demo)
