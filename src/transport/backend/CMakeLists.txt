cmake_minimum_required (VERSION 3.6.3)

add_subdirectory(MPI)
add_subdirectory(Thallium)

set(BACKEND_SRC
  ${MPI_SRC}
  ${THALLIUM_SRC}

  PARENT_SCOPE)
