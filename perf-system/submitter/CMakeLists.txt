cmake_minimum_required(VERSION 3.9.1)

project(submit)

set(CMAKE_CXX_STANDARD 17)

include_directories(/home/fotisk/include/)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L /home/fotisk/lib/")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--rpath=/home/fotisk/lib")

add_executable(
  submit submit.cpp handle_arguments.cpp handle_arguments.hpp parquet_data.cpp
         parquet_data.hpp
)

target_link_libraries(submit PRIVATE arrow)
target_link_libraries(submit PRIVATE parquet)
target_link_libraries(submit PRIVATE curl)
target_link_libraries(submit PRIVATE uv)
target_link_libraries(submit PRIVATE ssl)
target_link_libraries(submit PRIVATE crypto)
