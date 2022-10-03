cmake_minimum_required(VERSION 3.9.1)

project(submit)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -I /home/fotisk/include/")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L /home/fotisk/lib/")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--rpath=/home/fotisk/lib")

# find_package(Arrow REQUIRED)

add_executable(submit submit.cpp)

target_link_libraries(submit PRIVATE arrow)
target_link_libraries(submit PRIVATE parquet)
target_link_libraries(submit PRIVATE curl)

# message(${ARROW_VERSION})
