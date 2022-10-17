cmake_minimum_required(VERSION 3.16)

project(submit)

set(CMAKE_CXX_STANDARD 20)

include_directories(/home/fotisk/include/)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -L /home/fotisk/lib/")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wl,--rpath=/home/fotisk/lib")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -w")
add_executable(
  submit submit.cpp handle_arguments.cpp 
  handle_arguments.hpp parquet_data.cpp
  stdc++.h
         parquet_data.hpp 
         ~/fotisCCf/CCF/3rdparty/
         ~/fotisCCf/CCF/src/
         ~/fotisCCf/CCF/src/http/
         ~/fotisCCf/CCF/include/
)

set(CCF_DIR "/home/fotisk/fotisCCf/CCF")

include(${CCF_DIR}/cmake/tools.cmake)
include(${CCF_DIR}/cmake/crypto.cmake)

# target_compile_options(submit PUBLIC -stdlib=libc++)

target_link_libraries(submit PRIVATE arrow)
target_link_libraries(submit PRIVATE parquet)
target_link_libraries(submit PRIVATE curl)
target_link_libraries(submit PRIVATE uv)
target_link_libraries(submit PRIVATE ssl)
target_link_libraries(submit PRIVATE crypto)
target_link_libraries(submit PRIVATE ccfcrypto.host)
target_link_libraries(submit PRIVATE /home/fotisk/fotisCCf/CCF/build/CMakeFiles/http_parser.host.dir/3rdparty/exported/llhttp/llhttp.c.o)
target_link_libraries(submit PRIVATE /home/fotisk/fotisCCf/CCF/build/CMakeFiles/http_parser.host.dir/3rdparty/exported/llhttp/api.c.o)
target_link_libraries(submit PRIVATE /home/fotisk/fotisCCf/CCF/build/CMakeFiles/http_parser.host.dir/3rdparty/exported/llhttp/http.c.o)
