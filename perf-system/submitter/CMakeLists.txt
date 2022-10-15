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
         parquet_data.hpp 
         ~/fotisCCf/CCF/3rdparty/exported/nlohmann
         ~/fotisCCf/CCF/3rdparty/exported/fmt/format.h
         ~/fotisCCf/CCF/3rdparty/exported/fmt/chrono.h
         ~/fotisCCf/CCF/3rdparty/exported/fmt/std.h
         ~/fotisCCf/CCF/3rdparty/exported/fmt/ostream.h
         ~/fotisCCf/CCF/src/ds/x509_time_fmt.h
         ~/fotisCCf/CCF/src/ds/serialized.h
         ~/fotisCCf/CCF/src/ds/messaging.h
         ~/fotisCCf/CCF/src/ds
         ~/fotisCCf/CCF/src/ds/thread_messaging.h
         ~/fotisCCf/CCF/src/crypto/openssl/openssl_wrappers.h
         ~/fotisCCf/CCF/src/crypto
         ~/fotisCCf/CCF/src/crypto/openssl/hash.h
         ~/fotisCCf/CCF/src/crypto/openssl
         ~/fotisCCf/CCF/src/crypto/openssl/symmetric_key.h
         ~/fotisCCf/CCF/src/crypto/openssl/public_key.h
         ~/fotisCCf/CCF/src/crypto/openssl/hash.h
         ~/fotisCCf/CCF/src/crypto/openssl/rsa_key_pair.h
         ~/fotisCCf/CCF/src/crypto/openssl/verifier.h
         ~/fotisCCf/CCF/src/crypto/openssl/key_pair.h
         ~/fotisCCf/CCF/src/clients/rpc_tls_client.h
         ~/fotisCCf/CCF/perf-system/submitter/stdc++.h
         ~/fotisCCf/CCF/src/http/http_builder.h
         ~/fotisCCf/CCF/src/http/http_parser.h
         ~/fotisCCf/CCF/src/http/
         ~/fotisCCf/CCF/3rdparty/exported/llhttp/llhttp.h
         ~/fotisCCf/CCF/3rdparty/exported/small_vector
         ~/fotisCCf/CCF/src/enclave/tls_endpoint.h
         ~/fotisCCf/CCF/src/enclave/
         ~/fotisCCf/CCF/src/tls/
         ~/fotisCCf/CCF/include/
         ~/fotisCCf/CCF/include/ccf/crypto/
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
target_link_libraries(submit PRIVATE /home/fotisk/fotisCCf/CCF/build/CMakeFiles/http_parser.host.dir/3rdparty/exported/llhttp/llhttp.c.o)
target_link_libraries(submit PRIVATE /home/fotisk/fotisCCf/CCF/build/CMakeFiles/http_parser.host.dir/3rdparty/exported/llhttp/api.c.o)
target_link_libraries(submit PRIVATE /home/fotisk/fotisCCf/CCF/build/CMakeFiles/http_parser.host.dir/3rdparty/exported/llhttp/http.c.o)