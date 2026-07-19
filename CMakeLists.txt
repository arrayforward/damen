cmake_minimum_required(VERSION 3.16)
project(cloud-gateway VERSION 2.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(MSVC)
    add_compile_options(/W4 /wd4100 /wd4127 /wd4996 /utf-8)
else()
    add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)
endif()

if(UNIX AND NOT APPLE)
    set(THREADS_PREFER_PTHREAD_FLAG ON)
endif()
find_package(Threads REQUIRED)

if(WIN32)
    set(EXTRA_LIBS ws2_32)
else()
    set(EXTRA_LIBS)
endif()

add_library(creek STATIC
    tight/transport.cpp
    tight/reassembler.cpp
    tight/fragmenter.cpp
    tight/report.cpp
    tight/command.cpp
    tight/packet_codec.cpp
    tight/fec.cpp
    tight/bandwidth.cpp
    tight/address.cpp
    tight/crc32.cpp
    tight/wsa.cpp
)

target_include_directories(creek PUBLIC
    ${CMAKE_SOURCE_DIR}
)

target_link_libraries(creek PUBLIC
    Threads::Threads
    ${EXTRA_LIBS}
)

add_library(gateway_core INTERFACE)
target_include_directories(gateway_core INTERFACE
    ${CMAKE_SOURCE_DIR}
)
target_link_libraries(gateway_core INTERFACE creek)

add_executable(cloud_gateway
    gateway/gateway_main.cpp
)

target_link_libraries(cloud_gateway PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)

enable_testing()

add_executable(test_channel
    tests/test_channel.cpp
)
target_link_libraries(test_channel PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_channel COMMAND test_channel)

add_executable(test_blackboard
    tests/test_blackboard.cpp
)
target_link_libraries(test_blackboard PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_blackboard COMMAND test_blackboard)

add_executable(test_ratelimit
    tests/test_ratelimit.cpp
)
target_link_libraries(test_ratelimit PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_ratelimit COMMAND test_ratelimit)

add_executable(test_session
    tests/test_session.cpp
)
target_link_libraries(test_session PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_session COMMAND test_session)

add_executable(test_auth
    tests/test_auth.cpp
)
target_link_libraries(test_auth PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_auth COMMAND test_auth)

add_executable(test_server
    tests/test_server.cpp
)
target_link_libraries(test_server PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_server COMMAND test_server)

add_executable(test_e2e
    tests/test_e2e.cpp
)
target_link_libraries(test_e2e PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
# Old SimDevice-based tests (deprecated, replaced by test_e2e_final)
# add_test(NAME test_e2e COMMAND test_e2e)

add_executable(test_e2e_v2
    tests/test_e2e_v2.cpp
)
target_link_libraries(test_e2e_v2 PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
# add_test(NAME test_e2e_v2 COMMAND test_e2e_v2)
# add_test(NAME test_e2e_simple COMMAND test_e2e_simple)
# add_test(NAME test_e2e_dbg COMMAND test_e2e_dbg)

add_executable(test_e2e_final
    tests/test_e2e_final.cpp
)
target_link_libraries(test_e2e_final PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_e2e_final COMMAND test_e2e_final)

add_executable(test_e2e_sim
    tests/test_e2e_sim.cpp
)
target_link_libraries(test_e2e_sim PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_e2e_sim COMMAND test_e2e_sim)

add_executable(test_tight_pair
    tests/test_tight_pair.cpp
)
target_link_libraries(test_tight_pair PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_tight_pair COMMAND test_tight_pair)

add_executable(test_tight_fec
    tests/test_tight_fec.cpp
)
target_link_libraries(test_tight_fec PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_tight_fec COMMAND test_tight_fec)

add_executable(test_command
    tests/test_command.cpp
)
target_link_libraries(test_command PRIVATE
    creek
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)
add_test(NAME test_command COMMAND test_command)

install(TARGETS cloud_gateway RUNTIME DESTINATION bin)
