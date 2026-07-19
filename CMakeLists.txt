cmake_minimum_required(VERSION 3.16)
project(cloud-gateway VERSION 2.1.0 LANGUAGES CXX)

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

# tight 协议独立库（tight/ 自包含：include/ 公共接口 + src/ 私有实现）
add_subdirectory(tight)

# 网关公共接口：tight 库 + 网关自身头 + tight/src（仅供测试触达内部头）
add_library(gateway_core INTERFACE)
target_include_directories(gateway_core INTERFACE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/tight
)
target_link_libraries(gateway_core INTERFACE tight)

add_executable(cloud_gateway
    gateway/gateway_main.cpp
)

target_link_libraries(cloud_gateway PRIVATE
    tight
    gateway_core
    Threads::Threads
    ${EXTRA_LIBS}
)

# client 端（设备模拟/直连测试工具）
add_subdirectory(client)

enable_testing()

function(add_gateway_test name source)
    add_executable(${name} ${source})
    target_link_libraries(${name} PRIVATE
        tight
        gateway_core
        Threads::Threads
        ${EXTRA_LIBS}
    )
    add_test(NAME ${name} COMMAND ${name})
endfunction()

add_gateway_test(test_channel     tests/test_channel.cpp)
add_gateway_test(test_blackboard  tests/test_blackboard.cpp)
add_gateway_test(test_ratelimit   tests/test_ratelimit.cpp)
add_gateway_test(test_session     tests/test_session.cpp)
add_gateway_test(test_auth        tests/test_auth.cpp)
add_gateway_test(test_server      tests/test_server.cpp)
add_gateway_test(test_tight_pair  tests/test_tight_pair.cpp)
add_gateway_test(test_tight_fec   tests/test_tight_fec.cpp)
add_gateway_test(test_command     tests/test_command.cpp)
add_gateway_test(test_crypto      tests/test_crypto.cpp)
add_gateway_test(test_e2e_final   tests/test_e2e_final.cpp)
add_gateway_test(test_e2e_sim     tests/test_e2e_sim.cpp)

# 早期调试测试（已废弃，不参与 ctest，仅保留编译参考）
add_executable(test_e2e     tests/test_e2e.cpp)
target_link_libraries(test_e2e     PRIVATE tight gateway_core Threads::Threads ${EXTRA_LIBS})
add_executable(test_e2e_v2  tests/test_e2e_v2.cpp)
target_link_libraries(test_e2e_v2  PRIVATE tight gateway_core Threads::Threads ${EXTRA_LIBS})

install(TARGETS cloud_gateway RUNTIME DESTINATION bin)
