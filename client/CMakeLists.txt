# tight_client：设备端直连测试工具（链接独立 tight 库）
add_executable(tight_client
    tight_client.cpp
)

target_link_libraries(tight_client PRIVATE
    tight
    Threads::Threads
    ${EXTRA_LIBS}
)

install(TARGETS tight_client RUNTIME DESTINATION bin)
