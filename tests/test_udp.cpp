#include "tests/test_framework.hpp"

#include <cstring>
#include <string>
#include <thread>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
typedef int socklen_t;
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

TEST_CASE("raw_udp_localhost") {
    std::uint16_t port = 23000;

#ifdef _WIN32
    WSADATA wsa;
    WSAStartup(MAKEWORD(2, 2), &wsa);
#endif

    auto s1 = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    ASSERT_NE(s1, -1);

    sockaddr_in addr1{};
    addr1.sin_family = AF_INET;
    addr1.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr1.sin_port = htons(port);
    ASSERT_EQ(::bind(s1, (sockaddr*)&addr1, sizeof(addr1)), 0);

    auto s2 = ::socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
    ASSERT_NE(s2, -1);

    sockaddr_in addr2{};
    addr2.sin_family = AF_INET;
    addr2.sin_addr.s_addr = inet_addr("127.0.0.1");
    addr2.sin_port = htons(port + 1);
    ASSERT_EQ(::bind(s2, (sockaddr*)&addr2, sizeof(addr2)), 0);

    const char* msg = "hello-udp";
    sockaddr_in dest{};
    dest.sin_family = AF_INET;
    dest.sin_addr.s_addr = inet_addr("127.0.0.1");
    dest.sin_port = htons(port);
    int sent = ::sendto(s2, msg, (int)std::strlen(msg), 0,
                        (sockaddr*)&dest, sizeof(dest));
    ASSERT_GT(sent, 0);

    char buf[256]{};
    sockaddr_in from{};
    socklen_t flen = sizeof(from);
    int n = ::recvfrom(s1, buf, sizeof(buf), 0, (sockaddr*)&from, &flen);
    ASSERT_GT(n, 0);
    ASSERT_EQ(std::string(buf, n), std::string("hello-udp"));

#ifdef _WIN32
    closesocket(s1);
    closesocket(s2);
    WSACleanup();
#else
    ::close(s1);
    ::close(s2);
#endif
}
