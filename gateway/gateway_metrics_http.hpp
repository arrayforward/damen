#pragma once

#include "gateway/gateway_config.hpp"
#include "gateway/gateway_metrics.hpp"
#include "creek/logger.hpp"

#include <atomic>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
typedef int socklen_t;
#else
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#endif

namespace gateway {

class MetricsHttpServer {
public:
    MetricsHttpServer(const GatewayConfig& config, std::shared_ptr<Metrics> metrics)
        : m_port(config.m_metrics.port)
        , m_path(config.m_metrics.path)
        , m_metrics(std::move(metrics))
        , m_running(false) {
#ifdef _WIN32
        WSADATA wsa;
        WSAStartup(MAKEWORD(2, 2), &wsa);
#endif
    }

    ~MetricsHttpServer() { stop(); }

    MetricsHttpServer(const MetricsHttpServer&) = delete;
    MetricsHttpServer& operator=(const MetricsHttpServer&) = delete;

    bool start() {
        if (m_running.exchange(true)) return true;
        m_thread = std::thread([this] { serve(); });
        return true;
    }

    void stop() {
        if (!m_running.exchange(false)) return;
#ifdef _WIN32
        if (m_socket != INVALID_SOCKET) {
            closesocket(m_socket);
            m_socket = INVALID_SOCKET;
        }
        WSACleanup();
#else
        if (m_socket >= 0) {
            ::close(m_socket);
            m_socket = -1;
        }
#endif
        if (m_thread.joinable()) m_thread.join();
    }

private:
    void serve() {
#ifdef _WIN32
        m_socket = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (m_socket == INVALID_SOCKET) return;
#else
        m_socket = ::socket(AF_INET, SOCK_STREAM, 0);
        if (m_socket < 0) return;
#endif

        int opt = 1;
        setsockopt(m_socket, SOL_SOCKET, SO_REUSEADDR,
                   reinterpret_cast<const char*>(&opt), sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_ANY);
        addr.sin_port = htons(m_port);

        if (::bind(m_socket, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
            CREEK_LOG_WARN("Metrics HTTP bind failed on port " + std::to_string(m_port));
            return;
        }

        if (::listen(m_socket, 5) < 0) return;

#ifdef _WIN32
        u_long nonblock = 1;
        ioctlsocket(m_socket, FIONBIO, &nonblock);
#else
        int fl = fcntl(m_socket, F_GETFL, 0);
        if (fl >= 0) fcntl(m_socket, F_SETFL, fl | O_NONBLOCK);
#endif

        CREEK_LOG_INFO("Metrics HTTP server listening on :" + std::to_string(m_port) + m_path);

        while (m_running.load(std::memory_order_acquire)) {
            fd_set readfds;
            FD_ZERO(&readfds);
            FD_SET(m_socket, &readfds);
            timeval tv{1, 0};
            int sel = select(static_cast<int>(m_socket + 1), &readfds, nullptr, nullptr, &tv);
            if (sel <= 0) continue;

            sockaddr_in client{};
            socklen_t clen = sizeof(client);
#ifdef _WIN32
            auto client_sock = ::accept(m_socket, reinterpret_cast<sockaddr*>(&client), &clen);
            if (client_sock == INVALID_SOCKET) continue;
#else
            auto client_sock = ::accept(m_socket, reinterpret_cast<sockaddr*>(&client), &clen);
            if (client_sock < 0) continue;
#endif
            handle_client(client_sock);
#ifdef _WIN32
            closesocket(client_sock);
#else
            ::close(client_sock);
#endif
        }
    }

    void handle_client(
#ifdef _WIN32
        SOCKET sock
#else
        int sock
#endif
    ) {
        char buf[4096]{};
        int n = recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) return;

        std::string request(buf, n);
        bool is_metrics = request.find("GET " + m_path) != std::string::npos ||
                          request.find("GET /metrics") != std::string::npos;

        std::ostringstream response;
        if (is_metrics) {
            std::string metrics_text = m_metrics->prometheus_text();
            response << "HTTP/1.1 200 OK\r\n"
                     << "Content-Type: text/plain; version=0.0.4\r\n"
                     << "Content-Length: " << metrics_text.size() << "\r\n"
                     << "Connection: close\r\n"
                     << "\r\n"
                     << metrics_text;
        } else {
            std::string body = "{}";
            response << "HTTP/1.1 404 Not Found\r\n"
                     << "Content-Type: application/json\r\n"
                     << "Content-Length: " << body.size() << "\r\n"
                     << "Connection: close\r\n"
                     << "\r\n"
                     << body;
        }

        std::string resp_str = response.str();
        send(sock, resp_str.data(), static_cast<int>(resp_str.size()), 0);
    }

    std::uint16_t                   m_port;
    std::string                     m_path;
    std::shared_ptr<Metrics>        m_metrics;
    std::atomic<bool>               m_running{false};
    std::thread                     m_thread;
#ifdef _WIN32
    SOCKET                          m_socket{INVALID_SOCKET};
#else
    int                             m_socket{-1};
#endif
};

} // namespace gateway
