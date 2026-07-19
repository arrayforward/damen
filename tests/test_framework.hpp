#pragma once

#include <chrono>
#include <cstdlib>
#include <exception>
#include <functional>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#define ASSERT_TRUE(cond)  \
    do { if (!(cond)) { throw test::AssertionError(#cond, __FILE__, __LINE__); } } while (0)

#define ASSERT_FALSE(cond) \
    do { if (cond) { throw test::AssertionError("!(" #cond ")", __FILE__, __LINE__); } } while (0)

#define ASSERT_EQ(a, b) \
    do { if (!((a) == (b))) { \
        std::ostringstream _oss; _oss << #a " (" << (a) << ") == " #b " (" << (b) << ")"; \
        throw test::AssertionError(_oss.str(), __FILE__, __LINE__); \
    } } while (0)

#define ASSERT_NE(a, b) \
    do { if ((a) == (b)) { \
        std::ostringstream _oss; _oss << #a " == " #b; \
        throw test::AssertionError(_oss.str(), __FILE__, __LINE__); \
    } } while (0)

#define ASSERT_GT(a, b) \
    do { if (!((a) > (b))) { \
        std::ostringstream _oss; _oss << #a " (" << (a) << ") > " #b " (" << (b) << ")"; \
        throw test::AssertionError(_oss.str(), __FILE__, __LINE__); \
    } } while (0)

#define ASSERT_GE(a, b) \
    do { if (!((a) >= (b))) { \
        std::ostringstream _oss; _oss << #a " (" << (a) << ") >= " #b " (" << (b) << ")"; \
        throw test::AssertionError(_oss.str(), __FILE__, __LINE__); \
    } } while (0)

#define ASSERT_LT(a, b) \
    do { if (!((a) < (b))) { \
        std::ostringstream _oss; _oss << #a " (" << (a) << ") < " #b " (" << (b) << ")"; \
        throw test::AssertionError(_oss.str(), __FILE__, __LINE__); \
    } } while (0)

#define ASSERT_LE(a, b) \
    do { if (!((a) <= (b))) { \
        std::ostringstream _oss; _oss << #a " (" << (a) << ") <= " #b " (" << (b) << ")"; \
        throw test::AssertionError(_oss.str(), __FILE__, __LINE__); \
    } } while (0)

#define ASSERT_STREQ(a, b) \
    do { if (std::string(a) != std::string(b)) { \
        std::ostringstream _oss; _oss << "\"" << (a) << "\" == \"" << (b) << "\""; \
        throw test::AssertionError(_oss.str(), __FILE__, __LINE__); \
    } } while (0)

#define ASSERT_THROWS(expr) \
    do { bool _caught = false; try { (void)(expr); } catch (...) { _caught = true; } \
          if (!_caught) { throw test::AssertionError(#expr " should throw", __FILE__, __LINE__); } \
    } while (0)

#define ASSERT_NOTHROW(expr) \
    do { try { (void)(expr); } catch (...) { \
        throw test::AssertionError(#expr " should not throw", __FILE__, __LINE__); } \
    } while (0)

#define _TEST_CONCAT2(a, b) a##b
#define _TEST_CONCAT(a, b) _TEST_CONCAT2(a, b)

#define TEST_CASE(name) \
    static void _TEST_CONCAT(test_func_, __LINE__)(); \
    namespace { struct _TEST_CONCAT(TestReg_, __LINE__) { \
        _TEST_CONCAT(TestReg_, __LINE__)() { test::register_test(name, _TEST_CONCAT(test_func_, __LINE__)); } \
    } _TEST_CONCAT(test_reg_inst_, __LINE__); } \
    static void _TEST_CONCAT(test_func_, __LINE__)()

#define SUBCASE(name) if (true)

namespace test {

struct AssertionError : std::exception {
    std::string m_msg;
    std::string m_file;
    int         m_line;

    AssertionError(std::string msg, std::string file, int line)
        : m_msg(std::move(msg)), m_file(std::move(file)), m_line(line) {}

    const char* what() const noexcept override { return m_msg.c_str(); }
};

struct TestResult {
    std::string m_name;
    bool        m_passed{false};
    std::string m_error;
    std::chrono::microseconds m_duration{0};
};

using TestFn = std::function<void()>;

inline std::vector<std::pair<std::string, TestFn>>& test_registry() {
    static std::vector<std::pair<std::string, TestFn>> registry;
    return registry;
}

inline void register_test(const std::string& name, TestFn fn) {
    test_registry().emplace_back(name, std::move(fn));
}

inline int run_tests() {
    auto& registry = test_registry();
    std::vector<TestResult> results;
    std::size_t passed = 0;
    std::size_t failed = 0;
    std::size_t total = registry.size();

    std::cout << "\n=== Running " << total << " test(s) ===\n\n";

    for (const auto& [name, fn] : registry) {
        TestResult r;
        r.m_name = name;
        auto start = std::chrono::steady_clock::now();
        try {
            fn();
            r.m_passed = true;
            ++passed;
        } catch (const AssertionError& e) {
            r.m_passed = false;
            r.m_error = std::string(e.m_file) + ":" +
                        std::to_string(e.m_line) + ": " + e.m_msg;
            ++failed;
        } catch (const std::exception& e) {
            r.m_passed = false;
            r.m_error = std::string("exception: ") + e.what();
            ++failed;
        } catch (...) {
            r.m_passed = false;
            r.m_error = "unknown exception";
            ++failed;
        }
        auto end = std::chrono::steady_clock::now();
        r.m_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
        results.push_back(std::move(r));
    }

    for (const auto& r : results) {
        if (r.m_passed) {
            std::cout << "[PASS] " << r.m_name << " ("
                      << r.m_duration.count() << " us)\n";
        } else {
            std::cout << "[FAIL] " << r.m_name << "\n"
                      << "       " << r.m_error << "\n";
        }
    }

    std::cout << "\n=== Results: " << passed << "/" << total
              << " passed";
    if (failed > 0) {
        std::cout << ", " << failed << " FAILED";
    }
    std::cout << " ===\n";

    return failed > 0 ? 1 : 0;
}

} // namespace test

int main() {
    return test::run_tests();
}
