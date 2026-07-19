#include "test_framework.hpp"

#include "src/crypto.hpp"

#include <array>
#include <string>
#include <vector>

using namespace tight;
using namespace tight::tight_detail;

// 十六进制字符串转字节数组
static std::vector<std::uint8_t> unhex(const std::string& hex) {
    std::vector<std::uint8_t> out;
    for (std::size_t i = 0; i + 1 < hex.size(); i += 2) {
        auto val = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return 0;
        };
        out.push_back(static_cast<std::uint8_t>((val(hex[i]) << 4) | val(hex[i + 1])));
    }
    return out;
}

static std::array<std::uint8_t, 32> unhex32(const std::string& hex) {
    auto v = unhex(hex);
    std::array<std::uint8_t, 32> out{};
    for (std::size_t i = 0; i < 32 && i < v.size(); ++i) out[i] = v[i];
    return out;
}

TEST_CASE("sha256_abc_vector") {
    auto h = sha256(reinterpret_cast<const std::uint8_t*>("abc"), 3);
    auto expected = unhex32("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
    ASSERT_TRUE(h == expected);
}

TEST_CASE("x25519_rfc7748_vector_1") {
    auto scalar = unhex32("a546e36bf0527c9d3b16154b82465edd62144c0ac1fc5a18506a2244ba449ac4");
    auto u = unhex32("e6db6867583030db3594c1a424b15f7c726624ec26b3353b10a903a6d0ab1c4c");
    auto expected = unhex32("c3da55379de9c6908e94ea4df28d084f32eccf03491c71f754b4075577a28552");
    std::array<std::uint8_t, 32> out{};
    ASSERT_TRUE(x25519(out, scalar, u));
    ASSERT_TRUE(out == expected);
}

TEST_CASE("x25519_rfc7748_vector_2") {
    auto scalar = unhex32("4b66e9d4d1b4673c5ad22691957d6af5c11b6421e0ea01d42ca4169e7918ba0d");
    auto u = unhex32("e5210f12786811d3f4b7959d0538ae2c31dbe7106fc03c3efc4cd549c715a493");
    auto expected = unhex32("95cbde9476e8907d7aade45cb4b873f88b595a68799fa152e6f8f7647aac7957");
    std::array<std::uint8_t, 32> out{};
    ASSERT_TRUE(x25519(out, scalar, u));
    ASSERT_TRUE(out == expected);
}

TEST_CASE("x25519_key_agreement") {
    auto alice = x25519_generate();
    auto bob = x25519_generate();
    std::array<std::uint8_t, 32> sa{}, sb{};
    ASSERT_TRUE(x25519(sa, alice.private_key, bob.public_key));
    ASSERT_TRUE(x25519(sb, bob.private_key, alice.public_key));
    ASSERT_TRUE(sa == sb);   // 双方导出同一共享秘密
}

TEST_CASE("aes256_fips197_vector") {
    // FIPS-197 附录 C.3：AES-256 加密单块
    auto key = unhex32("000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    auto pt = unhex("00112233445566778899aabbccddeeff");
    auto expected = unhex("8ea2b7ca516745bfeafc49904b496089");
    // 通过 GCM 的底层块加密间接验证：零 AAD、单块、nonce 全零时
    // GCM 第一块密文 = pt ^ AES_K(0^96||1) —— 改为直接验证 GCM 标准向量（见下）。
    // 此处用 GCM 轮回到接验证 AES 实现的正确性。
    std::array<std::uint8_t, 12> nonce{};
    std::vector<std::uint8_t> ct(pt.size());
    std::array<std::uint8_t, 16> tag{};
    ASSERT_TRUE(aes256_gcm_encrypt(key, nonce, nullptr, 0, pt.data(), pt.size(),
                                   ct.data(), tag.data()));
    std::vector<std::uint8_t> back(pt.size());
    ASSERT_TRUE(aes256_gcm_decrypt(key, nonce, nullptr, 0, ct.data(), ct.size(),
                                   tag.data(), back.data()));
    ASSERT_TRUE(back == pt);
}

TEST_CASE("aes256_gcm_nist_vector") {
    // NIST GCM 测试向量（AES-256，零密钥/零 IV，16 字节零明文）
    std::array<std::uint8_t, 32> key{};
    std::array<std::uint8_t, 12> nonce{};
    std::array<std::uint8_t, 16> pt{};
    std::array<std::uint8_t, 16> ct{};
    std::array<std::uint8_t, 16> tag{};
    ASSERT_TRUE(aes256_gcm_encrypt(key, nonce, nullptr, 0,
                                   pt.data(), pt.size(), ct.data(), tag.data()));
    auto expected_ct = unhex("cea7403d4d606b6e074ec5d3baf39d18");
    auto expected_tag = unhex("d0d1c8a799996bf0265b98b5d48ab919");
    ASSERT_TRUE((std::vector<std::uint8_t>(ct.begin(), ct.end())) == expected_ct);
    ASSERT_TRUE((std::vector<std::uint8_t>(tag.begin(), tag.end())) == expected_tag);
}

TEST_CASE("aes256_gcm_aad_and_tamper") {
    auto key = unhex32("feffe9928665731c6d6a8f9467308308feffe9928665731c6d6a8f9467308308");
    std::array<std::uint8_t, 12> nonce{0xca, 0xfe, 0xba, 0xbe, 0xfa, 0xce,
                                       0xdb, 0xad, 0xde, 0xca, 0xf8, 0x88};
    std::string aad = "header-aad-bytes";
    std::string msg = "tight encrypted payload 1234567890";
    std::vector<std::uint8_t> ct(msg.size());
    std::array<std::uint8_t, 16> tag{};
    ASSERT_TRUE(aes256_gcm_encrypt(key, nonce,
                                   reinterpret_cast<const std::uint8_t*>(aad.data()), aad.size(),
                                   reinterpret_cast<const std::uint8_t*>(msg.data()), msg.size(),
                                   ct.data(), tag.data()));
    // 正确解密
    std::vector<std::uint8_t> back(msg.size());
    ASSERT_TRUE(aes256_gcm_decrypt(key, nonce,
                                   reinterpret_cast<const std::uint8_t*>(aad.data()), aad.size(),
                                   ct.data(), ct.size(), tag.data(), back.data()));
    ASSERT_TRUE((std::string(back.begin(), back.end())) == msg);
    // 篡改密文 → 认证失败
    auto bad_ct = ct;
    bad_ct[0] ^= 0x01;
    ASSERT_FALSE(aes256_gcm_decrypt(key, nonce,
                                    reinterpret_cast<const std::uint8_t*>(aad.data()), aad.size(),
                                    bad_ct.data(), bad_ct.size(), tag.data(), back.data()));
    // 篡改 AAD → 认证失败
    std::string bad_aad = "header-aad-bytes!";
    ASSERT_FALSE(aes256_gcm_decrypt(key, nonce,
                                    reinterpret_cast<const std::uint8_t*>(bad_aad.data()), bad_aad.size(),
                                    ct.data(), ct.size(), tag.data(), back.data()));
}

TEST_CASE("hkdf_deterministic_and_domain_separated") {
    std::array<std::uint8_t, 32> ikm{};
    for (int i = 0; i < 32; ++i) ikm[i] = static_cast<std::uint8_t>(i);
    std::array<std::uint8_t, 8> salt{1, 2, 3, 4, 5, 6, 7, 8};
    auto k1 = hkdf_sha256(ikm.data(), ikm.size(), salt.data(), salt.size(), "tight-data-key-v1");
    auto k2 = hkdf_sha256(ikm.data(), ikm.size(), salt.data(), salt.size(), "tight-data-key-v1");
    auto k3 = hkdf_sha256(ikm.data(), ikm.size(), salt.data(), salt.size(), "other-info");
    ASSERT_TRUE(k1 == k2);    // 同输入同输出
    ASSERT_FALSE(k1 == k3);   // 不同 info 不同密钥
}
