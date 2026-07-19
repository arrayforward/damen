#pragma once

// Reed-Solomon 擦除码（GF(2^8)，本原多项式 0x11D）。
// 公开 FEC 接口，实现在 tight/fec.cpp。
//
// 编码矩阵采用 Vandermonde 形式：第 i 个校验分片是全部数据分片以
// coef(i, j) = (j+1)^i 为系数的 GF 线性组合（i=0 时退化为 XOR）。
// 任意 p 个校验分片可恢复任意 p 个丢失的数据分片（高斯消元求解）。

#include "tight/types.hpp"

#include <cstddef>
#include <optional>
#include <utility>
#include <vector>

namespace tight {

class ReedSolomon {
public:
    // 编码：对 data 分片（统一长度 width）生成 parity_count 个校验分片。
    static std::vector<Bytes> encode(const std::vector<Bytes>& data,
                                     std::size_t parity_count, std::size_t width);

    // 解码：data 中以 nullopt 表示缺失分片；parity 为收到的
    // (校验分片索引, 内容) 列表。缺失数不超过校验数时可全部恢复并回填，
    // 恢复的分片长度均为 width（真实长度由报文内 4 字节总长前缀裁剪）。
    static bool decode(std::vector<std::optional<Bytes>>& data,
                       const std::vector<std::pair<std::size_t, Bytes>>& parity,
                       std::size_t width);
};

} // namespace tight
