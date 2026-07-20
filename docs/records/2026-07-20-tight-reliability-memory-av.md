# 工作记录：tight 可靠性增强 + 内存优化 + 音视频场景调参

日期：2026-07-20　提交：`b8e5ac2`、`407a9af`

## 背景

tight 协议库进入音视频业务阶段：双向 16kHz 单声道 G.711/PCM 音频 + 视频流 +
文件流，客户端为 lite 模式 IoT 设备。本轮围绕可靠性语义、内存占用、
场景化参数三条线展开。

## 完成内容

### 1. 可靠性：NACK 重复上报 + 缺口跳过（report.cpp）

- **问题**：丢失序号只上报一次即从 `m_missing_seqs` 擦除，洪泛时 Report 本身
  丢失 → 重传链单点失效；永久缺口冻结 `m_next_expected_seq` → ack 游标卡死、
  发送端 pending 泄漏
- **修复**：
  - 丢失序号确认前每个 report 周期**重复上报**（`report.cpp:53`）
  - 缺口超 3.5×RTT **立即跳过**（ack 游标不停滞，迟到重传照常投递）
  - 放弃兜底：超 `(kMaxRetries+2)` 个周期停止上报；missing 表硬上限 4096
  - 发送端修剪 `m_retries >= kMaxRetries` 的 pending
- **效果**：限速洪泛 2000/2000 全投递

### 2. max_message_bytes 可配置（默认 64KB，钳制 [8KB, 10MB]）

- 发送侧超限 `send()` 返回 false；接收侧按上限防御畸形 fragment_count
  （防内存耗尽）
- 丢弃日志为配置项 `TightConfig::drop_log`（默认开），经 `Peer::m_drop_log`
  传递，**lite 模式强制关闭**（静默丢弃），`set_lite_mode` 运行时刷新
- 网关接线：`tc.max_message_bytes = m_server.max_payload_bytes`；
  客户端 `--max-message` 参数

### 3. 内存优化

- **缓冲池**（`tight/src/buffer_pool.hpp`）：出站数据报 PooledBytes，
  thread_local 自由链表，2048B 块 ×16/线程；BlockingQueue 节点回收池（64 节点）
- **lite caps 收紧**：queue 1024→128、encode 256→64、outbound 1024→256
  → 队列最坏占用 ~49MB → **~5.4MB**
- **flush 节拍 lite 下限 10ms**：CPU 唤醒 500→100 次/秒（IoT 功耗）

### 4. 场景化调参（16kHz 音频 + 视频 + 文件）

- **MTU 默认 1200→1350**：单包载荷 1286B，PCM 40ms 帧（1280B）整包容纳零分片
- 组包间隔定 40ms（G.711 效率 87%，省 11% 带宽 vs 20ms）
- 优先级分流：音频 2 / 视频 1 / 文件 0（`send_priority`）
- 关键结论：重传对实时音频无价值（到达即过期），靠 FEC（熵驱动）；
  文件流需应用层块校验+命令通道补发（协议重传上限 10 次后静默丢弃）

### 5. 内存测试数据

| 测试 | 结果 |
|---|---|
| test_mem8k（单进程，2000×8KB） | 2000/2000，delta 1.3MB，PASS |
| test_mem8k_split（双进程 lite 客户端，无限速） | delta 2.28MB ≈ 发送队列积压 |
| 同上，限速 50KB/s ×200 条 | **delta 136KB，全程平台期** |
| 结论 | delta ≈ 在途字节，与总流量无关；进程地板 ~5.5MB，tight 实例 ~76KB |

### 6. 文档

- `tight/docs/usage.md`（441 行完整使用文档）：集成、最小示例、IoT 音频
  客户端/网关服务器完整示例、TightConfig 全参数表、API 参考、8 条行为约定、
  3 套场景配方
- `tight/README.md` 添加文档链接；docs 三处参数表同步 MTU 1350

## 测试状态

13/13 全绿（含新增 test_mem8k；test_mem8k_split 为双进程手动测试，未入 ctest）

## 遗留/后续

- FEC 冗余下限配置（当前纯熵驱动，冷启动偏保守）——未做
- 缓冲池 cap lite 感知（16→4，省 ~24KB）——收益小，未做
- 文件流应用层确认/补发示例代码——文档有约定，未落示例工程
