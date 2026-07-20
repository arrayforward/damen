---
name: damen-tight
description: Use when working on the damen ConvAI gateway or the tight reliable-UDP protocol library (tight/, gateway/, client/, tests/). Covers MinGW build/test commands, git identity conventions, project layout, protocol invariants, and memory-test methodology.
---

# damen / tight 项目工作约定

## 构建与测试（Windows / MinGW）

- 编译器：MinGW g++ 15.2.0（winget BrechtSanders.WinLibs），生成器 `"MinGW Makefiles"`，链接 `ws2_32`
- 配置：`cmake -B build -S . -G "MinGW Makefiles" -DCMAKE_CXX_COMPILER=g++`
- 构建：`cmake --build build -j 4`（或 `cmake --build build -j 4 --target <name>`）
- 测试：`ctest --test-dir build -j 4`；单跑：`ctest --test-dir build -R <name> --output-on-failure`
- 一键脚本：`build.bat [clean] [debug|release] [test]`
- 验收标准：**13/13 测试全绿**才能提交
- 测试可执行文件直接生成在 `build/<name>.exe`（不在 build/tests/）

## Git 约定

- 远程：https://github.com/arrayforward/damen.git
- 身份按命令注入，**不改全局配置**：
  `git -c user.name="arrayforward" -c user.email="arrayforward@users.noreply.github.com" commit ...`
- TLS 推送偶发失败属网络抖动，直接重试
- PowerShell 把 git stderr 进度输出当错误（NativeCommandError），看到 `main -> main` 即成功

## 项目结构

- `tight/`：自包含可靠 UDP 库（独立 CMake，`add_subdirectory` 两用）。
  公共 API 在 `tight/include/tight/`（聚合头 `tight.hpp`）；
  私有实现在 `tight/src/`（namespace `tight::tight_detail`）
- `gateway/`：云网关（GatewayServer + 观察者模式消息监听器 `*_listener.hpp`）
- `client/tight_client.cpp`：交互式测试客户端
- `tests/`：`add_gateway_test(name source)` 注册；`test_mem8k`（单进程）/
  `test_mem8k_split`（双进程，手动跑 server/client 两实例）
- 文档：根 `README.md`、`docs/`（中文）、`tight/docs/usage.md`（完整使用文档）
- 代码约定：命名空间 `tight`、成员 `m_` 前缀、日志宏 `TIGHT_LOG_*`、文件 UTF-8、中文注释

## tight 协议关键不变量（改动时勿破坏）

- 线格式：48B 头，magic 0x54474854，v1，CRC32；PacketType Handshake=0..Command=10；
  flags bit15 = kFlagEncrypted；encode() 用 `header.payload_size` 做 AAD（确定性）
- seq==0（Parity）不参与缺口跟踪/序列初始化（否则 ack 游标卡死泄漏）
- msg_id 用独立计数器 `m_msg_id_out`，不占数据序列号
- 数据面可靠性：NACK 经 Report 包，丢失序号**确认前每周期重复上报**；
  缺口超 3.5×RTT 即跳过（ack 不停滞），迟到重传照常投递；每包最多重传 10 次，
  耗尽静默丢弃 → **文件传输需应用层块校验+补发**
- `m_missing_seqs`/`m_recv_seqs`/`m_next_expected_seq` 由收线程与 reactor 双线程访问，
  必须持 `peer.m_mu`（否则 rb_tree 损坏崩溃）
- lite 模式：单线程 reactor、64KB 小栈、caps 收紧（queue≤128/encode≤64/outbound≤256/
  socket≤16KB）、flush≥10ms、drop_log 强制关；`m_drop_log` 在建 peer 时写入、
  `set_lite_mode` 时刷新全量 peer

## 内存测试方法论

- WS 用 `GetProcessMemoryInfo`；delta ≈ 在途字节，与总流量无关（平台期验证）
- CRT 高水位保留是正常现象：free 不还 OS；多轮曲线平台期即无泄漏
- 数据面洪泛必丢（socket 缓冲溢出 + Report 也会丢）：内存测试必须**限速**
  （如 50KB/s）或自适应等待 received 追上 sent
- 基准数据（lite 客户端）：进程地板 ~5.5MB，tight 实例 ~76KB，
  50KB/s×8KB 稳态 delta ~136KB
