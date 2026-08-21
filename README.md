# GQT Trader

GQT Trader 是面向 Binance USDT-M 永续合约的 Windows 原生量化客户端。客户端使用 Rust、`eframe` 和 `egui` 构建，不依赖浏览器或 WebView；界面采用黑色交易终端、黄色主操作、红绿行情配色。

## 客户端功能

- Binance Futures 实时 K 线、交易对和周期切换
- 标记价格、资金费率、多空比、持仓量和 24 小时成交额
- Binance U 本位真实钱包余额、可用余额、保证金、未实现盈亏和当前持仓
- 独立模拟账户权益、可用虚拟资金、累计盈亏、胜率和模拟持仓
- 趋势、仓位、资金费率组成的 0-100 市场情绪分值
- OpenAI、Claude、DeepSeek 和 OpenAI 兼容中转站风险研判，AI 不具备下单能力
- 本地策略编辑、Python 语法校验、历史数据下载和回测
- 回测会自动补齐所选日期范围及策略启动窗口所需的历史数据
- Docker、Python、回测和日志任务均以 Windows 隐藏后台进程运行
- Freqtrade 数据同步、回测、策略启动、停止、日志与 Dry-run 异常恢复
- 全币种 USDT 永续扫描、新闻/X 舆情去重、来源可信度、时间衰减与综合推荐
- 模拟盘与实盘模式切换；启用和启动实盘均需在客户端二次确认
- 模拟与实盘分别使用独立 SQLite 交易数据库，成交记录不会混合
- 仓位、最大持仓数和爆仓缓冲风控
- Windows DPAPI + AES-256-GCM 本地加密密钥库
- 保存前使用 Binance Futures 签名账户接口验证 Key、Secret 和合约交易权限

## 运行要求

- Windows 10/11 x64
- 仅查看行情和使用 AI：不需要 Docker，也不需要显卡
- 数据下载、回测和自动策略：需要 Docker Desktop
- 编辑策略后的语法校验：需要 Python 3

首次启动只填写 Binance API Key 和 Secret。密钥使用当前 Windows 账户的 DPAPI 加密保护，保存在本机应用数据目录的 `key.db`，不会写入仓库、网页或远程服务器；后续启动直接进入客户端。

## 从源码运行

安装 Rust MSVC 工具链后执行：

```powershell
cargo run --manifest-path desktop\Cargo.toml
```

构建优化后的单文件客户端：

```powershell
cargo build --release --manifest-path desktop\Cargo.toml
```

产物位于 `desktop\target\release\gqt-trader.exe`。

## 策略内核

内置 `FuturesFactorStrategy` 是可编辑的 Freqtrade Interface v3 基线策略，包含多空方向、动量、趋势、波动率和成交量因子。回测与数据下载由客户端调用本机 Docker 执行，默认时间周期为 `4h`。

客户端默认使用 `dry_run: true`。账户页显示真实 Binance Futures 资金和持仓，但模拟策略不会操作这些资金。完成历史数据验证、分段回测和至少数周模拟运行后，才应在执行页显式切换并二次确认实盘。合约杠杆可能造成全部保证金损失，任何策略都不保证收益。

## 安全边界

- 不使用 `/root/key.txt`，没有隐藏接口或凭据回传逻辑。
- AI 接口只接收当前公开市场数据和用户分析问题，不能触发交易。
- 中转站支持自定义 HTTPS Base URL 和模型名，API Key 仍使用本地加密密钥库保存。
- Binance Key 应关闭提现权限，并只授予合约交易所需权限。
- `desktop/target/`、`desktop/dist/`、交易数据库、日志和回测结果不会提交到 Git。
- 自动恢复只作用于 Dry-run Freqtrade 容器，不会切换为实盘。
- Freqtrade 旧配置会自动迁移到当前 schema；实盘模式不会自动恢复或静默启动。

## 实时新闻与舆情

实时扫描和合规新闻/X API 的配置见 [`docs/realtime-signal.md`](docs/realtime-signal.md)。系统默认读取公开 RSS；未配置 X Bearer Token 时不会伪造社交情绪，推荐页会明确显示舆情证据不足。

仓库根目录中的 Node/Web 代码是前期版本，原生 Rust 客户端是当前主线。
