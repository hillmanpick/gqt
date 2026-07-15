# GQT 币安合约量化平台

GQT 是一个面向 Binance USDT-M 永续合约的自托管量化控制台。平台负责行情、策略编辑、数据下载、回测、风控和 Freqtrade 自动执行；AI 只做市场研判，不直接下单。

## 功能

- 首次进入配置管理员密码和 Binance API Key / Secret
- AES-256-GCM 加密 SQLite 数据库 `key.db`，前端不会读回密钥
- Binance Futures 实时 K 线、合约切换、资金费率、多空比、持仓量
- 基于 24 小时趋势、资金费率和多空比的 0-100 市场情绪分值
- OpenAI、Claude、DeepSeek 市场分析接口
- Freqtrade 策略编辑、Python 语法校验、历史数据下载和回测
- Dry-run 自动执行与异常恢复；实盘由服务器环境变量二次放行

该平台不需要显卡。普通多核 CPU、16 GB 内存和 SSD 足以运行当前因子策略、回测与网页服务；大规模参数搜索时主要增加 CPU 核数和内存。

## 安全边界

- 不使用 `/root/key.txt`，也没有隐藏接口或凭据回传逻辑。
- `data/`、`.env`、交易数据库、日志和回测结果均被 Git 忽略。
- 密钥加密主密钥应通过 `CREDENTIAL_MASTER_KEY` 注入，并与 `key.db` 分开备份。
- 默认 `dry_run: true`，即使修改为实盘，仍需设置 `ALLOW_LIVE_TRADING=true` 才能启动。
- Binance Key 应关闭提现权限、限制服务器 IP，并只授予合约交易所需权限。
- AI 输出不能触发下单；只有经过校验并由 Freqtrade 加载的策略可以执行交易。

## 本地运行

前置条件：Node.js 22+、Python 3、Docker Engine 与 Docker Compose。

```bash
npm ci
npm test
npm start
```

打开 `http://127.0.0.1:4173`，首次设置时填写至少 12 位管理员密码。建议先使用 Binance Testnet 凭据或限制权限的专用 Key，并保持 Dry-run。

行情初始化与情绪数据由服务端访问 Binance 公共 Futures REST API；当前 K 线通过 Binance 公共 WebSocket 实时更新。服务器必须能够合法、稳定地访问这些地址。

## 配置

复制 `.env.example` 的变量到进程管理器或受保护的环境文件。生产环境至少配置：

```bash
CREDENTIAL_MASTER_KEY="$(openssl rand -base64 32)"
HOST=127.0.0.1
PORT=4173
TRUST_PROXY=true
COOKIE_SECURE=true
ALLOW_LIVE_TRADING=false
```

Node 不会自动读取 `.env`；应由 systemd、Docker 或其他进程管理器注入。

## 部署到 8.141.98.117

不要在纯 HTTP 页面输入真实 API Key。先为该服务器绑定域名并配置 HTTPS，然后部署：

```bash
sudo mkdir -p /opt/gqt
sudo chown "$USER":"$USER" /opt/gqt
git clone https://github.com/hillmanpick/gqt.git /opt/gqt
cd /opt/gqt
npm ci --omit=dev
npm test
```

推荐使用独立的低权限系统用户运行服务，并让该用户加入 `docker` 组以管理 Freqtrade。`/etc/gqt.env` 权限设为 `600`，内容使用上面的生产变量。systemd 服务示例：

```ini
[Unit]
Description=GQT Quant Platform
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service

[Service]
Type=simple
User=gqt
Group=gqt
SupplementaryGroups=docker
WorkingDirectory=/opt/gqt
EnvironmentFile=/etc/gqt.env
ExecStart=/usr/bin/node /opt/gqt/server.mjs
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Nginx 只代理到 `127.0.0.1:4173`，并传递 `Host`、`X-Forwarded-For` 和 `X-Forwarded-Proto`。使用 Certbot 或云负载均衡配置有效证书后，再访问首次设置页。防火墙只开放 `22`、`80`、`443`，不要公开 `4173` 或 Freqtrade 容器端口。

前端会根据入口 URL 自动识别部署前缀，因此也可以将 Nginx 的 `/gqt/` 代理到 GQT 根路径，同时保留域名下已有的其他服务。

仓库中的 `deploy/gqt.service` 是可直接安装的 systemd 单元，`deploy/nginx/` 包含 HTTP 强制跳转和 HTTPS 反向代理片段。

## 启用实盘

先完成数据下载、分段回测和至少数周 Dry-run。确认策略、手续费、滑点、最小下单量和极端行情保护后：

1. 停止 GQT 与 Freqtrade。
2. 将 `trading/user_data/config.json` 的 `dry_run` 改为 `false`。
3. 在服务器环境中设置 `ALLOW_LIVE_TRADING=true`。
4. 重启 GQT，从执行页手动启动策略并持续观察日志。

任何收益都不保证。合约杠杆可能导致全部保证金损失，首次实盘应使用独立账户和极小仓位。
