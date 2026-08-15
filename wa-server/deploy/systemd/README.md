# wa-server 依赖服务:开机自启 (Quadlet + user systemd)

Redis 和 PostgreSQL 作为 wa-server 的依赖服务,通过 **Podman Quadlet** 注册为
**用户级 systemd 单元**,WSL 启动时自动拉起,崩溃时自动重启。

## 前置条件 (本机已满足)

- `/etc/wsl.conf` 中 `systemd=true` (WSL 使用 systemd 作为 init)
- rootless podman (`podman info` 显示 `Rootless: true`)
- linger 已启用: `loginctl enable-linger justf` (否则无登录会话时 user systemd 不跑)

## 文件

- `wa-server/deploy/systemd/wa-postgres.container` — PostgreSQL 16
- `wa-server/deploy/systemd/wa-redis.container` — Redis (AOF + 256MB cap)

## 安装 (一次性)

```bash
mkdir -p ~/.config/containers/systemd/
cp wa-server/deploy/systemd/wa-*.container ~/.config/containers/systemd/
systemctl --user daemon-reload
systemctl --user start wa-postgres wa-redis
```

## 开机自启说明

Quadlet 的 generator 会在 `daemon-reload` 时自动应用 `[Install]` 节
(`WantedBy=default.target`),**不需要** `systemctl --user enable`。它会在
`/run/user/$UID/systemd/generator/default.target.wants/` 里建立符号链接,
WSL 启动时 user 级 `default.target` 激活,这两个服务随之自动启动。

验证:

```bash
systemctl --user list-dependencies default.target | grep wa-
# 应看到 wa-postgres.service 和 wa-redis.service
```

## 数据卷

容器挂载的是 compose 时代遗留的命名卷 (带 `compose_` 前缀):

- `compose_pg_data:/var/lib/postgresql/data`
- `compose_redis_data:/data`

> 为什么带 `compose_` 前缀: 早期用 `podman-compose` 启动,compose 会给卷名加
> 项目名前缀。为了**不迁移已有数据**,Quadlet 直接复用这些旧卷。若哪天卷丢失,
> 需要重新配对 (删除设备后重新关联)。

## 日常运维

```bash
# 查看状态
systemctl --user status wa-postgres wa-redis
podman ps --format '{{.Names}} {{.Status}}'

# 重启 / 停止 / 启动 (注意 stop 后容器被移除,start 重建)
systemctl --user restart wa-postgres wa-redis
systemctl --user stop wa-postgres wa-redis
systemctl --user start wa-postgres wa-redis

# 崩溃自动恢复 (Restart=always, 3s 后重试)
podman kill wa-redis    # 观察 systemd 自动重启

# 日志
journalctl --user -u wa-redis -f
journalctl --user -u wa-postgres -f
```

## 排障

| 现象 | 原因 | 处理 |
| --- | --- | --- |
| `daemon-reload` 后 unit 未生成 | 文件名/键名拼写错误 | `journalctl --user` 找 `quadlet-generator` 报错 |
| 容器退出不重启 | 缺 `[Service] Restart=always` | 检查 unit 文件 |
| `systemctl --user enable` 报 "transient or generated" | Quadlet 单元不可 enable | 正常,generator 已自动应用 `[Install]` |
| 端口占用 | 旧 compose 容器还活着 | `podman ps -a` 找旧容器删除 |

## 迁移自 compose (如果你之前用 podman-compose)

1. `podman-compose -f ... down` 会**删除容器但保留卷** (卷被命名卷持有)。
2. 确认卷存在: `podman volume ls`。
3. 按上面"安装"步骤配置 Quadlet,卷名改用 `compose_*` 前缀即复用旧数据。
