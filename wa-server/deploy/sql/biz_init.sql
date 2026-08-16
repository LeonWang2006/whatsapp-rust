-- ============================================================
-- wa-online 业务表设计 (v4 定稿: 用户=设备, 手机号可变, 官方平台类型为准)
-- schema: biz (独立, 与协议层 public 隔离)
-- 核心模型: 用户实体由客户端设备唯一标识 (device_uuid),
--           手机号是"当前号码"可变属性, 换卡不换用户。
-- 换卡流程: 客户端调"更新用户信息"API -> 服务器取消旧号关联 ->
--           对新号重新配对。旧号关联记录留存在 pair_history。
-- 平台: platform 列以 WhatsApp 官方 PlatformType 枚举为准 (0-25),
--       客户端 X-Platform 请求头直接传官方对应值, 无需业务侧映射。
-- ============================================================

CREATE SCHEMA IF NOT EXISTS biz;

-- ------------------------------------------------------------
-- 用户表 (用户 = 一台客户端设备)
-- ------------------------------------------------------------
CREATE TABLE biz.wa_user (
    id             BIGSERIAL PRIMARY KEY,
    device_uuid    TEXT        NOT NULL UNIQUE,       -- 客户端设备唯一标识 = 用户实体
    phone_number   TEXT        NOT NULL,              -- 当前绑定的 WhatsApp 号码(E.164), init 必填, 可换
    status         TEXT        NOT NULL DEFAULT 'init', -- init/pairing/online/logged_out/disabled
    wa_device_id   INTEGER,                           -- wa-server device.id, 配对成功后回填
    -- 平台 (官方 PlatformType 为准)
    platform       SMALLINT,                          -- WhatsApp 官方 PlatformType 0-25 (1=CHROME 7=DESKTOP 14=IOS_PHONE 16=ANDROID_PHONE...), 客户端 X-Platform 请求头直接传此值
    platform_display TEXT,                            -- 配对用的 companion_platform_display, 如 "Chrome (Linux)"
    -- 客户端设备信息 (init 提交)
    os_version     TEXT,                              -- device_info.osVersion
    manufacturer   TEXT,                              -- device_info.manufacturer
    device_model   TEXT,                              -- device_info.device
    os_build_number TEXT,                             -- device_info.osBuildNumber
    locale_language TEXT,                             -- device_info.localeLanguageIso6391
    locale_country TEXT,                              -- device_info.localeCountryIso31661Alpha2
    device_info_raw TEXT,                             -- device_info 原始 JSON 留存
    -- 推送
    firebase_token TEXT,                              -- Android FCM 令牌
    apns_token     TEXT,                              -- Apple APNs 令牌
    notification   BOOLEAN     NOT NULL DEFAULT false, -- 是否推送消息开关
    -- 生命周期
    last_heartbeat TIMESTAMPTZ,                       -- 客户端最后心跳
    last_online_at TIMESTAMPTZ,                       -- WhatsApp 最后在线
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_wa_user_phone ON biz.wa_user (phone_number);

-- ------------------------------------------------------------
-- 关联历史表: 每次配对/退出/重连/换卡记录
-- ------------------------------------------------------------
CREATE TABLE biz.pair_history (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT      NOT NULL REFERENCES biz.wa_user(id) ON DELETE CASCADE,
    phone_number  TEXT,                               -- 本次关联的手机号(快照)
    wa_device_id  INTEGER,                            -- 对应 wa-server device.id
    action        TEXT        NOT NULL,               -- pair/logout/stream_replaced/reconnect/replace_phone
    pair_code     TEXT,                               -- 8 位码
    detail        TEXT,                               -- 退出原因/换卡详情
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_pair_history_user ON biz.pair_history (user_id, created_at DESC);
