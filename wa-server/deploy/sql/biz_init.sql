-- ============================================================
-- wa-online 业务表 (v5: 用户 + 关联历史 + 联系人 + 订阅订单)
-- schema: biz (独立, 与协议层 public 隔离)
--
-- 模型:
-- - 用户实体由客户端设备唯一标识 (device_uuid), 手机号可变(换卡不换用户)。
-- - 用户添加联系人(号码), 为联系人购买/续费订阅。
-- - 联系人表: 每个用户对每个号码一条记录, 存"当前生效订单"的 order_id。
--   多个用户可各自添加同一联系人, 各自订单独立。
-- - 订阅订单表: 通用订单表(其他功能模块也用), 通过 module 区分。
--   记录初始购买 + 续费订单, 含商店原始订单 id 与回执。
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

-- ------------------------------------------------------------
-- 联系人表 (user 维度: 每个用户对每个号码一条)
-- ------------------------------------------------------------
CREATE TABLE biz.contact (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT      NOT NULL REFERENCES biz.wa_user(id) ON DELETE CASCADE,
    phone_number  TEXT        NOT NULL,              -- 联系人号码
    order_id      BIGINT,                            -- 当前生效订单 id (客户端提交最新 或 检测到续费后的最新), 指向 biz.subscription_order.id
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, phone_number)                   -- 一个用户下号码唯一; 不同用户可重复添加
);

CREATE INDEX idx_contact_user ON biz.contact (user_id);
CREATE INDEX idx_contact_phone ON biz.contact (phone_number);

-- ------------------------------------------------------------
-- 订阅订单表 (通用订单, 各功能模块共用)
-- ------------------------------------------------------------
CREATE TABLE biz.subscription_order (
    id             BIGSERIAL PRIMARY KEY,            -- 订单 id (自增), 联系人表 order_id 指向这里
    user_id        BIGINT      NOT NULL REFERENCES biz.wa_user(id) ON DELETE CASCADE,
    module         TEXT        NOT NULL DEFAULT 'contact_subscription', -- 功能模块标识
    contact_id     BIGINT,                           -- 本模块关联联系人 (其他模块可空)
    platform       SMALLINT,                         -- 1=Google 2=Apple
    store_order_id TEXT,                             -- Google/Apple 原始订单 id
    order_type     TEXT        NOT NULL DEFAULT 'initial_purchase', -- initial_purchase / renewal
    status         TEXT        NOT NULL DEFAULT 'pending', -- pending/active/cancelled/expired/refunded
    plan           TEXT,                             -- 套餐
    amount         NUMERIC(10,2),                    -- 金额
    currency       TEXT,                             -- 币种
    purchased_at   TIMESTAMPTZ,                      -- 购买时间
    expires_at     TIMESTAMPTZ,                      -- 到期时间
    receipt_raw    TEXT,                             -- 商店原始回执 JSON (服务端校验留存)
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_sub_order_user ON biz.subscription_order (user_id);
CREATE INDEX idx_sub_order_contact ON biz.subscription_order (contact_id);
CREATE INDEX idx_sub_order_store ON biz.subscription_order (platform, store_order_id);
CREATE INDEX idx_sub_order_status ON biz.subscription_order (status);

-- ------------------------------------------------------------
-- 联系人上下线事件表 (presence 在线时长追踪)
--   订阅联系人 presence 后, 上线/下线各记一行, 形成时间对。
--   客户端通过 GET /presence 查询指定范围内的上下线记录并计算在线时长。
-- ------------------------------------------------------------
CREATE TABLE biz.presence_event (
    id            BIGSERIAL PRIMARY KEY,
    owner_phone   TEXT        NOT NULL,              -- 归属账号手机号(哪个用户在观察联系人)
    contact_phone TEXT        NOT NULL,              -- 联系人手机号(LID 已归一化为 PN)
    event_type    TEXT        NOT NULL,              -- online / offline
    ts            BIGINT      NOT NULL,              -- 事件时间(Unix 秒)
    last_seen     BIGINT,                            -- 下线时携带的 last_seen
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_presence_event_lookup
    ON biz.presence_event (owner_phone, contact_phone, ts);

-- 幂等: 同一 (owner, contact, type, ts) 只保留一行 (重投/重连竞争去重), 配合插入的 ON CONFLICT DO NOTHING
ALTER TABLE biz.presence_event
    ADD CONSTRAINT uq_presence_event_unique
    UNIQUE (owner_phone, contact_phone, event_type, ts);
