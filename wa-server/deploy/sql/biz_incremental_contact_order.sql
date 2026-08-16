-- 增量: 联系人表 + 订阅订单表 (幂等, 已有表跳过)
CREATE TABLE IF NOT EXISTS biz.contact (
    id            BIGSERIAL PRIMARY KEY,
    user_id       BIGINT      NOT NULL REFERENCES biz.wa_user(id) ON DELETE CASCADE,
    phone_number  TEXT        NOT NULL,              -- 联系人号码
    order_id      BIGINT,                            -- 当前生效订单 id (客户端提交最新 或 检测到续费后的最新), 指向 biz.subscription_order.id
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, phone_number)                   -- 一个用户下号码唯一; 不同用户可重复添加
);

CREATE INDEX IF NOT EXISTS idx_contact_user ON biz.contact (user_id);
CREATE INDEX IF NOT EXISTS idx_contact_phone ON biz.contact (phone_number);

CREATE TABLE IF NOT EXISTS biz.subscription_order (
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

CREATE INDEX IF NOT EXISTS idx_sub_order_user ON biz.subscription_order (user_id);
CREATE INDEX IF NOT EXISTS idx_sub_order_contact ON biz.subscription_order (contact_id);
CREATE INDEX IF NOT EXISTS idx_sub_order_store ON biz.subscription_order (platform, store_order_id);
CREATE INDEX IF NOT EXISTS idx_sub_order_status ON biz.subscription_order (status);
