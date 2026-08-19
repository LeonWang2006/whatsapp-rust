-- 增量: 联系人上下线事件表 (幂等, 已有表跳过)
CREATE TABLE IF NOT EXISTS biz.presence_event (
    id            BIGSERIAL PRIMARY KEY,
    owner_phone   TEXT        NOT NULL,              -- 归属账号手机号(哪个用户在观察联系人)
    contact_phone TEXT        NOT NULL,              -- 联系人手机号(LID 已归一化为 PN)
    event_type    TEXT        NOT NULL,              -- online / offline
    ts            BIGINT      NOT NULL,              -- 事件时间(Unix 秒)
    last_seen     BIGINT,                            -- 下线时携带的 last_seen
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_presence_event_lookup
    ON biz.presence_event (owner_phone, contact_phone, ts);

-- 幂等: 同一 (owner, contact, type, ts) 只保留一行 (重投/重连竞争去重)
-- 已有表需先清理历史重复行再建唯一约束
DELETE FROM biz.presence_event a USING biz.presence_event b
WHERE a.id > b.id
  AND a.owner_phone = b.owner_phone
  AND a.contact_phone = b.contact_phone
  AND a.event_type = b.event_type
  AND a.ts = b.ts;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint WHERE conname = 'uq_presence_event_unique'
    ) THEN
        ALTER TABLE biz.presence_event
            ADD CONSTRAINT uq_presence_event_unique
            UNIQUE (owner_phone, contact_phone, event_type, ts);
    END IF;
END $$;
