-- wa-online 业务表注释 (中文留存) — biz schema
-- 用法: podman exec -i wa-postgres psql -U postgres -d mydb -f /tmp/biz_comments.sql

COMMENT ON SCHEMA biz IS 'wa-online 业务 schema。存放业务自有表,与协议层 public 表隔离';

COMMENT ON TABLE biz.wa_user IS '用户表。用户实体由客户端设备唯一标识(device_uuid), 手机号是当前号码可变属性(换卡不换用户)。init 必须带号码, wa_device_id 配对成功后回填';
COMMENT ON COLUMN biz.wa_user.id IS '用户主键';
COMMENT ON COLUMN biz.wa_user.device_uuid IS '客户端设备唯一标识, 一个设备=一个用户';
COMMENT ON COLUMN biz.wa_user.phone_number IS '当前绑定的 WhatsApp 号码(E.164), init 必填, 换卡时通过更新接口修改';
COMMENT ON COLUMN biz.wa_user.status IS '生命周期状态: init/pairing/online/logged_out/disabled';
COMMENT ON COLUMN biz.wa_user.wa_device_id IS 'wa-server device.id, 配对成功后回填, 用于关联基础设施表';
COMMENT ON COLUMN biz.wa_user.platform IS '客户端平台 X-Platform: 7=Desktop/Electron 等';
COMMENT ON COLUMN biz.wa_user.platform_display IS '推导的平台显示名, 如 "Chrome (Linux)"';
COMMENT ON COLUMN biz.wa_user.os_version IS 'device_info.osVersion 操作系统版本';
COMMENT ON COLUMN biz.wa_user.manufacturer IS 'device_info.manufacturer 制造商';
COMMENT ON COLUMN biz.wa_user.device_model IS 'device_info.device 设备型号';
COMMENT ON COLUMN biz.wa_user.os_build_number IS 'device_info.osBuildNumber 系统构建号';
COMMENT ON COLUMN biz.wa_user.locale_language IS 'device_info.localeLanguageIso6391 语言';
COMMENT ON COLUMN biz.wa_user.locale_country IS 'device_info.localeCountryIso31661Alpha2 国家';
COMMENT ON COLUMN biz.wa_user.device_info_raw IS 'device_info 原始 JSON 留存, 便于审计与复刻平台指纹';
COMMENT ON COLUMN biz.wa_user.firebase_token IS 'Android FCM 推送令牌';
COMMENT ON COLUMN biz.wa_user.apns_token IS 'Apple APNs 推送令牌';
COMMENT ON COLUMN biz.wa_user.notification IS '是否推送消息开关';
COMMENT ON COLUMN biz.wa_user.last_heartbeat IS '客户端最后心跳时间';
COMMENT ON COLUMN biz.wa_user.last_online_at IS 'WhatsApp 最后在线时间';
COMMENT ON COLUMN biz.wa_user.created_at IS '创建时间';
COMMENT ON COLUMN biz.wa_user.updated_at IS '更新时间';

COMMENT ON TABLE biz.pair_history IS '关联历史表。记录每次配对/退出/重连/换卡事件, 供审计与问题排查';
COMMENT ON COLUMN biz.pair_history.id IS '主键';
COMMENT ON COLUMN biz.pair_history.user_id IS '用户 id (外键 wa_user.id)';
COMMENT ON COLUMN biz.pair_history.phone_number IS '本次关联的手机号快照';
COMMENT ON COLUMN biz.pair_history.wa_device_id IS '对应 wa-server device.id';
COMMENT ON COLUMN biz.pair_history.action IS '动作: pair/logout/stream_replaced/reconnect/replace_phone';
COMMENT ON COLUMN biz.pair_history.pair_code IS '8 位配对码';
COMMENT ON COLUMN biz.pair_history.detail IS '详情: 退出原因/换卡说明等';
COMMENT ON COLUMN biz.pair_history.created_at IS '创建时间';

COMMENT ON TABLE biz.contact IS '联系人表。用户添加的联系人号码, 每个用户对每个号码一条记录; 不同用户可添加同一联系人且订单独立';
COMMENT ON COLUMN biz.contact.id IS '主键';
COMMENT ON COLUMN biz.contact.user_id IS '用户 id (外键 wa_user.id)';
COMMENT ON COLUMN biz.contact.phone_number IS '联系人号码';
COMMENT ON COLUMN biz.contact.order_id IS '当前生效订单 id (客户端提交最新 或 检测到续费后的最新), 指向 biz.subscription_order.id';
COMMENT ON COLUMN biz.contact.created_at IS '创建时间';
COMMENT ON COLUMN biz.contact.updated_at IS '更新时间';

COMMENT ON TABLE biz.subscription_order IS '订阅订单表 (通用订单, 各功能模块共用)。记录初始购买 + 续费订单, 含商店原始订单 id 与回执';
COMMENT ON COLUMN biz.subscription_order.id IS '订单 id (自增), 联系人表 order_id 指向这里';
COMMENT ON COLUMN biz.subscription_order.user_id IS '下单用户 id (外键 wa_user.id)';
COMMENT ON COLUMN biz.subscription_order.module IS '功能模块标识, 默认 contact_subscription';
COMMENT ON COLUMN biz.subscription_order.contact_id IS '本模块关联的联系人 (其他模块可空)';
COMMENT ON COLUMN biz.subscription_order.platform IS '商店平台: 1=Google 2=Apple';
COMMENT ON COLUMN biz.subscription_order.store_order_id IS 'Google/Apple 原始订单 id';
COMMENT ON COLUMN biz.subscription_order.order_type IS '订单类型: initial_purchase / renewal';
COMMENT ON COLUMN biz.subscription_order.status IS '状态: pending/active/cancelled/expired/refunded';
COMMENT ON COLUMN biz.subscription_order.plan IS '套餐';
COMMENT ON COLUMN biz.subscription_order.amount IS '金额';
COMMENT ON COLUMN biz.subscription_order.currency IS '币种';
COMMENT ON COLUMN biz.subscription_order.purchased_at IS '购买时间';
COMMENT ON COLUMN biz.subscription_order.expires_at IS '到期时间';
COMMENT ON COLUMN biz.subscription_order.receipt_raw IS '商店原始回执 JSON (服务端校验留存)';
COMMENT ON COLUMN biz.subscription_order.created_at IS '创建时间';
COMMENT ON COLUMN biz.subscription_order.updated_at IS '更新时间';
