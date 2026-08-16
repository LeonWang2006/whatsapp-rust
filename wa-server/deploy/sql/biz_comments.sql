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
