-- wa-server PostgreSQL schema comments (中文留存)
-- 用法: podman exec -i wa-postgres psql -U postgres -d mydb -f /tmp/table_comments.sql
-- 说明: 基础设施表由 storages/postgres-storage 的 Diesel 迁移自动创建,
--       本文件只写注释,不改表结构。业务表设计可参照此注释对齐外键。

-- ============ 一、核心表 ============

COMMENT ON TABLE public.device IS 'WhatsApp 设备凭据表。一行 = 一台已关联的 WhatsApp 设备（一个手机号在服务器上的登录身份），wa-server 用它存储 Signal 密钥与账号凭据，登录/恢复都依赖它';
COMMENT ON COLUMN public.device.id IS '设备主键 ID，wa-server 按此 ID 路由设备';
COMMENT ON COLUMN public.device.lid IS 'Linked ID，WhatsApp 新版隐私 ID 体系中的链接 ID';
COMMENT ON COLUMN public.device.pn IS '手机号，格式如 8618666206882:5@s.whatsapp.net';
COMMENT ON COLUMN public.device.registration_id IS 'Signal 协议注册 ID';
COMMENT ON COLUMN public.device.noise_key IS 'Noise 握手密钥，连接建立时用于加密握手';
COMMENT ON COLUMN public.device.identity_key IS 'Signal 长期身份密钥';
COMMENT ON COLUMN public.device.signed_pre_key IS '签名预密钥，E2E 握手用';
COMMENT ON COLUMN public.device.signed_pre_key_id IS '签名预密钥 ID';
COMMENT ON COLUMN public.device.signed_pre_key_signature IS '签名预密钥的签名';
COMMENT ON COLUMN public.device.adv_secret_key IS '高级密钥，用于历史同步数据解密';
COMMENT ON COLUMN public.device.account IS '账号 blob，登录成功后写入的登录凭证';
COMMENT ON COLUMN public.device.push_name IS '推送显示名（WhatsApp 设置中的昵称）';
COMMENT ON COLUMN public.device.app_version_primary IS '客户端版本号-主版本';
COMMENT ON COLUMN public.device.app_version_secondary IS '客户端版本号-次版本';
COMMENT ON COLUMN public.device.app_version_tertiary IS '客户端版本号-补丁版本';
COMMENT ON COLUMN public.device.app_version_last_fetched_ms IS '最近一次获取版本的时间戳(ms)';
COMMENT ON COLUMN public.device.edge_routing_info IS '边缘路由信息';
COMMENT ON COLUMN public.device.props_hash IS '客户端能力(属性)配置的 hash';
COMMENT ON COLUMN public.device.next_pre_key_id IS '下一个待生成的预密钥 ID 游标';
COMMENT ON COLUMN public.device.nct_salt IS 'NCT 盐值';
COMMENT ON COLUMN public.device.server_has_prekeys IS '服务器是否已保存本设备的预密钥';
COMMENT ON COLUMN public.device.first_unupload_pre_key_id IS '第一个未上传到服务器的预密钥 ID';
COMMENT ON COLUMN public.device.server_cert_chain IS '服务器证书链';
COMMENT ON COLUMN public.device.login_counter IS '登录次数计数';
COMMENT ON COLUMN public.device.lid_migrated IS '是否已完成 LID 迁移';
COMMENT ON COLUMN public.device.last_signed_pre_key_rotation_ms IS '上次签名预密钥轮换时间(ms)';
COMMENT ON COLUMN public.device.read_receipts_disabled IS '是否关闭已读回执';

-- ============ 二、Signal 加密层 ============

COMMENT ON TABLE public.sessions IS 'Signal 加密会话表。每行是与一个对端(地址+设备)建立的 E2E 会话记录';
COMMENT ON COLUMN public.sessions.address IS '对端地址（JID + 设备号）';
COMMENT ON COLUMN public.sessions.record IS '会话记录 blob（Signal 加密上下文）';
COMMENT ON COLUMN public.sessions.device_id IS '所属本地设备 ID（外键 device.id）';

COMMENT ON TABLE public.prekeys IS '一次性预密钥表。Signal 协议用于建立会话的预密钥';
COMMENT ON COLUMN public.prekeys.id IS '预密钥 ID';
COMMENT ON COLUMN public.prekeys.key IS '预密钥公钥 blob';
COMMENT ON COLUMN public.prekeys.uploaded IS '是否已上传到服务器';
COMMENT ON COLUMN public.prekeys.device_id IS '所属设备 ID';

COMMENT ON TABLE public.signed_prekeys IS '签名预密钥表。长期有效，用于 E2E 握手';
COMMENT ON COLUMN public.signed_prekeys.id IS '签名预密钥 ID';
COMMENT ON COLUMN public.signed_prekeys.record IS '签名预密钥记录 blob';
COMMENT ON COLUMN public.signed_prekeys.device_id IS '所属设备 ID';

COMMENT ON TABLE public.base_keys IS '基密钥表。按消息粒度记录基密钥，用于 Signal 会话派生';
COMMENT ON COLUMN public.base_keys.address IS '对端地址';
COMMENT ON COLUMN public.base_keys.message_id IS '消息 ID';
COMMENT ON COLUMN public.base_keys.base_key IS '基密钥 blob';
COMMENT ON COLUMN public.base_keys.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.base_keys.created_at IS '创建时间(epoch)';

COMMENT ON TABLE public.identities IS '身份密钥表。存储对端的身份公钥，用于验证消息签名';
COMMENT ON COLUMN public.identities.address IS '对端地址';
COMMENT ON COLUMN public.identities.key IS '对端身份公钥 blob';
COMMENT ON COLUMN public.identities.device_id IS '所属设备 ID';

COMMENT ON TABLE public.sender_keys IS '群组发送密钥表。用于群聊的 E2E 加密';
COMMENT ON COLUMN public.sender_keys.address IS '群组地址';
COMMENT ON COLUMN public.sender_keys.record IS '发送密钥记录 blob';
COMMENT ON COLUMN public.sender_keys.device_id IS '所属设备 ID';

COMMENT ON TABLE public.sender_key_devices IS '群组发送密钥设备状态表。记录群内各设备是否已持有发送密钥';
COMMENT ON COLUMN public.sender_key_devices.group_jid IS '群组 JID';
COMMENT ON COLUMN public.sender_key_devices.device_jid IS '群内设备 JID';
COMMENT ON COLUMN public.sender_key_devices.has_key IS '是否已持有密钥（0/1）';
COMMENT ON COLUMN public.sender_key_devices.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.sender_key_devices.updated_at IS '更新时间(epoch)';

COMMENT ON TABLE public.msg_secrets IS '消息密钥表。记录每条消息的解密密钥';
COMMENT ON COLUMN public.msg_secrets.chat IS '会话 JID';
COMMENT ON COLUMN public.msg_secrets.sender IS '发送者 JID';
COMMENT ON COLUMN public.msg_secrets.msg_id IS '消息 ID';
COMMENT ON COLUMN public.msg_secrets.secret IS '消息密钥 blob';
COMMENT ON COLUMN public.msg_secrets.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.msg_secrets.created_at IS '创建时间(epoch)';
COMMENT ON COLUMN public.msg_secrets.expires_at IS '过期时间(epoch，0=不过期)';
COMMENT ON COLUMN public.msg_secrets.message_ts IS '消息时间戳(epoch)';

-- ============ 三、消息层 ============

COMMENT ON TABLE public.sent_messages IS '已发送消息表。记录已发送消息的加密载荷';
COMMENT ON COLUMN public.sent_messages.chat_jid IS '会话 JID';
COMMENT ON COLUMN public.sent_messages.message_id IS '消息 ID';
COMMENT ON COLUMN public.sent_messages.payload IS '加密后的消息载荷';
COMMENT ON COLUMN public.sent_messages.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.sent_messages.created_at IS '创建时间(epoch)';

COMMENT ON TABLE public.pending_inbound_messages IS '待处理入站消息表。乱序到达的消息先暂存，等前置消息就绪后解密';
COMMENT ON COLUMN public.pending_inbound_messages.chat IS '会话 JID';
COMMENT ON COLUMN public.pending_inbound_messages.sender IS '发送者 JID';
COMMENT ON COLUMN public.pending_inbound_messages.id IS '消息 ID';
COMMENT ON COLUMN public.pending_inbound_messages.message IS '消息 blob';
COMMENT ON COLUMN public.pending_inbound_messages.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.pending_inbound_messages.inserted_at IS '插入时间(epoch)';

COMMENT ON TABLE public.tc_tokens IS '信任 token 表。用于消息去重/重放防护';
COMMENT ON COLUMN public.tc_tokens.jid IS '地址 JID';
COMMENT ON COLUMN public.tc_tokens.token IS 'token blob';
COMMENT ON COLUMN public.tc_tokens.token_timestamp IS 'token 时间戳';
COMMENT ON COLUMN public.tc_tokens.sender_timestamp IS '发送者时间戳';
COMMENT ON COLUMN public.tc_tokens.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.tc_tokens.updated_at IS '更新时间(epoch)';

-- ============ 四、应用状态同步层 ============

COMMENT ON TABLE public.app_state_keys IS '应用状态密钥表。AppState(标签/设置等)同步用密钥';
COMMENT ON COLUMN public.app_state_keys.key_id IS '状态密钥 ID';
COMMENT ON COLUMN public.app_state_keys.key_data IS '状态密钥数据 blob';
COMMENT ON COLUMN public.app_state_keys.device_id IS '所属设备 ID';

COMMENT ON TABLE public.app_state_versions IS '应用状态版本表。记录 AppState 的版本与数据';
COMMENT ON COLUMN public.app_state_versions.name IS '状态名（如 label）';
COMMENT ON COLUMN public.app_state_versions.state_data IS '状态数据 blob';
COMMENT ON COLUMN public.app_state_versions.device_id IS '所属设备 ID';

COMMENT ON TABLE public.app_state_mutation_macs IS '应用状态变更 MAC 表。校验 AppState 变更完整性';
COMMENT ON COLUMN public.app_state_mutation_macs.name IS '状态名';
COMMENT ON COLUMN public.app_state_mutation_macs.version IS '版本号';
COMMENT ON COLUMN public.app_state_mutation_macs.index_mac IS '索引校验 MAC';
COMMENT ON COLUMN public.app_state_mutation_macs.value_mac IS '值校验 MAC';
COMMENT ON COLUMN public.app_state_mutation_macs.device_id IS '所属设备 ID';

-- ============ 五、元数据层 ============

COMMENT ON TABLE public.group_metadata IS '群组元数据表。缓存群信息';
COMMENT ON COLUMN public.group_metadata.group_jid IS '群组 JID';
COMMENT ON COLUMN public.group_metadata.info IS '群信息 blob';
COMMENT ON COLUMN public.group_metadata.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.group_metadata.updated_at IS '更新时间(epoch)';

COMMENT ON TABLE public.lid_pn_mapping IS 'LID 与手机号映射表。WhatsApp 隐私 ID 体系下 LID<->PN 双向映射';
COMMENT ON COLUMN public.lid_pn_mapping.lid IS 'LID';
COMMENT ON COLUMN public.lid_pn_mapping.phone_number IS '手机号';
COMMENT ON COLUMN public.lid_pn_mapping.created_at IS '创建时间(epoch)';
COMMENT ON COLUMN public.lid_pn_mapping.learning_source IS '学习来源（如历史同步）';
COMMENT ON COLUMN public.lid_pn_mapping.updated_at IS '更新时间(epoch)';
COMMENT ON COLUMN public.lid_pn_mapping.device_id IS '所属设备 ID';

COMMENT ON TABLE public.device_registry IS '设备注册表。记录对端用户的设备列表';
COMMENT ON COLUMN public.device_registry.user_id IS '用户 JID';
COMMENT ON COLUMN public.device_registry.devices_json IS '设备列表 JSON';
COMMENT ON COLUMN public.device_registry.timestamp IS '时间戳';
COMMENT ON COLUMN public.device_registry.phash IS '能力 hash';
COMMENT ON COLUMN public.device_registry.device_id IS '所属设备 ID';
COMMENT ON COLUMN public.device_registry.updated_at IS '更新时间(epoch)';
COMMENT ON COLUMN public.device_registry.raw_id IS '原始 ID';

COMMENT ON TABLE public.jid_device_map IS 'JID 到设备的路由表。wa-server 按此路由每个 JID 到对应 device';
COMMENT ON COLUMN public.jid_device_map.jid IS 'JID（如 8618666206882@s.whatsapp.net）';
COMMENT ON COLUMN public.jid_device_map.device_id IS 'device 表 ID';
COMMENT ON COLUMN public.jid_device_map.created_at IS '创建时间(epoch)';

-- ============ 六、工具表 ============

COMMENT ON TABLE public.__diesel_schema_migrations IS 'Diesel 迁移版本表，由 Diesel 自动管理，勿手改';
COMMENT ON COLUMN public.__diesel_schema_migrations.version IS '迁移版本号';
COMMENT ON COLUMN public.__diesel_schema_migrations.run_on IS '执行时间';
