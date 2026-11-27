# 检验告警订阅的客户端身份与接班

检验线消息入口按已核验的连接事实连同订阅登记、消息及接收回执进行本地联调。这些资料不需要外部 MQTT 设施。

## 联调资料

`inputs/` 中每份 JSON 以该来源自己的记录编号为键。记录编号作用于对应来源，检测时可同时查询原始编号与业务引用。相关内容均为合成，回环只读来源用于本机开发。

- `devices.json`。连线认证方给出证书持有事实。client_name 是客户端自报的会话名。实际实例以 instance 与 generation 判定，production_line 和 tenant 由资产方给出。ownership-change 的 at 是归属实际交接时刻。
- `subscriptions.json`。公共设备状态 latest 在其现行租户与产线内保留最后一条。值班组的成员可获得标为 group 的告警，instance-private 限最初分配的实例；跨班交接受组的配置约束。
- `messages.json`。topic 属于本地演练的 MQTT 主题空间。消息同时给出发送实例、设备归属代次及有效的接收组。报文号从发送方协议会话的编号空间分配。同一持久会话断线后恢复，重发未确认的 QoS 1 PUBLISH 时沿用原报文号并置 DUP 标志。接班另开一次业务交付时使用新办理引用，新会话仍可能分配到和原会话相同的报文号。原消息号及各次办理引用均可核对。
- `deliveries.json`。accepted 表示连接确收，sent_unknown 只表示发送后未确认。ack 按连接、订阅实例、组和办理引用解释。接收观察时间取虚拟时钟秒数。 group-only 允许接班时为组告警开启 QoS 1 的独立补投尝试。旧会话的回执只证明其自身观察到的结果。新办理引用已经分配后，接班号的状态以自己实际收到的回执为准。

## 运行

使用 CPython 3.11。查看来源或读取一项观测。

```bash
python3 source_reader.py show
python3 source_reader.py show devices sensor-old
python3 source_reader.py serve --port 8172
```

读取端只监听回环地址。使用 `GET /observations` 列出来源，`GET /observations/{source}` 读一份原始资料，`GET /observations/{source}/{record}` 取一项记录。未登记的来源或记录返回 404。通过 `--inputs` 可以连接另一组相同格式的本地资料。

执行资料与联调入口检查。

```bash
python3 -m unittest discover -s tests -v
```

## 报文样本

使用 `python3 mqtt_samples.py` 查看持久会话 CONNECT、订阅、QoS 1 发布及接收确认的报文样本。连接的已验证证书持有及设备归属由 `devices.json` 提供。

## 本地消息入口

`message_gateway.py` 在上述已核验事实之上实现本地消息入口，不依赖外部 MQTT 设施；`mqtt_codec.py` 负责与 `mqtt_samples.py` 互通的 MQTT 3.1.1 报文解析与构造。

### 身份与值班目录

- 连接只接受 `certificate_verified` 为真的已登记设备；自报的 `client_name` 与登记不符直接拒绝（CONNACK 标识符拒绝）。实例（instance）与部署代次（generation）才是身份本身，client_name 不是认领凭证。
- 值班目录按实例分配订阅组、数据类别（alerts/latest）与班次范围；同一连接可以分别获准公共设备状态与内部告警两类订阅，越权订阅在 SUBACK 中返回 0x80。
- 换租户或重新分配的设备名不能认领旧队列：租户/产线不符记 `tenant-mismatch`，新代次没有值班登记记 `device-generation`，均隔离处理。

### 接续与接班

- 同一实例以持久会话重连时只做续传：未确认的 QoS 1 PUBLISH 沿用原报文号并置 DUP，PUBACK 仍落在原办理引用（attempt）上。
- 新连接携带旧 client_name 且实例不同时，按消息产生时的租户/产线归属、组的 `group-only` 交接范围和当前订阅者身份逐条核对：组告警可由同组下一班以新办理引用补投（新会话可再次分到相同报文号）；实例私有消息只能向原订阅者核对（`verify-original`），新成员无权领取。
- 旧连接的迟到 PUBACK 在新办理引用分配后被拒绝，不能抹掉新队列；一条告警在两个组的领取与确认互相独立。
- 订阅撤回与中断相遇时，已交付的收件记录保留；组待办仍可交接，实例私有待办隔离。未确认内容分为 `safe-redeliver`、`verify-original`、`quarantine` 三类，见 `Gateway.unconfirmed()`。

### 公共状态与诊断

- latest 公共状态按租户与产线保留最后一条；读取人仍须具备范围相符的身份。设备交接给另一租户后，旧状态不成为新租户的当前状态。
- `message_status(device, message)` 供设备运维查询消息此刻还在等哪个组、是否因接收资格或设备代次改变而停止交付；只有同租户同产线的管辖方，且只能看到自己所在组的领取状态。

### 运行

```bash
python3 message_gateway.py            # 端到端字节演练
python3 -m unittest discover -s tests -v
```

领域 API 与字节 API 的入口分别是 `Gateway.open_connection / subscribe / publish_device / acknowledge / handoff` 和 `Gateway.connect_from_packet / Connection.feed`。
