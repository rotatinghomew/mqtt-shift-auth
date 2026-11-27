# 检验告警订阅的客户端身份与接班

检验线消息入口按已核验的连接事实连同订阅登记、消息及接收回执进行本地联调。这些资料不需要外部 MQTT 设施。

## 联调资料

`inputs/` 中每份 JSON 以该来源自己的记录编号为键。记录编号作用于对应来源，检测时可同时查询原始编号与业务引用。相关内容均为合成，回环只读来源用于本机开发。

- `devices.json`。连线认证方给出证书持有事实。client_name 是客户端自报的会话名。实际实例以 instance 与 generation 判定，production_line 和 tenant 由资产方给出。
- `subscriptions.json`。公共设备状态 latest 在其现行租户与产线内保留最后一条。值班组的成员可获得标为 group 的告警，instance-private 限最初分配的实例；跨班交接受组的配置约束。
- `messages.json`。topic 属于本地演练的 MQTT 主题空间。消息同时给出发送实例、设备归属代次及有效的接收组。deliveries 的报文号只在连接内使用，重发会改变网络报文号，原消息号及原办理引用仍可核对。
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
