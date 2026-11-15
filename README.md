# 观测站 OPC UA 通知的确认与失联补收

这是资料接收程序的基础版。它请求来源接口，保留响应状态、头部、原始内容与每次观察的时间，能够从 JSON、CSV 或门户嵌入的数据块读回来源记录。当前不生成面向业务的有效版本，也不自动执行门户提交；重复观察会分别保留，原始内容按摘要复用。

## 运行环境

使用 Python 3.11 及以上，业务运行依赖均为标准库；SQLite 由 Python 自带。时区资料使用运行环境内的系统数据库。运行过程不安装依赖，也不下载数据。本地来源服务器和接收程序在同一进程环境中即可运行。

构建工具及其传递依赖精确锁定在 `requirements-build.txt`。首次准备开发环境时先执行下面命令；之后测试、构建和演示均可离线运行，不在运行命令中安装包。

```sh
python3 -m pip install -r requirements-build.txt
```

```sh
python3 -m unittest discover -s tests -v
python3 -m build --wheel --no-isolation
python3 -m compileall -q field_observatory tests
python3 -m field_observatory demo
python3 -m field_observatory drill
```

演示从空的临时目录启动本地来源，真实完成一次 HTTP 获取及归档，并输出原样记录；结束自动清理。本地测试、编译和演示合计一般不超过十秒，可直接展示，不依赖历史日志。

## 常驻来源与接收

```sh
python3 -m field_observatory serve
```

终端会输出本次分配的回环地址。在另一终端将下面地址替换为实际地址：

```sh
python3 -m field_observatory capture --base-url http://127.0.0.1:实际端口 --archive .runtime/raw.sqlite3 --source local-source --path /feed
```

归档库只追加来源观察，不用最后一条响应覆盖先前响应。`archive.py` 管理原始内容及完整性，`client.py` 执行一次请求，`decoding.py` 负责来源格式解码，`origin.py` 提供协议演练。业务解释可与原始接收独立演进。

## 订阅通知的可靠接收

`subscriptions.py` 实现先写后确认的接收：每条网关信封在一个事务内完成校验以及消息、监控项绑定、观测（含每条值的质量与源时间）的落库，并把确认排入本地可审计队列；只有事务提交成功后，`reception.py` 才把确认发往演练网关（POST `/acks`，网关按到达顺序记入请求账本）。岸站重启时未发送的确认仍在队列中，恢复后补发；网关重发的消息按 (站点, 代际, 序号, 句柄) 去重，不重复形成观测。

- 序号按 (站点, 订阅代际) 独立跟踪，是非零 32 位计数并回绕到 1，不是全站流水；保活占用序号、需要确认，但不产生观测；重建订阅启用新代际，复用的序号与监控句柄不会与旧代际混淆。
- 序号断档登记为待补收；接收端向网关请求 Republish，服务器已把报文移出重传队列（返回 Bad 类状态）时登记为永久缺口，不用当前值填平历史。每个断档每次运行最多补收一次、累计尝试有上限（默认 3 次）；未决的断档不阻塞其他序号与其他站点的写入和确认。
- 监控项绑定按代际与序号生效：观测按“不超过其序号的最近一次绑定”解析传感器；绑定更新与在途通知交错时，旧句柄不会被解释成新传感器；无法解析的句柄保持未解析，不猜测。

```sh
python3 -m field_observatory subscribe --base-url http://127.0.0.1:实际端口 --state .runtime/reception.sqlite3
python3 -m field_observatory status --state .runtime/reception.sqlite3
python3 -m field_observatory drill
```

`subscribe` 完成一轮接收（落库、确认、补收），也可用 `--from-file` 只读取本地信封文件；`status` 按站点输出已收观测（含质量与源时间）、仍可补收的序号与永久缺口；`drill` 从空临时目录启动本地网关，完整演练接收、断档补收、确认账本与整批重放，结束自动清理。演练只使用本地网关记录，不连接或控制真实设备。

## 开发资料

来源协议见 `docs/source-protocol.md`，领域观察关系见 `tests/test_source_contract.py`。`fixtures/business_cases.json` 保存业务侧关注的交错情况及可观察口径；基础版测试验证的是原始采集和协议，不代表业务处理已经具备这些能力。所有条目、机构、文件内容和门户回执均为本地合成资料，不包含真实用户身份。

## Wheel 分发

构建产物位于 `dist/`，包含 `field_observatory` 和命名空间内的合成来源 JSON。安装 wheel 后，即使当前目录不是源码目录，也可执行 `python3 -m field_observatory demo`。提供 `serve --scenario` 时仍以指定文件为准，找不到指定文件会直接报错，不回退到默认资料。
