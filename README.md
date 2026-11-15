# 时间区间维表 + 事实表：接入端 / 投递进程 / HTTP 数仓

纯 Python 标准库（`sqlite3`、`http.server`、`urllib`、`json`）实现，无第三方依赖。
三个可独立启动的角色：

| 角色 | 入口 | 状态 |
| --- | --- | --- |
| 接入端（source） | `python3 -m tsdb.source --db source.db --port 8080` | 独立 SQLite 文件，初态空、版本 0 |
| 投递进程（delivery） | `python3 -m tsdb.delivery --source ... --warehouse ...` | 无自有状态，游标即数仓版本 |
| 数仓（warehouse） | `python3 -m tsdb.warehouse --db warehouse.db --port 8090` | 另一个独立 SQLite 文件，初态空、版本 0 |

两端各自持久化，互不共享文件；投递通过 HTTP 逐版推送。

代码结构：

```
tsdb/common.py      全部校验、JSON 规范化、错误路径（两端共用同一套规则）
tsdb/http.py        极简 JSON over HTTP 服务（HTTP/1.1，Content-Length）
tsdb/source.py      接入端：submit / snapshot / delta，区间推导，差量生成
tsdb/warehouse.py   数仓：apply / snapshot，整批原子应用与外键终检
tsdb/delivery.py    投递：按版本递增、重试、丢确认续传
tests/e2e.py        端到端验收（含真实子进程 SIGKILL 恢复）
```

运行测试：

```
python3 -m unittest tests.e2e -v
```

---

## 1. 接口

### 1.1 `POST /submit`（接入端）

请求：

```json
{"id": "s1", "d": [[record_id, [key,a,b,p,value] | null], ...],
                  "f": [[record_id, [key,t,reading]    | null], ...]}
```

* `id`：非空 ASCII 字母数字串；`d`、`f` 必须存在且为数组。
* `[record_id, null]` 表示删除该记录。
* 成功：`200 {"v": n}`，版本加一。
* 拒绝：`400 {"error": "<JSONPath>"}`，下标零基；JSON 语法错误报 `$`。拒绝不改变任何状态。

`submit` 的语义是"把这一批变更合并到当前源状态，并产生一个新版本"：

* 批内所有操作**同时生效**（不是数组顺序依次插值）。所以 `d` 里同一个 key 的多条声明会一起参与区间竞争。
* 记录 id 全局唯一（跨 `d`/`f` 两张表），可更新（同 id 的完整行即覆盖）。批内重复 id 拒绝；
  删除一个不存在的记录拒绝；空批合法（版本仍会加一，差量为空）。
* **先判重试**：若 `id` 已受理过，且请求体与当时受理的 JSON 结构相同（对象键序无关，数组顺序与取值有关），
  返回当时的版本号且不新增版本；结构不同则 `409 {"error": "$.id"}`。
  重试判定先于所有业务校验，因此即使被引用的记录后来被删，重放仍返回原版本。
* 并发提交等价于串行：写入用 `BEGIN IMMEDIATE`，全部校验、推导、落盘在同一事务内完成。

### 1.2 `GET /snapshot`（两端）

```json
{"v": n,
 "d": [[[decl_id, start, end], key, value], ...],
 "f": [[id, key, t, reading, [decl_id, start, end] | null], ...]}
```

行序不限（实现按主键排序输出，便于比对）。

### 1.3 `GET /delta/<v>`、`GET /deltas?after=<v>`（接入端，投递用）

* `/delta/<v>`：该版本的差量批 `{v,d,dd,f,fd}`，不存在则 404。
* `/deltas?after=<v>`：`{latest, batches:[...]}`，`batches` 只含版本 > v 的批，按版本升序。

### 1.4 `POST /apply`（数仓）

```json
{"v": n, "d": [[[sid,start,end], key, value], ...],
           "dd": [[sid,start,end], ...],
           "f": [[id,key,t,reading,s], ...],
           "fd": [id, ...]}
```

* `d`/`f` 是**替换行**（按主键 upsert），`dd`/`fd` 是**删除主键**；只包含相对上一版的净变化。
* 仅当应用后整张表的外键全部合法才整批原子生效，否则 `409 {"error": ...}` 且不改任何状态。
* 幂等：`v == 当前版本+1` 且内容不同则拒绝；`v <= 当前版本` 时，若与当时应用的批内容完全一致
  返回 `{"v": <该版本>}`，否则 `409`；跳号 `409`。
* 结构/取值非法（含布尔冒充整数、`a>=b`、越界、批内重复主键）同样 `400`，不改状态。

---

## 2. 维度推导

### 2.1 覆盖与获胜者

对每个 `key` 单独处理。声明记录的区间是 `[a, b)`（`a < b`）。

1. 取该 key 下所有 `a`、`b` 得到边界点，切成互不相邻的原子小区间。
2. 每个小区间（左闭右开）若被至少一条声明覆盖，则获胜者为：`p` 最大者；`p` 相同取 `record_id`
   按 ASCII 最小者。没有声明覆盖的小区间**不产生维度行**。
3. 相邻且胜者相同的连续小区间合并为一段。合并依据是"同一获胜声明 id"，而不是"值相同"：
   两个不同声明 id 即使 key/value 相同也各自成段。

维度行 `[s, key, value]`，主键 `s = [decl_id, 段起点, 段终点]`。`decl_id` 是获胜声明的记录 id，
所以同一声明若在多个不连续区间获胜，会产生多个 `s` 不同的行。

`p` 接受任意 JSON 整数（不限 int64）。`value` 为字符串或 `null`；`null` 仍是一条有效声明，
照常产生维度行并参与覆盖。

### 2.2 事实挂接

事实 `[id, key, t, reading]` 按 `key` 相同、`t` 落在 `[start, end)` 的维度行挂接，
写入 `f` 行的第五项 `s`；无覆盖则 `s = null`。挂接只看时间段的半开区间；
`reading` 可为 `null`，不影响挂接。

**声明变化必须重挂旧事实**：推导每次都对"当前全部事实 × 当前全部维度"重新求交，
差值由 `compute_delta` 与上一版本的派生表比较得出，因此移动、删除、新增声明都会自动
在新版本里重挂受影响的事实（见 §5 的示例）。

### 2.3 版本快照与不可变差量

`submit` 在一个事务里写入：

* `source_state.version`（版本号）
* `submission`（id → 版本 + 请求指纹），用于重试判定
* `decl` / `fact`（当前源记录，可变）
* `dim` / `factv`（当前派生表，可变）
* `delta`（本版差量，`INSERT` 后永不更新/删除）

所以"源状态、两表、不可变差量"是同一事务的原子结果；崩溃只会留下上一版或完整的新版，
WAL + `synchronous=FULL` 保证已提交事务不丢。

---

## 3. 投递

投递进程**不保存自己的游标**，以数仓 `GET /version` 为唯一水位：

1. 读数仓版本 `cur`，向接入端拉 `after=cur` 的批。
2. 只接受 `v == cur + 1` 的批，逐版 POST `/apply`，确认后才前进。
3. 传输失败按指数退避无限重试（默认 0.2s → 5s 上限）；4xx 视为不可恢复分歧，退出非零。
4. 丢确认：写已落库但响应丢失时，重试同一批；数仓按内容幂等返回 `{"v": v}`，
   不会重复应用也不会重写旧批。
5. 重启：从数仓版本继续，天然续传；接入端的 `delta` 表不可变，旧批永远可重放。

因此不存在跳版、重复应用或改写历史批的路径。

---

## 4. 验收

### 4.1 自动化

```
$ python3 -m unittest tests.e2e -v
Ran 28 tests ... OK
```

覆盖：题面示例（含补入 / 撤回 e）、优先级并列取 ASCII 小 id、相邻合并、空洞不建维度、
`null` 值声明与 `null` reading、批内重复 id、删除缺项、空批、语法错 `$`、路径定位、
重试先于校验、并发串行等价、apply 幂等 / 跳号 / 外键整批拒绝、投递续传、
以及真实子进程 SIGKILL 恢复。

### 4.2 手工复现（两端 + 投递）

```bash
rm -rf /tmp/demo && mkdir -p /tmp/demo
python3 -m tsdb.source    --db /tmp/demo/source.db    --port 8080 &
python3 -m tsdb.warehouse --db /tmp/demo/warehouse.db --port 8090 &
python3 -m tsdb.delivery  --source http://127.0.0.1:8080 \
                          --warehouse http://127.0.0.1:8090 &

curl -s localhost:8080/submit -d '{"id":"s1","d":[["d",["k",0,20,1,null]]],"f":[["f",["k",15,7]]]}'
# {"v":1}
curl -s localhost:8080/submit -d '{"id":"s2","d":[["e",["k",10,20,2,"x"]]],"f":[]}'
# {"v":2}
curl -s localhost:8080/submit -d '{"id":"s3","d":[["e",null]],"f":[]}'
# {"v":3}
curl -s localhost:8080/snapshot
curl -s localhost:8090/snapshot   # 与上一行一致（v=3, d=[[["d",0,20],"k",null]], f=[[f,k,15,7,["d",0,20]]]）
```

`v=2` 时数仓应看到 `d=[[["d",0,10],"k",null],[["e",10,20],"k","x"]]`，
`f=[["f","k",15,7,["e",10,20]]]`——旧事实被重挂到 e；`v=3` 撤回 e 后恢复 `["d",0,20]`。

### 4.3 强退恢复

```bash
# 提交两版后强杀接入端
kill -9 <source pid>
python3 -m tsdb.source --db /tmp/demo/source.db --port 8080 &
curl -s localhost:8080/snapshot      # 版本与两表完整，无半写状态

# 强杀投递进程后重启，自动从数仓版本续传；强杀数仓后重启再投递，结果不变
```

自动化版本见 `tests/e2e.py::TestHardKill::test_sigkill_between_versions_keeps_consistency`，
它对三个角色分别 `os.kill(pid, SIGKILL)` 后校验：接入端版本与派生表一致、投递可从断点续传、
数仓重启后重复投递幂等且版本不回退。

---

## 5. 边界与约定

* 标识符：非空 ASCII 字母数字串（`^[A-Za-z0-9]+$`），键、声明 id、事实 id、提交 id 同规则。
* `a`、`b`、`t`：非负整数秒，上限 `2^63-1`（SQLite 有符号 64 位）；`a < b` 必须成立。
* `reading`：int64 或 `null`；`value`：字符串或 `null`（空串合法）。
* 所有"整数"位置都拒绝布尔（Python 的 `bool` 是 `int` 子集，已显式排除），也拒绝浮点与字符串。
* `p` 为任意 JSON 整数，参与比较时按数值，不受 int64 限制。
* 请求体必须是 UTF-8 JSON；重复对象键、`NaN`/`Infinity` 视为语法错误（`$`）。
* 请求对象只允许协议字段（`submit`：`id`/`d`/`f`，`apply`：`v`/`d`/`dd`/`f`/`fd`）；多余成员按
  该成员路径拒绝，这样"同 JSON 结构"有唯一含义。
* 词典序 / ASCII 序均按 Python `str` 比较（码位序），与 ASCII 序一致。
