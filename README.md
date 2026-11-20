# distributed-lock-fencing-validator

基于 **逻辑时间 + fencing token** 的分布式锁与资源写入离线验证器，
使用 Python 3.8.2 兼容语法（无 wall clock 依赖、无第三方依赖）。

## 它解决什么问题

只检查租约是否过期，并不能证明资源写入仍然安全：旧持有者的请求
可能在失去锁之前就已经进入网络，锁丢失后才晚到资源端；客户端时钟
漂移、GC 停顿、主备切换都会让“我还持有租约”的本地判断失效。

本验证器实现 Kleppmann 风格的 fencing 方案：

1. 每次成功获取锁，锁服务铸造一个**严格递增**的 fencing token；
2. 客户端携带 token 写入资源；
3. 资源端保存已接受的最大 token（高水位），拒绝一切
   `token <= 高水位` 的写入——即使旧客户端仍认为自己持有租约。

## 重要边界：这不是网络锁服务

本仓库是一个**确定性的离线状态机 / 教学与校验工具**，不实现可部署
的网络锁服务：

- 没有真实网络、套接字、并发线程或进程间协议；
- 不读取系统时钟（无 `time.time()` / `datetime.now()`），租约到期
  只比较输入事件携带的逻辑时间；
- 不实现真实持久化介质，主备切换、缓存失效均由输入事件显式驱动；
- 网络延迟通过“晚到事件”建模：事件按到达顺序送入，事件上的
  `at` 是逻辑时间戳，晚到请求与续租会自然落到租约到期之后。

真实系统中的锁服务还需要共识协议（如 etcd/ZooKeeper/Raft）、
持久化 WAL、会话管理与重试策略，这些均不在本验证器范围内。

## 目录结构

- `fencing_validator/events.py`：输入事件与结果数据模型；
- `fencing_validator/lock_service.py`：锁服务状态机（申请/续租/
  释放、token 计数器、持有者重启、主备切换与恢复）；
- `fencing_validator/resource.py`：资源端 fencing 守卫（持久化与
  缓存双高水位、写入日志与重建）；
- `fencing_validator/validator.py`：编排验证器，消费事件并产出
  每次写入结论、违规事件与可恢复状态；
- `tests/test_validator.py`：全部场景的单元测试。

## 使用方式

```python
from fencing_validator import (
    Acquire, FencingValidator, Release, Tick, Write,
)

v = FencingValidator()
v.process([
    Acquire(at=1, client_id="A", ttl=5),
    Write(at=2, client_id="A", token=1, payload="a1"),
    Tick(at=7),                       # 纯逻辑时间推进，租约到期
    Acquire(at=8, client_id="B", ttl=10),
    Write(at=9, client_id="B", token=2, payload="b1"),
    Write(at=10, client_id="A", token=1, payload="late"),  # 晚到
])

print(v.write_decisions())      # 每次写入的 ACCEPT / REJECT 与高水位
print(v.current_high_token)     # 当前最高 token
print(v.safety_violations())    # 违反安全约束的事件
print(v.recoverable_state())    # 可用于主备恢复的状态快照
```

事件类型（均携带逻辑时间 `at`）：

| 事件 | 含义 |
| --- | --- |
| `Acquire` | 申请锁，成功则获得下一个严格递增 token |
| `Renew` | 续租；租约已到期/释放或 token 不匹配则拒绝 |
| `Release` | 显式释放；token 不匹配时拒绝，防止误释放新租约 |
| `Write` | 携带 token 的资源写入，由资源端高水位守门 |
| `ClientRestart` | 持有者重启，服务端吊销其未到期租约 |
| `Failover` | 服务端主备切换，`durable=False` 模拟持久化丢失 |
| `CacheFlush` | 资源端缓存失效，回退到持久化高水位 |
| `Tick` | 仅推进逻辑时间，让租约自然到期 |

## 安全约束与违规代码

资源端的硬性规则是 **token 必须严格大于已接受高水位**，任何回退、
重复或伪造 token 都会被拒绝。验证器同时记录以下违规事件：

| 代码 | 含义 |
| --- | --- |
| `STALE_FENCING_TOKEN` | token 不大于资源端高水位，写入被拒绝 |
| `INVALID_TOKEN` | 非正数 token |
| `UNKNOWN_FENCING_TOKEN` | token 从未由锁服务铸造（伪造/失忆节点） |
| `STALE_LEASE_WRITE` | 客户端在租约失效后仍发起写入（晚到请求） |
| `STALE_LEASE_OPERATION` | 续租/释放晚到、重复释放等失效租约操作 |
| `LEASE_OVERLAP` | 与当前有效租约重叠的并发申请，被拒绝 |
| `TOKEN_MISMATCH` | 续租/释放使用的 token 与当前持有者不符 |
| `HOLDER_RESTART` | 持有者重启，其租约被吊销 |
| `FAILOVER_STATE_LOSS` | 非持久化主备切换丢失状态，服务进入不可用 |
| `COUNTER_RECOVERED` | 以资源端持久化高水位为下界恢复计数器 |

### 已知的 fencing 固有局限

若旧持有者的晚到写入发生在**新持有者第一次写入之前**，资源端高水位
尚未抬升，仅凭 token 无法拒绝它（测试
`test_late_write_before_new_holder_writes_is_flagged` 演示该情形）。
验证器会把它标记为 `STALE_LEASE_WRITE`；生产系统通常还需要租约版本
号二次校验，或要求交接前新持有者先写入以抬升高水位。

## 主备切换与恢复

- **持久化切换**（`Failover(durable=True)`）：备用节点从快照恢复
  token 计数器与租约，下一个 token 继续严格递增；
- **非持久化切换**（`durable=False`）：锁服务进入不可用状态并拒绝
  一切 `Acquire`，必须调用 `recover_after_state_loss()`，以资源端
  持久化高水位为下界重建计数器，确保之后铸造的 token 大于所有已
  接受写入的 token；
- **资源端缓存丢失**（`CacheFlush`）：易失缓存清零后回退到持久化
  高水位，回退 token 依旧被拒绝；`FencingResource.recover()` 可从
  持久化写入日志重建高水位。

## 运行测试

```bash
python -m unittest
```

覆盖场景：顺序交接、旧写入晚到、token 回退/重复/伪造、并发申请与
租约重叠、续租与释放晚到、持有者重启、资源端缓存失效、持久化与
非持久化主备切换、逻辑时间倒退保护与可恢复状态快照。
