"""锁与 fencing token 验证器（离线、确定性状态机）。

验证器消费一组按到达顺序排列、且携带逻辑时间的输入事件，模拟：

1. 锁服务的申请 / 续租 / 释放 / 持有者重启 / 主备切换；
2. 资源端基于 fencing token 高水位的写入守卫；
3. 网络延迟造成的请求晚到与 token 回退。

它是一个验证与教学工具，不是可部署的网络锁服务：没有真实网络、
真实时钟、并发线程或持久化介质，所有时序均由输入的逻辑时间决定。
"""

from typing import Any, Dict, List, Optional

from .events import (
    Acquire,
    AcquireResult,
    CacheFlush,
    ClientRestart,
    Event,
    Failover,
    RecoverableState,
    Release,
    ReleaseResult,
    Renew,
    RenewResult,
    Tick,
    Violation,
    Write,
    WriteResult,
)
from .lock_service import LockService
from .resource import FencingResource


class FencingValidator:
    def __init__(self):
        self.lock_service = LockService()
        self.resource = FencingResource()
        self.now = 0
        self.write_results = []  # type: List[WriteResult]
        self.acquire_results = []  # type: List[AcquireResult]
        self.renew_results = []  # type: List[RenewResult]
        self.release_results = []  # type: List[ReleaseResult]
        self.violations = []  # type: List[Violation]

    @property
    def current_high_token(self) -> int:
        """当前最高 token：已接受写入高水位与已铸造 token 的最大值。"""
        return max(
            self.lock_service.max_issued_token,
            self.resource.effective_high_water_mark,
        )

    def _advance(self, at: int) -> None:
        if at < self.now:
            raise ValueError(
                "逻辑时间不能倒退：事件 at=%d 小于当前时间 %d"
                % (at, self.now))
        self.now = at

    def _record(self, violation: Optional[Violation]) -> None:
        if violation is not None:
            self.violations.append(violation)

    def process(self, events: List[Event]) -> None:
        for event in events:
            self._advance(event.at)
            handler = self._handlers.get(type(event))
            if handler is None:
                raise TypeError("不支持的事件类型: %r" % type(event))
            handler(self, event)

    def _acquire(self, event: Acquire) -> None:
        result, violation = self.lock_service.acquire(
            event.at, event.client_id, event.ttl)
        self.acquire_results.append(result)
        self._record(violation)

    def _renew(self, event: Renew) -> None:
        result, violation = self.lock_service.renew(
            event.at, event.client_id, event.token, event.ttl)
        self.renew_results.append(result)
        self._record(violation)

    def _release(self, event: Release) -> None:
        result, violation = self.lock_service.release(
            event.at, event.client_id, event.token)
        self.release_results.append(result)
        self._record(violation)

    def _write(self, event: Write) -> None:
        token = event.token
        # 非正数 token 直接由资源端的数值校验拒绝。
        if token <= 0:
            result, violation = self.resource.write(
                event.at, event.client_id, token, event.payload)
            self.write_results.append(result)
            self._record(violation)
            return

        # 资源端是独立的安全边界，但从未铸造过的 token 必须在到达
        # 资源写入路径前被拒绝，避免伪造 token 抬高高水位。
        if not self.lock_service.is_known_token(token):
            hwm = self.resource.effective_high_water_mark
            result = WriteResult(
                event.at, event.client_id, token, False, hwm,
                "UNKNOWN_FENCING_TOKEN")
            self.write_results.append(result)
            self._record(Violation(
                event.at, "UNKNOWN_FENCING_TOKEN", event.client_id, token,
                "token=%d 从未由锁服务铸造，资源端拒绝写入" % token))
            return

        owner = self.lock_service.owner_of(token)
        if owner is not None and owner != event.client_id:
            self._record(Violation(
                event.at, "TOKEN_OWNER_MISMATCH", event.client_id, token,
                "token=%d 属于客户端 %s，而非写入发起方 %s"
                % (token, owner, event.client_id)))

        lease = self.lock_service.lease_for(event.client_id, event.at)
        stale_lease = lease is None or lease.token != token

        # 资源端只比较 fencing token 高水位，不依赖锁服务的租约判断。
        result, violation = self.resource.write(
            event.at, event.client_id, token, event.payload)
        self.write_results.append(result)
        self._record(violation)

        if stale_lease:
            self._record(Violation(
                event.at, "STALE_LEASE_WRITE", event.client_id, token,
                "客户端 %s 在不再持有 token=%d 有效租约的情况下发起写入"
                "（典型的晚到请求）；资源端结果：%s"
                % (event.client_id, token,
                   "接受" if result.accepted else "拒绝")))

    def _client_restart(self, event: ClientRestart) -> None:
        self._record(
            self.lock_service.on_client_restart(
                event.at, event.client_id))

    def _failover(self, event: Failover) -> None:
        snapshot = (
            self.lock_service.snapshot() if event.durable else None)
        self._record(
            self.lock_service.failover(event.at, snapshot, event.durable))

    def _cache_flush(self, event: CacheFlush) -> None:
        self.resource.flush_cache()

    def _tick(self, event: Tick) -> None:
        # 仅推进逻辑时间，让租约自然到期，不产生任何 I/O。
        return None

    _handlers = {
        Acquire: _acquire,
        Renew: _renew,
        Release: _release,
        Write: _write,
        ClientRestart: _client_restart,
        Failover: _failover,
        CacheFlush: _cache_flush,
        Tick: _tick,
    }

    def recover_after_state_loss(self, at: int) -> None:
        """非持久化主备切换后的显式恢复入口。"""
        self._advance(at)
        self._record(
            self.lock_service.recover(
                at, self.resource.durable_high_water_mark))

    def recoverable_state(self) -> RecoverableState:
        lease = self.lock_service.active_lease(self.now)
        return RecoverableState(
            at=self.now,
            epoch=self.lock_service.epoch,
            token_counter=self.lock_service.token_counter,
            available=self.lock_service.available,
            holder_client_id=lease.client_id if lease else None,
            holder_token=lease.token if lease else None,
            lease_expires_at=lease.expires_at if lease else None,
            max_issued_token=self.lock_service.max_issued_token,
            resource_durable_high_water_mark=(
                self.resource.durable_high_water_mark),
            resource_cached_high_water_mark=(
                self.resource.cached_high_water_mark),
            resource_accepted_writes=len(
                self.resource.accepted_writes),
            known_tokens=dict(self.lock_service._token_owner),
        )

    def write_decisions(self) -> List[Dict[str, Any]]:
        """按写入发生顺序输出接受 / 拒绝结论。"""
        return [
            {
                "at": result.at,
                "client_id": result.client_id,
                "token": result.token,
                "decision": "ACCEPT" if result.accepted else "REJECT",
                "high_water_mark": result.high_water_mark,
                "reason": result.reason,
            }
            for result in self.write_results
        ]

    def safety_violations(self) -> List[Dict[str, Any]]:
        return [
            {
                "at": item.at,
                "code": item.code,
                "client_id": item.client_id,
                "token": item.token,
                "detail": item.detail,
            }
            for item in self.violations
        ]
