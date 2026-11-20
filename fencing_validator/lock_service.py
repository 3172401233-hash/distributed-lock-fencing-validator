"""基于逻辑时间与 fencing token 的锁服务（离线状态机）。

关键设计：

* 不读取系统时钟，租约到期只比较输入的逻辑时间。
* 每次成功 ``acquire`` 都从单调计数器铸造一个严格递增的 fencing
  token；token 一经铸造即记入持久化的 token 账本，持有者切换不会
  复用旧值。
* 主备切换默认从持久化快照恢复；非持久化切换会让服务进入不可用
  状态，必须显式恢复（且计数器只增不减），避免铸造出回退 token。
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

from .events import (
    AcquireResult,
    ReleaseResult,
    RenewResult,
    Violation,
)


@dataclass
class _Lease:
    client_id: str
    token: int
    expires_at: int
    released: bool = False


class LockService:
    def __init__(self):
        self.epoch = 0
        self._counter = 0
        self._max_issued = 0
        self.available = True
        self._lease_by_client = {}  # type: Dict[str, _Lease]
        self._token_owner = {}  # type: Dict[int, str]

    @property
    def token_counter(self) -> int:
        return self._counter

    @property
    def max_issued_token(self) -> int:
        return self._max_issued

    def active_lease(self, now: int) -> Optional[_Lease]:
        for lease in self._lease_by_client.values():
            if not lease.released and lease.expires_at > now:
                return lease
        return None

    def holder_client_id(self, now: int) -> Optional[str]:
        lease = self.active_lease(now)
        return lease.client_id if lease else None

    def lease_for(self, client_id: str, now: int) -> Optional[_Lease]:
        lease = self._lease_by_client.get(client_id)
        if lease is None or lease.released or lease.expires_at <= now:
            return None
        return lease

    def is_known_token(self, token: int) -> bool:
        return token in self._token_owner

    def owner_of(self, token: int) -> Optional[str]:
        return self._token_owner.get(token)

    def acquire(
        self, at: int, client_id: str, ttl: int
    ) -> Tuple[AcquireResult, Optional[Violation]]:
        if not self.available:
            return (
                AcquireResult(at, client_id, False, None, None,
                              "LOCK_SERVICE_UNAVAILABLE"),
                Violation(at, "SERVICE_UNAVAILABLE", client_id, None,
                          "锁服务在主备切换后尚未恢复，拒绝发放新租约"),
            )
        if ttl <= 0:
            return (
                AcquireResult(at, client_id, False, None, None,
                              "INVALID_TTL"),
                Violation(at, "INVALID_INPUT", client_id, None,
                          "ttl 必须为正整数"),
            )
        existing = self.active_lease(at)
        if existing is not None:
            return (
                AcquireResult(at, client_id, False, None, None,
                              "LEASE_HELD"),
                Violation(
                    at, "LEASE_OVERLAP", client_id, None,
                    "客户端 %s 在逻辑时间 %d 与持有者 %s（token=%d，"
                    "到期=%d）申请重叠租约，被拒绝"
                    % (client_id, at, existing.client_id, existing.token,
                       existing.expires_at),
                ),
            )
        self._counter += 1
        token = self._counter
        if token <= self._max_issued:
            raise RuntimeError("内部错误：fencing token 失去严格递增性")
        self._max_issued = token
        expires_at = at + ttl
        lease = _Lease(client_id, token, expires_at)
        self._lease_by_client[client_id] = lease
        self._token_owner[token] = client_id
        return (
            AcquireResult(at, client_id, True, token, expires_at, None),
            None,
        )

    def renew(
        self, at: int, client_id: str, token: int, ttl: int
    ) -> Tuple[RenewResult, Optional[Violation]]:
        lease = self._lease_by_client.get(client_id)
        if lease is None:
            return self._renew_reject(
                at, client_id, token, "UNKNOWN_HOLDER")
        if lease.token != token:
            return (
                RenewResult(at, client_id, token, False, None,
                            "TOKEN_MISMATCH"),
                Violation(
                    at, "TOKEN_MISMATCH", client_id, token,
                    "续租 token=%d 与该客户端当前 token=%d 不匹配"
                    % (token, lease.token),
                ),
            )
        if lease.released:
            return (
                RenewResult(at, client_id, token, False, None,
                            "LEASE_RELEASED"),
                Violation(at, "STALE_LEASE_OPERATION", client_id, token,
                          "租约已被显式释放，续租被拒绝"),
            )
        if lease.expires_at <= at:
            return (
                RenewResult(at, client_id, token, False, None,
                            "LEASE_EXPIRED"),
                Violation(
                    at, "STALE_LEASE_OPERATION", client_id, token,
                    "续租请求晚到：租约在逻辑时间 %d 已到期（当前 %d）"
                    % (lease.expires_at, at),
                ),
            )
        lease.expires_at = at + ttl
        return RenewResult(at, client_id, token, True, lease.expires_at,
                           None), None

    @staticmethod
    def _renew_reject(
        at: int, client_id: str, token: int, reason: str
    ) -> Tuple[RenewResult, Optional[Violation]]:
        return (
            RenewResult(at, client_id, token, False, None, reason),
            Violation(at, "STALE_LEASE_OPERATION", client_id, token,
                      "找不到持有者 %s 的有效租约（%s）"
                      % (client_id, reason)),
        )

    def release(
        self, at: int, client_id: str, token: int
    ) -> Tuple[ReleaseResult, Optional[Violation]]:
        lease = self._lease_by_client.get(client_id)
        if lease is None:
            return (
                ReleaseResult(at, client_id, token, False,
                              "UNKNOWN_HOLDER"),
                Violation(at, "STALE_LEASE_OPERATION", client_id, token,
                          "释放请求来自未知持有者，被忽略"),
            )
        if lease.token != token:
            return (
                ReleaseResult(at, client_id, token, False,
                              "TOKEN_MISMATCH"),
                Violation(
                    at, "TOKEN_MISMATCH", client_id, token,
                    "释放 token=%d 与当前 token=%d 不匹配，拒绝释放"
                    % (token, lease.token),
                ),
            )
        if lease.released:
            return (
                ReleaseResult(at, client_id, token, False,
                              "LEASE_RELEASED"),
                Violation(at, "STALE_LEASE_OPERATION", client_id, token,
                          "重复释放：租约此前已经释放"),
            )
        lease.released = True
        return ReleaseResult(at, client_id, token, True, None), None

    def on_client_restart(
        self, at: int, client_id: str
    ) -> Optional[Violation]:
        lease = self._lease_by_client.get(client_id)
        if lease is None or lease.released or lease.expires_at <= at:
            return None
        lease.released = True
        return Violation(
            at, "HOLDER_RESTART", client_id, lease.token,
            "持有者 %s 重启，服务端吊销其 token=%d 的租约（到期=%d）"
            % (client_id, lease.token, lease.expires_at),
        )

    def failover(
        self, at: int, snapshot: Optional[Dict[str, Any]], durable: bool
    ) -> Optional[Violation]:
        self.epoch += 1
        violation = None
        if durable and snapshot is not None:
            self._counter = int(snapshot["token_counter"])
            self._max_issued = int(snapshot["max_issued_token"])
            self._lease_by_client = snapshot["leases"]
            self._token_owner = snapshot["token_owner"]
            self.available = True
        else:
            # 持久化日志丢失：内存里的旧状态不可信，必须停止发锁，
            # 等待以资源端已接受的最大 token 为下界恢复计数器。
            self._lease_by_client = {}
            self.available = False
            violation = Violation(
                at, "FAILOVER_STATE_LOSS", None, None,
                "主备切换丢失持久化状态，epoch=%d，锁服务进入不可用状态"
                % self.epoch,
            )
        return violation

    def recover(self, at: int, floor_token: int) -> Violation:
        """以已知下界（通常是资源端 durable HWM）重建单调计数器。"""
        new_counter = max(self._counter, self._max_issued, floor_token)
        if new_counter > self._counter:
            self._counter = new_counter
        if self._max_issued < new_counter:
            self._max_issued = new_counter
        self.available = True
        return Violation(
            at, "COUNTER_RECOVERED", None, new_counter,
            "锁服务以 token 下界 %d 恢复计数器，后续 token 严格大于该值"
            % new_counter,
        )

    def snapshot(self) -> Dict[str, Any]:
        return {
            "epoch": self.epoch,
            "token_counter": self._counter,
            "max_issued_token": self._max_issued,
            "leases": dict(self._lease_by_client),
            "token_owner": dict(self._token_owner),
        }
