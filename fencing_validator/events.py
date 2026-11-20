"""输入事件模型。

所有事件都携带一个 ``at`` 字段，它是输入提供的逻辑时间（逻辑时钟
tick），不是 wall clock。验证器的所有租约判定都只使用该值。
"""

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass(frozen=True)
class Event:
    at: int


@dataclass(frozen=True)
class Acquire(Event):
    client_id: str
    ttl: int


@dataclass(frozen=True)
class Renew(Event):
    client_id: str
    token: int
    ttl: int


@dataclass(frozen=True)
class Release(Event):
    client_id: str
    token: int


@dataclass(frozen=True)
class Write(Event):
    client_id: str
    token: int
    payload: Any = None


@dataclass(frozen=True)
class ClientRestart(Event):
    client_id: str


@dataclass(frozen=True)
class Failover(Event):
    durable: bool = True


@dataclass(frozen=True)
class CacheFlush(Event):
    pass


@dataclass(frozen=True)
class Tick(Event):
    pass


@dataclass(frozen=True)
class AcquireResult:
    at: int
    client_id: str
    granted: bool
    token: Optional[int]
    lease_expires_at: Optional[int]
    reason: Optional[str]


@dataclass(frozen=True)
class RenewResult:
    at: int
    client_id: str
    token: int
    renewed: bool
    lease_expires_at: Optional[int]
    reason: Optional[str]


@dataclass(frozen=True)
class ReleaseResult:
    at: int
    client_id: str
    token: int
    released: bool
    reason: Optional[str]


@dataclass(frozen=True)
class WriteResult:
    at: int
    client_id: str
    token: int
    accepted: bool
    high_water_mark: int
    reason: Optional[str]


@dataclass(frozen=True)
class Violation:
    at: int
    code: str
    client_id: Optional[str]
    token: Optional[int]
    detail: str


@dataclass
class RecoverableState:
    """主备切换 / 进程崩溃后可用于重建的最小状态快照。"""

    at: int
    epoch: int
    token_counter: int
    available: bool
    holder_client_id: Optional[str]
    holder_token: Optional[int]
    lease_expires_at: Optional[int]
    max_issued_token: int
    resource_durable_high_water_mark: int
    resource_cached_high_water_mark: int
    resource_accepted_writes: int
    known_tokens: dict = field(default_factory=dict)
