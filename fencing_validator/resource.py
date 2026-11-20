"""资源端 fencing 守卫。

资源端不信任任何客户端对“我还持有租约”的声明，只比较 fencing token：

* token 必须严格大于已接受的最大 token（高水位），否则拒绝；
* 高水位同时存在于持久化存储与本地缓存中，缓存只允许让判断更严格，
  缓存被清空后回退到持久化高水位，不会接受回退 token；
* 持久化写入日志可在资源重启后重建高水位。
"""

from dataclasses import dataclass
from typing import Any, List, Optional, Tuple

from .events import Violation, WriteResult


@dataclass(frozen=True)
class AcceptedWrite:
    at: int
    client_id: str
    token: int
    payload: Any


class FencingResource:
    def __init__(self):
        self._durable_hwm = 0
        self._cached_hwm = 0
        self._durable_log = []  # type: List[AcceptedWrite]

    @property
    def durable_high_water_mark(self) -> int:
        return self._durable_hwm

    @property
    def cached_high_water_mark(self) -> int:
        return self._cached_hwm

    @property
    def effective_high_water_mark(self) -> int:
        # 缓存是持久化状态的易失副本，二者取最大值确保缓存丢失只会
        # 让守卫变宽松到“已持久化的安全边界”，绝不会变宽松到边界以下。
        return max(self._durable_hwm, self._cached_hwm)

    @property
    def accepted_writes(self) -> Tuple[AcceptedWrite, ...]:
        return tuple(self._durable_log)

    def write(
        self, at: int, client_id: str, token: int, payload: Any
    ) -> Tuple[WriteResult, Optional[Violation]]:
        hwm = self.effective_high_water_mark
        if token <= hwm:
            if token <= 0:
                reason = "INVALID_TOKEN"
                code = "INVALID_TOKEN"
            else:
                reason = "STALE_FENCING_TOKEN"
                code = "STALE_FENCING_TOKEN"
            violation = Violation(
                at, code, client_id, token,
                "资源端拒绝写入：token=%d 不大于已接受高水位 %d"
                % (token, hwm),
            )
            return (
                WriteResult(at, client_id, token, False, hwm, reason),
                violation,
            )
        self._durable_hwm = token
        self._cached_hwm = token
        self._durable_log.append(
            AcceptedWrite(at, client_id, token, payload))
        return (
            WriteResult(at, client_id, token, True, token, None),
            None,
        )

    def flush_cache(self) -> None:
        """模拟缓存失效 / 资源进程重启：清空易失高水位。"""
        self._cached_hwm = 0

    def recover(self) -> int:
        """从持久化写入日志重建高水位与缓存。"""
        self._durable_hwm = 0
        for entry in self._durable_log:
            if entry.token <= self._durable_hwm:
                raise RuntimeError("持久化日志中出现非递增 token")
            self._durable_hwm = entry.token
        self._cached_hwm = self._durable_hwm
        return self._durable_hwm
