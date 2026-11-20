import unittest

from fencing_validator import (
    Acquire,
    CacheFlush,
    ClientRestart,
    Failover,
    FencingValidator,
    Release,
    Renew,
    Tick,
    Write,
)


def decisions(validator):
    return [
        (item["token"], item["decision"])
        for item in validator.write_decisions()
    ]


def violation_codes(validator):
    return [item["code"] for item in validator.safety_violations()]


class OrderedHandoverTest(unittest.TestCase):
    def test_tokens_strictly_increase_and_later_wins(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=10),
            Write(at=2, client_id="A", token=1, payload="a1"),
            Release(at=3, client_id="A", token=1),
            Acquire(at=4, client_id="B", ttl=10),
            Write(at=5, client_id="B", token=2, payload="b1"),
        ])
        self.assertEqual(decisions(v), [(1, "ACCEPT"), (2, "ACCEPT")])
        self.assertEqual(v.current_high_token, 2)
        state = v.recoverable_state()
        self.assertEqual(state.holder_client_id, "B")
        self.assertEqual(state.holder_token, 2)
        self.assertEqual(state.lease_expires_at, 14)


class StaleWriteTest(unittest.TestCase):
    def test_old_holder_late_write_is_fenced(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=5),
            Write(at=2, client_id="A", token=1, payload="a1"),
            Tick(at=7),  # A 的租约在逻辑时间 6 到期，不依赖 wall clock
            Acquire(at=8, client_id="B", ttl=10),
            Write(at=9, client_id="B", token=2, payload="b1"),
            # A 的请求早已进入网络，晚到后它仍“以为”自己持有租约。
            Write(at=10, client_id="A", token=1, payload="a-late"),
        ])
        self.assertEqual(
            decisions(v),
            [(1, "ACCEPT"), (2, "ACCEPT"), (1, "REJECT")])
        late = v.write_results[-1]
        self.assertFalse(late.accepted)
        self.assertEqual(late.high_water_mark, 2)
        self.assertEqual(late.reason, "STALE_FENCING_TOKEN")
        codes = violation_codes(v)
        self.assertIn("STALE_LEASE_WRITE", codes)
        self.assertIn("STALE_FENCING_TOKEN", codes)

    def test_late_write_before_new_holder_writes_is_flagged(self):
        # 资源端只能凭 token 守门：若新持有者还没写过，旧 token 仍大于
        # 高水位会被接受，但验证器必须把“租约已失效仍写入”记为违规。
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=3),
            Tick(at=5),
            Acquire(at=6, client_id="B", ttl=10),
            Write(at=7, client_id="A", token=1, payload="a-late"),
            Write(at=8, client_id="B", token=2, payload="b1"),
        ])
        self.assertEqual(
            decisions(v), [(1, "ACCEPT"), (2, "ACCEPT")])
        self.assertIn("STALE_LEASE_WRITE", violation_codes(v))
        # B 的写入立刻把旧持有者挡在高水位之外。
        v.process([Write(at=9, client_id="A", token=1, payload="a2")])
        self.assertEqual(decisions(v)[-1], (1, "REJECT"))


class TokenRollbackTest(unittest.TestCase):
    def test_duplicate_and_lower_tokens_rejected(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=20),
            Write(at=2, client_id="A", token=1),
            Write(at=3, client_id="A", token=1),  # 重复 token
            Write(at=4, client_id="A", token=0),  # 回退到更小 token
        ])
        self.assertEqual(
            decisions(v),
            [(1, "ACCEPT"), (1, "REJECT"), (0, "REJECT")])
        self.assertEqual(
            violation_codes(v).count("STALE_FENCING_TOKEN"), 1)
        self.assertIn("INVALID_TOKEN", violation_codes(v))

    def test_unknown_token_rejected(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=10),
            Write(at=2, client_id="A", token=99),
        ])
        self.assertEqual(decisions(v), [(99, "REJECT")])
        self.assertEqual(
            v.write_results[-1].reason, "UNKNOWN_FENCING_TOKEN")
        self.assertIn("UNKNOWN_FENCING_TOKEN", violation_codes(v))


class ConcurrentAcquireTest(unittest.TestCase):
    def test_overlapping_acquire_rejected_until_lease_expires(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=5),
            Acquire(at=2, client_id="B", ttl=5),
        ])
        self.assertTrue(v.acquire_results[0].granted)
        self.assertFalse(v.acquire_results[1].granted)
        self.assertEqual(v.acquire_results[1].reason, "LEASE_HELD")
        self.assertIn("LEASE_OVERLAP", violation_codes(v))
        # 租约到期后 B 才能拿到下一个严格递增 token。
        v.process([Tick(at=7), Acquire(at=8, client_id="B", ttl=5)])
        self.assertTrue(v.acquire_results[-1].granted)
        self.assertEqual(v.acquire_results[-1].token, 2)

    def test_renew_and_late_renew(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=5),
            Renew(at=3, client_id="A", token=1, ttl=10),
        ])
        self.assertTrue(v.renew_results[0].renewed)
        self.assertEqual(v.renew_results[0].lease_expires_at, 13)
        v.process([Tick(at=14), Renew(at=15, client_id="A", token=1,
                                      ttl=10)])
        self.assertFalse(v.renew_results[-1].renewed)
        self.assertEqual(v.renew_results[-1].reason, "LEASE_EXPIRED")
        self.assertIn("STALE_LEASE_OPERATION", violation_codes(v))

    def test_release_with_wrong_token_refused(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=10),
            Release(at=2, client_id="A", token=7),
        ])
        self.assertFalse(v.release_results[0].released)
        self.assertEqual(v.release_results[0].reason, "TOKEN_MISMATCH")


class FailoverTest(unittest.TestCase):
    def test_durable_failover_preserves_counter_and_lease(self):
        v = FencingValidator()
        v.process([Acquire(at=1, client_id="A", ttl=20)])
        v.process([Failover(at=5, durable=True)])
        self.assertEqual(v.lock_service.epoch, 1)
        self.assertTrue(v.lock_service.available)
        v.process([Write(at=6, client_id="A", token=1)])
        self.assertEqual(decisions(v), [(1, "ACCEPT")])
        # 备用节点铸造的下一个 token 继续严格递增，不重复 1。
        v.process([
            Release(at=7, client_id="A", token=1),
            Acquire(at=8, client_id="B", ttl=10),
        ])
        self.assertEqual(v.acquire_results[-1].token, 2)

    def test_nondurable_failover_recovers_from_resource_hwm(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=20),
            Write(at=2, client_id="A", token=1),
            Release(at=3, client_id="A", token=1),
            Acquire(at=4, client_id="B", ttl=20),
            Write(at=5, client_id="B", token=2),
            Failover(at=6, durable=False),
        ])
        self.assertFalse(v.lock_service.available)
        self.assertIn("FAILOVER_STATE_LOSS", violation_codes(v))
        v.process([Acquire(at=7, client_id="C", ttl=10)])
        self.assertFalse(v.acquire_results[-1].granted)
        # 以资源端持久化高水位 2 为下界恢复，新 token 必须 > 2。
        v.recover_after_state_loss(at=8)
        v.process([Acquire(at=9, client_id="C", ttl=10)])
        result = v.acquire_results[-1]
        self.assertTrue(result.granted)
        self.assertGreater(result.token, 2)
        self.assertEqual(result.token, 3)
        v.process([Write(at=10, client_id="C", token=3)])
        self.assertEqual(decisions(v)[-1], (3, "ACCEPT"))
        # 恢复后旧 token 依旧被资源端拒绝。
        v.process([Write(at=11, client_id="B", token=2)])
        self.assertEqual(decisions(v)[-1], (2, "REJECT"))


class RestartAndCacheTest(unittest.TestCase):
    def test_holder_restart_revokes_lease(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=20),
            ClientRestart(at=4, client_id="A"),
            Write(at=5, client_id="A", token=1),
        ])
        self.assertIn("HOLDER_RESTART", violation_codes(v))
        self.assertIn("STALE_LEASE_WRITE", violation_codes(v))
        # 吊销后同一客户端可以重新申请并获得新 token。
        v.process([Acquire(at=6, client_id="A", ttl=20)])
        self.assertEqual(v.acquire_results[-1].token, 2)

    def test_cache_flush_falls_back_to_durable_hwm(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=30),
            Write(at=2, client_id="A", token=1),
            Acquire(at=3, client_id="B", ttl=30),  # 会被拒绝
        ])
        self.assertIn("LEASE_OVERLAP", violation_codes(v))
        v.process([
            Release(at=4, client_id="A", token=1),
            Acquire(at=5, client_id="B", ttl=30),
            Write(at=6, client_id="B", token=2),
            CacheFlush(at=7),
        ])
        self.assertEqual(v.resource.cached_high_water_mark, 0)
        # 缓存丢失，持久化高水位仍挡住回退 token。
        v.process([Write(at=8, client_id="A", token=1)])
        self.assertEqual(decisions(v)[-1], (1, "REJECT"))
        self.assertEqual(
            v.write_results[-1].high_water_mark, 2)


class LogicalTimeTest(unittest.TestCase):
    def test_events_with_regressing_logical_time_are_rejected(self):
        v = FencingValidator()
        with self.assertRaises(ValueError):
            v.process([
                Acquire(at=10, client_id="A", ttl=5),
                Write(at=5, client_id="A", token=1),
            ])

    def test_recoverable_state_snapshot_shape(self):
        v = FencingValidator()
        v.process([
            Acquire(at=1, client_id="A", ttl=10),
            Write(at=2, client_id="A", token=1),
        ])
        state = v.recoverable_state()
        self.assertEqual(state.at, 2)
        self.assertEqual(state.token_counter, 1)
        self.assertEqual(state.max_issued_token, 1)
        self.assertEqual(
            state.resource_durable_high_water_mark, 1)
        self.assertEqual(state.known_tokens, {1: "A"})


if __name__ == "__main__":
    unittest.main()
