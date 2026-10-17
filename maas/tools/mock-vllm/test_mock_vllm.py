"""mock-vllm 核心逻辑的单元测试：python -m unittest test_mock_vllm.py"""
import random
import unittest

from mock_vllm import BlockPool, Engine, EngineConfig, FaultConfig, Seq, State, chain_block_hashes, tokenize


def make_engine(**overrides) -> Engine:
    cfg = EngineConfig(replica_id="test", block_size=4, num_gpu_blocks=64, max_num_batched_tokens=64, max_num_seqs=8)
    for key, value in overrides.items():
        setattr(cfg, key, value)
    return Engine(cfg, FaultConfig())


def make_seq(engine: Engine, tokens: list[str], plan_len: int) -> Seq:
    return Seq(
        rid=str(id(tokens)),
        prompt_len=len(tokens),
        prompt_hashes=chain_block_hashes(tokens, engine.cfg.block_size),
        max_tokens=plan_len,
        plan_len=plan_len,
        rng=random.Random(0),
        arrival=0.0,
    )


def drain(engine: Engine, max_steps: int = 10_000) -> int:
    """同步驱动引擎直到所有请求结束，返回执行的 step 数。"""
    steps = 0
    while engine.running or engine.waiting:
        engine._advance(engine._schedule())
        steps += 1
        if steps > max_steps:
            raise AssertionError("engine did not converge")
    return steps


class TokenizerTest(unittest.TestCase):
    def test_tokenize_rules(self):
        self.assertEqual(tokenize("<|im_start|>你好 world123456"), ["<|im_start|>", "你", "好", " ", "worl", "d123", "456"])

    def test_chain_hash_shares_only_common_prefix(self):
        a = chain_block_hashes(list("一二三四五六七八"), 4)
        b = chain_block_hashes(list("一二三四五六七九"), 4)
        c = chain_block_hashes(list("零二三四五六七八"), 4)
        self.assertEqual(a[0], b[0])
        self.assertNotEqual(a[1], b[1])
        # 第一个 block 不同，后续 block 内容相同也不能命中
        self.assertNotEqual(a[1], c[1])


class BlockPoolTest(unittest.TestCase):
    def test_lru_eviction_drops_cached_hash(self):
        pool = BlockPool(2)
        first, second = pool.allocate(2)
        pool.register(first, b"h1")
        pool.register(second, b"h2")
        pool.release([first, second])  # 逆序释放：second 先进入空闲队列，先被驱逐
        self.assertEqual(pool.lookup([b"h1"]), [first])
        (reused,) = pool.allocate(1)
        self.assertIs(reused, second)
        self.assertEqual(pool.lookup([b"h1", b"h2"]), [first])
        self.assertEqual(pool.evictions, 1)

    def test_usage_counts_only_referenced_blocks(self):
        pool = BlockPool(4)
        blocks = pool.allocate(2)
        self.assertAlmostEqual(pool.usage(), 0.5)
        pool.release(blocks)
        self.assertAlmostEqual(pool.usage(), 0.0)


class EngineTest(unittest.TestCase):
    def test_second_identical_prompt_hits_prefix_cache(self):
        engine = make_engine()
        tokens = list("甲乙丙丁戊己庚辛壬癸子丑寅卯辰巳午未申酉")  # 20 个 token = 5 个完整 block
        first = make_seq(engine, tokens, plan_len=3)
        engine.submit(first)
        drain(engine)
        self.assertEqual(first.cached_tokens, 0)
        self.assertEqual(first.num_out, 3)

        second = make_seq(engine, tokens, plan_len=3)
        engine.submit(second)
        drain(engine)
        # prompt 全部命中时要重算最后一个 block 才能产出首个 token，所以命中 4 个 block = 16 token
        self.assertEqual(second.cached_tokens, 16)
        self.assertEqual(second.state, State.FINISHED)
        self.assertEqual(engine.prefix_hits, 16)

    def test_preemption_recomputes_and_finishes(self):
        # 10 个 block × 4 token = 40 token 的 KV；每个请求 prompt 7 token、输出 20 token，
        # 峰值各占 7 个 block，两个同时运行放不下，必然发生抢占
        engine = make_engine(num_gpu_blocks=10)
        seqs = [make_seq(engine, list(f"{i}请求前缀内容"), plan_len=20) for i in range(2)]
        for seq in seqs:
            engine.submit(seq)
        drain(engine)
        self.assertGreater(engine.num_preemptions, 0)
        for seq in seqs:
            self.assertEqual(seq.state, State.FINISHED)
            self.assertEqual(seq.num_out, 20)
            self.assertEqual(seq.finish_reason, "length")
        self.assertEqual(engine.pool.num_free, 10)

    def test_chunked_prefill_respects_token_budget(self):
        engine = make_engine(max_num_batched_tokens=8, num_gpu_blocks=64)
        seq = make_seq(engine, list("一二三四五六七八九十甲乙丙丁戊己庚辛壬癸"), plan_len=1)
        engine.submit(seq)
        batch = engine._schedule()
        self.assertEqual(batch, [(seq, 8)])
        engine._advance(batch)
        self.assertEqual(seq.num_out, 0)
        self.assertEqual(drain(engine), 2)  # 剩余 12 个 token 再分两步 prefill（8 + 4），第二步产出首个 token
        self.assertEqual(seq.num_out, 1)

    def test_long_prefill_stats(self):
        engine = make_engine(long_prefill_tokens=16, max_num_batched_tokens=8)
        seqs = [make_seq(engine, list(f"{i}" + "长" * 30), plan_len=1) for i in range(2)]
        for seq in seqs:
            engine.submit(seq)
        engine._advance(engine._schedule())
        self.assertEqual(engine.long_prefill_inflight, 1)  # 预算只够调度第一个请求
        drain(engine)
        self.assertEqual(engine.long_prefill_inflight, 0)
        self.assertEqual(engine.long_prefill_peak, 2)  # 第一个还没产出首 token 时第二个已被接纳
        engine.reset_stats()
        self.assertEqual(engine.long_prefill_peak, 0)

    def test_abort_releases_blocks(self):
        engine = make_engine()
        seq = make_seq(engine, list("一二三四五六七八"), plan_len=50)
        engine.submit(seq)
        engine._advance(engine._schedule())
        self.assertLess(engine.pool.num_free, 64)
        engine.abort(seq)
        self.assertEqual(seq.state, State.ABORTED)
        self.assertEqual(engine.pool.num_free, 64)


class MetricsTest(unittest.TestCase):
    def test_kv_usage_offset_only_affects_reported_usage(self):
        engine = make_engine()
        engine.fault.update({"kv_usage_offset": 0.95})
        text = engine.render_metrics()
        self.assertIn("vllm:kv_cache_usage_perc{", text)
        reported = [line for line in text.splitlines() if line.startswith("vllm:kv_cache_usage_perc{")][0]
        self.assertAlmostEqual(float(reported.rsplit(" ", 1)[1]), 0.95)
        self.assertEqual(engine.pool.num_free, 64)


class FaultConfigTest(unittest.TestCase):
    def test_update_validates_types(self):
        fault = FaultConfig()
        fault.update({"slow_factor": 3, "down": True})
        self.assertEqual(fault.slow_factor, 3.0)
        self.assertTrue(fault.down)
        for bad in ({"error_rate": 2}, {"down": "yes"}, {"unknown": 1}, {"slow_factor": -1}, {"kv_usage_offset": 1.5}):
            with self.assertRaises(ValueError):
                FaultConfig().update(bad)

    def test_rejected_update_keeps_previous_config(self):
        fault = FaultConfig()
        with self.assertRaises(ValueError):
            fault.update({"slow_factor": 5, "error_rate": 2})
        self.assertEqual(fault, FaultConfig())


if __name__ == "__main__":
    unittest.main()
