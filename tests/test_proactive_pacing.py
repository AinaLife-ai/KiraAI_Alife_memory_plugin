"""主动感知的节奏：固定/随机间隔、每轮会话数、轮流挑选。"""

import asyncio
import importlib
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_proactive_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_proactive_test", package)
c = importlib.import_module("alife_proactive_test.contracts")
s = importlib.import_module("alife_proactive_test.storage")
e = importlib.import_module("alife_proactive_test.engine")

SESSIONS = ["a:gm:1", "a:gm:2", "a:gm:3", "a:gm:4"]


def engine(tmp_path, **overrides):
    store = s.Store(tmp_path / "m.db")
    store.initialize()
    cfg = c.Settings(proactive_enabled=True, **overrides)
    return e.Engine(store, lambda: cfg, None, None, None), cfg


def test_delay_is_fixed_without_jitter(tmp_path):
    _, cfg = engine(tmp_path, proactive_interval=3600, proactive_jitter=0)
    assert {e.Engine.proactive_delay(cfg) for _ in range(20)} == {3600}


def test_delay_adds_bounded_jitter(tmp_path):
    _, cfg = engine(tmp_path, proactive_interval=3600, proactive_jitter=600)
    draws = [e.Engine.proactive_delay(cfg) for _ in range(200)]
    assert all(3600 <= value <= 4200 for value in draws)
    assert len(set(draws)) > 1


def test_picker_default_triggers_every_session(tmp_path):
    eng, cfg = engine(tmp_path, proactive_sessions=SESSIONS)
    assert sorted(eng.pick_proactive_sessions(cfg)) == sorted(SESSIONS)


def test_picker_equal_bounds_are_fixed(tmp_path):
    eng, cfg = engine(
        tmp_path,
        proactive_sessions=SESSIONS,
        proactive_min_sessions=2,
        proactive_max_sessions=2,
    )
    for _ in range(20):
        picked = eng.pick_proactive_sessions(cfg)
        assert len(picked) == 2 and len(set(picked)) == 2


def test_picker_stays_within_bounds(tmp_path):
    eng, cfg = engine(
        tmp_path,
        proactive_sessions=SESSIONS,
        proactive_min_sessions=1,
        proactive_max_sessions=3,
    )
    counts = {len(eng.pick_proactive_sessions(cfg)) for _ in range(60)}
    assert counts <= {1, 2, 3} and len(counts) > 1


def test_picker_clamps_to_available_sessions(tmp_path):
    eng, cfg = engine(
        tmp_path,
        proactive_sessions=["a:gm:1"],
        proactive_min_sessions=3,
        proactive_max_sessions=5,
    )
    assert eng.pick_proactive_sessions(cfg) == ["a:gm:1"]
    empty, cfg2 = engine(tmp_path, proactive_sessions=[])
    assert empty.pick_proactive_sessions(cfg2) == []


def test_rotate_mode_covers_every_session_first(tmp_path):
    eng, cfg = engine(
        tmp_path,
        proactive_sessions=SESSIONS,
        proactive_min_sessions=1,
        proactive_max_sessions=1,
        proactive_rotate=True,
    )
    rounds = [eng.pick_proactive_sessions(cfg)[0] for _ in range(len(SESSIONS))]
    assert sorted(rounds) == sorted(SESSIONS), rounds
    # 第二轮之后仍然均匀：再跑一轮，每个会话各命中一次
    again = [eng.pick_proactive_sessions(cfg)[0] for _ in range(len(SESSIONS))]
    assert sorted(again) == sorted(SESSIONS), again


def test_tick_waits_one_interval_before_first_fire(tmp_path):
    eng, cfg = engine(
        tmp_path,
        proactive_interval=3600,
        proactive_jitter=0,
        proactive_sessions=SESSIONS,
    )

    async def run():
        # 调度器第一次看到它：只排期，不触发（重启不会立刻刷屏）
        assert await eng.proactive_tick(1000.0, cfg) == []
        assert await eng.proactive_tick(2000.0, cfg) == []
        # 到点：整批入队
        picked = await eng.proactive_tick(4600.0, cfg)
        assert sorted(picked) == sorted(SESSIONS)
        with eng.store.connect() as db:
            queued = db.execute(
                "SELECT sid FROM jobs WHERE kind='proactive'"
            ).fetchall()
        assert sorted(row[0] for row in queued) == sorted(SESSIONS)
        # 下一轮要再等一个间隔
        assert await eng.proactive_tick(5000.0, cfg) == []

    asyncio.run(run())


def test_tick_resets_when_disabled(tmp_path):
    eng, cfg = engine(tmp_path, proactive_sessions=SESSIONS)
    off = cfg.model_copy(update={"proactive_enabled": False})

    async def run():
        await eng.proactive_tick(1000.0, cfg)
        assert eng.proactive_due is not None
        assert await eng.proactive_tick(1001.0, off) == []
        assert eng.proactive_due is None

    asyncio.run(run())
