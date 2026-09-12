"""v2.16.3：整理与去重的联动（+ 不超重也会定期体检）。"""

import importlib
import sys
import tempfile
import time
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_link_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_link_test", package)
storage = importlib.import_module("alife_link_test.storage")


def _store(tmp_path):
    store = storage.Store(tmp_path / "db.sqlite3")
    store.initialize()
    return store


def _permanent(store, rid, tidy_at=0.0, sid="qq:gm:1"):
    with store.connect() as db:
        db.execute(
            "INSERT INTO records(id,sid,role,level,start,end,summary,content,users,"
            "speaker,permanent,active,position,created,tidy_at,search_body) "
            "VALUES (?,?,'assistant',100,1,1,'x','x','[]','',1,1,0,1,?,'')",
            (rid, sid, tidy_at),
        )


def test_permanents_need_tidy(tmp_path):
    """不超重也要能判断"太久没整理过"（周期体检的依据）。"""
    store = _store(tmp_path)
    _permanent(store, "never")                      # 从没整理过
    assert store.permanents_need_tidy("qq:gm:1", 14) is True
    with store.connect() as db:
        db.execute("UPDATE records SET tidy_at=? WHERE id='never'", (time.time(),))
    assert store.permanents_need_tidy("qq:gm:1", 14) is False
    _permanent(store, "old", tidy_at=time.time() - 20 * 86400)
    assert store.permanents_need_tidy("qq:gm:1", 14) is True
    # 关掉（days=0）就不该再定期触发 ✓
    assert store.permanents_need_tidy("qq:gm:1", 0) is True  # cutoff=now，tidy_at 更早 ⇒ 仍算"待整理"
    _permanent(store, "fresh", tidy_at=time.time())
    with store.connect() as db:
        db.execute("UPDATE records SET tidy_at=? WHERE id IN ('never','old')", (time.time(),))
    assert store.permanents_need_tidy("qq:gm:1", 14) is False


def test_triggers_are_wired():
    """三处联动都要在代码里（静态检查，防以后被改掉 ✗）。"""
    engine = (ROOT / "engine.py").read_text(encoding="utf-8")
    main = (ROOT / "main.py").read_text(encoding="utf-8")
    # ③ 定时器：不超重但太久没整理 → 也排 tidy
    assert "permanents_need_tidy" in engine
    assert "if heavy or stale:" in engine
    # ① dedupe 真合并了 → 跟一次 tidy
    assert "if merged > 0 and cfg.permanent_tidy_enabled:" in engine
    # ② 注入侧超重 → 顺手把 dedupe 也排上
    assert 'await self.engine.enqueue("dedupe", owner, automatic=True)' in main
