"""Identity canonicalisation: migrated numbers merge instead of staying archives."""

import importlib
import json
import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
package = types.ModuleType("alife_identity_test")
package.__path__ = [str(ROOT)]
sys.modules.setdefault("alife_identity_test", package)
i = importlib.import_module("alife_identity_test.identity")
m = importlib.import_module("alife_identity_test.migration")
s = importlib.import_module("alife_identity_test.storage")
r = importlib.import_module("alife_identity_test.retrieval")


def write(root, relative, text):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def toml(entry_id, text, session=None):
    source = 'time="2025-01-02T03:04:05Z"'
    if session:
        source = f'session="{session}", ' + source
    return (
        f'id="{entry_id}"\ntext={json.dumps(text, ensure_ascii=False)}\n'
        f'type="fact"\nsource={{{source}}}\n'
    )


def fixture(root):
    write(
        root,
        "entities/user_3303169514/facts/a.toml",
        toml("a", "私聊裸数字", "qq:dm:3303169514"),
    )
    write(
        root,
        "entities/user_qq:3787534211/facts/b.toml",
        toml("b", "带前缀用户", "qq:dm:3787534211"),
    )
    write(
        root,
        "entities/group_qq:188395693/facts/c.toml",
        toml("c", "群聊记录", "qq:gm:188395693"),
    )
    write(
        root,
        "entities/group_188395694/facts/d.toml",
        toml("d", "裸数字群", "qq:gm:188395694"),
    )
    write(
        root,
        "entities/user_777/facts/e.toml",
        toml("e", "无会话裸数字"),
    )
    write(root, "global/self/facts/f.toml", toml("f", "机器人自述", "qq:dm:1"))
    write(root, "global/facts/g.toml", toml("g", "全局杂项", "qq:dm:2"))


def fresh(tmp_path, adapters=(), seed=None):
    root = tmp_path / "memory"
    fixture(root)
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    if seed:
        seed(store)
    store.import_legacy(m.snapshot(root, m.KIRAOS, 120))
    return store, store.canonicalize_identity(adapters)


def facts(store):
    return {f["content"]: f for f in store.facts(limit=100, global_scope=True)}


def records(store):
    return {rec["summary"]: rec for rec in store.export()["records"]}


def test_merges_same_number_and_splits_global_self(tmp_path):
    def seed(store):
        store.observe_name("napcat:3303169514", "小北", kind="user", observed=1.0)

    store, report = fresh(tmp_path, ("qq",), seed)
    assert report["records"] and report["entities"] and not report["pending"]
    rows = records(store)
    # The existing same-number account wins even though it lives on another adapter.
    assert rows["私聊裸数字"]["sid"] == "napcat:dm:3303169514"
    assert "napcat:3303169514" in rows["私聊裸数字"]["users"]
    assert facts(store)["私聊裸数字"]["subject"] == "napcat:3303169514"
    # Adapter-qualified data keeps its own adapter, group data becomes a gm session.
    assert facts(store)["带前缀用户"]["subject"] == "qq:3787534211"
    assert rows["带前缀用户"]["sid"] == "qq:dm:3787534211"
    assert facts(store)["群聊记录"]["subject"] == "qq:gm:188395693"
    assert rows["群聊记录"]["sid"] == "qq:gm:188395693"
    assert facts(store)["裸数字群"]["subject"] == "qq:gm:188395694"
    # Global and self are separate real buckets, not synthetic archives.
    assert rows["机器人自述"]["sid"] == "self"
    assert rows["全局杂项"]["sid"] == "global"
    kinds = {e["id"]: e["kind"] for e in store.entities(limit=100)}
    assert kinds["self"] == "self" and kinds["global"] == "global"
    assert not [e for e in kinds if e.startswith("legacy:")]
    assert store.synthetic_identity() is False


def test_single_adapter_binds_bare_number(tmp_path):
    store, report = fresh(tmp_path, ("napcat",))
    assert facts(store)["无会话裸数字"]["subject"] == "napcat:777"
    assert records(store)["无会话裸数字"]["sid"] == "napcat:dm:777"
    assert not report["pending"]


def test_ambiguous_number_stays_pending(tmp_path):
    def seed(store):
        store.observe_name("qq:777", "甲", kind="user", observed=1.0)
        store.observe_name("telegram:777", "乙", kind="user", observed=2.0)

    store, report = fresh(tmp_path, ("qq", "telegram"), seed)
    assert facts(store)["无会话裸数字"]["subject"] == "unresolved:user:777"
    assert "unresolved:user:777" in report["ambiguous"]
    # Both real accounts survive untouched.
    kinds = {e["id"] for e in store.entities(limit=100)}
    assert {"qq:777", "telegram:777"} <= kinds


def test_digest_is_stable_and_reimport_adds_nothing(tmp_path):
    root = tmp_path / "memory"
    fixture(root)
    raw = m.snapshot(root, m.KIRAOS, 120)
    resolved = m.snapshot(root, m.KIRAOS, 120, i.Resolver(adapters=("napcat",)))
    assert [x["hash"] for x in raw["items"]] == [x["hash"] for x in resolved["items"]]
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    store.import_legacy(resolved)
    before = len(store.export()["records"])
    again = store.import_legacy(raw)
    assert len(store.export()["records"]) == before
    assert again["imported"] == 0


def test_resolved_snapshot_writes_canonical_ids(tmp_path):
    root = tmp_path / "memory"
    fixture(root)
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    store.import_legacy(
        m.snapshot(root, m.KIRAOS, 120, i.Resolver(adapters=("qq",)))
    )
    rows = records(store)
    assert rows["带前缀用户"]["sid"] == "qq:dm:3787534211"
    assert rows["群聊记录"]["sid"] == "qq:gm:188395693"
    assert rows["机器人自述"]["sid"] == "self"
    assert rows["全局杂项"]["sid"] == "global"
    assert facts(store)["带前缀用户"]["subject"] == "qq:3787534211"
    assert facts(store)["群聊记录"]["subject"] == "qq:gm:188395693"
    assert store.synthetic_identity() is False


def test_name_history_moves_to_canonical_entity(tmp_path):
    def seed(store):
        store.observe_name("legacy:user:qq:3787534211", "旧称呼", observed=5.0)

    store, _ = fresh(tmp_path, ("qq",), seed)
    target = store.entities(ids=["qq:3787534211"])[0]
    assert target["name"] == "旧称呼"
    assert not store.entities(ids=["legacy:user:qq:3787534211"])


def test_cross_bucket_duplicate_facts_merge(tmp_path):
    root = tmp_path / "memory"
    write(root, "entities/user_5/facts/a.toml", toml("a", "同一件事"))
    write(root, "entities/user_qq:5/facts/b.toml", toml("b", "同一件事"))
    store = s.Store(tmp_path / "db.sqlite3")
    store.initialize()
    store.import_legacy(m.snapshot(root, m.KIRAOS, 120))
    assert len([f for f in store.facts(limit=50, global_scope=True) if f["content"] == "同一件事"]) == 2
    store.canonicalize_identity(("qq",))
    merged = [f for f in store.facts(limit=50, global_scope=True) if f["content"] == "同一件事"]
    assert len(merged) == 1 and len(merged[0]["sources"]) == 2


def test_forced_binding_merges_pending_placeholder(tmp_path):
    store, report = fresh(tmp_path, ())
    assert "unresolved:user:777" in report["pending"]
    store.canonicalize_identity(
        ("napcat",),
        {"unresolved:user:777": ("napcat:777", "napcat:dm:777", "user")},
    )
    assert facts(store)["无会话裸数字"]["subject"] == "napcat:777"
    assert records(store)["无会话裸数字"]["sid"] == "napcat:dm:777"
    assert not store.synthetic_identity()


def test_identity_labels_drop_legacy_wording():
    assert r.identity_info("legacy:global")["label"] == "全局记忆"
    assert r.identity_info("legacy:self")["label"] == "机器人自身"
    assert r.identity_info("legacy:unscoped")["label"] == "未分类 · 来源会话未确定"
    assert r.identity_info("unresolved:user:42")["label"] == "待绑定 · 人物 42"
    assert r.identity_info("legacy:user:qq:42")["lookup_id"] == "qq:42"
    for value in ("legacy:user:qq:42", "legacy:group:42", "unresolved:group:42"):
        assert "旧插件" not in r.identity_info(value)["label"]
    assert r.identity_info("qq:42")["label"] == "名称待补全"


def test_resolver_prefers_existing_same_number_over_prefix():
    resolver = i.Resolver(
        [("napcat:9", "user"), ("qq:9", "user")], ("qq", "telegram")
    )
    assert resolver.user("qq", "9") == ("qq:9", "qq:dm:9", "user")
    assert resolver.user("", "9") == ("unresolved:user:9", "unscoped", "user")
    assert i.Resolver([("napcat:9", "user")], ("qq",)).user("", "9")[0] == "napcat:9"
    assert i.Resolver([], ("qq",)).user("", "9")[0] == "qq:9"
    assert i.Resolver([], ()).user("", "9")[0] == "unresolved:user:9"
