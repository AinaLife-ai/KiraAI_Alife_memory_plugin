"""Versioned one-shot configuration migrations.

Only keys that still hold the *previous default* are rewritten, so anything the
user customised is never touched. This keeps existing installs on the current
defaults (including prompt wording) without overriding personal choices.
"""

from .contracts import FACT_MERGE_PROMPT, RECORD_MERGE_PROMPT

CURRENT_VERSION = 3

# version -> [(key, previous default, new default), ...]
MIGRATIONS = {
    2: [
        ("audit_interval", 1800, 7200),
    ],
    3: [
        # v2.6.0 把「检索默认只搜常驻」的默认值翻成关闭（归档也参与召回，更全），
        # 但老配置里多半存着旧默认 true。只改写「仍等于旧默认」的那一个键，
        # 用户自己设过的一律不动。
        ("search_active_only", True, False),
    ],
}


def migrate(config):
    """Return ``(changed_keys, updated_config)``; pure function, no I/O."""
    config = dict(config or {})
    meta = dict(config.get("alife_meta") or {})
    try:
        version = int(meta.get("config_version") or 1)
    except (TypeError, ValueError):
        version = 1
    if version >= CURRENT_VERSION:
        return [], config
    settings = dict(config.get("alife") or {})
    changed = []
    for step in range(version + 1, CURRENT_VERSION + 1):
        for key, previous, current in MIGRATIONS.get(step, ()):
            if key in settings and settings[key] == previous:
                settings[key] = current
                changed.append(key)
    config["alife"] = settings
    meta["config_version"] = CURRENT_VERSION
    config["alife_meta"] = meta
    return changed, config


def prompt_defaults():
    """Exposed for tests: the shipped merge prompt templates."""
    return {"fact_merge_prompt": FACT_MERGE_PROMPT, "record_merge_prompt": RECORD_MERGE_PROMPT}
