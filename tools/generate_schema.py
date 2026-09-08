"""Generate the KiraAI configuration schema from the validated settings contract."""

import importlib.util
import json
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("contracts", root / "contracts.py")
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
schema = module.Settings.model_json_schema()
help_spec = importlib.util.spec_from_file_location(
    "setting_help", root / "setting_help.py"
)
help_module = importlib.util.module_from_spec(help_spec)
help_spec.loader.exec_module(help_module)
names = dict(
    zip(
        module.Settings.model_fields,
        [
            "启用记忆系统",
            "记录对话与感知",
            "持续上下文注入",
            "首层压缩阈值",
            "首层每批条数",
            "自动压缩概率",
            "最大压缩层级",
            "压缩模型",
            "审计模型",
            "可选向量模型（启用后可能计费）",
            "启用向量检索（默认关闭）",
            "后台审计",
            "审计间隔（秒）",
            "每批审计事实数",
            "模型超时（秒）",
            "模型失败重试次数",
            "后台并发数",
            "上下文字符预算",
            "估算 Token 警告线",
            "回忆提示词",
            "Bot可访问范围",
            "感知事实批量（×10）",
            "定时主动感知",
            "主动感知间隔（秒）",
            "主动感知会话",
            "压缩补充要求",
            "自动安全迁移旧记忆",
            "迁移成功后互斥旧插件",
            "KiraOS 迁移字符上限",
            "每批压缩输入预算（字符）",
            "打开面板时播放载入动画",
            "载入动画重播冷却（秒）",
        ],
    )
)
fields = {}
for key, p in schema["properties"].items():
    kind = {
        "boolean": "switch",
        "number": "float",
        "integer": "integer",
        "array": "list",
        "string": "string",
    }[p["type"]]
    field = {
        "type": kind,
        "name": names[key],
        "default": module.Settings().model_dump()[key],
        "description": help_module.HELP[key],
    }
    for constraint in ("minimum", "maximum"):
        if constraint in p:
            field[constraint] = p[constraint]
    if p["type"] == "array":
        field["item_type"] = "string"
    if "enum" in p:
        field["options"] = p["enum"]
    if key.endswith("_model"):
        field["type"] = "model_select"
        field["model_type"] = "embedding" if key == "embedding_model" else "llm"
    fields[key] = field
(root / "schema.json").write_text(
    json.dumps(
        {
            "alife": {
                "type": "section",
                "name": "Alife 完整记忆移植",
                "collapsed": False,
                "fields": fields,
            }
        },
        ensure_ascii=False,
        indent=2,
    )
    + "\n",
    encoding="utf-8",
)
