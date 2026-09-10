"""前端静态一致性：id 引用不能悬空，选择器不能互相覆盖。

这类问题在浏览器里才暴露（而且往往要点到某个按钮才发作），所以用静态检查兜住。
"""

import re
from pathlib import Path

WEB = Path(__file__).resolve().parents[1] / "web"
# 运行期由 JS 动态创建的元素，本来就不在 index.html 里
DYNAMIC_IDS = {"graphClear", "newSid"}


def test_every_referenced_id_exists():
    html = (WEB / "index.html").read_text(encoding="utf-8")
    js = (WEB / "app.js").read_text(encoding="utf-8")
    ids = set(re.findall(r'id="([^"]+)"', html))
    used = set(re.findall(r'\$\("#([A-Za-z0-9_-]+)"\)', js))
    missing = sorted(used - ids - DYNAMIC_IDS)
    assert missing == [], "JS 引用了 index.html 里不存在的 id: %s" % missing


def test_job_detail_button_does_not_reuse_manual_job_attribute():
    """「明细」按钮与手动排队按钮必须用不同属性。

    两者都用 data-job 时，bindJobButtons() 会把「压缩存档 / 审计与合并 / 合并相似永久记忆」
    这些手动按钮的点击覆盖成「打开明细」，于是请求 /job/audit → 404。
    """
    js = (WEB / "app.js").read_text(encoding="utf-8")
    assert 'data-jobdetail="' in js, "任务卡片明细按钮应使用 data-jobdetail"
    assert '$$("[data-jobdetail]").forEach' in js, "明细绑定应只选取 data-jobdetail"
    assert js.count('$$("[data-job]")') == 1, "data-job 只应由手动排队按钮使用一次"


def test_style_braces_balanced():
    css = (WEB / "style.css").read_text(encoding="utf-8")
    assert css.count("{") == css.count("}"), "style.css 大括号不配对"
