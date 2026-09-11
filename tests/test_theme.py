"""主题选择的记忆：用户选过就要留住，且开屏动画不能闪主题。

线上问题：黑夜/明亮按钮点完刷新就回到默认——`app.js` 只改了
`document.documentElement.dataset.theme`，既没存也没在启动时恢复。
"""

import re

from test_web_static import WEB


def test_theme_choice_is_persisted_and_restored():
    js = (WEB / "app.js").read_text(encoding="utf-8")
    html = (WEB / "index.html").read_text(encoding="utf-8")
    key = "alife-theme"
    # 切换时要存
    assert 'localStorage.setItem("%s"' % key in js, "切换主题必须写入 localStorage"
    # 启动时要恢复（内联脚本 + 运行期兜底都要认同一个 key）
    assert js.count('localStorage.getItem("%s")' % key) >= 1
    assert html.count('localStorage.getItem("%s")' % key) == 1
    # 没选过就跟系统偏好
    assert "prefers-color-scheme: dark" in html, "没选过时应当跟随系统主题"
    # 隐私模式（localStorage 抛错）不能把页面搞崩
    assert html.count("catch") >= 1 and "catch (err)" in js


def test_theme_is_applied_before_stylesheet():
    """内联脚本必须在 <link rel=stylesheet> 之前，否则开屏动画会先闪一下亮色。"""
    html = (WEB / "index.html").read_text(encoding="utf-8")
    script = html.index("alife-theme")
    css = html.index('href="style.css"')
    assert script < css, "主题脚本要放在样式表之前才能首帧生效"


def test_boot_screen_uses_theme_variables_only():
    """开屏动画不能有写死的亮色：黑夜主题下会白得刺眼。

    （实测的漏网：图标扫光写的是 #ffffff96，暗色下非常扎眼。）
    """
    css = (WEB / "style.css").read_text(encoding="utf-8")
    start = css.index(".boot {")
    end = css.index("@keyframes boot-ring-in")
    block = css[start:end]
    hardcoded = [
        hexcode
        for hexcode in re.findall(r"#[0-9a-fA-F]{3,8}", block)
        if hexcode.lower() not in {"#000", "#0000"}
    ]
    assert hardcoded == [], "开屏样式里不该有写死的颜色：%s" % hardcoded
    # 扫光必须有明暗两套值
    assert "var(--sheen)" in block
    assert css.count("--sheen:") == 2, "亮色/暗色两个主题各要有一份 --sheen"
