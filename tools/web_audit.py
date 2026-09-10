"""前端静态审计：属性读写一致性 + 接口与后端路由一致性。

背景：v2.5.7 引入 `data-jobdetail` 后，处理器仍读 `dataset.job`，
点击「明细」请求打到 /job/undefined → 404。这类错位不会报语法错、测试也测不到，
所以固化成静态检查，由 tests/test_web_static.py 调用。

四类检查：
  A. dataset.X 被读取，但没有任何地方写出 data-x（→ 拼出 undefined）
  B. data-x 被写出，但没有任何读取方，也不被 CSS 使用（→ 属性名可能写错）
  C. $$("[data-x]") 绑定的处理器里，读的 dataset 键与选择器对不上
  D. app.js 调用的接口路径/方法，在 main.py 注册的路由里不存在

用法：python3 tools/web_audit.py   （有 A/C/D 类问题则退出码 1）
"""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8") if path.exists() else ""


def kebab(name: str) -> str:
    """dataset 键 → data-* 属性名：jobDetail → data-job-detail"""
    return "data-" + re.sub(r"(?<!^)(?=[A-Z])", "-", name).lower()


def camel(attr: str) -> str:
    """data-job-detail → jobDetail"""
    return re.sub(r"-([a-z])", lambda m: m.group(1).upper(), attr[len("data-") :])


def _written_attrs(js: str, html: str) -> tuple[dict[str, list[str]], set[str]]:
    """收集 data-* 属性。

    返回 (真正被写出的属性, 仅在选择器/CSS 里被引用的属性)。
    区分二者很重要：只在选择器里出现 ≠ 有元素带这个属性（绑定会落空）。
    """
    written: dict[str, list[str]] = {}
    referenced: set[str] = set()
    for m in re.finditer(r"\bdata-([a-zA-Z][a-zA-Z0-9-]*)", html):
        written.setdefault("data-" + m.group(1), []).append("index.html")
    # JS 模板里拼出的属性：data-x="…" 或 data-x=' + …
    for m in re.finditer(r"data-([a-zA-Z][a-zA-Z0-9-]*)\s*=", js):
        written.setdefault("data-" + m.group(1), []).append("app.js")
    # document.documentElement.dataset.x = ...
    for m in re.finditer(r"\.dataset\.([a-zA-Z][a-zA-Z0-9]*)\s*=(?!=)", js):
        written.setdefault(kebab(m.group(1)), []).append("app.js:dataset-write")
    # 选择器里出现的属性（可能是运行时 setAttribute 出来的，单独归类）
    for m in re.finditer(r"""\$\$?\(\s*["'`][^"'`]*\[(data-[a-zA-Z0-9-]+)\]""", js):
        referenced.add(m.group(1))
    for m in re.finditer(r"setAttribute\(\s*[\"'](data-[a-zA-Z0-9-]+)", js):
        written.setdefault(m.group(1), []).append("app.js:setAttribute")
    return written, referenced


def _read_attrs(js: str) -> dict[str, list[int]]:
    reads: dict[str, list[int]] = {}
    for m in re.finditer(r"\.dataset\.([a-zA-Z][a-zA-Z0-9]*)", js):
        reads.setdefault(kebab(m.group(1)), []).append(js[: m.start()].count("\n") + 1)
    return reads


def _handler_body(js: str, start: int, limit: int = 400) -> str:
    """取绑定点之后的一小段处理器代码，遇到下一个语句块/绑定就停。"""
    tail = js[start : start + limit]
    stop = re.search(r"\n\s*(?:\$\$?\(|function )", tail[1:])
    if stop:
        tail = tail[: stop.start() + 1]
    return tail


def _check_bindings(js: str) -> list[str]:
    """C 类：选择器 [data-x] 与处理器实际读取的 dataset 键是否对应。"""
    issues: list[str] = []
    for m in re.finditer(r"""\$\$?\(\s*["'`]([^"'`]*\[data-[^"'`]*)["'`]\s*\)""", js):
        selector = m.group(1)
        attrs = re.findall(r"\[(data-[a-zA-Z0-9-]+)\]", selector)
        if not attrs:
            continue
        body = _handler_body(js, m.end())
        read_keys = {
            kebab(k) for k in re.findall(r"\.dataset\.([a-zA-Z][a-zA-Z0-9]*)", body)
        }
        if not read_keys:
            continue  # 处理器没读 dataset：可能用事件目标或外部状态，跳过
        line = js[: m.start()].count("\n") + 1
        for attr in attrs:
            if attr not in read_keys:
                issues.append(
                    f"app.js:{line} 绑定 [{attr}]，但处理器读取的是 {sorted(read_keys)}"
                )
    return issues


def _backend_routes(main_py: str) -> set[tuple[str, str]]:
    return {
        (m.group(1), m.group(2))
        for m in re.finditer(
            r'@register\.api\(\s*method="([A-Z]+)"\s*,\s*path="([^"]+)"', main_py
        )
    }


def _api_calls(js: str) -> list[tuple[int, str, str]]:
    """抓 api("...") 调用 → (行号, 路径字面量, 方法)。

    只看第一个参数的字符串字面量；第二参数存在即视为 POST（api() 的约定）。
    """
    calls: list[tuple[int, str, str]] = []
    for m in re.finditer(r"\bapi\(", js):
        start = m.end()
        depth, i = 1, start
        while i < len(js) and depth:
            if js[i] in "([{":
                depth += 1
            elif js[i] in ")]}":
                depth -= 1
            i += 1
        args = js[start : i - 1]
        # 顶层逗号 → 有第二参数
        depth = 0
        has_second = False
        for ch in args:
            if ch in "([{":
                depth += 1
            elif ch in ")]}":
                depth -= 1
            elif ch == "," and depth == 0:
                has_second = True
                break
        first = args.split(",")[0].strip()
        lit = re.match(r"""^["'`]([^"'`]*)["'`]""", first)
        if not lit or not lit.group(1).startswith("/"):
            continue
        line = js[: m.start()].count("\n") + 1
        calls.append((line, lit.group(1), "POST" if has_second else "GET"))
    return calls


def _check_routes(js: str, main_py: str) -> list[str]:
    """D 类：前端调用的路径在不在后端路由表里（路径前缀 + 方法）。"""
    routes = _backend_routes(main_py)
    if not routes:
        return []

    def placeholder(p: str) -> str:
        return re.sub(r"\{[^}]*\}", "{}", p.split("?")[0])

    def norm(p: str) -> str:
        return placeholder(p).rstrip("/")

    issues: list[str] = []
    for line, raw, method in _api_calls(js):
        want = norm(raw)
        exact = [(mm, pp) for mm, pp in routes if norm(pp) == want]
        # /memory/ + id 这类拼接调用：按前缀匹配「集合 + 路径参数」的子资源路由
        prefix = [
            (mm, pp)
            for mm, pp in routes
            if placeholder(pp).startswith(placeholder(raw))
            and norm(pp) != want
        ]
        same = exact + [c for c in prefix if c not in exact]
        if not same:
            issues.append(f"app.js:{line} 调用 {method} {raw} → 后端无对应路由")
            continue
        if not any(mm == method for mm, _ in same):
            got = sorted({mm for mm, _ in same})
            issues.append(
                f"app.js:{line} 调用 {method} {raw} → 后端只有 {got} "
                f"（{'/'.join(pp for _, pp in same)}）"
            )
    return issues


def audit(root: Path | str = ROOT) -> dict[str, list[str]]:
    root = Path(root)
    js = _read(root / "web" / "app.js")
    html = _read(root / "web" / "index.html")
    css = _read(root / "web" / "style.css")
    main_py = _read(root / "main.py")

    written, referenced = _written_attrs(js, html)
    reads = _read_attrs(js)
    css_used = {
        "data-" + m.group(1)
        for m in re.finditer(r"\[data-([a-zA-Z][a-zA-Z0-9-]*)", css)
    }

    missing_writer = {
        attr: lines
        for attr, lines in sorted(reads.items())
        if attr not in written and attr not in css_used and attr not in referenced
    }
    orphan_writer = {
        attr: where
        for attr, where in sorted(written.items())
        if attr not in reads and attr not in css_used
    }
    return {
        "missing_writer": [
            f"读取了无处写入的属性 {attr}（app.js:{lines}）"
            for attr, lines in missing_writer.items()
        ],
        "orphan_writer": [
            f"写出了无人读取的属性 {attr}（{where}）" for attr, where in orphan_writer.items()
        ],
        "binding_mismatch": _check_bindings(js),
        "route_mismatch": _check_routes(js, main_py),
    }


def main() -> int:
    result = audit()
    titles = {
        "missing_writer": "A. dataset 读取方找不到写入方（→ undefined）",
        "orphan_writer": "B. data-* 写出后没有任何读取方（属性名可能写错）",
        "binding_mismatch": "C. 绑定选择器与处理器读取的 dataset 键不一致",
        "route_mismatch": "D. 前端调用的接口与后端路由不一致",
    }
    fatal = 0
    for key, title in titles.items():
        print("=" * 66)
        print(title)
        print("=" * 66)
        items = result[key]
        if items:
            for it in items:
                print("  ✗ " + it)
            if key != "orphan_writer":
                fatal += len(items)
        else:
            print("  （无）")
        print()
    print(f"需要修复的问题：{fatal} 个")
    return 1 if fatal else 0


if __name__ == "__main__":
    raise SystemExit(main())
