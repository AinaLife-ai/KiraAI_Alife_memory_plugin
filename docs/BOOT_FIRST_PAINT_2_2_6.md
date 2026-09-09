# 2.2.6：开屏动画改为首屏即出现

## 问题

开屏动画原来由 app.js 在脚本执行时**动态注入**，于是页面主体先渲染出来、过一会儿才弹出动画——像"加载完才补一个弹窗"，不像开机。

## 改动

1. **静态标记**：`#boot` 的结构直接写进 `index.html`，并去掉 `hidden`。浏览器首次绘制时它就带着不透明背景盖住整页，不再有"先看到主页"的空窗。
2. **预加载素材**：`<link rel="preload" as="image" href="brand.png">`，避免动画等图片下载。
3. **关闭时零闪烁**：动画标记后面紧跟一段内联脚本，读取本地镜像 `alife-boot`，若用户关了动画就立刻 `hidden = true`——在首屏绘制前就藏好，不会闪一下。
4. **重播用克隆**：`playBoot(true)` 通过 `cloneNode` 替换节点来重启动画（含 `::before/::after` 伪元素），静态标记仍是唯一来源，不再有两份 HTML。
5. 点击跳过改为**事件委托**（`closest("#boot")`），克隆后依然有效；无 JS 时动画也会按 CSS 自行淡出并 `visibility:hidden`，不会挡住界面。

## 验证

- 浏览器实测：首屏即显示动画（静态标记 + 不透明背景）；点击任意处跳过；`playBoot(true)` 克隆重播并换一句格言；`localStorage.alife-boot={"enabled":false}` 时刷新后 `hidden=true`、`display:none`（无闪烁）。
- 全量 `pytest tests/`：**77 passed, 1 skipped, 7 subtests passed**。
