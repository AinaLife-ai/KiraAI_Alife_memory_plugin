/* Page-local visualization: edges always open their supporting fact. */
const graphView = { scale: 1, x: 0, y: 0 };
const GRAPH_CENTER = { x: 450, y: 285 };
const GRAPH_MIN = 0.5;
const GRAPH_MAX = 2.5;
let graphSliderBound = false;

function graphTransform() {
  return `translate(${graphView.x} ${graphView.y}) scale(${graphView.scale})`;
}

function graphZoomAt(px, py, factor) {
  const scale = Math.min(GRAPH_MAX, Math.max(GRAPH_MIN, graphView.scale * factor));
  const ratio = scale / graphView.scale;
  graphView.x = px - ratio * (px - graphView.x);
  graphView.y = py - ratio * (py - graphView.y);
  graphView.scale = scale;
}

function graphSyncZoom() {
  const slider = document.querySelector("#graphZoom"),
    output = document.querySelector("#graphZoomValue");
  if (slider) slider.value = String(Math.round(graphView.scale * 100));
  if (output)
    output.textContent =
      (slider ? Number(slider.value) : Math.round(graphView.scale * 100)) + "%";
}

function bindGraphSlider(container) {
  const slider = document.querySelector("#graphZoom");
  if (!slider || graphSliderBound) return;
  graphSliderBound = true;
  slider.addEventListener("input", () => {
    const target = Number(slider.value) / 100;
    if (!target) return;
    graphZoomAt(GRAPH_CENTER.x, GRAPH_CENTER.y, target / graphView.scale);
    const viewport = container.querySelector("#memory-zoom");
    if (viewport) viewport.setAttribute("transform", graphTransform());
    graphSyncZoom();
  });
}

function resetGraphZoom(container) {
  graphView.scale = 1;
  graphView.x = 0;
  graphView.y = 0;
  const viewport = (container || document).querySelector("#memory-zoom");
  if (viewport) viewport.setAttribute("transform", graphTransform());
  graphSyncZoom();
}

function nodeColorClass(id) {
  let hash = 0;
  for (const ch of String(id)) hash = (hash * 31 + ch.codePointAt(0)) >>> 0;
  return " c" + (hash % 8);
}

function drawMemoryGraph(container, facts, mode, names, openFact, onNode, selectedId) {
  container.replaceChildren();
  const nodes = new Map(),
    edges = [];
  const add = (id, label, dimension = false) => {
    if (!nodes.has(id) && nodes.size < 36)
      nodes.set(id, { id, label, dimension });
    return nodes.has(id);
  };
  for (const fact of facts) {
    const relations =
      mode === "relations"
        ? fact.verified_relations || []
        : [
            {
              subject: fact.subject,
              predicate: labels[fact.category],
              object: "dimension:" + fact.category,
            },
          ];
    for (const relation of relations) {
      const a = relation.subject,
        b = relation.object;
      const dimension = b.startsWith("dimension:");
      if (
        add(a, names[a] || a) &&
        add(b, dimension ? labels[fact.category] : names[b] || b, dimension)
      ) {
        edges.push({ a, b, label: relation.predicate, fact });
      }
    }
  }
  if (!edges.length) {
    const p = document.createElement("p");
    p.className = "empty";
    p.textContent =
      mode === "relations"
        ? "还没有可展示的具体关系。待审校的连线保留在下方事实卡片中；也可切换维度网络。"
        : "这个筛选范围内还没有事实。";
    container.append(p);
    graphSyncZoom();
    return;
  }
  const ns = "http://www.w3.org/2000/svg";
  const element = (tag, attributes = {}, text) => {
    const el = document.createElementNS(ns, tag);
    for (const [key, value] of Object.entries(attributes))
      el.setAttribute(key, value);
    if (text !== undefined) el.textContent = text;
    return el;
  };
  const svg = element("svg", {
    viewBox: "0 0 900 580",
    role: "group",
    "aria-label": "交互记忆网络，点击连线读取来源事实",
  });
  const defs = element("defs");
  const marker = element("marker", {
    id: "memory-arrow",
    viewBox: "0 0 10 10",
    refX: 9,
    refY: 5,
    markerWidth: 5,
    markerHeight: 5,
    orient: "auto-start-reverse",
  });
  marker.append(
    element("path", { d: "M 0 0 L 10 5 L 0 10 z", fill: "var(--accent)" }),
  );
  defs.append(marker);
  svg.append(defs);
  const viewport = element("g", { id: "memory-zoom" });
  viewport.setAttribute("transform", graphTransform());
  svg.append(viewport);
  const drift = element("g", { class: "graph-drift" });
  viewport.append(drift);
  const ordered = [...nodes.values()];
  ordered.forEach((node, i) => {
    const angle = (Math.PI * 2 * i) / ordered.length - Math.PI / 2;
    node.x = 450 + Math.cos(angle) * 330;
    node.y = 285 + Math.sin(angle) * 205;
  });
  const edgeGroups = [];
  const nodeEntries = [];
  let activeNode = null;
  const dimEdges = (nodeId) => {
    edgeGroups.forEach(({ group: edgeGroup, edge }) => {
      const off = !!nodeId && edge.a !== nodeId && edge.b !== nodeId;
      edgeGroup.classList.toggle("dimmed", off);
      edgeGroup.setAttribute("tabindex", off ? "-1" : "0");
    });
  };
  const applyVisual = (nodeId) => {
    nodeEntries.forEach(({ node, group }) =>
      group.classList.toggle("active", node.id === nodeId),
    );
    dimEdges(nodeId);
  };
  const applySelection = (nodeId) => {
    activeNode = nodeId;
    applyVisual(nodeId);
    if (onNode) onNode(nodeId);
  };
  edges.slice(0, 100).forEach((edge, i) => {
    const a = nodes.get(edge.a),
      b = nodes.get(edge.b);
    const dx = b.x - a.x,
      dy = b.y - a.y,
      distance = Math.hypot(dx, dy) || 1;
    const endX = b.x - (dx / distance) * 34,
      endY = b.y - (dy / distance) * 34;
    const group = element("g", {
      class: "graph-edge",
      tabindex: 0,
      role: "button",
      "aria-label":
        a.label + " → " + edge.label + " → " + b.label + "，查看证据",
    });
    const path = `M ${a.x} ${a.y} Q 450 ${285 + ((i % 3) - 1) * 45} ${endX} ${endY}`;
    group.append(element("path", { d: path, class: "edge-hit" }));
    group.append(
      element("path", {
        d: path,
        class: "edge-line",
        "marker-end": "url(#memory-arrow)",
      }),
    );
    group.append(element("title", {}, edge.label + " · " + edge.fact.content));
    const label = element(
      "text",
      {
        x: (a.x + b.x) / 4 + 225,
        y: (a.y + b.y) / 4 + 142 + ((i % 3) - 1) * 22,
        class: "edge-label",
      },
      edge.label,
    );
    group.append(label);
    group.onclick = () => {
      if (group.classList.contains("dimmed")) return;
      openFact(edge.fact);
    };
    group.onkeydown = (event) => {
      if (["Enter", " "].includes(event.key)) {
        event.preventDefault();
        group.onclick();
      }
    };
    drift.append(group);
    edgeGroups.push({ group, edge });
  });
  for (const node of ordered) {
    const group = element("g", {
      class:
        "graph-node" +
        nodeColorClass(node.id) +
        (node.dimension ? " dimension-node" : ""),
      tabindex: 0,
      role: "button",
      "aria-label": node.label + " · " + node.id + "，点击查看联结与事实",
      transform: `translate(${node.x} ${node.y})`,
    });
    const float = element("g", { class: "node-float" });
    float.style.animationDuration = 6 + ((node.x + node.y) % 5) + "s";
    float.style.animationDelay = "-" + ((node.x * 7 + node.y * 13) % 9) + "s";
    float.append(element("circle", { r: 32, class: "node-halo" }));
    float.append(element("circle", { r: 23 }));
    float.append(
      element(
        "text",
        { y: 5, class: "node-initial" },
        node.dimension ? "◇" : [...node.label][0],
      ),
    );
    float.append(
      element(
        "text",
        { y: 51, class: "node-label" },
        [...node.label].slice(0, 11).join("") +
          ([...node.label].length > 11 ? "…" : ""),
      ),
    );
    group.append(float);
    group.append(element("title", {}, node.label + "\n" + node.id));
    const hover = () => {
      if (!activeNode) dimEdges(node.id);
    };
    group.onmouseenter = hover;
    group.onfocus = hover;
    group.onmouseleave = () => {
      if (!activeNode) dimEdges(null);
    };
    group.onblur = group.onmouseleave;
    group.onclick = () =>
      applySelection(activeNode === node.id ? null : node.id);
    group.onkeydown = (event) => {
      if (["Enter", " "].includes(event.key)) {
        event.preventDefault();
        group.onclick();
      }
    };
    drift.append(group);
    nodeEntries.push({ node, group });
  }
  svg.addEventListener("click", (event) => {
    if (event.target === svg) applySelection(null);
  });
  if (selectedId && nodeEntries.some((entry) => entry.node.id === selectedId)) {
    activeNode = selectedId;
    applyVisual(selectedId);
  }
  svg.addEventListener(
    "wheel",
    (event) => {
      event.preventDefault();
      const matrix = svg.getScreenCTM();
      if (!matrix) return;
      const point = svg.createSVGPoint();
      point.x = event.clientX;
      point.y = event.clientY;
      const local = point.matrixTransform(matrix.inverse());
      graphZoomAt(local.x, local.y, Math.exp(-event.deltaY * 0.0015));
      viewport.setAttribute("transform", graphTransform());
      graphSyncZoom();
    },
    { passive: false },
  );
  container.append(svg);
  bindGraphSlider(container);
  graphSyncZoom();
  const caption = document.createElement("small");
  caption.textContent = `${nodes.size} 个节点 · ${Math.min(edges.length, 100)} 条联结。每页最多展示36个节点与100条连线；完整内容见下方卡片和翻页。`;
  container.append(caption);
}
