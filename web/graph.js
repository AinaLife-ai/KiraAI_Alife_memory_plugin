/* Page-local visualization: edges always open their supporting fact. */
function drawMemoryGraph(container, facts, mode, names, openFact) {
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
  const ordered = [...nodes.values()];
  ordered.forEach((node, i) => {
    const angle = (Math.PI * 2 * i) / ordered.length - Math.PI / 2;
    node.x = 450 + Math.cos(angle) * 330;
    node.y = 285 + Math.sin(angle) * 205;
  });
  const edgeGroups = [];
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
    group.onclick = () => openFact(edge.fact);
    group.onkeydown = (event) => {
      if (["Enter", " "].includes(event.key)) {
        event.preventDefault();
        openFact(edge.fact);
      }
    };
    svg.append(group);
    edgeGroups.push({ group, edge });
  });
  for (const node of ordered) {
    const group = element("g", {
      class: "graph-node" + (node.dimension ? " dimension-node" : ""),
      tabindex: 0,
      role: "button",
      "aria-label": node.label + " · " + node.id + "，聚焦联结",
      transform: `translate(${node.x} ${node.y})`,
    });
    group.append(element("circle", { r: 32, class: "node-halo" }));
    group.append(element("circle", { r: 23 }));
    group.append(
      element(
        "text",
        { y: 5, class: "node-initial" },
        node.dimension ? "◇" : [...node.label][0],
      ),
    );
    group.append(
      element(
        "text",
        { y: 51, class: "node-label" },
        [...node.label].slice(0, 11).join("") +
          ([...node.label].length > 11 ? "…" : ""),
      ),
    );
    group.append(element("title", {}, node.label + "\n" + node.id));
    const focus = () =>
      edgeGroups.forEach(({ group: edgeGroup, edge }) =>
        edgeGroup.classList.toggle(
          "dimmed",
          edge.a !== node.id && edge.b !== node.id,
        ),
      );
    group.onmouseenter = focus;
    group.onfocus = focus;
    group.onclick = focus;
    group.onmouseleave = () =>
      edgeGroups.forEach(({ group: edgeGroup }) =>
        edgeGroup.classList.remove("dimmed"),
      );
    group.onblur = group.onmouseleave;
    svg.append(group);
  }
  container.append(svg);
  const caption = document.createElement("small");
  caption.textContent = `${nodes.size} 个节点 · ${Math.min(edges.length, 100)} 条联结。每页最多展示36个节点与100条连线；完整内容见下方卡片和翻页。`;
  container.append(caption);
}
