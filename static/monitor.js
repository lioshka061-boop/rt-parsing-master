(() => {
  const root = document.getElementById("system-monitor");
  if (!root) return;

  const endpoint = root.dataset.endpoint || "/control_panel/system_stats";
  const toggle = root.querySelector("[data-monitor-toggle]");
  const panel = root.querySelector("[data-monitor-panel]");
  const summary = root.querySelector("[data-monitor-summary]");
  const imports = root.querySelector("[data-monitor-imports]");
  const errors = root.querySelector("[data-monitor-errors]");

  const setText = (node, text) => {
    if (!node) return;
    node.textContent = text;
  };

  const formatLoad = (load) =>
    `${load.one.toFixed(2)} / ${load.five.toFixed(2)} / ${load.fifteen.toFixed(2)}`;

  const formatMemory = (mem) =>
    `${mem.used_mb}MB / ${mem.total_mb}MB (${mem.used_percent}%)`;

  const formatImports = (data) => {
    const parts = [];
    parts.push(`site: ${data.site_in_progress} in progress`);
    if (data.site_enqueued > 0) parts.push(`${data.site_enqueued} queued`);
    if (data.site_suspended > 0) parts.push(`${data.site_suspended} suspended`);
    if (data.site_failed > 0) parts.push(`${data.site_failed} failed`);
    if (data.ddaudio_in_progress > 0) parts.push(`ddaudio: ${data.ddaudio_in_progress} running`);
    if (data.ddaudio_failed > 0) parts.push(`ddaudio: ${data.ddaudio_failed} failed`);
    if (data.exports_in_progress > 0) parts.push(`exports: ${data.exports_in_progress} running`);
    if (data.dt_stage) {
      const progress =
        data.dt_ready !== null && data.dt_total !== null
          ? ` (${data.dt_ready}/${data.dt_total})`
          : "";
      parts.push(`dt: ${data.dt_stage}${progress}`);
    }
    return parts.join(" | ");
  };

  const renderErrors = (list) => {
    if (!errors) return;
    if (!list || list.length === 0) {
      errors.textContent = "";
      return;
    }
    errors.textContent = list.join(" | ");
  };

  const update = async () => {
    try {
      const res = await fetch(endpoint, { cache: "no-store" });
      if (!res.ok) throw new Error("status");
      const data = await res.json();
      setText(summary, `Load: ${formatLoad(data.load)} | RAM: ${formatMemory(data.memory)}`);
      setText(imports, formatImports(data.imports));
      renderErrors(data.errors);
    } catch (err) {
      setText(summary, "Дані недоступні");
    }
  };

  if (toggle) {
    toggle.addEventListener("click", () => {
      root.classList.toggle("open");
      if (panel && root.classList.contains("open")) {
        update();
      }
    });
  }

  update();
  setInterval(update, 15000);
})();
