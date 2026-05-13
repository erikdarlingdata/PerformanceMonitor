const state = {
  selectedServerId: null,
  servers: [],
  healthSnapshot: JSON.parse(localStorage.getItem("pm-headless-health") || "{}"),
  loadedOnce: false
};

const els = {
  green: document.getElementById("metric-green"),
  yellow: document.getElementById("metric-yellow"),
  red: document.getElementById("metric-red"),
  disabled: document.getElementById("metric-disabled"),
  generatedAt: document.getElementById("generated-at"),
  serverCount: document.getElementById("server-count"),
  storagePaths: document.getElementById("storage-paths"),
  serverCardGrid: document.getElementById("server-card-grid"),
  serverRows: document.getElementById("server-rows"),
  collectorLog: document.getElementById("collector-log"),
  selectedTitle: document.getElementById("selected-server-title"),
  selectedSubtitle: document.getElementById("selected-server-subtitle"),
  waitList: document.getElementById("wait-list"),
  cpuCanvas: document.getElementById("cpu-chart"),
  refresh: document.getElementById("refresh-button"),
  notify: document.getElementById("notify-button"),
  toastRegion: document.getElementById("toast-region")
};

els.refresh.addEventListener("click", () => loadAll());
els.notify.addEventListener("click", async () => {
  if (!("Notification" in window)) {
    showToast("Browser notifications are not supported here.", "yellow");
    return;
  }

  const permission = await Notification.requestPermission();
  updateNotifyButton();
  showToast(permission === "granted" ? "Browser notifications enabled." : "Browser notifications not enabled.", permission === "granted" ? "green" : "yellow");
});

async function fetchJson(url) {
  const response = await fetch(url, { headers: { "Accept": "application/json" } });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

async function loadAll() {
  const [summary, storage, logs] = await Promise.all([
    fetchJson("/api/summary"),
    fetchJson("/api/storage"),
    fetchJson("/api/collection-log?limit=50")
  ]);

  state.servers = summary.servers || [];
  if (!state.selectedServerId && state.servers.length > 0) {
    const firstOnline = state.servers.find(s => s.isEnabled) || state.servers[0];
    state.selectedServerId = firstOnline.serverId;
  }

  renderSummary(summary);
  renderStorage(storage);
  renderServerCards(state.servers);
  renderServers(state.servers);
  renderLog(logs);
  handleHealthNotifications(state.servers);
  state.loadedOnce = true;
  await loadSelectedServer();
  updateNotifyButton();
}

function renderSummary(summary) {
  els.green.textContent = summary.greenCount;
  els.yellow.textContent = summary.yellowCount;
  els.red.textContent = summary.redCount;
  els.disabled.textContent = summary.disabledCount;
  els.generatedAt.textContent = `Updated ${formatDate(summary.generatedAt)}`;
  els.serverCount.textContent = `${summary.serverCount} configured`;
}

function renderStorage(storage) {
  els.storagePaths.textContent = `DuckDB ${storage.duckdb} | Parquet ${storage.parquet}`;
}

function renderServerCards(servers) {
  els.serverCardGrid.innerHTML = "";
  for (const server of servers) {
    const card = document.createElement("button");
    card.type = "button";
    card.className = `server-card health-${server.healthState || "yellow"} ${server.serverId === state.selectedServerId ? "selected" : ""}`;
    card.addEventListener("click", async () => {
      state.selectedServerId = server.serverId;
      renderServerCards(state.servers);
      renderServers(state.servers);
      await loadSelectedServer();
    });

    card.innerHTML = `
      <span class="card-status">${escapeHtml((server.healthState || "yellow").toUpperCase())}</span>
      <strong>${escapeHtml(server.displayName || server.serverId)}</strong>
      <span>${escapeHtml(server.healthReason || "No status yet")}</span>
      <small>${server.activeAlertCount ? `${server.activeAlertCount} active alert(s)` : formatDate(server.lastSeenTime) || "No contact yet"}</small>
    `;
    els.serverCardGrid.appendChild(card);
  }
}

function renderServers(servers) {
  els.serverRows.innerHTML = "";
  for (const server of servers) {
    const tr = document.createElement("tr");
    tr.className = server.serverId === state.selectedServerId ? "selected" : "";
    tr.addEventListener("click", async () => {
      state.selectedServerId = server.serverId;
      renderServers(state.servers);
      await loadSelectedServer();
    });

    const statusClass = server.healthState || "yellow";

    tr.innerHTML = `
      <td><span class="traffic ${statusClass}">${escapeHtml((server.healthState || "yellow").toUpperCase())}</span></td>
      <td title="${escapeHtml(server.serverId)}">${escapeHtml(server.displayName || server.serverId)}</td>
      <td>${escapeHtml(server.edition || "")}</td>
      <td>${escapeHtml(server.productVersion || "")}</td>
      <td>${formatDate(server.lastSeenTime)}</td>
      <td title="${escapeHtml(server.healthReason || server.lastError || "")}">${escapeHtml(server.healthReason || server.lastError || "")}</td>
    `;

    els.serverRows.appendChild(tr);
  }
}

function handleHealthNotifications(servers) {
  const nextSnapshot = {};
  for (const server of servers) {
    const health = server.healthState || "yellow";
    nextSnapshot[server.serverId] = health;

    const isAlert = health === "red" || health === "yellow";
    const prior = state.healthSnapshot[server.serverId];
    const isNewOrChanged = prior !== health;
    const shouldNotifyInitial = !state.loadedOnce && health === "red";

    if (isAlert && (isNewOrChanged || shouldNotifyInitial)) {
      const title = `${server.displayName || server.serverId} is ${health.toUpperCase()}`;
      const body = server.healthReason || "Server needs attention";
      showToast(`${title}: ${body}`, health);
      if ("Notification" in window && Notification.permission === "granted") {
        new Notification(title, { body });
      }
    }
  }

  state.healthSnapshot = nextSnapshot;
  localStorage.setItem("pm-headless-health", JSON.stringify(nextSnapshot));
}

function showToast(message, health) {
  const toast = document.createElement("div");
  toast.className = `toast ${health}`;
  toast.textContent = message;
  els.toastRegion.appendChild(toast);
  window.setTimeout(() => toast.remove(), 9000);
}

function updateNotifyButton() {
  if (!("Notification" in window)) {
    els.notify.textContent = "Notifications Unavailable";
    els.notify.disabled = true;
    return;
  }

  els.notify.textContent = Notification.permission === "granted"
    ? "Notifications Enabled"
    : "Enable Notifications";
}

async function loadSelectedServer() {
  const server = state.servers.find(s => s.serverId === state.selectedServerId);
  if (!server) {
    els.selectedTitle.textContent = "Server Detail";
    els.selectedSubtitle.textContent = "Select a server row";
    els.waitList.innerHTML = "";
    drawCpuChart([]);
    return;
  }

  els.selectedTitle.textContent = server.displayName || server.serverId;
  els.selectedSubtitle.textContent = server.edition || server.productVersion || server.serverId;

  const [waits, cpu] = await Promise.all([
    fetchJson(`/api/servers/${encodeURIComponent(server.serverId)}/waits?hours=1&limit=12`),
    fetchJson(`/api/servers/${encodeURIComponent(server.serverId)}/cpu?hours=1`)
  ]);

  renderWaits(waits);
  drawCpuChart(cpu);
}

function renderWaits(waits) {
  els.waitList.innerHTML = "";
  if (!waits.length) {
    els.waitList.innerHTML = `<div class="wait-row">No wait deltas yet</div>`;
    return;
  }

  const max = Math.max(...waits.map(w => w.waitTimeDeltaMs), 1);
  for (const wait of waits) {
    const row = document.createElement("div");
    row.className = "wait-row";
    const width = Math.max(2, Math.round(wait.waitTimeDeltaMs / max * 100));
    row.innerHTML = `
      <strong title="${escapeHtml(wait.waitType)}">${escapeHtml(wait.waitType)}</strong>
      <span class="bar-track"><span class="bar-fill" style="width:${width}%"></span></span>
      <span>${formatMs(wait.waitTimeDeltaMs)}</span>
    `;
    els.waitList.appendChild(row);
  }
}

function renderLog(logs) {
  els.collectorLog.innerHTML = "";
  for (const log of logs) {
    const row = document.createElement("div");
    const statusClass = (log.status || "").toLowerCase();
    row.className = "log-row";
    row.title = log.errorMessage || "";
    row.innerHTML = `
      <span>${formatTime(log.collectionTime)}</span>
      <span>${escapeHtml(log.serverName)} / ${escapeHtml(log.collectorName)}</span>
      <span class="status ${statusClass}">${escapeHtml(log.status)}</span>
      <span>${log.rowsCollected ?? 0}</span>
    `;
    els.collectorLog.appendChild(row);
  }
}

function drawCpuChart(samples) {
  const canvas = els.cpuCanvas;
  const rect = canvas.getBoundingClientRect();
  const ratio = window.devicePixelRatio || 1;
  canvas.width = Math.max(1, Math.round(rect.width * ratio));
  canvas.height = Math.max(1, Math.round(rect.height * ratio));

  const ctx = canvas.getContext("2d");
  ctx.scale(ratio, ratio);
  ctx.clearRect(0, 0, rect.width, rect.height);

  ctx.strokeStyle = cssVar("--border", "#2a313c");
  ctx.lineWidth = 1;
  for (let i = 0; i <= 4; i++) {
    const y = 12 + (rect.height - 24) * i / 4;
    ctx.beginPath();
    ctx.moveTo(0, y);
    ctx.lineTo(rect.width, y);
    ctx.stroke();
  }

  if (!samples.length) {
    ctx.fillStyle = cssVar("--muted", "#9ca8b8");
    ctx.font = "13px Segoe UI, sans-serif";
    ctx.fillText("No CPU samples yet", 12, 28);
    return;
  }

  const points = samples.map((sample, index) => ({
    x: samples.length === 1 ? rect.width - 10 : 10 + index * (rect.width - 20) / (samples.length - 1),
    y: 10 + (100 - Math.min(100, Math.max(0, sample.sqlServerCpuUtilization))) * (rect.height - 20) / 100
  }));

  ctx.strokeStyle = cssVar("--accent", "#62b6ff");
  ctx.lineWidth = 2;
  ctx.beginPath();
  points.forEach((point, index) => {
    if (index === 0) ctx.moveTo(point.x, point.y);
    else ctx.lineTo(point.x, point.y);
  });
  ctx.stroke();

  const latest = samples[samples.length - 1];
  ctx.fillStyle = cssVar("--text", "#edf2f7");
  ctx.font = "700 13px Segoe UI, sans-serif";
  ctx.fillText(`${latest.sqlServerCpuUtilization}% SQL CPU`, 12, 22);
}

function cssVar(name, fallback) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

function formatDate(value) {
  if (!value) return "";
  return new Date(value).toLocaleString();
}

function formatTime(value) {
  if (!value) return "";
  return new Date(value).toLocaleTimeString();
}

function formatMs(ms) {
  if (ms > 3600000) return `${(ms / 3600000).toFixed(1)}h`;
  if (ms > 60000) return `${(ms / 60000).toFixed(1)}m`;
  return `${(ms / 1000).toFixed(1)}s`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

window.addEventListener("resize", () => loadSelectedServer());
loadAll().catch(error => {
  els.generatedAt.textContent = error.message;
});
