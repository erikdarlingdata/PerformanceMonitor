const state = {
  selectedServerId: null,
  activeTab: "stats",
  purposeFilter: "all",
  servers: [],
  logs: [],
  alerts: [],
  healthSnapshot: JSON.parse(localStorage.getItem("pm-headless-health") || "{}"),
  loadedOnce: false
};

const els = {
  overviewView: document.getElementById("overview-view"),
  serverView: document.getElementById("server-view"),
  green: document.getElementById("metric-green"),
  yellow: document.getElementById("metric-yellow"),
  red: document.getElementById("metric-red"),
  disabled: document.getElementById("metric-disabled"),
  generatedAt: document.getElementById("generated-at"),
  alertCount: document.getElementById("alert-count"),
  purposeFilter: document.getElementById("purpose-filter"),
  serverCardGrid: document.getElementById("server-card-grid"),
  alertList: document.getElementById("alert-list"),
  collectorLog: document.getElementById("collector-log"),
  selectedTitle: document.getElementById("selected-server-title"),
  selectedSubtitle: document.getElementById("selected-server-subtitle"),
  serverStatsGrid: document.getElementById("server-stats-grid"),
  waitList: document.getElementById("wait-list"),
  cpuCanvas: document.getElementById("cpu-chart"),
  refresh: document.getElementById("refresh-button"),
  notify: document.getElementById("notify-button"),
  back: document.getElementById("back-button"),
  toastRegion: document.getElementById("toast-region")
};

els.refresh.addEventListener("click", () => loadAll());
els.back.addEventListener("click", () => navigateOverview());
els.purposeFilter.addEventListener("change", () => {
  state.purposeFilter = els.purposeFilter.value;
  renderServerCards();
});
document.querySelectorAll(".server-menu button").forEach(button => {
  button.addEventListener("click", () => {
    if (!state.selectedServerId) return;
    navigateServer(state.selectedServerId, button.dataset.tab || "stats");
  });
});

els.notify.addEventListener("click", async () => {
  if (!("Notification" in window)) {
    showToast("Browser notifications are not supported here.", "yellow");
    return;
  }

  const permission = await Notification.requestPermission();
  updateNotifyButton();
  showToast(permission === "granted" ? "Browser notifications enabled." : "Browser notifications not enabled.", permission === "granted" ? "green" : "yellow");
});

window.addEventListener("hashchange", () => applyRoute());
window.addEventListener("resize", () => {
  if (state.activeTab === "cpu") {
    loadSelectedServer();
  }
});

async function fetchJson(url) {
  const response = await fetch(url, { headers: { "Accept": "application/json" } });
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`);
  return response.json();
}

async function loadAll() {
  const [summary, logs] = await Promise.all([
    fetchJson("/api/summary"),
    fetchJson("/api/collection-log?limit=50")
  ]);

  state.servers = summary.servers || [];
  state.logs = logs || [];
  state.alerts = buildAlerts(state.servers, state.logs);

  if (!state.selectedServerId && state.servers.length > 0) {
    const firstActive = state.servers.find(s => s.isEnabled) || state.servers[0];
    state.selectedServerId = firstActive.serverId;
  }

  renderSummary(summary);
  renderPurposeOptions(state.servers);
  renderServerCards();
  renderAlerts(state.alerts);
  handleHealthNotifications(state.servers);
  state.loadedOnce = true;
  applyRoute();
  updateNotifyButton();
}

function renderSummary(summary) {
  els.green.textContent = summary.greenCount;
  els.yellow.textContent = summary.yellowCount;
  els.red.textContent = summary.redCount;
  els.disabled.textContent = summary.disabledCount;
  els.generatedAt.textContent = `Updated ${formatDate(summary.generatedAt)}`;
}

function renderPurposeOptions(servers) {
  const purposes = [...new Set(servers.map(server => normalizePurpose(server.purpose)))].sort(sortPurposes);
  const options = ["all", ...purposes];
  const current = options.includes(state.purposeFilter) ? state.purposeFilter : "all";
  state.purposeFilter = current;
  els.purposeFilter.innerHTML = options
    .map(value => `<option value="${escapeHtml(value)}">${escapeHtml(value === "all" ? "All" : value)}</option>`)
    .join("");
  els.purposeFilter.value = current;
}

function renderServerCards() {
  const visibleServers = state.servers.filter(server => {
    return state.purposeFilter === "all" || normalizePurpose(server.purpose) === state.purposeFilter;
  });

  els.serverCardGrid.innerHTML = "";
  if (!visibleServers.length) {
    els.serverCardGrid.innerHTML = `<div class="empty-state">No servers.</div>`;
    return;
  }

  for (const group of groupServersByPurpose(visibleServers)) {
    const section = document.createElement("section");
    section.className = "purpose-section";
    section.innerHTML = `
      <div class="purpose-heading">
        <h3>${escapeHtml(group.purpose)}</h3>
        <span>${group.servers.length}</span>
      </div>
      <div class="server-card-row"></div>
    `;

    const row = section.querySelector(".server-card-row");
    for (const server of group.servers) {
      row.appendChild(createServerCard(server));
    }

    els.serverCardGrid.appendChild(section);
  }
}

function createServerCard(server) {
  const card = document.createElement("button");
  card.type = "button";
  card.className = `server-card health-${server.healthState || "yellow"} ${server.serverId === state.selectedServerId ? "selected" : ""}`;
  card.addEventListener("click", () => navigateServer(server.serverId, "stats"));

  const title = server.displayName || server.serverId;
  const platform = "SQL Server";
  const os = server.productVersion ? `v${server.productVersion}` : "Windows";
  const health = server.healthState || "yellow";

  card.innerHTML = `
    <div class="server-card-top">
      <span class="server-icon" aria-hidden="true"></span>
      <div>
        <strong>${escapeHtml(title)}</strong>
        <small>${escapeHtml(platform)} / ${escapeHtml(os)}</small>
      </div>
      <span class="card-menu" aria-hidden="true">...</span>
    </div>
    <div class="mini-stats">
      <span><b>${server.latestSqlCpuUtilization ?? "--"}${server.latestSqlCpuUtilization == null ? "" : "%"}</b><small>CPU</small></span>
      <span><b>${escapeHtml(compactWait(server.topWaitType))}</b><small>Wait</small></span>
      <span><b>${server.activeAlertCount ?? 0}</b><small>Alerts</small></span>
      <span><b>${compactSeen(server.lastSeenTime)}</b><small>Seen</small></span>
    </div>
    <div class="server-card-ribbon ${health}">
      <span class="ribbon-dot"></span>
      <span>${escapeHtml(server.healthReason || "All good")}</span>
    </div>
  `;

  return card;
}

function groupServersByPurpose(servers) {
  const groups = new Map();
  for (const server of servers) {
    const purpose = normalizePurpose(server.purpose);
    if (!groups.has(purpose)) {
      groups.set(purpose, []);
    }

    groups.get(purpose).push(server);
  }

  return [...groups.entries()]
    .sort(([left], [right]) => sortPurposes(left, right))
    .map(([purpose, groupedServers]) => ({
      purpose,
      servers: groupedServers.sort((left, right) => {
        const leftHealth = healthRank(left.healthState);
        const rightHealth = healthRank(right.healthState);
        if (leftHealth !== rightHealth) return leftHealth - rightHealth;
        return (left.displayName || left.serverId).localeCompare(right.displayName || right.serverId);
      })
    }));
}

function normalizePurpose(value) {
  const purpose = String(value || "").trim();
  if (!purpose) return "Unassigned";

  switch (purpose.toLowerCase()) {
    case "prod":
      return "Production";
    case "stage":
      return "Staging";
    case "dev":
      return "Development";
    default:
      return purpose;
  }
}

function sortPurposes(left, right) {
  const rank = purpose => {
    switch (purpose.toLowerCase()) {
      case "production":
        return 1;
      case "staging":
        return 2;
      case "development":
        return 3;
      case "test":
        return 4;
      case "unassigned":
        return 99;
      default:
        return 20;
    }
  };

  const leftRank = rank(left);
  const rightRank = rank(right);
  if (leftRank !== rightRank) return leftRank - rightRank;
  return left.localeCompare(right);
}

function healthRank(health) {
  switch ((health || "").toLowerCase()) {
    case "red":
      return 1;
    case "yellow":
      return 2;
    case "green":
      return 3;
    case "disabled":
      return 4;
    default:
      return 5;
  }
}

function buildAlerts(servers, logs) {
  const alerts = [];

  for (const server of servers) {
    const health = server.healthState || "yellow";
    if (health === "red" || health === "yellow") {
      alerts.push({
        serverId: server.serverId,
        serverName: server.displayName || server.serverId,
        state: health,
        title: `${server.displayName || server.serverId} is ${health.toUpperCase()}`,
        body: server.healthReason || "Server needs attention",
        targetTab: "stats",
        time: server.lastSeenTime || null
      });
    }
  }

  for (const log of logs) {
    const status = (log.status || "").toUpperCase();
    if (status === "ERROR" || status === "PERMISSIONS") {
      alerts.push({
        serverId: log.serverId,
        serverName: log.serverName,
        state: status === "PERMISSIONS" ? "yellow" : "red",
        title: `${log.serverName} / ${log.collectorName}`,
        body: log.errorMessage || status,
        targetTab: "logs",
        time: log.collectionTime
      });
    }
  }

  return alerts.slice(0, 30);
}

function renderAlerts(alerts) {
  els.alertCount.textContent = alerts.length === 1 ? "1 active" : `${alerts.length} active`;
  els.alertList.innerHTML = "";

  if (!alerts.length) {
    els.alertList.innerHTML = `
      <div class="alert-empty">
        <strong>No alerts</strong>
      </div>
    `;
    return;
  }

  for (const alert of alerts) {
    const item = document.createElement("button");
    item.type = "button";
    item.className = `alert-item ${alert.state}`;
    item.addEventListener("click", () => navigateServer(alert.serverId, alert.targetTab || "stats"));
    item.innerHTML = `
      <span class="alert-severity">${escapeHtml(alert.state.toUpperCase())}</span>
      <strong>${escapeHtml(alert.title)}</strong>
      <span>${escapeHtml(alert.body)}</span>
      <small>${formatDate(alert.time) || "Needs attention"}</small>
    `;
    els.alertList.appendChild(item);
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

function applyRoute() {
  const hash = window.location.hash || "#/overview";
  const serverMatch = hash.match(/^#\/servers\/([^/]+)(?:\/([^/]+))?/);

  if (serverMatch) {
    state.selectedServerId = decodeURIComponent(serverMatch[1]);
    state.activeTab = serverMatch[2] || "stats";
    showServerView();
    loadSelectedServer();
    return;
  }

  state.activeTab = "overview";
  els.overviewView.classList.remove("hidden");
  els.serverView.classList.add("hidden");
  renderServerCards();
}

function navigateOverview() {
  window.location.hash = "#/overview";
}

function navigateServer(serverId, tab) {
  window.location.hash = `#/servers/${encodeURIComponent(serverId)}/${tab}`;
}

function showServerView() {
  els.overviewView.classList.add("hidden");
  els.serverView.classList.remove("hidden");

  document.querySelectorAll(".server-menu button").forEach(button => {
    button.classList.toggle("active", button.dataset.tab === state.activeTab);
  });

  document.querySelectorAll(".server-tab").forEach(tab => tab.classList.add("hidden"));
  const activePanel = document.getElementById(`tab-${state.activeTab}`);
  (activePanel || document.getElementById("tab-stats")).classList.remove("hidden");
}

async function loadSelectedServer() {
  const server = state.servers.find(s => s.serverId === state.selectedServerId);
  if (!server) {
    els.selectedTitle.textContent = "Server Detail";
    els.selectedSubtitle.textContent = "Select a server";
    return;
  }

  els.selectedTitle.textContent = server.displayName || server.serverId;
  els.selectedSubtitle.textContent = `${normalizePurpose(server.purpose)} / ${(server.healthState || "yellow").toUpperCase()} / ${server.healthReason || ""}`;
  renderServerStats(server);
  renderServerLog(server.serverId);

  if (state.activeTab === "stats" || state.activeTab === "cpu") {
    const [waits, cpu] = await Promise.all([
      fetchJson(`/api/servers/${encodeURIComponent(server.serverId)}/waits?hours=1&limit=12`),
      fetchJson(`/api/servers/${encodeURIComponent(server.serverId)}/cpu?hours=1`)
    ]);

    if (state.activeTab === "stats") {
      renderWaits(waits);
    }

    if (state.activeTab === "cpu") {
      drawCpuChart(cpu);
    }
  }
}

function renderServerStats(server) {
  els.serverStatsGrid.innerHTML = `
    <article class="stat-tile"><span>Status</span><strong>${escapeHtml((server.healthState || "yellow").toUpperCase())}</strong><small>${escapeHtml(server.healthReason || "")}</small></article>
    <article class="stat-tile"><span>SQL CPU</span><strong>${server.latestSqlCpuUtilization ?? "--"}${server.latestSqlCpuUtilization == null ? "" : "%"}</strong><small>Latest sample</small></article>
    <article class="stat-tile"><span>Top Wait</span><strong>${escapeHtml(compactWait(server.topWaitType))}</strong><small>Latest snapshot</small></article>
    <article class="stat-tile"><span>Alerts</span><strong>${server.activeAlertCount ?? 0}</strong><small>Last 15 minutes</small></article>
    <article class="stat-tile wide"><span>Edition</span><strong>${escapeHtml(server.edition || "Unknown")}</strong><small>${escapeHtml(server.productVersion || "No version collected")}</small></article>
    <article class="stat-tile wide"><span>Last Contact</span><strong>${formatDate(server.lastSeenTime) || "Never"}</strong><small>${escapeHtml(normalizePurpose(server.purpose))}</small></article>
  `;
}

function renderServerLog(serverId) {
  const rows = state.logs.filter(log => log.serverId === serverId).slice(0, 50);
  els.collectorLog.innerHTML = "";

  if (!rows.length) {
    els.collectorLog.innerHTML = `<div class="empty-state">No log entries.</div>`;
    return;
  }

  for (const log of rows) {
    const row = document.createElement("div");
    const statusClass = (log.status || "").toLowerCase();
    row.className = "log-row";
    row.title = log.errorMessage || "";
    row.innerHTML = `
      <span>${formatTime(log.collectionTime)}</span>
      <span>${escapeHtml(log.collectorName)}</span>
      <span class="status ${statusClass}">${escapeHtml(log.status)}</span>
      <span>${log.rowsCollected ?? 0}</span>
    `;
    els.collectorLog.appendChild(row);
  }
}

function renderWaits(waits) {
  els.waitList.innerHTML = "";
  if (!waits.length) {
    els.waitList.innerHTML = `<div class="empty-state">No waits.</div>`;
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
    ctx.fillText("No CPU samples", 12, 28);
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

function cssVar(name, fallback) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

function compactWait(waitType) {
  if (!waitType) return "--";
  return waitType.length > 8 ? `${waitType.slice(0, 8)}...` : waitType;
}

function compactSeen(value) {
  if (!value) return "--";
  const seconds = Math.max(0, (Date.now() - new Date(value).getTime()) / 1000);
  if (seconds < 60) return `${Math.floor(seconds)}s`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h`;
  return `${Math.floor(seconds / 86400)}d`;
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

loadAll().catch(error => {
  els.generatedAt.textContent = error.message;
});
