# Alerts & Notifications

Part of the [Performance Monitor documentation](../README.md#documentation).

Every edition includes a real-time alert engine that monitors for performance issues and sends notifications via system tray balloons (Lite/Dashboard), email, and webhooks.

### Alert Types

| Metric | Default Threshold | Description |
|---|---|---|
| **Blocking** | 5 seconds | Fires when the longest blocked session exceeds the threshold |
| **Deadlocks** | 1 | Fires when the rolling one-hour deadlock count reaches the threshold. Graded: WARNING by default, CRITICAL when the hour's rate reaches the deadlock health band's critical tier (20/hr, the measured bar the fleet card uses) — one deadlock and a storm no longer wear the same colour |
| **Poison waits** | 600 s accumulated in 10 min (WARNING); 6,000 s (CRITICAL) | Fires when a poison wait type accumulates enough wait inside a rolling ten-minute window to average **one task continuously stuck** for the whole window (WARNING), or ten (CRITICAL) — the same shape and the same bars as the PostgreSQL edition's Poison Wait, judged per wait type. Accumulation, not a per-wait average: one slow wait does not page, and a storm of thousands of short waits does not sleep. Fleet-calibrated — the worst ten-minute bucket measured across a 43-server production fleet over four days sat ~100× under the WARNING bar. Clears once the window's accumulated wait falls back under the bar (up to ten minutes after the last of it), and only on an observed window: a collector that stops delivering holds the alert open rather than announcing a recovery. The older `Poison wait threshold (ms)` setting remains in Settings for compatibility but is no longer consulted |
| **Runnable queue** | 8 runnable tasks per scheduler (WARNING); 16 (CRITICAL) | Fires when the average runnable-task count per visible online scheduler reaches the bar. The average covers the collections in the last 5 minutes when there are at least 3, otherwise the newest 3 if all fall in the last 10 minutes (or in 4 times the gap between cpu_scheduler_stats collections when that is longer, so the Low-Impact preset's 5-minute collection still evaluates) (anything else is not an observation, so nothing fires and nothing clears). A standing alert resolves, with that reason stated, when 30 minutes pass without an evaluated reading. The 30 minutes count from the last evaluated reading, and a read that is not an observation never restarts the count. A Warning that worsens to Critical is delivered at once, without waiting out the cooldown. A queue of runnable tasks means work is waiting for CPU, which can time out clients before High CPU fires. Both bars are settings (Warning at least 1, Critical never below Warning). The alert carries the CPU for the same minutes and the heaviest requests in the newest query snapshot. |
| **Client timeouts** | 10 attentions per minute for WARNING, 60 for CRITICAL. Off by default | Fires when clients cancel queries or time out at or above the bar. An attention is the event that SQL Server records when a client cancels a query or its timeout expires. The rate is the number of attentions that the attention collector counted, divided by the minutes that its last runs cover (about 3 minutes). A Warning that worsens to Critical is delivered at once, without waiting out the cooldown. The alert is off by default because no measured normal rate exists yet, and issue #5736 sets the bars from field data. Both bars are settings (Warning at least 1, Critical never below Warning). A mute rule that names a client application lowers the count in proportion to the share of the sampled events that the rule covers. |
| **Long-running queries** | 5 minutes | Fires when any query exceeds the elapsed-time threshold. Sessions matching the **opt-out knob** are not evaluated at all — not counted, not fingerprinted; the opposite of a mute — and the exclusion is applied inside the read, ahead of the result cap, so a permanent background request can never hide a real one. Two lists, both editable and both seeded from a 7-day read of one large production store, whose long-running population fell into four classes: (1) SQL Agent job steps (`program_name` starting `SQLAgent - TSQL JobStep`, ~460 sessions a week across 11 jobs, medians 35–62 min) — excluded by **program-name prefix**, the seeded default; (2) the `NT AUTHORITY\SYSTEM` and `NT AUTHORITY\NETWORK SERVICE` logins (the permanent multi-day CDC-shaped background) — excluded by **exact login**, the seeded default; (3) the application's admin login — deliberately **not** excluded, because it runs the job wave but also real ad-hoc long-runners, and the job-step prefix already covers its share; (4) named humans — never excluded, they are what the page is for. Matching is case-insensitive with no wildcard grammar. The seeds are defaults: clear a list to disable it (an empty list excludes nothing on that arm). The fired card states how many sessions each list removed |
| **TempDB space** | 80%, held for 3 samples | Fires (WARNING; there is no CRITICAL tier, because no measured "nearly full" bar exists to grade one on) when TempDB reserved space is at or above the percentage threshold on **3 consecutive collected samples** — about three minutes at the one-minute `tempdb_stats` cadence — and resolves on the first collected sample below it. The count is per collected SAMPLE, not per alert sweep, so a sweep re-reading a row it has already seen does not advance it; a tempdb reading that stops arriving holds the count where it is rather than announcing a recovery. Measured on a 43-server production fleet over 14 days: one server's tempdb crossed the bar 22 times, every excursion 1–4 samples long, so a single sample above the threshold is detected but is not delivered as an incident. Measured against tempdb's **growth ceiling** (`SUM(max_size)` over the ROWS files) where there is one, and against the current allocation where the files grow without limit — so the percentage means "distance to the point where tempdb cannot grow further" on every engine |
| **Long-running agent jobs** | 3× average | Fires when a job's current duration exceeds a multiple of its historical average |
| **High CPU** | 80%, held for 3 samples | Fires when total CPU (SQL + other) is at or above the threshold on **3 consecutive collected samples** — about three minutes at the one-minute CPU sample cadence — and resolves after 2 consecutive samples below it. A momentary spike above a steady baseline is detected but is not an incident, so it is not delivered. The count is per SAMPLE, not per alert sweep, so a sweep re-reading a sample it has already seen does not advance it. A CPU reading that stops arriving holds the count where it is rather than announcing a recovery. Graded WARNING at the threshold and CRITICAL at 95% — the same bar the health band colours the server card red at |
| **Volume free space** | 10% or 5 GB free | Fires when a monitored volume's free space drops below the percentage or absolute threshold (either check can be disabled). Never fires on Azure SQL Database. |
| **Failed agent job** | 60-minute lookback | Fires when a SQL Agent job run fails within the lookback window. Skipped on Azure SQL Database. |
| **Server unreachable** | N/A | Fires when a monitored server goes offline or comes back online |
| **Collection stopped** | No run in 30 min | Fires when collection stalls (Agent/service stopped or collectors erroring). App-computed, so it survives the collector being off; clears with a "Collection Resumed" notice. Never fires on Azure SQL Database; degrades gracefully where msdb is restricted (e.g. AWS RDS). |

All thresholds are configurable in Settings.

**Poison wait types** monitored: [`THREADPOOL`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql#threadpool) (worker thread exhaustion), [`RESOURCE_SEMAPHORE`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql#resource_semaphore) (memory grant pressure), and [`RESOURCE_SEMAPHORE_QUERY_COMPILE`](https://learn.microsoft.com/en-us/sql/relational-databases/system-dynamic-management-views/sys-dm-os-wait-stats-transact-sql#resource_semaphore_query_compile) (compilation memory pressure). These waits indicate severe resource starvation and should never occur under normal operation — which is what makes an accumulation bar meaningful across all three: any sustained pile-up is abnormal, and the alert asks how much piled up in the window rather than how long any one wait lasted.

### Notification Channels

- **System tray** — balloon notifications with a configurable per-metric cooldown (default: 5 minutes)
- **Email (SMTP)** — styled HTML emails with a configurable per-metric cooldown (default: 15 minutes), plus configurable SMTP settings (server, port, SSL, authentication, recipients)
- **Microsoft Teams** — styled card messages with color-coded severity indicators sent to Teams channels via incoming webhooks
- **Slack** — styled messages with color-coded severity indicators sent to Slack channels via incoming webhooks
- **PagerDuty** — native Events API v2 integration that triggers PagerDuty incidents with proper severity mapping (critical/error/warning/info) and automatic incident correlation via dedup_key. Supports both US and EU data centers
- **Generic webhook** — HTTP POST to any configurable endpoint for integration with other alerting systems (Opsgenie, n8n, custom automation, etc.)

All cooldown periods are independently configurable in Settings under the Performance Alerts section.

#### PagerDuty Setup

To receive alerts in PagerDuty:

1. In PagerDuty, create a service and add an **Events API v2** integration
2. Copy the 32-character **Integration Key** (routing key)
3. In Performance Monitor Settings, enable PagerDuty notifications and paste the routing key
4. Check **Use EU data center** if your PagerDuty account is in the EU region
5. Click **Send Test Notification** to verify the integration

PagerDuty alerts include the metric name, server, current value, threshold, and severity. Repeated alerts for the same incident automatically correlate into a single PagerDuty incident via the dedup_key, preventing alert fatigue.

The setting **Auto-resolve the incident when the alert's condition clears** is off by default. When it is on, a recovery sends a resolve event, which closes the incident that the outage alert opened. A recovery is a server coming back or an Availability Group replica reconnecting. When it is off, the recovery is sent as an info-level alert and the incident stays open. Both Lite and the Darling viewer have this setting.

### Notification Routes (Lite)

A notification route sends some alerts to their own destinations. By default every alert goes to the app-wide Teams, Slack, generic webhook, PagerDuty and email settings. A route lets one server, or one group of servers, use different ones.

To add a route, open **Settings** and select **Manage Notification Routes**. Each route has these parts:

- **Matches.** An exact metric name, a family such as `performance`, or `*` for every metric. The `*` match is offered only when the route has a server or tag scope.
- **Applies to.** All servers, one server, or one tag. A tag covers every server assigned to it and to any of its sub-tags.
- **Destinations.** A Teams URL, a Slack URL, a generic webhook URL, a PagerDuty routing key, and email recipients.

A route replaces the app-wide setting for each channel that it fills in. A channel that the route leaves empty goes to the next matching route that fills it. If no other route fills that channel, it goes to the app-wide setting. An alert never goes to both the route destination and the app-wide destination for the same channel.

When several routes match an alert, Lite picks one in this order:

1. A server route, then a tag route, then an all-servers route.
2. Within one of those, an exact metric, then a family, then `*`.
3. Among tag routes, the tag that is deeper in the tag tree. If the depth is the same, the route with the lowest number.

Each channel is chosen on its own. A route with no Teams URL does not block a less specific route that has one.

Lite stores the URLs and the PagerDuty key in Windows Credential Manager, not in the database. The routes window shows only whether each one is set. The edit dialog never shows a saved value. Leave the dots in a box to keep the value, type to replace it, or clear the box to remove it. Sometimes Windows cannot read a saved value. Then the Status column shows **Saved, unreadable**, and that channel stays off for the route until you enter a new value.

The generic webhook headers, the generic body template, the proxy addresses and the PagerDuty region stay app-wide. A route cannot change them.

If you remove a server or delete a tag, a route that names it stays in the list. Its status is **Orphaned: server removed** or **Orphaned: tag deleted**. An orphaned route matches nothing. Edit it to pick a new scope, or delete it.

While a saved-settings restore is pending, Lite refuses changes to routes and says why.

### Email Alerts

Alert emails include:

- **Metric summary** — what triggered the alert, current value vs threshold
- **Detail section** — recent blocking chains or deadlock participants with query text, wait times, lock modes, database names, and client application
- **XML attachment** — full `blocked_process_report.xml` or `deadlock_graph.xml` for offline analysis

### Alert Behavior

- **Resolved notifications** — when a condition clears (e.g., blocking ends), a "Cleared" notification fires
- **Lasting problems alert once** — a problem that lasts, such as an unreachable server, an Availability Group secondary that fell behind, or (Darling) Collection Stopped, sends one alert when it starts and one when it ends. It sends another only if the setting **Re-alert while a problem lasts every** (`connection_refire_minutes`) is above 0 minutes. The default is 0, which means never.
- **Server silencing** — right-click a server to acknowledge alerts, silence all alerts, or unsilence
- **Always-on** — the alert engine runs independently of which tab is active, including when minimized to the system tray
- **Alert history** — Lite logs alerts to DuckDB (`config_alert_log`); Darling logs to its Postgres store; both are accessible via MCP
- **Alert muting** — create rules to suppress specific recurring alerts while still logging them. Rules match on server name, metric type, database, query text, wait type, or job name (AND logic across fields). Access via Settings → Manage Mute Rules, or right-click an alert in the Alert History tab. The context menu offers **Mute This Alert** (pre-fills server + metric) and **Mute Similar Alerts** (pre-fills metric only, matching across all servers). Muted alerts appear grayed out and are still recorded for auditability. Rules support optional expiration (1h, 24h, 7 days, or permanent).
- **Collection watch (Darling):** at start, the service raises a "Collection Gap At Start" warning when the newest earlier collection is 15 minutes or more older than the start.
- **History audit (Darling):** the service runs this audit once a day, after 03:00 UTC. It compares each hour of the previous UTC day with the median of the 7 days before. It raises one "Collection Gaps In History" warning that names the hours under half of usual.
- **Heartbeat (Darling):** an optional `heartbeat` block in `darling.json` is off by default. It sends an HTTP GET to a URL you give it, every 60 to 3600 seconds, while collection is healthy. An outside check can then alert when the service stops or stops collecting.
- **Alert details** — right-click any alert in the Alert History tab and choose **View Details** for core fields (time, server, metric, value, threshold, notification type, status) plus context-sensitive details that vary by metric.
- **A slow store no longer blinds a check** (#3848, Darling) — the alert pass reads its evidence out of the Postgres store under a deliberately short 10-second per-read deadline, because a long read holds a fleet-sweep permit and delays collection for every server queued behind it. On a busy store that deadline is crossed by the store's own write bands — compression, aggregate materialization, checkpoint fsync tails — and every such read used to skip its condition for one 30-second cycle. Each read is now retried **once, two seconds later**, on a command-timeout and nothing else: never on an error the store returned (which would answer the same way twice), and never during shutdown. `get_collection_health`'s `alert_read_health` block reports `retried_reads` beside `server_read_failures`, so the write bands' cost stays a visible count instead of a blind alert — retries rising while failures stay at zero is a store under write pressure whose alerting is intact; both rising means a second attempt twelve seconds later still found the band on, which points at the store's write schedule rather than the reader.
- **Confidence chooses the channel** (#3712) — a scheduled-analysis finding earns a delivery by *corroboration*, never by severity alone: two or more facts in its story chain, a matched co-fire check on its root fact, or one of the two by-construction stories. A lone uncorroborated fact at or above the notify floor is recorded (Alert History status **Digest**, with the reason on the row and on `get_alert_history` as `routing_reason`) and shown in Recommendations with a *Not paged* marker, but it is not e-mailed, posted or toasted. It pages the moment it gains corroboration, as a new firing. Settings → Alerts → *Only notify on corroborated findings* (default on; `analysis_uncorroborated_route` in the settings file) restores delivery of every notify-worthy finding when unchecked. Darling delivers the same singles once a day as the Analysis Singles Digest.
