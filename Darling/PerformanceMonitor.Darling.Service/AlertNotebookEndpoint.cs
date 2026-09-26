/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// <c>GET /api/alert-notebook</c> (#4222 slice C): the server half of the alert -&gt; notebook MVP. Turns an
/// alert link's <c>server</c> + <c>metric</c> + <c>at</c> + <c>dedup</c> into a bound, read-only notebook
/// definition — the mechanical conversion of <see cref="DarlingTriageEndpoint.SectionsFor"/>'s per-metric
/// reads into <c>{type:"read"}</c> cells, plus the alert this link is ABOUT (dedup-matched, not just
/// nearest-in-time) and a four-arm status answering whether the condition is still live.
///
/// <para><b>Reuses <see cref="DarlingTriageEndpoint"/>, never copies it.</b> The anchor math
/// (<see cref="DarlingTriageEndpoint.ResolveAnchor"/>), the resolution-title aliasing
/// (<see cref="DarlingTriageEndpoint.ResolutionAliases"/>) and the per-metric read list
/// (<see cref="DarlingTriageEndpoint.SectionsFor"/> / <see cref="DarlingTriageEndpoint.DefaultSections"/>) are
/// the SAME members that page already exposed as <c>internal</c>; this endpoint widens nothing new beyond the
/// two lookback constants below, both already declared on that type.</para>
///
/// <para><b>Templates are mechanical for every metric in this slice.</b> The authored blocking/deadlock
/// templates (forensic multi-read layouts) are a LATER slice; every metric here gets <c>mechanical/&lt;metric&gt;</c>
/// v1 — a header cell, a status cell, then one read cell per <see cref="DarlingTriageEndpoint.SectionsFor"/>
/// entry (or the fallback), each carrying <c>server</c>, <c>as_of</c> and an explicit <c>hours</c>/<c>limit</c>.
/// No composed cells are emitted here, so no cell carries an absolute <c>range</c> yet — that is this slice's
/// honest gap, not an oversight (composed panels aren't part of <see cref="DarlingTriageEndpoint.TriageSection"/>
/// at all).</para>
///
/// <para><b>Degrade, never error (#2710).</b> A stale or empty firing — no matching history row, no sections
/// with data — still answers 200 with honest empty cells and a note, exactly like the triage page it sits
/// beside.</para>
/// </summary>
internal static partial class AlertNotebookEndpoint
{
    /// <summary>The template id/version pair every mechanical conversion carries. Bumped only if the SHAPE of
    /// the mechanical conversion below changes; a new metric added to <see cref="DarlingTriageEndpoint.SectionsByMetric"/>
    /// does not bump it, because the conversion rule — not the per-metric read list — is what "mechanical/…"
    /// versions.</summary>
    internal const int MechanicalTemplateVersion = 1;

    /// <summary>Maps <c>GET /api/alert-notebook</c>. Called once from <see cref="DarlingWebEndpoints.MapAll"/>,
    /// after the auth middleware like every sibling route.</summary>
    public static void Map(WebApplication app, NpgsqlDataSource postgres, DarlingAnalysisService analysis, ILogger logger)
    {
        app.MapGet("/api/alert-notebook", async (HttpContext context) =>
        {
            var serverQuery = Query(context, "server");
            var metric = Query(context, "metric");
            var dedup = Query(context, "dedup");
            var now = DateTime.UtcNow;
            var (anchor, asOf) = DarlingTriageEndpoint.ResolveAnchor(Query(context, "at"), now);

            var notes = new JsonArray();

            var fleetLevelStore = DarlingTriageEndpoint.IsFleetLevelStoreServer(serverQuery)
                || DarlingTriageEndpoint.IsFleetLevelStoreMetric(metric);

            int? serverId = null;
            string? serverName = serverQuery;
            if (!fleetLevelStore && !string.IsNullOrWhiteSpace(serverQuery))
            {
                var resolveStopwatch = Stopwatch.StartNew();
                try
                {
                    var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverQuery);
                    if (error is null)
                    {
                        serverId = resolved.ServerId;
                        serverName = resolved.ServerName;
                    }
                    else if (error.StartsWith(DarlingServerResolver.RegistryReadFaultPrefix, StringComparison.Ordinal))
                    {
                        DarlingWebFailureLog.Report(logger, "/api/alert-notebook:resolve-server", resolveStopwatch.ElapsedMilliseconds, error);
                        notes.Add((JsonNode)(DarlingWebFailureLog.IsStatementTimeoutSentence(error)
                            ? DarlingWebFailureLog.TimeoutMessage
                            : DarlingWebFailureLog.GenericMessage));
                    }
                    else
                    {
                        notes.Add((JsonNode)McpHelpers.ErrorMessageOf(error));
                    }
                }
                catch (Exception ex) when (ex is not OperationCanceledException)
                {
                    DarlingWebFailureLog.Report(logger, "/api/alert-notebook:resolve-server", resolveStopwatch.ElapsedMilliseconds, ex);
                    notes.Add((JsonNode)"Server resolution failed. The service log names what failed.");
                }
            }

            /* window_end = min(at + 15 min, now) (#4222's spec). The paired window_start = min(Incident
               Since, at) - family lookback only binds a composed cell's absolute range — #2735/#2788 — and
               this mechanical slice emits none (every cell here is a header, status, or read cell; read cells
               take server/as_of/hours, not a range). So window_start is not computed below; the anchor
               minus AlertMatchLookback already scopes the match query itself, same as the triage endpoint. */
            var windowEnd = anchor + DarlingTriageEndpoint.AnchorSlack;
            if (windowEnd > now)
            {
                windowEnd = now;
            }

            JsonNode? alertNode = null;
            AlertIncident? matchedIncident = null;
            DarlingAlertReader.AlertHistoryReadRow? matchedRow = null;
            var alertHistoryStopwatch = Stopwatch.StartNew();
            List<DarlingAlertReader.AlertHistoryReadRow> historyRows = new();
            try
            {
                historyRows = await DarlingAlertReader.GetAlertHistoryAsync(
                    postgres, anchor - DarlingTriageEndpoint.AlertMatchLookback, windowEnd, serverId, 200, context.RequestAborted);

                (matchedRow, matchedIncident) = MatchAlert(historyRows, metric, dedup, anchor);

                if (matchedRow is not null)
                {
                    alertNode = AlertRowNode(matchedRow, matchedIncident);
                }
                else
                {
                    notes.Add((JsonNode)(
                        "No matching alert-history row was found near this instant - the row may have aged " +
                        "past retention, or the link predates delivery logging. The cells below still cover " +
                        "the window."));
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/alert-notebook:alert-history", alertHistoryStopwatch.ElapsedMilliseconds, ex);
                notes.Add((JsonNode)"Alert-history lookup failed. The service log names what failed.");
            }

            /* F1 (#4366 review): the match window above ends at anchor+15m, so a resolution or re-fire that
               lands later than that is invisible to the status arms below unless they see a second, wider
               read. This one runs [anchor, min(anchor + AlertMatchLookback, now)] -- forward-looking, where
               the match read above is centered on `anchor` and mostly backward-looking -- bounded by the SAME
               row cap and cancellable the SAME way. */
            List<DarlingAlertReader.AlertHistoryReadRow> statusRows = new();
            var statusHistoryStopwatch = Stopwatch.StartNew();
            try
            {
                var statusUntil = anchor + DarlingTriageEndpoint.AlertMatchLookback;
                if (statusUntil > now)
                {
                    statusUntil = now;
                }

                statusRows = anchor < statusUntil
                    ? await DarlingAlertReader.GetAlertHistoryAsync(
                        postgres, anchor, statusUntil, serverId, 200, context.RequestAborted)
                    : new();
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                DarlingWebFailureLog.Report(logger, "/api/alert-notebook:status-history", statusHistoryStopwatch.ElapsedMilliseconds, ex);
                notes.Add((JsonNode)"Status lookup failed. The service log names what failed.");
            }

            List<DarlingAlertReader.AlertHistoryReadRow> statusHistoryRows;
            if (historyRows.Count == 0)
            {
                statusHistoryRows = statusRows;
            }
            else if (statusRows.Count == 0)
            {
                statusHistoryRows = historyRows;
            }
            else
            {
                statusHistoryRows = new List<DarlingAlertReader.AlertHistoryReadRow>(historyRows.Count + statusRows.Count);
                statusHistoryRows.AddRange(historyRows);
                statusHistoryRows.AddRange(statusRows);
            }

            var status = await ResolveStatusAsync(
                postgres, serverId, fleetLevelStore, metric, anchor, now, statusHistoryRows, matchedRow, logger, context.RequestAborted);

            var lookbackHours = FamilyLookbackHours(metric);
            var trimmedMetric = string.IsNullOrWhiteSpace(metric) ? null : metric.Trim();
            var resolvedAuthored = ResolveAuthored(trimmedMetric);

            JsonArray cells;
            string templateId;
            int templateVersion;

            if (resolvedAuthored is not null)
            {
                var (authored, kind) = resolvedAuthored.Value;
                var windowStart = windowEnd - AuthoredLookback(trimmedMetric!);
                var authoredContext = ShouldPrefetch(authored, kind)
                    ? await PrefetchAsync(kind, trimmedMetric!, serverId, anchor, postgres, analysis, context.RequestAborted, logger, notes)
                    : AuthoredContext.Empty;
                cells = authored.Invoke(
                    metric, serverName, asOf, windowStart, windowEnd, matchedIncident, matchedRow, status, authoredContext);
                templateId = authored.Id;
                templateVersion = authored.Version;
            }
            else
            {
                var sections = DarlingTriageEndpoint.SectionsFor(metric);
                cells = new JsonArray
                {
                    HeaderCell(metric, serverName, matchedIncident, matchedRow),
                    StatusCell(status),
                };

                foreach (var section in sections)
                {
                    cells.Add(ReadCell(section, serverName, asOf, lookbackHours));
                }

                templateId = "mechanical/" + (trimmedMetric ?? "default");
                templateVersion = MechanicalTemplateVersion;
            }

            var body = new JsonObject
            {
                ["alert"] = alertNode,
                ["status"] = status,
                ["notes"] = notes,
                ["template"] = new JsonObject
                {
                    ["id"] = templateId,
                    ["version"] = templateVersion,
                },
                ["definition"] = new JsonObject
                {
                    ["kind"] = "notebook",
                    ["cells"] = cells,
                },
            };

            return Results.Text(body.ToJsonString(), "application/json");
        });
    }

    /// <summary>The header cell every mechanical template opens with: a markdown-shaped read-only summary of
    /// the alert this notebook is bound to. Not a <c>read</c> cell — it carries nothing
    /// <see cref="DarlingWebEndpoints.BuildReadDispatch"/> would recognise — so the SPA render slice renders
    /// it directly rather than dispatching it.</summary>
    private static JsonObject HeaderCell(
        string? metric, string? serverName, AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row)
    {
        var cell = new JsonObject
        {
            ["type"] = "header",
            ["title"] = string.IsNullOrWhiteSpace(metric) ? "Alert" : metric,
            ["server"] = serverName,
        };

        if (row is not null)
        {
            cell["alert_time"] = row.AlertTime.ToString("o", CultureInfo.InvariantCulture);
        }

        if (incident is not null)
        {
            cell["incident_since"] = incident.IncidentStartedUtc?.ToString("o", CultureInfo.InvariantCulture);
            cell["involved_objects"] = string.Join(", ", incident.InvolvedObjects);
            cell["database"] = incident.Database;
            cell["total_occurrences"] = incident.TotalOccurrences;
        }

        return cell;
    }

    /// <summary>The status cell: same shape as <see cref="HeaderCell"/> — a directly-rendered cell, not a
    /// <c>read</c> cell.</summary>
    private static JsonObject StatusCell(string status) => new()
    {
        ["type"] = "status",
        ["title"] = "Status",
        ["status"] = status,
    };

    /// <summary>Converts one <see cref="DarlingTriageEndpoint.TriageSection"/> into a <c>{type:"read"}</c> cell
    /// — the mechanical conversion every metric gets in this slice. <c>hours</c> is the SectionsFor entry's own
    /// declared value where it has one, else the family lookback; <c>limit</c> is the entry's own value where
    /// declared, else a fixed default, because every read cell must carry an explicit limit (the #4222 budget
    /// pin).</summary>
    private static JsonObject ReadCell(
        DarlingTriageEndpoint.TriageSection section, string? serverName, string? asOf, string lookbackHours)
    {
        var parameters = new JsonObject();
        if (!section.FleetLevel && !string.IsNullOrWhiteSpace(serverName))
        {
            parameters["server"] = serverName;
        }

        if (!string.IsNullOrEmpty(asOf))
        {
            parameters["as_of"] = asOf;
        }

        var hasHours = false;
        var hasLimit = false;
        foreach (var (key, value) in section.Params)
        {
            parameters[key] = value;
            hasHours |= string.Equals(key, "hours", StringComparison.Ordinal);
            hasLimit |= string.Equals(key, "limit", StringComparison.Ordinal);
        }

        if (!hasHours)
        {
            parameters["hours"] = lookbackHours;
        }

        if (!hasLimit)
        {
            parameters["limit"] = DefaultMechanicalCellLimit;
        }

        return new JsonObject
        {
            ["type"] = "read",
            ["read"] = section.Read,
            ["params"] = parameters,
            ["viz"] = "table",
            ["title"] = section.Title,
        };
    }

    /// <summary>The fixed limit a mechanical read cell gets when its <see cref="DarlingTriageEndpoint.TriageSection"/>
    /// declared none — generous for a drill-down table, bounded against an unbounded read on a busy SQL
    /// Server.</summary>
    private const string DefaultMechanicalCellLimit = "50";

    /// <summary>The family lookback (hours, as the wire string a read cell's <c>hours</c> param takes) used
    /// when a section declares none of its own. One flat value across every metric in this slice — the
    /// per-metric tuning <see cref="DarlingTriageEndpoint.SectionsByMetric"/> already carries on most entries
    /// is respected first; this is only the mechanical conversion's own fallback.</summary>
    private static string FamilyLookbackHours(string? metric) => "24";

    /* ═══════════════════════════ authored templates (#4222 slice b) ═══════════════════════════ */

    /// <summary>The authored family lookback (spec §3): 24h for both Blocking and Deadlocks, unless a future
    /// authored family needs a different one — kept as a per-metric switch, not a shared constant, so that
    /// day comes without touching this one's callers.</summary>
    private static TimeSpan AuthoredLookback(string metric) => TimeSpan.FromHours(24);

    /// <summary>The pre-fetched, immutable data an authored template's context builder gets to read — never
    /// the store itself (#4223). A missing rule or finding (deleted since the firing) is an honest empty
    /// context, per #2710's degrade rule: the missing flag is set, never an exception.</summary>
    internal sealed record AuthoredContext(CustomAlertRule? CustomRule, bool CustomRuleMissing, AnalysisFinding? Finding, bool FindingMissing)
    {
        /// <summary>The context every no-context family gets — no rule, no finding, nothing missing.</summary>
        public static readonly AuthoredContext Empty = new(null, false, null, false);
    }

    /// <summary>Which pre-fetch (if any) a prefix-routed authored template needs before its context builder
    /// runs. <see cref="PrefetchAsync"/> switches on this; <c>None</c> costs zero store reads.</summary>
    internal enum AuthoredContextKind
    {
        None,
        CustomRule,
        AnalysisFinding,
    }

    /// <summary>The endpoint's pre-fetch gate (#4223): an entry pays for <see cref="PrefetchAsync"/> only
    /// when it registered a <see cref="AuthoredTemplateEntry.BuildCellsWithContext"/> builder AND the
    /// resolved <paramref name="kind"/> is not <see cref="AuthoredContextKind.None"/> — every plain family
    /// (the overwhelming majority) reads neither the custom-rule nor the finding store.</summary>
    internal static bool ShouldPrefetch(AuthoredTemplateEntry entry, AuthoredContextKind kind) =>
        entry.BuildCellsWithContext is not null && kind != AuthoredContextKind.None;

    /// <summary>One authored template's cell-building delegate plus its id/version — the server-side
    /// evolution of a mechanical conversion for a metric whose forensic shape (spec §3) is worth composing
    /// by hand instead of listing reads. Exactly one of <see cref="BuildCells"/> (the plain, no-context
    /// shape every existing family registration uses, unchanged) and <see cref="BuildCellsWithContext"/> (a
    /// #4223 family whose template needs the pre-fetched <see cref="AuthoredContext"/>) is set — enforced at
    /// construction, not by a caller checking for null. <see cref="Invoke"/> is the single call site both
    /// shapes go through.</summary>
    internal readonly record struct AuthoredTemplateEntry
    {
        public AuthoredTemplateEntry(
            string Id,
            int Version,
            Func<string?, string?, string?, DateTime, DateTime, AlertIncident?, DarlingAlertReader.AlertHistoryReadRow?, string, JsonArray>? BuildCells,
            Func<string?, string?, string?, DateTime, DateTime, AlertIncident?, DarlingAlertReader.AlertHistoryReadRow?, string, AuthoredContext, JsonArray>? BuildCellsWithContext = null)
        {
            if ((BuildCells is null) == (BuildCellsWithContext is null))
            {
                throw new ArgumentException(
                    $"authored template '{Id}' must set exactly one of BuildCells or BuildCellsWithContext.");
            }

            this.Id = Id;
            this.Version = Version;
            this.BuildCells = BuildCells;
            this.BuildCellsWithContext = BuildCellsWithContext;
        }

        public string Id { get; }

        public int Version { get; }

        public Func<string?, string?, string?, DateTime, DateTime, AlertIncident?, DarlingAlertReader.AlertHistoryReadRow?, string, JsonArray>? BuildCells { get; }

        public Func<string?, string?, string?, DateTime, DateTime, AlertIncident?, DarlingAlertReader.AlertHistoryReadRow?, string, AuthoredContext, JsonArray>? BuildCellsWithContext { get; }

        /// <summary>Calls whichever builder this entry set — <see cref="BuildCellsWithContext"/> with
        /// <paramref name="context"/> for a #4223 context family, or the plain <see cref="BuildCells"/> for
        /// every existing family (ignoring <paramref name="context"/>, which the caller passes
        /// <see cref="AuthoredContext.Empty"/> for by convention). The one call site every template goes
        /// through, in <see cref="Map"/> and in the shared test theories alike.</summary>
        public JsonArray Invoke(
            string? metric, string? serverName, string? asOf, DateTime windowStart, DateTime windowEnd,
            AlertIncident? incident, DarlingAlertReader.AlertHistoryReadRow? row, string status, AuthoredContext context) =>
            BuildCellsWithContext is not null
                ? BuildCellsWithContext(metric, serverName, asOf, windowStart, windowEnd, incident, row, status, context)
                : BuildCells!(metric, serverName, asOf, windowStart, windowEnd, incident, row, status);
    }

    /// <summary>The authored-template registration table, sorted case-insensitively by the metric name each
    /// row matches on. One row per family, holding every alert-engine <c>MetricName</c> string that routes to
    /// it. <see cref="AuthoredTemplate"/> is a lookup against this table — a new family is added here, not by
    /// growing an if-chain.</summary>
    internal static readonly (string[] Metrics, AuthoredTemplateEntry Entry)[] s_authoredTemplates =
    {
        (new[] { "Blocking Detected", "Blocking Wait Time" },
            new AuthoredTemplateEntry("authored/blocking", BlockingTemplateVersion, BuildBlockingCells)),
        (new[] { "Deadlocks Detected" },
            new AuthoredTemplateEntry("authored/deadlocks", DeadlocksTemplateVersion, BuildDeadlockCells)),
        (new[] { "Forced Plan Failing" },
            new AuthoredTemplateEntry("authored/forced-plan-failing", ForcedPlanFailingTemplateVersion, BuildForcedPlanFailingCells)),
        (new[] { "Long-Running Query" },
            new AuthoredTemplateEntry("authored/long-running-query", LongRunningQueryTemplateVersion, BuildLongRunningQueryCells)),
    };

    /// <summary>The authored template for a metric, or null when the metric falls back to the mechanical
    /// conversion — every metric NOT named in <see cref="s_authoredTemplates"/> keeps the byte-identical
    /// mechanical path. Keyed on the EXACT alert-engine <c>MetricName</c> strings (the same literals
    /// <see cref="DarlingTriageEndpoint.SectionsByMetric"/> keys on), case-insensitively, matching every other
    /// metric lookup on this endpoint. Made <c>internal</c> (not private) so
    /// <see cref="Darling.Tests.AlertNotebookAuthoredTemplateTests"/> can call it directly via
    /// <c>InternalsVisibleTo</c> instead of reflection — the same visibility <see cref="MatchAlert"/> and
    /// <see cref="StatusFromHistory"/> already use for their own pins.</summary>
    internal static AuthoredTemplateEntry? AuthoredTemplate(string? metric)
    {
        if (string.IsNullOrWhiteSpace(metric))
        {
            return null;
        }

        foreach (var (metrics, entry) in s_authoredTemplates)
        {
            foreach (var candidate in metrics)
            {
                if (string.Equals(metric, candidate, StringComparison.OrdinalIgnoreCase))
                {
                    return entry;
                }
            }
        }

        return null;
    }

    /// <summary>The prefix-routed authored templates (#4223): a metric that fails the exact-name lookup
    /// above falls through to here, EMPTY in this step — the families that register a <c>Custom:</c> or
    /// <c>Analysis: </c> row, and the pre-fetch each needs, follow in the next PRs. Ordered by nothing in
    /// particular yet; <see cref="ResolveAuthoredPrefixed(string, (string Prefix, AuthoredContextKind Kind, AuthoredTemplateEntry Entry)[])"/>
    /// picks the LONGEST matching prefix, so table order never matters.</summary>
    internal static readonly (string Prefix, AuthoredContextKind Kind, AuthoredTemplateEntry Entry)[] s_authoredPrefixTemplates =
        Array.Empty<(string Prefix, AuthoredContextKind Kind, AuthoredTemplateEntry Entry)>();

    /// <summary>Exact-then-prefix resolution, with the kind the caller needs to run the right pre-fetch
    /// (<see cref="PrefetchAsync"/>) before invoking the entry. Exact names win outright (unchanged
    /// behavior); failing that, the LONGEST registered prefix that matches wins, ordinal case-insensitive
    /// like every other metric lookup on this endpoint; no match falls through to the mechanical
    /// template.</summary>
    internal static (AuthoredTemplateEntry Entry, AuthoredContextKind Kind)? ResolveAuthored(string? metric)
    {
        if (string.IsNullOrWhiteSpace(metric))
        {
            return null;
        }

        var exact = AuthoredTemplate(metric);
        if (exact is not null)
        {
            return (exact.Value, AuthoredContextKind.None);
        }

        return ResolveAuthoredPrefixed(metric, s_authoredPrefixTemplates);
    }

    /// <summary>The prefix half of <see cref="ResolveAuthored"/>, taking the table as a parameter so the test
    /// theories can exercise exact-vs-prefix and longest-prefix-wins against a table the production one
    /// isn't (this step's table is empty).</summary>
    internal static (AuthoredTemplateEntry Entry, AuthoredContextKind Kind)? ResolveAuthoredPrefixed(
        string metric, (string Prefix, AuthoredContextKind Kind, AuthoredTemplateEntry Entry)[] prefixTable)
    {
        (string Prefix, AuthoredContextKind Kind, AuthoredTemplateEntry Entry)? best = null;

        foreach (var row in prefixTable)
        {
            if (metric.StartsWith(row.Prefix, StringComparison.OrdinalIgnoreCase)
                && (best is null || row.Prefix.Length > best.Value.Prefix.Length))
            {
                best = row;
            }
        }

        return best is null ? null : (best.Value.Entry, best.Value.Kind);
    }

    /// <summary>The async pre-fetch behind a #4223 context family (spec §4): read exactly the store the
    /// resolved <see cref="AuthoredContextKind"/> names, into an <see cref="AuthoredContext"/> the builder
    /// reads but never writes through. <c>None</c> touches no store at all — the caller only invokes this for
    /// an entry whose <see cref="AuthoredTemplateEntry.BuildCellsWithContext"/> is set, so a plain family
    /// never pays for a read it doesn't use. A deleted rule or finding degrades to the matching "missing"
    /// flag (#2710), never an exception; only cancellation propagates.</summary>
    internal static async Task<AuthoredContext> PrefetchAsync(
        AuthoredContextKind kind, string metric, int? serverId, DateTime anchor,
        NpgsqlDataSource postgres, DarlingAnalysisService analysis, CancellationToken ct)
    {
        return await PrefetchAsync(kind, metric, serverId, anchor, postgres, analysis, ct, logger: null, notes: null);
    }

    /// <summary>The full pre-fetch, with the optional logger/notes the endpoint's call site passes so a store
    /// failure (not cancellation) is reported the same way every sibling read on this endpoint reports one,
    /// AND surfaces as a note on the response instead of vanishing silently.</summary>
    internal static async Task<AuthoredContext> PrefetchAsync(
        AuthoredContextKind kind, string metric, int? serverId, DateTime anchor,
        NpgsqlDataSource postgres, DarlingAnalysisService analysis, CancellationToken ct,
        ILogger? logger, JsonArray? notes)
    {
        switch (kind)
        {
            case AuthoredContextKind.None:
                return AuthoredContext.Empty;

            case AuthoredContextKind.CustomRule:
                return await PrefetchCustomRuleAsync(metric, postgres, ct, logger, notes);

            case AuthoredContextKind.AnalysisFinding:
                return await PrefetchAnalysisFindingAsync(metric, serverId, anchor, analysis, ct, logger, notes);

            default:
                return AuthoredContext.Empty;
        }
    }

    /// <summary>Parses the <c>Custom:&lt;id&gt;</c> metric (the exact inverse of
    /// <see cref="CustomAlertEvaluator.MetricNameFor"/>) and reads the rule. A bad parse or a store
    /// <c>NotFound</c>/null-<c>Ok</c> row is an honest missing context, not an error.</summary>
    private static async Task<AuthoredContext> PrefetchCustomRuleAsync(
        string metric, NpgsqlDataSource postgres, CancellationToken ct, ILogger? logger, JsonArray? notes)
    {
        var idText = metric.Length > "Custom:".Length ? metric["Custom:".Length..] : string.Empty;
        if (!long.TryParse(idText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var ruleId))
        {
            return AuthoredContext.Empty with { CustomRuleMissing = true };
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            var result = await new CustomAlertRuleStore(postgres).GetAsync(ruleId, ct);
            return result switch
            {
                CustomAlertRuleResult.Ok(CustomAlertRule rule) => new AuthoredContext(rule, false, null, false),
                _ => AuthoredContext.Empty with { CustomRuleMissing = true },
            };
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (logger is not null)
            {
                DarlingWebFailureLog.Report(logger, "/api/alert-notebook:prefetch", stopwatch.ElapsedMilliseconds, ex);
            }

            notes?.Add((JsonNode)"The rule could not be read.");
            return AuthoredContext.Empty with { CustomRuleMissing = true };
        }
    }

    /// <summary>Parses the 8-character story-path-hash suffix off an <c>Analysis: {category} [{hash8}]</c>
    /// metric (<see cref="FindingMessageFormatter.MetricName"/>'s exact format) and matches it against the
    /// server's recent findings — the finding whose FULL <c>StoryPathHash</c> STARTS WITH all 8 characters,
    /// so two findings sharing a shorter prefix but differing at the 8th are never confused. None found (aged
    /// past retention, muted, or the parse itself failed) is an honest missing context.</summary>
    private static async Task<AuthoredContext> PrefetchAnalysisFindingAsync(
        string metric, int? serverId, DateTime anchor, DarlingAnalysisService analysis, CancellationToken ct,
        ILogger? logger, JsonArray? notes)
    {
        var openBracket = metric.LastIndexOf('[');
        var closeBracket = metric.LastIndexOf(']');
        var hash8 = openBracket >= 0 && closeBracket > openBracket
            ? metric[(openBracket + 1)..closeBracket]
            : string.Empty;

        if (hash8.Length != 8 || serverId is null)
        {
            return AuthoredContext.Empty with { FindingMissing = true };
        }

        var stopwatch = Stopwatch.StartNew();
        try
        {
            var findings = await analysis.GetRecentFindingsAsync(serverId.Value, hoursBack: 24, limit: 200, asOfUtc: anchor, ct);
            var match = PickFindingByHash(findings, hash8);

            return match is null
                ? AuthoredContext.Empty with { FindingMissing = true }
                : new AuthoredContext(null, false, match, false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            if (logger is not null)
            {
                DarlingWebFailureLog.Report(logger, "/api/alert-notebook:prefetch", stopwatch.ElapsedMilliseconds, ex);
            }

            notes?.Add((JsonNode)"The finding could not be read.");
            return AuthoredContext.Empty with { FindingMissing = true };
        }
    }

    /// <summary>The eight-character finding match (#4223): the first finding whose FULL <c>StoryPathHash</c>
    /// starts with all of <paramref name="hash8"/> (ordinal), skipping any finding with no hash at all — the
    /// same first-match rule <see cref="PrefetchAnalysisFindingAsync"/> always used, unchanged. Two findings
    /// sharing a shorter prefix but differing at the 8th character are never confused, because the compare is
    /// always all 8 characters.</summary>
    internal static AnalysisFinding? PickFindingByHash(IEnumerable<AnalysisFinding> findings, string hash8) =>
        findings.FirstOrDefault(f =>
            !string.IsNullOrEmpty(f.StoryPathHash) && f.StoryPathHash.StartsWith(hash8, StringComparison.Ordinal));

    /// <summary>The reads this endpoint's authored templates call that declare no <c>limit</c> param at all
    /// (<see cref="DarlingWebEndpoints.BuildReadDispatch"/>'s own catalog) — a chart/trend read whose budget
    /// is the bucket count, not a row cap (spec §3's own budget rule: "every read cell has an explicit limit,
    /// OR its trend goes through the chart bucket budget"). <see cref="AuthoredReadCell"/> must not force a
    /// 'limit' onto one of these, or <c>ValidateReadPanelSpec</c>'s undeclared-param check reds it.</summary>
    private static readonly IReadOnlySet<string> s_authoredLimitlessTrendReads = new HashSet<string>(StringComparer.Ordinal)
    {
        "get_deadlock_trend",
    };

    /// <summary>An authored template's read cell (spec §1 binding): <c>server</c>, <c>as_of = window_end</c>
    /// and every fixed param the call declares, PLUS an explicit <c>hours</c> when the call did not, PLUS an
    /// explicit <c>limit</c> when the call did not AND the read is not one of
    /// <see cref="s_authoredLimitlessTrendReads"/> (a bucketed trend read has no <c>limit</c> param to
    /// carry). <c>viz</c> is always "table" — these are drill-down reads, not charts.</summary>
    private static JsonObject AuthoredReadCell(
        string read, string title, string? serverName, string? asOf, params (string Key, string Value)[] fixedParams)
    {
        var parameters = new JsonObject();
        if (!string.IsNullOrWhiteSpace(serverName))
        {
            parameters["server"] = serverName;
        }

        if (!string.IsNullOrEmpty(asOf))
        {
            parameters["as_of"] = asOf;
        }

        var hasHours = false;
        var hasLimit = false;
        foreach (var (key, value) in fixedParams)
        {
            parameters[key] = value;
            hasHours |= string.Equals(key, "hours", StringComparison.Ordinal);
            hasLimit |= string.Equals(key, "limit", StringComparison.Ordinal);
        }

        if (!hasHours)
        {
            parameters["hours"] = "24";
        }

        if (!hasLimit && !s_authoredLimitlessTrendReads.Contains(read))
        {
            parameters["limit"] = DefaultMechanicalCellLimit;
        }

        return new JsonObject
        {
            ["type"] = "read",
            ["read"] = read,
            ["params"] = parameters,
            ["viz"] = "table",
            ["title"] = title,
        };
    }

    /// <summary>Ports the <c>blocking-rca</c> / <c>deadlock-postmortem</c> time-series panel spec
    /// (<c>notebook.js</c>'s <c>tsPanel</c>, <c>aggregate: "count"</c>, a deadlock/blocking annotation) into a
    /// server-side composed cell (spec §1 binding): an absolute <c>range</c> equal to the computed window, and
    /// — where the incident carries a database and the source has that dimension — a bound <c>database_name</c>
    /// filter.</summary>
    private static JsonObject TimelinePanel(
        string title, string source, string measure, DateTime windowStart, DateTime windowEnd,
        string annotation, string? databaseFilter)
    {
        var cell = new JsonObject
        {
            ["type"] = "panel",
            ["title"] = title,
            ["source"] = source,
            ["measure"] = measure,
            ["aggregate"] = "count",
            ["viz"] = "line",
            ["timeBucket"] = "hour",
            ["annotations"] = new JsonArray { annotation },
            ["range"] = new JsonObject
            {
                ["windowStart"] = windowStart.ToString("o", CultureInfo.InvariantCulture),
                ["windowEnd"] = windowEnd.ToString("o", CultureInfo.InvariantCulture),
            },
        };

        if (!string.IsNullOrWhiteSpace(databaseFilter))
        {
            cell["filters"] = new JsonArray
            {
                new JsonObject
                {
                    ["dimension"] = "database_name",
                    ["op"] = "eq",
                    ["value"] = databaseFilter,
                },
            };
        }

        return cell;
    }

    /// <summary>Ports a <c>rankedPanel</c> spec (<c>notebook.js</c>, <c>aggregate: "count"</c>, top 10 unless
    /// stated, <c>viz: "bar"</c>/<c>"pie"</c>) into a server-side composed cell — same binding rule as
    /// <see cref="TimelinePanel"/>. Lock modes keep the source template's <c>pie</c>/topN-8 shape; every other
    /// ranked breakdown here keeps the source template's <c>bar</c>/topN-10.</summary>
    private static JsonObject RankedPanel(
        string title, string source, string measure, DateTime windowStart, DateTime windowEnd,
        string groupBy, string? databaseFilter)
    {
        var isLockMode = string.Equals(groupBy, "lock_mode", StringComparison.Ordinal);
        var cell = new JsonObject
        {
            ["type"] = "panel",
            ["title"] = title,
            ["source"] = source,
            ["measure"] = measure,
            ["aggregate"] = "count",
            ["viz"] = isLockMode ? "pie" : "bar",
            ["topN"] = isLockMode ? 8 : 10,
            ["groupBy"] = new JsonArray { groupBy },
            ["range"] = new JsonObject
            {
                ["windowStart"] = windowStart.ToString("o", CultureInfo.InvariantCulture),
                ["windowEnd"] = windowEnd.ToString("o", CultureInfo.InvariantCulture),
            },
        };

        /* Only bind the database filter when the breakdown ISN'T grouping by database itself -- "Blocking by
           database" must show every database, not just the incident's one. */
        if (!string.IsNullOrWhiteSpace(databaseFilter) && !string.Equals(groupBy, "database_name", StringComparison.Ordinal))
        {
            cell["filters"] = new JsonArray
            {
                new JsonObject
                {
                    ["dimension"] = "database_name",
                    ["op"] = "eq",
                    ["value"] = databaseFilter,
                },
            };
        }

        return cell;
    }

    /// <summary>The collector-freshness read (arms 3/4): the newest row for this server that counts as
    /// evidence the instrument is up. Only <c>SUCCESS</c> counts (#4378): <c>SKIPPED</c> can be written
    /// WITHOUT the run ever contacting the target — <c>Lite/Services/RemoteCollectorService.cs</c>'s
    /// user-cancelled-MFA catch writes <c>SKIPPED</c> before any query reaches the server — and Darling's
    /// own per-run classifier, <c>EnumeratedCollectorDriver.ClassifyReturnedRun</c>, never returns
    /// <c>SKIPPED</c> at all, so it proves nothing about server reachability here. An <c>ERROR</c> row must
    /// NOT count either: it is proof the collector attempted and failed, which is exactly the case this arm
    /// exists to tell apart from a collector that never ran at all.</summary>
    internal const string CollectorFreshnessSql =
        "SELECT collection_time FROM v_collection_log WHERE server_id = $1 AND status = 'SUCCESS' " +
        "ORDER BY collection_time DESC LIMIT 1";

    /// <summary>
    /// The four status arms (#4222): a resolution row for this metric after <c>at</c> -&gt; "Resolved at T";
    /// else a later same-metric firing -&gt; "Fired again at T"; else, if the metric's collector has run since
    /// <c>at</c> -&gt; "No resolution recorded"; else "Unknown (not collected since T)". NEVER "ongoing" — an
    /// absence of rows is not evidence while the instrument is down.
    /// </summary>
    internal static async Task<string> ResolveStatusAsync(
        NpgsqlDataSource postgres, int? serverId, bool fleetLevelStore, string? metric, DateTime anchor, DateTime now,
        List<DarlingAlertReader.AlertHistoryReadRow> historyRows,
        DarlingAlertReader.AlertHistoryReadRow? matchedRow, ILogger logger, System.Threading.CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(metric))
        {
            return "Unknown (not collected since " + anchor.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) + ")";
        }

        var arm12 = StatusFromHistory(historyRows, metric, anchor, matchedRow, serverId, fleetLevelStore);
        if (arm12 is not null)
        {
            return arm12;
        }

        /* Arms 3/4: collector freshness since `at`. get_collection_log's own store (v_collection_log) is read
           directly here — a fleet-level store metric has no server, so a null serverId reads "not collected"
           honestly rather than faking a server scope. */
        var anchorStamp = anchor.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        if (serverId is null)
        {
            return "Unknown (not collected since " + anchorStamp + ")";
        }

        var freshnessStopwatch = Stopwatch.StartNew();
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken);
            await using var command = new NpgsqlCommand(CollectorFreshnessSql, connection)
            {
                CommandTimeout = McpCommandDeadlines.ReadSeconds,
            };
            command.Parameters.AddWithValue(serverId.Value);
            var result = await command.ExecuteScalarAsync(cancellationToken);
            if (result is DateTime lastCollected && lastCollected >= anchor)
            {
                return "No resolution recorded";
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            DarlingWebFailureLog.Report(logger, "/api/alert-notebook:collector-freshness", freshnessStopwatch.ElapsedMilliseconds, ex);
        }

        return "Unknown (not collected since " + anchorStamp + ")";
    }

    /// <summary>The endpoint's dedup-first / nearest-in-time alert match (#4222), extracted as a pure seam
    /// (#4366 review F4) so <see cref="AlertNotebookEndpointTests"/> can pin it directly against real
    /// <see cref="DarlingAlertReader.AlertHistoryReadRow"/> values instead of copying the rule into the test.
    ///
    /// <para><b>F3 fix.</b> Rows arrive newest-first, and a dedup key identifies the INCIDENT, not a single
    /// row — so a same-key re-fire inside the match window used to win by being first in iteration order
    /// (effectively "newest", since the caller's rows are DESC). That let a re-fire masquerade as the
    /// original firing and hide itself from the status arms' "later same-metric firing" check. This picks the
    /// NEAREST dedup match to <paramref name="anchor"/> instead, ties broken toward the earlier
    /// (closer-to-firing) row, matching the nearest-in-time fallback's own tie behaviour below.</para></summary>
    internal static (DarlingAlertReader.AlertHistoryReadRow? Row, AlertIncident? Incident) MatchAlert(
        IReadOnlyList<DarlingAlertReader.AlertHistoryReadRow> rows, string? metric, string? dedup, DateTime anchor)
    {
        var sameMetric = new List<DarlingAlertReader.AlertHistoryReadRow>();
        foreach (var row in rows)
        {
            if (string.IsNullOrWhiteSpace(metric)
                || string.Equals(row.MetricName, metric.Trim(), StringComparison.OrdinalIgnoreCase))
            {
                sameMetric.Add(row);
            }
        }

        DarlingAlertReader.AlertHistoryReadRow? matchedRow = null;
        AlertIncident? matchedIncident = null;

        if (!string.IsNullOrWhiteSpace(dedup))
        {
            var key = dedup.Trim();
            var best = long.MaxValue;
            foreach (var row in sameMetric)
            {
                if (!AlertContextSerializer.TryDeserialize(row.ContextJson, out var ctx) || ctx.Incidents is not { Count: > 0 })
                {
                    continue;
                }

                foreach (var incident in ctx.Incidents)
                {
                    if (!string.Equals(incident.DedupKey, key, StringComparison.Ordinal))
                    {
                        continue;
                    }

                    var distance = Math.Abs((row.AlertTime - anchor).Ticks);
                    if (distance < best || (distance == best && matchedRow is not null && row.AlertTime < matchedRow.AlertTime))
                    {
                        best = distance;
                        matchedRow = row;
                        matchedIncident = incident;
                    }

                    break;
                }
            }
        }

        if (matchedRow is null && sameMetric.Count > 0)
        {
            sameMetric.Sort((a, b) =>
            {
                var cmp = Math.Abs((a.AlertTime - anchor).Ticks).CompareTo(Math.Abs((b.AlertTime - anchor).Ticks));
                return cmp != 0 ? cmp : b.AlertTime.CompareTo(a.AlertTime);
            });
            matchedRow = sameMetric[0];
            if (AlertContextSerializer.TryDeserialize(matchedRow.ContextJson, out var ctx) && ctx.Incidents is { Count: > 0 })
            {
                matchedIncident = ctx.Incidents[0];
            }
        }

        return (matchedRow, matchedIncident);
    }

    /// <summary>The AG and self-monitor connect family's own recovery edges (#4378): each pair already fires
    /// under its OWN metric name (<see cref="DarlingTriageEndpoint.SectionsByMetric"/> maps every one of
    /// these directly, so they need no <see cref="DarlingTriageEndpoint.ResolutionAliases"/> entry — that
    /// list folds a resolution TITLE onto a different firing metric, which none of these do). Kept here
    /// rather than in <c>ResolutionAliases</c> because that list also builds
    /// <see cref="DarlingTriageEndpoint.SectionsByMetric"/>, and every metric below already has its own
    /// entry there; adding these pairs to it would define the SAME key twice. <c>AG Failover</c> has no
    /// natural recovery — a failover is an event, not a condition that clears — so it is deliberately absent.</summary>
    internal static readonly IReadOnlyList<(string Firing, string Recovery)> NotebookRecoveryEdges = new[]
    {
        ("AG Replica Disconnected", "AG Replica Reconnected"),
        ("AG Database Suspended", "AG Data Movement Resumed"),
        ("AG Sync Fell Behind", "AG Sync Recovered"),
        ("Server Unreachable", "Server Restored"),
    };

    /// <summary>Status arms 1 and 2 (#4222), extracted as a pure seam (#4366 review F4) over whatever rows the
    /// caller has already gathered — the caller is responsible for the window (#4366 F1: the caller now
    /// passes rows from BOTH the backward match read and a forward-looking read so a late resolution or
    /// re-fire is visible here). Returns null when neither arm fires, meaning the caller should fall through
    /// to the collector-freshness arms.
    ///
    /// <para><b>F2 fix.</b> When the caller has no resolved server id and the alert is not a fleet-level
    /// metric, this returns Unknown immediately rather than scanning fleet-wide rows — an unresolved
    /// <c>server=</c> query used to let another host's resolution or re-fire answer for a server the caller
    /// never actually reached. When a server id IS known, rows are filtered to that server before either arm
    /// runs, for the same reason.</para></summary>
    internal static string? StatusFromHistory(
        IReadOnlyList<DarlingAlertReader.AlertHistoryReadRow> rows, string metric, DateTime anchor,
        DarlingAlertReader.AlertHistoryReadRow? matchedRow, int? serverId, bool fleetLevelStore)
    {
        if (serverId is null && !fleetLevelStore)
        {
            return "Unknown (not collected since " + anchor.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture) + ")";
        }

        var trimmedMetric = metric.Trim();
        var scoped = serverId.HasValue
            ? rows.Where(row => row.ServerId == serverId.Value)
            : rows;

        /* Arm 1: a resolution row for THIS metric after `at`. ResolutionAliases maps a resolution TITLE to
           its firing metric; a resolution row's own metric_name is the alias, not the canonical name, so the
           search is over every alias that folds onto this metric plus the metric's own resolved-title
           siblings the alert engine may write directly. */
        DateTime? resolvedAt = null;
        foreach (var row in scoped)
        {
            if (row.AlertTime <= anchor)
            {
                continue;
            }

            var isResolutionRow = false;
            foreach (var (alias, canonical) in DarlingTriageEndpoint.ResolutionAliases)
            {
                if (string.Equals(canonical, trimmedMetric, StringComparison.OrdinalIgnoreCase)
                    && string.Equals(row.MetricName, alias, StringComparison.OrdinalIgnoreCase))
                {
                    isResolutionRow = true;
                    break;
                }
            }

            if (!isResolutionRow)
            {
                foreach (var (firing, recovery) in NotebookRecoveryEdges)
                {
                    if (string.Equals(firing, trimmedMetric, StringComparison.OrdinalIgnoreCase)
                        && string.Equals(row.MetricName, recovery, StringComparison.OrdinalIgnoreCase))
                    {
                        isResolutionRow = true;
                        break;
                    }
                }
            }

            if (isResolutionRow && (resolvedAt is null || row.AlertTime < resolvedAt))
            {
                resolvedAt = row.AlertTime;
            }
        }

        if (resolvedAt is DateTime resolved)
        {
            return "Resolved at " + resolved.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        }

        /* Arm 2: a later same-metric firing. */
        DateTime? refiredAt = null;
        foreach (var row in scoped)
        {
            if (row.AlertTime <= anchor)
            {
                continue;
            }

            if (string.Equals(row.MetricName, trimmedMetric, StringComparison.OrdinalIgnoreCase)
                && (matchedRow is null || row.AlertTime != matchedRow.AlertTime))
            {
                if (refiredAt is null || row.AlertTime < refiredAt)
                {
                    refiredAt = row.AlertTime;
                }
            }
        }

        if (refiredAt is DateTime refired)
        {
            return "Fired again at " + refired.ToString("yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture);
        }

        return null;
    }

    /// <summary>One alert-history row plus its dedup-matched incident's facts, in the SAME wire shape
    /// <see cref="DarlingTriageEndpoint"/>'s own row node uses, extended with the incident facts the spec
    /// requires (Incident Since, Involved Objects, Database, Total Occurrences).</summary>
    private static JsonObject AlertRowNode(DarlingAlertReader.AlertHistoryReadRow row, AlertIncident? incident)
    {
        var node = new JsonObject
        {
            ["alert_time"] = row.AlertTime.ToString("o", CultureInfo.InvariantCulture),
            ["server_id"] = row.ServerId,
            ["server_name"] = row.ServerName,
            ["metric_name"] = row.MetricName,
            ["current_value"] = row.CurrentValue,
            ["threshold_value"] = row.ThresholdValue,
            ["detail_text"] = row.DetailText,
        };

        if (incident is not null)
        {
            node["incident_since"] = incident.IncidentStartedUtc?.ToString("o", CultureInfo.InvariantCulture);
            node["involved_objects"] = string.Join(", ", incident.InvolvedObjects);
            node["database"] = incident.Database;
            node["total_occurrences"] = incident.TotalOccurrences;
        }

        return node;
    }

    private static string? Query(HttpContext context, string key)
    {
        var value = context.Request.Query[key].ToString();
        return string.IsNullOrEmpty(value) ? null : value;
    }
}
