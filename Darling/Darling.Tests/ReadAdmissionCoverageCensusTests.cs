/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using PerformanceMonitor.Darling.Storage;

namespace Darling.Tests;

/// <summary>
/// #5506 part 3 (D8): every read that opens a <see cref="ReadScope"/> goes through read admission, or is a health read on
/// the skip roster with its reason. Source-text census over the service: a new read site that opens a scope without
/// admitting fails here, and so does a change to the MCP filter order or the filter's one skipped tool.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class ReadAdmissionCoverageCensusTests
{
    private static readonly string ServiceDir = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service");

    /// <summary>The files that open a read scope today, and how many each opens. A new site is added here only with its
    /// admission call beside it.</summary>
    private static readonly Dictionary<string, int> ScopeSites = new(StringComparer.Ordinal)
    {
        ["DarlingWebEndpoints.cs"] = 10, // /api/active-queries/live/{commandId} (#6386), /api/investigation-export (#6236), /api/query-tag-rules/test (#5986), the /api/read/{name} loop, the composed panel (its run and its cache), /api/fleet, /api/alerts/test, /api/ag and /api/catalog (round 2, L5)
        ["DarlingTriageEndpoint.cs"] = 1, // /api/triage (the dedicated routes record their own samples, #5506 part 3)
        ["AlertNotebookEndpoint.cs"] = 1, // /api/alert-notebook (round 1, M3)
        ["DarlingFleetSweepEndpoints.cs"] = 4, // /api/sweeps, /latest, /{id} and /watch-items (round 1, M3)
        ["DarlingServerDatabasesEndpoint.cs"] = 1, // the database picker (round 1, M3)
        ["McpToolLatencyFilter.cs"] = 1, // every MCP tool call but run_custom_view_panel
    };

    [Fact]
    public void EveryReadScopeSite_IsFollowedByAnAdmissionCall()
    {
        var found = new Dictionary<string, int>(StringComparer.Ordinal);
        var offenders = new List<string>();
        foreach (var file in Directory.EnumerateFiles(ServiceDir, "*.cs", SearchOption.AllDirectories)
                     .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                                 && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)))
        {
            var lines = File.ReadAllLines(file);
            for (var i = 0; i < lines.Length; i++)
            {
                if (!lines[i].Contains("ReadScope.Open(", StringComparison.Ordinal) || lines[i].TrimStart().StartsWith("///", StringComparison.Ordinal))
                {
                    continue;
                }

                var name = IsWebEndpointsPartial(file) ? "DarlingWebEndpoints.cs" : Path.GetFileName(file);
                found[name] = found.GetValueOrDefault(name) + 1;

                /* The admission call comes after the scope and the band, within the next few statements, or (#5506 part 3)
                   the read cache's dispatch does, whose miss path runs the method that admits
                   (CacheMissPath_RunsTheMethodThatAdmits). */
                var window = string.Join("\n", lines.Skip(i + 1).Take(25));
                if (!window.Contains("AdmitAsync(", StringComparison.Ordinal) && !window.Contains("AdmitWebRouteAsync(", StringComparison.Ordinal)
                    && !window.Contains(".IsCached(", StringComparison.Ordinal) && !window.Contains("cache.Join(key, ttl,", StringComparison.Ordinal))
                {
                    offenders.Add($"{name}:{i + 1}");
                }
            }
        }

        Assert.Empty(offenders);
        Assert.Equal(ScopeSites.OrderBy(p => p.Key), found.OrderBy(p => p.Key));
    }

    /// <summary>#5506 part 3: a read the cache cannot answer runs through the same method as an uncached one, and that
    /// method admits it, so only a hit (or a caller that joins a read already admitted) skips admission.</summary>
    [Theory]
    [InlineData("Mcp/McpToolLatencyFilter.cs", "private async Task<CallToolResult> RunReadAsync(", "cache.Join(key, ttl, token => RunReadAsync(")]
    [InlineData("DarlingWebEndpoints.cs", "async Task<WebReadAnswer> RunWebReadAsync(HttpContext context)", "cache.Join(key, ttl, token => RunWebReadAsync(")]
    [InlineData("DarlingWebEndpoints.cs", "private static async Task<ComposeRunOutcome> RunComposedPanelAdmittedAsync(", "token => RunComposedPanelAdmittedAsync(")]
    public void CacheMissPath_RunsTheMethodThatAdmits(string file, string method, string joinCall)
    {
        var source = RepoFile.ReadRepoFile(Path.Combine(ServiceDir, file));
        var at = source.IndexOf(method, StringComparison.Ordinal);
        Assert.True(at >= 0, $"{file} no longer declares {method}");
        var body = source.Substring(at, Math.Min(4_000, source.Length - at));
        Assert.Contains("AdmitAsync(", body, StringComparison.Ordinal);
        Assert.Contains(joinCall, source, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSkipRoster_IsTheHealthReads_EachWithAReason_AndEachAToolThatExists()
    {
        Assert.Equal(
            new[] { "get_collection_health", "get_collection_log", "get_read_latency", "get_slow_reads", "get_store_log", "get_store_metrics", "list_servers" },
            ReadAdmission.SkippedRoutes.Keys.OrderBy(k => k, StringComparer.Ordinal));

        var toolSource = string.Join("\n", Directory.EnumerateFiles(Path.Combine(ServiceDir, "Mcp"), "*.cs").Select(File.ReadAllText));
        foreach (var (name, reason) in ReadAdmission.SkippedRoutes)
        {
            Assert.StartsWith("health read: ", reason, StringComparison.Ordinal);
            Assert.Contains($"Name = \"{name}\"", toolSource, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheFilterOrder_IsUnchanged_AndTheLatencyFilterAdmits()
    {
        var host = File.ReadAllText(Path.Combine(ServiceDir, "Mcp", "DarlingMcpHostService.cs"));
        var guard = host.IndexOf(".AddCallToolFilter(McpUnknownArgumentGuard.Instance)", StringComparison.Ordinal);
        var latency = host.IndexOf(".AddCallToolFilter(toolLatency.AsFilter())", StringComparison.Ordinal);
        var gcf = host.IndexOf(".AddCallToolFilter(GcfCallToolFilter.Instance)", StringComparison.Ordinal);
        var sweep = host.IndexOf(".AddCallToolFilter(SensitiveStatementOutputFilter.Instance)", StringComparison.Ordinal);
        Assert.True(guard >= 0 && guard < latency && latency < gcf && gcf < sweep, "The call-tool filter order changed.");
        Assert.Contains("new McpToolLatencyFilter(hostReadLatency, readLatencyLogger, slowReads, readAdmission)", host, StringComparison.Ordinal);

        var filter = File.ReadAllText(Path.Combine(ServiceDir, "Mcp", "McpToolLatencyFilter.cs"));
        Assert.Contains("ReadScope.AdmitAsync(_admission, ReadSurface.Mcp, toolName, windowBand, cancellationToken)", filter, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSharedRunnerAdmitsTheComposedPanelTool()
    {
        /* #5856 review round 1: which tools the latency filter passes through is no longer counted here by its spelling;
           McpToolLatencyRecordingTests.TheInvestigationWait_IsNeitherTimedNorAdmitted_WhileAnotherRead_IsBoth calls them. */
        Assert.Equal("run_custom_view_panel", McpToolLatencyFilter.SkippedToolName);

        var web = RepoFile.ReadRepoFile(Path.Combine(ServiceDir, "DarlingWebEndpoints.cs"));
        var runner = web.IndexOf("internal static async Task<ComposeRunOutcome> RunComposedPanelAsync(", StringComparison.Ordinal);
        var core = web.IndexOf("RunComposedPanelCoreAsync(ComposeHost, postgres, body,", runner, StringComparison.Ordinal);
        Assert.True(runner >= 0 && core > runner);
        Assert.Contains("ReadScope.AdmitAsync(admission, ReadSurface.Compose, composeRoute, composeBand, cancellationToken)", web[runner..core], StringComparison.Ordinal);

        /* D4: the MCP tool passes the request's token, so a client that leaves the queue leaves it. */
        var tool = File.ReadAllText(Path.Combine(ServiceDir, "Mcp", "DarlingMcpCustomViewTools.cs"));
        Assert.Contains("RunComposedPanelAsync(postgres, body, cancellationToken, readLatency", tool, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDedicatedWebReadRoutes_Admit()
    {
        var web = RepoFile.ReadRepoFile(Path.Combine(ServiceDir, "DarlingWebEndpoints.cs"));
        Assert.Contains("AdmitWebRouteAsync(readAdmission, \"get_fleet_overview\"", web, StringComparison.Ordinal);
        Assert.Contains("AdmitWebRouteAsync(readAdmission, \"test_custom_alert_rule\"", web, StringComparison.Ordinal);
        var triage = File.ReadAllText(Path.Combine(ServiceDir, "DarlingTriageEndpoint.cs"));
        Assert.Contains("AdmitWebRouteAsync(readAdmission, TriageRoute", triage, StringComparison.Ordinal);
        Assert.Contains("AdmitWebRouteAsync(readAdmission, AgHealthRoute", web, StringComparison.Ordinal);
        Assert.Equal("get_ag_health", DarlingWebEndpoints.AgHealthRoute);
        Assert.Equal("get_sweep_reports", DarlingFleetSweepEndpoints.SweepReportsRoute);
        Assert.Equal("alert_notebook", AlertNotebookEndpoint.NotebookRoute);
        Assert.Equal("server_databases", DarlingServerDatabasesEndpoint.AdmissionRoute);
    }

    /// <summary>
    /// #5506 part 3 (round 1, M3): every route the service maps is either admitted or exempt with a reason. The scope census
    /// above counts only routes that open a read scope, so a new route that reads the store without one would pass it; this
    /// one fails until the new route is placed on one side or the other. Exempt: health, static answers, definition reads
    /// (one config row, light by D8's "Simpler" rule) and writes.
    ///
    /// <para>Round 2, L5: a route is keyed by its file and its route text, so a second map of the same route in another file
    /// is a new entry the roster does not hold (<see cref="TheCensus_CatchesASecondMapOfTheSameRouteInAnotherFile"/>), and
    /// a handler's text ends where its map call does (<see cref="EndOfCall"/>), so a helper below the last map in a file
    /// cannot satisfy the admit check for it.</para>
    /// </summary>
    private const string WebFile = "DarlingWebEndpoints.cs: ";
    private const string SweepsFile = "DarlingFleetSweepEndpoints.cs: ";

    private static readonly Dictionary<string, string?> MappedRoutes = new(StringComparer.Ordinal)
    {
        [WebFile + "GET \"/api/read/\" + name"] = null,
        [WebFile + "GET \"/api/fleet\""] = null,
        [WebFile + "GET \"/api/ag\""] = null,
        ["DarlingTriageEndpoint.cs: GET \"/api/triage\""] = null,
        ["AlertNotebookEndpoint.cs: GET \"/api/alert-notebook\""] = null,
        [SweepsFile + "GET \"/api/sweeps\""] = null,
        [SweepsFile + "GET \"/api/sweeps/latest\""] = null,
        [SweepsFile + "GET \"/api/sweeps/{id:long}\""] = null,
        [SweepsFile + "GET \"/api/sweeps/watch-items\""] = null,
        ["DarlingServerDatabasesEndpoint.cs: GET Route"] = null, // the database picker, /api/server-databases
        ["DarlingKnownPatternRoutes.cs: POST \"/api/known-patterns\""] = "write (#6334)",
        ["DarlingKnownPatternRoutes.cs: PUT \"/api/known-patterns/{id:long}\""] = "write (#6334)",
        ["DarlingKnownPatternRoutes.cs: DELETE \"/api/known-patterns/{id:long}\""] = "write (#6334)",
        [WebFile + "POST \"/api/compose/run\""] = null,
        [WebFile + "POST \"/api/alerts/test\""] = null,
        [WebFile + "GET \"/api/investigation-export\""] = null, // #6236: admitted like the other heavy reads; up to 2000 cases, read a page at a time
        [WebFile + "GET \"/api/catalog\""] = null, // round 2, L5: it reads the schedule overrides and resolves ?server=
        [WebFile + "GET \"/api/ping\""] = "health: answers without reading the store (D8)",
        [WebFile + "GET \"/api/webhook-signing\""] = "status: answers from what the worker read when it applied the configuration, no store read (#5998)",
        [WebFile + "GET \"/api/ag/count\""] = "the nav-gate probe: one aggregate count over the AG topology, light by construction (#4189)",
        [WebFile + "GET \"/api/session\""] = "the seat's capability probe: reads no store",
        [WebFile + "GET \"/api/alert-templates\""] = "static: the starter templates, built in memory",
        [WebFile + "GET \"/api/views\""] = "definition read: the saved views' summaries (config rows, light)",
        [WebFile + "GET \"/api/views/{id:long}\""] = "definition read: one saved view (one config row)",
        [WebFile + "GET \"/api/alerts\""] = "definition read: the custom alert rules (config rows, light)",
        [WebFile + "GET \"/api/alerts/{id:long}\""] = "definition read: one custom alert rule (one config row)",
        [WebFile + "GET \"/api/admin/servers/{id:int}\""] = "admin definition read: one registry row",
        [WebFile + "GET DarlingAdminServersReader.Route"] = "admin definition read: the server registry (config rows)",
        [WebFile + "POST \"/api/alerts/validate\""] = "validator: parses a rule definition, reads no collected data",
        [WebFile + "POST \"/api/views\""] = "write",
        [WebFile + "PUT \"/api/views/{id:long}\""] = "write",
        [WebFile + "DELETE \"/api/views/{id:long}\""] = "write",
        [WebFile + "POST \"/api/alerts\""] = "write",
        [WebFile + "PUT \"/api/alerts/{id:long}\""] = "write",
        [WebFile + "DELETE \"/api/alerts/{id:long}\""] = "write",
        [WebFile + "POST \"/api/mute-rules\""] = "write",
        [WebFile + "PATCH \"/api/mute-rules/{id}\""] = "write",
        [WebFile + "PUT \"/api/mute-rules/{id}/enabled\""] = "write",
        [WebFile + "DELETE \"/api/mute-rules/{id}\""] = "write",
        [WebFile + "POST \"/api/servers\""] = "admin write",
        [WebFile + "PATCH \"/api/servers/{id:int}\""] = "admin write",
        [WebFile + "POST \"/api/alert-history/dismiss\""] = "write",
        [WebFile + "GET \"/api/investigation-feedback\""] = "definition read: reported requests with their feedback and the false-positive counts (two small config-table reads, capped at 100 requests; #5913)",
        [WebFile + "PUT \"/api/investigation-feedback/{requestId}\""] = "write",
        [WebFile + "GET \"/api/investigation-practice-scores\""] = "definition read: the practice scores per agent (two small config-table reads over the practice requests in the window; advisory, #5918)",
        [WebFile + "GET \"/api/investigation-notes/pending\""] = "definition read: the notes agents proposed that nobody has decided (one small config table, capped at 200 rows; #5903)",
        [WebFile + "GET \"/api/agent-changes\""] = "definition read: the journal's agent calls and refusals (one capped journal read plus the switch; #5917)",
        [WebFile + "GET \"/api/agent-changes/{id:long}/muted-alerts\""] = "definition read: the alerts muted since one agent mute (one capped alert-log read; #5917)",
        [WebFile + "PUT \"/api/agent-changes/{id:long}/undo\""] = "write",
        [WebFile + "PUT \"/api/agent-changes/switch\""] = "write",
        [WebFile + "GET \"/api/server-actions\""] = "definition read: every server with its three action switches (one registry join, small; #6269)",
        [WebFile + "PUT \"/api/servers/{id:int}/actions/{action}\""] = "write",
        [WebFile + "PUT \"/api/investigation-notes/{id:long}/approve\""] = "write",
        [WebFile + "PUT \"/api/investigation-notes/{id:long}/reject\""] = "write",
        [WebFile + "PUT \"/api/investigation-requests/{requestId}/close\""] = "write",
        [WebFile + "PUT \"/api/investigation-notes\""] = "write",
        [WebFile + "PUT \"/api/investigation-practice-requests/{requestId}\""] = "write",
        [WebFile + "GET \"/api/threshold-overrides\""] = "definition read: the threshold overrides, one small config table, plus the registry's names (#5916)",
        [WebFile + "PUT \"/api/threshold-overrides\""] = "write",
        [WebFile + "GET \"/api/agent-setup/access\""] = "definition read: two privilege checks, no data (#6264)",
        [WebFile + "GET \"/api/environment-profiles\""] = "definition read: the environment profiles, one small config table (#6264)",
        [WebFile + "PUT \"/api/environment-profiles\""] = "write",
        [WebFile + "POST \"/api/notification-routes\""] = "write",
        [WebFile + "PUT \"/api/notification-routes/{id:int}\""] = "write",
        [WebFile + "DELETE \"/api/notification-routes/{id:int}\""] = "write",
        [WebFile + "PUT \"/api/alert-settings/daily-report\""] = "write",
        [WebFile + "POST \"/api/collectors/run\""] = "write",
        [WebFile + "GET \"/api/collectors/run/{commandId:long}\""] = "definition read: one command row's status and result (a single indexed row, #5941)",
        [WebFile + "GET \"/api/active-queries/live/available\""] = "definition read: whether this seat and store login can queue a live read (one catalog lookup, #6386)",
        [WebFile + "POST \"/api/active-queries/live\""] = "write",
        [WebFile + "GET \"/api/analyze-now/available\""] = "definition read: whether this seat and store login can queue an analysis run (one catalog lookup, #6389)",
        [WebFile + "POST \"/api/analyze-now\""] = "write",
        [WebFile + "GET \"/api/analyze-now/{commandId:long}\""] = "one command row by id: its status words, no result payload (#6389)",
        [WebFile + "GET \"/api/analysis/ask-ai-prompt\""] = "one server-clock row by server id, then a string (#6389)",
        [WebFile + "GET \"/api/active-queries/live/{commandId:long}\""] = null, // #6386: the result read returns the stored plan XML of a live read, so it is admitted
        [WebFile + "POST \"/api/server-tags\""] = "write",
        [WebFile + "PATCH \"/api/server-tags/{id:int}\""] = "write",
        [WebFile + "DELETE \"/api/server-tags/{id:int}\""] = "write",
        [WebFile + "POST \"/api/server-tags/{id:int}/servers\""] = "write",
        [WebFile + "DELETE \"/api/server-tags/{id:int}/servers\""] = "write",
        [WebFile + "POST \"/api/query-tags\""] = "write",
        [WebFile + "PATCH \"/api/query-tags/{id:int}\""] = "write",
        [WebFile + "DELETE \"/api/query-tags/{id:int}\""] = "write",
        [WebFile + "POST \"/api/query-tags/{id:int}/assignments\""] = "write",
        [WebFile + "DELETE \"/api/query-tags/{id:int}/assignments\""] = "write",
        [WebFile + "POST \"/api/query-tag-rules\""] = "write",
        [WebFile + "PATCH \"/api/query-tag-rules/{id:long}\""] = "write",
        [WebFile + "DELETE \"/api/query-tag-rules/{id:long}\""] = "write",
        [WebFile + "GET \"/api/query-tag-rules\""] = "definition read: the query tag rules (one small config table, capped by the rule count; #5986)",
        [WebFile + "GET \"/api/query-tag-rules/test\""] = null, // #5986: the dry run reads up to 10,000 stored sightings, so it is admitted
    };

    /// <summary>The service's own source files, as (path relative to the service folder, text).</summary>
    private static IEnumerable<(string File, string Source)> ServiceSources() =>
        Directory.EnumerateFiles(ServiceDir, "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                        && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                        && !IsWebEndpointsPartial(f))
            .Select(f => (Path.GetRelativePath(ServiceDir, f).Replace(Path.DirectorySeparatorChar, '/'), RepoFile.ReadRepoFile(f)));

    /// <summary>#7026: <c>DarlingWebEndpoints.&lt;Area&gt;.cs</c> is part of <c>DarlingWebEndpoints.cs</c>, so the roster counts
    /// and reads the pieces as the one file they were.</summary>
    private static bool IsWebEndpointsPartial(string path) =>
        Path.GetFileName(path).StartsWith("DarlingWebEndpoints.", StringComparison.Ordinal)
        && !string.Equals(Path.GetFileName(path), "DarlingWebEndpoints.cs", StringComparison.Ordinal);

    [Fact]
    public void EveryMappedRoute_IsAdmittedOrExemptWithAReason()
    {
        var found = MappedRouteHandlers(ServiceSources());
        Assert.Equal(MappedRoutes.Keys.OrderBy(k => k, StringComparer.Ordinal), found.Keys.OrderBy(k => k, StringComparer.Ordinal));
        foreach (var (route, reason) in MappedRoutes)
        {
            if (reason is null)
            {
                var handler = found[route];
                Assert.True(handler.Contains("AdmitWebRouteAsync(", StringComparison.Ordinal)
                    || handler.Contains("ReadScope.AdmitAsync(", StringComparison.Ordinal)
                    || handler.Contains("RunComposedPanelAsync(", StringComparison.Ordinal),
                    $"{route} is on the admitted roster but its handler does not admit");
            }
            else
            {
                Assert.False(string.IsNullOrWhiteSpace(reason));
            }
        }
    }

    /// <summary>Round 2, L5: a second map of a route already on the roster, in another file, is a key of its own and fails the
    /// roster check; a handler's text ends at its own map call, so an admit in a helper below it does not count.</summary>
    [Fact]
    public void TheCensus_CatchesASecondMapOfTheSameRouteInAnotherFile()
    {
        /* Members only, with no type declaration: the guard-stage scanner reads this file's text, and must not take the
           planted text for a type of its own. */
        const string secondMap = """
            internal static void MapSecond(WebApplication app) =>
                app.MapGet("/api/fleet", async (HttpContext context) => Results.Json(new { note = "a ) in a string, and '(' too" }));

            private static Task AdmitBelow(HttpContext context) => DarlingWebEndpoints.AdmitWebRouteAsync(null, "x", "none", context);
            """;
        var found = MappedRouteHandlers(ServiceSources().Append(("SecondMap.cs", secondMap)));

        Assert.Contains(WebFile + "GET \"/api/fleet\"", found.Keys);
        var second = Assert.Single(found, f => f.Key == "SecondMap.cs: GET \"/api/fleet\"").Value;
        Assert.DoesNotContain("AdmitWebRouteAsync(", second, StringComparison.Ordinal);
        Assert.EndsWith("too\" }))", second, StringComparison.Ordinal);
        Assert.NotEqual(MappedRoutes.Keys.OrderBy(k => k, StringComparer.Ordinal), found.Keys.OrderBy(k => k, StringComparer.Ordinal));
    }

    /// <summary>Every <c>Map*</c> call in <paramref name="sources"/>, keyed <c>file: METHOD route</c>, with the text of the
    /// call itself (from <c>.Map</c> to its closing parenthesis). A call whose parentheses do not balance, or a key mapped
    /// twice in one file, fails here.</summary>
    private static Dictionary<string, string> MappedRouteHandlers(IEnumerable<(string File, string Source)> sources)
    {
        var mapCall = new System.Text.RegularExpressions.Regex(@"\.Map(Get|Post|Put|Delete|Patch)\(\s*([^,]+?)\s*,");
        var found = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (file, source) in sources)
        {
            var matches = mapCall.Matches(source);
            for (var i = 0; i < matches.Count; i++)
            {
                var match = matches[i];
                var lineStart = source.LastIndexOf('\n', match.Index) + 1;
                if (source.AsSpan(lineStart, match.Index - lineStart).TrimStart().StartsWith("//", StringComparison.Ordinal)
                    || source.AsSpan(lineStart, match.Index - lineStart).TrimStart().StartsWith("*", StringComparison.Ordinal))
                {
                    continue;
                }

                var key = file + ": " + match.Groups[1].Value.ToUpperInvariant() + " " + match.Groups[2].Value;
                var open = match.Index + 4 + match.Groups[1].Length;
                var end = EndOfCall(source, open);
                Assert.True(end > open, $"{key}: the census could not find where its map call ends");
                Assert.True(found.TryAdd(key, source.Substring(match.Index, end - match.Index)), $"{key} is mapped twice in one file");
            }
        }

        return found;
    }

    /// <summary>The index just past the parenthesis that closes the one at <paramref name="open"/>, or -1. Comments, string
    /// literals (regular, verbatim, raw and interpolated, with the holes of an interpolated one read as code) and character
    /// literals are skipped, so a parenthesis inside one is not counted.</summary>
    internal static int EndOfCall(string source, int open)
    {
        var close = Matching(source, open, '(', ')');
        return close < 0 ? -1 : close + 1;
    }

    private static int Matching(string s, int open, char opener, char closer)
    {
        var depth = 0;
        for (var i = open; i < s.Length; i++)
        {
            var c = s[i];
            if (c == '/' && i + 1 < s.Length && s[i + 1] == '/')
            {
                i = s.IndexOf('\n', i);
                if (i < 0)
                {
                    return -1;
                }
            }
            else if (c == '/' && i + 1 < s.Length && s[i + 1] == '*')
            {
                i = s.IndexOf("*/", i + 2, StringComparison.Ordinal);
                if (i < 0)
                {
                    return -1;
                }

                i++;
            }
            else if (c == '\'')
            {
                i = s.IndexOf('\'', i + (i + 1 < s.Length && s[i + 1] == '\\' ? 3 : 2));
                if (i < 0)
                {
                    return -1;
                }
            }
            else if (c == '"' || ((c == '$' || c == '@') && StringStart(s, i) >= 0))
            {
                i = EndOfString(s, i);
                if (i < 0)
                {
                    return -1;
                }
            }
            else if (c == opener)
            {
                depth++;
            }
            else if (c == closer && --depth == 0)
            {
                return i;
            }
        }

        return -1;
    }

    /// <summary>The index of the first quote of a string literal whose prefix (<c>$</c>, <c>@</c>) starts at
    /// <paramref name="i"/>, or -1 when no quote follows the prefix.</summary>
    private static int StringStart(string s, int i)
    {
        var j = i;
        while (j < s.Length && (s[j] == '$' || s[j] == '@'))
        {
            j++;
        }

        return j < s.Length && s[j] == '"' ? j : -1;
    }

    /// <summary>The index of the last character of the string literal that starts at <paramref name="i"/>, or -1.</summary>
    private static int EndOfString(string s, int i)
    {
        var quote = s[i] == '"' ? i : StringStart(s, i);
        var prefix = s.AsSpan(i, quote - i);
        var verbatim = prefix.Contains('@');
        var interpolated = prefix.Contains('$');
        var quotes = 0;
        while (quote + quotes < s.Length && s[quote + quotes] == '"')
        {
            quotes++;
        }

        if (quotes >= 3)
        {
            var end = s.IndexOf(new string('"', quotes), quote + quotes, StringComparison.Ordinal);
            return end < 0 ? -1 : end + quotes - 1;
        }

        for (var k = quote + 1; k < s.Length; k++)
        {
            var c = s[k];
            if (!verbatim && c == '\\')
            {
                k++;
            }
            else if (c == '"')
            {
                if (verbatim && k + 1 < s.Length && s[k + 1] == '"')
                {
                    k++;
                }
                else
                {
                    return k;
                }
            }
            else if (interpolated && c == '{')
            {
                if (k + 1 < s.Length && s[k + 1] == '{')
                {
                    k++;
                }
                else
                {
                    k = Matching(s, k, '{', '}');
                    if (k < 0)
                    {
                        return -1;
                    }
                }
            }
        }

        return -1;
    }
}

/// <summary>#5506 part 3: a refused web read answers 429 with a Retry-After header and the sentence as <c>error</c>, and
/// records one <c>busy</c> sample. Refused before the handler runs, so no store is needed.</summary>
public sealed class ReadAdmissionWebTests
{
    private sealed class NullLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;
        public bool IsEnabled(LogLevel logLevel) => false;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) { }
    }

    [Fact]
    public async Task AFullQueue_Answers429_WithRetryAfter_AndRecordsBusy()
    {
        const string route = "get_notification_routes";
        var admission = new ReadAdmission(TimeProvider.System, ReadAdmission.DefaultWaitCap, null, 2);
        /* #5506 part 3: these tests measure admission; with the read cache on, identical calls would share one read. */
        admission.Cache.Enabled = false;
        var band = DarlingWebEndpoints.WebWindowBand(route, _ => null);
        admission.SetTable(new Dictionary<(ReadSurface, string, string), long> { [(ReadSurface.Web, route, band)] = 3_000 });

        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            ContentRootPath = Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Service"),
            WebRootPath = "wwwroot",
        });
        builder.WebHost.UseTestServer();
        builder.Logging.ClearProviders();
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Database=none;Username=none;Timeout=1");
        builder.Services.AddSingleton(postgres);
        DarlingWebHostService.ConfigureResponseCompression(builder.Services);
        var app = builder.Build();

        var readLatency = new ReadLatencyAccumulator();
        var host = new DarlingWebHostService(
            new NullLogger<DarlingWebHostService>(), new WebRuntimeState(), new CollectorRuntimeState(), new WebTlsCertificateState(),
            baselineCache: null, readLatency: readLatency, readAdmission: admission);
        host.ConfigurePipeline(app, postgres, networkMode: false, networkListenIp: null,
            allowedCidr: IPNetwork.Parse("127.0.0.1/32"), accessToken: "unused-in-loopback-mode", oidcClient: null);
        await app.StartAsync(TestContext.Current.CancellationToken);

        var held = new List<Task<ReadAdmissionTicket>>();
        for (var i = 0; i < 2 + ReadAdmission.QueueLimit; i++)
        {
            held.Add(Task.Run(() => admission.AdmitAsync(ReadSurface.Web, route, band, CancellationToken.None)));
            var queued = Math.Max(0, i - 1);
            for (var spin = 0; spin < 500 && admission.Stats().Queued != queued; spin++)
            {
                await Task.Delay(10, TestContext.Current.CancellationToken);
            }
        }

        using var client = app.GetTestServer().CreateClient();
        var response = await client.GetAsync("/api/read/" + route, TestContext.Current.CancellationToken);
        Assert.Equal((HttpStatusCode)429, response.StatusCode);
        /* Retry-After is ceil(ahead * largest p90 / limit / 1,000 ms) with ahead = 2 running + 16 queued = 18, the planted p90 of
           3,000 ms (heavy is p90 > 2,000, so the next histogram bucket) and limit 2: 18 * 3,000 / 2 = 27,000 ms, so 27 seconds. */
        Assert.Equal("27", response.Headers.GetValues("Retry-After").Single());
        var body = await response.Content.ReadAsStringAsync(TestContext.Current.CancellationToken);
        Assert.Contains("\"error\":\"The service is running as many long reads as it allows (2), and 16 more are waiting.", body, StringComparison.Ordinal);

        var sample = Assert.Single(readLatency.Drain(), d => d.Route == route);
        Assert.Equal(ReadOutcome.Busy, sample.Outcome);

        foreach (var ticket in held)
        {
            (await ticket).Dispose();
        }

        await app.StopAsync(TestContext.Current.CancellationToken);
    }
}
