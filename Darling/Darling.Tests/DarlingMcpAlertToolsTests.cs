/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Pins the alerts MCP slice — the three READS (get_alert_history, get_alert_settings, get_mute_rules) plus the
/// five Darling-only WRITES (update_alert_settings, create_mute_rule, update_mute_rule, delete_mute_rule,
/// set_mute_rule_enabled) over the Postgres store.
/// Ungated: the tool surface is EXACTLY the eleven names (all static, on a [McpServerToolType] class, returning
/// Task&lt;string&gt;); each read param contract matches Lite's (plus the fleet-only optional server_name on
/// get_alert_history); the write tools require exactly their target (settings_json / rule_id); the read SQL is
/// Postgres-dialect + positional-param + excludes dismissed rows; the advertised tools/list schema is Gemini-clean
/// (#1074) with the expected required-param set; and update_alert_settings VALIDATES a partial update BEFORE any
/// write — a bad or unknown field returns {status:"invalid"} without ever opening a connection. The live
/// tune / CRUD round-trip (and the config_version self-bump) is gated below.
/// </summary>
public sealed class DarlingMcpAlertToolsSurfaceAndSqlTests
{
    /// <summary>A dead data source (unroutable port) — proves the validate-before-write path bails on a bad
    /// partial update WITHOUT ever opening a connection (the call returns before touching the store).</summary>
    private const string DeadStore = "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1";

    private static readonly string[] AlertToolSurface =
    {
        "create_mute_rule",
        "delete_mute_rule",
        /* #3598 (V131): the notification-routes trio — the read publishes the family taxonomy and the routes
           (destinations withheld, presence reported); the two writes are the ones that move no destination,
           the set_mute_rule_enabled / delete_mute_rule shape. */
        "delete_notification_route",
        "get_alert_history",
        "get_alert_settings",
        "get_mute_rules",
        "get_notification_routes",
        "set_mute_rule_enabled",
        "set_notification_route_enabled",
        "update_alert_settings",
        "update_mute_rule",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpAlertTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheElevenAlertTools()
    {
        var toolMethods = ToolMethods();
        var names = toolMethods
            .Select(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(AlertToolSurface, names);
        Assert.NotNull(typeof(DarlingMcpAlertTools).GetCustomAttribute<McpServerToolTypeAttribute>());
        Assert.All(toolMethods, m => Assert.True(m.IsStatic, $"{m.Name} must be static"));
        Assert.All(toolMethods, m => Assert.True(m.ReturnType == typeof(Task<string>), $"{m.Name} must return Task<string>"));
    }

    private static (string Name, bool Optional)[] McpParams(string toolName)
    {
        var method = ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName);
        return method.GetParameters()
            .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null)
            .Select(p => (p.Name!, p.HasDefaultValue))
            .ToArray();
    }

    [Theory]
    [InlineData("get_alert_history", "server_name,hours_back,limit,as_of,include_dismissed")]
    [InlineData("get_mute_rules", "enabled_only")]
    [InlineData("update_alert_settings", "settings_json")]
    [InlineData("create_mute_rule", "server_name,metric_name,database_pattern,query_text_pattern,wait_type_pattern,job_name_pattern,reason,expires_at")]
    [InlineData("delete_mute_rule", "rule_id")]
    [InlineData("set_mute_rule_enabled", "rule_id,enabled")]
    [InlineData("update_mute_rule", "rule_id,changes_json")]
    [InlineData("set_notification_route_enabled", "route_id,enabled")]
    [InlineData("delete_notification_route", "route_id")]
    public void ParamContract_MatchesContract(string toolName, string expectedCsv)
    {
        Assert.Equal(expectedCsv.Split(','), McpParams(toolName).Select(p => p.Name).ToArray());
    }

    [Fact]
    public void ParamContract_AlertSettings_TakesNoInputParameters()
    {
        /* Only the injected NpgsqlDataSource, which is not [Description]-decorated — an empty input schema. */
        Assert.Empty(McpParams("get_alert_settings"));
    }

    [Fact]
    public void ParamContract_ReadsAndCreate_AreAllOptional()
    {
        /* The reads auto-select/omit their params; create_mute_rule's scope/pattern fields are ALL optional (a
           rule with no fields mutes everything). Only update_alert_settings + delete_mute_rule require input. */
        foreach (var tool in new[] { "get_alert_history", "get_mute_rules", "create_mute_rule" })
            Assert.All(McpParams(tool), p => Assert.True(p.Optional, $"{tool}.{p.Name} must be optional"));
    }

    [Theory]
    [InlineData("update_alert_settings", "settings_json")]
    [InlineData("delete_mute_rule", "rule_id")]
    /* Both, not just the target: a set-flag verb whose value defaulted would let a caller that meant to
       disable a rule enable it by omission, on a surface where the omission is the common typing mistake. */
    [InlineData("set_mute_rule_enabled", "rule_id,enabled")]
    /* Both again: an edit verb whose changes defaulted to "{}" would turn a mis-typed call into a silent
       no-op reported as invalid-later, and one whose rule_id defaulted has no subject at all. */
    [InlineData("update_mute_rule", "rule_id,changes_json")]
    public void ParamContract_WriteTools_RequireTheirTarget(string toolName, string requiredCsv)
    {
        var required = McpParams(toolName).Where(p => !p.Optional).Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(requiredCsv.Split(',').OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    /* ---------------- read SQL pins ---------------- */

    [Fact]
    public void AlertHistorySql_ReadsLog_ExcludesDismissedByDefault_ServerScoped()
    {
        var sql = Reader.AlertHistorySql;
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        /* #3541 A3: the exclusion is the DEFAULT arm of a caller's choice, not a hidden constant — $5 lifts
           it. The literal stays so the grid's read and this one keep saying the same words. */
        Assert.Contains("(dismissed = FALSE OR $5)", sql, StringComparison.Ordinal);
        /* And the row SAYS which population it belongs to, so an include-dismissed page can label each row. */
        Assert.Contains("dismissed\n", sql.Replace("\r\n", "\n"), StringComparison.Ordinal);
        /* #2495: BOTH window edges are bound, so server_id and the cap moved up one ordinal each. */
        Assert.Contains("alert_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("alert_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY alert_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AlertHistoryAllServersSql_ReadsLog_ExcludesDismissedByDefault_NoServerFilter()
    {
        var sql = Reader.AlertHistoryAllServersSql;
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("(dismissed = FALSE OR $4)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_id =", sql, StringComparison.Ordinal);   /* fleet-wide */
        Assert.Contains("alert_time <= $2", sql, StringComparison.Ordinal);       /* #2495 upper edge */
        Assert.Contains("LIMIT $3", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3541 A3: the hidden filter is MEASURED, not just disclosed. The count reads the same table over the
    /// same window and scope as the history read, with the predicate inverted, so it is exactly the rows the
    /// default read removed — and it must NOT carry a LIMIT, or a busy window would under-count what it hid.
    /// </summary>
    [Theory]
    [InlineData(nameof(Reader.DismissedAlertCountSql), true)]
    [InlineData(nameof(Reader.DismissedAlertCountAllServersSql), false)]
    public void DismissedAlertCountSql_CountsTheRowsTheDefaultReadHides_Unbounded(string sqlName, bool serverScoped)
    {
        var sql = sqlName == nameof(Reader.DismissedAlertCountSql) ? Reader.DismissedAlertCountSql : Reader.DismissedAlertCountAllServersSql;

        Assert.Contains("SELECT COUNT(*)", sql, StringComparison.Ordinal);
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("alert_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("alert_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("dismissed = TRUE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("LIMIT", sql, StringComparison.Ordinal);
        Assert.Equal(serverScoped, sql.Contains("server_id = $3", StringComparison.Ordinal));
    }

    /// <summary>
    /// <c>include_dismissed</c> is an APPENDED optional, after <c>as_of</c>, defaulting to the grid's own read
    /// — a caller who never sends it reads what they always read, the /api/read dispatch (which passes
    /// <c>as_of</c> by name) is untouched, and no positional C# caller is re-bound. The description must name
    /// the filter in both directions: what it hides and how to lift it.
    /// </summary>
    [Fact]
    public void GetAlertHistory_IncludeDismissed_IsAnAppendedOptional_AndTheDescriptionNamesTheFilter()
    {
        var method = typeof(DarlingMcpAlertTools).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .Single(m => m.GetCustomAttribute<McpServerToolAttribute>()?.Name == "get_alert_history");
        var names = McpParams("get_alert_history").Select(p => p.Name).ToArray();

        var flag = method.GetParameters().Single(p => p.Name == "include_dismissed");
        Assert.True(flag.HasDefaultValue);
        Assert.False((bool)flag.DefaultValue!);
        Assert.True(Array.IndexOf(names, "include_dismissed") > Array.IndexOf(names, "as_of"));

        var description = method.GetCustomAttribute<DescriptionAttribute>()!.Description;
        Assert.Contains("EXCLUDES DISMISSED ALERTS", description, StringComparison.Ordinal);
        Assert.Contains("dismissed_excluded_count", description, StringComparison.Ordinal);
        Assert.Contains("include_dismissed", description, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3541 A14 rider (from #3594's residuals): the web mirror of this read could NOT lift the dismissed
    /// filter. <c>/api/read/get_alert_history</c> dispatched without <c>include_dismissed</c> and its
    /// <c>/api/catalog</c> entry did not advertise it, so a browser or API caller received
    /// <c>dismissed_excluded_count</c> — "N rows were hidden" — with no wire key to un-hide them. Both halves
    /// are pinned: the catalog names the parameter as an optional boolean defaulting to the tool's own default,
    /// and the dispatch passes it BY NAME from the query string through the same <c>QueryBool</c> every other
    /// optional boolean uses (source-text pin, because the dispatch is a lambda over an HttpContext and the
    /// tool would need a store to observe the flag downstream).
    /// </summary>
    [Fact]
    public void WebRead_GetAlertHistory_AdvertisesAndDispatchesIncludeDismissed()
    {
        var descriptor = DarlingWebEndpoints.CatalogDescriptors["get_alert_history"];
        var param = descriptor.Params.Single(p => p.Name == "include_dismissed");
        Assert.Equal("bool", param.Type);
        Assert.False(param.Required);
        Assert.Equal(false, param.Default);

        /* The catalog prose says what the flag does — the description is the only thing a web caller reads. */
        Assert.Contains("include_dismissed", descriptor.Description, StringComparison.Ordinal);
        Assert.Contains("dismissed", descriptor.Description, StringComparison.Ordinal);

        /* And the dispatch line carries it, by name, off the query string. */
        var source = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingWebEndpoints.cs"));
        var dispatchLine = source.Split('\n').Single(l =>
            l.Contains("DarlingMcpAlertTools.GetAlertHistory(", StringComparison.Ordinal));
        Assert.Contains("include_dismissed: QueryBool(c,", dispatchLine, StringComparison.Ordinal);
        Assert.Contains("as_of: AsOf(c)", dispatchLine, StringComparison.Ordinal);
    }

    /// <summary>
    /// #2391: #2349's four knobs are readable and writable through the MCP. They reached 3.5.0 with the
    /// store plane only — clamped on read in <c>DarlingAlertSettings</c>, columns in V79 — but with no group
    /// on either tool and no Viewer control, so an alert that ships OFF could only be switched on with a
    /// hand-written <c>UPDATE</c> against <c>config_alert_settings</c>. Reported by @gotqn, who correctly
    /// held it against the #2107 precedent: a knob rides the store plane, the Settings window AND the two
    /// MCP tools, or it is not reachable.
    /// </summary>
    [Fact]
    public void FileGrowthKnobs_AreOnBothMcpTools()
    {
        Assert.Contains("file_growth_enabled", Reader.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains("file_growth_rise_mb", Reader.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains("file_growth_volume_percent", Reader.AlertSettingsSelectSql, StringComparison.Ordinal);
        Assert.Contains("file_growth_lookback_minutes", Reader.AlertSettingsSelectSql, StringComparison.Ordinal);

        var tools = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs"));

        /* Readable AND writable — a read-only knob still leaves the UPDATE in someone's runbook. */
        Assert.Contains("file_growth = new", tools, StringComparison.Ordinal);
        Assert.Contains("case \"file_growth\":", tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// The write bounds must match <c>DarlingAlertSettings</c>' clamps exactly. If they drift the tool
    /// accepts a value the engine then silently rewrites on read, which presents as the setting not
    /// sticking — a worse bug than a rejected input, because nothing says no.
    ///
    /// <para>Zero is deliberately IN range for the rise: #2349 disables one gate with zero rather than
    /// treating it as invalid, so a floor of 1 would remove the rise-only and level-only configurations.</para>
    /// </summary>
    [Fact]
    public void FileGrowthWriteBounds_MatchTheEngineClamps()
    {
        var tools = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs"));
        var settings = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertSettings.cs"));

        /* engine: Max(0, rise) | Clamp(volume, 0, 100) | Clamp(lookback, 5, 1440) */
        Assert.Contains("Math.Max(0, _config.Alerts.FileGrowthRiseMb)", settings, StringComparison.Ordinal);
        Assert.Contains("FileGrowthVolumePercent, 0, 100", settings, StringComparison.Ordinal);
        Assert.Contains("FileGrowthLookbackMinutes, 5, 1440", settings, StringComparison.Ordinal);

        /* tool: the same numbers */
        Assert.Contains("\"file_growth.rise_mb\", 0, int.MaxValue", tools, StringComparison.Ordinal);
        Assert.Contains("\"file_growth.volume_percent\", 0, 100", tools, StringComparison.Ordinal);
        Assert.Contains("\"file_growth.lookback_minutes\", 5, 1440", tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3539 A8c: <c>file_growth.rise_mb</c> became a RATE (megabytes per hour, averaged over
    /// <c>lookback_minutes</c>) without changing its key, its column or its integer — so the wire contract is
    /// pinned as UNCHANGED here: the read emits the row's value under the same key, the write accepts the same
    /// key into the same column, and the meaning lives in both tool descriptions, which is where an agent reads
    /// it. A rename would have broken every client that reads or writes the setting to say something the
    /// description says just as well.
    /// </summary>
    [Fact]
    public void FileGrowthRise_KeepsItsKeyAndColumn_AndBothDescriptionsSayItIsPerHour()
    {
        var payload = SerializedSettingsPayload(SampleSettingsRow());
        Assert.Equal(1024, payload["file_growth"]!["rise_mb"]!.GetValue<int>());
        Assert.Equal(60, payload["file_growth"]!["lookback_minutes"]!.GetValue<int>());

        var (targets, error) = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(
            "{\"file_growth\":{\"rise_mb\":2048}}")!);
        Assert.Null(error);
        Assert.Equal(new[] { (DarlingMcpAlertTools.AlertSettingsTable, "file_growth_rise_mb") }, targets.ToArray());

        /* The unit, on both descriptions — read off the attributes the MCP host serves, the way the
           get_alert_history pin above reads its own. The census in FileGrowthRiseUnitCensusTests holds the
           phrase across every surface; this is the MCP half stated where the MCP contract is pinned. */
        Assert.Contains("rise_mb is megabytes per HOUR", ToolDescription("get_alert_settings"), StringComparison.Ordinal);
        Assert.Contains("file_growth.rise_mb is megabytes per HOUR", ToolDescription("update_alert_settings"), StringComparison.Ordinal);
    }

    /// <summary>
    /// #3712 (V137): <c>analysis.uncorroborated_route</c> has TWO homes since the rung — the settings row's
    /// <c>analysis_uncorroborated_route</c> column and darling.json's <c>analysis.uncorroboratedRoute</c> — and the
    /// read reports the EFFECTIVE route (never null) beside <c>uncorroborated_route_source</c> saying which home
    /// decided, resolved store-over-file by the SAME function the engine seam uses. The store half comes off the
    /// row; the file half off the ambient publish, as before. The pre-rung pin ("reported from the publish, the
    /// writer refuses to change it") lived here; what replaces it is the four resolutions and the writer's three
    /// arms: a route writes the column in its canonical spelling, an explicit null clears it (the third state,
    /// reachable no other way once a route is stored), anything else is refused by name with the accepted
    /// spellings. The note no longer says FILE-LEVEL — that sentence became a lie the moment the column landed.
    ///
    /// <para>The ambient publish is reset at both ends WITHOUT a <c>finally</c>: this class carries the
    /// live-collection attribute, and the #1902 ratchet reads every <c>finally</c> in such a file as a store
    /// teardown. A leaked value is harmless to the siblings here — the store half on the sample row wins over
    /// any published file value — so the discipline costs nothing on a failure.</para>
    /// </summary>
    [Fact]
    public void UncorroboratedRoute_IsTheEffectiveRoute_WithItsSource_AndTheWriterTakesDigestPageOrNull()
    {
        DarlingFileLevelAlertSettings.ResetForTests();

        /* The sample row's store half is 'page' (deliberately not the shipped digest), so with nothing published
           the store decides: effective 'page', source 'store'. */
        var payload = SerializedSettingsPayload(SampleSettingsRow());
        Assert.Equal("page", payload["analysis"]!["uncorroborated_route"]!.GetValue<string>());
        Assert.Equal(DarlingAlertSettings.RouteSourceStore, payload["analysis"]!["uncorroborated_route_source"]!.GetValue<string>());
        Assert.Equal(DarlingMcpAlertTools.UncorroboratedRouteNote, payload["analysis"]!["uncorroborated_route_note"]!.GetValue<string>());
        Assert.StartsWith("#3712: the EFFECTIVE route, resolved store-over-file", DarlingMcpAlertTools.UncorroboratedRouteNote, StringComparison.Ordinal);
        Assert.DoesNotContain("FILE-LEVEL", DarlingMcpAlertTools.UncorroboratedRouteNote, StringComparison.Ordinal);

        /* A NULL store half with nothing published: neither home holds a route, so the shipped digest applies and
           the source SAYS default — a true statement in a harness, where 'file' would claim a value nothing loaded
           and the pre-rung null claimed only that nothing had been published. */
        var unset = SerializedSettingsPayload(SampleSettingsRow() with { AnalysisUncorroboratedRoute = null });
        Assert.Equal("digest", unset["analysis"]!["uncorroborated_route"]!.GetValue<string>());
        Assert.Equal(DarlingAlertSettings.RouteSourceDefault, unset["analysis"]!["uncorroborated_route_source"]!.GetValue<string>());

        /* Published 'page' in the file, NULL in the store: the file decides. Published 'page' beside a stored
           'digest': the store decides, and the file's page is what the operator overrode. */
        DarlingFileLevelAlertSettings.Publish(new AnalysisConfig { UncorroboratedRoute = "Page" });
        var fileDecides = SerializedSettingsPayload(SampleSettingsRow() with { AnalysisUncorroboratedRoute = null });
        Assert.Equal("page", fileDecides["analysis"]!["uncorroborated_route"]!.GetValue<string>());
        Assert.Equal(DarlingAlertSettings.RouteSourceFile, fileDecides["analysis"]!["uncorroborated_route_source"]!.GetValue<string>());
        var storeDecides = SerializedSettingsPayload(SampleSettingsRow() with { AnalysisUncorroboratedRoute = "digest" });
        Assert.Equal("digest", storeDecides["analysis"]!["uncorroborated_route"]!.GetValue<string>());
        Assert.Equal(DarlingAlertSettings.RouteSourceStore, storeDecides["analysis"]!["uncorroborated_route_source"]!.GetValue<string>());

        /* A store value the CHECK would have refused (a hand-dropped constraint) is ignored, not obeyed: the
           file's page governs and the source says so — the same fall-through LoadViewAsync applies and logs. */
        var garbage = SerializedSettingsPayload(SampleSettingsRow() with { AnalysisUncorroboratedRoute = "pgae" });
        Assert.Equal("page", garbage["analysis"]!["uncorroborated_route"]!.GetValue<string>());
        Assert.Equal(DarlingAlertSettings.RouteSourceFile, garbage["analysis"]!["uncorroborated_route_source"]!.GetValue<string>());

        /* The writer: a route writes the column in the canonical lower-case spelling the CHECK admits. */
        var (pageTargets, pageError, pageWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"analysis\":{\"uncorroborated_route\":\"PAGE\"}}")!);
        Assert.Null(pageError);
        Assert.Equal(new[] { (DarlingMcpAlertTools.AlertSettingsTable, "analysis_uncorroborated_route") }, pageTargets.ToArray());
        Assert.Empty(pageWarnings);
        Assert.Equal("page", Assert.IsType<NpgsqlParameter<string>>(SingleParameterOf("{\"analysis\":{\"uncorroborated_route\":\"PAGE\"}}")).TypedValue);

        /* An explicit null CLEARS the column — a typed text NULL, and a target, because null is a value here. */
        var (nullTargets, nullError, nullWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"analysis\":{\"uncorroborated_route\":null}}")!);
        Assert.Null(nullError);
        Assert.Equal(new[] { (DarlingMcpAlertTools.AlertSettingsTable, "analysis_uncorroborated_route") }, nullTargets.ToArray());
        Assert.Empty(nullWarnings);
        var nullParameter = SingleParameterOf("{\"analysis\":{\"uncorroborated_route\":null}}");
        Assert.Equal(DBNull.Value, nullParameter.Value);
        Assert.Equal(NpgsqlTypes.NpgsqlDbType.Text, nullParameter.NpgsqlDbType);

        /* Anything else is refused by name with the three accepted spellings, and nothing is written. */
        foreach (var refused in new[] { "\"pgae\"", "\"\"", "true", "1", "[\"page\"]" })
        {
            var (refusedTargets, refusedError, _) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
                "{\"analysis\":{\"uncorroborated_route\":" + refused + "}}")!);
            Assert.NotNull(refusedError);
            Assert.Contains("analysis.uncorroborated_route", refusedError, StringComparison.Ordinal);
            Assert.Contains("'digest', 'page', or null", refusedError, StringComparison.Ordinal);
            Assert.Empty(refusedTargets);
        }

        /* The read-only companions coming home claim no column and, alone, are not a write. */
        var (companionTargets, companionError, companionWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"analysis\":{\"uncorroborated_route_source\":\"store\",\"uncorroborated_route_note\":\"anything\"}}")!);
        Assert.Null(companionError);
        Assert.Empty(companionTargets);
        Assert.Empty(companionWarnings);

        /* The provenance warning: a whole-payload round-trip whose own echo said the FILE had been deciding
           writes the effective route into the store (the write is exactly what was sent) and SAYS the decision
           moved — in either key order. An echo of 'store' is a round-trip of a store value and warns nothing; a
           partial update naming the route alone is a deliberate write and warns nothing. */
        foreach (var echoed in new[] { DarlingAlertSettings.RouteSourceFile, DarlingAlertSettings.RouteSourceDefault })
        {
            foreach (var body in new[]
            {
                "{\"analysis\":{\"uncorroborated_route\":\"digest\",\"uncorroborated_route_source\":\"" + echoed + "\"}}",
                "{\"analysis\":{\"uncorroborated_route_source\":\"" + echoed + "\",\"uncorroborated_route\":\"digest\"}}",
            })
            {
                var (movedTargets, movedError, movedWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(body)!);
                Assert.Null(movedError);
                Assert.Single(movedTargets);
                var moved = Assert.Single(movedWarnings);
                Assert.Contains("now governs", moved, StringComparison.Ordinal);
                Assert.Contains("'" + echoed + "'", moved, StringComparison.Ordinal);
                Assert.Contains("analysis.uncorroborated_route: null", moved, StringComparison.Ordinal);
            }
        }

        var (_, storeEchoError, storeEchoWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"analysis\":{\"uncorroborated_route\":\"page\",\"uncorroborated_route_source\":\"store\"}}")!);
        Assert.Null(storeEchoError);
        Assert.Empty(storeEchoWarnings);

        /* And a refused route with a file echo beside it is refused, not warned about. */
        var (_, refusedWithEchoError, refusedWithEchoWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"analysis\":{\"uncorroborated_route\":\"pgae\",\"uncorroborated_route_source\":\"file\"}}")!);
        Assert.NotNull(refusedWithEchoError);
        Assert.Empty(refusedWithEchoWarnings);

        /* Both descriptions say it, where an agent reads before calling: the precedence, the source key, the
           null-clears arm, and no trace of the retired FILE-LEVEL sentence. */
        var read = ToolDescription("get_alert_settings");
        var write = ToolDescription("update_alert_settings");
        Assert.Contains("analysis.uncorroborated_route is where a notify-worthy but UNCORROBORATED finding goes", read, StringComparison.Ordinal);
        Assert.Contains("the STORE winning", read, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route_source", read, StringComparison.Ordinal);
        Assert.Contains("'store', 'file' or 'default'", read, StringComparison.Ordinal);
        Assert.Contains("analysis.uncorroborated_route is WRITABLE since V137", write, StringComparison.Ordinal);
        Assert.Contains("null CLEARS the column", write, StringComparison.Ordinal);
        Assert.Contains("uncorroborated_route_source", write, StringComparison.Ordinal);
        foreach (var description in new[] { read, write })
        {
            Assert.DoesNotContain("FILE-LEVEL", description, StringComparison.Ordinal);
            Assert.DoesNotContain("restart to change", description, StringComparison.Ordinal);
        }

        DarlingFileLevelAlertSettings.ResetForTests();
    }

    /// <summary>The one bound parameter a single-field body produces — for the pins that need its TYPE and
    /// VALUE, which the (table, column) projection <see cref="ParseAsPartialUpdate"/> returns cannot see.</summary>
    private static NpgsqlParameter SingleParameterOf(string body)
    {
        var build = typeof(DarlingMcpAlertTools).GetMethod(
            "BuildAlertSettingsUpdate", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = build.Invoke(null, new object[] { (JsonObject)JsonNode.Parse(body)! })!;
        var updates = ((System.Collections.IEnumerable)result.GetType().GetField("Item1")!.GetValue(result)!).Cast<object>().ToList();
        var target = Assert.Single(updates);
        return (NpgsqlParameter)target.GetType().GetProperty("Param")!.GetValue(target)!;
    }

    /// <summary>
    /// #3653 (from #3541): <c>poison_wait.threshold_ms</c> is reported and accepted but consulted by nothing
    /// since #3593 moved the alert to accumulated wait over a window. The key STAYS (a published field), the
    /// note sits beside it on every read, and the writer accepts the value with a warning rather than either
    /// refusing it (which would break the round-trip the tool's own description prescribes) or storing it in
    /// silence (which would let an operator believe they had tuned an alert).
    ///
    /// <para>The premise is pinned too: the shared engine and the PostgreSQL host must not read the member.
    /// A doc-comment mention is allowed (the member's own summary explains why it survives); a CODE read is
    /// the day this note becomes a lie, and this is where that day fails.</para>
    /// </summary>
    [Fact]
    public void PoisonWaitThresholdMs_IsReportedWithItsRetirementNote_AndWrittenWithAWarning()
    {
        var payload = SerializedSettingsPayload(SampleSettingsRow());
        var poison = payload["poison_wait"]!;
        Assert.Equal(1000, poison["threshold_ms"]!.GetValue<int>());
        Assert.Equal(DarlingMcpAlertTools.PoisonWaitThresholdMsNote, poison["threshold_ms_note"]!.GetValue<string>());
        Assert.StartsWith("retired by #3593", DarlingMcpAlertTools.PoisonWaitThresholdMsNote, StringComparison.Ordinal);

        /* Setting the value: stored under its old bounds, named in the targets, and warned about. */
        var (targets, error, warnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"poison_wait\":{\"threshold_ms\":700}}")!);
        Assert.Null(error);
        Assert.Equal(new[] { (DarlingMcpAlertTools.AlertSettingsTable, "poison_wait_threshold_ms") }, targets.ToArray());
        var warning = Assert.Single(warnings);
        Assert.Contains("poison_wait.threshold_ms", warning, StringComparison.Ordinal);
        Assert.Contains(DarlingMcpAlertTools.PoisonWaitThresholdMsNote, warning, StringComparison.Ordinal);

        /* The bound still runs FIRST: a value that was never valid is refused, not stored-with-a-warning. */
        var (_, boundError, boundWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"poison_wait\":{\"threshold_ms\":0}}")!);
        Assert.NotNull(boundError);
        Assert.Empty(boundWarnings);

        /* A field nothing here retires carries no warning, so warnings is empty rather than absent-or-noisy. */
        var (_, enabledError, enabledWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"poison_wait\":{\"enabled\":false}}")!);
        Assert.Null(enabledError);
        Assert.Empty(enabledWarnings);

        /* The note handed back (a whole-payload round-trip) is accepted, claims no column, and is not a warning:
           it is the tool's own text coming home. The full round-trip is
           EveryColumnRead_IsEmittedByThePayload_AndAcceptedByTheWriter's; this is the one key it could not see
           the reason for. */
        var (noteTargets, noteError, noteWarnings) = ParseAsPartialUpdateWithWarnings((JsonObject)JsonNode.Parse(
            "{\"poison_wait\":{\"threshold_ms_note\":\"anything\"}}")!);
        Assert.Null(noteError);
        Assert.Empty(noteTargets);
        Assert.Empty(noteWarnings);

        /* Both descriptions say it, in the same words, where an agent reads before calling. */
        Assert.Contains("poison_wait.threshold_ms is RETIRED", ToolDescription("get_alert_settings"), StringComparison.Ordinal);
        Assert.Contains("threshold_ms_note", ToolDescription("get_alert_settings"), StringComparison.Ordinal);
        Assert.Contains("poison_wait.threshold_ms is RETIRED", ToolDescription("update_alert_settings"), StringComparison.Ordinal);
        Assert.Contains("warnings:[...]", ToolDescription("update_alert_settings"), StringComparison.Ordinal);

        /* The premise. Comments and strings blanked, so the member's own summary and the two engines' rationale
           paragraphs do not count as reads; the Lite/Darling settings CLASSES that implement the property are
           deliberately outside this population — they must keep implementing it for the contract to compile. */
        foreach (var path in new[]
        {
            System.IO.Path.Combine("PerformanceMonitor.Alerting", "AlertEngine.cs"),
            System.IO.Path.Combine("PerformanceMonitor.Alerting", "PostgresAlertEvaluator.cs"),
            System.IO.Path.Combine("PerformanceMonitor.Alerting", "PoisonWaitEvaluator.cs"),
            System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"),
        })
        {
            var code = CSharpSourceWalker.StripCommentsAndStrings(ReadRepoFile(path));
            Assert.DoesNotContain("PoisonWaitThresholdMs", code, StringComparison.Ordinal);
        }
    }

    private static string ToolDescription(string toolName) =>
        ToolMethods().Single(m => m.GetCustomAttribute<McpServerToolAttribute>()!.Name == toolName)
            .GetCustomAttribute<DescriptionAttribute>()!.Description;

    /// <summary>
    /// The SELECT's column count and the positional read must agree. Every field on
    /// <c>AlertSettingsReadRow</c> is read by ORDINAL, so a column inserted anywhere but the end re-maps
    /// every field after it — silently, since the types mostly line up. Derived rather than hand-counted so
    /// the next knob cannot get this wrong.
    /// </summary>
    [Fact]
    public void AlertSettingsSelect_ColumnCount_MatchesTheOrdinalsRead()
    {
        var source = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingAlertReader.cs"));

        var sql = Reader.AlertSettingsSelectSql;
        var select = sql[(sql.IndexOf("SELECT", StringComparison.Ordinal) + 6)..
                          sql.IndexOf("FROM config_alert_settings", StringComparison.Ordinal)];
        var columns = select.Replace("\n", " ").Split(',', StringSplitOptions.RemoveEmptyEntries).Length;

        var at = source.IndexOf("return new AlertSettingsReadRow(", StringComparison.Ordinal);
        var body = source[at..source.IndexOf(");", at, StringComparison.Ordinal)];
        var ordinals = System.Text.RegularExpressions.Regex.Matches(body, @"reader\.\w+\((\d+)\)")
            .Select(m => int.Parse(m.Groups[1].Value)).Distinct().ToList();

        Assert.Equal(columns, ordinals.Count);
        Assert.Equal(columns - 1, ordinals.Max());
    }

    /// <summary>
    /// The round-trip invariant, DERIVED rather than hand-pinned: the set of columns the reader SELECTs must
    /// equal the set of columns <c>update_alert_settings</c> writes when it is handed <c>get_alert_settings</c>'
    /// own payload. That is the round trip the tool's description tells a caller to perform — read the
    /// settings, change one number, write them back — asserted as one set comparison rather than as a list of
    /// keys somebody has to remember to extend.
    ///
    /// <para>#2417 found the drift running in BOTH directions at once, which is why the assertion is an
    /// equality and not a subset. Six columns were read out of the store on every call and emitted to nobody
    /// (the AG four and the connection two) — those are missing from the write set. And one key WAS emitted
    /// that the writer refused (<c>blocking.wait_threshold_seconds</c>), so feeding a read payload back
    /// failed with "Unknown field", naming a field the caller never chose to send — that shows up as a
    /// non-null parse error instead of a column set. The third direction, a key emitted for a column nobody
    /// reads, no hand-maintained list would have noticed at all.</para>
    ///
    /// <para>There are NO exemptions, deliberately. Every column on this row is a knob the Viewer's Settings
    /// window already writes, so leaving one readable-but-not-writable just moves an operator to a hand-typed
    /// UPDATE — and it would need an exemption entry, which is the same hand-maintained fact that let six
    /// columns go missing in the first place. If a genuinely read-only column ever arrives, exempt it BY NAME
    /// here with its reason; do not loosen the equality.</para>
    ///
    /// <para>#3314 made the tool span TWO config tables, so the equality is held PER TABLE and each read's
    /// own SELECT list is the expectation for its own plane. A single equality over the union would be
    /// satisfiable by compensating drift — a column dropped from one table's write set and a stray added to
    /// the other's would net to the same set — and it is the weaker check precisely where the new plane is
    /// thinnest.</para>
    /// </summary>
    [Fact]
    public void EveryColumnRead_IsEmittedByThePayload_AndAcceptedByTheWriter()
    {
        var (targets, error) = ParseAsPartialUpdate(SerializedSettingsPayload(SampleSettingsRow()));

        /* Every key the payload emits is accepted. The parse stops at the FIRST rejection, so a non-null
           error here names the exact key update_alert_settings would refuse from its own read. */
        Assert.Null(error);

        /* Each accepted key claimed its own column — two keys sharing one would make whichever lost the
           parse order a silent no-op, with the caller told both were updated. */
        Assert.Equal(targets.Count, targets.Distinct().Count());

        /* Every table the writer can reach is compared, driven off the tool's own list — so a third plane
           added without a read to match it fails here instead of going uncompared. */
        foreach (var table in DarlingMcpAlertTools.WritableTables)
        {
            Assert.Equal(
                SelectedColumnsOf(table).OrderBy(c => c, StringComparer.Ordinal).ToArray(),
                ColumnsFor(targets, table).OrderBy(c => c, StringComparer.Ordinal).ToArray());
        }

        /* And the writer reaches no table outside that list — the direction the loop above cannot see. */
        Assert.Empty(targets.Select(t => t.Table).Except(DarlingMcpAlertTools.WritableTables, StringComparer.Ordinal));
    }

    /// <summary>
    /// #3314: <c>updated_fields</c> reports BARE column names across two tables, which is only unambiguous
    /// while no writable column name appears on both. Qualifying them instead would have redefined every
    /// existing entry of a consumer-visible array, so the uniqueness is the thing being relied on — asserted
    /// here rather than assumed, and it is the assertion that fails on the day a second table grows a
    /// same-named column.
    /// </summary>
    [Fact]
    public void WritableColumnNames_DoNotCollideAcrossTheTwoTables()
    {
        var byTable = DarlingMcpAlertTools.WritableTables
            .Select(t => SelectedColumnsOf(t).ToArray())
            .ToArray();

        /* A count comparison is satisfied by two empty sets, and an emptied SELECT list is exactly the
           accident that would produce them -- so each plane is asserted non-empty first. */
        Assert.Equal(DarlingMcpAlertTools.WritableTables.Length, byTable.Length);
        Assert.All(byTable, columns => Assert.NotEmpty(columns));

        Assert.Equal(
            byTable.Sum(columns => columns.Length),
            byTable.SelectMany(columns => columns).Distinct(StringComparer.Ordinal).Count());
    }

    /// <summary>
    /// #3314 round two: the two config tables are read under ONE snapshot, and the ISOLATION LEVEL is the
    /// whole mechanism. PostgreSQL takes a fresh snapshot per statement under READ COMMITTED, so wrapping
    /// the two SELECTs in a default transaction reads exactly like a fix and changes nothing — this is the
    /// one line whose being wrong is invisible to every behavioural test that does not race a writer.
    ///
    /// <para>The split itself cannot come back by accident: the two single-table reads are private and take
    /// the combined method's connection and transaction, so calling one alone does not compile. That is why
    /// this test pins the LEVEL and the entry point rather than counting call sites — the compiler already
    /// holds the part a test would be redundant for, and the level is the part it cannot.</para>
    /// </summary>
    [Fact]
    public void TheTwoConfigTables_AreReadUnderOneRepeatableReadSnapshot()
    {
        var reader = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingAlertReader.cs"));
        var tools = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs"));

        Assert.Contains("System.Data.IsolationLevel.RepeatableRead", reader, StringComparison.Ordinal);
        Assert.DoesNotContain("IsolationLevel.ReadCommitted", reader, StringComparison.Ordinal);

        /* The single-table reads are private, so the split cannot be reintroduced -- asserted so that
           widening either back to public is a decision someone makes here rather than a quiet edit. */
        Assert.Contains("private static async Task<AlertSettingsReadRow?> ReadAlertSettingsAsync", reader, StringComparison.Ordinal);
        Assert.Contains("private static async Task<int?> ReadDeliveryCooldownAsync", reader, StringComparison.Ordinal);

        /* And both tool paths go through the combined entry point -- get_alert_settings and the post-write
           re-read, which is the one described to the caller as the authoritative merged state. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(
            tools, @"GetAlertConfigurationAsync\(postgres(, cancellationToken)?\)").Count);
    }

    /// <summary>
    /// #3314: the delivery cooldown is reachable through the control plane under a CHANNEL-NEUTRAL name, and
    /// the stored name still works. The whole defect was that the only throttle on a Slack / Teams /
    /// PagerDuty / generic-webhook post was named for email, lived in the SMTP config block, and could not be
    /// read or written by any MCP tool — so on a headless box with no SMTP at all the sole path to it was a
    /// desktop app.
    ///
    /// <para>Both spellings are asserted to reach the SAME column, and sending BOTH in one body is asserted
    /// to be REFUSED. Two SET clauses for one column is a Postgres error, so without the guard the failure
    /// would surface as a dialect message naming neither key the caller sent; and were the duplicate ever
    /// tolerated instead, one of the two values would win silently while the caller was told both applied.</para>
    ///
    /// <para>The canonical name is also asserted to be the ONLY one the read emits. An alias that round-trips
    /// is an alias that becomes a second name for the same setting on the wire, which is its own defect for a
    /// client diffing a read against a write.</para>
    /// </summary>
    [Fact]
    public void DeliveryCooldown_IsWritableUnderBothNames_ButEmittedUnderOnlyOne()
    {
        var canonical = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(
            "{\"delivery\":{\"cooldown_minutes\":45}}")!);
        Assert.Null(canonical.Error);
        Assert.Equal(
            new[] { (DarlingMcpAlertTools.NotificationTable, DarlingMcpAlertTools.DeliveryCooldownColumn) },
            canonical.Targets.ToArray());

        var alias = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(
            "{\"email_cooldown_minutes\":45}")!);
        Assert.Null(alias.Error);
        Assert.Equal(canonical.Targets.ToArray(), alias.Targets.ToArray());

        /* Both spellings at once: refused, and the message names both so the caller knows which to drop. */
        var both = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(
            "{\"email_cooldown_minutes\":45,\"delivery\":{\"cooldown_minutes\":45}}")!);
        Assert.NotNull(both.Error);
        Assert.Contains("delivery.cooldown_minutes", both.Error!, StringComparison.Ordinal);
        Assert.Contains(DarlingMcpAlertTools.DeliveryCooldownColumn, both.Error!, StringComparison.Ordinal);

        /* The read emits the channel-neutral name and NOT the stored alias. */
        var payload = SerializedSettingsPayload(SampleSettingsRow(), deliveryCooldownMinutes: 45);
        Assert.Equal(45, payload["delivery"]!["cooldown_minutes"]!.GetValue<int>());
        Assert.DoesNotContain(DarlingMcpAlertTools.DeliveryCooldownColumn, payload.Select(kv => kv.Key));
    }

    /// <summary>
    /// #3314: the delivery cooldown's write bound is <c>DarlingAlertSettings</c>' clamp EXACTLY — the same
    /// parity <see cref="FileGrowthWriteBounds_MatchTheEngineClamps"/> and
    /// <see cref="AgConnectionAndBlockingWaitWriteBounds_MatchTheEngineClamps"/> hold, and for the same
    /// reason: a wider bound lets the tool ACCEPT a value the engine silently rewrites on read, which
    /// presents to the operator as the setting not sticking, with nothing saying no.
    ///
    /// <para>This is what makes the 120-minute ceiling an ENGINE decision rather than a bound edit. Raising
    /// it here alone would reintroduce exactly that class of bug; raising it properly means moving the clamp
    /// in both SKUs. The ceiling stayed: the cooldown is one global number applied to every fingerprint on
    /// every server, so stretching it to silence ONE recurring signature silences everything else at the same
    /// cadence — and a mute rule does that job scoped, expiring, and disclosed by get_mute_rules, where a
    /// multi-hour cooldown suppresses posts that no tool reports.</para>
    /// </summary>
    [Theory]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(120, true)]
    [InlineData(121, false)]
    public void DeliveryCooldownWriteBounds_MatchTheEngineClamp(int minutes, bool accepted)
    {
        foreach (var body in new[]
        {
            $"{{\"delivery\":{{\"cooldown_minutes\":{minutes}}}}}",
            $"{{\"email_cooldown_minutes\":{minutes}}}",
        })
        {
            var parsed = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(body)!);
            Assert.Equal(accepted, parsed.Error is null);
            Assert.Equal(accepted ? 1 : 0, parsed.Targets.Count);
        }

        /* The engine's own clamp, so the numbers above are not a second opinion about the range. */
        var settings = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertSettings.cs"));
        Assert.Contains("Math.Clamp(_config.Smtp.EmailCooldownMinutes, 1, 120)", settings, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3297: the Retention Held tiers are writable, and the accepted range is the engine's clamp EXACTLY —
    /// the same parity the file-growth, AG and delivery-cooldown bounds hold, for the same reason.
    ///
    /// <para>Driven off the CONSTANTS rather than literals, so this is a behavioural check of what the
    /// parser does with the shared bounds rather than a second opinion about what they are (that identity is
    /// the compiler's, and <c>RetentionHoldRatioKnobRungTests</c> pins that neither side carries a bare
    /// literal). What the theory adds is the boundary behaviour: inclusive at both ends, refused just
    /// outside, and an integer JSON number accepted for a double column — <c>2</c> rather than <c>2.0</c> is
    /// what a client that read back 2.0 and re-serialized it is quite likely to send.</para>
    ///
    /// <para>The floor being the shipped WARNING default is the decision this issue took: healthy whole-chunk
    /// granularity reaches 1.4x measured on production, so a lower threshold fires on a store that is working
    /// correctly — the knobs raise the tiers and cannot lower them. Asserted as the identity rather than the
    /// number so the reasoning and the bound cannot come apart.</para>
    /// </summary>
    [Theory]
    [InlineData("1.4", false)]
    [InlineData("1.5", false)]
    [InlineData("1.9", false)]
    [InlineData("2.0", true)]
    [InlineData("2", true)]
    [InlineData("9.5", true)]
    [InlineData("100.0", true)]
    [InlineData("100.1", false)]
    public void RetentionHoldWriteBounds_MatchTheEngineClamps(string value, bool accepted)
    {
        foreach (var field in new[] { "retention_hold_warn_ratio", "retention_hold_critical_ratio" })
        {
            var parsed = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(
                $"{{\"self_alerts\":{{\"{field}\":{value}}}}}")!);

            Assert.Equal(accepted, parsed.Error is null);
            Assert.Equal(accepted ? 1 : 0, parsed.Targets.Count);
            if (accepted)
            {
                Assert.Equal(
                    (DarlingMcpAlertTools.AlertSettingsTable, field),
                    Assert.Single(parsed.Targets));
            }
        }

        /* The bound the theory's numbers came from, and the decision behind its floor. */
        Assert.Equal(2.0, TimescaleSupport.RetentionHoldRatioFloor);
        Assert.Equal(100.0, TimescaleSupport.RetentionHoldRatioCeiling);
        Assert.Equal(TimescaleSupport.RetentionHoldWarnRatioDefault, TimescaleSupport.RetentionHoldRatioFloor);
    }

    /// <summary>
    /// #3297: the two tiers are validated INDEPENDENTLY, so a body that puts critical below warn is accepted.
    ///
    /// <para>Pinned as a decision rather than left implicit. The alternative — refusing the pair, or
    /// flooring critical at warn on read — would either reject a coherent configuration or accept a value
    /// and then use a different one. Firing is gated on warn and severity on critical, so the degenerate
    /// pair already means exactly one thing: every fire is Critical, with no Warning tier. That is what
    /// setting it that way asks for, and <c>DarlingSelfAlertTests</c> holds the behaviour end.</para>
    /// </summary>
    [Fact]
    public void RetentionHoldTiers_AreValidatedIndependently_SoAnInvertedPairIsAccepted()
    {
        var parsed = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(
            "{\"self_alerts\":{\"retention_hold_warn_ratio\":8.0,\"retention_hold_critical_ratio\":2.0}}")!);

        Assert.Null(parsed.Error);
        Assert.Equal(2, parsed.Targets.Count);

        /* Two DISTINCT columns, which is what makes both SET clauses land — the two-names-one-column guard
           would otherwise be the thing that refused this pair, for the wrong reason. */
        Assert.Equal(2, parsed.Targets.Distinct().Count());
    }

    /// <summary>
    /// #3444: the PostgreSQL count gates are writable under <c>blocking.pg_count_threshold</c> /
    /// <c>deadlocks.pg_count_threshold</c>, and the accepted range is the engine's clamp EXACTLY — the same
    /// parity every write bound in this file holds, for the same reason: a wider bound lets the tool ACCEPT
    /// a value <c>DarlingAlertSettings</c> then silently rewrites on read, which presents as the setting not
    /// sticking with nothing saying no.
    ///
    /// <para>Zero is OUT of range, unlike most of the numerics in this file, and that is the decision rather
    /// than an oversight: at 0 the gate's <c>count &gt;= threshold</c> test is true for a count of zero, so
    /// an accepted 0 would fire "Deadlocks Detected" on a server with no deadlocks. There is no
    /// disable-by-zero reading to preserve — the <c>enabled</c> switch each group already carries is the off
    /// lever, and it covers both engines.</para>
    /// </summary>
    [Theory]
    [InlineData(-1, false)]
    [InlineData(0, false)]
    [InlineData(1, true)]
    [InlineData(500, true)]
    public void PgCountThresholdWriteBounds_MatchTheEngineClamps(int value, bool accepted)
    {
        foreach (var (group, column) in new[]
        {
            ("blocking", "pg_blocking_count_threshold"),
            ("deadlocks", "pg_deadlock_count_threshold"),
        })
        {
            var parsed = ParseAsPartialUpdate((JsonObject)JsonNode.Parse(
                $"{{\"{group}\":{{\"pg_count_threshold\":{value}}}}}")!);

            Assert.Equal(accepted, parsed.Error is null);
            Assert.Equal(accepted ? 1 : 0, parsed.Targets.Count);
            if (accepted)
            {
                Assert.Equal(
                    (DarlingMcpAlertTools.AlertSettingsTable, column),
                    Assert.Single(parsed.Targets));
            }
        }

        /* The engine's floor, so the theory's numbers are not a second opinion about the range — and the
           writer names the SAME constant, pinned as the whole accepting case so a retyped bound or a
           re-targeted column cannot hide inside a looser substring. */
        Assert.Equal(1, PostgresAlertEvaluator.CountThresholdFloor);
        var tools = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs"));
        Assert.Contains(
            "case \"pg_count_threshold\": AddInt(\"pg_blocking_count_threshold\", n, \"blocking.pg_count_threshold\", PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue); break;",
            tools, StringComparison.Ordinal);
        Assert.Contains(
            "case \"pg_count_threshold\": AddInt(\"pg_deadlock_count_threshold\", n, \"deadlocks.pg_count_threshold\", PostgresAlertEvaluator.CountThresholdFloor, int.MaxValue); break;",
            tools, StringComparison.Ordinal);
    }

    /// <summary>
    /// #3444: get_alert_settings reports the PostgreSQL gates INSIDE the <c>blocking</c> and <c>deadlocks</c>
    /// groups — the key-placement decision the V122 rung argues: the two engines' figures belong side by
    /// side, because that is the only placement where an operator reading "blocking" sees that there are two
    /// of them. Serialized through the tool's own options and re-parsed, so what is asserted is the wire
    /// shape a client receives, not a C# identifier.
    /// </summary>
    [Fact]
    public void PgCountThresholds_AreReportedInsideTheBlockingAndDeadlocksGroups()
    {
        var payload = SerializedSettingsPayload(SampleSettingsRow());

        Assert.Equal(4, payload["deadlocks"]!["pg_count_threshold"]!.GetValue<int>());
        Assert.Equal(6, payload["blocking"]!["pg_count_threshold"]!.GetValue<int>());

        /* Distinct from the SQL Server numbers beside them — the sample row keeps all four different, so a
           payload that emitted the wrong engine's figure under either key cannot pass. */
        Assert.NotEqual(
            payload["deadlocks"]!["count_threshold"]!.GetValue<int>(),
            payload["deadlocks"]!["pg_count_threshold"]!.GetValue<int>());
        Assert.NotEqual(
            payload["blocking"]!["count_threshold"]!.GetValue<int>(),
            payload["blocking"]!["pg_count_threshold"]!.GetValue<int>());

        /* And NOT in a postgres section of their own — a knob in a second group somebody has to know to
           look in is the placement the rung's argument rejects. */
        Assert.DoesNotContain("postgres_alerts", payload.Select(kv => kv.Key));
    }

    /// <summary>
    /// The write bounds for everything #2417 made writable, against <c>DarlingAlertSettings</c>' clamps —
    /// the same parity <see cref="FileGrowthWriteBounds_MatchTheEngineClamps"/> holds for the file-growth
    /// knobs, and for the same reason: a bound that differs lets the tool ACCEPT a value the engine then
    /// silently rewrites on read, which presents as the setting not sticking. Nothing says no.
    ///
    /// <para>The two booleans are asserted to reach the engine UNCLAMPED. That is the case that would
    /// otherwise go unnoticed — a range added to one of them later needs a matching gate here, and this is
    /// what makes the day it appears loud.</para>
    ///
    /// <para>Zero is IN range on all four numerics because 0 is a shipped configuration on each: no
    /// connection re-fire, no AG lag gate, no redo-queue gate (<c>ag_redo_queue_alert_kb</c> ships at 0
    /// precisely because a healthy queue size is workload-specific), no second blocking gate. A floor of 1
    /// would make the shipped row unwritable through the tool that reports it.</para>
    /// </summary>
    [Fact]
    public void AgConnectionAndBlockingWaitWriteBounds_MatchTheEngineClamps()
    {
        var tools = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpAlertTools.cs"));
        var settings = ReadRepoFile(System.IO.Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingAlertSettings.cs"));

        /* engine */
        Assert.Contains("NotifyConnectionDownAtStartup => _config.Alerts.NotifyConnectionDownAtStartup;", settings, StringComparison.Ordinal);
        Assert.Contains("_config.Alerts.ConnectionRefireMinutes, 0, 1440", settings, StringComparison.Ordinal);
        Assert.Contains("NotifyAgHealth => _config.Alerts.NotifyAgHealth;", settings, StringComparison.Ordinal);
        Assert.Contains("_config.Alerts.AgLagAlertSeconds, 0, 86400", settings, StringComparison.Ordinal);
        Assert.Contains("_config.Alerts.AgRedoQueueAlertKb, 0L, 1073741824L", settings, StringComparison.Ordinal);
        Assert.Contains("_config.Alerts.AgDisconnectRefireMinutes, 0, 1440", settings, StringComparison.Ordinal);
        Assert.Contains("Math.Max(0, _config.Alerts.BlockingWaitSecondsThreshold)", settings, StringComparison.Ordinal);

        /* tool: the same numbers, and no gate at all on the two booleans */
        Assert.Contains("AddBool(\"notify_connection_down_at_startup\"", tools, StringComparison.Ordinal);
        Assert.Contains("\"connection_refire_minutes\", 0, 1440", tools, StringComparison.Ordinal);
        Assert.Contains("AddBool(\"notify_ag_health\"", tools, StringComparison.Ordinal);
        Assert.Contains("\"ag.lag_threshold_seconds\", 0, 86400", tools, StringComparison.Ordinal);
        Assert.Contains("\"ag.redo_queue_threshold_kb\", 0L, 1073741824L", tools, StringComparison.Ordinal);
        Assert.Contains("\"ag.disconnect_refire_minutes\", 0, 1440", tools, StringComparison.Ordinal);
        Assert.Contains("\"blocking.wait_threshold_seconds\", 0, int.MaxValue", tools, StringComparison.Ordinal);
    }

    /// <summary>The reader's SELECT list, split from the SHIPPED constant the same way
    /// <see cref="AlertSettingsSelect_ColumnCount_MatchesTheOrdinalsRead"/> counts it — so it cannot drift
    /// from what the reader actually asks the store for.</summary>
    private static IReadOnlyList<string> SelectedAlertSettingsColumns() =>
        SelectedColumnsOf(DarlingMcpAlertTools.AlertSettingsTable);

    /// <summary>The SELECT list of whichever SHIPPED read constant serves <paramref name="table"/>, split the
    /// same way. Mapped from the table name rather than taking the SQL as a parameter so a caller iterating
    /// <c>WritableTables</c> cannot silently compare a plane against the wrong read — an unmapped table
    /// throws here instead of being skipped.</summary>
    private static IReadOnlyList<string> SelectedColumnsOf(string table)
    {
        var sql = table switch
        {
            DarlingMcpAlertTools.AlertSettingsTable => Reader.AlertSettingsSelectSql,
            DarlingMcpAlertTools.NotificationTable => Reader.DeliveryCooldownSelectSql,
            _ => throw new ArgumentOutOfRangeException(nameof(table), table, "No MCP read constant is mapped to this table."),
        };

        var select = sql[(sql.IndexOf("SELECT", StringComparison.Ordinal) + 6)..
                          sql.IndexOf("FROM " + table, StringComparison.Ordinal)];
        return select.Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToList();
    }

    /// <summary>get_alert_settings' payload for one row, serialized through the SAME options the tool
    /// serializes with and re-parsed. Runtime rather than source-parsing for the reason Lite's
    /// <c>McpAlertSettingsKeyTests</c> gives: the C# identifier is not automatically the wire key, so only
    /// serializing proves what a client receives — and therefore what it would hand back.</summary>
    private static JsonObject SerializedSettingsPayload(Reader.AlertSettingsReadRow row, int deliveryCooldownMinutes = 15)
    {
        var build = typeof(DarlingMcpAlertTools).GetMethod(
            "BuildAlertSettingsPayload", BindingFlags.NonPublic | BindingFlags.Static)!;
        var payload = build.Invoke(null, new object[] { row, deliveryCooldownMinutes })!;
        var json = JsonSerializer.Serialize(payload, payload.GetType(), McpHelpers.JsonOptions);
        return (JsonObject)JsonNode.Parse(json)!;
    }

    /// <summary>Runs a body through the tool's REAL partial-update parser and reports the (table, column)
    /// pairs it would write plus the first validation error, if any. Reflected over the UpdateTarget record's
    /// properties rather than cast to a tuple shape, so adding a field to it does not silently change what
    /// this reads.</summary>
    private static (IReadOnlyList<(string Table, string Column)> Targets, string? Error) ParseAsPartialUpdate(JsonObject body)
    {
        var build = typeof(DarlingMcpAlertTools).GetMethod(
            "BuildAlertSettingsUpdate", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = build.Invoke(null, new object[] { body })!;
        var type = result.GetType();
        var updates = ((System.Collections.IEnumerable)type.GetField("Item1")!.GetValue(result)!).Cast<object>().ToList();
        var targets = updates.Select(u =>
        {
            var t = u.GetType();
            return ((string)t.GetProperty("Table")!.GetValue(u)!, (string)t.GetProperty("Column")!.GetValue(u)!);
        }).ToList();
        return (targets, (string?)type.GetField("Item2")!.GetValue(result));
    }

    /// <summary><see cref="ParseAsPartialUpdate"/> plus the third element the parser returns since #3653: the
    /// warnings for fields that were written but are consulted by nothing. Read off <c>Item3</c> the same
    /// reflective way, so the two-element callers above did not have to change.</summary>
    private static (IReadOnlyList<(string Table, string Column)> Targets, string? Error, IReadOnlyList<string> Warnings)
        ParseAsPartialUpdateWithWarnings(JsonObject body)
    {
        var (targets, error) = ParseAsPartialUpdate(body);
        var build = typeof(DarlingMcpAlertTools).GetMethod(
            "BuildAlertSettingsUpdate", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = build.Invoke(null, new object[] { body })!;
        var warnings = ((System.Collections.IEnumerable)result.GetType().GetField("Item3")!.GetValue(result)!)
            .Cast<string>().ToList();
        return (targets, error, warnings);
    }

    /// <summary>The columns the parser would write to one table.</summary>
    private static IReadOnlyList<string> ColumnsFor(
        IReadOnlyList<(string Table, string Column)> targets, string table) =>
        targets.Where(t => t.Table == table).Select(t => t.Column).ToList();

    /// <summary>A plausible settings row whose every value sits INSIDE the writer's bounds, so the invariant
    /// above fails on a missing or unaccepted KEY rather than on a value. Named arguments deliberately: a new
    /// column makes this stop compiling until someone supplies it, which is the moment to decide whether the
    /// payload and the writer learn about it too. (Ordinal safety of the SELECT itself is
    /// <see cref="AlertSettingsSelect_ColumnCount_MatchesTheOrdinalsRead"/>'s job, not this one's.)</summary>
    private static Reader.AlertSettingsReadRow SampleSettingsRow() => new(
        Enabled: true,
        CpuEnabled: true, CpuThresholdPercent: 80, CpuMode: "sql",
        BlockingEnabled: true, BlockingCountThreshold: 5,
        DeadlockEnabled: true, DeadlockCountThreshold: 3,
        PoisonWaitEnabled: true, PoisonWaitThresholdMs: 1000,
        LongRunningQueryEnabled: true, LongRunningQueryThresholdMinutes: 5,
        TempDbSpaceEnabled: true, TempDbSpaceThresholdPercent: 80,
        LowDiskEnabled: true, LowDiskThresholdPercent: 10, LowDiskThresholdGb: 20,
        LongRunningJobEnabled: true, LongRunningJobMultiplier: 3,
        FailedJobEnabled: true, FailedJobLookbackMinutes: 60,
        CooldownMinutes: 15,
        ExcludedDatabases: new[] { "tempdb" },
        AnalysisEnabled: true, AnalysisIntervalMinutes: 60,
        AnalysisNotificationsEnabled: true, AnalysisNotifySeverity: 1.0,
        DeliveryMode: "Summary", PerEventMax: 10,
        LongRunningQueryMaxResults: 25,
        LongRunningQueryExcludeSpServerDiagnostics: true,
        LongRunningQueryExcludeWaitFor: true,
        LongRunningQueryExcludeBackups: true,
        LongRunningQueryExcludeMiscWaits: true,
        LongRunningQueryExcludeCdc: true,
        NotifyConnectionChanges: true,
        NotifyConnectionDownAtStartup: true,
        ConnectionRefireMinutes: 30,
        NotifyAgHealth: true,
        AgLagAlertSeconds: 300,
        AgRedoQueueAlertKb: 1048576L,
        AgDisconnectRefireMinutes: 30,
        BlockingWaitSecondsThreshold: 60,
        PvsEnabled: true, PvsThresholdPercent: 40, PvsFloorGb: 1,
        DatabaseStateEnabled: true,
        SelfDiskFreeWarnPercent: 15,
        CollectionStaleMinutes: 30,
        CollectionFailureThreshold: 3,
        DiskCriticalFreePercent: 5,
        DiskCriticalFreeGb: 10,
        AnalysisNotifyCooldownMinutes: 360,
        StoreJobCadenceWarnPercent: 80,
        FileGrowthEnabled: true, FileGrowthRiseMb: 1024, FileGrowthVolumePercent: 10,
        FileGrowthLookbackMinutes: 60,
        /* #3297: deliberately NOT the shipped 2.0/4.0. A sample row equal to the defaults would let a
           payload that emitted a constant instead of the row's value round-trip unnoticed. */
        RetentionHoldWarnRatio: 3.0, RetentionHoldCriticalRatio: 7.5,
        /* #3368: inside [1.0, 1000.0], and deliberately NOT the shipped 5.0 / 20.0 — a sample equal to the
           default would let a surface that dropped the column and fell back to the default still match. */
        DeadlockWarnPerHour: 7.0, DeadlockCriticalPerHour: 31.0,
        /* #3444: inside the write bound (>= 1), deliberately NOT the shipped 1s, and deliberately NOT the
           SQL Server numbers above (3 and 5) — equal pairs would let a payload that emitted the wrong
           engine's figure under either key round-trip unnoticed. */
        PgDeadlockCountThreshold: 4, PgBlockingCountThreshold: 6,
        /* #3466 (V124): inside the write bound [15, 1440], deliberately NOT the shipped 60 — a sample
           equal to the default would let a payload that dropped the column and fell back to the default
           still match — and enabled deliberately FALSE against the shipped TRUE for the same reason. */
        FleetSweepEnabled: false, FleetSweepIntervalMinutes: 240,
        /* #3528 (V126): inside the write bound (>= 0), deliberately NOT the shipped 50 — a sample equal
           to the default would let a payload that dropped the column and fell back to the default still
           match. */
        SelfDiskFreeWarnGb: 75,
        /* #3653 (A5, Q5): in the stored canonical form (already normalised) and deliberately NOT the seeded
           defaults (the job-step prefix; the two NT AUTHORITY logins) — a sample equal to the V135 DEFAULT would
           let a payload that dropped the column and fell back to the default still match, and an un-normalised
           sample would let the writer's normaliser change what the round-trip compares. */
        LongRunningQueryExcludedProgramNamePrefixes: new[] { "HammerDB", "QueueWorker" },
        LongRunningQueryExcludedLogins: new[] { "svc_replication" },
        /* #3712 (V137): the route knob's store half, deliberately 'page' — NOT the shipped digest and NOT NULL
           (the upgrade-day state) — so a payload that dropped the column and fell through to the file or the
           default would read 'digest' / not-'store' and fail rather than match; and a value the writer hands
           back to the same column, which is what the round-trip equality needs to see. */
        AnalysisUncorroboratedRoute: "page");

    [Fact]
    public void AlertSettingsSql_ReadsSingleGlobalRow()
    {
        var sql = Reader.AlertSettingsSelectSql;
        Assert.Contains("FROM config_alert_settings", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE id = 1", sql, StringComparison.Ordinal);
        Assert.Contains("cpu_threshold_percent", sql, StringComparison.Ordinal);
        Assert.Contains("delivery_mode", sql, StringComparison.Ordinal);
        Assert.Contains("notify_connection_changes", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(nameof(Reader.AlertHistorySql))]
    [InlineData(nameof(Reader.AlertHistoryAllServersSql))]
    [InlineData(nameof(Reader.DismissedAlertCountSql))]
    [InlineData(nameof(Reader.DismissedAlertCountAllServersSql))]
    [InlineData(nameof(Reader.AlertSettingsSelectSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(Reader.AlertHistorySql) => Reader.AlertHistorySql,
            nameof(Reader.AlertHistoryAllServersSql) => Reader.AlertHistoryAllServersSql,
            nameof(Reader.DismissedAlertCountSql) => Reader.DismissedAlertCountSql,
            nameof(Reader.DismissedAlertCountAllServersSql) => Reader.DismissedAlertCountAllServersSql,
            _ => Reader.AlertSettingsSelectSql,
        };
        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("convert(", lower);
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    /* ---------------- advertised MCP schema ---------------- */

    private static Dictionary<string, ModelContextProtocol.Protocol.Tool> BuildToolSchemas()
    {
        var services = new ServiceCollection();
        services.AddSingleton(typeof(NpgsqlDataSource), _ => null!);
        services.AddMcpServer().WithGeminiCompatibleTools<DarlingMcpAlertTools>();
        using var provider = services.BuildServiceProvider();
        return provider.GetServices<McpServerTool>().ToDictionary(t => t.ProtocolTool.Name, t => t.ProtocolTool);
    }

    [Fact]
    public void AdvertisedSchema_IsGeminiClean_ForAllElevenTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(AlertToolSurface.Length, tools.Count);
        var violations = tools.Values.SelectMany(t => DarlingMcpSchemaAssert.Violations(t.Name, t.InputSchema)).ToList();
        Assert.True(violations.Count == 0, "Gemini-incompatible schema keywords leaked:\n" + string.Join("\n", violations));
    }

    [Theory]
    [InlineData("get_alert_history", "")]
    [InlineData("get_alert_settings", "")]
    [InlineData("get_mute_rules", "")]
    [InlineData("update_alert_settings", "settings_json")]
    [InlineData("create_mute_rule", "")]
    [InlineData("delete_mute_rule", "rule_id")]
    [InlineData("set_mute_rule_enabled", "enabled,rule_id")]
    [InlineData("update_mute_rule", "changes_json,rule_id")]
    [InlineData("get_notification_routes", "")]
    [InlineData("set_notification_route_enabled", "enabled,route_id")]
    [InlineData("delete_notification_route", "route_id")]
    public void AdvertisedSchema_RequiredParams_MatchTheContract(string toolName, string expectedCsv)
    {
        var expected = expectedCsv.Length == 0 ? Array.Empty<string>() : expectedCsv.Split(',');
        var required = DarlingMcpSchemaAssert.RequiredOf(BuildToolSchemas()[toolName].InputSchema)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(expected.OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    /* ---------------- validate BEFORE write (no connection opened) ---------------- */

    [Theory]
    [InlineData("{\"cpu\":{\"threshold_percent\":900}}")]     // out of range (1-100)
    [InlineData("{\"cpu\":{\"threshold_percent\":0}}")]       // below min
    [InlineData("{\"cpu\":{\"mode\":\"bogus\"}}")]            // bad enum
    [InlineData("{\"delivery\":{\"mode\":\"Nope\"}}")]        // bad enum
    [InlineData("{\"cooldown_minutes\":0}")]                  // below min (1-120)
    [InlineData("{\"analysis\":{\"notify_severity\":9.9}}")]  // above max (0.0-2.0)
    [InlineData("{\"long_running_job\":{\"multiplier\":1}}")] // below min (2-20)
    [InlineData("{\"blocking\":{\"pg_count_threshold\":0}}")]  // below the shared floor (#3444)
    [InlineData("{\"deadlocks\":{\"pg_count_threshold\":0}}")] // below the shared floor (#3444)
    [InlineData("{\"cpu\":{\"threshold_percent\":\"90\"}}")]  // wrong type (string, not int)
    [InlineData("{\"alerts_enabled\":\"yes\"}")]              // wrong type (string, not bool)
    [InlineData("{\"excluded_databases\":\"tempdb\"}")]       // wrong type (string, not array)
    [InlineData("{\"cpu\":{\"nonsense\":1}}")]                // unknown NESTED field
    [InlineData("{\"nonsense\":1}")]                          // unknown TOP-LEVEL field
    [InlineData("{\"cpu\":\"notanobject\"}")]                 // a group must be an object
    [InlineData("{}")]                                        // nothing to update
    [InlineData("not json")]                                  // not valid JSON
    [InlineData("[1,2,3]")]                                   // valid JSON but not an object
    public async Task UpdateAlertSettings_BadInput_ReturnsInvalid_WithoutTouchingTheStore(string settingsJson)
    {
        /* Validation runs BEFORE persistence, so every bad input returns 'invalid' without ever opening a
           connection (the dead store would throw if it were reached). */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpAlertTools.UpdateAlertSettings(dead, settingsJson);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Fact]
    public async Task DeleteMuteRule_BlankId_ReturnsInvalid_WithoutTouchingTheStore()
    {
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpAlertTools.DeleteMuteRule(dead, "   ");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Fact]
    public async Task CreateMuteRule_BadExpiresAt_ReturnsInvalid_WithoutTouchingTheStore()
    {
        /* The only create_mute_rule input that can fail validation is a malformed expires_at — it is rejected
           before the store insert (the dead store would throw if reached). */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpAlertTools.CreateMuteRule(dead, expires_at: "not-a-timestamp");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }

    [Theory]
    [InlineData("not json")]                                        // not valid JSON
    [InlineData("[1,2,3]")]                                         // valid JSON but not an object
    [InlineData("{}")]                                              // nothing to update
    [InlineData("{\"nonsense\":1}")]                                // unknown field
    [InlineData("{\"enabled\":false}")]                             // set_mute_rule_enabled's field
    [InlineData("{\"id\":\"other\"}")]                              // the rule's identity
    [InlineData("{\"created_at_utc\":\"2026-01-01T00:00:00Z\"}")]   // the #3306 clock
    [InlineData("{\"summary\":\"derived\"}")]                       // derived, not stored
    [InlineData("{\"reason\":\"\"}")]                               // blank string is not a clear
    [InlineData("{\"reason\":\"   \"}")]                            // whitespace is not a clear either
    [InlineData("{\"server_name\":123}")]                           // wrong type (number, not string)
    [InlineData("{\"expires_at_utc\":\"not-a-timestamp\"}")]        // malformed expiry
    [InlineData("{\"expires_at_utc\":123}")]                        // wrong expiry type
    [InlineData("{\"expires_at\":\"2026-08-01T00:00:00Z\",\"expires_at_utc\":\"2026-08-01T00:00:00Z\"}")] // both spellings
    public async Task UpdateMuteRule_BadInput_ReturnsInvalid_WithoutTouchingTheStore(string changesJson)
    {
        /* Validation runs BEFORE persistence, so every bad input returns 'invalid' without ever opening a
           connection (the dead store would throw if it were reached) — update_alert_settings' discipline. */
        await using var dead = NpgsqlDataSource.Create(DeadStore);
        var result = await DarlingMcpAlertTools.UpdateMuteRule(dead, "some-rule", changesJson);
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(result));
    }
}

/// <summary>
/// An in-memory <see cref="IMuteRuleStore"/> with <see cref="PgMuteRuleStore"/>'s row semantics, so a
/// decision the shipped statements could not produce does not pass here.
///
/// <para>Rows are cloned on the way in and on the way out. A caller holding a rule from
/// <see cref="LoadAllAsync"/> therefore cannot reach the stored row by reference, and an in-memory flag flip
/// that never reached a store reads as what it is rather than as a successful write.</para>
///
/// <para><see cref="SetEnabledAsync"/> writes <c>Enabled</c> alone; <see cref="UpdateAsync"/> writes every
/// field EXCEPT <c>CreatedAtUtc</c>. Those are the two shipped UPDATE statements' SET lists, held to the
/// shipped SQL by <see cref="SetMuteRuleEnabledTests.TheShippedMuteRuleUpdates_NeverSetTheCreationDate"/>.
/// Reads come back newest-first, matching the ORDER BY.</para>
/// </summary>
internal sealed class FakeMuteRuleStore : IMuteRuleStore
{
    private readonly List<MuteRule> _rows = new();

    internal int LoadAllCalls { get; private set; }
    internal int SetEnabledCalls { get; private set; }
    internal int UpdateCalls { get; private set; }

    /// <summary>Runs immediately after a <see cref="SetEnabledAsync"/> write lands, so a test can drive what
    /// another writer does to the row in the window before the read back.</summary>
    internal Action? AfterSetEnabled { get; set; }

    /// <summary>Runs immediately after an <see cref="UpdateAsync"/> write lands — <see cref="AfterSetEnabled"/>'s
    /// twin for the edit verb's write-to-read-back window.</summary>
    internal Action? AfterUpdate { get; set; }

    /// <summary>Runs immediately after an <see cref="InsertAsync"/> write lands, handed the row AS STORED — the
    /// create verb's write-to-read-back window (#3450). Takes the rule because, unlike the flag/edit verbs, the
    /// caller does not know the generated id before the write.</summary>
    internal Action<MuteRule>? AfterInsert { get; set; }

    internal FakeMuteRuleStore Seed(MuteRule rule)
    {
        _rows.Add(rule.Clone());
        return this;
    }

    /// <summary>The stored row by id, cloned — the assertion surface for what a write left behind.</summary>
    internal MuteRule? Row(string id) => _rows.Find(r => r.Id == id)?.Clone();

    internal int Count => _rows.Count;

    internal void Remove(string id) => _rows.RemoveAll(r => r.Id == id);

    public Task<IReadOnlyList<MuteRule>> LoadAllAsync(CancellationToken cancellationToken = default)
    {
        LoadAllCalls++;
        return Task.FromResult<IReadOnlyList<MuteRule>>(
            _rows.OrderByDescending(r => r.CreatedAtUtc).Select(r => r.Clone()).ToList());
    }

    public Task InsertAsync(MuteRule rule)
    {
        if (_rows.Exists(r => r.Id == rule.Id))
        {
            throw new InvalidOperationException($"duplicate mute rule id '{rule.Id}'");
        }

        _rows.Add(rule.Clone());
        AfterInsert?.Invoke(rule.Clone());
        return Task.CompletedTask;
    }

    public Task UpdateAsync(MuteRule rule)
    {
        UpdateCalls++;
        var index = _rows.FindIndex(r => r.Id == rule.Id);
        if (index < 0)
        {
            return Task.CompletedTask;
        }

        var replacement = rule.Clone();
        replacement.CreatedAtUtc = _rows[index].CreatedAtUtc;
        _rows[index] = replacement;
        AfterUpdate?.Invoke();
        return Task.CompletedTask;
    }

    public Task SetEnabledAsync(string ruleId, bool enabled)
    {
        SetEnabledCalls++;
        var row = _rows.Find(r => r.Id == ruleId);
        if (row is not null)
        {
            row.Enabled = enabled;
        }

        AfterSetEnabled?.Invoke();
        return Task.CompletedTask;
    }

    public Task DeleteAsync(string ruleId)
    {
        Remove(ruleId);
        return Task.CompletedTask;
    }

    public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds)
    {
        foreach (var id in expiredIds)
        {
            Remove(id);
        }

        return Task.CompletedTask;
    }
}

/// <summary>
/// set_mute_rule_enabled (#3432): the headless verb for taking a mute rule out of force and putting it back,
/// over the <see cref="IMuteRuleStore"/> seam the tool's store implements.
///
/// <para>The load-bearing claim is that neither direction moves <c>created_at_utc</c>. The Stale Mute Rules
/// self-alert (#3306) ages a rule from it, so a verb that stamped it would give a disable/re-enable cycle
/// the same week of invisibility a delete-and-re-create buys — the defect this tool exists to end, at lower
/// cost and keeping the id, so with nothing changing for a reader to notice.</para>
///
/// <para>Every creation-date assertion here is driven from a rule authored
/// <see cref="PlantedAgeDays"/> days before the test runs, so a reset to "now" is a visibly different value
/// and not a difference the assertions could miss. The same claim is put to #3306's real evaluator in
/// <c>DarlingSelfAlertTests.StaleMute_ARuleDisabledAndReEnabledThroughTheMcpVerb_IsStaleOnItsRealAge</c>,
/// where a reset costs the alert entirely.</para>
/// </summary>
public sealed class SetMuteRuleEnabledTests
{
    private const string RuleId = "rule-under-test";

    /// <summary>Far past #3306's seven-day bound, so the planted value cannot be confused with a fresh one.</summary>
    private const int PlantedAgeDays = 90;

    /// <summary>The authored instant, truncated to the microsecond the <c>timestamp</c> column keeps.</summary>
    private static readonly DateTime Planted = TruncateToMicroseconds(DateTime.UtcNow.AddDays(-PlantedAgeDays));

    private static DateTime TruncateToMicroseconds(DateTime value) =>
        new(value.Ticks - (value.Ticks % 10), DateTimeKind.Utc);

    private static FakeMuteRuleStore StoreWith(bool enabled, DateTime? expiresAtUtc = null) =>
        new FakeMuteRuleStore().Seed(new MuteRule
        {
            Id = RuleId,
            Enabled = enabled,
            CreatedAtUtc = Planted,
            ExpiresAtUtc = expiresAtUtc,
            Reason = "fixture reason",
            MetricName = "High CPU",
        });

    private static JsonNode Payload(string json) =>
        JsonNode.Parse(json)!["mute_rule"] ?? throw new InvalidOperationException($"no mute_rule in {json}");

    private static DateTime ReportedCreatedAt(string json) =>
        DateTime.Parse(
            (string)Payload(json)["created_at_utc"]!,
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind);

    /* ---------------- the instrument ---------------- */

    [Fact]
    public async Task TheFixtureStore_DoesNotHandOutTheStoredRow()
    {
        /* A subject that flipped a loaded rule in memory and never called the store would pass every
           assertion below if this leaked a reference. */
        var store = StoreWith(enabled: true);

        var loaded = (await store.LoadAllAsync()).Single();
        loaded.Enabled = false;
        loaded.CreatedAtUtc = DateTime.UtcNow;

        Assert.True(store.Row(RuleId)!.Enabled);
        Assert.Equal(Planted, store.Row(RuleId)!.CreatedAtUtc);
    }

    [Fact]
    public async Task TheFixtureStore_WritesEnabledAlone_AndUpdateSpares_TheCreationDate()
    {
        var store = StoreWith(enabled: true);

        await store.SetEnabledAsync(RuleId, false);
        Assert.False(store.Row(RuleId)!.Enabled);
        Assert.Equal(Planted, store.Row(RuleId)!.CreatedAtUtc);

        var rebuilt = store.Row(RuleId)!;
        rebuilt.CreatedAtUtc = DateTime.UtcNow;
        rebuilt.Reason = "rewritten";
        await store.UpdateAsync(rebuilt);

        Assert.Equal("rewritten", store.Row(RuleId)!.Reason);
        Assert.Equal(Planted, store.Row(RuleId)!.CreatedAtUtc);
    }

    /// <summary>
    /// The fixture's fidelity is a claim about the shipped SQL, so it is held to it: neither
    /// <c>config_mute_rules</c> UPDATE in <see cref="PgMuteRuleStore"/> names <c>created_at_utc</c> in its
    /// SET list. Reading the statements out of the source rather than restating them, because the value of
    /// this pin is that it fails when the shipped SQL changes.
    /// </summary>
    [Fact]
    public void TheShippedMuteRuleUpdates_NeverSetTheCreationDate()
    {
        var source = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "PgMuteRuleStore.cs");

        Assert.Contains(
            "UPDATE config_mute_rules SET enabled = $2 WHERE id = $1",
            source,
            StringComparison.Ordinal);

        var setLists = System.Text.RegularExpressions.Regex.Matches(
            source,
            @"UPDATE config_mute_rules SET(?<body>.*?)WHERE",
            System.Text.RegularExpressions.RegexOptions.Singleline);

        /* Non-vacuous floor: a regex that matched nothing would assert nothing. Both UPDATEs are here —
           the full-row one UpdateAsync issues and the narrow flag one SetEnabledAsync issues. */
        Assert.Equal(2, setLists.Count);

        foreach (System.Text.RegularExpressions.Match match in setLists)
        {
            Assert.DoesNotContain("created_at_utc", match.Groups["body"].Value, StringComparison.Ordinal);
        }

        /* The column IS in the file — in the SELECT and the INSERT — so the assertion above is about where
           it appears, not about the name being absent. */
        Assert.Contains("created_at_utc", source, StringComparison.Ordinal);
    }

    /* ---------------- the verb ---------------- */

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task BlankId_ReturnsInvalid_WithoutReadingOrWriting(string ruleId)
    {
        var store = StoreWith(enabled: true);

        Assert.Equal("invalid",
            DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, ruleId, false)));
        Assert.Equal(0, store.LoadAllCalls);
        Assert.Equal(0, store.SetEnabledCalls);
    }

    [Fact]
    public async Task AnUnknownId_ReturnsNotFound_AndWritesNothing()
    {
        var store = StoreWith(enabled: true);

        Assert.Equal("not_found",
            DarlingMcpTestData.StatusOf(
                await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, "no-such-rule", false)));
        Assert.Equal(0, store.SetEnabledCalls);
        Assert.True(store.Row(RuleId)!.Enabled);
    }

    /// <summary>Acceptance 1: the rule stops suppressing and is still there.</summary>
    [Fact]
    public async Task Disabling_LeavesTheRuleInTheStore_AndReportsTheStoredRow()
    {
        var store = StoreWith(enabled: true);

        var json = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, RuleId, enabled: false);

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(1, store.Count);
        Assert.False(store.Row(RuleId)!.Enabled);
        Assert.False((bool)Payload(json)["enabled"]!);
        Assert.Equal(RuleId, (string)Payload(json)["id"]!);
        /* The scope and the reason survive too — a disabled rule an operator re-enables must still be the
           rule they wrote, which is the whole difference from a re-create. */
        Assert.Equal("High CPU", (string)Payload(json)["metric_name"]!);
        Assert.Equal("fixture reason", (string)Payload(json)["reason"]!);
        Assert.Equal(1, store.SetEnabledCalls);
        Assert.Equal(0, store.UpdateCalls);
    }

    /// <summary>
    /// Acceptance 2: the planted creation date survives the disable, in the store AND on the wire. The
    /// second assertion in each pair is what makes a reset visible — a stamped value would be minutes old,
    /// not <see cref="PlantedAgeDays"/> days.
    /// </summary>
    [Fact]
    public async Task Disabling_PreservesThePlantedCreationDate_InTheStoreAndOnTheWire()
    {
        var store = StoreWith(enabled: true);

        var json = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, RuleId, enabled: false);

        Assert.Equal(Planted, store.Row(RuleId)!.CreatedAtUtc);
        Assert.True(DateTime.UtcNow - store.Row(RuleId)!.CreatedAtUtc >= TimeSpan.FromDays(PlantedAgeDays - 1));

        Assert.Equal(Planted, ReportedCreatedAt(json));
        Assert.True(DateTime.UtcNow - ReportedCreatedAt(json) >= TimeSpan.FromDays(PlantedAgeDays - 1));
    }

    /// <summary>
    /// The direction that matters most: re-enabling is the one a delete-and-re-create emulation would have
    /// used to buy another <c>StaleMuteAge</c> of invisibility, and it is the cheaper of the two to reach
    /// (the id does not change, so nothing downstream breaks to signal it).
    /// </summary>
    [Fact]
    public async Task ReEnabling_PreservesThePlantedCreationDate_Too()
    {
        var store = StoreWith(enabled: true);

        Assert.Equal("updated",
            DarlingMcpTestData.StatusOf(
                await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, RuleId, enabled: false)));

        var json = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, RuleId, enabled: true);

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.True(store.Row(RuleId)!.Enabled);
        Assert.True((bool)Payload(json)["enabled"]!);
        Assert.Equal(Planted, store.Row(RuleId)!.CreatedAtUtc);
        Assert.Equal(Planted, ReportedCreatedAt(json));
        Assert.True(DateTime.UtcNow - ReportedCreatedAt(json) >= TimeSpan.FromDays(PlantedAgeDays - 1));
        Assert.Equal(2, store.SetEnabledCalls);
        Assert.Equal(0, store.UpdateCalls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task SettingTheValueTheRuleAlreadyHolds_WritesNothing_AndReportsUnchanged(bool enabled)
    {
        var store = StoreWith(enabled);

        var json = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, RuleId, enabled);

        Assert.Equal("unchanged", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(0, store.SetEnabledCalls);
        Assert.Equal(enabled, (bool)Payload(json)["enabled"]!);
        Assert.Equal(Planted, ReportedCreatedAt(json));
    }

    /// <summary>
    /// The reported rule is read back AFTER the write, so a rule deleted in that window has no stored state
    /// to report. Both facts are named: the flag landed and the rule is gone.
    /// </summary>
    [Fact]
    public async Task ARuleDeletedBetweenTheWriteAndTheReadBack_ReportsNotFound_NamingTheWriteThatLanded()
    {
        var store = StoreWith(enabled: true);
        store.AfterSetEnabled = () => store.Remove(RuleId);

        var json = await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, RuleId, enabled: false);

        Assert.Equal("not_found", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(1, store.SetEnabledCalls);
        Assert.Contains("enabled=false", json, StringComparison.Ordinal);
        Assert.Contains("deleted concurrently", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// A store fault is reported, not swallowed into a success. <see cref="IMuteRuleStore"/> requires every
    /// member to throw on failure precisely so this surface can tell the two apart.
    /// </summary>
    [Fact]
    public async Task AStoreFault_IsReportedAsAnError()
    {
        await using var dead = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1");

        var result = await DarlingMcpAlertTools.SetMuteRuleEnabled(dead, RuleId, false);

        /* The failure is the one error envelope (#3653 Q11), not a bare sentence: status says error, the
           message carries the unchanged grammar, and hints.operation names the verb. */
        Assert.True(McpHelpers.IsErrorEnvelope(result), result);
        using var envelope = JsonDocument.Parse(result);
        Assert.Equal("error", envelope.RootElement.GetProperty("status").GetString());
        Assert.StartsWith("Error during set_mute_rule_enabled: ", envelope.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("set_mute_rule_enabled", envelope.RootElement.GetProperty("hints").GetProperty("operation").GetString());
    }
}

/// <summary>
/// update_mute_rule (#3450): the headless verb for editing a mute rule in place — the changes the enabled
/// flag cannot express — over the <see cref="IMuteRuleStore"/> seam the tool's store implements.
///
/// <para>The load-bearing claims, in the order they matter: NO edit moves <c>created_at_utc</c> (the #3306
/// clock, and the whole reason the verb exists — the delete/recreate workaround resets it); a field not
/// sent does not change (the partial-update contract); an explicit null CLEARS a field, following the
/// store's own <c>UpdateAsync</c> semantics (a full-row write of nullable columns, the Viewer's Edit dialog's
/// shape); and <c>enabled</c> is refused here, carried through an edit untouched.</para>
///
/// <para>Creation-date assertions are driven from a rule authored <see cref="PlantedAgeDays"/> days before
/// the test runs, the same instrument <see cref="SetMuteRuleEnabledTests"/> uses and for the same reason: a
/// reset to "now" is a visibly different value. The fixture's fidelity to the shipped SQL is held by
/// <see cref="SetMuteRuleEnabledTests.TheShippedMuteRuleUpdates_NeverSetTheCreationDate"/>, which pins BOTH
/// shipped UPDATE statements' SET lists — the full-row one this verb writes through included. The same
/// preservation claim is put to #3306's real evaluator in <c>DarlingSelfAlertTests</c>.</para>
/// </summary>
public sealed class UpdateMuteRuleTests
{
    private const string RuleId = "rule-under-edit";

    /// <summary>Far past #3306's seven-day bound, so the planted value cannot be confused with a fresh one.</summary>
    private const int PlantedAgeDays = 90;

    /// <summary>The authored instant, truncated to the microsecond the <c>timestamp</c> column keeps.</summary>
    private static readonly DateTime Planted = TruncateToMicroseconds(DateTime.UtcNow.AddDays(-PlantedAgeDays));

    private static DateTime TruncateToMicroseconds(DateTime value) =>
        new(value.Ticks - (value.Ticks % 10), DateTimeKind.Utc);

    /// <summary>A rule with EVERY editable field populated, so "a field not sent does not change" is asserted
    /// against values that would visibly vanish if the merge dropped one, rather than against nulls that a
    /// dropped field would leave looking untouched.</summary>
    private static FakeMuteRuleStore StoreWith(bool enabled = true, DateTime? expiresAtUtc = null) =>
        new FakeMuteRuleStore().Seed(new MuteRule
        {
            Id = RuleId,
            Enabled = enabled,
            CreatedAtUtc = Planted,
            ExpiresAtUtc = expiresAtUtc,
            Reason = "fixture reason",
            ServerName = "fixture-server",
            MetricName = "High CPU",
            DatabasePattern = "fixture-db",
            QueryTextPattern = "fixture-query",
            WaitTypePattern = "fixture-wait",
            JobNamePattern = "fixture-job",
        });

    private static JsonNode Payload(string json) =>
        JsonNode.Parse(json)!["mute_rule"] ?? throw new InvalidOperationException($"no mute_rule in {json}");

    private static string[] UpdatedFields(string json) =>
        JsonNode.Parse(json)!["updated_fields"]!.AsArray().Select(n => (string)n!).ToArray();

    private static DateTime ReportedCreatedAt(string json) =>
        DateTime.Parse(
            (string)Payload(json)["created_at_utc"]!,
            System.Globalization.CultureInfo.InvariantCulture,
            System.Globalization.DateTimeStyles.RoundtripKind);

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task BlankId_ReturnsInvalid_WithoutReadingOrWriting(string ruleId)
    {
        var store = StoreWith();

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, ruleId, "{\"reason\":\"x\"}")));
        Assert.Equal(0, store.LoadAllCalls);
        Assert.Equal(0, store.UpdateCalls);
    }

    [Fact]
    public async Task BadChanges_ReturnInvalid_BeforeTheStoreIsEvenRead()
    {
        /* The dead-store theory proves no connection opens; this proves the ordering against the seam —
           parse and validate first, so a bad body costs no read either. */
        var store = StoreWith();

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, RuleId, "{\"enabled\":true}")));
        Assert.Equal(0, store.LoadAllCalls);
    }

    [Fact]
    public async Task AnUnknownId_ReturnsNotFound_AndWritesNothing()
    {
        var store = StoreWith();

        Assert.Equal("not_found", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, "no-such-rule", "{\"reason\":\"x\"}")));
        Assert.Equal(0, store.UpdateCalls);
        Assert.Equal("fixture reason", store.Row(RuleId)!.Reason);
    }

    /// <summary>Acceptance 1: the partial contract. One field moves; every other populated field — and the
    /// flag, and the creation date — is exactly what was stored.</summary>
    [Fact]
    public async Task EditingOneField_LeavesEveryOtherFieldAsStored()
    {
        var store = StoreWith();

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, RuleId, "{\"reason\":\"root cause found\"}");

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(new[] { "reason" }, UpdatedFields(json));
        Assert.Equal(1, store.UpdateCalls);
        Assert.Equal(0, store.SetEnabledCalls);

        var row = store.Row(RuleId)!;
        Assert.Equal("root cause found", row.Reason);
        Assert.Equal("fixture-server", row.ServerName);
        Assert.Equal("High CPU", row.MetricName);
        Assert.Equal("fixture-db", row.DatabasePattern);
        Assert.Equal("fixture-query", row.QueryTextPattern);
        Assert.Equal("fixture-wait", row.WaitTypePattern);
        Assert.Equal("fixture-job", row.JobNamePattern);
        Assert.True(row.Enabled);
        Assert.Null(row.ExpiresAtUtc);

        /* And the wire reports the stored row, not the caller's intent restated. */
        Assert.Equal("root cause found", (string)Payload(json)["reason"]!);
        Assert.Equal("fixture-server", (string)Payload(json)["server_name"]!);
    }

    /// <summary>
    /// Acceptance 2: the planted creation date survives an edit, in the store AND on the wire — the #3306
    /// clock this verb exists to stop resetting. The age assertion is what makes a reset visible: a stamped
    /// value would be minutes old, not <see cref="PlantedAgeDays"/> days.
    /// </summary>
    [Fact]
    public async Task Editing_PreservesThePlantedCreationDate_InTheStoreAndOnTheWire()
    {
        var store = StoreWith();

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(
            store, RuleId, "{\"reason\":\"edited\",\"job_name_pattern\":\"nightly\"}");

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(Planted, store.Row(RuleId)!.CreatedAtUtc);
        Assert.True(DateTime.UtcNow - store.Row(RuleId)!.CreatedAtUtc >= TimeSpan.FromDays(PlantedAgeDays - 1));

        Assert.Equal(Planted, ReportedCreatedAt(json));
        Assert.True(DateTime.UtcNow - ReportedCreatedAt(json) >= TimeSpan.FromDays(PlantedAgeDays - 1));
    }

    /// <summary>Acceptance 3: an explicit null CLEARS — the store's own UpdateAsync semantics. The bounded
    /// rule becomes permanent, the pattern dimension stops constraining, and both are visible in the store
    /// and on the wire.</summary>
    [Fact]
    public async Task AnExplicitNull_ClearsTheField()
    {
        var store = StoreWith(expiresAtUtc: DateTime.UtcNow.AddDays(7));

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(
            store, RuleId, "{\"expires_at_utc\":null,\"job_name_pattern\":null}");

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(new[] { "expires_at_utc", "job_name_pattern" }, UpdatedFields(json).OrderBy(f => f, StringComparer.Ordinal).ToArray());
        Assert.Null(store.Row(RuleId)!.ExpiresAtUtc);
        Assert.Null(store.Row(RuleId)!.JobNamePattern);
        /* The dimensions NOT cleared still constrain — a clear is a scalpel, not a reset. */
        Assert.Equal("fixture-wait", store.Row(RuleId)!.WaitTypePattern);
        Assert.Null(Payload(json)["expires_at_utc"]);
        Assert.Null(Payload(json)["job_name_pattern"]);
    }

    /// <summary>The widening hazard the description warns about, shown real: clearing every constraining
    /// field leaves a rule that matches EVERY alert, and the payload's summary says so.</summary>
    [Fact]
    public async Task ClearingEveryScopeField_LeavesABlanketRule_AndTheSummarySaysSo()
    {
        var store = StoreWith();

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, RuleId,
            "{\"server_name\":null,\"metric_name\":null,\"database_pattern\":null,\"query_text_pattern\":null,\"wait_type_pattern\":null,\"job_name_pattern\":null}");

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.True(store.Row(RuleId)!.MatchesEveryAlert);
        Assert.Equal("(matches all alerts)", (string)Payload(json)["summary"]!);
    }

    /// <summary>create_mute_rule's expires_at spelling lands on the same field and reports under the
    /// canonical name — the alias is write-only, exactly like the settings tool's stored-name alias.</summary>
    [Fact]
    public async Task TheExpiresAtAlias_SetsTheExpiry_AndReportsTheCanonicalName()
    {
        var store = StoreWith();
        var bound = TruncateToMicroseconds(DateTime.UtcNow.AddDays(30));

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(
            store, RuleId, $"{{\"expires_at\":\"{bound:O}\"}}");

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(new[] { "expires_at_utc" }, UpdatedFields(json));
        Assert.Equal(bound, store.Row(RuleId)!.ExpiresAtUtc);
    }

    /// <summary>The refusal that keeps the two mute-write verbs from overlapping: <c>enabled</c> is
    /// set_mute_rule_enabled's field, and an edit CARRIES the flag it read — a disabled rule stays disabled
    /// through an edit, in the store and on the wire.</summary>
    [Fact]
    public async Task Enabled_IsRefused_AndCarriedUntouchedThroughAnEdit()
    {
        var store = StoreWith(enabled: false);

        var refused = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, RuleId, "{\"enabled\":true}");
        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(refused));
        Assert.Contains("set_mute_rule_enabled", refused, StringComparison.Ordinal);
        Assert.False(store.Row(RuleId)!.Enabled);

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, RuleId, "{\"reason\":\"still disabled\"}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.False(store.Row(RuleId)!.Enabled);
        Assert.False((bool)Payload(json)["enabled"]!);
    }

    /// <summary>Sending the values the rule already holds writes nothing and says so — a retry is safe, and
    /// an explicit null against an already-null field is "unchanged", not a phantom edit.</summary>
    [Fact]
    public async Task SendingTheStoredValues_WritesNothing_AndReportsUnchanged()
    {
        var store = StoreWith();

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(
            store, RuleId, "{\"reason\":\"fixture reason\",\"expires_at_utc\":null}");

        Assert.Equal("unchanged", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(0, store.UpdateCalls);
        Assert.Equal("fixture reason", (string)Payload(json)["reason"]!);
        Assert.Equal(Planted, ReportedCreatedAt(json));
    }

    /// <summary>The comparison is ORDINAL: a case-only edit IS an edit. Matching is case-insensitive, but the
    /// stored text is the operator's, and second-guessing a deliberate respelling would report "unchanged"
    /// for a write the caller asked for.</summary>
    [Fact]
    public async Task ACaseOnlyEdit_CountsAsAChange()
    {
        var store = StoreWith();

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, RuleId, "{\"server_name\":\"FIXTURE-SERVER\"}");

        Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));
        Assert.Equal("FIXTURE-SERVER", store.Row(RuleId)!.ServerName);
    }

    /// <summary>
    /// The reported rule is read back AFTER the write, so a rule deleted in that window has no stored state
    /// to report. Both facts are named: the edit landed and the rule is gone.
    /// </summary>
    [Fact]
    public async Task ARuleDeletedBetweenTheWriteAndTheReadBack_ReportsNotFound_NamingTheWriteThatLanded()
    {
        var store = StoreWith();
        store.AfterUpdate = () => store.Remove(RuleId);

        var json = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, RuleId, "{\"reason\":\"edited\"}");

        Assert.Equal("not_found", DarlingMcpTestData.StatusOf(json));
        Assert.Equal(1, store.UpdateCalls);
        Assert.Contains("was updated", json, StringComparison.Ordinal);
        Assert.Contains("deleted concurrently", json, StringComparison.Ordinal);
    }

    /// <summary>
    /// A store fault is reported, not swallowed into a success. <see cref="IMuteRuleStore"/> requires every
    /// member to throw on failure precisely so this surface can tell the two apart.
    /// </summary>
    [Fact]
    public async Task AStoreFault_IsReportedAsAnError()
    {
        await using var dead = NpgsqlDataSource.Create(
            "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=1");

        var result = await DarlingMcpAlertTools.UpdateMuteRule(dead, RuleId, "{\"reason\":\"x\"}");

        /* The one error envelope (#3653 Q11) — see SetMuteRuleEnabled's twin above for what each field pins. */
        Assert.True(McpHelpers.IsErrorEnvelope(result), result);
        using var envelope = JsonDocument.Parse(result);
        Assert.Equal("error", envelope.RootElement.GetProperty("status").GetString());
        Assert.StartsWith("Error during update_mute_rule: ", envelope.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
        Assert.Equal("update_mute_rule", envelope.RootElement.GetProperty("hints").GetProperty("operation").GetString());
    }
}

/// <summary>
/// The web dashboard's mute-rule CREATE half (#3450): <c>CreateMuteRuleCore</c>, the JSON-body twin of the MCP
/// <c>create_mute_rule</c> tool, over the same <see cref="IMuteRuleStore"/> seam — what
/// <c>POST /api/mute-rules</c> calls with the raw request body. The load-bearing claims: the body is parsed by
/// the SAME whitelist authority the update verb runs (so a stray key, a blank, a bad expiry, and the four
/// non-editable keys are refused with the update path's own messages, and NOTHING is stored on a refusal); the
/// new rule is born enabled with a fresh clock; and the reported rule is the store's read-back, not the local
/// copy — proven on the arm where the two answers differ.
/// </summary>
public sealed class CreateMuteRuleCoreTests
{
    private static JsonNode Payload(string json) =>
        JsonNode.Parse(json)!["mute_rule"] ?? throw new InvalidOperationException($"no mute_rule in {json}");

    [Fact]
    public async Task AFullBody_CreatesAnEnabledRule_AndReportsTheStoredRow()
    {
        var store = new FakeMuteRuleStore();

        var result = await DarlingMcpAlertTools.CreateMuteRuleCore(store, /*lang=json*/ """
            {
              "server_name": "pm-server-1",
              "metric_name": "High CPU",
              "database_pattern": "sales",
              "query_text_pattern": "UPDATE big",
              "wait_type_pattern": "LCK",
              "job_name_pattern": "nightly",
              "reason": "  known load window  ",
              "expires_at_utc": "2026-12-01T00:00:00Z"
            }
            """);

        Assert.Equal("created", DarlingMcpTestData.StatusOf(result));

        var payload = Payload(result);
        var stored = store.Row((string)payload["id"]!)!;

        /* Born enabled with the caller's fields — whitespace trimmed exactly as the parser promises. */
        Assert.True(stored.Enabled);
        Assert.Equal("pm-server-1", stored.ServerName);
        Assert.Equal("known load window", stored.Reason);
        Assert.Equal("nightly", stored.JobNamePattern);
        Assert.Equal(new DateTime(2026, 12, 1, 0, 0, 0, DateTimeKind.Utc), stored.ExpiresAtUtc);

        /* The #3306 clock starts NOW — the store's value, which is also what the payload reports. */
        Assert.True((DateTime.UtcNow - stored.CreatedAtUtc).Duration() < TimeSpan.FromMinutes(1));
        Assert.True((bool)payload["enabled"]!);
        Assert.Equal(1, store.Count);
    }

    [Fact]
    public async Task AnEmptyObject_IsLegal_AndCreatesTheWholeFleetRule()
    {
        /* The MCP twin accepts an argument-less create (a rule with no constraining fields mutes EVERY
           alert); the web body's {} is the same request and gets the same answer — the warning lives on the
           surface's description, not in a refusal one twin makes and the other does not. */
        var store = new FakeMuteRuleStore();

        var result = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{}");

        Assert.Equal("created", DarlingMcpTestData.StatusOf(result));
        var stored = store.Row((string)Payload(result)["id"]!)!;
        Assert.True(stored.Enabled);
        Assert.Null(stored.ServerName);
        Assert.Null(stored.MetricName);
        Assert.Null(stored.ExpiresAtUtc);
    }

    [Theory]
    [InlineData("{\"reason\":\"\"}")]                       // blank refused (null is the one spelling of clear/omit)
    [InlineData("{\"reasn\":\"typo\"}")]                    // unknown key refused by the shared whitelist
    [InlineData("{\"enabled\":false}")]                     // the flag belongs to set_mute_rule_enabled / PUT .../enabled
    [InlineData("{\"id\":\"mine\"}")]                       // identity is generated, never supplied
    [InlineData("{\"created_at_utc\":\"2020-01-01T00:00:00Z\"}")] // the #3306 clock is never caller-set
    [InlineData("{\"expires_at_utc\":\"not-a-date\"}")]     // the same expiry parse the update path runs
    [InlineData("{\"expires_at\":\"2026-01-01T00:00:00Z\",\"expires_at_utc\":\"2026-01-01T00:00:00Z\"}")] // alias duplicate
    [InlineData("not json")]
    [InlineData("[1,2]")]                                   // a body that is not an object
    public async Task ARefusedBody_ReturnsInvalid_AndStoresNothing(string body)
    {
        var store = new FakeMuteRuleStore();

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.CreateMuteRuleCore(store, body)));
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task TheExpiresAtAlias_IsAccepted_AndReportsUnderTheCanonicalName()
    {
        /* create_mute_rule's parameter spelling, honored here through the SAME parser normalization. */
        var store = new FakeMuteRuleStore();

        var result = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"expires_at\":\"2026-06-01T00:00:00Z\"}");

        Assert.Equal("created", DarlingMcpTestData.StatusOf(result));
        Assert.Equal(new DateTime(2026, 6, 1, 0, 0, 0, DateTimeKind.Utc),
            store.Row((string)Payload(result)["id"]!)!.ExpiresAtUtc);
    }

    [Fact]
    public async Task ACreateWhoseRowVanishesBeforeTheReadBack_ReportsTheAbsence_NamingBothFacts()
    {
        /* The discriminating arm for "the reported rule is the store's read-back": a core that reported its
           local copy would answer 'created' here and hand the caller a rule that no longer exists. */
        var store = new FakeMuteRuleStore();
        store.AfterInsert = inserted => store.Remove(inserted.Id);

        var result = await DarlingMcpAlertTools.CreateMuteRuleCore(store, "{\"reason\":\"raced\"}");

        Assert.Equal("not_found", DarlingMcpTestData.StatusOf(result));
        Assert.Contains("was created", result, StringComparison.Ordinal);
        Assert.Contains("deleted concurrently", result, StringComparison.Ordinal);
    }
}

/// <summary>
/// delete_mute_rule's hoisted core (#3450), over the seam — what <c>DELETE /api/mute-rules/{id}</c> and the MCP
/// tool both run. The decisions are small and all here: an honest 'deleted' vs 'not_found' off the same store
/// read get_mute_rules uses, and a blank id refused before the store is touched.
/// </summary>
public sealed class DeleteMuteRuleCoreTests
{
    private const string RuleId = "rule-to-delete";

    private static FakeMuteRuleStore StoreWithRule() =>
        new FakeMuteRuleStore().Seed(new MuteRule { Id = RuleId, Reason = "doomed" });

    [Fact]
    public async Task DeletesAnExistingRule_AndNamesTheIdTheCallerShouldStopCiting()
    {
        var store = StoreWithRule();

        var result = await DarlingMcpAlertTools.DeleteMuteRuleCore(store, RuleId);

        Assert.Equal("deleted", DarlingMcpTestData.StatusOf(result));
        Assert.Equal(RuleId, (string)JsonNode.Parse(result)!["rule_id"]!);
        Assert.Equal(0, store.Count);
    }

    [Fact]
    public async Task AnUnknownId_ReturnsNotFound_AndDeletesNothing()
    {
        var store = StoreWithRule();

        Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.DeleteMuteRuleCore(store, "no-such")));
        Assert.Equal(1, store.Count);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    public async Task ABlankId_ReturnsInvalid_WithoutReading(string ruleId)
    {
        var store = StoreWithRule();

        Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.DeleteMuteRuleCore(store, ruleId)));
        Assert.Equal(0, store.LoadAllCalls);
        Assert.Equal(1, store.Count);
    }
}

/// <summary>
/// The whole web write surface (#3450) driven end-to-end through the EXACT core seam the
/// <c>/api/mute-rules</c> routes call, with the raw JSON bodies the routes pass — create, then a partial edit,
/// then an explicit-null clear, then the flag both ways — asserting the one invariant the issue pins across
/// every path: <c>created_at_utc</c> never moves. This is the #3306 clock; a surface that reset it would hand a
/// weekly edit the same staleness invisibility a delete-and-recreate buys.
/// </summary>
public sealed class WebMuteRuleEndpointFlowTests
{
    [Fact]
    public async Task CreateEditClearAndFlip_NeverMoveTheCreationDate()
    {
        var store = new FakeMuteRuleStore();

        /* POST /api/mute-rules */
        var created = await DarlingMcpAlertTools.CreateMuteRuleCore(store,
            "{\"metric_name\":\"High CPU\",\"reason\":\"initial\",\"expires_at_utc\":\"2026-12-01T00:00:00Z\"}");
        Assert.Equal("created", DarlingMcpTestData.StatusOf(created));
        var id = (string)JsonNode.Parse(created)!["mute_rule"]!["id"]!;
        var born = store.Row(id)!.CreatedAtUtc;

        /* PATCH /api/mute-rules/{id} — a partial edit: only the sent field moves. */
        var edited = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, "{\"reason\":\"root cause found\"}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(edited));
        Assert.Equal("root cause found", store.Row(id)!.Reason);
        Assert.Equal("High CPU", store.Row(id)!.MetricName);   // a field not sent does not change
        Assert.Equal(born, store.Row(id)!.CreatedAtUtc);

        /* PATCH again — the explicit-null clear: the rule becomes permanent, the clock still stands. */
        var cleared = await DarlingMcpAlertTools.UpdateMuteRuleCore(store, id, "{\"expires_at_utc\":null}");
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(cleared));
        Assert.Null(store.Row(id)!.ExpiresAtUtc);
        Assert.Equal(born, store.Row(id)!.CreatedAtUtc);

        /* PUT /api/mute-rules/{id}/enabled — both directions. */
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, id, false)));
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.SetMuteRuleEnabledCore(store, id, true)));
        Assert.Equal(born, store.Row(id)!.CreatedAtUtc);

        /* DELETE /api/mute-rules/{id} closes the loop. */
        Assert.Equal("deleted", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.DeleteMuteRuleCore(store, id)));
        Assert.Equal(0, store.Count);
    }

    /// <summary>
    /// #3450's carried caveat, surface-tested as the issue asked: alert rows spell server_name two ways
    /// (the self-alert family uses the display-short name, engine alerts the registry name), and a mute
    /// rule matches the row's exact spelling — so the web write surface must store WHATEVER spelling it
    /// was given, verbatim. A normalization here — case folding, registry resolution, trimming a domain —
    /// would silently move a rule from one family's spelling to the other and turn a working mute inert,
    /// which is precisely the operational trap the caveat documents. Both spellings round-trip through
    /// create and through an update that swaps between them; equality is Ordinal because that is the
    /// matcher's own comparison.
    /// </summary>
    [Fact]
    public async Task TheTwoServerNameSpellings_StoreVerbatim_NeitherNormalized()
    {
        var store = new FakeMuteRuleStore();
        const string shortSpelling = "pm-server-7";
        const string fullSpelling = "pm-server-7.fleet.example.test";

        var fromShort = await DarlingMcpAlertTools.CreateMuteRuleCore(store,
            "{\"server_name\":\"" + shortSpelling + "\",\"metric_name\":\"Collector Cost Regression\",\"reason\":\"short spelling\"}");
        Assert.Equal("created", DarlingMcpTestData.StatusOf(fromShort));
        var shortId = (string)JsonNode.Parse(fromShort)!["mute_rule"]!["id"]!;
        Assert.Equal(shortSpelling, store.Row(shortId)!.ServerName);

        var fromFull = await DarlingMcpAlertTools.CreateMuteRuleCore(store,
            "{\"server_name\":\"" + fullSpelling + "\",\"metric_name\":\"Blocking Detected\",\"reason\":\"full spelling\"}");
        Assert.Equal("created", DarlingMcpTestData.StatusOf(fromFull));
        var fullId = (string)JsonNode.Parse(fromFull)!["mute_rule"]!["id"]!;
        Assert.Equal(fullSpelling, store.Row(fullId)!.ServerName);

        /* The update endpoint is where the caveat bites hardest — an operator repointing a rule between
           the two families must get the exact bytes they sent, both directions. */
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, shortId, "{\"server_name\":\"" + fullSpelling + "\"}")));
        Assert.Equal(fullSpelling, store.Row(shortId)!.ServerName);
        Assert.Equal("updated", DarlingMcpTestData.StatusOf(
            await DarlingMcpAlertTools.UpdateMuteRuleCore(store, shortId, "{\"server_name\":\"" + shortSpelling + "\"}")));
        Assert.Equal(shortSpelling, store.Row(shortId)!.ServerName);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the alert tools. The READ test plants an alert-log row, seeds the
/// single alert-settings row, and plants a mute rule, then asserts each read surfaces its data. The WRITE test
/// proves update_alert_settings flips a threshold AND self-bumps config_version (the reload beacon), and that
/// create_mute_rule → get_mute_rules → delete_mute_rule round-trips. A third drives set_mute_rule_enabled both
/// directions and asserts the row's created_at_utc survives every transition (#3432). A fourth drives
/// update_mute_rule through a set, a partial edit and an explicit-null clear and asserts the same clock never
/// moves (#3450). All four connect as the DARLING_TEST_PG owner (a THROWAWAY dev Postgres) and are own-scoped /
/// restore what they touch, so a shared store is left as it was.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingMcpAlertToolsLivePostgresTests
{
    private const string ServerName = "darling-mcp-alerts-e2e";
    private static readonly int ServerId = ServerIdHelper.GetDeterministicHashCode(ServerName);
    private const string MuteRuleId = "darling-mcp-alerts-e2e-rule";
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AlertTools_ReadPlantedRows_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);
            var when = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow).AddMinutes(-5);

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                when, ServerId, ServerName, "High CPU", 92.5, 80.0, true, "email", null, false, "CPU sustained above threshold");

            /* #3539 A8e: a Poison Wait row that FIRED Warning, with the tier persisted the way both SKUs'
               deliverers persist it (the serializer's Severity member), and a legacy Deadlocks row carrying
               no context at all. */
            var gradedContext = new AlertContext { SeverityOverride = AlertSeverityLevel.Warning };
            gradedContext.Details.Add(new AlertDetailItem { Heading = "THREADPOOL" });
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text, context_json)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                when.AddMinutes(-1), ServerId, ServerName, "Poison Wait", 61000.0, 60000.0, true, "webhook", null, false, "THREADPOOL", AlertContextSerializer.Serialize(gradedContext));
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO config_alert_log (alert_time, server_id, server_name, metric_name, current_value, threshold_value, alert_sent, notification_type, send_error, muted, detail_text)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                when.AddMinutes(-2), ServerId, ServerName, "Deadlocks Detected", 1.0, 1.0, true, "email", null, false, null);

            /* Seed the single global settings row — every column has a default, so id alone suffices.
               BOTH singletons, because #3314 made get_alert_settings read the delivery cooldown off
               config_notification: the service seeds the two in one pass, and the tool reports `unavailable`
               rather than a fabricated default when either is missing. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO config_mute_rules (id, enabled, created_at_utc, expires_at_utc, reason, server_name, metric_name, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                MuteRuleId, true, DarlingMcpTestData.Naive(DateTime.UtcNow), null, "e2e rule", ServerName, "High CPU", null, null, null, null);

            /* Alert history — server-scoped + fleet-wide both surface the planted alert. */
            var scoped = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(scoped, ServerName, "alerts");
            Assert.Contains("High CPU", scoped, StringComparison.Ordinal);
            /* #3541 A3: three planted, undismissed rows — the page says so, says the filter applied and hid
               nothing, and carries no `total_` key. */
            JsonAssert.Contains("\"alerts_returned\": 3", scoped);
            JsonAssert.Contains("\"truncated\": false", scoped);
            JsonAssert.Contains("\"dismissed_excluded\": true", scoped);
            JsonAssert.Contains("\"dismissed_excluded_count\": 0", scoped);
            Assert.DoesNotContain("total_alerts", scoped, StringComparison.Ordinal);

            /* #3539 A8e: the tier the alert FIRED at, per row, and where it came from. The Poison Wait row
               reads the Warning it fired at off its context ("fired") — not the red its name implies — while
               the two rows with no context are classified by name and say so. Asserted on the row objects
               rather than by substring, so a "warning" from one row cannot satisfy a pin about another. */
            using (var page = JsonDocument.Parse(scoped))
            {
                var byMetric = page.RootElement.GetProperty("alerts").EnumerateArray()
                    .ToDictionary(a => a.GetProperty("metric_name").GetString()!, a => a);
                Assert.Equal("warning", byMetric["Poison Wait"].GetProperty("severity").GetString());
                Assert.Equal(AlertHistoryRowSeverity.SourceFired, byMetric["Poison Wait"].GetProperty("severity_source").GetString());
                Assert.Equal("critical", byMetric["Deadlocks Detected"].GetProperty("severity").GetString());
                Assert.Equal(AlertHistoryRowSeverity.SourceMetricName, byMetric["Deadlocks Detected"].GetProperty("severity_source").GetString());
                Assert.Equal("warning", byMetric["High CPU"].GetProperty("severity").GetString());
                Assert.Equal(AlertHistoryRowSeverity.SourceMetricName, byMetric["High CPU"].GetProperty("severity_source").GetString());
            }

            var fleet = await DarlingMcpAlertTools.GetAlertHistory(postgres);
            Assert.False(McpHelpers.IsErrorEnvelope(fleet), fleet);
            Assert.Contains("(all servers)", fleet, StringComparison.Ordinal);
            Assert.Contains("High CPU", fleet, StringComparison.Ordinal);

            /* Alert settings — the seeded row round-trips its default thresholds. */
            var settings = await DarlingMcpAlertTools.GetAlertSettings(postgres);
            Assert.False(McpHelpers.IsErrorEnvelope(settings), settings);
            Assert.Contains("threshold_percent", settings, StringComparison.Ordinal);
            Assert.Contains("delivery", settings, StringComparison.Ordinal);

            /* Mute rules — the planted rule surfaces. */
            var mutes = await DarlingMcpAlertTools.GetMuteRules(postgres);
            Assert.False(McpHelpers.IsErrorEnvelope(mutes), mutes);
            Assert.Contains(MuteRuleId, mutes, StringComparison.Ordinal);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, cleanupCt));
        }
    }

    [Fact]
    public async Task AlertWriteTools_TuneSettings_AndMuteRuleRoundTrip_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live alert-write-tools test.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        /* Seed the two singleton rows the writes touch (a no-op if they already exist on a shared store). */
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_service (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING");
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        var originalThreshold = Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT cpu_threshold_percent FROM config_alert_settings WHERE id = 1"));
        /* #3314: captured for the same reason as the threshold — these two are SINGLETONS the whole store
           shares, and the delivery cooldown now governs channel volume for every later test and every later
           run on a reused database. */
        var originalFireCooldown = Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT cooldown_minutes FROM config_alert_settings WHERE id = 1"));
        var originalDeliveryCooldown = Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT email_cooldown_minutes FROM config_notification WHERE id = 1"));
        /* #3712 (V137): the route knob's store half is a singleton tri-state the whole store shares too, and the
           only one of these whose ORIGINAL is expected to be NULL (the upgrade-day state) — captured as the raw
           object so the restore can put NULL back as NULL rather than as a default. */
        var originalRoute = await ScalarAsync(connection, ct, "SELECT analysis_uncorroborated_route FROM config_alert_settings WHERE id = 1");
        var versionBefore = Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT config_version FROM config_service WHERE id = 1"));
        var newThreshold = originalThreshold == 91 ? 71 : 91;                 // a distinct, in-range value
        var muteTag = "mcp_alert_write_e2e_" + Guid.NewGuid().ToString("N");  // own-scoped cleanup tag

        var bodySucceeded = false;
        try
        {
            /* update_alert_settings — a PARTIAL update flips ONE threshold; the response echoes the full new settings. */
            var updated = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, $"{{\"cpu\":{{\"threshold_percent\":{newThreshold}}}}}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(updated));
            using (var doc = JsonDocument.Parse(updated))
            {
                Assert.Equal(newThreshold, doc.RootElement.GetProperty("settings").GetProperty("cpu").GetProperty("threshold_percent").GetInt32());
                Assert.Contains("cpu_threshold_percent", doc.RootElement.GetProperty("updated_fields").EnumerateArray().Select(e => e.GetString()));
            }

            /* The store row actually changed, and the config-table trigger self-bumped config_version (the service's
               reload beacon) — so the running service hot-reloads the change within one sweep. */
            Assert.Equal(newThreshold, Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT cpu_threshold_percent FROM config_alert_settings WHERE id = 1")));
            var versionAfter = Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT config_version FROM config_service WHERE id = 1"));
            Assert.True(versionAfter > versionBefore, "config_version should self-bump on a config_alert_settings write");

            /* #3314: the delivery cooldown round-trips THROUGH THE STORE, and through the OTHER table. The
               shape pins prove the parser routes it; only this proves the two-table write executes, that the
               value lands in config_notification, and that a read straight afterwards reports what was
               written. Both spellings are exercised, since the alias exists so an existing config keeps
               working and an alias nobody writes with is an alias nobody has tested. */
            foreach (var (body, expected) in new[]
            {
                ("{\"delivery\":{\"cooldown_minutes\":37}}", 37),
                ("{\"email_cooldown_minutes\":41}", 41),
            })
            {
                var cooldown = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, body);
                Assert.Equal("updated", DarlingMcpTestData.StatusOf(cooldown));
                using var doc = JsonDocument.Parse(cooldown);
                Assert.Equal(expected, doc.RootElement.GetProperty("settings").GetProperty("delivery").GetProperty("cooldown_minutes").GetInt32());
                Assert.Contains("email_cooldown_minutes", doc.RootElement.GetProperty("updated_fields").EnumerateArray().Select(e => e.GetString()));
                Assert.Equal(expected, Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT email_cooldown_minutes FROM config_notification WHERE id = 1")));
            }

            /* One body spanning BOTH tables: two UPDATE statements in one transaction, and both land. A
               partial application is the failure this transaction exists to prevent, and a same-table-only
               body could never expose it. */
            var spanning = await DarlingMcpAlertTools.UpdateAlertSettings(
                postgres, $"{{\"cooldown_minutes\":7,\"delivery\":{{\"cooldown_minutes\":53}}}}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(spanning));
            Assert.Equal(7, Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT cooldown_minutes FROM config_alert_settings WHERE id = 1")));
            Assert.Equal(53, Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT email_cooldown_minutes FROM config_notification WHERE id = 1")));

            /* Out of range on either spelling, and both spellings at once, write NOTHING. */
            foreach (var rejected in new[]
            {
                "{\"delivery\":{\"cooldown_minutes\":121}}",
                "{\"email_cooldown_minutes\":0}",
                "{\"email_cooldown_minutes\":60,\"delivery\":{\"cooldown_minutes\":60}}",
            })
            {
                Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres, rejected)));
                Assert.Equal(53, Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT email_cooldown_minutes FROM config_notification WHERE id = 1")));
            }

            /* An unknown field writes NOTHING (validated before the UPDATE). */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres, "{\"cpu\":{\"bogus\":1}}")));

            /* #3712 (V137): the route knob round-trips THROUGH THE STORE in all three states. 'Page' lands as the
               canonical 'page' the CHECK admits and the read-back says the STORE decided; null CLEARS the column
               to SQL NULL and the read-back falls to the file half — 'default' here, because this harness has
               published no file value, which is the honest word for it; a misspelling is refused before the
               UPDATE (the tool's invalid arm, not the CHECK's 23514) and the column is untouched. */
            DarlingFileLevelAlertSettings.ResetForTests();
            var routed = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, "{\"analysis\":{\"uncorroborated_route\":\"Page\"}}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(routed));
            using (var doc = JsonDocument.Parse(routed))
            {
                var analysis = doc.RootElement.GetProperty("settings").GetProperty("analysis");
                Assert.Equal("page", analysis.GetProperty("uncorroborated_route").GetString());
                Assert.Equal(DarlingAlertSettings.RouteSourceStore, analysis.GetProperty("uncorroborated_route_source").GetString());
                Assert.Contains("analysis_uncorroborated_route", doc.RootElement.GetProperty("updated_fields").EnumerateArray().Select(e => e.GetString()));
                Assert.Equal(0, doc.RootElement.GetProperty("warnings").GetArrayLength());
            }
            Assert.Equal("page", (string)(await ScalarAsync(connection, ct, "SELECT analysis_uncorroborated_route FROM config_alert_settings WHERE id = 1"))!);

            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres, "{\"analysis\":{\"uncorroborated_route\":\"pgae\"}}")));
            Assert.Equal("page", (string)(await ScalarAsync(connection, ct, "SELECT analysis_uncorroborated_route FROM config_alert_settings WHERE id = 1"))!);

            var cleared = await DarlingMcpAlertTools.UpdateAlertSettings(postgres, "{\"analysis\":{\"uncorroborated_route\":null}}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(cleared));
            using (var doc = JsonDocument.Parse(cleared))
            {
                var analysis = doc.RootElement.GetProperty("settings").GetProperty("analysis");
                Assert.Equal("digest", analysis.GetProperty("uncorroborated_route").GetString());
                Assert.Equal(DarlingAlertSettings.RouteSourceDefault, analysis.GetProperty("uncorroborated_route_source").GetString());
                Assert.Contains("analysis_uncorroborated_route", doc.RootElement.GetProperty("updated_fields").EnumerateArray().Select(e => e.GetString()));
            }
            Assert.Equal(DBNull.Value, await ScalarAsync(connection, ct, "SELECT analysis_uncorroborated_route FROM config_alert_settings WHERE id = 1"));

            /* create_mute_rule → get_mute_rules → delete_mute_rule round-trip (own-scoped by the GUID reason tag). */
            var created = await DarlingMcpAlertTools.CreateMuteRule(postgres, server_name: "e2e-write-server", metric_name: "High CPU", reason: muteTag);
            Assert.Equal("created", DarlingMcpTestData.StatusOf(created));
            string ruleId;
            using (var doc = JsonDocument.Parse(created))
            {
                var rule = doc.RootElement.GetProperty("mute_rule");
                ruleId = rule.GetProperty("id").GetString()!;
                Assert.False(string.IsNullOrWhiteSpace(ruleId));
                Assert.Equal("High CPU", rule.GetProperty("metric_name").GetString());
            }

            Assert.Contains(ruleId, await DarlingMcpAlertTools.GetMuteRules(postgres), StringComparison.Ordinal);

            Assert.Equal("deleted", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.DeleteMuteRule(postgres, ruleId)));
            Assert.DoesNotContain(ruleId, await DarlingMcpAlertTools.GetMuteRules(postgres), StringComparison.Ordinal);
            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.DeleteMuteRule(postgres, ruleId)));

            bodySucceeded = true;
        }
        finally
        {
            /* Restore the singleton threshold and drop any leftover test mute rule (own-scoped by the GUID reason).

               The RESTORE is why this teardown matters more than most (#1902): config_alert_settings is a
               SINGLETON the whole store shares, so abandoning this leaves every later test — and every later
               run on a reused database — reading a CPU threshold this test invented. */
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "UPDATE config_alert_settings SET cpu_threshold_percent = $1, cooldown_minutes = $2 WHERE id = 1", originalThreshold, originalFireCooldown);
                /* #3712: NULL back as NULL — ExecAsync binds a null argument as DBNull, and a store that read NULL
                   before this test must read NULL after it, or every later test sees a route this test chose. */
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "UPDATE config_alert_settings SET analysis_uncorroborated_route = $1 WHERE id = 1", originalRoute is DBNull ? null : originalRoute);
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "UPDATE config_notification SET email_cooldown_minutes = $1 WHERE id = 1", originalDeliveryCooldown);
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM config_mute_rules WHERE reason = $1", muteTag);
            });
        }
    }

    /// <summary>How far back the round-trip below authors its rule. Days rather than minutes so a reset to
    /// "now" is a visibly different value rather than a rounding difference.</summary>
    private const int PlantedAgeDays = 90;

    /// <summary>
    /// #3432 against the real store: <c>set_mute_rule_enabled</c> moves the flag and nothing else, in both
    /// directions, on the SHIPPED SQL rather than on a fixture of it.
    ///
    /// <para>This is the arm no in-memory pin reaches. Those hold the tool's decisions against an
    /// <c>IMuteRuleStore</c> fixture and hold <see cref="PgMuteRuleStore"/>'s statements as TEXT; only a real
    /// round trip fails when the statement itself starts writing <c>created_at_utc</c>, which is the column
    /// the Stale Mute Rules self-alert (#3306) ages a rule from.</para>
    ///
    /// <para>Own-scoped by a GUID reason tag, so a shared store is left as it was.</para>
    /// </summary>
    [Fact]
    public async Task SetMuteRuleEnabled_MovesOnlyTheFlag_BothDirections_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-enable round-trip.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var muteTag = "mcp_mute_enable_e2e_" + Guid.NewGuid().ToString("N");
        var ruleId = muteTag;
        /* Truncated to the microsecond the `timestamp` column keeps, so the read back compares equal for the
           reason it is being asserted rather than surviving a tolerance. */
        var authored = DateTime.UtcNow.AddDays(-PlantedAgeDays);
        var planted = DarlingMcpTestData.Naive(new DateTime(authored.Ticks - (authored.Ticks % 10), DateTimeKind.Utc));

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO config_mute_rules (id, enabled, created_at_utc, expires_at_utc, reason, server_name, metric_name, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                ruleId, true, planted, null, muteTag, "e2e-mute-enable-server", "High CPU", null, null, null, null);

            /* Off, back on, and off again: the second disable proves the round trip is repeatable rather than
               a first transition that happens to work. */
            foreach (var target in new[] { false, true, false })
            {
                var json = await DarlingMcpAlertTools.SetMuteRuleEnabled(postgres, ruleId, target);
                Assert.Equal("updated", DarlingMcpTestData.StatusOf(json));

                using (var doc = JsonDocument.Parse(json))
                {
                    var rule = doc.RootElement.GetProperty("mute_rule");
                    Assert.Equal(target, rule.GetProperty("enabled").GetBoolean());
                    Assert.Equal(planted.Ticks, rule.GetProperty("created_at_utc").GetDateTime().Ticks);
                    /* The scope and the reason are the rule's identity to an operator, and a re-create would
                       have had to restate them. */
                    Assert.Equal("High CPU", rule.GetProperty("metric_name").GetString());
                    Assert.Equal(muteTag, rule.GetProperty("reason").GetString());
                }

                /* And the ROW, read outside the tool: what the wire reported is what is stored. */
                Assert.Equal(target, Convert.ToBoolean(await ScalarAsync(connection, ct, "SELECT enabled FROM config_mute_rules WHERE id = $1", ruleId)));
                Assert.Equal(planted.Ticks, ((DateTime)(await ScalarAsync(connection, ct, "SELECT created_at_utc FROM config_mute_rules WHERE id = $1", ruleId))!).Ticks);

                /* Repeating the same value writes nothing and says so. */
                Assert.Equal("unchanged", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.SetMuteRuleEnabled(postgres, ruleId, target)));
                Assert.Equal(planted.Ticks, ((DateTime)(await ScalarAsync(connection, ct, "SELECT created_at_utc FROM config_mute_rules WHERE id = $1", ruleId))!).Ticks);
            }

            /* One row throughout — the id never changed, so nothing citing it broke. */
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT COUNT(*) FROM config_mute_rules WHERE id = $1", ruleId)));

            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(
                await DarlingMcpAlertTools.SetMuteRuleEnabled(postgres, ruleId + "-absent", true)));

            /* delete_mute_rule still works on a rule this tool has been toggling. */
            Assert.Equal("deleted", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.DeleteMuteRule(postgres, ruleId)));
            Assert.Equal(0L, Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT COUNT(*) FROM config_mute_rules WHERE id = $1", ruleId)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM config_mute_rules WHERE reason = $1", muteTag));
        }
    }

    /// <summary>
    /// #3450 against the real store: <c>update_mute_rule</c> edits in place — a set, a partial edit and an
    /// explicit-null clear — and <c>created_at_utc</c> never moves, on the SHIPPED SQL rather than on a
    /// fixture of it.
    ///
    /// <para>This is the arm no in-memory pin reaches, for the reason the enable verb's live test gives: the
    /// fixture holds the tool's decisions and a source-text pin holds <see cref="PgMuteRuleStore"/>'s
    /// statements as TEXT; only a real round trip fails when the full-row UPDATE itself starts writing
    /// <c>created_at_utc</c> — the column the Stale Mute Rules self-alert (#3306) ages a rule from, and the
    /// one every edit used to reset via delete/recreate.</para>
    ///
    /// <para>Own-scoped by its rule id, which — unlike the enable test's reason tag — is the one column an
    /// edit cannot change, so cleanup finds the row whatever the edits did to it.</para>
    /// </summary>
    [Fact]
    public async Task UpdateMuteRule_EditsInPlace_AndNeverMovesTheCreationDate_AgainstDevPostgres()
    {
        var cs = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the live mute-edit round-trip.");

        var ct = TestContext.Current.CancellationToken;
        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var postgres = NpgsqlDataSource.Create(cs!);

        var ruleId = "mcp_mute_edit_e2e_" + Guid.NewGuid().ToString("N");
        /* Truncated to the microsecond the `timestamp` column keeps, so the read back compares equal for the
           reason it is being asserted rather than surviving a tolerance. */
        var authored = DateTime.UtcNow.AddDays(-PlantedAgeDays);
        var planted = DarlingMcpTestData.Naive(new DateTime(authored.Ticks - (authored.Ticks % 10), DateTimeKind.Utc));
        var boundSource = DateTime.UtcNow.AddDays(30);
        var bound = DarlingMcpTestData.Naive(new DateTime(boundSource.Ticks - (boundSource.Ticks % 10), DateTimeKind.Utc));

        var bodySucceeded = false;
        try
        {
            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO config_mute_rules (id, enabled, created_at_utc, expires_at_utc, reason, server_name, metric_name, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                ruleId, true, planted, null, "authored reason", "e2e-mute-edit-server", "High CPU", null, null, null, "nightly-etl");

            /* A partial edit: reason moves, the untouched job pattern and server scope do not. */
            var edited = await DarlingMcpAlertTools.UpdateMuteRule(postgres, ruleId, "{\"reason\":\"root cause found\"}");
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(edited));
            using (var doc = JsonDocument.Parse(edited))
            {
                var rule = doc.RootElement.GetProperty("mute_rule");
                Assert.Equal("root cause found", rule.GetProperty("reason").GetString());
                Assert.Equal("nightly-etl", rule.GetProperty("job_name_pattern").GetString());
                Assert.Equal("e2e-mute-edit-server", rule.GetProperty("server_name").GetString());
                Assert.Equal(planted.Ticks, rule.GetProperty("created_at_utc").GetDateTime().Ticks);
                Assert.Equal("reason", doc.RootElement.GetProperty("updated_fields").EnumerateArray().Single().GetString());
            }

            /* An expiry SET, then an explicit-null CLEAR — the direction only a full-row UPDATE can express,
               and the case that used to force delete/recreate ("make it permanent again"). */
            Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateMuteRule(
                postgres, ruleId, $"{{\"expires_at_utc\":\"{DateTime.SpecifyKind(bound, DateTimeKind.Utc):O}\"}}")));
            Assert.Equal(bound.Ticks, ((DateTime)(await ScalarAsync(connection, ct, "SELECT expires_at_utc FROM config_mute_rules WHERE id = $1", ruleId))!).Ticks);

            Assert.Equal("updated", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateMuteRule(
                postgres, ruleId, "{\"expires_at_utc\":null}")));
            Assert.Equal(DBNull.Value, await ScalarAsync(connection, ct, "SELECT expires_at_utc FROM config_mute_rules WHERE id = $1", ruleId));

            /* The ROW's clock, read outside the tool, after every transition above: never moved. */
            Assert.Equal(planted.Ticks, ((DateTime)(await ScalarAsync(connection, ct, "SELECT created_at_utc FROM config_mute_rules WHERE id = $1", ruleId))!).Ticks);

            /* Repeating the same values writes nothing and says so. */
            Assert.Equal("unchanged", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateMuteRule(
                postgres, ruleId, "{\"reason\":\"root cause found\",\"expires_at_utc\":null}")));

            /* One row throughout — the id never changed, so nothing citing it broke. */
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(connection, ct, "SELECT COUNT(*) FROM config_mute_rules WHERE id = $1", ruleId)));

            Assert.Equal("not_found", DarlingMcpTestData.StatusOf(
                await DarlingMcpAlertTools.UpdateMuteRule(postgres, ruleId + "-absent", "{\"reason\":\"x\"}")));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM config_mute_rules WHERE id = $1", ruleId));
        }
    }

    private static async Task<object?> ScalarAsync(
        NpgsqlConnection connection, CancellationToken ct, string sql, params object?[] args)
    {
        using var command = new NpgsqlCommand(sql, connection);
        foreach (var arg in args)
        {
            command.Parameters.Add(new NpgsqlParameter { Value = arg ?? (object)DBNull.Value });
        }

        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var sql = $"DELETE FROM config_alert_log WHERE server_id = {ServerId};"
            + $" DELETE FROM config_mute_rules WHERE id = '{MuteRuleId}';"
            + $" DELETE FROM servers WHERE server_id = {ServerId};";
        using var cleanup = new NpgsqlCommand(sql, connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
