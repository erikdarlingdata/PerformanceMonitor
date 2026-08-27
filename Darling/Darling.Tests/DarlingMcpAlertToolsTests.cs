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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader;

namespace Darling.Tests;

/// <summary>
/// Pins the alerts MCP slice — the three READS (get_alert_history, get_alert_settings, get_mute_rules) plus the
/// three Darling-only WRITES (update_alert_settings, create_mute_rule, delete_mute_rule) over the Postgres store.
/// Ungated: the tool surface is EXACTLY the six names (all static, on a [McpServerToolType] class, returning
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
        "get_alert_history",
        "get_alert_settings",
        "get_mute_rules",
        "update_alert_settings",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpAlertTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheSixAlertTools()
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
    [InlineData("get_alert_history", "server_name,hours_back,limit,as_of")]
    [InlineData("get_mute_rules", "enabled_only")]
    [InlineData("update_alert_settings", "settings_json")]
    [InlineData("create_mute_rule", "server_name,metric_name,database_pattern,query_text_pattern,wait_type_pattern,job_name_pattern,reason,expires_at")]
    [InlineData("delete_mute_rule", "rule_id")]
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
    public void ParamContract_WriteTools_RequireTheirTarget(string toolName, string requiredCsv)
    {
        var required = McpParams(toolName).Where(p => !p.Optional).Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal).ToArray();
        Assert.Equal(requiredCsv.Split(',').OrderBy(n => n, StringComparer.Ordinal).ToArray(), required);
    }

    /* ---------------- read SQL pins ---------------- */

    [Fact]
    public void AlertHistorySql_ReadsLog_ExcludesDismissed_ServerScoped()
    {
        var sql = Reader.AlertHistorySql;
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("dismissed = FALSE", sql, StringComparison.Ordinal);
        /* #2495: BOTH window edges are bound, so server_id and the cap moved up one ordinal each. */
        Assert.Contains("alert_time >= $1", sql, StringComparison.Ordinal);
        Assert.Contains("alert_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("server_id = $3", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY alert_time DESC", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $4", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void AlertHistoryAllServersSql_ReadsLog_ExcludesDismissed_NoServerFilter()
    {
        var sql = Reader.AlertHistoryAllServersSql;
        Assert.Contains("FROM config_alert_log", sql, StringComparison.Ordinal);
        Assert.Contains("dismissed = FALSE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("server_id =", sql, StringComparison.Ordinal);   /* fleet-wide */
        Assert.Contains("alert_time <= $2", sql, StringComparison.Ordinal);       /* #2495 upper edge */
        Assert.Contains("LIMIT $3", sql, StringComparison.Ordinal);
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
    /// </summary>
    [Fact]
    public void EveryColumnRead_IsEmittedByThePayload_AndAcceptedByTheWriter()
    {
        var (columns, error) = ParseAsPartialUpdate(SerializedSettingsPayload(SampleSettingsRow()));

        /* Every key the payload emits is accepted. The parse stops at the FIRST rejection, so a non-null
           error here names the exact key update_alert_settings would refuse from its own read. */
        Assert.Null(error);

        /* Each accepted key claimed its own column — two keys sharing one would make whichever lost the
           parse order a silent no-op, with the caller told both were updated. */
        Assert.Equal(columns.Count, columns.Distinct(StringComparer.Ordinal).Count());

        Assert.Equal(
            SelectedAlertSettingsColumns().OrderBy(c => c, StringComparer.Ordinal).ToArray(),
            columns.OrderBy(c => c, StringComparer.Ordinal).ToArray());
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
    private static IReadOnlyList<string> SelectedAlertSettingsColumns()
    {
        var sql = Reader.AlertSettingsSelectSql;
        var select = sql[(sql.IndexOf("SELECT", StringComparison.Ordinal) + 6)..
                          sql.IndexOf("FROM config_alert_settings", StringComparison.Ordinal)];
        return select.Split(',', StringSplitOptions.RemoveEmptyEntries)
            .Select(c => c.Trim())
            .Where(c => c.Length > 0)
            .ToList();
    }

    /// <summary>get_alert_settings' payload for one row, serialized through the SAME options the tool
    /// serializes with and re-parsed. Runtime rather than source-parsing for the reason Lite's
    /// <c>McpAlertSettingsKeyTests</c> gives: the C# identifier is not automatically the wire key, so only
    /// serializing proves what a client receives — and therefore what it would hand back.</summary>
    private static JsonObject SerializedSettingsPayload(Reader.AlertSettingsReadRow row)
    {
        var build = typeof(DarlingMcpAlertTools).GetMethod(
            "BuildAlertSettingsPayload", BindingFlags.NonPublic | BindingFlags.Static)!;
        var payload = build.Invoke(null, new object[] { row })!;
        var json = JsonSerializer.Serialize(payload, payload.GetType(), McpHelpers.JsonOptions);
        return (JsonObject)JsonNode.Parse(json)!;
    }

    /// <summary>Runs a body through the tool's REAL partial-update parser and reports the columns it would
    /// write plus the first validation error, if any.</summary>
    private static (IReadOnlyList<string> Columns, string? Error) ParseAsPartialUpdate(JsonObject body)
    {
        var build = typeof(DarlingMcpAlertTools).GetMethod(
            "BuildAlertSettingsUpdate", BindingFlags.NonPublic | BindingFlags.Static)!;
        var result = build.Invoke(null, new object[] { body })!;
        var type = result.GetType();
        var updates = (IEnumerable<(string Column, NpgsqlParameter Param)>)type.GetField("Item1")!.GetValue(result)!;
        return (updates.Select(u => u.Column).ToList(), (string?)type.GetField("Item2")!.GetValue(result));
    }

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
        FileGrowthLookbackMinutes: 60);

    private static string ReadRepoFile(
        string relative, [System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        for (var dir = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(thisFile)!);
             dir is not null; dir = dir.Parent)
        {
            var candidate = System.IO.Path.Combine(dir.FullName, relative);
            if (System.IO.File.Exists(candidate)) return System.IO.File.ReadAllText(candidate);
        }

        throw new System.IO.FileNotFoundException($"Could not locate {relative}");
    }

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
    [InlineData(nameof(Reader.AlertSettingsSelectSql))]
    public void Reads_ArePostgresDialect_NoTsqlIsms(string sqlName)
    {
        var sql = sqlName switch
        {
            nameof(Reader.AlertHistorySql) => Reader.AlertHistorySql,
            nameof(Reader.AlertHistoryAllServersSql) => Reader.AlertHistoryAllServersSql,
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
    public void AdvertisedSchema_IsGeminiClean_ForAllSixTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(6, tools.Count);
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
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the alert tools. The READ test plants an alert-log row, seeds the
/// single alert-settings row, and plants a mute rule, then asserts each read surfaces its data. The WRITE test
/// proves update_alert_settings flips a threshold AND self-bumps config_version (the reload beacon), and that
/// create_mute_rule → get_mute_rules → delete_mute_rule round-trips. Both connect as the DARLING_TEST_PG owner
/// (a THROWAWAY dev Postgres) and are own-scoped / restore what they touch, so a shared store is left as it was.
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

            /* Seed the single global settings row — every column has a default, so id alone suffices. */
            await DarlingMcpTestData.ExecAsync(connection, ct,
                "INSERT INTO config_alert_settings (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

            await DarlingMcpTestData.ExecAsync(connection, ct,
                @"INSERT INTO config_mute_rules (id, enabled, created_at_utc, expires_at_utc, reason, server_name, metric_name, database_pattern, query_text_pattern, wait_type_pattern, job_name_pattern)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
                MuteRuleId, true, DarlingMcpTestData.Naive(DateTime.UtcNow), null, "e2e rule", ServerName, "High CPU", null, null, null, null);

            /* Alert history — server-scoped + fleet-wide both surface the planted alert. */
            var scoped = await DarlingMcpAlertTools.GetAlertHistory(postgres, ServerName);
            DarlingMcpTestData.AssertEnvelope(scoped, ServerName, "alerts");
            Assert.Contains("High CPU", scoped, StringComparison.Ordinal);

            var fleet = await DarlingMcpAlertTools.GetAlertHistory(postgres);
            Assert.False(fleet.StartsWith("Error during", StringComparison.Ordinal), fleet);
            Assert.Contains("(all servers)", fleet, StringComparison.Ordinal);
            Assert.Contains("High CPU", fleet, StringComparison.Ordinal);

            /* Alert settings — the seeded row round-trips its default thresholds. */
            var settings = await DarlingMcpAlertTools.GetAlertSettings(postgres);
            Assert.False(settings.StartsWith("Error during", StringComparison.Ordinal), settings);
            Assert.Contains("threshold_percent", settings, StringComparison.Ordinal);
            Assert.Contains("delivery", settings, StringComparison.Ordinal);

            /* Mute rules — the planted rule surfaces. */
            var mutes = await DarlingMcpAlertTools.GetMuteRules(postgres);
            Assert.False(mutes.StartsWith("Error during", StringComparison.Ordinal), mutes);
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

        var originalThreshold = Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT cpu_threshold_percent FROM config_alert_settings WHERE id = 1"));
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

            /* An unknown field writes NOTHING (validated before the UPDATE). */
            Assert.Equal("invalid", DarlingMcpTestData.StatusOf(await DarlingMcpAlertTools.UpdateAlertSettings(postgres, "{\"cpu\":{\"bogus\":1}}")));

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
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "UPDATE config_alert_settings SET cpu_threshold_percent = $1 WHERE id = 1", originalThreshold);
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "DELETE FROM config_mute_rules WHERE reason = $1", muteTag);
            });
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, CancellationToken ct, string sql)
    {
        using var command = new NpgsqlCommand(sql, connection);
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
