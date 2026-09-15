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
using PerformanceMonitor.Notifications;
using Xunit;

using Reader = PerformanceMonitor.Darling.Service.Mcp.DarlingAlertReader;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Pins the alerts MCP slice — the three READS (get_alert_history, get_alert_settings, get_mute_rules) plus the
/// four Darling-only WRITES (update_alert_settings, create_mute_rule, delete_mute_rule,
/// set_mute_rule_enabled) over the Postgres store.
/// Ungated: the tool surface is EXACTLY the seven names (all static, on a [McpServerToolType] class, returning
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
        "set_mute_rule_enabled",
        "update_alert_settings",
    };

    private static MethodInfo[] ToolMethods() => typeof(DarlingMcpAlertTools)
        .GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance)
        .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
        .ToArray();

    [Fact]
    public void ToolSurface_ExactlyTheSevenAlertTools()
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
    [InlineData("set_mute_rule_enabled", "rule_id,enabled")]
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
            tools, @"GetAlertConfigurationAsync\(postgres\)").Count);
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
        DeadlockWarnPerHour: 7.0, DeadlockCriticalPerHour: 31.0);

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
    public void AdvertisedSchema_IsGeminiClean_ForAllSevenTools()
    {
        var tools = BuildToolSchemas();
        Assert.Equal(7, tools.Count);
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

    internal FakeMuteRuleStore Seed(MuteRule rule)
    {
        _rows.Add(rule.Clone());
        return this;
    }

    /// <summary>The stored row by id, cloned — the assertion surface for what a write left behind.</summary>
    internal MuteRule? Row(string id) => _rows.Find(r => r.Id == id)?.Clone();

    internal int Count => _rows.Count;

    internal void Remove(string id) => _rows.RemoveAll(r => r.Id == id);

    public Task<IReadOnlyList<MuteRule>> LoadAllAsync()
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

        Assert.StartsWith("Error during set_mute_rule_enabled", result, StringComparison.Ordinal);
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
        await DarlingMcpTestData.ExecAsync(connection, ct, "INSERT INTO config_notification (id) VALUES (1) ON CONFLICT (id) DO NOTHING");

        var originalThreshold = Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT cpu_threshold_percent FROM config_alert_settings WHERE id = 1"));
        /* #3314: captured for the same reason as the threshold — these two are SINGLETONS the whole store
           shares, and the delivery cooldown now governs channel volume for every later test and every later
           run on a reused database. */
        var originalFireCooldown = Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT cooldown_minutes FROM config_alert_settings WHERE id = 1"));
        var originalDeliveryCooldown = Convert.ToInt32(await ScalarAsync(connection, ct, "SELECT email_cooldown_minutes FROM config_notification WHERE id = 1"));
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
                await DarlingMcpTestData.ExecAsync(cleanup, cleanupCt, "UPDATE config_notification SET email_cooldown_minutes = $1 WHERE id = 1", originalDeliveryCooldown);
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
