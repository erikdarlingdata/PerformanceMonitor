/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3653 A10 slice two on the Darling side: the two Postgres reads that feed the <c>database_config</c> and
/// <c>trace_flags</c> families into the <c>CONFIG_CHANGED</c> attribution
/// (<see cref="DarlingAnalysisService.DatabaseConfigSnapshotsForAttributionSql"/>,
/// <see cref="DarlingAnalysisService.TraceFlagSnapshotsForAttributionSql"/>), and the same-connect fold
/// exercised once more from this assembly so the two SKUs' test trees cannot drift on what "one event" means.
/// The Lite twin of the pure pins is <c>Lite.Tests/ConfigChangeAttributionTests</c>; the live arms are in
/// <see cref="ConfigChangeFamiliesLivePostgresTests"/>.
///
/// <para><b>Why the projection ORDER is pinned.</b> The wide <c>database_config</c> row is read positionally:
/// the reader hands the 27 option columns to <c>ConfigChangeDiff.DatabaseConfigSnapshot.Values</c> as a list,
/// and the diff names each change by <see cref="ConfigChangeDiff.DatabaseConfigChangeSettingNames"/> at the
/// same index. A column added or reordered in the SELECT without the list moving with it would not fail —
/// it would attribute a recovery-model change to the collation, silently, on every card. The MCP history
/// reader carries the same projection unbounded; this read bounds it to the pass window plus the baseline
/// capture, and the pin below is the one place the order is compared to the diff's list by name.</para>
/// </summary>
public sealed class ConfigChangeFamilyReadTests
{
    private static readonly DateTime T0 = new(2026, 9, 18, 14, 0, 0, DateTimeKind.Utc);
    private const string Maxdop = "max degree of parallelism";

    /* ───────────────────────── the Postgres reads ───────────────────────── */

    [Fact]
    public void TheDatabaseConfigRead_ProjectsTheDiffsColumns_InTheDiffsOrder_AfterTheTwoKeys()
    {
        var sql = DarlingAnalysisService.DatabaseConfigSnapshotsForAttributionSql;
        var projection = sql[(sql.IndexOf("SELECT", StringComparison.Ordinal) + "SELECT".Length)..sql.IndexOf("FROM database_config", StringComparison.Ordinal)];

        /* Each projected line is `name` or `name::text` — the text cast is what lets the diff compare a
           boolean and an integer column the same way it compares recovery_model. */
        var columns = projection
            .Split(',')
            .Select(c => Regex.Replace(c.Trim(), @"::text$", string.Empty))
            .Where(c => c.Length > 0)
            .ToList();

        Assert.Equal("capture_time", columns[0]);
        Assert.Equal("database_name", columns[1]);
        Assert.Equal(ConfigChangeDiff.DatabaseConfigChangeSettingNames, columns.Skip(2).ToList());
        Assert.Equal(ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count + 2, columns.Count);

        /* The non-text columns carry the cast; the text ones do not need it and do not have it. */
        Assert.Contains("compatibility_level::text", sql, StringComparison.Ordinal);
        Assert.Contains("is_read_committed_snapshot_on::text", sql, StringComparison.Ordinal);
        Assert.Contains("target_recovery_time_seconds::text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("recovery_model::text", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("state_desc::text", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void BothFamilyReads_CarryTheWindowRule_WithTheBaselineCapture()
    {
        foreach (var (sql, table) in new[]
        {
            (DarlingAnalysisService.DatabaseConfigSnapshotsForAttributionSql, "database_config"),
            (DarlingAnalysisService.TraceFlagSnapshotsForAttributionSql, "trace_flags"),
        })
        {
            Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
            Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
            Assert.Contains("AND   capture_time <= $3", sql, StringComparison.Ordinal);
            /* The last capture BEFORE the window is the diff baseline; without it a change on the first
               in-window capture is invisible. COALESCE to the window start covers a server with none. */
            Assert.Contains($"(SELECT MAX(capture_time) FROM {table} WHERE server_id = $1 AND capture_time < $2)", sql, StringComparison.Ordinal);
            Assert.Contains("AND   capture_time >= COALESCE(", sql, StringComparison.Ordinal);
            Assert.EndsWith("$2)", sql[..sql.LastIndexOf("ORDER BY", StringComparison.Ordinal)].TrimEnd(), StringComparison.Ordinal);
            Assert.DoesNotContain("now()", sql, StringComparison.Ordinal);
            Assert.DoesNotContain("v_", sql, StringComparison.Ordinal);
        }

        Assert.EndsWith("ORDER BY database_name, capture_time", DarlingAnalysisService.DatabaseConfigSnapshotsForAttributionSql.TrimEnd(), StringComparison.Ordinal);
        Assert.EndsWith("ORDER BY capture_time, trace_flag", DarlingAnalysisService.TraceFlagSnapshotsForAttributionSql.TrimEnd(), StringComparison.Ordinal);
        Assert.Contains("SELECT capture_time, trace_flag, status, is_global, is_session", DarlingAnalysisService.TraceFlagSnapshotsForAttributionSql, StringComparison.Ordinal);
    }

    /* ───────────────────────── the fold, from this assembly ───────────────────────── */

    [Fact]
    public void TheFold_MakesOneEventOfAServerSettingAndAFlagCapturedSecondsApart_AndTheFactSaysBoth()
    {
        var serverEvent = MaxdopEvent(T0, T0.AddHours(-23));
        var traceSnapshots = new List<ConfigChangeDiff.TraceFlagSnapshot>
        {
            new(T0.AddHours(-23).AddSeconds(2), 3226, true, true, false),
            new(T0.AddSeconds(2), 3226, true, true, false),
            new(T0.AddSeconds(2), 4199, true, true, false),
        };
        var traceEvent = Assert.Single(ConfigChangeAttribution.GroupIntoEvents(
            ConfigChangeDiff.DiffTraceFlagChanges(traceSnapshots, T0.AddHours(-4), T0.AddHours(1))
                .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForTraceFlag(c.TraceFlag, c.ChangeType, c.Scope, c.PreviousStatus, c.NewStatus))),
            traceSnapshots.Select(s => s.CaptureTime)));

        var folded = Assert.Single(ConfigChangeAttribution.MergeSameConnectEvents([traceEvent, serverEvent]));
        Assert.Equal(T0.AddSeconds(2), folded.ChangeTime);
        Assert.Equal(T0.AddHours(-23), folded.PreviousCaptureTime);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.ServerConfig | ConfigChangeAttribution.ChangeFamily.TraceFlags, folded.Families);

        var fact = ConfigChangeAttribution.BuildFact(1, folded, 0, ConfigChangeAttribution.WindowsFor(folded.ChangeTime, T0.AddHours(4)), compare: null, null, null);
        Assert.Equal(5, fact.Metadata[ConfigChangeAttribution.MetaChangeFamily]);
        Assert.Equal($"{Maxdop}; tf|4199|enabled|GLOBAL", fact.ObjectName);
        Assert.Null(fact.DatabaseName);

        var advice = FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!;
        Assert.StartsWith("Configuration changed: 2 configuration changes observed together", advice.Headline, StringComparison.Ordinal);
        Assert.Contains($"`{Maxdop}` 0 → 8, trace flag 4199 enabled (GLOBAL)", advice.Investigation, StringComparison.Ordinal);

        /* The same two, an hour apart: two connects, two events, the server setting the subject. */
        var farTrace = traceEvent with { ChangeTime = T0.AddHours(-1), PreviousCaptureTime = T0.AddHours(-23) };
        var two = ConfigChangeAttribution.MergeSameConnectEvents([farTrace, serverEvent]);
        Assert.Equal(2, two.Count);
        Assert.Equal(ConfigChangeAttribution.ChangeFamily.ServerConfig, two[0].Families);
    }

    [Fact]
    public void TheDatabaseFamily_CarriesTheDatabase_AndDropsTheStatusColumn()
    {
        var snapshots = new List<ConfigChangeDiff.DatabaseConfigSnapshot>
        {
            DbSnapshot(T0.AddHours(-23), "Sales", ("recovery_model", "FULL"), ("log_reuse_wait_desc", "NOTHING")),
            DbSnapshot(T0, "Sales", ("recovery_model", "SIMPLE"), ("log_reuse_wait_desc", "LOG_BACKUP")),
        };
        var evt = Assert.Single(ConfigChangeAttribution.GroupIntoEvents(
            ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0.AddHours(-4), T0.AddHours(1))
                .Where(c => ConfigChangeAttribution.IsAttributableDatabaseSetting(c.SettingName))
                .Select(c => (c.ChangeTime, ConfigChangeAttribution.SettingChange.ForDatabase(c.DatabaseName, c.SettingName, c.OldValue, c.NewValue))),
            snapshots.Select(s => s.CaptureTime)));

        var change = Assert.Single(evt.Changes);
        Assert.Equal("recovery_model", change.Setting);
        Assert.Equal(("FULL", "SIMPLE"), (change.OldText, change.NewText));

        var fact = ConfigChangeAttribution.BuildFact(1, evt, 0, ConfigChangeAttribution.WindowsFor(T0, T0.AddHours(4)), compare: null, null, null);
        Assert.Equal("Sales", fact.DatabaseName);
        Assert.Equal(2, fact.Metadata[ConfigChangeAttribution.MetaChangeFamily]);
        Assert.Equal("db|recovery_model|FULL|SIMPLE|Sales", fact.ObjectName);
        Assert.StartsWith("Database configuration changed: `Sales` recovery_model FULL → SIMPLE", FactAdvice.Compose(ConfigChangeAttribution.FactKey, new[] { fact }.ToFactLookup())!.Headline, StringComparison.Ordinal);
    }

    /* ───────────────────────── helpers ───────────────────────── */

    private static ConfigChangeAttribution.ChangeEvent MaxdopEvent(DateTime observedAt, DateTime previousCapture)
    {
        var snapshots = new List<ConfigChangeDiff.ServerConfigSnapshot>
        {
            new(previousCapture, Maxdop, 0, 0, true, true),
            new(observedAt, Maxdop, 8, 8, true, true),
        };
        return Assert.Single(ConfigChangeAttribution.GroupIntoEvents(
            ConfigChangeDiff.DiffServerConfigChanges(snapshots, observedAt.AddHours(-4), observedAt)
                .Select(c => (c.ChangeTime, new ConfigChangeAttribution.SettingChange(
                    c.ConfigurationName, c.OldValueConfigured, c.NewValueConfigured, c.OldValueInUse, c.NewValueInUse, c.RequiresRestart))),
            snapshots.Select(s => s.CaptureTime)));
    }

    private static ConfigChangeDiff.DatabaseConfigSnapshot DbSnapshot(DateTime at, string database, params (string Setting, string? Value)[] set)
    {
        var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
        foreach (var (setting, value) in set)
            values[ConfigChangeDiff.DatabaseConfigChangeSettingNames.ToList().IndexOf(setting)] = value;
        return new ConfigChangeDiff.DatabaseConfigSnapshot(at, database, values);
    }
}
