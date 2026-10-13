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
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

using Snap = PerformanceMonitor.Common.ConfigChangeDiff;

namespace Darling.Tests;

/// <summary>
/// Pins the Configuration Changes tab's snapshot-read SQL (the Dashboard's ConfigChangesContent ported to the
/// viewer). Unlike the latest-snapshot Configuration reads, the change reads are WINDOWED — but only the UPPER
/// bound (<c>capture_time &lt;= $2</c>, the window end) is applied in SQL; the window START is applied to the
/// computed change_time in C# (see <see cref="ViewerConfigChangesDiffTests"/>), so the snapshot immediately
/// BEFORE the window is kept as the diff baseline. That mirrors the Dashboard, whose report.*_changes views LAG
/// over the full history and then filter change_time to [from,to]. Reads take server_id ($1) and window-end ($2)
/// only — never a lower $3 (which would drop the baseline).
/// </summary>
public sealed class ViewerConfigChangesSqlTests
{
    private static string StripWhitespace(string sql) =>
        new string(sql.Where(c => !char.IsWhiteSpace(c)).ToArray());

    [Fact]
    public void ServerConfigChangesSnapshotsSql_ReadsView_UpperBounded_OrderedForPerNameDiff()
    {
        var sql = ViewerDataService.ServerConfigChangesSnapshotsSql;
        Assert.Contains("FROM v_server_config", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time", sql, StringComparison.Ordinal);
        Assert.Contains("value_configured", sql, StringComparison.Ordinal);
        Assert.Contains("value_in_use", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY configuration_name, capture_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseConfigChangesSnapshotsSql_ReadsView_CastsSettingsToText_UpperBounded()
    {
        var sql = ViewerDataService.DatabaseConfigChangesSnapshotsSql;
        Assert.Contains("FROM v_database_config", sql, StringComparison.Ordinal);
        Assert.Contains("is_read_committed_snapshot_on::text", sql, StringComparison.Ordinal);
        Assert.Contains("compatibility_level::text", sql, StringComparison.Ordinal);
        Assert.Contains("is_optimized_locking_on::text", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY database_name, capture_time", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TraceFlagChangesSnapshotsSql_ReadsView_UpperBounded_OrderedForSetDiff()
    {
        var sql = ViewerDataService.TraceFlagChangesSnapshotsSql;
        Assert.Contains("FROM v_trace_flags", sql, StringComparison.Ordinal);
        Assert.Contains("trace_flag", sql, StringComparison.Ordinal);
        Assert.Contains("is_global", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Contains("capture_time <= $2", sql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY capture_time, trace_flag", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("server")]
    [InlineData("database")]
    [InlineData("traceflags")]
    public void ChangeReads_PgDialect_ServerIdAndWindowEnd_NoLowerBound_NoBareNow_NoNLiterals(string which)
    {
        var sql = which switch
        {
            "server" => ViewerDataService.ServerConfigChangesSnapshotsSql,
            "database" => ViewerDataService.DatabaseConfigChangesSnapshotsSql,
            _ => ViewerDataService.TraceFlagChangesSnapshotsSql,
        };

        var lower = sql.ToLowerInvariant();
        Assert.DoesNotContain("getdate", lower);
        Assert.DoesNotContain("now(", lower);
        Assert.DoesNotContain("convert(", lower); /* ::text casts, not T-SQL CONVERT */
        Assert.DoesNotContain("top (", lower);
        Assert.DoesNotContain("isnull(", lower);
        Assert.DoesNotContain("N'", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);
        Assert.Contains("$2", sql, StringComparison.Ordinal);
        /* Upper bound only — the window START is applied to change_time in C# so the pre-window baseline is
           kept for correct left-edge change detection. A lower $3 read bound would under-report. */
        if (which == "database")
        {
            /* #1319: the database-config change read gains the global database filter ($3, a nullable
               text[] applied as database_name = ANY($3)) — a database-name predicate, not a lower time bound. */
            Assert.Contains("database_name = ANY($3)", sql, StringComparison.Ordinal);
        }
        else
        {
            Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DatabaseConfigChangeSettingNames_Match27CollectorColumns_InOrder()
    {
        var table = PgSchemaGenerator.CreateTable(DatabaseConfigCollector.Instance);
        Assert.Equal(27, ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count);
        foreach (var name in ConfigChangeDiff.DatabaseConfigChangeSettingNames)
            Assert.Contains(name, table, StringComparison.Ordinal);

        /* The wide-row diff walks these positionally, so the order must match the SELECT's column order. */
        Assert.Equal("state_desc", ConfigChangeDiff.DatabaseConfigChangeSettingNames[0]);
        Assert.Equal("is_optimized_locking_on", ConfigChangeDiff.DatabaseConfigChangeSettingNames[^1]);
    }

    [Fact]
    public void ServerConfigChangesSnapshotsSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("server_config", ServerConfigCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(ServerConfigCollector.Instance);
        foreach (var column in new[] { "capture_time", "configuration_name", "value_configured", "value_in_use", "is_dynamic", "is_advanced" })
            Assert.Contains(column, ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void TraceFlagChangesSnapshotsSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("trace_flags", TraceFlagsCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(TraceFlagsCollector.Instance);
        foreach (var column in new[] { "capture_time", "trace_flag", "status", "is_global", "is_session" })
            Assert.Contains(column, ddl, StringComparison.Ordinal);
    }

    [Fact]
    public void DatabaseConfigChangesSnapshotsSql_ReadsColumnsThatExistInTheGeneratedTable()
    {
        Assert.Equal("database_config", DatabaseConfigCollector.Instance.TargetTable);
        var ddl = PgSchemaGenerator.CreateTable(DatabaseConfigCollector.Instance);
        Assert.Contains("capture_time", ddl, StringComparison.Ordinal);
        Assert.Contains("database_name", ddl, StringComparison.Ordinal);
        foreach (var column in ConfigChangeDiff.DatabaseConfigChangeSettingNames)
            Assert.Contains(column, ddl, StringComparison.Ordinal);
    }
}

/// <summary>
/// The C# snapshot-diff logic (pure, no live PG) — mirrors DarlingConfigHistoryReader's diff (PR #1421) plus the
/// window's UPPER bound and the derived display columns the Dashboard grids show. Exercises value moves, the
/// &gt;= 2-snapshot rule, BOTH window edges on change_time, the crucial pre-window baseline (a change landing on
/// the first in-window snapshot is still detected), the wide-row UNPIVOT, trace-flag enable/disable/modify, and
/// the derived requires_restart / change_description / scope columns.
/// </summary>
public sealed class ViewerConfigChangesDiffTests
{
    private static readonly DateTime T0 = new(2026, 7, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T1 = new(2026, 7, 2, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T2 = new(2026, 7, 3, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime T3 = new(2026, 7, 4, 0, 0, 0, DateTimeKind.Unspecified);

    /* ---------------- server config ---------------- */

    [Fact]
    public void ServerConfigChanges_DetectsValueMove_NewestFirst()
    {
        var snapshots = new List<Snap.ServerConfigSnapshot>
        {
            new(T0, "max degree of parallelism", 0, 0, true, true),
            new(T1, "max degree of parallelism", 4, 4, true, true),   /* changed */
            new(T0, "cost threshold for parallelism", 5, 5, true, true),
            new(T1, "cost threshold for parallelism", 5, 5, true, true), /* unchanged */
        };

        var changes = ConfigChangeDiff.DiffServerConfigChanges(snapshots, T0, T3);
        var change = Assert.Single(changes);
        Assert.Equal("max degree of parallelism", change.ConfigurationName);
        Assert.Equal(0L, change.OldValueConfigured);
        Assert.Equal(4L, change.NewValueConfigured);
        Assert.Equal(T1, change.ChangeTime);
    }

    [Fact]
    public void ServerConfigChanges_SingleSnapshot_YieldsNothing()
    {
        var single = new List<Snap.ServerConfigSnapshot> { new(T0, "max server memory (MB)", 8000, 8000, true, true) };
        Assert.Empty(ConfigChangeDiff.DiffServerConfigChanges(single, T0, T3));
    }

    [Fact]
    public void ServerConfigChanges_WindowBoundsBothSides_OnChangeTime()
    {
        var two = new List<Snap.ServerConfigSnapshot>
        {
            new(T1, "max server memory (MB)", 8000, 8000, true, true),
            new(T2, "max server memory (MB)", 16000, 16000, true, true),  /* change at T2 */
        };

        /* Change at T2 is inside [T0, T3]. */
        Assert.Single(ConfigChangeDiff.DiffServerConfigChanges(two, T0, T3));
        /* Window START after the change (start T3) excludes it. */
        Assert.Empty(ConfigChangeDiff.DiffServerConfigChanges(two, T3, T3));
        /* Window END before the change (end T1) excludes it. */
        Assert.Empty(ConfigChangeDiff.DiffServerConfigChanges(two, T0, T1));
    }

    [Fact]
    public void ServerConfigChanges_PreWindowBaseline_ChangeOnFirstInWindowSnapshot_IsDetected()
    {
        /* T0 is BEFORE the window [T1, T3]; the value moves T0 -> T1. The change at T1 must still be detected,
           using the pre-window T0 as the diff baseline — this is why the read is not lower-bounded. */
        var snapshots = new List<Snap.ServerConfigSnapshot>
        {
            new(T0, "max degree of parallelism", 0, 0, true, true),
            new(T1, "max degree of parallelism", 8, 8, true, true),
        };

        var change = Assert.Single(ConfigChangeDiff.DiffServerConfigChanges(snapshots, T1, T3));
        Assert.Equal(0L, change.OldValueConfigured);
        Assert.Equal(8L, change.NewValueConfigured);
        Assert.Equal(T1, change.ChangeTime);
    }

    [Fact]
    public void ServerConfigChanges_DerivedColumns_RestartAndDescription()
    {
        /* Non-dynamic, new configured != new in-use => requires restart; configured value moved. */
        var restartNeeded = new List<Snap.ServerConfigSnapshot>
        {
            new(T0, "max server memory (MB)", 8000, 8000, false, false),
            new(T1, "max server memory (MB)", 16000, 8000, false, false),
        };
        var change = Assert.Single(ConfigChangeDiff.DiffServerConfigChanges(restartNeeded, T0, T3));
        Assert.True(change.RequiresRestart);
        Assert.Equal("Yes", change.RequiresRestartDisplay);
        Assert.Equal("No", change.DynamicDisplay);
        Assert.Equal("Configured value changed from 8000 to 16000", change.ChangeDescription);

        /* Dynamic setting whose in-use caught up => no restart; in-use narrative when configured is stable. */
        var inUseMove = new List<Snap.ServerConfigSnapshot>
        {
            new(T0, "cost threshold for parallelism", 50, 5, true, true),
            new(T1, "cost threshold for parallelism", 50, 50, true, true),
        };
        var c2 = Assert.Single(ConfigChangeDiff.DiffServerConfigChanges(inUseMove, T0, T3));
        Assert.False(c2.RequiresRestart);
        Assert.Equal("In-use value changed from 5 to 50", c2.ChangeDescription);
    }

    /* ---------------- database config (wide-row unpivot) ---------------- */

    [Fact]
    public void DatabaseConfigChanges_UnpivotsWideRow_NamesTheChangedColumn_WithDescription()
    {
        const int rcsiIndex = 10; /* is_read_committed_snapshot_on */
        Assert.Equal("is_read_committed_snapshot_on", ConfigChangeDiff.DatabaseConfigChangeSettingNames[rcsiIndex]);

        var snapshots = new List<Snap.DatabaseConfigSnapshot>
        {
            new(T0, "StackOverflow", DbValues()),
            new(T1, "StackOverflow", DbValues((rcsiIndex, "true"))),  /* RCSI flipped on */
        };

        var change = Assert.Single(ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0, T3));
        Assert.Equal("StackOverflow", change.DatabaseName);
        Assert.Equal("is_read_committed_snapshot_on", change.SettingName);
        Assert.Equal("false", change.OldValue);
        Assert.Equal("true", change.NewValue);
        Assert.Equal(T1, change.ChangeTime);
        Assert.Equal("Changed from false to true", change.ChangeDescription);
    }

    [Fact]
    public void DatabaseConfigChanges_PerDatabase_UnchangedYieldsNothing()
    {
        var snapshots = new List<Snap.DatabaseConfigSnapshot>
        {
            new(T0, "DbA", DbValues()),
            new(T1, "DbA", DbValues()),                 /* no change */
            new(T0, "DbB", DbValues((3, "FULL"))),
            new(T1, "DbB", DbValues((3, "SIMPLE"))),    /* recovery_model changed */
        };

        var change = Assert.Single(ConfigChangeDiff.DiffDatabaseConfigChanges(snapshots, T0, T3));
        Assert.Equal("DbB", change.DatabaseName);
        Assert.Equal("recovery_model", change.SettingName);
        Assert.Equal("FULL", change.OldValue);
        Assert.Equal("SIMPLE", change.NewValue);
    }

    [Fact]
    public void DatabaseConfigChanges_NullTransitions_SetAndCleared()
    {
        var setValue = new List<Snap.DatabaseConfigSnapshot>
        {
            new(T0, "Db", DbValues((3, null))),
            new(T1, "Db", DbValues((3, "SIMPLE"))),
        };
        Assert.Equal("Set to: SIMPLE", Assert.Single(ConfigChangeDiff.DiffDatabaseConfigChanges(setValue, T0, T3)).ChangeDescription);

        var cleared = new List<Snap.DatabaseConfigSnapshot>
        {
            new(T0, "Db", DbValues((3, "SIMPLE"))),
            new(T1, "Db", DbValues((3, null))),
        };
        Assert.Equal("Cleared (was: SIMPLE)", Assert.Single(ConfigChangeDiff.DiffDatabaseConfigChanges(cleared, T0, T3)).ChangeDescription);
    }

    /* ---------------- trace flags (set-diff) ---------------- */

    [Fact]
    public void TraceFlagChanges_DetectsEnableDisableModify()
    {
        var snapshots = new List<Snap.TraceFlagSnapshot>
        {
            /* T0: 3226 global on. */
            new(T0, 3226, true, true, false),
            /* T1: 3226 still on, 1117 appears (enabled). */
            new(T1, 3226, true, true, false),
            new(T1, 1117, true, true, false),
            /* T2: 1117 disappears (disabled), 3226 becomes session-scoped (modified). */
            new(T2, 3226, true, false, true),
        };

        var changes = ConfigChangeDiff.DiffTraceFlagChanges(snapshots, T0, T3);
        Assert.Equal(3, changes.Count);

        var enabled = Assert.Single(changes, c => c.TraceFlag == 1117 && c.ChangeType == "enabled");
        Assert.Equal(T1, enabled.ChangeTime);
        Assert.Equal("Trace flag 1117 ENABLED", enabled.ChangeDescription);
        Assert.Equal("", enabled.PreviousStatusDisplay);
        Assert.Equal("ON", enabled.NewStatusDisplay);
        Assert.Equal("GLOBAL", enabled.Scope);

        var disabled = Assert.Single(changes, c => c.TraceFlag == 1117 && c.ChangeType == "disabled");
        Assert.Equal(T2, disabled.ChangeTime);
        Assert.Equal("Trace flag 1117 DISABLED", disabled.ChangeDescription);
        Assert.Equal("", disabled.NewStatusDisplay);

        var modified = Assert.Single(changes, c => c.TraceFlag == 3226 && c.ChangeType == "modified");
        Assert.Equal("SESSION", modified.Scope);
        Assert.Equal("No", modified.GlobalDisplay);
        Assert.Equal("Yes", modified.SessionDisplay);
        Assert.Equal("Trace flag 3226 scope changed", modified.ChangeDescription);
    }

    /* #5633 item 19: a successful trace_flags run that stores no row is an EMPTY snapshot at that run's time. */

    [Fact]
    public void TraceFlagChanges_FlagOnThenSuccessfulEmptyRun_IsTurnedOffAtTheEmptyRun()
    {
        var snapshots = new List<Snap.TraceFlagSnapshot> { new(T0, 3226, true, true, false) };

        var change = Assert.Single(ConfigChangeDiff.DiffTraceFlagChanges(snapshots, [T2], T0, T3));

        Assert.Equal(3226, change.TraceFlag);
        Assert.Equal("disabled", change.ChangeType);
        Assert.Equal(T2, change.ChangeTime);
        /* The rows alone (the old shape) never showed it. */
        Assert.Empty(ConfigChangeDiff.DiffTraceFlagChanges(snapshots, T0, T3));
    }

    [Fact]
    public void TraceFlagChanges_FlagOnThenNoEmptyRun_IsNoChange()
    {
        /* A failed or missing run is never passed as an empty snapshot, so the callers hand an empty list. */
        var snapshots = new List<Snap.TraceFlagSnapshot> { new(T0, 3226, true, true, false) };

        Assert.Empty(ConfigChangeDiff.DiffTraceFlagChanges(snapshots, [], T0, T3));
    }

    [Fact]
    public void TraceFlagChanges_SuccessfulEmptyRunThenFlagOn_IsTurnedOnAtTheCapture()
    {
        var snapshots = new List<Snap.TraceFlagSnapshot> { new(T2, 1117, true, true, false) };

        var change = Assert.Single(ConfigChangeDiff.DiffTraceFlagChanges(snapshots, [T0], T0, T3));

        Assert.Equal("enabled", change.ChangeType);
        Assert.Equal(T2, change.ChangeTime);
    }

    [Fact]
    public void TraceFlagChanges_AnEmptyRunAtACaptureThatHasRows_IsIgnored_AndRepeatedEmptyRunsAddNothing()
    {
        var snapshots = new List<Snap.TraceFlagSnapshot> { new(T0, 3226, true, true, false), new(T1, 3226, true, true, false) };

        /* T1 holds the flag's row, so the "empty" run stamped T1 cannot contradict it; two empties after it report ONE turn-off. */
        var change = Assert.Single(ConfigChangeDiff.DiffTraceFlagChanges(snapshots, [T1, T2, T3], T0, T3));

        Assert.Equal("disabled", change.ChangeType);
        Assert.Equal(T2, change.ChangeTime);
    }

    [Fact]
    public void TraceFlagChanges_StableYieldsNothing_AndWindowBoundsBothSides()
    {
        var stable = new List<Snap.TraceFlagSnapshot>
        {
            new(T0, 3226, true, true, false),
            new(T1, 3226, true, true, false),
        };
        Assert.Empty(ConfigChangeDiff.DiffTraceFlagChanges(stable, T0, T3));

        var enableAtT2 = new List<Snap.TraceFlagSnapshot>
        {
            new(T1, 3226, true, true, false),
            new(T1, 1117, true, true, false),
            new(T2, 3226, true, true, false),   /* 1117 gone at T2 => disabled at T2 */
        };
        Assert.Single(ConfigChangeDiff.DiffTraceFlagChanges(enableAtT2, T0, T3));   /* T2 in window */
        Assert.Empty(ConfigChangeDiff.DiffTraceFlagChanges(enableAtT2, T0, T1));    /* window ends at T1 */
        Assert.Empty(ConfigChangeDiff.DiffTraceFlagChanges(enableAtT2, T3, T3));    /* window starts at T3 */
    }

    private static string?[] DbValues(params (int Index, string? Value)[] overrides)
    {
        var values = new string?[ConfigChangeDiff.DatabaseConfigChangeSettingNames.Count];
        for (var i = 0; i < values.Length; i++)
            values[i] = "false";
        foreach (var (index, value) in overrides)
            values[index] = value;
        return values;
    }
}

/// <summary>
/// Pins the reworked inner-tab order: the resource run (Overview … Configuration Changes) followed by a
/// diagnostics tail — Daily Summary -> System Events -> Latches &amp; Spinlocks -> Collection Health. These
/// constants are the source of truth for LoadInnerTabAsync's switch + the drill-down SelectedIndex navigation,
/// so a stale constant would route to the wrong loader / tab. Must match the &lt;TabItem&gt; order in
/// ViewerServerTab.xaml.
/// </summary>
public sealed class ViewerConfigChangesTabTests
{
    [Fact]
    public void InnerTabIndices_MatchTheReworkedDiagnosticsTailOrder()
    {
        /* Resource run. */
        Assert.Equal(0, ViewerServerTab.OverviewInnerTabIndex);
        Assert.Equal(1, ViewerServerTab.WaitStatsInnerTabIndex);
        Assert.Equal(2, ViewerServerTab.QueriesInnerTabIndex);
        Assert.Equal(3, ViewerServerTab.PlanViewerInnerTabIndex);
        Assert.Equal(4, ViewerServerTab.CpuInnerTabIndex);
        Assert.Equal(5, ViewerServerTab.MemoryInnerTabIndex);
        Assert.Equal(6, ViewerServerTab.FileIoInnerTabIndex);
        Assert.Equal(7, ViewerServerTab.TempDbInnerTabIndex);
        Assert.Equal(8, ViewerServerTab.BlockingInnerTabIndex);
        Assert.Equal(9, ViewerServerTab.PerfmonInnerTabIndex);
        Assert.Equal(10, ViewerServerTab.SessionStatsInnerTabIndex);
        Assert.Equal(11, ViewerServerTab.RunningJobsInnerTabIndex);
        Assert.Equal(12, ViewerServerTab.ConfigurationInnerTabIndex);
        Assert.Equal(13, ViewerServerTab.ConfigChangesInnerTabIndex);
        /* Diagnostics tail: Daily Summary -> System Events -> Latches & Spinlocks -> Collection Health. */
        Assert.Equal(14, ViewerServerTab.DailySummaryInnerTabIndex);
        Assert.Equal(15, ViewerServerTab.SystemEventsInnerTabIndex);
        Assert.Equal(16, ViewerServerTab.LatchSpinlockInnerTabIndex);
        Assert.Equal(17, ViewerServerTab.HealthInnerTabIndex);
    }
}

/// <summary>
/// Gated (DARLING_TEST_PG) live round-trips for the change reads — validates the snapshot-read SQL end-to-end
/// (columns exist, params bind, the <c>capture_time &lt;= $2</c> upper bound trims post-window captures while the
/// pre-window baseline is kept). Plants two server-config captures (one changed) and two trace-flag captures (a
/// flag disappears), then asserts the change reads surface the drift and that a window ending before the change
/// hides it. Shares the serialized "live-postgres" collection and cleans up in finally.
/// </summary>
[Collection("live-postgres")]
public sealed class ViewerConfigChangesLivePostgresTests
{
    private const int ServerConfigServerId = -970801;
    private const int TraceFlagsServerId = -970802;

    [Fact]
    public async Task ServerConfigChanges_SurfaceInWindow_ExcludedWhenWindowEndsBeforeChange_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live server-config-changes test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "server_config", ServerConfigServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var newer = older.AddMinutes(5);

            await InsertServerConfigAsync(connection, older, "max degree of parallelism", 0, 0, true, true);
            await InsertServerConfigAsync(connection, newer, "max degree of parallelism", 4, 4, true, true);

            /* Window covers both captures: the 0 -> 4 change at `newer` surfaces. */
            var inWindow = await viewer.GetServerConfigChangesAsync(ServerConfigServerId, older.AddMinutes(-1), newer.AddMinutes(1));
            var change = Assert.Single(inWindow);
            Assert.Equal("max degree of parallelism", change.ConfigurationName);
            Assert.Equal(0L, change.OldValueConfigured);
            Assert.Equal(4L, change.NewValueConfigured);

            /* Window ends before `newer` (upper bound trims the newer capture) => no change. */
            var trimmed = await viewer.GetServerConfigChangesAsync(ServerConfigServerId, older.AddMinutes(-1), older.AddMinutes(1));
            Assert.Empty(trimmed);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "server_config", ServerConfigServerId, cleanupCt));
        }
    }

    [Fact]
    public async Task TraceFlagChanges_SetDiffDisable_SurfacesInWindow_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live trace-flag-changes test.");

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await PgMigrations.MigrateAsync(connection, TestContext.Current.CancellationToken);
        await DeleteRowsAsync(connection, "trace_flags", TraceFlagsServerId, TestContext.Current.CancellationToken);

        await using var viewer = new ViewerDataService(connectionString!);

        var bodySucceeded = false;
        try
        {
            var older = TruncateToSeconds(DateTime.UtcNow.AddMinutes(-10));
            var newer = older.AddMinutes(5);

            /* 1117 present at the older capture, absent at the newer => 'disabled' at `newer`. 3226 anchors the
               newer capture so it exists as a distinct set-diff snapshot. */
            await InsertTraceFlagAsync(connection, older, 1117, status: true, isGlobal: true, isSession: false);
            await InsertTraceFlagAsync(connection, newer, 3226, status: true, isGlobal: true, isSession: false);

            var changes = await viewer.GetTraceFlagChangesAsync(TraceFlagsServerId, older.AddMinutes(-1), newer.AddMinutes(1));
            var disabled = Assert.Single(changes, c => c.TraceFlag == 1117);
            Assert.Equal("disabled", disabled.ChangeType);
            Assert.Equal("Trace flag 1117 DISABLED", disabled.ChangeDescription);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteRowsAsync(cleanup, "trace_flags", TraceFlagsServerId, cleanupCt));
        }
    }

    private static async Task InsertServerConfigAsync(
        NpgsqlConnection connection, DateTime captureTimeUtc,
        string configurationName, long valueConfigured, long valueInUse, bool isDynamic, bool isAdvanced)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name,
     configuration_name, value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(ServerConfigServerId);
        command.Parameters.AddWithValue("viewer-server-config-changes-e2e");
        command.Parameters.AddWithValue(configurationName);
        command.Parameters.AddWithValue(valueConfigured);
        command.Parameters.AddWithValue(valueInUse);
        command.Parameters.AddWithValue(isDynamic);
        command.Parameters.AddWithValue(isAdvanced);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task InsertTraceFlagAsync(
        NpgsqlConnection connection, DateTime captureTimeUtc,
        int traceFlag, bool status, bool isGlobal, bool isSession)
    {
        using var command = new NpgsqlCommand(@"
INSERT INTO trace_flags
    (config_id, capture_time, server_id, server_name,
     trace_flag, status, is_global, is_session)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", connection);
        command.Parameters.AddWithValue(1L);
        command.Parameters.AddWithValue(DateTime.SpecifyKind(captureTimeUtc, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(TraceFlagsServerId);
        command.Parameters.AddWithValue("viewer-trace-flag-changes-e2e");
        command.Parameters.AddWithValue(traceFlag);
        command.Parameters.AddWithValue(status);
        command.Parameters.AddWithValue(isGlobal);
        command.Parameters.AddWithValue(isSession);
        await command.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static DateTime TruncateToSeconds(DateTime value) =>
        DateTime.SpecifyKind(new DateTime(value.Ticks - (value.Ticks % TimeSpan.TicksPerSecond)), DateTimeKind.Unspecified);

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, string table, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand($"DELETE FROM {table} WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
