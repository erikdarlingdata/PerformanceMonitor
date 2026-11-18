/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6873 (part of #6872): the <c>database_protection</c> collector. Its reading of the three facts (never, unknown, a recent time,
/// none in the window, SIMPLE recovery), its platform label for each target shape, and its V223 table.
/// </summary>
public sealed class DatabaseProtectionCollectorTests
{
    private static readonly DateTime Checked = new(2026, 10, 20, 3, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Full = new(2026, 10, 26, 1, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime Log = new(2026, 10, 28, 9, 45, 0, DateTimeKind.Unspecified);

    private static CollectorContext Context(CollectorTargetInfo? target = null) => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s",
        Target = target ?? new CollectorTargetInfo(),
        Deltas = new CollectorDeltaCalculator(),
    };

    private static DataTable Shape()
    {
        var table = new DataTable();
        table.Columns.Add("name", typeof(string));
        table.Columns.Add("state_desc", typeof(string));
        table.Columns.Add("recovery_model_desc", typeof(string));
        table.Columns.Add("create_date", typeof(DateTime));
        table.Columns.Add("replica_role", typeof(string));
        table.Columns.Add("last_good_checkdb_time", typeof(DateTime));
        table.Columns.Add("last_full", typeof(DateTime));
        table.Columns.Add("last_log", typeof(DateTime));
        table.Columns.Add("backups_readable", typeof(bool));
        table.Columns.Add("instance_name", typeof(string));
        table.Columns.Add("replica_allow_connections", typeof(string));
        return table;
    }

    /* #8865: the second result set, the error logs the instance lists. A single (0, NULL, NULL, error) row means the listing was refused or
       failed, with the error it raised. */
    private static DataTable LogShape()
    {
        var table = new DataTable();
        table.Columns.Add("enumerate_ok", typeof(bool));
        table.Columns.Add("archive", typeof(int));
        table.Columns.Add("log_date", typeof(DateTime));
        table.Columns.Add("enumerate_error", typeof(int));
        return table;
    }

    private static DataTable RefusedList(int error = 27219)
    {
        var table = LogShape();
        table.Rows.Add(false, DBNull.Value, DBNull.Value, error);
        return table;
    }

    /* Archive n is dated n days before Checked, so a higher archive number is an older log, as on a real instance. */
    private static DataTable Logs(int count)
    {
        var table = LogShape();
        for (var archive = 0; archive < count; archive++)
        {
            table.Rows.Add(true, archive, Checked.AddDays(-archive), DBNull.Value);
        }

        return table;
    }

    private static string CheckValue(DateTime time, string options = "NO_INFOMSGS", int errors = 0) =>
        new DatabaseProtectionErrorLogState.StoredCheck(time, options, errors).Format();

    private static CollectorContext ContextWith(IReadOnlyDictionary<string, string> state, CollectorTargetInfo? target = null) => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s",
        Target = target ?? new CollectorTargetInfo(),
        Deltas = new CollectorDeltaCalculator(),
        State = state,
    };

    private static Dictionary<string, string> Carry(CollectorContext context, IReadOnlyDictionary<string, string> state)
    {
        var next = new Dictionary<string, string>(state, StringComparer.Ordinal);
        foreach (var pair in context.PendingState)
        {
            next[pair.Key] = pair.Value;
        }

        return next;
    }

    private static async Task<List<DatabaseProtectionCollector.Row>> ReadWithAsync(CollectorContext context, DataTable primary, DataTable? logs)
    {
        using var reader = logs is null ? primary.CreateDataReader() : new DataTableReader(new[] { primary, logs });
        return await DatabaseProtectionCollector.Instance.ReadAsync(reader, context, TestContext.Current.CancellationToken);
    }

    private static void Add(DataTable table, string name, string recovery, DateTime? checkDb, DateTime? full, DateTime? log, bool readable = true, string state = "ONLINE", string? role = null,
        DateTime? created = null, string instance = "INST1", string? allowConnections = null) =>
        table.Rows.Add(name, state, recovery, created ?? Checked.AddYears(-1), (object?)role ?? DBNull.Value,
            (object?)checkDb ?? DBNull.Value, (object?)full ?? DBNull.Value, (object?)log ?? DBNull.Value, readable,
            instance, (object?)allowConnections ?? DBNull.Value);

    /* The primary result set is followed by the log list unless the target is platform managed, which builds no second set. */
    private static async Task<List<DatabaseProtectionCollector.Row>> ReadAsync(DataTable table, CollectorTargetInfo? target = null)
    {
        var managed = target is not null && (target.IsAzureSqlDb || target.IsAzureManagedInstance);
        return await ReadWithAsync(Context(target), table, managed ? null : Logs(1));
    }

    [Fact]
    public async Task ACheckDbOf1900_ReadsNever_ANullOnAnOnlineDatabaseReadsUnknown_ARealTimeReadsChecked()
    {
        var table = Shape();
        Add(table, "NeverChecked", "FULL", new DateTime(1900, 1, 1), Full, Log);
        Add(table, "BuildCannotSay", "FULL", null, Full, Log);
        Add(table, "Checked", "FULL", Checked, Full, Log);

        var rows = await ReadAsync(table);

        Assert.Equal(DatabaseProtectionCollector.CheckDbNever, rows[0].CheckDbState);
        Assert.Null(rows[0].LastGoodCheckDbTime);
        Assert.Equal(DatabaseProtectionCollector.CheckDbUnknown, rows[1].CheckDbState);
        Assert.Null(rows[1].LastGoodCheckDbTime);
        Assert.Equal(DatabaseProtectionCollector.CheckDbChecked, rows[2].CheckDbState);
        Assert.Equal(Checked, rows[2].LastGoodCheckDbTime);
    }

    [Fact]
    public async Task ABackupTimeReadsRecorded_NoBackupInTheWindowReadsNoneInWindow_AndSimpleRecoveryExpectsNoLogBackup()
    {
        var table = Shape();
        Add(table, "Backed", "FULL", Checked, Full, Log);
        Add(table, "OlderThan60Days", "FULL", Checked, null, null);
        Add(table, "Simple", "SIMPLE", Checked, Full, null);

        var rows = await ReadAsync(table);

        Assert.Equal(DatabaseProtectionCollector.BackupRecorded, rows[0].FullBackupState);
        Assert.Equal(Full, rows[0].LastFullBackupTime);
        Assert.Equal(DatabaseProtectionCollector.BackupRecorded, rows[0].LogBackupState);
        Assert.Equal(Log, rows[0].LastLogBackupTime);

        Assert.Equal(DatabaseProtectionCollector.BackupNoneInWindow, rows[1].FullBackupState);
        Assert.Equal(DatabaseProtectionCollector.BackupNoneInWindow, rows[1].LogBackupState);
        Assert.Null(rows[1].LastFullBackupTime);

        Assert.Equal(DatabaseProtectionCollector.BackupRecorded, rows[2].FullBackupState);
        Assert.Equal(DatabaseProtectionCollector.BackupNotExpected, rows[2].LogBackupState);
        Assert.Null(rows[2].LastLogBackupTime);
    }

    [Fact]
    public async Task AzureSqlDatabaseWithBackupRows_IsPlatformManaged_WithTheAgesFromTheDmv()
    {
        var table = Shape();
        Add(table, "AppDb", "FULL", new DateTime(1900, 1, 1), Full, Log);

        var row = Assert.Single(await ReadAsync(table, new CollectorTargetInfo { IsAzureSqlDb = true }));

        Assert.Equal(DatabaseProtectionCollector.PlatformManaged, row.Platform);
        Assert.Equal(Full, row.LastFullBackupTime);
        Assert.Equal(DatabaseProtectionCollector.BackupRecorded, row.FullBackupState);
        Assert.Equal(DatabaseProtectionCollector.BackupRecorded, row.LogBackupState);
        Assert.Equal(DatabaseProtectionCollector.CheckDbNever, row.CheckDbState);
    }

    [Fact]
    public async Task ARefusedBackupRead_IsPlatformManagedWithNoAge_AndUnavailable_NotAnError()
    {
        var table = Shape();
        Add(table, "HyperscaleDb", "FULL", new DateTime(1900, 1, 1), null, null, readable: false);

        var row = Assert.Single(await ReadAsync(table, new CollectorTargetInfo { IsAzureSqlDb = true }));

        Assert.Equal(DatabaseProtectionCollector.PlatformManaged, row.Platform);
        Assert.Null(row.LastFullBackupTime);
        Assert.Null(row.LastLogBackupTime);
        Assert.Equal(DatabaseProtectionCollector.BackupUnavailable, row.FullBackupState);
        Assert.Equal(DatabaseProtectionCollector.BackupUnavailable, row.LogBackupState);
    }

    [Fact]
    public async Task HyperscaleWithABackupReadThatSucceedsWithNoRows_IsPlatformManagedWithNoAge_AndNoneInWindow()
    {
        var table = Shape();
        Add(table, "HyperscaleDb", "FULL", new DateTime(1900, 1, 1), null, null, readable: true);

        var row = Assert.Single(await ReadAsync(table, new CollectorTargetInfo { IsAzureSqlDb = true }));

        Assert.Equal(DatabaseProtectionCollector.PlatformManaged, row.Platform);
        Assert.Null(row.LastFullBackupTime);
        Assert.Equal(DatabaseProtectionCollector.BackupNoneInWindow, row.FullBackupState);
        Assert.Equal(DatabaseProtectionCollector.BackupNoneInWindow, row.LogBackupState);
    }

    [Fact]
    public async Task ManagedInstanceRows_AreLabelledPlatformManaged_AndKeepTheirAges_AndRdsIsNativeOnly()
    {
        var table = Shape();
        Add(table, "MiDb", "FULL", Checked, Full, Log);

        var managed = Assert.Single(await ReadAsync(table, new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.Equal(DatabaseProtectionCollector.PlatformManaged, managed.Platform);
        Assert.Equal(Full, managed.LastFullBackupTime);
        Assert.Equal(Log, managed.LastLogBackupTime);

        var rds = Assert.Single(await ReadAsync(table, new CollectorTargetInfo { IsAwsRds = true }));
        Assert.Equal(DatabaseProtectionCollector.PlatformNativeOnly, rds.Platform);

        var native = Assert.Single(await ReadAsync(table));
        Assert.Equal(DatabaseProtectionCollector.PlatformNative, native.Platform);
    }

    [Fact]
    public async Task StateAndReplicaRole_AreKeptAsTheServerSaidThem()
    {
        var table = Shape();
        Add(table, "Secondary", "FULL", null, null, null, state: "ONLINE", role: "SECONDARY");
        Add(table, "Restoring", "FULL", null, null, null, state: "RESTORING");

        var rows = await ReadAsync(table);

        Assert.Equal("SECONDARY", rows[0].ReplicaRole);
        Assert.Equal("ONLINE", rows[0].StateDesc);
        Assert.Equal("RESTORING", rows[1].StateDesc);
        Assert.Null(rows[1].ReplicaRole);
    }

    [Fact]
    public async Task TheRows_CarryTheRememberedCheck_AndSayFailedUntilTheSupplementalApplies()
    {
        var table = Shape();
        Add(table, "Sales", "FULL", null, Full, Log, role: "SECONDARY");
        var checkTime = Checked.AddDays(-1);
        var state = new Dictionary<string, string> { [DatabaseProtectionErrorLogState.CheckKeyPrefix + "Sales"] = CheckValue(checkTime, "NO_INFOMSGS, DATA_PURITY", 2) };

        var row = Assert.Single(await ReadWithAsync(ContextWith(state), table, Logs(1)));

        Assert.Equal(checkTime, row.ErrorLogCheckDbTime);
        Assert.Equal("NO_INFOMSGS, DATA_PURITY", row.ErrorLogCheckDbOptions);
        Assert.Equal(2, row.ErrorLogCheckDbErrors);
        Assert.Equal(DatabaseProtectionCollector.ErrorLogFailed, row.ErrorLogState);
        Assert.Equal("The error log read did not complete in this cycle (timeout or error).", row.ErrorLogMessage);
        Assert.Null(row.ErrorLogGrant);
    }

    [Fact]
    public async Task ARememberedCheckBeforeTheCreateDate_IsNotCarried()
    {
        var table = Shape();
        Add(table, "Sales", "FULL", null, Full, Log, created: Checked);
        var state = new Dictionary<string, string> { [DatabaseProtectionErrorLogState.CheckKeyPrefix + "Sales"] = CheckValue(Checked.AddDays(-1)) };

        var row = Assert.Single(await ReadWithAsync(ContextWith(state), table, Logs(1)));

        Assert.Null(row.ErrorLogCheckDbTime);
        Assert.Null(row.ErrorLogCheckDbOptions);
        Assert.Null(row.ErrorLogCheckDbErrors);
    }

    [Fact]
    public async Task TheLogList_BecomesThePlan_AndThePlannedArchivesGetAnAttempt()
    {
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var context = ContextWith(new Dictionary<string, string>());

        await ReadWithAsync(context, table, Logs(7));

        var plan = Assert.IsType<DatabaseProtectionErrorLogState.ErrorLogPlan>(context.DatabaseProtectionErrorLogPlan);
        Assert.Equal(new[] { 0, 1, 2, 3 }, plan.Read.Select(a => a.Archive).ToArray());
        Assert.Equal(
            DatabaseProtectionErrorLogState.FormatAttempts(new Dictionary<DateTime, int>
            {
                [Checked.AddDays(-1)] = 1, [Checked.AddDays(-2)] = 1, [Checked.AddDays(-3)] = 1,
            }),
            context.PendingState[DatabaseProtectionErrorLogState.AttemptsKey]);
        Assert.Equal("INST1", context.PendingState[DatabaseProtectionErrorLogState.InstanceKey]);
    }

    [Fact]
    public async Task AChangedInstance_StartsOver()
    {
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var state = new Dictionary<string, string>
        {
            [DatabaseProtectionErrorLogState.InstanceKey] = "OLDNODE",
            [DatabaseProtectionErrorLogState.CheckKeyPrefix + "Sales"] = CheckValue(Checked.AddDays(-1)),
            [DatabaseProtectionErrorLogState.BackfillDoneKey] = bool.TrueString,
            [DatabaseProtectionErrorLogState.NewestArchiveKey] = "2026-10-20T00:00:00.000",
        };
        var context = ContextWith(state);

        var row = Assert.Single(await ReadWithAsync(context, table, Logs(7)));

        Assert.Null(row.ErrorLogCheckDbTime);
        Assert.True(context.DatabaseProtectionErrorLogPlan!.Before.Reset);
        Assert.Equal("INST1", context.PendingState[DatabaseProtectionErrorLogState.InstanceKey]);
        Assert.False(row.ErrorLogBackfillComplete);
        Assert.NotEqual(bool.TrueString, context.PendingState.GetValueOrDefault(DatabaseProtectionErrorLogState.BackfillDoneKey));
    }

    [Fact]
    public async Task AResetCycle_StagesEmptyValuesForTheOldChecks_AndTheNextCycleReadsNoRememberedCheck()
    {
        /* #8865 R7: only overwriting the instance key would bring the old node's checks back the next cycle, once the key matches. */
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var salesKey = DatabaseProtectionErrorLogState.CheckKeyPrefix + "Sales";
        var state = new Dictionary<string, string>
        {
            [DatabaseProtectionErrorLogState.InstanceKey] = "OLDNODE",
            [salesKey] = CheckValue(Checked.AddDays(-1)),
            [DatabaseProtectionErrorLogState.CheckKeyPrefix + "Gone"] = CheckValue(Checked.AddDays(-2)),
        };
        var first = ContextWith(state);

        await ReadWithAsync(first, table, Logs(1));

        Assert.Equal(string.Empty, first.PendingState[salesKey]);
        Assert.Equal(string.Empty, first.PendingState[DatabaseProtectionErrorLogState.CheckKeyPrefix + "Gone"]);

        var carried = Carry(first, state);
        var second = ContextWith(carried);
        var row = Assert.Single(await ReadWithAsync(second, table, Logs(1)));

        Assert.False(second.DatabaseProtectionErrorLogPlan!.Before.Reset);
        Assert.Null(row.ErrorLogCheckDbTime);
        Assert.DoesNotContain(DatabaseProtectionErrorLogState.InstanceKey, second.PendingState.Keys);
    }

    [Fact]
    public async Task WhenTheSupplementalNeverApplies_TheBackfillStillEnds()
    {
        /* #8865: skipped archives count as done, so the markers move past them even though no cycle ever reads one: the backlog
           ends on a server whose current log times out every cycle. Seven logs are six archives, three a cycle, two attempts each:
           2 x ceil(6 / 3) = 4 cycles to give up on every archive, and one more to see that nothing is left. */
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var state = new Dictionary<string, string>();
        bool? complete = null;
        var cycles = 0;
        while (cycles < 10 && complete != true)
        {
            var context = ContextWith(state);
            var row = Assert.Single(await ReadWithAsync(context, table, Logs(7)));
            complete = row.ErrorLogBackfillComplete;
            state = Carry(context, state);
            cycles++;
        }

        Assert.True(complete);
        Assert.Equal(5, cycles);
        Assert.Equal(6, DatabaseProtectionErrorLogState.ParseDates(state[DatabaseProtectionErrorLogState.SkippedKey]).Count);
    }

    [Fact]
    public async Task AMinuteOfArchivesTooManyForTheList_IsSkippedOnItsFirstRead_AndTheBackfillMovesPastIt()
    {
        /* #8865: sixty archives closed in one minute do not fit the archive list. Every read going fine, the date must still end skipped
           and the markers move past it; before the fix it gained no attempt, was re-read in part every cycle and the backfill never ended. */
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var logs = LogShape();
        logs.Rows.Add(true, 0, Checked, DBNull.Value);
        for (var archive = 1; archive <= 60; archive++)
        {
            logs.Rows.Add(true, archive, Checked.AddDays(-1), DBNull.Value);
        }

        logs.Rows.Add(true, 61, Checked.AddDays(-2), DBNull.Value);
        var state = new Dictionary<string, string>();
        var plans = new List<int[]>();
        var rows = new List<DatabaseProtectionCollector.Row>();
        for (var cycle = 0; cycle < 2; cycle++)
        {
            var context = ContextWith(state);
            rows = await ReadWithAsync(context, table, logs);
            var plan = context.DatabaseProtectionErrorLogPlan!;
            plans.Add(plan.Read.Select(a => a.Archive).ToArray());
            var lines = LineShape();
            if (cycle == 0)
            {
                lines.Rows.Add(Checked.AddDays(-1), CheckLine("Sales", 1));
            }

            await ApplyAsync(context, rows, Status(plan.Read.Select(a => (a.Archive, true)).ToArray()), lines);
            state = Carry(context, state);
        }

        Assert.True(plans[0].Length > 4);
        Assert.Equal(new[] { 0, 61 }, plans[1]);
        Assert.Equal(new[] { Checked.AddDays(-1) }, DatabaseProtectionErrorLogState.ParseDates(state[DatabaseProtectionErrorLogState.SkippedKey]));
        Assert.True(rows[0].ErrorLogBackfillComplete);
        /* The part that was read still counts: its line is the database's check. */
        Assert.Equal(Checked.AddDays(-1), rows[0].ErrorLogCheckDbTime);
        Assert.Equal(1, rows[0].ErrorLogCheckDbErrors);
    }

    [Fact]
    public async Task ManagedInstanceAndAzure_HaveNoPlan_AndAreNotApplicable()
    {
        foreach (var target in new[] { new CollectorTargetInfo { IsAzureSqlDb = true }, new CollectorTargetInfo { IsAzureManagedInstance = true } })
        {
            var table = Shape();
            Add(table, "AppDb", "FULL", Checked, Full, Log);
            var context = ContextWith(new Dictionary<string, string>(), target);

            var row = Assert.Single(await ReadWithAsync(context, table, null));

            Assert.Null(context.DatabaseProtectionErrorLogPlan);
            Assert.Empty(context.PendingState);
            Assert.Equal(DatabaseProtectionCollector.ErrorLogNotApplicable, row.ErrorLogState);
            Assert.Null(row.ErrorLogMessage);
            Assert.Null(row.ErrorLogBackfillComplete);
            Assert.Null(row.ReplicaReadable);
        }
    }

    [Fact]
    public async Task ARefusedListing_PlansTheCurrentLogOnly_AndTheBackfillIsComplete()
    {
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var context = ContextWith(new Dictionary<string, string>());

        var row = Assert.Single(await ReadWithAsync(context, table, RefusedList()));

        var plan = context.DatabaseProtectionErrorLogPlan!;
        Assert.False(plan.EnumerateOk);
        Assert.Equal(new[] { 0 }, plan.Read.Select(a => a.Archive).ToArray());
        Assert.True(row.ErrorLogBackfillComplete);
        Assert.Equal(new[] { DatabaseProtectionErrorLogState.InstanceKey }, context.PendingState.Keys.ToArray());
    }

    private static readonly string[] MarkerKeys =
    {
        DatabaseProtectionErrorLogState.NewestArchiveKey, DatabaseProtectionErrorLogState.BackfillBeforeKey, DatabaseProtectionErrorLogState.BackfillDoneKey,
        DatabaseProtectionErrorLogState.AttemptsKey, DatabaseProtectionErrorLogState.SkippedKey,
    };

    [Fact]
    public async Task AListRefusedForSomeCycles_ThenReadable_StillBackfillsEveryArchiveNewestFirst()
    {
        /* #8865 I2: a refused list must not end the backfill. Before the fix the refused cycles stored the backfill as done with no steady
           marker, so the first list that worked took its newest archive as already read and no retained archive was ever read. */
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log, role: "SECONDARY");
        var state = new Dictionary<string, string>();
        for (var cycle = 0; cycle < 3; cycle++)
        {
            var refusedCycle = ContextWith(state);
            var rows = await ReadWithAsync(refusedCycle, table, RefusedList());
            await ApplyAsync(refusedCycle, rows, Status((0, true)), LineShape());

            Assert.True(rows[0].ErrorLogBackfillComplete);
            Assert.All(MarkerKeys, k => Assert.DoesNotContain(k, refusedCycle.PendingState.Keys));
            state = Carry(refusedCycle, state);
        }

        var plans = new List<int[]>();
        bool? complete = null;
        for (var cycle = 0; cycle < 3 && complete != true; cycle++)
        {
            var context = ContextWith(state);
            var rows = await ReadWithAsync(context, table, Logs(7));
            var plan = context.DatabaseProtectionErrorLogPlan!;
            plans.Add(plan.Read.Select(a => a.Archive).ToArray());
            await ApplyAsync(context, rows, Status(plan.Read.Select(a => (a.Archive, true)).ToArray()), LineShape());
            complete = rows[0].ErrorLogBackfillComplete;
            state = Carry(context, state);
        }

        Assert.Equal(new[] { new[] { 0, 1, 2, 3 }, new[] { 0, 4, 5, 6 } }, plans);
        Assert.True(complete);
    }

    [Fact]
    public async Task AResetCycleWithARefusedList_StillOverwritesTheOldNodesMarkers()
    {
        /* R7 holds when the list is refused too: the old node's markers must not survive into the new node's first readable list. */
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var state = new Dictionary<string, string>
        {
            [DatabaseProtectionErrorLogState.InstanceKey] = "OLDNODE",
            [DatabaseProtectionErrorLogState.NewestArchiveKey] = DatabaseProtectionErrorLogState.FormatDates(new[] { Checked }),
            [DatabaseProtectionErrorLogState.BackfillDoneKey] = bool.TrueString,
        };
        var context = ContextWith(state);

        await ReadWithAsync(context, table, RefusedList());

        Assert.Equal(string.Empty, context.PendingState[DatabaseProtectionErrorLogState.NewestArchiveKey]);
        Assert.Equal(bool.FalseString, context.PendingState[DatabaseProtectionErrorLogState.BackfillDoneKey]);
    }

    [Fact]
    public async Task AListThatFailsForAnotherReason_IsNamedInTheMessage_AndARefusedListIsNot()
    {
        /* #8865 C1: the CATCH returns the error number, so a list that failed (8114, a date the session could not convert) is not read as a
           refusal, which is the documented, silent current-log-only fallback. */
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);

        var failed = ContextWith(SettledState());
        var failedRows = await ReadWithAsync(failed, table, RefusedList(8114));
        await ApplyAsync(failed, failedRows, Status((0, true)), LineShape());
        Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, failedRows[0].ErrorLogState);
        Assert.Contains("8114", failedRows[0].ErrorLogMessage, StringComparison.Ordinal);

        var refused = ContextWith(SettledState());
        var refusedRows = await ReadWithAsync(refused, table, RefusedList());
        await ApplyAsync(refused, refusedRows, Status((0, true)), LineShape());
        Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, refusedRows[0].ErrorLogState);
        Assert.Null(refusedRows[0].ErrorLogMessage);
    }

    [Fact]
    public async Task AListWithADateThatCouldNotBeConverted_IsUnreadable_AndMovesNoMarker()
    {
        var table = Shape();
        Add(table, "Sales", "FULL", Checked, Full, Log);
        var logs = Logs(4);
        logs.Rows[2]["log_date"] = DBNull.Value;
        var context = ContextWith(new Dictionary<string, string>());

        await ReadWithAsync(context, table, logs);

        var plan = context.DatabaseProtectionErrorLogPlan!;
        Assert.False(plan.EnumerateOk);
        Assert.Equal(new[] { 0 }, plan.Read.Select(a => a.Archive).ToArray());
        Assert.All(MarkerKeys, k => Assert.DoesNotContain(k, context.PendingState.Keys));
    }

    [Fact]
    public async Task AReplicaThatIsNotReadable_ReadsReplicaReadableFalse()
    {
        var table = Shape();
        Add(table, "NoConn", "FULL", Checked, Full, Log, role: "SECONDARY", allowConnections: "NO");
        Add(table, "ReadOnly", "FULL", Checked, Full, Log, role: "SECONDARY", allowConnections: "READ_ONLY");
        Add(table, "AllConn", "FULL", Checked, Full, Log, role: "SECONDARY", allowConnections: "ALL");
        Add(table, "Standalone", "FULL", Checked, Full, Log);
        Add(table, "PrimaryNoConn", "FULL", Checked, Full, Log, role: "PRIMARY", allowConnections: "NO");

        var rows = await ReadWithAsync(ContextWith(new Dictionary<string, string>()), table, Logs(1));

        Assert.False(rows[0].ReplicaReadable);
        Assert.True(rows[1].ReplicaReadable);
        Assert.True(rows[2].ReplicaReadable);
        Assert.Null(rows[3].ReplicaReadable);
        /* R8: the setting is the secondary role's, so a replica that is primary right now says nothing. */
        Assert.Null(rows[4].ReplicaReadable);
    }

    /* #8865: the supplemental's two result sets. The first is one row per log read (archive, ok, error_number, message), the second the newest
       line per head (line_time_utc, line_text). */
    private static DataTable StatusShape()
    {
        var table = new DataTable();
        table.Columns.Add("archive", typeof(int));
        table.Columns.Add("ok", typeof(bool));
        table.Columns.Add("error_number", typeof(int));
        table.Columns.Add("message", typeof(string));
        return table;
    }

    /* The newest line per head the server kept (line_time_utc, line_text); the collector matches them to its rows by name. */
    private static DataTable LineShape()
    {
        var table = new DataTable();
        table.Columns.Add("line_time_utc", typeof(DateTime));
        table.Columns.Add("line_text", typeof(string));
        return table;
    }

    private static string CheckLine(string database, int errors = 0) =>
        $"DBCC CHECKDB ({database}) WITH NO_INFOMSGS executed by someone found {errors} errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 1 seconds.";

    private static DataTable Status(params (int Archive, bool Ok)[] logs)
    {
        var table = StatusShape();
        foreach (var (archive, ok) in logs)
        {
            table.Rows.Add(archive, ok, ok ? DBNull.Value : 21, ok ? DBNull.Value : "boom");
        }

        return table;
    }

    private static async Task ApplyAsync(CollectorContext context, List<DatabaseProtectionCollector.Row> rows, DataTable status, DataTable lines)
    {
        using var reader = new DataTableReader(new[] { status, lines });
        await DatabaseProtectionCollector.Instance.ApplySupplementalAsync(rows, reader, context, TestContext.Current.CancellationToken);
    }

    /* The state of an instance that has read everything: a cycle over one log (the current one) then stages nothing but a changed check. */
    private static Dictionary<string, string> SettledState() => new()
    {
        [DatabaseProtectionErrorLogState.InstanceKey] = "INST1",
        [DatabaseProtectionErrorLogState.NewestArchiveKey] = DatabaseProtectionErrorLogState.FormatNewestArchive(new DatabaseProtectionErrorLogState.Markers(DateTime.MinValue, null, true)),
        [DatabaseProtectionErrorLogState.BackfillDoneKey] = bool.TrueString,
    };

    [Fact]
    public void TheSupplementalQuery_IsNullWithNoPlan_AndOtherwiseListsTheArchivesCurrentFirst()
    {
        Assert.Null(DatabaseProtectionCollector.Instance.BuildSupplementalQuery(Context()));
        var context = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "s", Deltas = new CollectorDeltaCalculator(),
            ExcludedDatabases = new[] { "skipme" },
        };
        context.DatabaseProtectionErrorLogPlan = DatabaseProtectionErrorLogState.PlanArchives(
            DatabaseProtectionErrorLogState.Load(context.State, "INST1"),
            new[] { new DatabaseProtectionErrorLogState.ArchiveInfo(0, Checked), new DatabaseProtectionErrorLogState.ArchiveInfo(1, Checked.AddDays(-1)) },
            true, 3);

        var query = DatabaseProtectionCollector.Instance.BuildSupplementalQuery(context)!;

        /* The excluded list is not bound: the lines are matched to the primary rows, which already left those databases out. */
        Assert.Equal("0,1", Assert.Single(query.Parameters).Value);
    }

    [Fact]
    public async Task AnAbnormalTerminationLine_DoesNotShadowTheGoodLineBeforeIt_AndIsNotCountedAsUnparsed()
    {
        /* #8865 I5: message 5235 starts like an 8957 line, and before the fix it was taken as the newest check and counted as unparsed. */
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var context = ContextWith(SettledState());
        var rows = await ReadWithAsync(context, table, Logs(1));
        var lines = LineShape();
        lines.Rows.Add(Checked.AddDays(-3), CheckLine("A", 2));
        lines.Rows.Add(Checked.AddDays(-1), "DBCC CHECKDB (A) WITH NO_INFOMSGS executed by someone terminated abnormally due to error state 5. Elapsed time: 0 hours 0 minutes 0 seconds.");

        await ApplyAsync(context, rows, Status((0, true)), lines);

        Assert.Equal(Checked.AddDays(-3), rows[0].ErrorLogCheckDbTime);
        Assert.Equal(2, rows[0].ErrorLogCheckDbErrors);
        Assert.Equal(0, rows[0].ErrorLogUnparsedLines);
    }

    [Fact]
    public async Task ANoIndexLine_IsMatched_AndTheNewestLineOfADatabaseWinsAcrossHeads()
    {
        /* #8865 I6: NOINDEX is logged inside the parentheses, "DBCC CHECKDB (A, noindex)", which the ")"-after-the-name match missed. The
           server keeps one line per head, so a database can come back with several (here a full check and a later NOINDEX one). A newer
           line about a database whose name holds ", " ("A, Inc") is that database's alone, not A's too. */
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        Add(table, "A, Inc", "FULL", null, Full, Log);
        var context = ContextWith(SettledState());
        var rows = await ReadWithAsync(context, table, Logs(1));
        var lines = LineShape();
        lines.Rows.Add(Checked.AddDays(-4), CheckLine("A"));
        lines.Rows.Add(Checked.AddDays(-2), "DBCC CHECKDB (A, noindex) WITH no_infomsgs executed by sa found 0 errors and repaired 0 errors. Elapsed time: 0 hours 0 minutes 0 seconds.");
        lines.Rows.Add(Checked.AddDays(-1), CheckLine("A, Inc", 4));

        await ApplyAsync(context, rows, Status((0, true)), lines);

        Assert.Equal(Checked.AddDays(-2), rows[0].ErrorLogCheckDbTime);
        Assert.Equal("full,noindex", rows[0].ErrorLogCheckDbOptions);
        Assert.Equal(0, rows[0].ErrorLogCheckDbErrors);
        Assert.Equal(Checked.AddDays(-1), rows[1].ErrorLogCheckDbTime);
        Assert.Equal(4, rows[1].ErrorLogCheckDbErrors);
        Assert.Equal(0, rows[0].ErrorLogUnparsedLines);
    }

    [Fact]
    public async Task ANewLine_ReplacesTheRememberedCheck_AndStagesOnlyItsKey()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        Add(table, "B", "FULL", null, Full, Log);
        var state = SettledState();
        state[DatabaseProtectionErrorLogState.CheckKeyPrefix + "A"] = CheckValue(Checked.AddDays(-10), "full");
        state[DatabaseProtectionErrorLogState.CheckKeyPrefix + "B"] = CheckValue(Checked.AddDays(-2), "full");
        var context = ContextWith(state);
        var rows = await ReadWithAsync(context, table, Logs(1));
        var lines = LineShape();
        lines.Rows.Add(Checked.AddDays(-1), CheckLine("A", 3));

        await ApplyAsync(context, rows, Status((0, true)), lines);

        Assert.Equal(Checked.AddDays(-1), rows[0].ErrorLogCheckDbTime);
        Assert.Equal("full", rows[0].ErrorLogCheckDbOptions);
        Assert.Equal(3, rows[0].ErrorLogCheckDbErrors);
        Assert.Equal(Checked.AddDays(-2), rows[1].ErrorLogCheckDbTime);
        Assert.All(rows, r => Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, r.ErrorLogState));
        Assert.All(rows, r => Assert.Null(r.ErrorLogMessage));
        Assert.All(rows, r => Assert.Equal(0, r.ErrorLogUnparsedLines));
        Assert.Equal(new[] { DatabaseProtectionErrorLogState.CheckKeyPrefix + "A" }, context.PendingState.Keys.ToArray());
        Assert.Equal(CheckValue(Checked.AddDays(-1), "full", 3), context.PendingState[DatabaseProtectionErrorLogState.CheckKeyPrefix + "A"]);
    }

    [Fact]
    public async Task AQuietCycle_StagesNothing()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var state = SettledState();
        state[DatabaseProtectionErrorLogState.CheckKeyPrefix + "A"] = CheckValue(Checked.AddDays(-1), "full");
        var context = ContextWith(state);
        var rows = await ReadWithAsync(context, table, Logs(1));
        Assert.Empty(context.PendingState);
        var lines = LineShape();
        lines.Rows.Add(Checked.AddDays(-1), CheckLine("A"));

        await ApplyAsync(context, rows, Status((0, true)), lines);

        Assert.Empty(context.PendingState);
        Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, rows[0].ErrorLogState);
    }

    [Fact]
    public async Task AnUnparseableLine_IsCountedAndNotUsed()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        Add(table, "B", "FULL", null, Full, Log);
        var context = ContextWith(SettledState());
        var rows = await ReadWithAsync(context, table, Logs(1));
        var lines = LineShape();
        lines.Rows.Add(Checked.AddDays(-1), "DBCC CHECKDB (A) executed by someone, in a language we cannot read");
        lines.Rows.Add(Checked.AddDays(-1), CheckLine("B"));

        await ApplyAsync(context, rows, Status((0, true)), lines);

        Assert.All(rows, r => Assert.Equal(1, r.ErrorLogUnparsedLines));
        Assert.Null(rows[0].ErrorLogCheckDbTime);
        Assert.Equal(Checked.AddDays(-1), rows[1].ErrorLogCheckDbTime);
        Assert.Equal(new[] { DatabaseProtectionErrorLogState.CheckKeyPrefix + "B", DatabaseProtectionErrorLogState.UnparsedKey }, context.PendingState.Keys.Order(StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public async Task ALineInAnotherLanguage_IsCountedUnparsed_AndTheCountHoldsUntilALineParses()
    {
        /* #8865: on an instance whose error log is not English the 8957 line has no "executed by", so before the fix the server never
           returned it and the count stayed 0. Some languages start the line with the command (German), others put the login first
           (Japanese) or between the command and the name (Turkish). A line about a database this collector does not list is not ours,
           and an English corruption advisory that names the command is not a completion: neither is counted. */
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log, role: "PRIMARY");
        Add(table, "B", "FULL", null, Full, Log, role: "PRIMARY");
        Add(table, "C", "FULL", null, Full, Log, role: "PRIMARY");
        var state = SettledState();
        var context = ContextWith(state);
        var rows = await ReadWithAsync(context, table, Logs(1));
        var lines = LineShape();
        lines.Rows.Add(Checked.AddDays(-1), "DBCC CHECKDB (A) WITH physical_only wurde von sa ausgeführt und hat 0 Fehler gefunden und 0 Fehler repariert.");
        lines.Rows.Add(Checked.AddDays(-1), "sa から実行された DBCC CHECKDB (B) では、0 個のエラーが見つかり、0 個のエラーが修復されました。経過時間: 0 時間 0 分 0 秒。");
        lines.Rows.Add(Checked.AddDays(-1), "DBCC CHECKDB sa tarafından yürütülen (C) 0 hata buldu ve 0 hatayı onardı. Geçen süre: 0 saat 0 dakika 0 saniye.");
        lines.Rows.Add(Checked.AddDays(-1), "DBCC CHECKDB (Ventes) a été exécuté par sa et a trouvé 0 erreurs et réparé 0 erreurs.");
        lines.Rows.Add(Checked.AddDays(-1), "Sort operation failed during an index build. The overwriting of the allocation page in database 'A' was prevented by terminating the sort. Run DBCC CHECKDB to check for allocation and consistency errors.");
        lines.Rows.Add(Checked.AddDays(-1), CheckLine("NotListed"));

        await ApplyAsync(context, rows, Status((0, true)), lines);

        Assert.All(rows, r => Assert.Equal(3, r.ErrorLogUnparsedLines));
        Assert.Null(rows[0].ErrorLogCheckDbTime);

        /* The log rolls over and the next cycles read no CHECKDB line: the instance still writes lines this build cannot read, so the
           count is remembered rather than dropping to 0 (which would read every AG database as never checked). */
        state = Carry(context, state);
        context = ContextWith(state);
        rows = await ReadWithAsync(context, table, Logs(1));
        Assert.Equal(3, rows[0].ErrorLogUnparsedLines);
        await ApplyAsync(context, rows, Status((0, true)), LineShape());
        Assert.Equal(3, rows[0].ErrorLogUnparsedLines);
        Assert.Empty(context.PendingState);

        /* A cycle that parses a line and finds none unreadable clears it. */
        state = Carry(context, state);
        context = ContextWith(state);
        rows = await ReadWithAsync(context, table, Logs(1));
        lines = LineShape();
        lines.Rows.Add(Checked, CheckLine("A"));
        await ApplyAsync(context, rows, Status((0, true)), lines);
        Assert.Equal(0, rows[0].ErrorLogUnparsedLines);
        Assert.Equal(Checked, rows[0].ErrorLogCheckDbTime);
        Assert.Equal(string.Empty, Carry(context, state).GetValueOrDefault(DatabaseProtectionErrorLogState.UnparsedKey, string.Empty));
    }

    [Fact]
    public async Task ArchivesNumberedFromAHundred_AreNeverPlanned_AndTheBackfillEndsWithoutThem()
    {
        /* #8865: only SQL Server on Linux keeps more than 99 archives, and reading archive 100 or higher fails the whole batch with a
           severe error through SqlClient, so the archives past 99 are left out of the list as if the instance did not retain them. */
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var logs = LogShape();
        foreach (var archive in new[] { 0, 1, 2, 99, 100, 101 })
        {
            logs.Rows.Add(true, archive, Checked.AddDays(-archive), DBNull.Value);
        }

        var state = new Dictionary<string, string>(StringComparer.Ordinal);
        var planned = new List<int>();
        DatabaseProtectionCollector.Row row = default;
        for (var cycle = 0; cycle < 5; cycle++)
        {
            var context = ContextWith(state);
            var rows = await ReadWithAsync(context, table, logs);
            var plan = context.DatabaseProtectionErrorLogPlan!;
            planned.AddRange(plan.Read.Select(a => a.Archive));
            await ApplyAsync(context, rows, Status(plan.Read.Select(a => (a.Archive, true)).ToArray()), LineShape());
            row = rows[0];
            state = Carry(context, state);
            if (row.ErrorLogBackfillComplete == true)
            {
                break;
            }
        }

        Assert.True(row.ErrorLogBackfillComplete);
        Assert.Contains(99, planned);
        Assert.DoesNotContain(planned, a => a >= 100);
    }

    [Fact]
    public async Task ARefusedCurrentLog_IsRefusedWithTheGrant()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var state = SettledState();
        state[DatabaseProtectionErrorLogState.CheckKeyPrefix + "A"] = CheckValue(Checked.AddDays(-1), "full");
        var context = ContextWith(state);
        var rows = await ReadWithAsync(context, table, Logs(1));
        var status = StatusShape();
        status.Rows.Add(0, false, 27219, "Caller does not have permissions to execute the stored procedure.");

        await ApplyAsync(context, rows, status, LineShape());

        var row = Assert.Single(rows);
        Assert.Equal(DatabaseProtectionCollector.ErrorLogRefused, row.ErrorLogState);
        Assert.Equal("VIEW SERVER STATE", row.ErrorLogGrant);
        Assert.False(string.IsNullOrEmpty(row.ErrorLogMessage));
        Assert.Equal(Checked.AddDays(-1), row.ErrorLogCheckDbTime);
    }

    [Fact]
    public async Task ACurrentLogThatFailsForAnotherReason_IsFailedNotRefused()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var context = ContextWith(SettledState());
        var rows = await ReadWithAsync(context, table, Logs(1));

        await ApplyAsync(context, rows, Status((0, false)), LineShape());

        var row = Assert.Single(rows);
        Assert.Equal(DatabaseProtectionCollector.ErrorLogFailed, row.ErrorLogState);
        Assert.Null(row.ErrorLogGrant);
        Assert.False(string.IsNullOrEmpty(row.ErrorLogMessage));
    }

    [Fact]
    public async Task ASuccessfulArchive_ClearsItsAttempt_AndAFailedOneKeepsIt()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var context = ContextWith(new Dictionary<string, string>());
        var rows = await ReadWithAsync(context, table, Logs(4));
        Assert.Equal(
            new[] { 0, 1, 2, 3 },
            context.DatabaseProtectionErrorLogPlan!.Read.Select(a => a.Archive).ToArray());
        Assert.Equal(3, DatabaseProtectionErrorLogState.ParseAttempts(context.PendingState[DatabaseProtectionErrorLogState.AttemptsKey]).Count);

        await ApplyAsync(context, rows, Status((0, true), (1, true), (2, false), (3, true)), LineShape());

        Assert.Equal(
            DatabaseProtectionErrorLogState.FormatAttempts(new Dictionary<DateTime, int> { [Checked.AddDays(-2)] = 1 }),
            context.PendingState[DatabaseProtectionErrorLogState.AttemptsKey]);
        /* A first run backfills newest first: the backfill marker moved over archive 1 only, because archive 2 failed and is retried, never jumped over. */
        Assert.Equal(
            DatabaseProtectionErrorLogState.FormatDates(new[] { Checked.AddDays(-1) }),
            context.PendingState[DatabaseProtectionErrorLogState.BackfillBeforeKey]);
        Assert.False(context.PendingState.ContainsKey(DatabaseProtectionErrorLogState.BackfillDoneKey) && context.PendingState[DatabaseProtectionErrorLogState.BackfillDoneKey] == bool.TrueString);
        Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, rows[0].ErrorLogState);
        Assert.False(string.IsNullOrEmpty(rows[0].ErrorLogMessage));
    }

    [Fact]
    public async Task ARefusedCurrentLog_DoesNotCountAttempts_OnOlderArchives()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var state = new Dictionary<string, string>();
        for (var cycle = 0; cycle < 3; cycle++)
        {
            var context = ContextWith(state);
            var rows = await ReadWithAsync(context, table, Logs(4));
            var status = StatusShape();
            foreach (var archive in new[] { 0, 1, 2, 3 })
            {
                status.Rows.Add(archive, false, 27219, "Caller does not have permissions to execute the stored procedure.");
            }

            await ApplyAsync(context, rows, status, LineShape());

            Assert.Equal(DatabaseProtectionCollector.ErrorLogRefused, rows[0].ErrorLogState);
            Assert.False(context.PendingState.ContainsKey(DatabaseProtectionErrorLogState.AttemptsKey), "cycle " + cycle);
            Assert.False(context.PendingState.ContainsKey(DatabaseProtectionErrorLogState.SkippedKey), "cycle " + cycle);
            Assert.Equal(new[] { 0, 1, 2, 3 }, context.DatabaseProtectionErrorLogPlan!.Read.Select(a => a.Archive).ToArray());
            state = Carry(context, state);
        }
    }

    [Fact]
    public async Task AnArchiveWithAPermissionError_KeepsItsLoadedAttempts_AndOtherFailuresStillCount()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var context = ContextWith(new Dictionary<string, string>());
        var rows = await ReadWithAsync(context, table, Logs(4));
        var status = StatusShape();
        status.Rows.Add(0, true, DBNull.Value, DBNull.Value);
        status.Rows.Add(1, false, 229, "denied");
        status.Rows.Add(2, false, 21, "boom");
        status.Rows.Add(3, true, DBNull.Value, DBNull.Value);

        await ApplyAsync(context, rows, status, LineShape());

        Assert.Equal(
            DatabaseProtectionErrorLogState.FormatAttempts(new Dictionary<DateTime, int> { [Checked.AddDays(-2)] = 1 }),
            context.PendingState[DatabaseProtectionErrorLogState.AttemptsKey]);
    }

    [Fact]
    public async Task ArchivesSharingADate_CountOneAttempt_AndOneFailingKeepsTheDateUnread()
    {
        /* #8865 I1: three archives cycled in one minute. They are one date: one attempt, and the date is read only when all three read. */
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var logs = LogShape();
        logs.Rows.Add(true, 0, Checked, DBNull.Value);
        foreach (var archive in new[] { 1, 2, 3 })
        {
            logs.Rows.Add(true, archive, Checked.AddDays(-1), DBNull.Value);
        }

        var context = ContextWith(new Dictionary<string, string>());
        var rows = await ReadWithAsync(context, table, logs);
        var oneAttempt = DatabaseProtectionErrorLogState.FormatAttempts(new Dictionary<DateTime, int> { [Checked.AddDays(-1)] = 1 });
        Assert.Equal(oneAttempt, context.PendingState[DatabaseProtectionErrorLogState.AttemptsKey]);

        await ApplyAsync(context, rows, Status((0, true), (1, true), (2, false), (3, true)), LineShape());

        Assert.Equal(oneAttempt, context.PendingState[DatabaseProtectionErrorLogState.AttemptsKey]);
        Assert.False(rows[0].ErrorLogBackfillComplete);
    }

    [Fact]
    public async Task SkipsAndAttemptsForArchivesThatAgedOut_AreDropped_WhenTheListIsRead()
    {
        /* #8865 M1: a skipped archive that later rolled off kept its "skipped archives" note for good, and nothing could clear it. */
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var agedOut = Checked.AddDays(-30);
        var stillListed = Checked.AddDays(-2);
        var state = SettledState();
        state[DatabaseProtectionErrorLogState.NewestArchiveKey] = DatabaseProtectionErrorLogState.FormatNewestArchive(
            new DatabaseProtectionErrorLogState.Markers(Checked.AddDays(-1), null, true, 1));
        state[DatabaseProtectionErrorLogState.SkippedKey] = DatabaseProtectionErrorLogState.FormatDates(new[] { agedOut, stillListed });
        state[DatabaseProtectionErrorLogState.AttemptsKey] = DatabaseProtectionErrorLogState.FormatAttempts(new Dictionary<DateTime, int> { [agedOut] = 1 });
        var context = ContextWith(state);

        var row = Assert.Single(await ReadWithAsync(context, table, Logs(3)));

        Assert.Equal(DatabaseProtectionErrorLogState.FormatDates(new[] { stillListed }), context.PendingState[DatabaseProtectionErrorLogState.SkippedKey]);
        Assert.Equal(string.Empty, context.PendingState[DatabaseProtectionErrorLogState.AttemptsKey]);
        Assert.Equal(DatabaseProtectionErrorLogState.FormatDates(new[] { stillListed }), row.ErrorLogSkippedArchives);

        /* A list that was not read trims nothing. */
        var refused = ContextWith(state);
        await ReadWithAsync(refused, table, RefusedList());
        Assert.DoesNotContain(DatabaseProtectionErrorLogState.SkippedKey, refused.PendingState.Keys);
    }

    [Fact]
    public async Task AnAttemptThatSucceeds_IsUnstaged_WhenStateNeverHeldIt()
    {
        var table = Shape();
        Add(table, "A", "FULL", null, Full, Log);
        var context = ContextWith(new Dictionary<string, string>());
        var rows = await ReadWithAsync(context, table, Logs(2));
        Assert.Contains(DatabaseProtectionErrorLogState.AttemptsKey, context.PendingState.Keys);

        await ApplyAsync(context, rows, Status((0, true), (1, true)), LineShape());

        /* The read staged the attempt in advance; the archive read fine, so the value equals what state holds (nothing) and the stale entry must go. */
        Assert.DoesNotContain(DatabaseProtectionErrorLogState.AttemptsKey, context.PendingState.Keys);
    }

    [Fact]
    public void TheQuery_IsTheAzureOneOnAzureSqlDatabase_AndTheMsdbOneElsewhere()
    {
        var azure = DatabaseProtectionCollector.Instance.BuildQuery(Context(new CollectorTargetInfo { IsAzureSqlDb = true })).Text;
        var other = DatabaseProtectionCollector.Instance.BuildQuery(Context(new CollectorTargetInfo { IsAzureManagedInstance = true })).Text;

        /* Azure SQL Database has no msdb, so it reads sys.dm_database_backups; every other shape reads the backupset in msdb. */
        Assert.DoesNotContain("msdb.", azure, StringComparison.Ordinal);
        Assert.Contains("msdb.dbo.backupset", other, StringComparison.Ordinal);
        Assert.DoesNotContain("dm_database_backups", other, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCollector_IsRegistered_Hourly_NinetyDays_AsAPlainDarlingOnlyTable_ThatRunsOnEveryShape()
    {
        var instance = DatabaseProtectionCollector.Instance;
        Assert.Contains(instance, CollectorCatalog.All);
        Assert.Equal(60, CollectorScheduleDefaults.All["database_protection"].FrequencyMinutes);
        Assert.Equal(90, CollectorScheduleDefaults.All["database_protection"].RetentionDays);
        Assert.True(instance.StaysPlainTable);
        Assert.True(instance.IsDarlingOnly);
        Assert.True(CollectorCatalog.YieldsOnLockTimeout("database_protection"));
        Assert.False(instance.RunsPerDatabase(new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAzureSqlDb = true }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAwsRds = true }));
        Assert.True(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { IsAzureManagedInstance = true }));
        Assert.False(CollectorCatalog.AppliesTo(instance, new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));
        Assert.True(instance.CommandTimeoutSecondsOverride is > 0 and <= 300);
    }

    [Fact]
    public void TheTableAfterV223AndTheNewRung_IsTheGeneratorsOutput()
    {
        var v223 = PgMigrations.Scripts.Single(m => m.Version == 223).Sql.Replace("collect.", "", StringComparison.Ordinal);
        var rung = PgMigrations.Scripts.Single(m => m.Version == 278);
        Assert.Equal("database-protection-error-log", rung.Name);

        static string Norm(string s) => string.Join(' ', s.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries));

        /* #8865: the table is V223's CREATE TABLE with the new rung's ADD COLUMNs folded in, in order, so a fresh store (the generator)
           and an upgraded one (V223 then the rung) have one physical column order, which the positional binary COPY needs. */
        var columns = System.Text.RegularExpressions.Regex.Matches(
                rung.Sql, @"ADD COLUMN IF NOT EXISTS (\w+ [a-z]+)", System.Text.RegularExpressions.RegexOptions.None, TimeSpan.FromSeconds(5))
            .Select(m => m.Groups[1].Value)
            .ToList();
        Assert.Equal(10, columns.Count);

        var v223Table = Norm(v223);
        var close = v223Table.IndexOf(");", StringComparison.Ordinal);
        var folded = v223Table[..close].TrimEnd() + ", " + string.Join(", ", columns) + " " + v223Table[close..];

        var generated = PgSchemaGenerator.CreateTable(DatabaseProtectionCollector.Instance) + "\n\n" + PgSchemaGenerator.CreateIndex(DatabaseProtectionCollector.Instance);

        /* The V223 rung also adds the three alert switches to config_alert_settings, so the table and its index are its start, not all of it. */
        Assert.StartsWith(Norm(generated).TrimEnd(';'), folded, StringComparison.Ordinal);
    }
}

/// <summary>
/// #6873: the rows land in the V223 table through the product's own writer, and read back as written.
/// </summary>
[Collection("live-postgres")]
public sealed class DatabaseProtectionStoreLiveTests
{
    [Fact]
    public async Task RowsWrittenThroughTheCollectorsWriter_LandInTheTableWithTheirStates()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the database protection store test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct, emptyStore: true);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var definition = DatabaseProtectionCollector.Instance;
        var at = new DateTime(2026, 10, 28, 12, 0, 0, DateTimeKind.Unspecified);
        var context = new CollectorContext
        {
            CollectionTime = at, ServerId = 7, ServerName = "protect", Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { IsAzureSqlDb = true },
        };

        var rows = new List<DatabaseProtectionCollector.Row>
        {
            new("AppDb", "ONLINE", "FULL", at.AddYears(-1), null, DatabaseProtectionCollector.PlatformManaged,
                null, DatabaseProtectionCollector.CheckDbNever, null, DatabaseProtectionCollector.BackupUnavailable,
                null, DatabaseProtectionCollector.BackupUnavailable,
                DatabaseProtectionCollector.ErrorLogNotApplicable, null, null, null, null, null, null, null, null, null),
            new("Other", "ONLINE", "SIMPLE", at.AddYears(-2), "PRIMARY", DatabaseProtectionCollector.PlatformManaged,
                at.AddDays(-3), DatabaseProtectionCollector.CheckDbChecked, at.AddDays(-1), DatabaseProtectionCollector.BackupRecorded,
                null, DatabaseProtectionCollector.BackupNotExpected,
                DatabaseProtectionCollector.ErrorLogRead, "read ok", "grant exec", true, "2 archives", 3,
                at.AddDays(-2), "NO_INFOMSGS", 0, true),
        };

        var writer = new PgCollectorRowWriter();
        await using (var transaction = await connection.BeginTransactionAsync(ct))
        {
            using (var importer = await connection.BeginBinaryImportAsync(PgCollectorRowWriter.CopyCommandFor(definition), ct))
            {
                writer.Importer = importer;
                foreach (var row in rows)
                {
                    await importer.StartRowAsync(ct);
                    writer.Value(CollectionIdGenerator.Next());
                    writer.Value(at).Value(7).Value("protect");
                    writer.BeginPayload();
                    definition.WritePayload(row, writer, context);
                    writer.EndPayload(definition.PayloadColumns.Count);
                }

                await importer.CompleteAsync(ct);
            }

            await transaction.CommitAsync(ct);
        }

        await using var read = new NpgsqlCommand(
            "SELECT database_name, platform, checkdb_state, full_backup_state, log_backup_state, last_full_backup_time, last_good_checkdb_time, errorlog_state, errorlog_message, errorlog_grant, errorlog_backfill_complete, errorlog_skipped_archives, errorlog_unparsed_lines, errorlog_checkdb_time, errorlog_checkdb_options, errorlog_checkdb_errors, replica_readable FROM collect.database_protection WHERE server_id = 7 ORDER BY database_name", connection);
        await using var reader = await read.ExecuteReaderAsync(ct);

        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal("AppDb", reader.GetString(0));
        Assert.Equal("platform_managed", reader.GetString(1));
        Assert.Equal("never", reader.GetString(2));
        Assert.Equal("unavailable", reader.GetString(3));
        Assert.True(reader.IsDBNull(5));
        Assert.Equal("not_applicable", reader.GetString(7));
        for (var i = 8; i <= 16; i++)
        {
            Assert.True(reader.IsDBNull(i), "column " + i);
        }

        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal("Other", reader.GetString(0));
        Assert.Equal("checked", reader.GetString(2));
        Assert.Equal("recorded", reader.GetString(3));
        Assert.Equal("not_expected", reader.GetString(4));
        Assert.Equal(at.AddDays(-1), reader.GetDateTime(5));
        Assert.Equal(at.AddDays(-3), reader.GetDateTime(6));
        Assert.Equal("read", reader.GetString(7));
        Assert.Equal("read ok", reader.GetString(8));
        Assert.Equal("grant exec", reader.GetString(9));
        Assert.True(reader.GetBoolean(10));
        Assert.Equal("2 archives", reader.GetString(11));
        Assert.Equal(3, reader.GetInt32(12));
        Assert.Equal(at.AddDays(-2), reader.GetDateTime(13));
        Assert.Equal("NO_INFOMSGS", reader.GetString(14));
        Assert.Equal(0, reader.GetInt32(15));
        Assert.True(reader.GetBoolean(16));
        Assert.False(await reader.ReadAsync(ct));
    }
}

/// <summary>
/// #6873: the collector's real SQL Server batch against a live instance, gated on <c>DARLING_TEST_SQL</c> (optional
/// <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c>). It writes nothing to msdb: the batch's <c>msdb.dbo.backupset</c> is
/// pointed at a session temp table, so a copy_only backup (which counts, #8771) and a backup older than the 60-day window can be put in front of the real
/// query, which the fake-reader tests cannot reach.
/// </summary>
public sealed class DatabaseProtectionLiveSqlTests
{
    private static string? ConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host, InitialCatalog = "master", TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional, ApplicationName = "database-protection-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    [Fact]
    public async Task TheRealBatch_CountsCopyOnlyBackups_LeavesOutOldOnes_TakesTheNewestOfEachType_AndConvertsToUtc()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync(ct);

        /* The temp table carries the same five columns the batch reads, named for a database every instance has. A NULL time_zone
           means the backup did not record one, so the offset in force now applies. */
        await using (var setup = new SqlCommand(@"
CREATE TABLE #backupset (database_name nvarchar(128), type char(1), backup_finish_date datetime, is_copy_only bit, time_zone smallint);
INSERT #backupset VALUES
    (N'master', 'D', DATEADD(DAY, -2, GETDATE()), 0, NULL),
    (N'master', 'D', DATEADD(DAY, -1, GETDATE()), 1, NULL),   /* newer and copy_only: counts, so it wins (#8771) */
    (N'master', 'L', DATEADD(DAY, -70, GETDATE()), 0, NULL),  /* older than the 60-day window: reads as none */
    (N'model',  'D', DATEADD(DAY, -3, GETDATE()), 0, NULL),
    (N'model',  'L', DATEADD(HOUR, -1, GETDATE()), 1, NULL);   /* a copy-only log backup counts too (#8771) */", connection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        var text = DatabaseProtectionCollector.Instance.BuildQuery(Context()).Text.Replace("msdb.dbo.backupset", "#backupset", StringComparison.Ordinal);
        await using var command = new SqlCommand(text, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await DatabaseProtectionCollector.Instance.ReadAsync(reader, Context(), ct);

        var master = rows.Single(r => r.DatabaseName == "master");
        Assert.NotNull(master.LastFullBackupTime);
        /* yesterday's copy_only full, in UTC, not the older ordinary one */
        Assert.InRange((DateTime.UtcNow - master.LastFullBackupTime!.Value).TotalHours, 23.0, 25.0);
        var model = rows.Single(r => r.DatabaseName == "model");
        Assert.NotNull(model.LastLogBackupTime);
        Assert.InRange((DateTime.UtcNow - model.LastLogBackupTime!.Value).TotalMinutes, 55.0, 65.0);
        Assert.Null(master.LastLogBackupTime);
        /* master is SIMPLE on a stock instance, where no log backup is expected, otherwise the 70-day-old one reads as none in the window */
        Assert.Equal(
            master.RecoveryModelDesc == "SIMPLE" ? DatabaseProtectionCollector.BackupNotExpected : DatabaseProtectionCollector.BackupNoneInWindow,
            master.LogBackupState);
        Assert.DoesNotContain(rows, r => r.DatabaseName == "tempdb");
    }

    /// <summary>
    /// #8775: a database on the server's Excluded Databases list is left out of the rows, with its backup history, and the other databases
    /// stay. Both names are real on every instance, so the batch has rows for them to leave out. The backup of the excluded one proves the
    /// exclusion works on the catalog read the backups join to, not on the backups.
    /// </summary>
    [Fact]
    public async Task ADatabaseOnTheExcludedList_IsLeftOutOfTheRows_AndTheOthersStay()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync(ct);

        await using (var setup = new SqlCommand(@"
CREATE TABLE #backupset (database_name nvarchar(128), type char(1), backup_finish_date datetime, is_copy_only bit, time_zone smallint);
INSERT #backupset VALUES
    (N'master', 'D', DATEADD(DAY, -1, GETDATE()), 0, NULL),
    (N'model',  'D', DATEADD(DAY, -1, GETDATE()), 0, NULL);", connection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        var excluded = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "live-sql", Deltas = new CollectorDeltaCalculator(),
            ExcludedDatabases = new[] { "model" },
        };
        var query = DatabaseProtectionCollector.Instance.BuildQuery(excluded);
        await using var command = new SqlCommand(query.Text.Replace("msdb.dbo.backupset", "#backupset", StringComparison.Ordinal), connection);
        foreach (var parameter in query.Parameters)
        {
            command.Parameters.Add(parameter.Name, System.Data.SqlDbType.NVarChar, 128).Value = parameter.Value;
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await DatabaseProtectionCollector.Instance.ReadAsync(reader, excluded, ct);

        Assert.DoesNotContain(rows, r => r.DatabaseName == "model");
        Assert.Contains(rows, r => r.DatabaseName == "master");
    }

    /// <summary>
    /// A backup converts with the offset recorded on its own row (<c>time_zone</c>, 15-minute steps), not the offset in force now,
    /// and the newest is chosen after the conversion. Two log backups on a database are given zones that put the raw-newest one
    /// EARLIER in UTC than the other, so taking the MAX before converting reads the wrong one. A full backup with time_zone 127
    /// (unknown) falls back to the current offset. Zone -20 is five hours behind UTC and +16 is four hours ahead, and the current
    /// offset of the test server is read from the server so the assertion holds in any zone.
    /// </summary>
    [Fact]
    public async Task AGroupedBackupConvertsWithItsOwnTimeZone_TheMaxIsTakenAfterTheConversion_AnUnknownZoneUsesTheCurrentOffset()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;

        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync(ct);

        DateTime g;
        int currentOffsetMinutes;
        await using (var clock = new SqlCommand("SELECT DATEADD(DAY, -1, GETDATE()), DATEDIFF(MINUTE, GETDATE(), GETUTCDATE());", connection))
        await using (var clockReader = await clock.ExecuteReaderAsync(ct))
        {
            Assert.True(await clockReader.ReadAsync(ct));
            g = clockReader.GetDateTime(0);
            currentOffsetMinutes = clockReader.GetInt32(1);
        }

        /* A literal, not a parameter: a parameterized command runs under sp_executesql, which drops the temp table when it returns. */
        var literal = "'" + g.ToString("yyyy-MM-ddTHH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture) + "'";
        await using (var setup = new SqlCommand($@"
CREATE TABLE #backupset (database_name nvarchar(128), type char(1), backup_finish_date datetime, is_copy_only bit, time_zone smallint);
INSERT #backupset VALUES
    (N'msdb', 'L', {literal}, 0, -20),                         /* five hours behind UTC: UTC is local + 5 h */
    (N'msdb', 'L', DATEADD(HOUR, 1, {literal}), 0, 16),        /* raw-newest, four hours ahead of UTC: UTC is local - 4 h, so older than the one above */
    (N'msdb', 'D', {literal}, 0, 127);                         /* unknown zone: the current offset */", connection))
        {
            await setup.ExecuteNonQueryAsync(ct);
        }

        var text = DatabaseProtectionCollector.Instance.BuildQuery(Context()).Text.Replace("msdb.dbo.backupset", "#backupset", StringComparison.Ordinal);
        await using var command = new SqlCommand(text, connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        var rows = await DatabaseProtectionCollector.Instance.ReadAsync(reader, Context(), ct);

        var msdb = rows.Single(r => r.DatabaseName == "msdb");
        Assert.Equal(g.AddHours(5), msdb.LastLogBackupTime);
        Assert.Equal(g.AddMinutes(currentOffsetMinutes), msdb.LastFullBackupTime);
        Assert.Equal(DatabaseProtectionCollector.BackupRecorded, msdb.LogBackupState);
    }

    /* #8865: one real cycle against the instance, as the host runs it: the primary batch and ReadAsync, then the supplemental and its apply,
       all on one connection. The state a cycle staged is carried into the next, like the store does. */
    private static async Task<(List<DatabaseProtectionCollector.Row> Rows, CollectorContext Context)> RunCycleAsync(
        SqlConnection connection, Dictionary<string, string> state, int sqlMajorVersion, CancellationToken ct)
    {
        var context = new CollectorContext
        {
            CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "live-sql", Deltas = new CollectorDeltaCalculator(),
            Target = new CollectorTargetInfo { SqlMajorVersion = sqlMajorVersion }, State = new Dictionary<string, string>(state, StringComparer.Ordinal),
        };

        List<DatabaseProtectionCollector.Row> rows;
        var primary = DatabaseProtectionCollector.Instance.BuildQuery(context);
        await using (var command = BindCommand(connection, primary))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            rows = await DatabaseProtectionCollector.Instance.ReadAsync(reader, context, ct);
        }

        var supplemental = DatabaseProtectionCollector.Instance.BuildSupplementalQuery(context);
        Assert.NotNull(supplemental);
        await using (var command = BindCommand(connection, supplemental))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            await DatabaseProtectionCollector.Instance.ApplySupplementalAsync(rows, reader, context, ct);
        }

        foreach (var pair in context.PendingState)
        {
            state[pair.Key] = pair.Value;
        }

        return (rows, context);
    }

    private static SqlCommand BindCommand(SqlConnection connection, CollectorQuery query)
    {
        var command = new SqlCommand(query.Text, connection) { CommandTimeout = 60 };
        foreach (var parameter in query.Parameters)
        {
            command.Parameters.Add(parameter.Name, SqlDbType.NVarChar, 128).Value = parameter.Value;
        }

        return command;
    }

    private static async Task ExecuteAsync(string connectionString, string sql, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 120 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<int> MajorVersionAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand("SELECT CONVERT(int, SERVERPROPERTY('ProductMajorVersion'));", connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    /* A login that exists only for one test, created and dropped around it. Its connection string is returned with pooling off, so no
       pooled session keeps the login alive when it is dropped. */
    private static async Task<string> CreateLoginAsync(string adminConnectionString, string login, string grants, CancellationToken ct)
    {
        var password = "Pm8865-" + Guid.NewGuid().ToString("N");
        await ExecuteAsync(adminConnectionString, $"CREATE LOGIN [{login}] WITH PASSWORD = N'{password}', CHECK_POLICY = OFF;{grants}", ct);
        return new SqlConnectionStringBuilder(adminConnectionString) { UserID = login, Password = password, Pooling = false }.ConnectionString;
    }

    private static Task DropLoginAsync(string adminConnectionString, string login, CancellationToken ct) =>
        ExecuteAsync(adminConnectionString, $"IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'{login}') DROP LOGIN [{login}];", ct);

    private static string Bracket(string name) => "[" + name.Replace("]", "]]", StringComparison.Ordinal) + "]";

    private static Task DropDatabaseAsync(string adminConnectionString, string database, CancellationToken ct) =>
        ExecuteAsync(
            adminConnectionString,
            $"IF DB_ID(N'{database}') IS NOT NULL BEGIN ALTER DATABASE {Bracket(database)} SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE {Bracket(database)}; END;",
            ct);

    /* A name with spaces, brackets and parentheses: the match on the log line has to be exact, not a LIKE pattern or a quoted identifier. */
    private const string SpacedName = "pm 8865 [x] (a)";
    private const string LookalikeName = "pm 8865 [x] (a) old";

    [Fact]
    public async Task TheSupplemental_FindsTheLine_ForANameWithSpacesAndBrackets_AndNotForItsLookalike()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);

        try
        {
            await ExecuteAsync(cs!, $"CREATE DATABASE {Bracket(SpacedName)}; CREATE DATABASE {Bracket(LookalikeName)};", ct);
            await ExecuteAsync(cs!, $"DBCC CHECKDB (N'{SpacedName}');", ct);

            await using var connection = new SqlConnection(cs);
            await connection.OpenAsync(ct);
            var (rows, _) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);

            var found = rows.Single(r => r.DatabaseName == SpacedName);
            Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, found.ErrorLogState);
            Assert.NotNull(found.ErrorLogCheckDbTime);
            Assert.InRange((DateTime.UtcNow - found.ErrorLogCheckDbTime!.Value).TotalSeconds, -60, 60);
            Assert.Equal("full", found.ErrorLogCheckDbOptions);
            Assert.Equal(0, found.ErrorLogCheckDbErrors);
            Assert.Null(rows.Single(r => r.DatabaseName == LookalikeName).ErrorLogCheckDbTime);
        }
        finally
        {
            await DropDatabaseAsync(cs!, SpacedName, ct);
            await DropDatabaseAsync(cs!, LookalikeName, ct);
        }
    }

    /// <summary>
    /// #8865 I3: the primary batch stages the Availability Group sources into #temp tables before the join, and a platform-managed target
    /// skips the readability lookup. Both shapes run against the instance (the container has no Availability Group, so every row reads
    /// outside one) and return every database, master included. Each runs twice on one connection: with no excluded databases the batch is
    /// sent unparameterised and runs at session scope, where a #temp table it left behind would fail the next cycle.
    /// </summary>
    [Fact]
    public async Task ThePrimaryBatch_RunsInBothShapes_WithTheAgSourcesStaged_TwiceOnOneConnection()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);
        await using var connection = new SqlConnection(cs);
        await connection.OpenAsync(ct);

        foreach (var managed in new[] { false, false, true, true })
        {
            var context = new CollectorContext
            {
                CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "live-sql", Deltas = new CollectorDeltaCalculator(),
                Target = new CollectorTargetInfo { SqlMajorVersion = major, IsAzureManagedInstance = managed },
            };
            await using var command = BindCommand(connection, DatabaseProtectionCollector.Instance.BuildQuery(context));
            await using var reader = await command.ExecuteReaderAsync(ct);
            var rows = await DatabaseProtectionCollector.Instance.ReadAsync(reader, context, ct);

            var master = rows.Single(r => r.DatabaseName == "master");
            Assert.Null(master.ReplicaRole);
            Assert.Null(master.ReplicaReadable);
            Assert.Equal(managed ? DatabaseProtectionCollector.ErrorLogNotApplicable : DatabaseProtectionCollector.ErrorLogFailed, master.ErrorLogState);
        }
    }

    /// <summary>
    /// #8865 I5 and I6, on a real log. <c>DBCC CHECKDB (db, NOINDEX)</c> logs <c>DBCC CHECKDB (db, noindex) WITH ...</c>, which the old
    /// ")"-after-the-name match never found. A message 5235 line (planted with xp_logevent, the text SQL Server writes when a check ends
    /// abnormally) shares the good line's head, so without dropping it the server's newest-line-per-head reduction would keep it and lose
    /// the good line.
    /// </summary>
    [Fact]
    public async Task ANoIndexCheck_IsFound_AndALaterAbnormalEndDoesNotHideTheGoodLine()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);
        const string NoIndexDatabase = "pm_8865_noindex";
        const string AbnormalDatabase = "pm_8865_abnormal";

        try
        {
            await ExecuteAsync(cs!, $"CREATE DATABASE [{NoIndexDatabase}]; CREATE DATABASE [{AbnormalDatabase}];", ct);
            await ExecuteAsync(cs!, $"DBCC CHECKDB (N'{NoIndexDatabase}', NOINDEX) WITH NO_INFOMSGS;", ct);
            await ExecuteAsync(cs!, $"DBCC CHECKDB (N'{AbnormalDatabase}') WITH NO_INFOMSGS;", ct);

            /* The unparsed count is server-wide and the instance's logs may hold lines other tests planted, so the abnormal end is
               measured against the count of the same read before it is planted. */
            await using var connection = new SqlConnection(cs);
            await connection.OpenAsync(ct);
            var (before, _) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);
            await ExecuteAsync(
                cs!,
                $"EXECUTE master.dbo.xp_logevent 60000, N'DBCC CHECKDB ({AbnormalDatabase}) WITH no_infomsgs executed by sa terminated abnormally due to error state 5. Elapsed time: 0 hours 0 minutes 0 seconds.', informational;",
                ct);

            var (rows, _) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);

            var noIndex = rows.Single(r => r.DatabaseName == NoIndexDatabase);
            Assert.NotNull(noIndex.ErrorLogCheckDbTime);
            Assert.Equal("full,noindex", noIndex.ErrorLogCheckDbOptions);

            var abnormal = rows.Single(r => r.DatabaseName == AbnormalDatabase);
            Assert.NotNull(abnormal.ErrorLogCheckDbTime);
            Assert.Equal(0, abnormal.ErrorLogCheckDbErrors);
            Assert.Equal(before.Single(r => r.DatabaseName == AbnormalDatabase).ErrorLogUnparsedLines, abnormal.ErrorLogUnparsedLines);
        }
        finally
        {
            await DropDatabaseAsync(cs!, NoIndexDatabase, ct);
            await DropDatabaseAsync(cs!, AbnormalDatabase, ct);
        }
    }

    /// <summary>
    /// #8865: on an instance whose error log is not in English the 8957 line has no " executed by ", and a read that also searched for those
    /// words never returned it, so nothing was counted. Japanese puts the login before the command, so the line does not start with it
    /// either. The line (planted with xp_logevent in the Japanese shape) must come back and add one unparsed line to what the same read
    /// counted before it was planted. The database has a name of its own each run and is dropped after it, and a line about a database
    /// the collector does not list is never counted, so the planted line counts for no later read or test.
    /// </summary>
    [Fact]
    public async Task ACheckDbLineInAnotherLanguage_ComesBack_AndIsCountedUnparsed()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);
        var database = "pm_8865_localized_" + Guid.NewGuid().ToString("N")[..8];

        try
        {
            await ExecuteAsync(cs!, $"CREATE DATABASE [{database}];", ct);
            await using var connection = new SqlConnection(cs);
            await connection.OpenAsync(ct);
            var (before, _) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);

            await ExecuteAsync(
                cs!,
                $"EXECUTE master.dbo.xp_logevent 60000, N'sa から実行された DBCC CHECKDB ({database}) では、0 個のエラーが見つかり、0 個のエラーが修復されました。経過時間: 0 時間 0 分 0 秒。', informational;",
                ct);
            var (rows, _) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);

            var row = rows.Single(r => r.DatabaseName == database);
            Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, row.ErrorLogState);
            Assert.Equal(before.Single(r => r.DatabaseName == database).ErrorLogUnparsedLines + 1, row.ErrorLogUnparsedLines);
            Assert.Null(row.ErrorLogCheckDbTime);
        }
        finally
        {
            await DropDatabaseAsync(cs!, database, ct);
        }
    }

    /// <summary>
    /// A login that may read the catalog and the DMVs the primary batch uses but not the error log. SQL Server 2022 splits VIEW SERVER STATE
    /// into VIEW SERVER PERFORMANCE STATE (the DMVs) and VIEW SERVER SECURITY STATE; with only the first the primary facts still come and
    /// sp_readerrorlog is refused, error 27219. Earlier versions need VIEW SERVER STATE for the DMV the primary joins, which also reads
    /// the log, so no login reaches this state there.
    /// </summary>
    [Fact]
    public async Task ALoginWithoutTheGrant_IsRefused_AndThePrimaryFactsStillCome()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);
        Assert.SkipWhen(major < 16, "VIEW SERVER PERFORMANCE STATE exists from SQL Server 2022.");
        const string Login = "pm_8865_nogrant";

        try
        {
            var loginConnection = await CreateLoginAsync(cs!, Login, $" GRANT VIEW SERVER PERFORMANCE STATE TO [{Login}];", ct);
            await using var connection = new SqlConnection(loginConnection);
            await connection.OpenAsync(ct);
            var (rows, _) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);

            Assert.Contains(rows, r => r.DatabaseName == "master");
            Assert.All(rows, r => Assert.Equal(DatabaseProtectionCollector.ErrorLogRefused, r.ErrorLogState));
            Assert.All(rows, r => Assert.Equal("VIEW SERVER STATE", r.ErrorLogGrant));
            Assert.All(rows, r => Assert.Equal(DatabaseProtectionCollector.PlatformNative, r.Platform));
        }
        finally
        {
            await DropLoginAsync(cs!, Login, ct);
        }
    }

    /* Waits for the next minute when the server's clock is late in the current one, so a few quick log cycles land in one minute. */
    private static async Task WaitForEarlyInAMinuteAsync(string connectionString, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = new SqlCommand("SELECT DATEPART(SECOND, SYSDATETIME());", connection);
        var second = Convert.ToInt32(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
        if (second > 40)
        {
            await Task.Delay(TimeSpan.FromSeconds(61 - second), ct);
        }
    }

    /// <summary>
    /// #8865 I1: the list dates an archive to the minute, so logs cycled within one minute share one date. The CHECKDB line is written and
    /// then the log is cycled five times at once, which leaves the line in archive 5 only, behind four newer archives of the same minute.
    /// The old plan read three archives of that date and marked the date (so all five) read, and archive 5 was never read.
    /// </summary>
    [Fact]
    public async Task CycledLogs_InOneMinute_AreReadWhole_AndTheLineInTheOldestOfThemIsFound()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);
        const string Database = "pm_8865_cycled";
        const int Rolls = 5;

        try
        {
            await ExecuteAsync(cs!, $"CREATE DATABASE [{Database}];", ct);
            await WaitForEarlyInAMinuteAsync(cs!, ct);
            await ExecuteAsync(cs!, $"DBCC CHECKDB (N'{Database}');", ct);
            for (var i = 0; i < Rolls; i++)
            {
                await ExecuteAsync(cs!, "EXECUTE sys.sp_cycle_errorlog;", ct);
            }

            await using var connection = new SqlConnection(cs);
            await connection.OpenAsync(ct);
            var state = new Dictionary<string, string>(StringComparer.Ordinal);
            var plans = new List<DatabaseProtectionErrorLogState.ErrorLogPlan>();
            var maxCycles = int.MaxValue;
            DatabaseProtectionCollector.Row row;
            do
            {
                var (rows, context) = await RunCycleAsync(connection, state, major, ct);
                var plan = context.DatabaseProtectionErrorLogPlan!;
                plans.Add(plan);
                row = rows.Single(r => r.DatabaseName == Database);
                if (plans.Count == 1)
                {
                    /* Every read here succeeds, so each cycle finishes at least one listed date: the backfill ends within that many
                       cycles, and one more is spare. */
                    maxCycles = plan.Enumerated!.Where(a => a.Archive > 0).Select(a => a.LogDate).Distinct().Count() + 1;
                }
            }
            while (row.ErrorLogBackfillComplete != true && plans.Count < maxCycles);

            var listed = plans[0].Enumerated!.Where(a => a.Archive is >= 1 and <= Rolls).ToList();
            Assert.True(
                listed.Count == Rolls && listed.Select(a => a.LogDate).Distinct().Count() == 1,
                "the five rolls must share one minute for this test to say anything; rerun it");
            Assert.True(row.ErrorLogBackfillComplete);
            Assert.NotNull(row.ErrorLogCheckDbTime);
            Assert.InRange((DateTime.UtcNow - row.ErrorLogCheckDbTime!.Value).TotalMinutes, -1, 30);

            /* A cycle reads at most ArchivesPerCycle archives, unless it reads one date alone that holds more. */
            Assert.All(plans, p =>
            {
                var archives = p.Read.Where(a => a.Archive > 0).ToList();
                Assert.True(archives.Count <= DatabaseProtectionErrorLogState.ArchivesPerCycle || archives.Select(a => a.LogDate).Distinct().Count() == 1);
            });
        }
        finally
        {
            await DropDatabaseAsync(cs!, Database, ct);
        }
    }

    /* The archives one cycle lists on a connection whose session language is set first; archive 0 is left out, because its date is the
       current log's last write and moves while the test runs. */
    private static async Task<(bool Ok, List<DatabaseProtectionErrorLogState.ArchiveInfo> Archives)> ListUnderAsync(
        string connectionString, string language, int major, CancellationToken ct)
    {
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using (var set = new SqlCommand($"SET LANGUAGE {language};", connection))
        {
            await set.ExecuteNonQueryAsync(ct);
        }

        var (_, context) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);
        var plan = context.DatabaseProtectionErrorLogPlan!;
        return (plan.EnumerateOk, (plan.Enumerated ?? Array.Empty<DatabaseProtectionErrorLogState.ArchiveInfo>()).Where(a => a.Archive > 0).ToList());
    }

    /// <summary>
    /// #8865 C1: sp_enumerrorlogs returns its dates as month/day/year text. Read into a datetime under a day/month/year language, the insert
    /// failed on a day after the 12th (8114, and the list read as refused) and swapped day and month below it. The list must come back
    /// whole, with the dates a us_english session reads.
    /// </summary>
    [Fact]
    public async Task TheLogList_HasTheSameDates_UnderADayMonthYearLanguage()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);
        await ExecuteAsync(cs!, "EXECUTE sys.sp_cycle_errorlog;", ct);

        var english = await ListUnderAsync(cs!, "us_english", major, ct);
        var british = await ListUnderAsync(cs!, "British", major, ct);

        Assert.True(english.Ok);
        Assert.NotEmpty(english.Archives);
        Assert.True(british.Ok, "the list must not read as refused under a dmy language");
        Assert.Equal(english.Archives.OrderBy(a => a.Archive), british.Archives.OrderBy(a => a.Archive));
        Assert.All(british.Archives, a => Assert.InRange(a.LogDate, DateTime.UtcNow.AddYears(-5), DateTime.UtcNow.AddMinutes(2)));
    }

    /// <summary>
    /// Task 10's grant depends on this: a login with only CONNECT SQL and VIEW SERVER STATE. The outcome is written to the test output.
    /// </summary>
    [Fact]
    public async Task ViewServerStateAlone_ReadsTheLog()
    {
        var cs = ConnectionString();
        Assert.SkipWhen(cs is null, "Set DARLING_TEST_SQL to a SQL Server host to run the database protection live SQL test.");
        var ct = TestContext.Current.CancellationToken;
        var major = await MajorVersionAsync(cs!, ct);
        const string Login = "pm_8865_vss";
        const string Database = "pm_8865_vss_db";

        try
        {
            await ExecuteAsync(cs!, $"CREATE DATABASE [{Database}];", ct);
            await ExecuteAsync(cs!, $"DBCC CHECKDB (N'{Database}');", ct);
            var loginConnection = await CreateLoginAsync(cs!, Login, $" GRANT VIEW SERVER STATE TO [{Login}];", ct);

            await using var connection = new SqlConnection(loginConnection);
            await connection.OpenAsync(ct);
            var (rows, _) = await RunCycleAsync(connection, new Dictionary<string, string>(StringComparer.Ordinal), major, ct);

            var row = rows.Single(r => r.DatabaseName == Database);
            TestContext.Current.TestOutputHelper?.WriteLine($"SQL Server major version {major}: a login with only VIEW SERVER STATE reads the error log -> errorlog_state = {row.ErrorLogState}");
            Assert.Equal(DatabaseProtectionCollector.ErrorLogRead, row.ErrorLogState);
            Assert.NotNull(row.ErrorLogCheckDbTime);
        }
        finally
        {
            await DropLoginAsync(cs!, Login, ct);
            await DropDatabaseAsync(cs!, Database, ct);
        }
    }

    private static CollectorContext Context() => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "live-sql", Deltas = new CollectorDeltaCalculator(),
    };
}
