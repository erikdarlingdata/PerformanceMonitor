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

    /* #8865: the second result set, the error logs the instance lists. A single (0, NULL, NULL) row means the listing was refused. */
    private static DataTable LogShape()
    {
        var table = new DataTable();
        table.Columns.Add("enumerate_ok", typeof(bool));
        table.Columns.Add("archive", typeof(int));
        table.Columns.Add("log_date", typeof(DateTime));
        return table;
    }

    /* Archive n is dated n days before Checked, so a higher archive number is an older log, as on a real instance. */
    private static DataTable Logs(int count)
    {
        var table = LogShape();
        for (var archive = 0; archive < count; archive++)
        {
            table.Rows.Add(true, archive, Checked.AddDays(-archive));
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
        var refused = LogShape();
        refused.Rows.Add(false, DBNull.Value, DBNull.Value);
        var context = ContextWith(new Dictionary<string, string>());

        var row = Assert.Single(await ReadWithAsync(context, table, refused));

        var plan = context.DatabaseProtectionErrorLogPlan!;
        Assert.False(plan.EnumerateOk);
        Assert.Equal(new[] { 0 }, plan.Read.Select(a => a.Archive).ToArray());
        Assert.True(row.ErrorLogBackfillComplete);
    }

    [Fact]
    public async Task AReplicaThatIsNotReadable_ReadsReplicaReadableFalse()
    {
        var table = Shape();
        Add(table, "NoConn", "FULL", Checked, Full, Log, role: "SECONDARY", allowConnections: "NO");
        Add(table, "ReadOnly", "FULL", Checked, Full, Log, role: "SECONDARY", allowConnections: "READ_ONLY");
        Add(table, "AllConn", "FULL", Checked, Full, Log, role: "SECONDARY", allowConnections: "ALL");
        Add(table, "Standalone", "FULL", Checked, Full, Log);

        var rows = await ReadWithAsync(ContextWith(new Dictionary<string, string>()), table, Logs(1));

        Assert.False(rows[0].ReplicaReadable);
        Assert.True(rows[1].ReplicaReadable);
        Assert.True(rows[2].ReplicaReadable);
        Assert.Null(rows[3].ReplicaReadable);
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
        var rung = PgMigrations.Scripts.Single(m => m.Version == 269);
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

    private static CollectorContext Context() => new()
    {
        CollectionTime = DateTime.UtcNow, ServerId = 1, ServerName = "live-sql", Deltas = new CollectorDeltaCalculator(),
    };
}
