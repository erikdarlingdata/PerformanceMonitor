/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the pg_stat_database collector (#2539): the table name that must NOT shadow the catalog view, the
/// gate that deliberately gates on nothing, the UTC conversion, and the two shapes of NULL this view really
/// produces — the shared-relations row with no database name, and a <c>stats_reset</c> that is NULL until
/// the first reset ever.
/// </summary>
public class PgDatabaseStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(int major = 17, ICollectorDeltaCalculator? deltas = null)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 20, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas ?? s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    /// <summary>
    /// The table name is the one thing here that cannot be changed later without a migration, and it must
    /// not be <c>pg_stat_database</c>: pg_catalog is searched before search_path, so a store table by that
    /// name makes every unqualified read resolve to the MONITORING store's own view.
    /// </summary>
    [Fact]
    public void Identity_Pinned_AndTheTableDoesNotShadowTheCatalogView()
    {
        Assert.Equal("pg_database_stats", PgDatabaseStatsCollector.Instance.Name);
        Assert.Equal("pg_database_stats", PgDatabaseStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgDatabaseStatsCollector.Instance.TargetEngine);

        Assert.NotEqual("pg_stat_database", PgDatabaseStatsCollector.Instance.TargetTable);
    }

    /// <summary>
    /// Every PostgreSQL major, Aurora or not, writer or replica. Each of those is a decision:
    /// <list type="bullet">
    /// <item>No version floor — the newest column selected (temp_files / temp_bytes / deadlocks) arrived in
    /// PostgreSQL 9.2, so copying pg_stat_io's PG16 gate would have gated off a working view.</item>
    /// <item>No Aurora gate — this is core PostgreSQL, and it is the ONLY temp-file evidence a stock
    /// PostgreSQL target has, because pg_statement_stats reads aurora_stat_statements().</item>
    /// <item>No recovery gate — a sort on a read replica spills exactly the way it does on a writer, and a
    /// replica is where the heavy reporting queries usually are.</item>
    /// </list>
    /// </summary>
    [Theory]
    [InlineData(13, false, false)]
    [InlineData(15, false, true)]
    [InlineData(16, true, false)]
    [InlineData(17, true, true)]
    [InlineData(0, false, false)]
    public void AppliesToEveryPostgresTarget_IncludingStockAndReplicas(int major, bool isAurora, bool inRecovery)
    {
        Assert.True(PgDatabaseStatsCollector.Instance.AppliesTo(new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = major,
            IsAurora = isAurora,
            IsInRecovery = inRecovery,
        }));
    }

    /// <summary>
    /// The composed gate still refuses a SQL Server target: the collector's own AppliesTo says yes to
    /// everything, so the ENGINE half is the only thing standing between this PostgreSQL query text and a
    /// SQL Server connection. Asserting the collector's gate alone would not see that.
    /// </summary>
    [Fact]
    public void TheEngineHalfOfTheDispatchGateStillRefusesASqlServerTarget()
    {
        Assert.False(CollectorCatalog.AppliesTo(PgDatabaseStatsCollector.Instance, new CollectorTargetInfo()));
        Assert.True(CollectorCatalog.AppliesTo(
            PgDatabaseStatsCollector.Instance,
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));
    }

    /// <summary>
    /// Cluster-wide, so no per-database fan-out — which is what makes a per-minute cadence affordable and
    /// is the third leg of the per-database STORAGE argument (the grain is free, not paid for).
    /// </summary>
    [Fact]
    public void DoesNotRunPerDatabase()
    {
        Assert.False(PgDatabaseStatsCollector.Instance.RunsPerDatabase(MakeContext().Target));
    }

    /// <summary>
    /// The four questions, each present by name, plus the reset timestamp. A column dropped from the query
    /// is a silent capability loss — the payload column would still exist and would simply always be NULL.
    /// </summary>
    [Theory]
    [InlineData("d.temp_files")]
    [InlineData("d.temp_bytes")]
    [InlineData("d.blks_read")]
    [InlineData("d.blks_hit")]
    [InlineData("d.deadlocks")]
    [InlineData("d.xact_commit")]
    [InlineData("d.xact_rollback")]
    public void SelectsEveryCounterTheFourQuestionsNeed(string column)
    {
        Assert.Contains(column, PgDatabaseStatsCollector.Instance.BuildQuery(MakeContext()).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>timestamptz::text</c> renders in the SESSION's TimeZone and the store contract is naive UTC, so
    /// the conversion has to be explicit. Byte-identical to UTC on every instance in the fleet, which is
    /// exactly why it survives every probe and has to be pinned instead.
    /// </summary>
    [Fact]
    public void ConvertsStatsResetToUtcExplicitly()
    {
        var sql = PgDatabaseStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("(d.stats_reset AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("stats_reset::timestamp", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The query text is version-invariant, deliberately: every column it names has existed since 9.2, so
    /// there is nothing to branch on and a branch would be a source of drift rather than of correctness.
    /// </summary>
    [Fact]
    public void TheQueryIsIdenticalOnEveryMajor()
    {
        var majors = new[] { 13, 14, 15, 16, 17, 18 }
            .Select(m => PgDatabaseStatsCollector.Instance.BuildQuery(MakeContext(m)).Text)
            .Distinct(StringComparer.Ordinal)
            .ToArray();

        Assert.Single(majors);
    }

    /// <summary>
    /// The columns PostgreSQL added AFTER 9.2 are absent, and that is what makes the gate-free AppliesTo
    /// honest: selecting any of them would need a version floor, and a floor written without one would fail
    /// the whole collection every cycle on an older major with "column does not exist".
    /// </summary>
    [Theory]
    [InlineData("checksum_failures")]   // PostgreSQL 12
    [InlineData("session_time")]        // PostgreSQL 14
    [InlineData("sessions_killed")]     // PostgreSQL 14
    [InlineData("parallel_workers_launched")]  // PostgreSQL 18
    public void SelectsNoColumnThatWouldNeedAVersionFloor(string column)
    {
        Assert.DoesNotContain(column, PgDatabaseStatsCollector.Instance.BuildQuery(MakeContext()).Text, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_OrderAndKeyTypes_Pinned()
    {
        var columns = PgDatabaseStatsCollector.Instance.PayloadColumns;

        Assert.Equal(9, columns.Count);
        Assert.Equal(
            new[]
            {
                "database_name", "xact_commit", "xact_rollback", "blks_read", "blks_hit",
                "temp_files", "temp_bytes", "deadlocks", "stats_reset",
            },
            columns.Select(c => c.Name).ToArray());

        Assert.Equal(CollectorColumnType.Varchar, columns[0].Type);
        Assert.Equal(CollectorColumnType.BigInt, columns[5].Type);      // temp_files
        Assert.Equal(CollectorColumnType.Timestamp, columns[8].Type);   // stats_reset
    }

    /// <summary>
    /// A normal row: every counter populated, and a reset timestamp that has been set at some point.
    /// </summary>
    [Fact]
    public async Task ReadsAFullyPopulatedRow()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "appdb",
                18_442_113L, 90_115L,                       // xact_commit, xact_rollback
                4_112_889L, 10_150_937_570L,                // blks_read, blks_hit
                214L, 9_663_676_416L,                       // temp_files, temp_bytes
                3L,                                         // deadlocks
                new DateTime(2026, 5, 18, 7, 4, 22, DateTimeKind.Unspecified),
            });

        var rows = await PgDatabaseStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("appdb", row.DatabaseName);
        Assert.Equal(214L, row.TempFiles);
        Assert.Equal(9_663_676_416L, row.TempBytes);
        Assert.Equal(3L, row.Deadlocks);
        Assert.Equal(90_115L, row.XactRollback);
        Assert.Equal(new DateTime(2026, 5, 18, 7, 4, 22), row.StatsReset);
    }

    /// <summary>
    /// PostgreSQL's own shared-relations row: <c>datname</c> is NULL, and it must arrive as null rather
    /// than being dropped or turned into a sentinel string. Its block counters are real, and the read keys
    /// its series on that NULL.
    /// </summary>
    [Fact]
    public async Task PreservesTheNullDatabaseNameOfTheSharedRelationsRow()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                DBNull.Value,
                0L, 0L,
                8_412L, 1_004_991L,
                0L, 0L,
                0L,
                DBNull.Value,
            });

        var rows = await PgDatabaseStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.DatabaseName);
        Assert.Equal(1_004_991L, row.BlksHit);

        /* NULL stats_reset is the ordinary state of a server nobody has ever reset, and it means exactly
           "never reset" — not "unknown", and certainly not zero. */
        Assert.Null(row.StatsReset);

        /* A real zero stays a zero, so "no temp files" is distinguishable from "not reported". */
        Assert.Equal(0L, row.TempFiles);
    }

    [Fact]
    public async Task ReturnsNoRowsWhenTheViewReturnsNone()
    {
        var rows = await PgDatabaseStatsCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>
    /// No stored deltas: the raw cumulative counter is stored and the window is differenced at read time,
    /// so a reset stays visible in the data rather than being smoothed away at write time by a delta the
    /// reader cannot re-derive.
    /// </summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();

        PgDatabaseStatsCollector.Instance.WritePayload(
            new PgDatabaseStatsCollector.Row("appdb", 1, 0, 2, 3, 0, 0, 0, null),
            new RecordingCollectorRowWriter(),
            MakeContext(deltas: deltas));

        Assert.Empty(deltas.Calls);
    }

    /// <summary>
    /// Every payload column is written, in order. WritePayload is positional, so a column added without a
    /// matching Value() shifts everything after it and stores data that is silently wrong.
    /// </summary>
    [Fact]
    public void WritesEveryPayloadColumnInOrder()
    {
        var writer = new RecordingCollectorRowWriter();
        var reset = new DateTime(2026, 5, 18, 7, 4, 22);

        PgDatabaseStatsCollector.Instance.WritePayload(
            new PgDatabaseStatsCollector.Row(null, 10, 1, 20, 30, 4, 5_000, 2, reset),
            writer,
            MakeContext());

        Assert.Equal(PgDatabaseStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Null(writer.Values[0]);          // database_name — the shared-relations row
        Assert.Equal(10L, writer.Values[1]);    // xact_commit
        Assert.Equal(4L, writer.Values[5]);     // temp_files
        Assert.Equal(5_000L, writer.Values[6]); // temp_bytes
        Assert.Equal(2L, writer.Values[7]);     // deadlocks
        Assert.Equal(reset, writer.Values[8]);
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_database_stats");

        var schedule = CollectorScheduleDefaults.All["pg_database_stats"];

        /* Per-minute is affordable because pg_stat_database is cluster-wide — one query on the connection
           the collector already holds, returning one row per database. The cadence is also the value: a
           spill correlated to the minute names the deploy or the job that caused it. */
        Assert.Equal(1, schedule.FrequencyMinutes);
        Assert.Equal(30, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }

    /// <summary>
    /// The capability vocabulary has a noun phrase for this collector, so a SQL Server target asked
    /// get_pg_database_stats is told what it does not collect rather than "the data this read is served
    /// from", which explains nothing.
    /// </summary>
    [Fact]
    public void TheCapabilityMessageNamesWhatIsNotCollected()
    {
        var message = CollectorEngineCapability.NotCollectedMessage(
            "sql-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.SqlServer, "pg_database_stats");

        Assert.NotNull(message);
        Assert.Contains("pg_stat_database", message, StringComparison.Ordinal);
        Assert.Contains("temp-file spills", message, StringComparison.Ordinal);
        Assert.DoesNotContain("the data this read is served from", message, StringComparison.Ordinal);
    }
}
