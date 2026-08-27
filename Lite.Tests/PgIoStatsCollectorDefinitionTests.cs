/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the pg_stat_io collector: the version floor, the PG18 column removal, the UTC conversion, and the
/// one that matters most — that NULL is preserved rather than coalesced, because on Aurora the entire write
/// side is NULL and a zero there would be a measurement nobody took.
/// </summary>
public class PgIoStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(int major = 17, ICollectorDeltaCalculator? deltas = null)
        => new()
        {
            ServerId = 42,
            ServerName = "pg-target",
            CollectionTime = new DateTime(2026, 8, 11, 12, 0, 0, DateTimeKind.Utc),
            Deltas = deltas ?? s_deltas,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = major,
            },
            ExcludedDatabases = Array.Empty<string>(),
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pg_io_stats", PgIoStatsCollector.Instance.Name);
        Assert.Equal("pg_io_stats", PgIoStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgIoStatsCollector.Instance.TargetEngine);
    }

    /// <summary>pg_stat_io arrived in PostgreSQL 16; on 15 the view does not exist and the query would fail.</summary>
    [Theory]
    [InlineData(15, false)]
    [InlineData(16, true)]
    [InlineData(17, true)]
    [InlineData(18, true)]
    public void GatesOnThePostgres16VersionFloor(int major, bool applies)
    {
        Assert.Equal(applies, PgIoStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = major }));
    }

    /// <summary>
    /// Unlike the autovacuum collector, this one RUNS on a standby. A replica's own read traffic and its
    /// walreplay context are exactly what is wanted when a reader is slow, and unlike
    /// pg_stat_user_tables these counters are the instance's own rather than the writer's.
    /// </summary>
    [Fact]
    public void RunsOnAStandbyUnlikeTheAutovacuumCollector()
    {
        var reader = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = 17,
            IsInRecovery = true,
        };

        Assert.True(PgIoStatsCollector.Instance.AppliesTo(reader));
        Assert.False(PgAutovacuumStatsCollector.Instance.AppliesTo(reader));
    }

    /// <summary>Core view, so it must not be Aurora-gated the way the wait and statement collectors are.</summary>
    [Fact]
    public void IsNotAuroraGated()
    {
        Assert.True(PgIoStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 16, IsAurora = false,
            }));
        Assert.False(CollectorCatalog.AppliesTo(PgIoStatsCollector.Instance, new CollectorTargetInfo()));
    }

    /// <summary>Cluster-wide — a per-database fan-out here would multiply connections for identical data.</summary>
    [Fact]
    public void NeverRunsPerDatabase()
    {
        Assert.False(PgIoStatsCollector.Instance.RunsPerDatabase(MakeContext().Target));
    }

    /// <summary>
    /// PG18 REMOVED op_bytes. Selecting it there would fail with "column does not exist" and take the
    /// whole collection down, so it is substituted — and the substitution must keep the column count
    /// identical so the stored shape cannot drift.
    ///
    /// <para>Since V101 (#2655) the exchange runs BOTH ways, which is the point of pinning both arms: 18
    /// loses op_bytes and gains the three measured byte columns, while 17 and below select op_bytes and
    /// NULL for the three. Same shape either way, and each server generation carries whichever quantity it
    /// actually has.</para>
    /// </summary>
    [Fact]
    public void SubstitutesOpBytesOnPg18WithoutChangingShape()
    {
        var pg17 = PgIoStatsCollector.Instance.BuildQuery(MakeContext(17)).Text;
        var pg18 = PgIoStatsCollector.Instance.BuildQuery(MakeContext(18)).Text;

        Assert.Contains("    op_bytes  ", pg17, StringComparison.Ordinal);
        Assert.Contains("NULL::bigint", pg18, StringComparison.Ordinal);
        Assert.Equal(pg17.Split(" AS ").Length, pg18.Split(" AS ").Length);

        /* The measured columns are real on 18 and NULL below it — never the other way round, which is the
           failure that would make a pre-18 store claim byte totals it never had. */
        Assert.Contains("    read_bytes  ", pg18, StringComparison.Ordinal);
        Assert.Contains("NULL::numeric", pg17, StringComparison.Ordinal);
        Assert.DoesNotContain("NULL::numeric", pg18, StringComparison.Ordinal);
    }

    /// <summary>
    /// stats_reset is `timestamp with time zone`. AT TIME ZONE 'UTC', never ::timestamp (renders in the
    /// session's TimeZone) and never bare (Npgsql refuses to write a Kind=Utc DateTime to the store's
    /// `timestamp without time zone` column).
    /// </summary>
    [Fact]
    public void ConvertsStatsResetWithAnExplicitUtcZone()
    {
        var sql = PgIoStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("(stats_reset AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("stats_reset::timestamp", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Nothing may filter on backend_type / object / context. Aurora ADDS enum values — 17.7 showed a
    /// `walreplay` context plus 'aurora cache receiver process', 'aurora wal replay process' and
    /// 'slotsync worker' backend types that 16.11 did not — so any whitelist silently drops rows.
    /// </summary>
    [Fact]
    public void FiltersNothingBecauseAuroraAddsEnumValues()
    {
        var sql = PgIoStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.DoesNotContain("WHERE", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("IN (", sql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_stat_io", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// No coalesce anywhere in the projection. PostgreSQL uses NULL for "this counter does not apply here",
    /// and on Aurora the write side is NULL throughout because backends do not write data files. A
    /// coalesce to 0 would turn "never measured" into "measured zero" — and a consumer averaging write
    /// latency would then divide by it.
    /// </summary>
    [Fact]
    public void NeverCoalescesACounterToZero()
    {
        var sql = PgIoStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.DoesNotContain("coalesce", sql, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void PayloadColumns_CountAndKeyTypes_Pinned()
    {
        var columns = PgIoStatsCollector.Instance.PayloadColumns;

        Assert.Equal(21, columns.Count);
        Assert.Equal("backend_type", columns[0].Name);
        Assert.Equal("context", columns[2].Name);
        Assert.Equal("reads", columns[3].Name);
        Assert.Equal(CollectorColumnType.Double, columns[4].Type);      // read_time_ms
        Assert.Equal(CollectorColumnType.Timestamp, columns[17].Type);  // stats_reset

        /* V101 (#2655): PostgreSQL 18's measured byte totals, appended so no stored ordinal moved.
           Decimal, not BigInt — PostgreSQL declares them `numeric` while the counts beside them are
           `bigint`, and a byte total is exactly the quantity that outgrows a narrowing nobody promised. */
        Assert.Equal("read_bytes", columns[18].Name);
        Assert.Equal("write_bytes", columns[19].Name);
        Assert.Equal("extend_bytes", columns[20].Name);
        Assert.Equal(CollectorColumnType.Decimal, columns[18].Type);
        Assert.Equal(CollectorColumnType.Decimal, columns[19].Type);
        Assert.Equal(CollectorColumnType.Decimal, columns[20].Type);
    }

    /// <summary>
    /// The Aurora row shape, end to end: reads and hits populated, and every write-side counter NULL —
    /// which must survive into the row as null rather than becoming 0.
    /// </summary>
    [Fact]
    public async Task PreservesNullWriteCountersFromAurora()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "client backend", "relation", "normal",
                87L, 342.46,                                   // reads, read_time
                DBNull.Value, DBNull.Value,                     // writes, write_time — NULL on Aurora
                DBNull.Value, DBNull.Value,                     // writebacks, writeback_time
                0L, 0.0,                                        // extends, extend_time
                8192L,                                          // op_bytes
                10_150_937_570L, 0L, 0L,                        // hits, evictions, reuses
                DBNull.Value, DBNull.Value,                     // fsyncs, fsync_time — NULL on Aurora
                new DateTime(2026, 5, 18, 7, 4, 22, DateTimeKind.Unspecified),
                /* A pre-18 server, so the measured byte columns do not exist and the collector selects
                   NULL for them (#2655). op_bytes above is the byte answer here. */
                DBNull.Value, DBNull.Value, DBNull.Value,       // read_bytes, write_bytes, extend_bytes
            });

        var rows = await PgIoStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(87L, row.Reads);
        Assert.Equal(10_150_937_570L, row.Hits);

        /* The whole point: null, not 0. */
        Assert.Null(row.Writes);
        Assert.Null(row.WriteTimeMs);
        Assert.Null(row.Writebacks);
        Assert.Null(row.Fsyncs);
        Assert.Null(row.FsyncTimeMs);

        /* And a real zero stays a zero — the two are distinguishable, which is the requirement. */
        Assert.Equal(0L, row.Extends);
        Assert.NotNull(row.StatsReset);
    }

    /// <summary>
    /// The not-applicable-per-combination case, which is core PostgreSQL rather than Aurora: the
    /// checkpointer performs no reads and has no hit counter at all.
    /// </summary>
    [Fact]
    public async Task PreservesNullForCombinationsWhereACounterDoesNotApply()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "checkpointer", "relation", "normal",
                DBNull.Value, DBNull.Value,                     // reads / read_time do not apply
                0L, 0.0, DBNull.Value, DBNull.Value,
                0L, 0.0, 8192L,
                DBNull.Value,                                   // hits does not apply either
                DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value,       // read_bytes, write_bytes, extend_bytes
            });

        var rows = await PgIoStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.Reads);
        Assert.Null(row.Hits);
        Assert.Null(row.Evictions);
        Assert.Null(row.StatsReset);
    }

    /// <summary>
    /// No stored deltas. The windowed change is computed at read time, so a "not applicable" NULL never has
    /// to be given a value it does not have — which is exactly what a stored delta would force.
    /// </summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();

        PgIoStatsCollector.Instance.WritePayload(
            new PgIoStatsCollector.Row(
                "client backend", "relation", "normal", 1, 1.0, null, null, null, null,
                0, 0.0, 8192, 2, 0, 0, null, null, null, null, null, null),
            new RecordingCollectorRowWriter(),
            MakeContext(deltas: deltas));

        Assert.Empty(deltas.Calls);
    }

    /// <summary>Every payload column is written, in order, including the nulls.</summary>
    [Fact]
    public void WritesEveryPayloadColumn()
    {
        var writer = new RecordingCollectorRowWriter();

        PgIoStatsCollector.Instance.WritePayload(
            new PgIoStatsCollector.Row(
                "client backend", "relation", "bulkread", 5, 2.5, null, null, null, null,
                null, null, 8192, 7, null, 3, null, null, null, null, null, null),
            writer,
            MakeContext());

        Assert.Equal(PgIoStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);
        Assert.Equal("client backend", writer.Values[0]);
        Assert.Equal("bulkread", writer.Values[2]);
        Assert.Null(writer.Values[5]);   // writes
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_io_stats");

        var schedule = CollectorScheduleDefaults.All["pg_io_stats"];

        /* Per-minute is affordable because this is cluster-wide (one connection) and returned 25-37 rows
           per snapshot on the fleet — the same order as pg_wait_stats. */
        Assert.Equal(1, schedule.FrequencyMinutes);
        Assert.Equal(30, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }

}
