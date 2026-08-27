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
/// Pins the per-index usage collector (#2541): the ONE column that carries a version floor, the size floor
/// and the escape hatch that lets an invalid index through it, the writers-only gate, and — the substance of
/// the collector — the catalog facts that stop "unused" being read as "droppable".
/// </summary>
public class PgIndexUsageStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        int major = 17, ICollectorDeltaCalculator? deltas = null, string? currentDatabase = "appdb")
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
            CurrentDatabaseName = currentDatabase,
        };

    /// <summary>
    /// The table name cannot be changed later without a migration, and it must not be
    /// <c>pg_stat_user_indexes</c>: pg_catalog is searched before search_path, so a store table by a catalog
    /// name makes every unqualified read resolve to the MONITORING store's own view.
    /// </summary>
    [Fact]
    public void Identity_Pinned_AndTheTableDoesNotShadowACatalogView()
    {
        Assert.Equal("pg_index_usage_stats", PgIndexUsageStatsCollector.Instance.Name);
        Assert.Equal("pg_index_usage_stats", PgIndexUsageStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgIndexUsageStatsCollector.Instance.TargetEngine);

        Assert.NotEqual("pg_stat_user_indexes", PgIndexUsageStatsCollector.Instance.TargetTable);
        Assert.NotEqual("pg_stat_all_indexes", PgIndexUsageStatsCollector.Instance.TargetTable);
        Assert.NotEqual("pg_statio_user_indexes", PgIndexUsageStatsCollector.Instance.TargetTable);
    }

    /// <summary>
    /// WRITERS ONLY, and the gate reads <c>IsInRecovery</c> and nothing else — no version floor and no
    /// Aurora gate, so a stock PostgreSQL 13 writer is collected exactly like an Aurora 17 one.
    ///
    /// <para>The recovery half is the decision worth pinning. On a standby <c>idx_scan</c> counts the
    /// STANDBY's own scans, so an index the primary's workload uses a million times an hour reads as zero
    /// there — and this collector's whole output is a judgement about whether a zero means "drop it". A
    /// confidently wrong "unused" sends someone to DROP INDEX; a missing answer only sends them looking.</para>
    /// </summary>
    [Theory]
    [InlineData(13, false, false, true)]
    [InlineData(15, false, false, true)]
    [InlineData(16, true, false, true)]
    [InlineData(17, true, false, true)]
    [InlineData(18, false, false, true)]
    [InlineData(13, false, true, false)]
    [InlineData(16, true, true, false)]
    [InlineData(17, true, true, false)]
    [InlineData(18, false, true, false)]
    public void AppliesToWritersOnly_OnEveryMajorAndBothAuroraAndStock(
        int major, bool isAurora, bool inRecovery, bool expected)
    {
        Assert.Equal(expected, PgIndexUsageStatsCollector.Instance.AppliesTo(new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            PostgresMajorVersion = major,
            IsAurora = isAurora,
            IsInRecovery = inRecovery,
        }));
    }

    /// <summary>
    /// The composed gate still refuses a SQL Server target. The collector's own AppliesTo only asks about
    /// recovery, so the ENGINE half is the only thing standing between this PostgreSQL query text and a SQL
    /// Server connection — the #2213 class of defect, which asserting the collector's gate alone cannot see.
    /// </summary>
    [Fact]
    public void TheEngineHalfOfTheDispatchGateStillRefusesASqlServerTarget()
    {
        Assert.False(CollectorCatalog.AppliesTo(PgIndexUsageStatsCollector.Instance, new CollectorTargetInfo()));
        Assert.True(CollectorCatalog.AppliesTo(
            PgIndexUsageStatsCollector.Instance,
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));
    }

    /// <summary>
    /// <c>pg_stat_user_indexes</c> is scoped to the connected database and PostgreSQL has no cross-database
    /// read, so this is necessarily a fan-out — and on PostgreSQL a fan-out is one CONNECTION per database
    /// per cycle, which is what the daily cadence pays for.
    /// </summary>
    [Fact]
    public void RunsPerDatabase()
    {
        Assert.True(PgIndexUsageStatsCollector.Instance.RunsPerDatabase(MakeContext().Target));
    }

    /// <summary>
    /// MEASURED, not read from the documentation: <c>last_idx_scan</c> arrived in PostgreSQL 16. The gated-ON
    /// form was executed against live PostgreSQL 13, 14 and 15 and each returned
    /// <c>ERROR: column i.last_idx_scan does not exist</c> — which fails the WHOLE collection for that
    /// database, every cycle, not just that column. Hence the substitution rather than a bare select.
    /// </summary>
    [Theory]
    [InlineData(13)]
    [InlineData(14)]
    [InlineData(15)]
    public void OmitsLastIdxScanBelowPostgres16_SubstitutingATypedNull(int major)
    {
        var sql = PgIndexUsageStatsCollector.Instance.BuildQuery(MakeContext(major)).Text;

        Assert.DoesNotContain("last_idx_scan", sql, StringComparison.Ordinal);

        /* A TYPED null, so the row SHAPE does not change across a mixed-version fleet. NULL is also the
           honest value: on PostgreSQL 15 the server genuinely does not know when the index was last
           scanned, and a sentinel timestamp would have to be a real instant and would read as one. */
        Assert.Contains("NULL::timestamp", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// And on 16 and above it IS selected, converted to UTC. Without this half the pin would pass on a
    /// collector that had quietly stopped collecting the column everywhere.
    /// </summary>
    [Theory]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(18)]
    public void SelectsLastIdxScanOnPostgres16AndAbove(int major)
    {
        var sql = PgIndexUsageStatsCollector.Instance.BuildQuery(MakeContext(major)).Text;

        Assert.Contains("i.last_idx_scan", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("NULL::timestamp", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// <c>timestamptz::text</c> renders in the SESSION's TimeZone and the store contract is naive UTC, so
    /// both timestamps convert explicitly. Byte-identical to UTC on every instance in the fleet, which is
    /// exactly why it survives every probe and has to be pinned instead.
    /// </summary>
    [Fact]
    public void ConvertsBothTimestampsToUtcExplicitly()
    {
        var sql = PgIndexUsageStatsCollector.Instance.BuildQuery(MakeContext(17)).Text;

        Assert.Contains("(i.last_idx_scan AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
        Assert.Contains("AT TIME ZONE 'UTC')      AS stats_reset", sql, StringComparison.Ordinal);

        Assert.DoesNotContain("last_idx_scan::timestamp", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("stats_reset::timestamp", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The size floor is the fan-out cost control, and the <c>OR NOT s.is_valid</c> beside it is the escape
    /// hatch that keeps it honest. MEASURED live: a 16,384-byte INVALID index came through a 65,536-byte
    /// floor because of that clause — an invalid index is a finding at any size, since the planner will
    /// never use it while writes still maintain it.
    /// </summary>
    [Fact]
    public void FiltersOnTheSizeFloor_WithAnEscapeHatchForInvalidIndexes()
    {
        var sql = PgIndexUsageStatsCollector.Instance.BuildQuery(MakeContext(17)).Text;

        Assert.Contains("WHERE s.index_bytes >= 65536", sql, StringComparison.Ordinal);
        Assert.Contains("OR NOT s.is_valid", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The droppability facts, each present by name. THIS IS THE COLLECTOR: "nobody scans it" is the easy
    /// half, and a column dropped from here turns a safe read into one that tells somebody to drop their
    /// primary key. A uniqueness check is not a scan of the kind <c>idx_scan</c> counts, so the single most
    /// common zero-scan index on any schema is one that must never be dropped — and nothing in the usage
    /// counters can say so.
    /// </summary>
    [Theory]
    [InlineData("x.indisunique")]
    [InlineData("x.indisprimary")]
    [InlineData("x.indisvalid")]
    [InlineData("x.indisready")]
    [InlineData("x.indisreplident")]
    [InlineData("x.indpred IS NOT NULL")]
    [InlineData("x.indexprs IS NOT NULL")]
    [InlineData("con.conindid = i.indexrelid")]
    [InlineData("pg_get_indexdef(i.indexrelid)")]
    public void SelectsEveryFactThatDecidesWhetherAnIndexCanActuallyGo(string fragment)
    {
        Assert.Contains(fragment, PgIndexUsageStatsCollector.Instance.BuildQuery(MakeContext(17)).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The usage counters and the block counters. <c>idx_blks_hit</c> matters next to <c>idx_scan</c>
    /// because it accrues on WRITES too: an index with zero scans and millions of block accesses is being
    /// maintained by the write path and read by nobody, stated in the server's own units.
    /// </summary>
    [Theory]
    [InlineData("i.idx_scan::bigint")]
    [InlineData("i.idx_tup_read::bigint")]
    [InlineData("i.idx_tup_fetch::bigint")]
    [InlineData("io.idx_blks_read")]
    [InlineData("io.idx_blks_hit")]
    [InlineData("pg_relation_size(i.indexrelid)")]
    [InlineData("pg_relation_size(i.relid)")]
    public void SelectsTheUsageAndCostCounters(string fragment)
    {
        Assert.Contains(fragment, PgIndexUsageStatsCollector.Instance.BuildQuery(MakeContext(17)).Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every catalog reference is schema-qualified. <c>pg_catalog</c> is searched implicitly and FIRST, so
    /// an unqualified reference is not wrong today — it is a hostage to whatever a future search_path or a
    /// user object named after a catalog view does to it.
    /// </summary>
    [Theory]
    [InlineData("pg_catalog.pg_stat_user_indexes")]
    [InlineData("pg_catalog.pg_statio_user_indexes")]
    [InlineData("pg_catalog.pg_index")]
    [InlineData("pg_catalog.pg_class")]
    [InlineData("pg_catalog.pg_am")]
    [InlineData("pg_catalog.pg_constraint")]
    [InlineData("pg_catalog.pg_stat_database")]
    public void SchemaQualifiesEveryCatalogReference(string reference)
    {
        Assert.Contains(reference, PgIndexUsageStatsCollector.Instance.BuildQuery(MakeContext(17)).Text, StringComparison.Ordinal);
    }

    [Fact]
    public void PayloadColumns_OrderAndKeyTypes_Pinned()
    {
        var columns = PgIndexUsageStatsCollector.Instance.PayloadColumns;

        Assert.Equal(24, columns.Count);
        Assert.Equal(
            new[]
            {
                "database_name", "schema_name", "table_name", "index_name",
                "index_scans", "tuples_read", "tuples_fetched",
                "blocks_read", "blocks_hit",
                "index_bytes", "table_bytes",
                "is_unique", "is_primary_key", "is_valid", "is_ready",
                "is_replica_identity", "is_partial", "is_expression", "supports_constraint",
                "index_method", "column_count", "index_definition",
                "last_scan", "stats_reset",
            },
            columns.Select(c => c.Name).ToArray());

        Assert.Equal(CollectorColumnType.Varchar, columns[0].Type);      // database_name
        Assert.Equal(CollectorColumnType.BigInt, columns[4].Type);       // index_scans
        Assert.Equal(CollectorColumnType.BigInt, columns[9].Type);       // index_bytes
        Assert.Equal(CollectorColumnType.Boolean, columns[18].Type);     // supports_constraint
        Assert.Equal(CollectorColumnType.Integer, columns[20].Type);     // column_count
        Assert.Equal(CollectorColumnType.Varchar, columns[21].Type);     // index_definition
        Assert.Equal(CollectorColumnType.Timestamp, columns[22].Type);   // last_scan
        Assert.Equal(CollectorColumnType.Timestamp, columns[23].Type);   // stats_reset
    }

    /// <summary>
    /// Every field mapped to its own ordinal. The values are deliberately all DIFFERENT and the booleans
    /// deliberately alternate, so a transposed pair fails rather than passing on two equal values.
    /// </summary>
    [Fact]
    public async Task ReadsAFullyPopulatedRow_WithEveryFieldOnItsOwnOrdinal()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "widget", "widget_status_idx",
                418L, 9_042L, 8_113L,                       // index_scans, tuples_read, tuples_fetched
                276L, 852_057L,                             // blocks_read, blocks_hit
                6_758_400L, 56_516_608L,                    // index_bytes, table_bytes
                true, false, true, false, true, false, true, false,  // the eight droppability booleans
                "btree", 2,
                "CREATE INDEX widget_status_idx ON public.widget USING btree (status, qty)",
                new DateTime(2026, 8, 19, 6, 30, 0, DateTimeKind.Unspecified),
                new DateTime(2026, 5, 18, 7, 4, 22, DateTimeKind.Unspecified),
            });

        var rows = await PgIndexUsageStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal("public", row.SchemaName);
        Assert.Equal("widget", row.TableName);
        Assert.Equal("widget_status_idx", row.IndexName);
        Assert.Equal(418L, row.IndexScans);
        Assert.Equal(9_042L, row.TuplesRead);
        Assert.Equal(8_113L, row.TuplesFetched);
        Assert.Equal(276L, row.BlocksRead);
        Assert.Equal(852_057L, row.BlocksHit);
        Assert.Equal(6_758_400L, row.IndexBytes);
        Assert.Equal(56_516_608L, row.TableBytes);

        /* The alternating booleans, each on its own ordinal. Ordinals 10-17 in select order. */
        Assert.True(row.IsUnique);
        Assert.False(row.IsPrimaryKey);
        Assert.True(row.IsValid);
        Assert.False(row.IsReady);
        Assert.True(row.IsReplicaIdentity);
        Assert.False(row.IsPartial);
        Assert.True(row.IsExpression);
        Assert.False(row.SupportsConstraint);

        Assert.Equal("btree", row.IndexMethod);
        Assert.Equal(2, row.ColumnCount);
        Assert.Equal(
            "CREATE INDEX widget_status_idx ON public.widget USING btree (status, qty)",
            row.IndexDefinition);
        Assert.Equal(new DateTime(2026, 8, 19, 6, 30, 0), row.LastScan);
        Assert.Equal(new DateTime(2026, 5, 18, 7, 4, 22), row.StatsReset);
    }

    /// <summary>
    /// The two NULL shapes this collector really produces, and they mean different things.
    ///
    /// <para>The COUNTERS are NOT NULL in the catalog, so an absent one is a shape change rather than a
    /// value and 0 is the correct reading of "this cumulative count has never moved". The TIMESTAMPS are
    /// genuinely nullable and must stay null: <c>last_scan</c> is NULL on PostgreSQL 15 and below (the
    /// server does not record it) and on 16+ for an index never scanned since the reset;
    /// <c>stats_reset</c> is NULL until the first reset ever, which is the ordinary state and means
    /// "the counters run back to the beginning", NOT "unknown".</para>
    /// </summary>
    [Fact]
    public async Task CountersFallBackToZero_WhileTheTimestampsStayNull()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "widget", "widget_never_scanned_idx",
                DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value, DBNull.Value, DBNull.Value,
                DBNull.Value,
                DBNull.Value,
            });

        var rows = await PgIndexUsageStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Equal(0L, row.IndexScans);
        Assert.Equal(0L, row.TuplesRead);
        Assert.Equal(0L, row.BlocksHit);
        Assert.Equal(0L, row.IndexBytes);

        /* An absent boolean must never read TRUE: is_valid = false is what the read reports as "safe to
           drop", and is_unique / supports_constraint = true is what STOPS it recommending a drop. */
        Assert.False(row.IsUnique);
        Assert.False(row.IsPrimaryKey);
        Assert.False(row.SupportsConstraint);

        Assert.Equal(string.Empty, row.IndexMethod);
        Assert.Equal(0, row.ColumnCount);
        Assert.Equal(string.Empty, row.IndexDefinition);

        Assert.Null(row.LastScan);
        Assert.Null(row.StatsReset);
    }

    /// <summary>
    /// A PostgreSQL 15 target: <c>last_scan</c> arrives NULL because the server does not record it, and the
    /// row must still be complete in every other respect rather than being dropped.
    /// </summary>
    [Fact]
    public async Task ReadsARowFromAMajorThatDoesNotRecordLastScan()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "widget", "widget_pkey",
                200_000L, 0L, 0L,
                2L, 800_429L,
                4_513_792L, 49_233_920L,
                true, true, true, true, false, false, false, true,
                "btree", 1,
                "CREATE UNIQUE INDEX widget_pkey ON public.widget USING btree (id)",
                DBNull.Value,
                DBNull.Value,
            });

        var rows = await PgIndexUsageStatsCollector.Instance.ReadAsync(
            reader, MakeContext(major: 15), CancellationToken.None);

        var row = Assert.Single(rows);
        Assert.Null(row.LastScan);
        Assert.Equal(200_000L, row.IndexScans);
        Assert.True(row.IsPrimaryKey);
        Assert.True(row.SupportsConstraint);
    }

    [Fact]
    public async Task ReturnsNoRowsWhenTheViewReturnsNone()
    {
        var rows = await PgIndexUsageStatsCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>
    /// No stored deltas: the cumulative counter is stored RAW so the window is differenced at read time.
    /// That is what lets the read answer BOTH questions the data supports — the lifetime count and the
    /// windowed one — and keeps a statistics reset visible instead of smoothed away at write time.
    /// </summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();

        PgIndexUsageStatsCollector.Instance.WritePayload(
            SampleRow(),
            new RecordingCollectorRowWriter(),
            MakeContext(deltas: deltas));

        Assert.Empty(deltas.Calls);
    }

    /// <summary>
    /// Every payload column is written, in order. WritePayload is positional, so a column added without a
    /// matching Value() shifts everything after it and stores data that is silently wrong rather than
    /// failing.
    /// </summary>
    [Fact]
    public void WritesEveryPayloadColumnInOrder()
    {
        var writer = new RecordingCollectorRowWriter();

        PgIndexUsageStatsCollector.Instance.WritePayload(SampleRow(), writer, MakeContext());

        Assert.Equal(PgIndexUsageStatsCollector.Instance.PayloadColumns.Count, writer.Values.Count);

        /* The connection's database, NOT a value parsed from the result set: the per-database loop sets it
           and the connection's database IS the row's database, which a parsed value could not guarantee. */
        Assert.Equal("appdb", writer.Values[0]);

        Assert.Equal("public", writer.Values[1]);
        Assert.Equal("widget", writer.Values[2]);
        Assert.Equal("widget_status_idx", writer.Values[3]);
        Assert.Equal(418L, writer.Values[4]);        // index_scans
        Assert.Equal(852_057L, writer.Values[8]);    // blocks_hit
        Assert.Equal(6_758_400L, writer.Values[9]);  // index_bytes
        Assert.Equal(true, writer.Values[11]);       // is_unique
        Assert.Equal(false, writer.Values[18]);      // supports_constraint
        Assert.Equal("btree", writer.Values[19]);
        Assert.Equal(2, writer.Values[20]);          // column_count
        Assert.Equal(new DateTime(2026, 8, 19, 6, 30, 0), writer.Values[22]);   // last_scan
        Assert.Equal(new DateTime(2026, 5, 18, 7, 4, 22), writer.Values[23]);   // stats_reset
    }

    private static PgIndexUsageStatsCollector.Row SampleRow() => new(
        SchemaName: "public",
        TableName: "widget",
        IndexName: "widget_status_idx",
        IndexScans: 418,
        TuplesRead: 9_042,
        TuplesFetched: 8_113,
        BlocksRead: 276,
        BlocksHit: 852_057,
        IndexBytes: 6_758_400,
        TableBytes: 56_516_608,
        IsUnique: true,
        IsPrimaryKey: false,
        IsValid: true,
        IsReady: false,
        IsReplicaIdentity: true,
        IsPartial: false,
        IsExpression: true,
        SupportsConstraint: false,
        IndexMethod: "btree",
        ColumnCount: 2,
        IndexDefinition: "CREATE INDEX widget_status_idx ON public.widget USING btree (status, qty)",
        LastScan: new DateTime(2026, 8, 19, 6, 30, 0),
        StatsReset: new DateTime(2026, 5, 18, 7, 4, 22));

    /// <summary>
    /// DAILY, and the cadence is inherited from index_object_stats — the SQL Server collector answering the
    /// same question — rather than from the PostgreSQL fan-out sibling. "Has anything scanned this index" is
    /// a structural question, not a rate, so an hourly sample would record the same catalog facts 24 times a
    /// day at 24x the fan-out connections.
    ///
    /// <para>90 days is the number that actually matters: the retention window IS the evidence, because an
    /// index can only be called unused for as long as we have been watching it. 30 days cannot clear a
    /// monthly report.</para>
    /// </summary>
    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_index_usage_stats");

        var schedule = CollectorScheduleDefaults.All["pg_index_usage_stats"];

        Assert.Equal(1440, schedule.FrequencyMinutes);
        Assert.Equal(90, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }

    /// <summary>
    /// The capability vocabulary has a noun phrase for this collector, so a SQL Server target asked
    /// get_pg_index_usage is told what it does not collect — and pointed at the fact that get_index_usage is
    /// the tool there — rather than getting the generic fallback, which explains nothing.
    /// </summary>
    [Fact]
    public void TheCapabilityMessageNamesWhatIsNotCollected()
    {
        var message = CollectorEngineCapability.NotCollectedMessage(
            "sql-01", CollectorEngineCapability.UnknownEngineEdition, MonitoredEngineKind.SqlServer, "pg_index_usage_stats");

        Assert.NotNull(message);
        Assert.Contains("pg_stat_user_indexes", message, StringComparison.Ordinal);
        Assert.Contains("droppability", message, StringComparison.Ordinal);
        Assert.DoesNotContain("the data this read is served from", message, StringComparison.Ordinal);
    }
}
