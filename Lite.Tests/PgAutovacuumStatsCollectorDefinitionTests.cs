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
/// Pins the autovacuum collector: the per-table threshold computation that makes a dead-tuple count
/// mean something, the reloptions overrides it honours, the version gating that keeps the row shape
/// constant, and the activity filter — including the append-only case the filter must NOT drop.
/// </summary>
public class PgAutovacuumStatsCollectorDefinitionTests
{
    private static readonly RecordingCollectorDeltaCalculator s_deltas = new();

    private static CollectorContext MakeContext(
        int major = 17, string? database = "appdb", ICollectorDeltaCalculator? deltas = null)
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
            CurrentDatabaseName = database,
        };

    [Fact]
    public void Identity_Pinned()
    {
        Assert.Equal("pg_autovacuum_stats", PgAutovacuumStatsCollector.Instance.Name);
        Assert.Equal("pg_autovacuum_stats", PgAutovacuumStatsCollector.Instance.TargetTable);
        Assert.Equal(CollectorTargetEngine.PostgreSql, PgAutovacuumStatsCollector.Instance.TargetEngine);
    }

    [Fact]
    public void AppliesToAnyPostgresWriterButNeverSqlServer()
    {
        Assert.True(PgAutovacuumStatsCollector.Instance.AppliesTo(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = false }));
        Assert.False(CollectorCatalog.AppliesTo(
            PgAutovacuumStatsCollector.Instance, new CollectorTargetInfo()));
    }

    /// <summary>
    /// Writers only, and NOT because the view is unreadable on a standby — it reads fine and reports all
    /// zeros. Measured on Aurora 17.7, same cluster and database and 15 tables: the writer reported
    /// 13,654,458 dead tuples and 150,790,506 live tuples where the reader reported 0 for every one of
    /// n_dead_tup, n_mod_since_analyze, n_ins_since_vacuum and n_live_tup.
    /// <para>Ungated, a reader would yield zero rows, the activity filter would read that as "nothing has
    /// pending work", and the tool would report perfect autovacuum health for a cluster 13 million dead
    /// tuples behind. That false negative is the reason this gate exists.</para>
    /// </summary>
    [Fact]
    public void DoesNotRunOnAStandbyWhereEveryCounterReadsZero()
    {
        var reader = new CollectorTargetInfo
        {
            Engine = CollectorTargetEngine.PostgreSql,
            IsAurora = true,
            IsInRecovery = true,
        };

        Assert.False(PgAutovacuumStatsCollector.Instance.AppliesTo(reader));
        Assert.False(CollectorCatalog.AppliesTo(PgAutovacuumStatsCollector.Instance, reader));

        /* The engine gate is a separate concern and must still agree it is a Postgres collector — a
           reader is skipped for the RECOVERY reason, not because it stopped being PostgreSQL. */
        Assert.True(CollectorCatalog.EngineMatches(PgAutovacuumStatsCollector.Instance.Name, reader));
    }

    /// <summary>
    /// pg_stat_user_tables is scoped to the connected database and PostgreSQL has no cross-database
    /// read, so this must fan out on EVERY target — unlike the SQL Server collectors, where per-database
    /// is an Azure-only shape.
    /// </summary>
    [Fact]
    public void AlwaysRunsPerDatabase()
    {
        Assert.True(PgAutovacuumStatsCollector.Instance.RunsPerDatabase(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql }));
        Assert.True(PgAutovacuumStatsCollector.Instance.RunsPerDatabase(
            new CollectorTargetInfo { Engine = CollectorTargetEngine.PostgreSql, IsAurora = true }));
    }

    /// <summary>
    /// The threshold is the whole point: a dead-tuple count is not actionable without the line it has to
    /// cross. Both terms of autovacuum's formula must be present, and reltuples must be floored — it is
    /// -1 on a never-analyzed table (PG14+), which unfloored yields a NEGATIVE threshold and makes such a
    /// table read as permanently overdue.
    /// </summary>
    [Fact]
    public void ComputesTheAutovacuumThresholdFromBothTerms()
    {
        var sql = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("autovacuum_vacuum_threshold", sql, StringComparison.Ordinal);
        Assert.Contains("autovacuum_vacuum_scale_factor", sql, StringComparison.Ordinal);
        Assert.Contains("GREATEST(c.reltuples, 0)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// Per-table reloptions must win over the GUCs, because ALTER TABLE ... SET (autovacuum_*) is common
    /// on exactly the big hot tables where the global default is wrong. Reading only the GUC would report
    /// a threshold the server is not using, which is worse than reporting none — it looks authoritative.
    /// </summary>
    [Fact]
    public void PrefersPerTableReloptionsOverTheServerSettings()
    {
        var sql = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("pg_options_to_table(c.reloptions)", sql, StringComparison.Ordinal);
        /* coalesce(reloption, current_setting(...)) — the override first, the GUC as fallback. */
        Assert.Contains("current_setting('autovacuum_vacuum_threshold')", sql, StringComparison.Ordinal);
        Assert.Matches(
            @"coalesce\(\s*\(SELECT option_value FROM pg_options_to_table\(c\.reloptions\)\s*WHERE option_name = 'autovacuum_vacuum_threshold'\)::bigint,\s*current_setting",
            sql.Replace("\n", " ").Replace("\r", " "));
    }

    /// <summary>A table with autovacuum switched off is its own finding, so the flag must be read.</summary>
    [Fact]
    public void DetectsAutovacuumDisabledPerTable()
    {
        var sql = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        Assert.Contains("'autovacuum_enabled'", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The insert-only path is PG13+, substituted with -1 on older majors so the row shape is constant.
    /// A version-conditional COLUMN COUNT would change the table shape mid-fleet.
    /// </summary>
    [Fact]
    public void GatesTheInsertOnlyPathByVersionWithoutChangingShape()
    {
        var pg17 = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext(17)).Text;
        var pg12 = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext(12)).Text;

        Assert.Contains("t.n_ins_since_vacuum", pg17, StringComparison.Ordinal);
        Assert.Contains("autovacuum_vacuum_insert_threshold", pg17, StringComparison.Ordinal);

        Assert.DoesNotContain("n_ins_since_vacuum", pg12, StringComparison.Ordinal);
        Assert.DoesNotContain("autovacuum_vacuum_insert_threshold", pg12, StringComparison.Ordinal);

        Assert.Equal(pg17.Split(" AS ").Length, pg12.Split(" AS ").Length);
    }

    /// <summary>
    /// The filter's most important clause. An append-only table has NO dead tuples and NO modifications,
    /// so the dead-tuple and analyze predicates both miss it — and an append-only table that is never
    /// vacuumed is never frozen either, which is a route into a wraparound emergency. Filtering on dead
    /// tuples alone would drop exactly the tables this collector exists to surface.
    /// </summary>
    [Fact]
    public void ActivityFilterKeepsAppendOnlyTables()
    {
        var sql = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext(17)).Text;

        Assert.Contains("t.n_dead_tup > 0", sql, StringComparison.Ordinal);
        Assert.Contains("t.n_mod_since_analyze > 0", sql, StringComparison.Ordinal);
        Assert.Contains("OR t.n_ins_since_vacuum > 0", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// On a major with no insert counter the filter must still be VALID SQL — the clause is dropped, not
    /// left as a dangling OR.
    /// </summary>
    [Fact]
    public void ActivityFilterStaysWellFormedWithoutTheInsertCounter()
    {
        var sql = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext(12)).Text;

        Assert.DoesNotContain("OR t.n_ins_since_vacuum", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("OR  OR", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("( OR", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The maintenance timestamps are `timestamp with time zone` and must be converted with
    /// AT TIME ZONE 'UTC', never `::timestamp`. The cast form renders the instant in the SESSION's
    /// TimeZone before dropping the offset, so it agrees with UTC only while every server's timezone GUC
    /// says UTC — true across the fleet today, which is precisely what would keep the bug hidden. The
    /// store contract is naive UTC product-wide.
    /// </summary>
    [Fact]
    public void ConvertsTimestamptzColumnsWithAnExplicitUtcZoneNotACast()
    {
        var sql = PgAutovacuumStatsCollector.Instance.BuildQuery(MakeContext()).Text;

        foreach (var column in new[] { "last_vacuum", "last_autovacuum", "last_analyze", "last_autoanalyze" })
        {
            Assert.Contains($"(t.{column} AT TIME ZONE 'UTC')", sql, StringComparison.Ordinal);
            Assert.DoesNotContain($"t.{column}::timestamp", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void PayloadColumns_CountAndKeyTypes_Pinned()
    {
        var columns = PgAutovacuumStatsCollector.Instance.PayloadColumns;

        Assert.Equal(20, columns.Count);
        Assert.Equal("database_name", columns[0].Name);
        Assert.Equal("dead_tuples", columns[4].Name);
        Assert.Equal("vacuum_threshold", columns[7].Name);
        Assert.Equal(CollectorColumnType.Boolean, columns[10].Type);   // autovacuum_disabled
        Assert.Equal(CollectorColumnType.Timestamp, columns[12].Type); // last_vacuum
    }

    /// <summary>
    /// database_name comes from the per-database loop's connection, not the result set: pg_stat_user_tables
    /// shows only the connected database, so the connection IS the authoritative answer. A row written with
    /// no database context would be unattributable.
    /// </summary>
    [Fact]
    public void StampsTheDatabaseFromTheConnectionNotThePayload()
    {
        var writer = new RecordingCollectorRowWriter();

        PgAutovacuumStatsCollector.Instance.WritePayload(
            new PgAutovacuumStatsCollector.Row(
                "public", "orders", 1_000_000, 250_000, 40_000, 0, 200_050, -1, 100_025, false,
                8_589_934_592, null, new DateTime(2026, 8, 11, 6, 0, 0), null, null, 0, 12, 0, 9),
            writer,
            MakeContext(database: "warehouse"));

        Assert.Equal("warehouse", writer.Values[0]);
        Assert.Equal("public", writer.Values[1]);
        Assert.Equal("orders", writer.Values[2]);
    }

    /// <summary>
    /// The reading the collector exists to enable, end to end: dead tuples well past this table's own
    /// threshold, with autovacuum having last run hours ago.
    /// </summary>
    [Fact]
    public async Task ReadsATableThatIsPastItsOwnThreshold()
    {
        var reader = new FakeCollectorDataReader(
            new object[]
            {
                "public", "orders", 1_000_000L, 250_000L, 40_000L, 0L,
                200_050L, -1L, 100_025L, false, 8_589_934_592L,
                DBNull.Value, new DateTime(2026, 8, 11, 6, 0, 0, DateTimeKind.Unspecified),
                DBNull.Value, DBNull.Value, 0L, 12L, 0L, 9L,
            });

        var rows = await PgAutovacuumStatsCollector.Instance.ReadAsync(reader, MakeContext(), CancellationToken.None);

        var table = Assert.Single(rows);
        Assert.Equal(250_000L, table.DeadTuples);
        Assert.Equal(200_050L, table.VacuumThreshold);
        Assert.True(table.DeadTuples > table.VacuumThreshold);
        Assert.Equal(-1L, table.InsertVacuumThreshold);   // pre-13 sentinel, shape preserved
        Assert.NotNull(table.LastAutovacuum);
        Assert.Null(table.LastVacuum);                    // never manually vacuumed
    }

    /// <summary>A clean, quiet database yields no rows — that is the healthy case, not a failure.</summary>
    [Fact]
    public async Task NoTablesWithPendingWorkYieldsNoRows()
    {
        var rows = await PgAutovacuumStatsCollector.Instance.ReadAsync(
            new FakeCollectorDataReader(), MakeContext(), CancellationToken.None);

        Assert.Empty(rows);
    }

    /// <summary>
    /// Levels and lifetime counts read against a threshold — no deltas. The useful reading is how far past
    /// the line a table is now, and the timestamps say when maintenance last ran without any arithmetic.
    /// </summary>
    [Fact]
    public void TakesNoDeltas()
    {
        var deltas = new RecordingCollectorDeltaCalculator();

        PgAutovacuumStatsCollector.Instance.WritePayload(
            new PgAutovacuumStatsCollector.Row(
                "public", "t", 1, 1, 0, 0, 50, -1, 50, false, 8192, null, null, null, null, 0, 0, 0, 0),
            new RecordingCollectorRowWriter(),
            MakeContext(deltas: deltas));

        Assert.Empty(deltas.Calls);
    }

    [Fact]
    public void RegisteredInBothTheCatalogAndTheSchedule()
    {
        Assert.Contains(CollectorCatalog.All, d => d.Name == "pg_autovacuum_stats");

        var schedule = CollectorScheduleDefaults.All["pg_autovacuum_stats"];

        /* Hourly, not per-minute: this is a per-database fan-out, and on PostgreSQL that means one
           CONNECTION per database per cycle. */
        Assert.Equal(60, schedule.FrequencyMinutes);
        Assert.Equal(90, schedule.RetentionDays);
        Assert.True(schedule.DefaultEnabled);
    }
}
