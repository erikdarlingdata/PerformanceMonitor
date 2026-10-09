/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #5558: the pure rule that names the databases this node holds only as a secondary copy. Every fail-open case
/// is pinned: skipping a database wrongly hides a finding, so anything unknown skips nothing.
/// </summary>
[Trait("Reads", "Darling")]
public sealed class AgReplicaScopeTests
{
    private static readonly DateTime End = new(2026, 10, 8, 12, 0, 0, DateTimeKind.Utc);

    private static AgReplicaReading Replica(string ag, string role, bool? local = true, string server = "NODE1") =>
        new(ag, server, role, "CONNECTED", local);

    private static AgDatabaseMembership Db(string ag, string db, bool? local = true) => new(ag, db, local);

    [Fact]
    public void ThePrimary_SkipsNothing()
    {
        var set = AgReplicaScope.SecondaryDatabases([Replica("AG1", "PRIMARY")], [Db("AG1", "Sales")]);
        Assert.Empty(set);
    }

    [Fact]
    public void TheSecondary_SkipsItsDatabases_AndOnlyTheLocalRows()
    {
        var set = AgReplicaScope.SecondaryDatabases(
            [Replica("AG1", "SECONDARY"), Replica("AG1", "PRIMARY", local: false, server: "NODE2")],
            [Db("AG1", "Sales"), Db("AG1", "Orders"), Db("AG1", "RemoteOnly", local: false)]);
        Assert.Equal(["Orders", "Sales"], set.OrderBy(n => n).ToArray());
    }

    [Fact]
    public void TwoGroupsWithDifferentPrimaries_AreJudgedPerGroup()
    {
        var set = AgReplicaScope.SecondaryDatabases(
            [Replica("AG1", "PRIMARY"), Replica("AG2", "SECONDARY")],
            [Db("AG1", "OnPrimary"), Db("AG2", "OnSecondary")]);
        Assert.Equal(["OnSecondary"], set.ToArray());
    }

    [Theory]
    [InlineData("RESOLVING")]
    [InlineData("")]
    [InlineData(null)]
    [InlineData("UNKNOWN")]
    public void ARoleThatIsNotExplicitlySecondary_SkipsNothing(string? role)
    {
        Assert.Empty(AgReplicaScope.SecondaryDatabases([Replica("AG1", role!)], [Db("AG1", "Sales")]));
    }

    [Fact]
    public void ANullIsLocal_OnEitherGrain_SkipsNothing()
    {
        Assert.Empty(AgReplicaScope.SecondaryDatabases([Replica("AG1", "SECONDARY", local: null)], [Db("AG1", "Sales")]));
        Assert.Empty(AgReplicaScope.SecondaryDatabases([Replica("AG1", "SECONDARY")], [Db("AG1", "Sales", local: null)]));
        Assert.Empty(AgReplicaScope.SecondaryDatabases([Replica("AG1", "SECONDARY", local: false)], [Db("AG1", "Sales")]));
    }

    [Fact]
    public void NoRows_OrAStandaloneDatabase_SkipsNothing()
    {
        Assert.Empty(AgReplicaScope.SecondaryDatabases(null, null));
        Assert.Empty(AgReplicaScope.SecondaryDatabases([], []));
        Assert.Empty(AgReplicaScope.SecondaryDatabases([Replica("AG1", "SECONDARY")], []));
        Assert.Empty(AgReplicaScope.SecondaryDatabases([], [Db("AG1", "Sales")]));
        /* A database in a group the replica snapshot does not know about. */
        Assert.Empty(AgReplicaScope.SecondaryDatabases([Replica("AG1", "SECONDARY")], [Db("AG9", "Sales")]));
    }

    [Fact]
    public void RoleCaseDoesNotMatter_ButDatabaseNamesAreKeptExactly()
    {
        /* #5558 round 2: the snapshot's database_name is sys.databases.name, the same catalog the replicated facts'
           names come from, so the casing is identical on both sides and the match is ordinal. On a case-sensitive
           server collation a secondary SalesDb and a separate salesdb are two databases. */
        var set = AgReplicaScope.SecondaryDatabases([Replica("ag1", "secondary")], [Db("AG1", "SalesDb")]);
        Assert.Contains("SalesDb", set);
        Assert.DoesNotContain("salesdb", set);
        Assert.True(AgReplicaScope.IsSkipped(set, "SalesDb"));
        Assert.False(AgReplicaScope.IsSkipped(set, "salesdb"));
        Assert.Equal(["salesdb"], AgReplicaScope.WithoutSecondaries(["SalesDb", "salesdb"], set));
    }

    [Fact]
    public void TwoLocalRowsThatDisagree_AreATornSnapshot_AndSkipNothing()
    {
        Assert.Empty(AgReplicaScope.SecondaryDatabases(
            [Replica("AG1", "SECONDARY"), Replica("AG1", "PRIMARY")], [Db("AG1", "Sales")]));
    }

    [Fact]
    public void AStaleOrMissingSnapshot_SkipsNothing_AndAFutureOneIsNotUsed()
    {
        var replicas = new[] { Replica("AG1", "SECONDARY") };
        var databases = new[] { Db("AG1", "Sales") };

        Assert.Single(AgReplicaScope.SecondaryDatabases(replicas, End.AddMinutes(-1), databases, End.AddMinutes(-2), End));
        Assert.Empty(AgReplicaScope.SecondaryDatabases(replicas, End.AddHours(-2), databases, End.AddMinutes(-1), End));
        Assert.Empty(AgReplicaScope.SecondaryDatabases(replicas, End.AddMinutes(-1), databases, End.AddHours(-2), End));
        Assert.Empty(AgReplicaScope.SecondaryDatabases(replicas, null, databases, End.AddMinutes(-1), End));
        Assert.Empty(AgReplicaScope.SecondaryDatabases(replicas, End.AddMinutes(-1), databases, End.AddMinutes(1), End));
    }

    [Fact]
    public void TheNote_IsOneSentence_ForOneAndMany_AndNullForNone()
    {
        Assert.Null(AgReplicaScope.SkippedNote(0));
        Assert.Contains("1 database skipped", AgReplicaScope.SkippedNote(1));
        var many = AgReplicaScope.SkippedNote(3);
        Assert.StartsWith("3 databases skipped: this server holds a secondary copy of them in an availability group.", many);
        Assert.EndsWith("Check the primary replica for their findings.", many);
    }
}

/// <summary>#5558: the table that classifies facts, and the census that keeps it complete.</summary>
[Trait("Reads", "Darling")]
public sealed class FactReplicaScopeCoverageTests
{
    [Theory]
    [InlineData("DB_CONFIG")]
    [InlineData("DB_CONFIG_RCSI")]
    [InlineData("FILE_AUTOGROWTH_PERCENT")]
    [InlineData("PLAN_REGRESSION")]
    [InlineData("ANOMALY_OBJECT_GROWTH")]
    public void TheReplicatedFacts_AreFiltered(string key) => Assert.Equal(FactReplicaKind.Replicated, FactReplicaScope.Of(key));

    [Theory]
    [InlineData("MISSING_INDEX")]
    [InlineData("PLAN_WARNING")]
    [InlineData("BAD_ACTOR_0xF00D")]
    [InlineData("PARAMETER_SENSITIVITY")]
    [InlineData("QUERY_SPILLS")]
    [InlineData("QUERY_HIGH_DOP")]
    [InlineData("PROCEDURE_STATS")]
    [InlineData("IO_READ_LATENCY_MS")]
    [InlineData("DATABASE_TOTAL_SIZE_MB")]
    [InlineData("DISK_SPACE")]
    [InlineData("TEMPDB_USAGE")]
    [InlineData("DEADLOCKS")]
    [InlineData("ANOMALY_OBJECT_CONTENTION")]
    [InlineData("CONFIG_MAXDOP")]
    [InlineData("CXPACKET")]
    public void TheNodeLocalFacts_AreKept(string key) => Assert.Equal(FactReplicaKind.NodeLocal, FactReplicaScope.Of(key));

    [Fact]
    public void AReplicatedKey_YieldsNames_OnlyWhenTheContextHasASecondarySet()
    {
        var empty = new AnalysisContext();
        Assert.Empty(FactReplicaScope.SecondariesFor(empty, "DB_CONFIG"));

        var context = new AnalysisContext { SecondaryReplicaDatabases = new HashSet<string>(StringComparer.Ordinal) { "SecDb" } };
        Assert.Equal(["SecDb"], FactReplicaScope.SecondariesFor(context, "DB_CONFIG"));
        Assert.Empty(FactReplicaScope.SecondariesFor(context, "MISSING_INDEX"));
    }

    /// <summary>Every fact key a collector writes as a literal must be in the table, so a new fact is classified on
    /// purpose. Both products' collectors are scanned: Darling's port emits the same keys as Lite's.</summary>
    [Fact]
    public void EveryLiteralFactKeyTheCollectorsEmit_IsInTheTable()
    {
        var root = FindRepoRoot();
        var sources = new List<string>();
        sources.AddRange(Directory.GetFiles(Path.Combine(root, "Lite", "Analysis"), "*.cs"));
        var darling = Path.Combine(root, "Darling", "PerformanceMonitor.Darling.Analysis");
        sources.AddRange(Directory.GetFiles(darling, "PgFactCollector*.cs"));
        sources.Add(Path.Combine(darling, "PgAnomalyDetector.cs"));

        var emitted = new SortedSet<string>(StringComparer.Ordinal);
        var pattern = new Regex("\\bKey = \"([A-Z][A-Z0-9_]+)\"", RegexOptions.CultureInvariant);
        foreach (var file in sources)
            foreach (Match m in pattern.Matches(File.ReadAllText(file)))
                emitted.Add(m.Groups[1].Value);

        Assert.NotEmpty(emitted);
        var missing = emitted.Where(k => !FactReplicaScope.IsKnown(k)).ToArray();
        Assert.True(missing.Length == 0, "fact keys missing from FactReplicaScope: " + string.Join(", ", missing));
    }

    private static string FindRepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !Directory.Exists(Path.Combine(dir.FullName, "Lite", "Analysis"))) dir = dir.Parent;
        return dir?.FullName ?? throw new InvalidOperationException("repo root not found");
    }
}

/// <summary>
/// #5558 on a seeded DuckDB store: the replicated facts drop the database this node holds as a secondary copy and
/// the node-local ones keep it. Three databases: <c>SecDb</c> (local secondary), <c>PrimDb</c> (local primary of a
/// second group) and <c>StandDb</c> (not in a group).
/// </summary>
[Trait("Reads", "Darling")]
public sealed class SecondaryReplicaFactTests : IClassFixture<SharedDuckDbFixture>
{
    private const int ServerId = -558_001;
    private static long s_id = -55_800_000;

    private readonly DuckDbInitializer _duckDb;

    public SecondaryReplicaFactTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    private static DateTime Now() => LatestValueSeed.TruncateToSeconds(DateTime.UtcNow);

    private async Task SeedAsync(Func<DuckDBConnection, Task> seed)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        await seed(connection);
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql, params object[] values)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        foreach (var v in values) cmd.Parameters.Add(new DuckDBParameter { Value = v });
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private static async Task SeedAgAsync(DuckDBConnection c, DateTime at, string secondaryRole = "SECONDARY")
    {
        const string Replica = "INSERT INTO ag_replica_states (collection_id, collection_time, server_id, server_name, ag_name, replica_server_name, role_desc, is_local) VALUES ($1,$2,$3,'n',$4,$5,$6,$7)";
        await ExecAsync(c, Replica, Interlocked.Decrement(ref s_id), at, ServerId, "AG1", "NODE1", secondaryRole, true);
        await ExecAsync(c, Replica, Interlocked.Decrement(ref s_id), at, ServerId, "AG1", "NODE2", "PRIMARY", false);
        await ExecAsync(c, Replica, Interlocked.Decrement(ref s_id), at, ServerId, "AG2", "NODE1", "PRIMARY", true);
        const string Db = "INSERT INTO ag_database_replica_states (collection_id, collection_time, server_id, server_name, ag_name, database_name, replica_server_name, is_local) VALUES ($1,$2,$3,'n',$4,$5,$6,$7)";
        await ExecAsync(c, Db, Interlocked.Decrement(ref s_id), at, ServerId, "AG1", "SecDb", "NODE1", true);
        await ExecAsync(c, Db, Interlocked.Decrement(ref s_id), at, ServerId, "AG1", "SecDb", "NODE2", false);
        await ExecAsync(c, Db, Interlocked.Decrement(ref s_id), at, ServerId, "AG2", "PrimDb", "NODE1", true);
    }

    private static async Task SeedDatabasesAsync(DuckDBConnection c, DateTime end)
    {
        foreach (var db in new[] { "SecDb", "PrimDb", "StandDb" })
        {
            await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, end.AddDays(-1), db, autoShrink: true, rcsiOn: false);
            await LatestValueSeed.InsertSizeAsync(c, ServerId, end.AddMinutes(-30), db, 1, "ROWS", db + "_data", 20_480, true, "D:\\", 1_000_000, 500_000);
            await LatestValueSeed.InsertFileIoAsync(c, ServerId, end.AddMinutes(-5), db, db + "_data", 1000);
        }
    }

    private static AnalysisContext Context(DateTime end) => new()
    {
        ServerId = ServerId,
        ServerName = "ag-scope",
        TimeRangeStart = end.AddHours(-4),
        TimeRangeEnd = end,
    };

    private async Task<(Dictionary<string, Fact> Facts, AnalysisContext Context)> FactsAsync(DateTime end)
    {
        var context = Context(end);
        await SecondaryReplicaScope.EnsureAsync(_duckDb, context);
        var facts = (await new DuckDbFactCollector(_duckDb).CollectFactsAsync(context)).ToDictionary(f => f.Key);
        return (facts, context);
    }

    [Fact]
    public async Task OnASecondary_DbConfigAndAutogrowthDropTheSecondaryDatabase_AndNodeLocalFactsKeepIt()
    {
        var end = Now();
        await SeedAsync(async c => { await SeedDatabasesAsync(c, end); await SeedAgAsync(c, end.AddMinutes(-1)); });

        var (facts, context) = await FactsAsync(end);

        Assert.Equal(["SecDb"], context.SecondaryReplicaDatabases!.ToArray());
        var config = facts["DB_CONFIG"];
        Assert.Equal(2, config.Metadata["database_count"]);
        Assert.Equal(2, config.Metadata["auto_shrink_on_count"]);
        Assert.Equal(2, config.Metadata["rcsi_off_count"]);

        var autogrowth = facts["FILE_AUTOGROWTH_PERCENT"];
        Assert.Equal(2, autogrowth.Value);
        Assert.Equal(2, autogrowth.Metadata["database_count"]);

        /* Node-local: the secondary database's own I/O footprint is still this node's. */
        Assert.Equal(3000.0, facts["DATABASE_TOTAL_SIZE_MB"].Value, precision: 6);

        var drill = new DrillDownCollector(_duckDb);
        var files = await LatestValueSeed.AutogrowthFilesAsync(drill, context);
        Assert.Equal(["PrimDb", "StandDb"], files.Select(f => f.GetProperty("database").GetString()!).OrderBy(n => n).ToArray());
    }

    [Fact]
    public async Task ASecondarySecDb_DoesNotHideASeparateDatabaseNamedSecdb()
    {
        /* #5558 round 2: on a case-sensitive server collation these are two databases; the filter compares the
           exact name, so the lower-case twin keeps its DB_CONFIG row. */
        var end = Now();
        await SeedAsync(async c =>
        {
            await SeedDatabasesAsync(c, end);
            await LatestValueSeed.InsertDatabaseConfigAsync(c, ServerId, end.AddDays(-1), "secdb", autoShrink: true, rcsiOn: false);
            await SeedAgAsync(c, end.AddMinutes(-1));
        });

        var (facts, context) = await FactsAsync(end);

        Assert.Equal(["SecDb"], context.SecondaryReplicaDatabases!.ToArray());
        var config = facts["DB_CONFIG"];
        Assert.Equal(3, config.Metadata["database_count"]);
        Assert.Equal(3, config.Metadata["auto_shrink_on_count"]);
    }

    [Fact]
    public async Task CompareAndFactsReads_CarryTheSetTheirContextFilteredWith_SoTheNoteCannotDisagree()
    {
        /* #5558 round 2: compare_analysis and get_analysis_facts build secondary_replica_note from what the read
           returns, not from a second read of the role. The baseline window ends BEFORE the only snapshot (nothing known,
           nothing skipped) and the comparison window ends after it (SecDb skipped): each window's returned set is its own. */
        var end = Now();
        await SeedAsync(async c => { await SeedDatabasesAsync(c, end); await SeedAgAsync(c, end.AddMinutes(-1)); });
        var service = new AnalysisService(_duckDb);

        var (_, _, _, _, _, baselineSecondaries, comparisonSecondaries) = await service.ComparePeriodsAsync(
            ServerId, "ag-scope",
            end.AddHours(-30), end.AddHours(-26), end.AddHours(-4), end,
            TestContext.Current.CancellationToken);
        Assert.Empty(baselineSecondaries!);
        Assert.Null(AgReplicaScope.SkippedNote(baselineSecondaries));
        Assert.Equal(["SecDb"], comparisonSecondaries!.ToArray());
        Assert.Equal(AgReplicaScope.SkippedNote(1), AgReplicaScope.SkippedNote(comparisonSecondaries));

        var (_, _, caveats) = await service.CollectAndScoreFactsAsync(ServerId, "ag-scope", 4, end, TestContext.Current.CancellationToken);
        Assert.Equal(["SecDb"], caveats.SecondaryReplicaDatabases!.ToArray());
    }

    [Fact]
    public async Task TheDbConfigDrillDown_ListsTheSameDatabasesTheFactCounted()
    {
        var end = Now();
        await SeedAsync(async c => { await SeedDatabasesAsync(c, end); await SeedAgAsync(c, end.AddMinutes(-1)); });
        var (_, context) = await FactsAsync(end);

        var finding = new AnalysisFinding
        {
            ServerId = ServerId, RootFactKey = "DB_CONFIG", StoryPath = "DB_CONFIG", PathKeys = ["DB_CONFIG"], Severity = 0.3,
        };
        await new DrillDownCollector(_duckDb).EnrichFindingsAsync([finding], context);

        var json = System.Text.Json.JsonSerializer.Serialize(finding.DrillDown);
        Assert.Contains("PrimDb", json);
        Assert.Contains("StandDb", json);
        Assert.DoesNotContain("SecDb", json);
    }

    [Fact]
    public async Task OnThePrimary_OrWithNoOrStaleAgRows_NothingIsSkipped()
    {
        var end = Now();

        /* No AG rows at all. */
        await SeedAsync(c => SeedDatabasesAsync(c, end));
        var (noRows, noRowsContext) = await FactsAsync(end);
        Assert.Empty(noRowsContext.SecondaryReplicaDatabases!);
        Assert.Equal(3, noRows["DB_CONFIG"].Metadata["database_count"]);

        /* Rows exist, but the newest is three hours old: stale, so the role vouches for nothing. */
        await SeedAsync(c => SeedAgAsync(c, end.AddHours(-3)));
        var (stale, staleContext) = await FactsAsync(end);
        Assert.Empty(staleContext.SecondaryReplicaDatabases!);
        Assert.Equal(3, stale["DB_CONFIG"].Metadata["database_count"]);

        /* A fresh snapshot whose local role is PRIMARY for the group. */
        await SeedAsync(c => SeedAgAsync(c, end.AddMinutes(-1), secondaryRole: "PRIMARY"));
        var (primary, primaryContext) = await FactsAsync(end);
        Assert.Empty(primaryContext.SecondaryReplicaDatabases!);
        Assert.Equal(3, primary["DB_CONFIG"].Metadata["database_count"]);
    }

    [Fact]
    public async Task TheRoleIsReadFromTheArchive_WhenTheHotTablesHoldNoAgRows()
    {
        var end = Now();
        var dir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(Path.Combine(dir, "archive"));
        try
        {
            var dbPath = Path.Combine(dir, "test.duckdb");
            using var initializer = new DuckDbInitializer(dbPath);
            await initializer.InitializeFromTemplateAsync();

            /* The AG snapshot an AsOf or compare window reaches once archival has moved it to Parquet (after 7 days, or
               all of it at the 512 MB reset): the hot tables are empty afterwards, only the v_ views still see it. */
            var at = end.AddDays(-10);
            using (var connection = new DuckDBConnection($"Data Source={dbPath}"))
            {
                await connection.OpenAsync(TestContext.Current.CancellationToken);
                await SeedAgAsync(connection, at);
                foreach (var table in new[] { "ag_replica_states", "ag_database_replica_states" })
                {
                    var parquet = Path.Combine(dir, "archive", "20260101_0000_" + table + ".parquet").Replace("\\", "/");
                    await ExecAsync(connection, $"COPY {table} TO '{parquet}' (FORMAT PARQUET)");
                    await ExecAsync(connection, $"DELETE FROM {table}");
                }
            }

            await initializer.CreateArchiveViewsAsync();

            var archived = await SecondaryReplicaScope.ReadAsync(initializer, ServerId, at.AddMinutes(1), CancellationToken.None);
            Assert.Equal(["SecDb"], archived.ToArray());

            /* A bare-table read finds nothing there, which is the bug this pins: it would have skipped nothing. */
            using var bare = new DuckDBConnection($"Data Source={dbPath}");
            await bare.OpenAsync(TestContext.Current.CancellationToken);
            using var count = bare.CreateCommand();
            count.CommandText = "SELECT COUNT(*) FROM ag_replica_states";
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(TestContext.Current.CancellationToken)));
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    [Fact]
    public async Task AnAsOfPass_UsesTheRoleAtItsOwnEnd_NotTheCurrentOne()
    {
        var now = Now();
        /* Primary three hours ago, secondary a minute ago (a failover in between). */
        await SeedAsync(async c =>
        {
            await SeedDatabasesAsync(c, now);
            await SeedAgAsync(c, now.AddHours(-3).AddMinutes(-1), secondaryRole: "PRIMARY");
            await SeedAgAsync(c, now.AddMinutes(-1));
        });

        var current = await SecondaryReplicaScope.ReadAsync(_duckDb, ServerId, now, CancellationToken.None);
        Assert.Equal(["SecDb"], current.ToArray());

        var earlier = await SecondaryReplicaScope.ReadAsync(_duckDb, ServerId, now.AddHours(-3), CancellationToken.None);
        Assert.Empty(earlier);
    }

    [Fact]
    public async Task PlanRegression_DropsTheSecondaryDatabase()
    {
        var end = Now();
        await SeedAsync(async c =>
        {
            await SeedAgAsync(c, end.AddMinutes(-1));
            foreach (var db in new[] { "SecDb", "StandDb" })
            {
                await SeedRegressionAsync(c, end, db, planId: 1, hash: "0xGOOD", avgCpu: 100_000, lastExec: end.AddDays(-5), firstExec: end.AddDays(-6));
                await SeedRegressionAsync(c, end, db, planId: 2, hash: "0xBAD", avgCpu: 1_200_000, lastExec: end, firstExec: end.AddDays(-1));
            }
        });

        var (facts, context) = await FactsAsync(end);

        Assert.Equal(1, facts["PLAN_REGRESSION"].Metadata["offender_count"]);
        Assert.All(context.PlanRegressionOffenders!, o => Assert.Equal("StandDb", o.DatabaseName));
    }

    [Fact]
    public async Task ObjectGrowth_DropsTheSecondaryDatabase_ButTheContentionAnomalyIsNotTouched()
    {
        var end = Now();
        await SeedAsync(async c =>
        {
            await SeedAgAsync(c, end.AddMinutes(-1));
            /* SecDb also gains the largest lock-wait time, so the contention anomaly (node-local, never filtered) has a
               secondary-copy database to name. */
            foreach (var (db, grownMb, lockWaitMs) in new[] { ("SecDb", 900m, 900_000L), ("StandDb", 500m, 200_000L) })
            {
                await SeedObjectAsync(c, end.AddDays(-1), db, 100m);
                await SeedObjectAsync(c, end, db, 100m + grownMb, lockWaitMs);
            }
            for (var d = 1; d <= 5; d++)
                await ExecAsync(c, "INSERT INTO cpu_utilization_stats (collection_id, collection_time, server_id, server_name, sample_time, sqlserver_cpu_utilization, other_process_cpu_utilization) VALUES ($1,$2,$3,'n',$2,10,0)",
                    Interlocked.Decrement(ref s_id), end.AddDays(-d), ServerId);
        });

        var context = Context(end);
        await SecondaryReplicaScope.EnsureAsync(_duckDb, context);
        var detector = new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb));
        var anomalies = await detector.DetectAnomaliesAsync(context);

        var growth = Assert.Single(anomalies, f => f.Key == "ANOMALY_OBJECT_GROWTH");
        Assert.Equal("StandDb", growth.DatabaseName);

        var contention = Assert.Single(anomalies, f => f.Key == "ANOMALY_OBJECT_CONTENTION");
        Assert.Equal("SecDb", contention.DatabaseName);
    }

    private static Task SeedRegressionAsync(DuckDBConnection c, DateTime end, string db, long planId, string hash, long avgCpu,
        DateTime lastExec, DateTime firstExec) =>
        ExecAsync(c, @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
     first_execution_time, last_execution_time, query_text, query_hash, execution_count, avg_cpu_time_us,
     avg_duration_us, query_plan_hash, is_forced_plan, force_failure_count)
VALUES ($1,$2,$3,'n',$4,101,$5,'Regular',$6,$7,'SELECT 1','0xQH',100,$8,$8,$9,false,0)",
            Interlocked.Decrement(ref s_id), end, ServerId, db, planId, firstExec, lastExec, avgCpu, hash);

    private static Task SeedObjectAsync(DuckDBConnection c, DateTime at, string db, decimal reservedMb, long lockWaitMs = 0) =>
        ExecAsync(c, @"
INSERT INTO index_object_stats
    (collection_id, collection_time, server_id, server_name, sqlserver_start_time, database_name, database_id,
     schema_name, object_id, table_name, index_id, index_name, index_type_desc, reserved_mb, used_mb, total_rows,
     user_seeks, user_scans, user_lookups, user_updates, row_lock_wait_in_ms, index_lock_promotion_count)
VALUES ($1,$2,$3,'n',$4,$5,7,'dbo',100,'Big',1,'PK_Big','CLUSTERED',$6,$6,1000,0,0,0,0,$7,0)",
            Interlocked.Decrement(ref s_id), at, ServerId, at.AddDays(-10), db, reservedMb, lockWaitMs);
}
