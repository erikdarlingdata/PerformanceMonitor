/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3542 correction #1 to the design doc: the analysis service picks its engine set PER CALL from the
/// registry's <c>engine_kind</c>, not per constructor. Two of the service's three construction sites are
/// process-wide singletons shared by every server (the MCP host, the web endpoints) and the MCP tools resolve
/// a server by NAME and never see an engine, so the constructor could not have been told. These pins hold the
/// mapping (every token in <see cref="MonitoredEngineKind"/>, NULL, unknown), the three entry points, the two
/// span-gate SQLs, and the honesty rule for an unstamped row.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingAnalysisServiceEngineRoutingTests
{
    /// <summary>Construction never connects, so a dead store is enough to hold the two sets.</summary>
    private static NpgsqlDataSource DeadStore() => NpgsqlDataSource.Create(
        "Host=127.0.0.1;Port=1;Username=nobody;Password=nobody;Database=nowhere;Timeout=1;Command Timeout=1");

    [Theory]
    [InlineData(MonitoredEngineKind.SqlServer, false)]
    [InlineData(MonitoredEngineKind.Postgres, true)]
    [InlineData(MonitoredEngineKind.AuroraPostgres, true)]
    [InlineData("POSTGRES", true)]
    [InlineData(null, false)]
    [InlineData("", false)]
    [InlineData("oracle", false)]
    public async Task EngineFor_MapsTheRegistryToken_PostgresTokensToThePgSet_EverythingElseToSqlServer(string? token, bool expectPg)
    {
        await using var store = DeadStore();
        var service = new DarlingAnalysisService(store);

        var set = service.EngineFor(token);

        if (expectPg)
        {
            Assert.IsType<PgTargetFactCollector>(set.Collector);
            Assert.IsType<PgTargetAnomalyDetector>(set.Detector);
            Assert.IsType<PgTargetDrillDownCollector>(set.DrillDown);
            Assert.IsType<PgTargetBaselineProvider>(set.Baselines);
            Assert.Equal(DarlingAnalysisService.PgTargetDataSpanSql, set.DataSpanSql);
        }
        else
        {
            Assert.IsType<PgFactCollector>(set.Collector);
            Assert.IsType<PgAnomalyDetector>(set.Detector);
            Assert.IsType<PgDrillDownCollector>(set.DrillDown);
            Assert.IsType<PgBaselineProvider>(set.Baselines);
            Assert.Equal(DarlingAnalysisService.TotalDataSpanSql, set.DataSpanSql);
        }

        /* The mapping IS MonitoredEngineKind.IsPostgres — one decoder, never a string comparison here. */
        Assert.Equal(MonitoredEngineKind.IsPostgres(token), expectPg);
    }

    [Fact]
    public async Task TheTwoSets_AreHeldForTheLifeOfTheService_AndAreDistinctInEveryComponent()
    {
        await using var store = DeadStore();
        var service = new DarlingAnalysisService(store);

        /* Every token in the vocabulary lands on one of exactly two instances — no per-call construction. */
        var sets = MonitoredEngineKind.All.Select(k => (string?)k).Append(null).Select(service.EngineFor).Distinct().ToList();
        Assert.Equal(2, sets.Count);
        Assert.Same(service.EngineFor(MonitoredEngineKind.Postgres), service.EngineFor(MonitoredEngineKind.AuroraPostgres));
        Assert.Same(service.EngineFor(MonitoredEngineKind.SqlServer), service.EngineFor(null));

        var sqlServer = service.EngineFor(MonitoredEngineKind.SqlServer);
        var pg = service.EngineFor(MonitoredEngineKind.Postgres);
        Assert.NotSame(sqlServer.Collector, pg.Collector);
        Assert.NotSame(sqlServer.Detector, pg.Detector);
        Assert.NotSame(sqlServer.Engine, pg.Engine);
        Assert.NotSame(sqlServer.DrillDown, pg.DrillDown);
        Assert.NotSame(sqlServer.Baselines, pg.Baselines);
        Assert.NotEqual(sqlServer.DataSpanSql, pg.DataSpanSql);
    }

    [Fact]
    public void TheThreeEntryPoints_EachResolveTheEngine_BeforeTheirFirstCollectorRead()
    {
        var service = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Analysis", "DarlingAnalysisService.cs");

        foreach (var entryPoint in new[]
        {
            "public async Task<List<AnalysisFinding>> AnalyzeAsync(AnalysisContext context)",
            "CollectAndScoreFactsAsync(",
            "ComparePeriodsAsync(",
        })
        {
            var at = service.IndexOf(entryPoint, StringComparison.Ordinal);
            Assert.True(at > 0, $"{entryPoint} has moved");
            var resolve = service.IndexOf("await ResolveEngineAsync(", at, StringComparison.Ordinal);
            var collect = service.IndexOf(".Collector.CollectFactsAsync(", at, StringComparison.Ordinal);
            Assert.True(resolve > at && collect > resolve, $"{entryPoint} must resolve the engine before it collects");
        }

        /* Exactly three, one per entry point; the pass resolves ahead of its span gate, because the gate is
           engine-specific. */
        Assert.Equal(3, Regex.Matches(service, @"await ResolveEngineAsync\(").Count);
        var pass = service.IndexOf("public async Task<List<AnalysisFinding>> AnalyzeAsync(AnalysisContext context)", StringComparison.Ordinal);
        var resolveInPass = service.IndexOf("await ResolveEngineAsync(", pass, StringComparison.Ordinal);
        var gate = service.IndexOf("await GetTotalDataSpanHoursAsync(engine,", pass, StringComparison.Ordinal);
        Assert.True(resolveInPass < gate);

        /* Nothing caches the answer: no field of the set type other than the two the constructor builds. */
        var fields = Regex.Matches(service, @"private readonly AnalysisEngineSet (\w+);").Select(m => m.Groups[1].Value).ToArray();
        Assert.Equal(new[] { "_sqlServerEngine", "_pgTargetEngine" }, fields);
        Assert.DoesNotContain("private AnalysisEngineSet", service, StringComparison.Ordinal);

        /* No catch in the resolver: a registry that cannot answer fails the pass through the pass's own
           classifier rather than defaulting a PostgreSQL target onto the SQL Server tables. */
        var resolver = service[service.IndexOf("ResolveEngineAsync(int serverId, CancellationToken cancellationToken)", StringComparison.Ordinal)..];
        resolver = resolver[..resolver.IndexOf("internal AnalysisEngineSet EngineFor(", StringComparison.Ordinal)];
        Assert.DoesNotContain("catch", CSharpSourceWalker.StripCommentsAndStrings(resolver), StringComparison.Ordinal);
        Assert.Contains("CommandTimeout = AnalysisCommandTimeoutSeconds", resolver, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePgSpanGate_ReadsPgDatabaseStats_WithTheSqlServerGatesArithmetic()
    {
        var sql = DarlingAnalysisService.PgTargetDataSpanSql;
        Assert.Contains("FROM pg_database_stats", sql, StringComparison.Ordinal);
        Assert.Contains("EXTRACT(EPOCH FROM (MAX(collection_time) - MIN(collection_time))) / 3600.0", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", sql, StringComparison.Ordinal);
        Assert.Equal(
            DarlingAnalysisService.TotalDataSpanSql.Replace("FROM wait_stats", "FROM pg_database_stats", StringComparison.Ordinal).Replace("\r\n", "\n", StringComparison.Ordinal),
            sql.Replace("\r\n", "\n", StringComparison.Ordinal));

        /* The SQL Server gate is untouched. */
        Assert.Contains("FROM wait_stats", DarlingAnalysisService.TotalDataSpanSql, StringComparison.Ordinal);

        /* The three PostgreSQL reads that decide "does this server have history" agree on the series. */
        Assert.Contains("FROM pg_database_stats", PgTargetFactCollector.PgTargetCoverageSql, StringComparison.Ordinal);
        Assert.Contains("FROM pg_database_stats", PgTargetAnomalyDetector.HasBaselineDataSql, StringComparison.Ordinal);

        /* And the registry read is the capability helper's column. */
        Assert.Contains("SELECT engine_kind", DarlingAnalysisService.ServerEngineKindSql, StringComparison.Ordinal);
        Assert.Contains("FROM servers", DarlingAnalysisService.ServerEngineKindSql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", DarlingAnalysisService.ServerEngineKindSql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The honesty rule for an unstamped row (#2530: a NULL makes no claim). It takes the SQL Server set —
    /// today's behaviour exactly — and when that lands it on the span gate, the message names the missing
    /// engine stamp rather than letting "0 hours" read as "still collecting" for a PostgreSQL target whose
    /// connect has not yet recorded its kind. A stamped row's message is byte-identical to before.
    /// </summary>
    [Fact]
    public async Task ANullKindRow_TakesTheSqlServerSet_AndItsGateMessageNamesTheMissingStamp()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to a Postgres connection string to run the engine-routing e2e.");

        const string serverName = "darling-pg-target-unstamped-row";
        var serverId = ServerIdHelper.GetDeterministicHashCode(serverName);
        var ct = TestContext.Current.CancellationToken;

        using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteRowsAsync(connection, serverId, ct);

        await using var postgres = NpgsqlDataSource.Create(cs!);
        var bodySucceeded = false;
        try
        {
            /* A PostgreSQL target by its data (a day of pg_database_stats, no wait_stats) whose registry row
               carries no kind — the pre-stamp window between registration and first connect. */
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, engineKind: null, postgresMajor: null, ct);
            var end = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, serverId, serverName, end.AddHours(-25), ct);
            await PgTargetFactCollectorTests.PlantDatabaseStatsAsync(connection, serverId, serverName, end.AddMinutes(-1), ct);

            var service = new DarlingAnalysisService(postgres);
            var (engine, kind) = await service.ResolveEngineAsync(serverId, ct);
            Assert.Null(kind);
            Assert.IsType<PgFactCollector>(engine.Collector);

            var findings = await service.AnalyzeAsync(serverId, serverName, 4, cancellationToken: ct);
            Assert.Empty(findings);
            Assert.NotNull(service.InsufficientDataMessage);
            Assert.StartsWith("Not enough data for reliable analysis. Need 1.0 days of collected data, have 0.0 hours.", service.InsufficientDataMessage, StringComparison.Ordinal);
            Assert.Contains("no engine stamp yet", service.InsufficientDataMessage, StringComparison.Ordinal);
            Assert.Contains("pg_database_stats", service.InsufficientDataMessage, StringComparison.Ordinal);

            /* Stamp it, as a connect would, and the same pass measures the right series and says nothing
               about stamps. */
            await PgTargetFactCollectorTests.RegisterServerAsync(connection, serverId, serverName, MonitoredEngineKind.Postgres, postgresMajor: 18, ct);
            var stamped = new DarlingAnalysisService(postgres);
            await stamped.AnalyzeAsync(serverId, serverName, 4, cancellationToken: ct);
            /* Two rows 25 hours apart clear the span gate; the WINDOW holds one row and no interval, so the
               honest answer is the unobserved-window envelope — not insufficient data, and not the stamp caveat. */
            Assert.Null(stamped.InsufficientDataMessage);
            Assert.NotNull(stamped.WindowEmptyMessage);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(cs!, bodySucceeded, (cleanup, cleanupCt) => DeleteRowsAsync(cleanup, serverId, cleanupCt));
        }
    }

    private static async Task DeleteRowsAsync(NpgsqlConnection connection, int serverId, System.Threading.CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM pg_database_stats WHERE server_id = {serverId}; " +
            $"DELETE FROM analysis_findings WHERE server_id = {serverId}; " +
            $"DELETE FROM servers WHERE server_id = {serverId};", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }
}
