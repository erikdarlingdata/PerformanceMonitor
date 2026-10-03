using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #4605: a filtered raw Query Store panel ranks only the partitions that hold a matching row. Every pin runs
   the product's compiler and compares the rows with the same panel computed the unrestricted way (today's
   dedupe text, run on the same seed). Own scratch database, so it cannot race live collection. */
public sealed class ComposeQueryStoreRawDedupeFilterLiveTests
{
    private const int ServerId1 = -46061;
    private const int ServerId2 = -46062;
    private const string ServerName1 = "qsrawfilter-1";
    private const string ServerName2 = "qsrawfilter-2";

    private static readonly DateTime WindowStart = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(6);

    private const string Marker = " AND EXISTS (SELECT 1 FROM (SELECT DISTINCT";

    [Fact]
    public void FilteredRawPanel_CarriesThePartitionRestriction_AndAnUnfilteredPanelIsUnchanged()
    {
        var context = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown, QueryStoreWideEligible: false);

        var filtered = CompileSql(Plan("[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_A\"}]"), context);
        Assert.Contains(Marker, filtered, StringComparison.Ordinal);
        Assert.Contains("IS NOT DISTINCT FROM", filtered, StringComparison.Ordinal);
        Assert.Contains(") AS qs_ranked WHERE qs_rn = 1)", filtered, StringComparison.Ordinal);

        var unfiltered = CompileSql(Plan(null), context);
        Assert.Contains(
            "(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY server_id, server_name, database_name, "
            + "query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role "
            + "ORDER BY collection_time DESC, execution_count DESC) AS qs_rn "
            + "FROM collect.query_store_stats WHERE collection_time >= $1 AND collection_time <= $2) AS qs_ranked WHERE qs_rn = 1)",
            unfiltered, StringComparison.Ordinal);
        Assert.DoesNotContain("IS NOT DISTINCT FROM", unfiltered, StringComparison.Ordinal);
    }

    [Fact]
    public void Restriction_IsAHashableSemiJoin_NotAnInOrExists()
    {
        var context = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown, QueryStoreWideEligible: false);
        var sql = CompileSql(Plan("[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_A\"}]"), context);
        var start = sql.IndexOf(Marker, StringComparison.Ordinal);
        var end = sql.IndexOf(") AS qs_ranked", StringComparison.Ordinal);
        Assert.True(start > 0 && end > start, "the compiled SQL must carry the restriction");
        var restriction = sql.Substring(start, end - start);

        Assert.DoesNotContain(" IN (SELECT", restriction, StringComparison.Ordinal);
        Assert.DoesNotContain(" OR ", restriction, StringComparison.Ordinal);
        Assert.Contains("coalesce(k.replica_role, '') = coalesce(query_store_stats.replica_role, '')", restriction, StringComparison.Ordinal);
        Assert.Contains("k.replica_role IS NOT DISTINCT FROM query_store_stats.replica_role", restriction, StringComparison.Ordinal);
    }

    [Fact]
    public async Task FilteredRawPanel_EqualsTheUnrestrictedDedupe_AcrossRenameNullKeyAndInFilter()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #4605 raw dedupe filter live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId1, ServerName1, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId2, ServerName2, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());
        await SeedAsync(runner, ServerId1, ServerName1, ct);
        await SeedAsync(runner, ServerId2, ServerName2, ct);

        var rollups = await TimescaleSupport.DetectRollupsAsync(postgres, ct);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(postgres, rollups, ct);
        var context = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, rollups, WindowEnd, coverage, QueryStoreWideEligible: false);

        /* the seed really holds a two-name partition and a NULL-role partition */
        Assert.Equal(2L, await ScalarAsync(connection,
            "SELECT count(DISTINCT module_name) FROM collect.query_store_stats WHERE query_id = 20 AND server_id = " + ServerId1, ct));
        Assert.True(await ScalarAsync(connection,
            "SELECT count(*) FROM collect.query_store_stats WHERE query_id = 30 AND replica_role IS NULL", ct) > 0);

        var cases = new (string Name, string Filters)[]
        {
            ("multi-module, one module", "[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Orders\"}]"),
            ("rename: old name", "[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Old\"}]"),
            ("rename: new name", "[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_New\"}]"),
            ("NULL key partition", "[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_NullRole\"}]"),
            ("IN filter", "[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":[\"usp_Orders\",\"usp_Old\"]}]"),
            ("database filter", "[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"qsB\"}]"),
        };

        foreach (var (name, filters) in cases)
        {
            foreach (var groupBy in new[] { "module_name", "query_hash" })
            {
                var plan = Plan(filters, groupBy);
                var pushed = await RunAsync(connection, CompileSql(plan, context), plan, context, strip: false, ct);
                var unrestricted = await RunAsync(connection, CompileSql(plan, context), plan, context, strip: true, ct);
                Assert.True(pushed.SequenceEqual(unrestricted, StringComparer.Ordinal),
                    $"{name} (group by {groupBy}): restricted [{string.Join(";", pushed)}] != unrestricted [{string.Join(";", unrestricted)}]");
            }
        }

        /* the rename's old name yields no row: the newest snapshot of that partition carries the new name */
        var oldPlan = Plan(cases[1].Filters, "module_name");
        Assert.Empty(await RunAsync(connection, CompileSql(oldPlan, context), oldPlan, context, strip: true, ct));

        /* a measure filter is not part of the catalog's filter model (dimensions only), so none can be pushed */
    }

    private static string CompileSql(PanelPlan plan, ComposeRunContext context)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        return compiled!.Sql;
    }

    private static PanelPlan Plan(string? filters, string groupBy = "module_name")
    {
        var json = "{\"source\":\"query_store_stats\",\"measure\":\"qs_executions\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"" + groupBy + "\"],\"viz\":\"table\""
            + (filters is null ? string.Empty : ",\"filters\":" + filters) + "}";
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, Array.Empty<string>());
        Assert.True(error is null, error);
        return plan!;
    }

    /* strip=true runs today's text: the restriction removed from the same compiled SQL. */
    private static async Task<List<string>> RunAsync(
        NpgsqlConnection connection, string sql, PanelPlan plan, ComposeRunContext context, bool strip, CancellationToken ct)
    {
        if (strip)
        {
            var start = sql.IndexOf(Marker, StringComparison.Ordinal);
            var end = sql.IndexOf(") AS qs_ranked", StringComparison.Ordinal);
            Assert.True(start > 0 && end > start, "the compiled SQL must carry the restriction to strip");
            sql = sql.Remove(start, end - start);
        }

        var (compiled, _) = ComposeCompiler.Compile(plan, context);
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var p in compiled!.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture)!;
            }

            rows.Add(string.Join("|", values));
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), System.Globalization.CultureInfo.InvariantCulture);
    }

    private static async Task SeedAsync(DarlingCollectorRunner runner, int serverId, string serverName, CancellationToken ct)
    {
        var context = new CollectorContext { ServerId = serverId, ServerName = serverName, CollectionTime = DateTime.UtcNow, Deltas = new CollectorDeltaCalculator() };
        var server = new ServerRuntime
        {
            Config = new MonitoredServer { Name = serverName, Host = serverName },
            ConnectionString = "Server=" + serverName,
            Target = new CollectorTargetInfo { SqlMajorVersion = 16 },
            StorageName = serverName,
            ServerId = serverId,
            EngineEdition = 3,
        };

        async Task WriteAsync(DateTime collectionTime, QueryStoreCollector.Row row) =>
            await runner.WriteBackfillBatchAsync(QueryStoreCollector.Instance, new List<QueryStoreCollector.Row> { row }, server, collectionTime, context, ct);

        QueryStoreCollector.Row Row(string database, long queryId, long intervalId, DateTime first, long executions, string module, string? role) => new()
        {
            DatabaseName = database,
            QueryId = queryId,
            PlanId = queryId * 10,
            ExecutionTypeDesc = "Regular",
            FirstExecutionTime = first,
            LastExecutionTime = first.AddMinutes(4),
            QueryHash = "0x" + queryId.ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            QueryPlanHash = "0x" + (queryId * 10).ToString("X8", System.Globalization.CultureInfo.InvariantCulture),
            ExecutionCount = executions,
            AvgCpuTimeUs = 100,
            AvgDurationUs = 200,
            MaxDurationUs = 400,
            MaxCpuTimeUs = 200,
            ModuleName = module,
            IsForcedPlan = false,
            ForceFailureCount = 0,
            ReplicaRole = role,
            RuntimeStatsIntervalId = intervalId,
            IntervalStartTimeUtc = first,
        };

        var t0 = WindowStart.AddHours(1);
        /* several modules, open interval re-fetched (growing counts) */
        await WriteAsync(t0.AddMinutes(5), Row("qsA", 10, 100, t0, 3, "usp_Orders", "primary"));
        await WriteAsync(t0.AddMinutes(20), Row("qsA", 10, 100, t0, 9, "usp_Orders", "primary"));
        await WriteAsync(t0.AddMinutes(6), Row("qsA", 11, 100, t0, 5, "usp_Other", "primary"));
        await WriteAsync(t0.AddMinutes(7), Row("qsB", 12, 100, t0, 6, "usp_Orders", "primary"));
        /* a rename mid-window: ONE partition key, older snapshot under usp_Old, newer under usp_New */
        await WriteAsync(t0.AddMinutes(5), Row("qsA", 20, 200, t0, 4, "usp_Old", "primary"));
        await WriteAsync(t0.AddMinutes(25), Row("qsA", 20, 200, t0, 777, "usp_New", "primary"));
        /* a partition whose role key is NULL, re-fetched */
        await WriteAsync(t0.AddMinutes(5), Row("qsA", 30, 300, t0, 2, "usp_NullRole", null));
        await WriteAsync(t0.AddMinutes(25), Row("qsA", 30, 300, t0, 8, "usp_NullRole", null));
    }
}
