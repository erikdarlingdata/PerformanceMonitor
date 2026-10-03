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

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it, so it cannot race
   live collection.

   #4605: a filtered raw Query Store panel ranks only the partitions that hold a matching row. Every pin runs
   the product's compiler and compares the rows with the same panel computed the unrestricted way (today's
   dedupe text, run on the same seed). */
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
    public void PartitionColumnOnlyFilters_CompileByteIdenticalToTheUnrestrictedDedupe()
    {
        var context = new ComposeRunContext(null, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown, QueryStoreWideEligible: false);
        const string relationStart = "(SELECT * FROM (SELECT *, ROW_NUMBER";
        const string relationEnd = ") AS qs_ranked WHERE qs_rn = 1)";
        string Relation(string sql)
        {
            var a = sql.IndexOf(relationStart, StringComparison.Ordinal);
            var b = sql.IndexOf(relationEnd, a, StringComparison.Ordinal);
            return sql.Substring(a, b + relationEnd.Length - a);
        }

        var baseline = Relation(CompileSql(Plan(null), context));
        foreach (var filter in new[]
        {
            "[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"qsA\"}]",
            "[{\"dimension\":\"server\",\"op\":\"eq\",\"value\":\"qsrawfilter-1\"}]",
        })
        {
            var sql = CompileSql(Plan(filter), context);
            Assert.Equal(baseline, Relation(sql));
            Assert.DoesNotContain("IS NOT DISTINCT FROM", sql, StringComparison.Ordinal);
        }

        /* a non-partition filter alongside a partition one still restricts */
        Assert.Contains(Marker, CompileSql(Plan("[{\"dimension\":\"database_name\",\"op\":\"eq\",\"value\":\"qsA\"},{\"dimension\":\"query_hash\",\"op\":\"eq\",\"value\":\"0x1\"}]"), context), StringComparison.Ordinal);
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
            ("neq filter", "[{\"dimension\":\"module_name\",\"op\":\"neq\",\"value\":\"usp_Orders\"}]"),
            ("like filter", "[{\"dimension\":\"module_name\",\"op\":\"like\",\"value\":\"usp_Em%\"}]"),
            ("empty-string role key", "[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Empty\"}]"),
            ("out-of-window row", "[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Early\"}]"),
        };

        foreach (var (name, filters) in cases)
        {
            foreach (var groupBy in new[] { "module_name", "query_hash" })
            {
                var plan = Plan(filters, groupBy);
                Assert.Equal(!name.StartsWith("database", StringComparison.Ordinal), CompileSql(plan, context).Contains(Marker, StringComparison.Ordinal));
                var pushed = await RunAsync(connection, CompileSql(plan, context), plan, context, strip: false, ct);
                var unrestricted = await RunAsync(connection, CompileSql(plan, context), plan, context, strip: true, ct);
                Assert.True(pushed.SequenceEqual(unrestricted, StringComparer.Ordinal),
                    $"{name} (group by {groupBy}): restricted [{string.Join(";", pushed)}] != unrestricted [{string.Join(";", unrestricted)}]");
            }
        }

        /* the rename's old name yields no row: the newest snapshot of that partition carries the new name */
        var oldPlan = Plan(cases[1].Filters, "module_name");
        Assert.Empty(await RunAsync(connection, CompileSql(oldPlan, context), oldPlan, context, strip: true, ct));

        /* Over-inclusion is invisible in the panel's rows (the outer filter drops a survivor that does not match),
           so the TIGHTNESS of the restriction is pinned on the dedupe relation's own survivors: exactly the
           partitions holding an in-window, in-scope matching row. */
        var unscoped = Plan("[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Empty\"}]");
        Assert.Equal(new[] { $"{ServerId1}|40|" }.Concat(new[] { $"{ServerId2}|40|" }).OrderBy(x => x, StringComparer.Ordinal),
            await SurvivorsAsync(connection, unscoped, context, ct));

        Assert.Empty(await SurvivorsAsync(connection, Plan("[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Early\"}]"), context, ct));

        Assert.Equal(new[] { $"{ServerId1}|61|primary" },
            await SurvivorsAsync(connection, Plan("[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Dual\"}]"), context, ct));

        /* neq / like: the restriction must carry those operators too. usp_Orders is the ONLY module of queries
           10 and 12, so a neq panel must not rank them; a like panel must rank only the usp_Em* partitions. */
        var neqSurvivors = await SurvivorsAsync(connection, Plan(cases[6].Filters), context, ct);
        Assert.DoesNotContain(neqSurvivors, x => x.Contains("|10|", StringComparison.Ordinal) || x.Contains("|12|", StringComparison.Ordinal));
        Assert.Contains(neqSurvivors, x => x.Contains("|11|", StringComparison.Ordinal));
        var likeSurvivors = await SurvivorsAsync(connection, Plan(cases[7].Filters), context, ct);
        Assert.DoesNotContain(likeSurvivors, x => x.Contains("|11|", StringComparison.Ordinal));
        Assert.Contains(likeSurvivors, x => x.Contains("|40|", StringComparison.Ordinal));

        var scoped = context with { Servers = new[] { ServerName1 } };
        var scopedPlan = Plan("[{\"dimension\":\"module_name\",\"op\":\"eq\",\"value\":\"usp_Scoped\"}]");
        Assert.Empty(await SurvivorsAsync(connection, scopedPlan, scoped, ct));
        Assert.Equal(new[] { $"{ServerId2}|60|primary" }, await SurvivorsAsync(connection, scopedPlan, context, ct));
        var scopedRun = await RunAsync(connection, CompileSql(scopedPlan, scoped), scopedPlan, scoped, strip: false, ct);
        Assert.True(scopedRun.SequenceEqual(await RunAsync(connection, CompileSql(scopedPlan, scoped), scopedPlan, scoped, strip: true, ct), StringComparer.Ordinal));

        /* a measure filter is not part of the catalog's filter model (dimensions only), so none can be pushed */
    }

    /* The dedupe relation's survivors as "server_id|query_id|replica_role", parameters kept referenced. */
    private static async Task<List<string>> SurvivorsAsync(NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        var sql = compiled!.Sql;
        var start = sql.IndexOf("(SELECT * FROM (SELECT *, ROW_NUMBER", StringComparison.Ordinal);
        const string tail = ") AS qs_ranked WHERE qs_rn = 1)";
        var end = sql.IndexOf(tail, start, StringComparison.Ordinal);
        Assert.True(start >= 0 && end > start, "the compiled SQL must carry the dedupe relation");
        var relation = sql.Substring(start, end + tail.Length - start);
        var text = "SELECT r.server_id, r.query_id, r.replica_role FROM " + relation + " AS r WHERE true"
            + string.Concat(Enumerable.Range(1, compiled.Parameters.Count).Select(n => $" AND (${n} IS NULL OR true)"));
        var rows = new List<string>();
        await using var command = new NpgsqlCommand(text, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add($"{reader.GetInt32(0)}|{reader.GetInt64(1)}|{(reader.IsDBNull(2) ? "<null>" : reader.GetString(2))}");
        }

        rows.Sort(StringComparer.Ordinal);
        return rows;
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
            if (start > 0)
            {
                sql = sql.Remove(start, end - start);
            }
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
        /* one key family, role '' vs NULL: only the '' partition matches usp_Empty (a coalesce collision, not equality) */
        await WriteAsync(t0.AddMinutes(5), Row("qsA", 40, 400, t0, 2, "usp_Empty", ""));
        await WriteAsync(t0.AddMinutes(6), Row("qsA", 40, 400, t0, 3, "usp_Blank", null));
        /* a matching row BEFORE the window, in a partition whose in-window survivor has another module */
        await WriteAsync(WindowStart.AddHours(-2), Row("qsA", 50, 500, t0, 1, "usp_Early", "primary"));
        await WriteAsync(t0.AddMinutes(10), Row("qsA", 50, 500, t0, 5, "usp_Late", "primary"));
        /* the same key on both servers, the module differing: server scope and server_id must both narrow */
        await WriteAsync(t0.AddMinutes(10), Row("qsA", 60, 600, t0, 4, serverId == ServerId2 ? "usp_Scoped" : "usp_Plain", "primary"));
        await WriteAsync(t0.AddMinutes(10), Row("qsA", 61, 610, t0, 4, serverId == ServerId1 ? "usp_Dual" : "usp_Plain2", "primary"));
    }
}
