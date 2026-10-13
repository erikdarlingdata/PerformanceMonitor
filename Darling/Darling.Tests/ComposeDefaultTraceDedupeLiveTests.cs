using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The live tests here reach DARLING_TEST_PG only to
   CREATE and DROP their own database through ScratchPostgres, then work entirely inside it.

   #5738: a Custom Views measure over default_trace_events counts each stored event once, the way the readers do
   (MCP get_default_trace_events, the viewer, Lite). A COPY repeats the row and its event_sequence; a distinct event
   that differs only in event_sequence is still counted. */
public sealed class ComposeDefaultTraceDedupeLiveTests
{
    private const int ServerA = -57381;
    private const int ServerB = -57382;
    private const string NameA = "deftracededupe-a";
    private const string NameB = "deftracededupe-b";

    private static readonly DateTime WindowStart = new(2026, 8, 1, 0, 0, 0, DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = WindowStart.AddHours(6);
    private static readonly DateTime T0 = WindowStart.AddHours(1);

    private static ComposeRunContext Context(string[]? servers) =>
        new(servers, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, WindowEnd, RollupCoverage.Unknown, QueryStoreWideEligible: false);

    [Fact]
    public void PartitionKey_IsTheMcpReadersKey_PlusTheServerColumns()
    {
        /* The MCP reader keys on every DefaultTraceEventRow field plus event_sequence (ReadEventRowsAsync). The compiler's list
           must stay that set, with server_id and server_name in front, because the compiler works across servers. */
        static string Snake(string pascal) => Regex.Replace(pascal, "(?<!^)([A-Z])", "_$1").ToLowerInvariant();
        var readerKey = typeof(DarlingDefaultTraceReader.DefaultTraceEventRow)
            .GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Select(p => p.Name == nameof(DarlingDefaultTraceReader.DefaultTraceEventRow.EventTimeUtc) ? "event_time" : Snake(p.Name))
            .Append("event_sequence")
            .ToHashSet(StringComparer.Ordinal);

        var compilerKey = ComposeCompiler.s_defaultTracePartitionKey.ToList();
        Assert.Equal(new[] { "server_id", "server_name" }, compilerKey.Take(2));
        Assert.Equal(readerKey.OrderBy(x => x, StringComparer.Ordinal), compilerKey.Skip(2).OrderBy(x => x, StringComparer.Ordinal));
        Assert.Equal(compilerKey.Count, compilerKey.Distinct(StringComparer.Ordinal).Count());

        /* and the reader's SQL really selects each of those columns, so the list above is the key it dedupes on */
        foreach (var column in readerKey)
        {
            Assert.Contains("dte." + column, DarlingDefaultTraceReader.EventsByWindowSql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void DefaultTraceRelation_IsOneRowPerKey_InsideTheWindow()
    {
        var sql = CompileSql(Plan("count", null), Context(null));
        Assert.Contains(
            "(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + string.Join(", ", ComposeCompiler.s_defaultTracePartitionKey)
            + " ORDER BY collection_time DESC, default_trace_event_id DESC) AS dte_rn "
            + "FROM collect.default_trace_events WHERE collection_time >= $1 AND collection_time <= $2) AS dte_ranked WHERE dte_rn = 1)",
            sql, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CustomView_CountsAStoredCopyOnce_AndADistinctSequenceTwice_LikeTheMcpReader()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5738 default trace dedupe live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, NameB, ct);
        await SeedAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var readerRows = await DarlingDefaultTraceReader.ReadEventsAsync(postgres, ServerA, WindowStart, WindowEnd, ct);
        var expected = readerRows
            .GroupBy(r => r.EventName!, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => (Count: (long)g.Count(), Sum: g.Sum(r => r.DurationUs ?? 0)), StringComparer.Ordinal);

        /* the seed holds what it claims: 7 stored rows of server A in the window, 4 events after the readers' collapse */
        Assert.Equal(7L, await ScalarAsync(connection,
            $"SELECT count(*) FROM collect.default_trace_events WHERE server_id = {ServerA} AND collection_time >= '2026-08-01' AND collection_time <= '2026-08-01 06:00'", ct));
        Assert.Equal(1000L * 2 + 500 + 200, expected.Values.Sum(v => v.Sum));
        Assert.Equal(2L, expected["Object:Altered"].Count);

        var context = Context(new[] { NameA });
        foreach (var aggregate in new[] { "count", "sum" })
        {
            var rows = await RunAsync(connection, Plan(aggregate, null), context, ct);
            var actual = rows.ToDictionary(r => r[0], r => decimal.Parse(r[^1], CultureInfo.InvariantCulture), StringComparer.Ordinal);
            Assert.Equal(expected.Keys.OrderBy(x => x, StringComparer.Ordinal), actual.Keys.OrderBy(x => x, StringComparer.Ordinal));
            foreach (var (name, e) in expected)
            {
                Assert.Equal(aggregate == "count" ? e.Count : e.Sum, actual[name]);
            }
        }

        /* a dimension filter on a key column narrows the dedupe's survivors, not their ranking */
        var filtered = await RunAsync(connection, Plan("count", "[{\"dimension\":\"event_name\",\"op\":\"eq\",\"value\":\"Object:Altered\"}]"), context, ct);
        Assert.Equal(2m, decimal.Parse(Assert.Single(filtered)[^1], CultureInfo.InvariantCulture));

        /* the same event on another server is its own event: the fleet counts server A's 2 plus server B's 1 */
        var fleet = await RunAsync(connection, Plan("count", "[{\"dimension\":\"event_name\",\"op\":\"eq\",\"value\":\"Object:Altered\"}]"), Context(null), ct);
        Assert.Equal(3m, decimal.Parse(Assert.Single(fleet)[^1], CultureInfo.InvariantCulture));
    }

    [Fact]
    public async Task ServerAndWindowFilters_ReachTheScanInsideTheDedupeSubquery()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5738 default trace plan live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerA, NameA, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerB, NameB, ct);
        await SeedAsync(connection, ct);

        /* fleet-sized bulk so the planner prices a real scan: 40 servers x 1500 rows over 3 days */
        await DarlingMcpTestData.ExecAsync(connection, ct, @"
INSERT INTO collect.default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, event_class, spid, database_name, duration_us, event_sequence)
SELECT 1000000 + s * 10000 + g, TIMESTAMP '2026-07-30' + g * interval '2 minutes', -60000 - s, 'bulk-' || s,
       TIMESTAMP '2026-07-30' + g * interval '2 minutes', 'Object:Created', 46, 50, 'db' || (g % 5), 100 + g, g
FROM generate_series(1, 40) AS s, generate_series(1, 1500) AS g");
        await using (var analyze = new NpgsqlCommand("ANALYZE collect.default_trace_events", connection))
        {
            await analyze.ExecuteNonQueryAsync(ct);
        }

        foreach (var (name, filters) in new (string, string?)[]
        {
            ("server + window", null),
            ("server + window + event_name", "[{\"dimension\":\"event_name\",\"op\":\"eq\",\"value\":\"Object:Altered\"}]"),
        })
        {
            var plan = await ExplainAsync(connection, Plan("count", filters), Context(new[] { NameA }), ct);
            var lines = plan.Split('\n');
            var windowAgg = Array.FindIndex(lines, l => l.Contains("WindowAgg", StringComparison.Ordinal));
            Assert.True(windowAgg >= 0, $"{name}: the plan must carry the dedupe's WindowAgg:\n{plan}");

            /* the scan's own conditions (Index Cond or Filter) for the server (compiled as server_id = ANY (registry lookup)), the
               window and any dimension filter sit BELOW the WindowAgg line, inside the subquery. A copy of the window condition on
               the subquery scan above it is redundant and harmless: this pins that the scan itself is bounded. */
            var conditions = new List<string> { "server_id", "collection_time" };
            if (filters is not null)
            {
                conditions.Add("event_name");
            }

            foreach (var condition in conditions)
            {
                var hits = lines.Select((l, i) => (l, i)).Where(x => (x.l.Contains("Filter:", StringComparison.Ordinal) || x.l.Contains("Cond:", StringComparison.Ordinal))
                    && x.l.Contains(condition, StringComparison.Ordinal)).Select(x => x.i).ToList();
                Assert.True(hits.Count > 0, $"{name}: no scan condition names {condition}:\n{plan}");
                Assert.True(hits.Any(i => i > windowAgg), $"{name}: no {condition} condition reaches the scan below the WindowAgg:\n{plan}");
            }
        }
    }

    private static string CompileSql(PanelPlan plan, ComposeRunContext context)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        return compiled!.Sql;
    }

    private static PanelPlan Plan(string aggregate, string? filters)
    {
        var json = "{\"source\":\"default_trace_events\",\"measure\":\"deftrace_duration_us\",\"aggregate\":\"" + aggregate + "\"" + (aggregate == "sum" ? ",\"unit\":\"us\"" : string.Empty) + ",\"topN\":50,"
            + "\"groupBy\":[\"event_name\"],\"viz\":\"table\"" + (filters is null ? string.Empty : ",\"filters\":" + filters) + "}";
        var (plan, error) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(json)!, Array.Empty<string>());
        Assert.True(error is null, error);
        return plan!;
    }

    private static async Task<List<string[]>> RunAsync(NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        var rows = new List<string[]>();
        await using var command = new NpgsqlCommand(compiled!.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var values = new string[reader.FieldCount];
            for (var i = 0; i < reader.FieldCount; i++)
            {
                values[i] = reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), CultureInfo.InvariantCulture)!;
            }

            rows.Add(values);
        }

        return rows;
    }

    private static async Task<string> ExplainAsync(NpgsqlConnection connection, PanelPlan plan, ComposeRunContext context, CancellationToken ct)
    {
        var (compiled, error) = ComposeCompiler.Compile(plan, context);
        Assert.True(error is null, error);
        var text = new StringBuilder();
        await using var command = new NpgsqlCommand("EXPLAIN (COSTS OFF) " + compiled!.Sql, connection);
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(p);
        }

        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            text.Append(reader.GetString(0)).Append('\n');
        }

        return text.ToString();
    }

    private static async Task<long> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToInt64(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static long s_nextId = 5_000;

    private static Task PlantAsync(
        NpgsqlConnection connection, int serverId, string serverName, DateTime collectionTime, DateTime eventTime, string eventName,
        long durationUs, long? sequence, string? textData, CancellationToken ct) =>
        DarlingMcpTestData.ExecAsync(connection, ct,
            @"INSERT INTO collect.default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name, event_time, event_name, event_class, spid, database_name, duration_us, event_sequence, text_data)
VALUES ($1, $2, $3, $4, $5, $6, 164, 55, 'db1', $7, $8, $9)",
            ++s_nextId, collectionTime, serverId, serverName, eventTime, eventName, durationUs,
            (object?)sequence ?? DBNull.Value, (object?)textData ?? DBNull.Value);

    private static async Task SeedAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var e = T0.AddMinutes(10);

        /* a STORED COPY: the same event written twice (rows from before the upgrade, or re-written after a state reset) */
        await PlantAsync(connection, ServerA, NameA, T0.AddMinutes(11), e, "Object:Altered", 1000, 10, "ALTER TABLE t", ct);
        await PlantAsync(connection, ServerA, NameA, T0.AddMinutes(40), e, "Object:Altered", 1000, 10, "ALTER TABLE t", ct);
        /* a DISTINCT event that differs only in event_sequence (two identical audit events in one 3.33 ms tick) */
        await PlantAsync(connection, ServerA, NameA, T0.AddMinutes(11), e, "Object:Altered", 1000, 11, "ALTER TABLE t", ct);
        /* rows with no sequence (stored before the column): the column-only key still collapses them */
        await PlantAsync(connection, ServerA, NameA, T0.AddMinutes(12), e.AddMinutes(1), "Audit Login", 500, null, null, ct);
        await PlantAsync(connection, ServerA, NameA, T0.AddMinutes(41), e.AddMinutes(1), "Audit Login", 500, null, null, ct);
        /* an ordinary single event, and one that differs from its neighbour only in duration_us (a key column) */
        await PlantAsync(connection, ServerA, NameA, T0.AddMinutes(13), e.AddMinutes(2), "ErrorLog", 200, 12, "error one", ct);
        await PlantAsync(connection, ServerA, NameA, T0.AddMinutes(13), e.AddMinutes(2), "ErrorLog", 0, 12, "error one", ct);
        /* before the window: neither the readers nor the panel see it */
        await PlantAsync(connection, ServerA, NameA, WindowStart.AddHours(-3), WindowStart.AddHours(-3), "Object:Altered", 9999, 5, "old", ct);
        /* the same event on another server is its own event */
        await PlantAsync(connection, ServerB, NameB, T0.AddMinutes(11), e, "Object:Altered", 1000, 10, "ALTER TABLE t", ct);
    }
}
