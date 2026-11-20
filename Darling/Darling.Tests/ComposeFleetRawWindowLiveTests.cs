using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. Every test here reaches DARLING_TEST_PG only
   to CREATE and DROP its own database through ScratchPostgres, then works entirely inside it.

   #9151: procedure_stats and query_stats have no index on collection_time alone, so a fleet panel over a short window read the
   whole day's chunk and discarded most of it. The panel now carries a server list the (server_id, collection_time) index can drive.
   The answer must not change (a removed server's rows included). */
public sealed class ComposeFleetRawWindowLiveTests
{
    private const int RemovedServerId = -91510;
    private const string Panel = "{\"source\":\"procedure_stats\",\"measure\":\"proc_executions\",\"aggregate\":\"sum\",\"topN\":50,\"groupBy\":[\"object_name\"],\"viz\":\"table\","
        + "\"filters\":[{\"dimension\":\"object_name\",\"op\":\"like\",\"value\":\"%Order%\"}]}";

    private const string Seed = @"
INSERT INTO collect.procedure_stats(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
    execution_count, delta_execution_count, sample_interval_seconds)
SELECT s::bigint * 1000000000 + k * 1000 + p, t, s, 'srv' || s, 'db', 'dbo',
       CASE WHEN p % 7 = 0 THEN NULL ELSE 'usp_' || (ARRAY['Orders','Billing','Report','Sync'])[p % 4 + 1] || '_' || (p % 6) END,
       'P', k, 5 + (p % 3), 60
FROM generate_series(719, 0, -1) k, generate_series(1, 12) s, generate_series(1, 25) p,
     LATERAL (SELECT date_trunc('minute', now() AT TIME ZONE 'UTC') - k * interval '1 minute' AS t) tt
ORDER BY k DESC, s, 1;
INSERT INTO collect.procedure_stats(collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, object_type,
    execution_count, delta_execution_count, sample_interval_seconds)
SELECT -k, date_trunc('minute', now() AT TIME ZONE 'UTC') - k * interval '1 minute', -91510, 'srv-removed', 'db', 'dbo', 'usp_OrderRemoved', 'P', k, 3, 60
FROM generate_series(10, 20) k;
ANALYZE collect.procedure_stats;";

    [Fact]
    public async Task FleetPanel_ReturnsTheRowsItReturnedWithoutTheServerList()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #9151 live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        for (var id = 1; id <= 12; id++)
        {
            await DarlingMcpTestData.RegisterServerAsync(connection, id, "srv" + id, ct);
        }

        await using (var seed = new NpgsqlCommand(Seed, connection) { CommandTimeout = 300 })
        {
            await seed.ExecuteNonQueryAsync(ct);
        }

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var context = new ComposeRunContext(null, now.AddHours(-4), now, ComposeRunContext.NoVariables, RollupAvailability.None, now, RollupCoverage.Unknown, QueryStoreWideEligible: false);
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(Panel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var (compiled, compileError) = ComposeCompiler.Compile(plan!, context);
        Assert.True(compileError is null, compileError);

        /* today's text: the same statement without the server list */
        var sql = compiled!.Sql;
        var listStart = sql.IndexOf("  AND f.server_id = ANY(", StringComparison.Ordinal);
        Assert.True(listStart > 0, "an unscoped procedure_stats panel must carry the server list");
        var oldSql = sql.Remove(listStart, sql.IndexOf('\n', listStart) + 1 - listStart);
        Assert.NotEqual(sql, oldSql);

        var newRows = await RowsAsync(connection, sql, compiled, ct);
        var oldRows = await RowsAsync(connection, oldSql, compiled, ct);
        Assert.NotEmpty(newRows);
        Assert.Equal(oldRows, newRows);
        /* the removed server (no registry row) still counts, and NULL object names stay out of the LIKE */
        Assert.Contains(newRows, r => r.StartsWith("usp_OrderRemoved|", StringComparison.Ordinal));
        Assert.DoesNotContain(newRows, r => r.StartsWith("<null>|", StringComparison.Ordinal));
    }

    private static NpgsqlCommand Command(NpgsqlConnection connection, string sql, ComposeCompiled compiled)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        foreach (var p in compiled.Parameters)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = p.NpgsqlDbType, Value = p.Value });
        }

        return command;
    }

    private static async Task<List<string>> RowsAsync(NpgsqlConnection connection, string sql, ComposeCompiled compiled, CancellationToken ct)
    {
        var rows = new List<string>();
        await using var command = Command(connection, sql, compiled);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(string.Join("|", Enumerable.Range(0, reader.FieldCount)
                .Select(i => reader.IsDBNull(i) ? "<null>" : Convert.ToString(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture))));
        }

        return rows;
    }
}
