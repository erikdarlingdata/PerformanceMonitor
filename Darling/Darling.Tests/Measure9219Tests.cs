using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* TEMPORARY measurement, deleted before the last push. */
public sealed class Measure9219Tests
{
    private const string OldLookupSql = @"
WITH seen AS MATERIALIZED (
    SELECT DISTINCT s.query_text_digest AS digest
    FROM query_stats AS s
    JOIN servers AS v ON v.server_id = s.server_id AND v.is_enabled
    WHERE s.collection_time >= $3
    AND   s.collection_time < $4
    AND   s.query_text_digest IS NOT NULL
),
matched AS MATERIALIZED (
    SELECT f.digest
    FROM seen AS f
    CROSS JOIN LATERAL (SELECT 1 AS hit
                        FROM query_text_dim AS d
                        WHERE d.digest = f.digest
                        AND   d.query_text ILIKE '%' || $1 || '%'
                        LIMIT 1) AS t
),
hits AS (
    SELECT DISTINCT ON (s.server_id, s.query_hash, s.database_name)
           s.server_id, s.query_hash, s.database_name, s.collection_time AS last_collected, s.query_text_digest
    FROM query_stats AS s
    JOIN matched AS m ON m.digest = s.query_text_digest
    JOIN servers AS v ON v.server_id = s.server_id AND v.is_enabled
    WHERE s.collection_time >= $3
    AND   s.collection_time < $4
    ORDER BY s.server_id, s.query_hash, s.database_name, s.collection_time DESC
),
page AS (
    SELECT h.server_id, h.query_hash, h.database_name, h.last_collected, h.query_text_digest
    FROM hits AS h
    WHERE NOT EXISTS (
        SELECT 1
        FROM unnest($5::integer[], $6::text[], $7::text[]) AS k(server_id, query_hash, database_name)
        WHERE k.server_id = h.server_id
        AND   k.query_hash IS NOT DISTINCT FROM h.query_hash
        AND   k.database_name IS NOT DISTINCT FROM h.database_name
    )
    ORDER BY h.last_collected DESC, h.server_id, h.query_hash, h.database_name
    LIMIT $2
)
SELECT p.server_id, p.query_hash, p.database_name, p.last_collected, d.query_text
FROM page AS p
JOIN query_text_dim AS d ON d.digest = p.query_text_digest
ORDER BY p.last_collected DESC, p.server_id, p.query_hash, p.database_name";

    [Fact]
    public async Task Measure()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "no rig");
        var ct = TestContext.Current.CancellationToken;
        var report = new StringBuilder();
        await using var scratch = await ScratchPostgres.CreateAsync(cs!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);
        for (var s = 1; s <= 42; s++)
        {
            await DarlingMcpTestData.RegisterServerAsync(c, s, "m-" + s, ct);
        }

        var sw = Stopwatch.StartNew();
        const int texts = 600000;
        const int digests = 30000;
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO collect.query_text_dim (digest, query_text, last_seen) SELECT sha256(('t' || i)::text::bytea), "
            + "'SELECT ' || i || ' FROM dbo.Common ' || repeat('x', 450) || CASE WHEN i % 100000 = 7 THEN ' dbo.Rare' ELSE '' END, timezone('UTC', now()) FROM generate_series(1, " + texts + ") AS i");
        var newest = DarlingMcpTestData.TruncateToSeconds(DateTime.UtcNow);
        await DarlingMcpTestData.ExecAsync(c, ct,
            "INSERT INTO query_stats (collection_id, collection_time, server_id, server_name, database_name, query_hash, query_text_digest) "
            + "SELECT $2::bigint + i, $1::timestamp - ((i * 3600.0 / 972000) * interval '1 second'), 1 + (i % 42), 'x', 'Books', 'h' || ((i * 7919) % " + digests + "), "
            + "sha256(('t' || (1 + ((i * 7919) % " + digests + ") * 19))::text::bytea) FROM generate_series(0::bigint, 971999) AS i", newest, CollectionIdGenerator.Next());
        await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE collect.query_text_dim");
        await DarlingMcpTestData.ExecAsync(c, ct, "ANALYZE query_stats");
        report.AppendLine($"seeded in {sw.Elapsed.TotalSeconds:0.0}s: {texts} texts, 972000 samples in the newest hour");

        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        var floor = now - TimeSpan.FromHours(1);
        var ceiling = now.AddDays(1);

        async Task<string> Run(string label, string sql, string pattern, int wanted, DateTime from, bool eight)
        {
            await using var cmd = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, c) { CommandTimeout = 300 };
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = pattern });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = wanted });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = from });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = ceiling });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = Array.Empty<int>() });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = Array.Empty<string>() });
            cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = Array.Empty<string>() });
            if (eight)
            {
                cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = wanted * DarlingMcpFindQueriesByTextTools.AllServersLookupMatchFactor });
            }

            var root = JsonDocument.Parse((string)(await cmd.ExecuteScalarAsync(ct))!).RootElement[0];
            var plan = root.GetProperty("Plan");
            var ms = root.GetProperty("Execution Time").GetDouble();
            var hit = plan.GetProperty("Shared Hit Blocks").GetInt64();
            var read = plan.TryGetProperty("Shared Read Blocks", out var r) ? r.GetInt64() : 0;
            return $"{label}: {ms:0} ms, shared hit {hit}, read {read}, text rows examined {FindQueriesByTextLookupSpanLiveTests.TextRowsRead(plan)}";
        }

        for (var pass = 1; pass <= 2; pass++)
        {
            report.AppendLine($"-- pass {pass}");
            report.AppendLine(await Run("BEFORE common, whole first hour", OldLookupSql, "dbo.Common", 4, floor, false));
            report.AppendLine(await Run("BEFORE rare, whole first hour", OldLookupSql, "dbo.Rare", 4, floor, false));

            await using var rf = new NpgsqlCommand("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + DarlingMcpFindQueriesByTextTools.RowFloorSql, c);
            rf.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
            rf.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = ceiling });
            rf.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingMcpFindQueriesByTextTools.AllServersSpanRows - 1 });
            var rfRoot = JsonDocument.Parse((string)(await rf.ExecuteScalarAsync(ct))!).RootElement[0];
            report.AppendLine($"AFTER row-floor read: {rfRoot.GetProperty("Execution Time").GetDouble():0} ms, shared hit {rfRoot.GetProperty("Plan").GetProperty("Shared Hit Blocks").GetInt64()}");

            await using var rowFloorCmd = new NpgsqlCommand(DarlingMcpFindQueriesByTextTools.RowFloorSql, c);
            rowFloorCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = floor });
            rowFloorCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = ceiling });
            rowFloorCmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = DarlingMcpFindQueriesByTextTools.AllServersSpanRows - 1 });
            var capFloor = (DateTime)(await rowFloorCmd.ExecuteScalarAsync(ct))!;
            report.AppendLine($"AFTER first span covers {(now - capFloor).TotalSeconds:0} s of the hour");
            report.AppendLine(await Run("AFTER common, first span, page of 4", DarlingMcpFindQueriesByTextTools.PlanCacheAllServersLookupSpanSql, "dbo.Common", 4, capFloor, true));
            report.AppendLine(await Run("AFTER rare, first span, page of 4", DarlingMcpFindQueriesByTextTools.PlanCacheAllServersLookupSpanSql, "dbo.Rare", 4, capFloor, true));
        }

        Assert.Fail("MEASURE9219\n" + report);
    }
}
