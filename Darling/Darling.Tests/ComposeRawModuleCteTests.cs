/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5811: the raw route's module CTE for an <c>object_name</c> panel. Before #5811 it ranked every procedure_stats row of the window
/// with <c>ROW_NUMBER() OVER (PARTITION BY server_name, sql_handle ...)</c>, a sort of the whole wide table before the fact scan. Now
/// (ruling C) each server's overlay runs from <c>LEAST(its last collection, the floor)</c>, where the floor is the map's watermark
/// less the refresh slack when that is inside the window and the window start otherwise, and <c>collect.module_map</c> covers every
/// other handle. Only a store without the map compiles the old ranking.
/// </summary>
public sealed class ComposeRawModuleCteTests
{
    private static readonly DateTime Now = new(2026, 7, 23, 12, 0, 0, DateTimeKind.Utc);

    private const string ObjectPanel =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"object_name\"],\"viz\":\"bar\"}";

    /// <summary>The ranking that #5811 took off the raw route.</summary>
    private const string WindowRanking = "ROW_NUMBER() OVER (PARTITION BY server_name, sql_handle";

    private static ComposeCompiled Compile(DateTime? through = null, bool mapMissing = false, string[]? servers = null, string[]? unregistered = null)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(ObjectPanel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var context = new ComposeRunContext(
            servers, Now.AddHours(-24), Now, ComposeRunContext.NoVariables, RollupAvailability.None, Now, RollupCoverage.Unknown,
            UnregisteredServers: unregistered, ModuleMapThrough: through, ModuleMapMissing: mapMissing);
        var (compiled, error) = ComposeCompiler.Compile(plan!, context);
        Assert.True(error is null, error);
        Assert.Equal(ComposeSourceTier.Raw, compiled!.Route.Tier);
        return compiled;
    }

    [Fact]
    public void TheRawRoute_NamesTheMap_AndRanksNoWindowOfProcedureStats()
    {
        foreach (var compiled in new[] { Compile(Now.AddMinutes(-30)), Compile(Now.AddMinutes(-30), servers: ["A"]) })
        {
            Assert.StartsWith("WITH m_last AS MATERIALIZED (\n", compiled.Sql, StringComparison.Ordinal);
            Assert.Contains("FROM collect.module_map AS mm\n", compiled.Sql, StringComparison.Ordinal);
            Assert.Contains("NOT EXISTS (SELECT 1 FROM m_last AS r WHERE r.server_name = mm.server_name AND r.sql_handle = mm.sql_handle)", compiled.Sql, StringComparison.Ordinal);
            Assert.DoesNotContain(WindowRanking, compiled.Sql, StringComparison.Ordinal);
            Assert.DoesNotContain("ROW_NUMBER()", compiled.Sql, StringComparison.Ordinal);
            Assert.Contains("LEFT JOIN m ON m.sql_handle = f.sql_handle AND m.server_name = f.server_name", compiled.Sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void AFreshWatermark_BoundsTheOverlay_AtTheWatermarkLessTheSlack_OrTheLastCollection()
    {
        var through = Now.AddMinutes(-30);
        var compiled = Compile(through);

        /* $1 start, $2 end, $3 the floor, $4 the top N. */
        Assert.Contains("          AND ps.collection_time >= LEAST(last_ps.collection_time, $3)\n", compiled.Sql, StringComparison.Ordinal);
        Assert.Equal(through - DarlingModuleMap.WatermarkSlack, (DateTime)compiled.Parameters[2].Value!);
        Assert.Equal(DateTimeKind.Unspecified, ((DateTime)compiled.Parameters[2].Value!).Kind);
        Assert.Equal(4, compiled.Parameters.Count);
        /* The last collection is found per server by one backward read of the window, and the overlay keeps the newest row per pair. */
        Assert.Contains("        ORDER BY p.collection_time DESC\n        LIMIT 1\n", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("    SELECT DISTINCT ON (rows_ps.server_name, rows_ps.sql_handle)\n", compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("    ORDER BY rows_ps.server_name, rows_ps.sql_handle, rows_ps.collection_time DESC\n", compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void NoWatermark_AStaleOne_OrOneAheadOfNow_CompilesTheWindowRanking_AndBindsNothingNew()
    {
        /* The whole window either way: the map-first shape sorted it serially through its per-server LATERAL (12 to 16 s on the plant),
           where the ranking sorts in parallel (5 to 14 s), so the ranking itself is the stale path and costs what it did before #5811. */
        var none = Compile();
        Assert.StartsWith(ComposeRawModuleCteOracleLiveTests.OldModuleCte, none.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("module_map", none.Sql, StringComparison.Ordinal);
        Assert.Equal(3, none.Parameters.Count);

        /* A watermark whose floor is at or before the window start, or one later than now, is no watermark. */
        foreach (var through in new[] { Now.AddHours(-30), Now.AddHours(-24).Add(DarlingModuleMap.WatermarkSlack), Now.AddMinutes(1) })
        {
            var compiled = Compile(through);
            Assert.Equal(none.Sql, compiled.Sql);
            Assert.Equal(none.Parameters.Count, compiled.Parameters.Count);
        }
    }

    [Fact]
    public void AStoreWithoutTheMap_CompilesTheWindowRanking_AndIgnoresTheWatermark()
    {
        foreach (var through in new DateTime?[] { null, Now.AddMinutes(-30) })
        {
            var compiled = Compile(through, mapMissing: true);
            Assert.StartsWith(ComposeRawModuleCteOracleLiveTests.OldModuleCte, compiled.Sql, StringComparison.Ordinal);
            Assert.DoesNotContain("module_map", compiled.Sql, StringComparison.Ordinal);
            Assert.DoesNotContain("m_last", compiled.Sql, StringComparison.Ordinal);
            Assert.Equal(3, compiled.Parameters.Count);
        }
    }

    /// <summary>A removed server keeps its registry row, so its history reads the overlay by id. A scoped name with NO registry row
    /// (a renamed server's old name) has no srv row; the map lacks whatever arrived after its last refresh, so that name reads its
    /// stored rows over the whole window by name (window start, never the floor), and the map skips the pairs that arm carries.</summary>
    [Fact]
    public void AScopedNameWithNoRegistryRow_ReadsItsStoredRows_FromTheWindowStart_AndOnlyThen()
    {
        var through = Now.AddMinutes(-30);
        Assert.DoesNotContain("m_unreg", Compile(through, servers: ["A", "Old"]).Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("m_unreg", Compile(through).Sql, StringComparison.Ordinal);

        var compiled = Compile(through, servers: ["A", "Old"], unregistered: ["Old"]);
        var unregisteredOrdinal = 0;
        for (var i = 0; i < compiled.Parameters.Count; i++)
        {
            if (compiled.Parameters[i].Value is string[] { Length: 1 } names && names[0] == "Old")
            {
                unregisteredOrdinal = i + 1;
            }
        }

        Assert.True(unregisteredOrdinal > 0);
        var unreg = "m_unreg AS MATERIALIZED (\n"
            + "    SELECT DISTINCT ON (pu.server_name, pu.sql_handle)\n"
            + "           pu.server_name, pu.sql_handle, pu.object_name, pu.schema_name, pu.database_name\n"
            + "    FROM collect.procedure_stats AS pu\n"
            + "    WHERE pu.server_name = ANY($" + unregisteredOrdinal.ToString(CultureInfo.InvariantCulture) + ")\n"
            + "      AND pu.collection_time >= $1\n"
            + "      AND pu.collection_time <= $2\n"
            + "      AND pu.sql_handle IS NOT NULL\n"
            + "      AND pu.sql_handle <> ''\n"
            + "      AND NOT EXISTS (SELECT 1 FROM m_last AS r WHERE r.server_name = pu.server_name AND r.sql_handle = pu.sql_handle)\n"
            + "    ORDER BY pu.server_name, pu.sql_handle, pu.collection_time DESC\n"
            + "),\n"
            + "m AS (\n"
            + "    SELECT server_name, sql_handle, object_name, schema_name, database_name FROM m_last\n"
            + "    UNION ALL\n"
            + "    SELECT server_name, sql_handle, object_name, schema_name, database_name FROM m_unreg\n"
            + "    UNION ALL\n";
        Assert.Contains(unreg, compiled.Sql, StringComparison.Ordinal);
        Assert.Contains("      AND NOT EXISTS (SELECT 1 FROM m_unreg AS u WHERE u.server_name = mm.server_name AND u.sql_handle = mm.sql_handle)\n)\n",
            compiled.Sql, StringComparison.Ordinal);
        /* The registered servers keep the bounded overlay. */
        Assert.Matches(@"AND ps\.collection_time >= LEAST\(last_ps\.collection_time, \$\d+\)\n", compiled.Sql);
        Assert.DoesNotContain("LEAST(last_ps.collection_time, $1)", compiled.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain(WindowRanking, compiled.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheRawAndHourlyRawEdgesRoutes_ShareTheOneShape()
    {
        var raw = Compile(Now.AddMinutes(-30)).Sql;
        var rawCte = raw[..(raw.IndexOf(")\nSELECT ", StringComparison.Ordinal) + 2)];
        Assert.Contains("LEAST(last_ps.collection_time, $3)", rawCte, StringComparison.Ordinal);

        /* The hybrid route binds its two edge hours as $3 and $4, so its floor is $5; nothing else in the CTE differs. */
        var hybridSql = HourlyRawEdgesModuleCompile.HourlyRawEdgesSql(Now.AddMinutes(-30));
        Assert.StartsWith(rawCte.Replace("$3)", "$5)", StringComparison.Ordinal), hybridSql, StringComparison.Ordinal);
    }
}

/// <summary>The hourly-raw-edges compile of <see cref="ComposeRawModuleCteTests"/>'s panel, built the way
/// <c>DarlingComposeTests</c> builds its edge context.</summary>
internal static class HourlyRawEdgesModuleCompile
{
    private static readonly DateTime Now = new(2026, 7, 23, 12, 0, 0, DateTimeKind.Utc);

    public static string HourlyRawEdgesSql(DateTime through)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel(
            (JsonObject)JsonNode.Parse("{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":10,\"groupBy\":[\"object_name\"],\"viz\":\"table\"}")!,
            Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        var coverage = new RollupCoverage(
            new System.Collections.Generic.Dictionary<string, DateTime>(StringComparer.Ordinal) { [TimescaleSupport.QueryStatsIntervalHourlyView] = Now.AddDays(-3) },
            new System.Collections.Generic.Dictionary<string, DateTime>(StringComparer.Ordinal),
            RollupAvailability.All,
            new System.Collections.Generic.Dictionary<string, DateTime>(StringComparer.Ordinal) { [TimescaleSupport.QueryStatsIntervalHourlyView] = Now.AddMinutes(-20) });
        var start = Now.AddHours(-24);
        var candidate = ComposeSourceRouter.HourlyRawEdgesCandidate(plan!, Now, start, Now, RollupAvailability.All, coverage);
        Assert.NotNull(candidate);
        var context = new ComposeRunContext(
            null, start, Now, ComposeRunContext.NoVariables, RollupAvailability.All, Now, coverage,
            HourlyEdges: new ComposeHourlyEdgesVerdict(candidate!.SourceTable, candidate.HourStartUtc, candidate.HourEndUtc, null),
            ModuleMapThrough: through);
        var (compiled, error) = ComposeCompiler.Compile(plan!, context);
        Assert.True(error is null, error);
        Assert.Equal(ComposeSourceTier.HourlyRawEdges, compiled!.Route.Tier);
        return compiled.Sql;
    }
}

/// <summary>
/// #5811: the raw route's module CTE against the old window-wide ranking, kept here as the oracle, on one store: three servers that
/// reuse one set of handles for different modules, renames inside the window, handles seen only early in the window, and ad hoc
/// statements. The panel runs through the runner, which reads the map's watermark itself, and the oracle runs the same statement
/// with the old CTE in front. The rows must be the same with a stale map (the runner compiles that ranking itself) and with a fresh one (the
/// overlay is the last collection, and the map names every older handle). A fourth server was renamed, so a scope over its old name
/// has no registry row; that scope must match the oracle with both maps too.
/// </summary>
/* #1776 own-store: deliberately NOT [Collection("live-postgres")]. The test reaches DARLING_TEST_PG only to CREATE and
   DROP its own database through ScratchPostgres and works entirely inside it. */
public sealed class ComposeRawModuleCteOracleLiveTests
{
    /// <summary>The fleet module CTE every raw route compiled before #5811 (and a store without the map still compiles).</summary>
    internal const string OldModuleCte =
        "WITH m AS (\n"
        + "    SELECT server_name, sql_handle, object_name, schema_name, database_name\n"
        + "    FROM (\n"
        + "        SELECT server_name, sql_handle, object_name, schema_name, database_name,\n"
        + "               ROW_NUMBER() OVER (PARTITION BY server_name, sql_handle ORDER BY collection_time DESC) AS rn\n"
        + "        FROM collect.procedure_stats\n"
        + "        WHERE collection_time >= $1\n"
        + "          AND collection_time <= $2\n"
        + "          AND sql_handle IS NOT NULL\n"
        + "          AND sql_handle <> ''\n"
        + "    ) ranked_modules\n"
        + "    WHERE rn = 1\n"
        + ")\n";

    private const string Panel =
        "{\"source\":\"query_stats\",\"measure\":\"query_worker_us\",\"aggregate\":\"sum\",\"topN\":200,\"groupBy\":[\"database_name\",\"object_name\"],\"viz\":\"table\"}";

    /// <summary>The renamed server's old name, which only its stored rows carry.</summary>
    private const string UnregisteredName = "Oracle3";

    /// <summary>A scope over the renamed server's old name and one registered server.</summary>
    private static readonly string[] ScopedNames = [UnregisteredName, "Oracle1"];

    private static readonly DateTime Anchor = new(2026, 1, 5, 12, 37, 21, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = DateTime.SpecifyKind(Anchor.AddHours(-24), DateTimeKind.Unspecified);
    private static readonly DateTime WindowEnd = DateTime.SpecifyKind(Anchor, DateTimeKind.Unspecified);

    [Fact]
    public async Task TheRunnersRows_MatchTheWindowRanking_WithAStaleMapAndAFreshOne()
    {
        var baseCs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseCs), "Set DARLING_TEST_PG to a Postgres connection string to run the #5811 module CTE oracle live test.");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseCs!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        Assert.True(await DarlingModuleMap.EnsureTableAsync(connection, null, ct));
        await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            for (var i = 0; i < 3; i++)
            {
                await DarlingMcpTestData.RegisterServerAsync(connection, -5811100 - i, "Oracle" + i.ToString(CultureInfo.InvariantCulture), ct);
            }

            /* The fourth server was renamed: the registry holds its current name, and its rows keep the old one, Oracle3, which
               therefore has no registry row. */
            await DarlingMcpTestData.RegisterServerAsync(connection, -5811103, "Oracle3Renamed", ct);

            /* The stale map: its only refresh read one row from before the window. */
            await ExecAsync(connection, @"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (58110000, $1, -5811100, 'Oracle0', 'Db0', 'dbo', 'usp_Before', '0xOLD', 1, 1, 1, 300)", ct, WindowStart.AddHours(-6));
            Assert.Equal(1, await DarlingModuleMap.RefreshRecentAsync(connection, null, Anchor, ct));
            Assert.Equal(WindowStart.AddHours(-6), await DarlingModuleMap.ReadWatermarkAsync(connection, ct));

            for (var i = 0; i < 4; i++)
            {
                await SeedServerAsync(connection, i, ct);
            }

            var stale = await RunAsync(postgres, ct);
            Assert.StartsWith(OldModuleCte, (string)stale["sql"]!, StringComparison.Ordinal);
            var oracle = await OracleRowsAsync(postgres, ct);
            Assert.Equal(oracle, stale["rows"]!.ToJsonString());

            /* A scope over the old name, which has no registry row. With the stale map the ranking matches it by its stored name. */
            var staleScoped = await RunAsync(postgres, ct, ScopedNames);
            Assert.StartsWith("WITH m AS (", (string)staleScoped["sql"]!, StringComparison.Ordinal);
            var scopedOracle = await OracleRowsAsync(postgres, ct, ScopedNames);
            Assert.Equal(scopedOracle, staleScoped["rows"]!.ToJsonString());

            /* The fresh map: refreshed through the last collection, so the overlay is that collection and the map names the rest. */
            Assert.True(await DarlingModuleMap.RefreshRecentAsync(connection, null, Anchor, ct) > 0);
            Assert.Equal(WindowStart.AddHours(23), await DarlingModuleMap.ReadWatermarkAsync(connection, ct));
            var fresh = await RunAsync(postgres, ct);
            Assert.Contains("LEAST(last_ps.collection_time, $3)", (string)fresh["sql"]!, StringComparison.Ordinal);
            Assert.Equal(oracle, fresh["rows"]!.ToJsonString());
            var freshScoped = await RunAsync(postgres, ct, ScopedNames);
            Assert.Contains("m_unreg AS MATERIALIZED (", (string)freshScoped["sql"]!, StringComparison.Ordinal);
            Assert.Equal(scopedOracle, freshScoped["rows"]!.ToJsonString());

            /* With the fresh map the old name reads its stored rows over the whole window (m_unreg): a handle that arrived after the
               map's last refresh is in no map row and no registered server's overlay, so the map alone would read it '(ad hoc)'. */
            await ExecAsync(connection, @"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (58190000, $1, -5811103, 'Oracle3', 'Db3', 'dbo', 'usp_Late', '0xLATE', 1, 1, 1, 300)", ct, WindowStart.AddHours(23.5));
            await ExecAsync(connection, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
VALUES (58190001, $1, -5811103, 'Oracle3', 'Db3', 'QHLATE', '0xLATE', 987654, 987654, 1, 300)", ct, WindowStart.AddHours(23.5));
            var lateOracle = await OracleRowsAsync(postgres, ct, ScopedNames);
            Assert.Contains(JsonNode.Parse(lateOracle)!.AsArray(), r => (string?)r!["object_name"] == "usp_Late");
            Assert.Equal(lateOracle, (await RunAsync(postgres, ct, ScopedNames))["rows"]!.ToJsonString());

            var scopedRows = JsonNode.Parse(scopedOracle)!.AsArray();
            Assert.Equal(2 * 21, scopedRows.Count);
            Assert.Contains(scopedRows, r => (string?)r!["database_name"] == "Db3" && (string?)r["object_name"] == "usp_Oracle3_5_v2");
            Assert.Contains(scopedRows, r => (string?)r!["database_name"] == "Db3" && (string?)r["object_name"] == "usp_Oracle3_17");

            /* The rows are what the seed says: a rename reads its newest name, an early-only handle its own, and a handle reused on
               another server that server's module. */
            var rows = JsonNode.Parse(oracle)!.AsArray();
            Assert.Equal(4 * 21, rows.Count);
            Assert.Contains(rows, r => (string?)r!["database_name"] == "Db1" && (string?)r["object_name"] == "usp_Oracle1_5_v2");
            Assert.Contains(rows, r => (string?)r!["database_name"] == "Db2" && (string?)r["object_name"] == "usp_Oracle2_17");
            Assert.Contains(rows, r => (string?)r!["database_name"] == "Db0" && (string?)r["object_name"] == "(ad hoc)");
            Assert.DoesNotContain(rows, r => (string?)r!["object_name"] == "usp_Oracle1_5");

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => { });
        }
    }

    /// <summary>One server's plant: 20 procedures under handles every server reuses (0xH0..0xH19) in 12 collections two hours apart,
    /// every fifth renamed from the seventh collection on, and procedures 15 to 19 only in the first three collections; then two
    /// query_stats rows per handle and an ad hoc handle, each value distinct so the ranking has no ties.</summary>
    private static async Task SeedServerAsync(NpgsqlConnection connection, int i, CancellationToken ct)
    {
        var id = -5811100 - i;
        var name = "Oracle" + i.ToString(CultureInfo.InvariantCulture);
        var db = "Db" + i.ToString(CultureInfo.InvariantCulture);
        await ExecAsync(connection, @"
INSERT INTO collect.procedure_stats
    (collection_id, collection_time, server_id, server_name, database_name, schema_name, object_name, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT $2 + k * 100 + p, $1 + interval '1 hour' + k * interval '2 hours', $3, $4, $5, 'dbo',
       'usp_' || $4 || '_' || p || CASE WHEN p % 5 = 0 AND k >= 6 THEN '_v2' ELSE '' END, '0xH' || p, 1, 1, 1, 300
FROM generate_series(0, 11) AS k
CROSS JOIN generate_series(0, 19) AS p
WHERE p < 15 OR k <= 2", ct, WindowStart, 58120000L + (i * 10000L), id, name, db);
        await ExecAsync(connection, @"
INSERT INTO collect.query_stats
    (collection_id, collection_time, server_id, server_name, database_name, query_hash, sql_handle,
     delta_worker_time, delta_elapsed_time, delta_execution_count, sample_interval_seconds)
SELECT $2 + t * 100 + p, $1 + CASE WHEN t = 0 THEN interval '1 hour 5 minutes' ELSE interval '20 hours 5 minutes' END, $3, $4, $5,
       'QH' || p, CASE WHEN p = 20 THEN '0xADHOC' ELSE '0xH' || p END,
       t * 1000 + 100 + p + $6, t * 1000 + 100 + p + $6, 1, 300
FROM generate_series(0, 1) AS t
CROSS JOIN generate_series(0, 20) AS p", ct, WindowStart, 58160000L + (i * 10000L), id, name, db, (long)i * 100_000L);
    }

    private static async Task<JsonObject> RunAsync(NpgsqlDataSource postgres, CancellationToken ct, string[]? scope = null)
    {
        var body = new JsonObject
        {
            ["panel"] = JsonNode.Parse(Panel),
            ["windowStart"] = WindowStart.ToString("o", CultureInfo.InvariantCulture),
            ["windowEnd"] = WindowEnd.ToString("o", CultureInfo.InvariantCulture),
        };
        if (scope is not null)
        {
            var names = new JsonArray();
            foreach (var name in scope)
            {
                names.Add(name);
            }

            body["server"] = names;
        }
        var outcome = await DarlingWebEndpoints.RunComposedPanelAsync(postgres, body, ct, nowUtc: Anchor);
        Assert.True(outcome.Error is null, $"compose run failed: {outcome.Error}");
        var sql = (string)outcome.Payload!["sql"]!;
        return outcome.Payload;
    }

    /// <summary>The same panel with the pre-#5811 module CTE in front: the statement compiled for no watermark, its module CTE
    /// replaced by <see cref="OldModuleCte"/>, which binds the same window parameters ($1, $2).</summary>
    private static async Task<string> OracleRowsAsync(NpgsqlDataSource postgres, CancellationToken ct, string[]? scope = null)
    {
        var (plan, parseError) = ComposeSpec.TryParsePanel((JsonObject)JsonNode.Parse(Panel)!, Array.Empty<string>());
        Assert.True(parseError is null, parseError);
        /* The old CTE ranked every server's rows; the statement after it applies the scope, with the old name matched by its stored
           server_name exactly as the runner finds it. */
        var context = new ComposeRunContext(scope, WindowStart, WindowEnd, ComposeRunContext.NoVariables, RollupAvailability.None, Anchor, RollupCoverage.Unknown,
            UnregisteredServers: scope is null ? null : [UnregisteredName]);
        var (compiled, error) = ComposeCompiler.Compile(plan!, context);
        Assert.True(error is null, error);
        var cut = compiled!.Sql.IndexOf(")\nSELECT ", StringComparison.Ordinal);
        Assert.True(cut > 0);
        var sql = OldModuleCte + compiled.Sql[(cut + 2)..];

        await using var connection = await postgres.OpenConnectionAsync(ct);
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var parameter in compiled.Parameters)
        {
            command.Parameters.Add(parameter.Clone());
        }

        var rows = new JsonArray();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var row = new JsonObject();
            for (var c = 0; c < reader.FieldCount; c++)
            {
                row[reader.GetName(c)] = reader.IsDBNull(c) ? null : DarlingWebEndpoints.DbValueToJson(reader.GetValue(c));
            }

            rows.Add(row);
        }

        return rows.ToJsonString();
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var value in values)
        {
            command.Parameters.AddWithValue(value is DateTime at ? DateTime.SpecifyKind(at, DateTimeKind.Unspecified) : value);
        }

        await command.ExecuteNonQueryAsync(ct);
    }
}
