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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Lite v64 / Darling V137 (#3796), the Lite half of (a): <c>query_store_health</c> gains the two Query Store
/// CAPTURE modes the shared collector now selects — <c>query_capture_mode</c> (2016+, always) and
/// <c>wait_stats_capture_mode</c> (2017+, NULL by construction on a 2016 engine). The health row stored every
/// option that says whether Query Store works and none that says what it captures, and <c>ALL</c> on an ad-hoc
/// workload is the setting that turns Query Store into a plan-churn factory. Both columns are VARCHAR, nullable,
/// trailing; NULL on every pre-v64 row means "never asked".
///
/// <para>The collector's gate and reader (ten ordinals on 2016, eleven from 2017, both shapes from one body) are
/// <see cref="QueryStoreHealthCollectorDefinitionTests"/>; the Darling side is
/// <c>Darling.Tests/QsCaptureModeRouteKnobToastRungTests</c>. What is here is the DuckDB ladder, the generator, and
/// the climb on a real DuckDB file — a v63 database with its <c>v_</c> view present over the narrower table, the
/// shape every existing Lite database is in the morning of the upgrade.</para>
/// </summary>
public sealed class QsCaptureModeRungTests : IDisposable
{
    private readonly string _tempDir;

    public QsCaptureModeRungTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "QsCaptureModeRungTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
    }

    public void Dispose()
    {
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    /// <summary>The v64 block: both ALTERs, the log line, and the argument in the words the next reader will
    /// look for — including the one fact this side must carry about the gate it does not implement.</summary>
    [Fact]
    public void TheLiteMigration_StoresBothCaptureModes_AtSchemaVersion64()
    {
        Assert.True(DuckDbInitializer.CurrentSchemaVersion >= 64);

        var source = Lite.Tests.ParitySource.ReadFile("Lite/Database/DuckDbInitializer.cs");
        var start = source.IndexOf("if (fromVersion < 64)", StringComparison.Ordinal);
        Assert.True(start >= 0, "DuckDbInitializer has no v64 block");
        var block = source[start..];

        Assert.Contains("(\"query_store_health\", \"query_capture_mode\", \"VARCHAR\")", block, StringComparison.Ordinal);
        Assert.Contains("(\"query_store_health\", \"wait_stats_capture_mode\", \"VARCHAR\")", block, StringComparison.Ordinal);
        Assert.Contains("ADD COLUMN IF NOT EXISTS {column} {type}", block, StringComparison.Ordinal);
        Assert.Contains("Running migration to v64", block, StringComparison.Ordinal);
        foreach (var phrase in new[]
        {
            "#3796", "twinning Darling's V137", "plan-churn factory", "755 k", "*_desc", "ALL / AUTO / CUSTOM / NONE",
            "SQL Server 2017 (v14)", "fails to compile for the whole database", "HasWaitStatsCaptureMode", "never as OFF",
            "Appended at the end of the PayloadColumns list", "Nothing to backfill", "REQUIRED on this side",
            "Nothing on this side reads either column yet", "#3797",
        })
        {
            Assert.Contains(phrase, block, StringComparison.Ordinal);
        }
    }

    /// <summary>The DuckDB generator carries both columns into a fresh store's DDL as the trailing nullable
    /// columns — the same shape the v64 ALTER gives an upgraded store, so fresh and upgraded databases agree —
    /// and the collector declares them last, in this order, which is what makes the appender's positional write
    /// land in them.</summary>
    [Fact]
    public void TheDuckDbGenerator_EmitsBothColumnsAsTheTrailingNullableColumns()
    {
        var definition = CollectorCatalog.Find("query_store_health");
        Assert.NotNull(definition);
        Assert.Equal("query_store_health", definition!.TargetTable);
        Assert.Equal(new[] { "query_capture_mode", "wait_stats_capture_mode" }, definition.PayloadColumns.TakeLast(2).Select(c => c.Name).ToArray());
        Assert.Contains(definition, DuckDbSchemaGenerator.StoredCollectors);

        var ddl = DuckDbSchemaGenerator.CreateTable(definition);
        var lines = ddl.Split('\n').Select(l => l.Trim().TrimEnd(',')).Where(l => l.Length > 0).ToList();
        var closing = lines.FindLastIndex(l => l.StartsWith(")", StringComparison.Ordinal));
        Assert.True(closing > 0, ddl);
        Assert.Equal("wait_stats_capture_mode VARCHAR", lines[closing - 1]);
        Assert.Equal("query_capture_mode VARCHAR", lines[closing - 2]);
        Assert.Equal("interval_length_minutes BIGINT", lines[closing - 3]);

        /* And the frozen golden's entry ends the same way — the tail-append that is its one legal edit. */
        var golden = GoldenCollectorSchema.Tables["query_store_health"].Replace("\r\n", "\n", StringComparison.Ordinal);
        Assert.EndsWith("    interval_length_minutes BIGINT,\n    query_capture_mode VARCHAR,\n    wait_stats_capture_mode VARCHAR\n)", golden, StringComparison.Ordinal);
    }

    /// <summary>
    /// The upgrade itself, on a real DuckDB file: a database initialized at the current schema, both columns
    /// dropped and its version stamped back to 63 — the shape every existing Lite database has the morning of
    /// the upgrade, its <c>v_</c> view present over the narrower table — re-initialized. Both columns come back
    /// nullable at the tail, the pre-rung row reads NULL through the view, a post-rung row lands with both modes
    /// and a 2016-shaped row with the gated one NULL, and the version reads 64.
    /// </summary>
    [Fact]
    public async Task AV63Database_ClimbsToV64_AndItsOldRowsReadNull()
    {
        var dbPath = Path.Combine(_tempDir, "lite-v63.duckdb");

        var initializer = new DuckDbInitializer(dbPath);
        await initializer.InitializeAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();

            /* DuckDB refuses to DROP a column a view or index depends on, so the dependents go first for the
               fixture's sake; the v63 passthrough is then put BACK over the narrower table, because that is the
               state an existing database is in when the ALTER runs — the ADD COLUMN must succeed with the view
               present, and Lite's CreateArchiveViewsAsync re-expands it afterwards. */
            var dependents = new List<string>();
            using (var views = conn.CreateCommand())
            {
                views.CommandText = "SELECT view_name FROM duckdb_views() WHERE NOT internal AND sql ILIKE '%query_store_health%'";
                using var reader = await views.ExecuteReaderAsync();
                while (await reader.ReadAsync()) dependents.Add(reader.GetString(0));
            }

            Assert.Contains("v_query_store_health", dependents);
            foreach (var dependent in dependents) await ExecAsync(conn, $"DROP VIEW IF EXISTS {dependent}");

            var indexes = new List<string>();
            using (var idx = conn.CreateCommand())
            {
                idx.CommandText = "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'query_store_health'";
                using var reader = await idx.ExecuteReaderAsync();
                while (await reader.ReadAsync()) indexes.Add(reader.GetString(0));
            }

            foreach (var index in indexes) await ExecAsync(conn, $"DROP INDEX IF EXISTS {index}");
            await ExecAsync(conn, "ALTER TABLE query_store_health DROP COLUMN wait_stats_capture_mode");
            await ExecAsync(conn, "ALTER TABLE query_store_health DROP COLUMN query_capture_mode");
            await ExecAsync(conn, "CREATE VIEW v_query_store_health AS SELECT * FROM query_store_health");

            await ExecAsync(conn, "INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes) VALUES (-1, TIMESTAMP '2026-09-20 12:00:00', 1, 'pre-v64', 'appdb', 'READ_WRITE', 'READ_WRITE', 0, 512, 1000, 'AUTO', 30, 200, 60)");
            await ExecAsync(conn, "DELETE FROM schema_version");
            await ExecAsync(conn, "INSERT INTO schema_version (version) VALUES (63)");
        }

        var upgraded = new DuckDbInitializer(dbPath);
        await upgraded.InitializeAsync();
        await upgraded.CreateArchiveViewsAsync();

        using (var conn = new DuckDBConnection($"Data Source={dbPath}"))
        {
            await conn.OpenAsync();
            Assert.Equal(64L, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));
            Assert.Equal((long)DuckDbInitializer.CurrentSchemaVersion, Convert.ToInt64(await ScalarAsync(conn, "SELECT MAX(version) FROM schema_version")));

            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM duckdb_indexes() WHERE table_name = 'query_store_health'")));
            Assert.Equal(1L, Convert.ToInt64(await ScalarAsync(conn, "SELECT COUNT(*) FROM duckdb_views() WHERE view_name = 'v_query_store_health'")));

            using (var columns = conn.CreateCommand())
            {
                columns.CommandText = "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = 'query_store_health' ORDER BY ordinal_position";
                using var reader = await columns.ExecuteReaderAsync();
                var rows = new List<(string Name, string Type, string Nullable)>();
                while (await reader.ReadAsync())
                {
                    rows.Add((reader.GetString(0), reader.GetString(1), reader.GetString(2)));
                }

                Assert.Equal(("wait_stats_capture_mode", "VARCHAR", "YES"), rows[^1]);
                Assert.Equal(("query_capture_mode", "VARCHAR", "YES"), rows[^2]);
                Assert.Equal("interval_length_minutes", rows[^3].Name);
            }

            /* The pre-rung row reads NULL through the re-expanded view; post-rung rows land with their values —
               a 2019 row with both modes and a 2016-shaped row with the gated one NULL by construction. */
            await ExecAsync(conn, "INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode) VALUES (-2, TIMESTAMP '2026-09-20 13:00:00', 1, 'pre-v64', 'churny', 'READ_WRITE', 'READ_WRITE', 0, 7900, 8192, 'AUTO', 21, 200, 60, 'ALL', 'ON')");
            await ExecAsync(conn, "INSERT INTO query_store_health (config_id, capture_time, server_id, server_name, database_name, actual_state, desired_state, readonly_reason, current_storage_size_mb, max_storage_size_mb, size_based_cleanup_mode, stale_query_threshold_days, max_plans_per_query, interval_length_minutes, query_capture_mode, wait_stats_capture_mode) VALUES (-3, TIMESTAMP '2026-09-20 13:00:00', 1, 'pre-v64', 'legacy2016', 'READ_WRITE', 'READ_WRITE', 0, 100, 1000, 'AUTO', 30, 200, 60, 'AUTO', NULL)");

            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT query_capture_mode FROM v_query_store_health WHERE config_id = -1"));
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT wait_stats_capture_mode FROM v_query_store_health WHERE config_id = -1"));
            Assert.Equal("ALL", await ScalarAsync(conn, "SELECT query_capture_mode FROM v_query_store_health WHERE config_id = -2"));
            Assert.Equal("ON", await ScalarAsync(conn, "SELECT wait_stats_capture_mode FROM v_query_store_health WHERE config_id = -2"));
            Assert.Equal("AUTO", await ScalarAsync(conn, "SELECT query_capture_mode FROM v_query_store_health WHERE config_id = -3"));
            Assert.Equal(DBNull.Value, await ScalarAsync(conn, "SELECT wait_stats_capture_mode FROM v_query_store_health WHERE config_id = -3"));
        }
    }

    private static async Task ExecAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task<object?> ScalarAsync(DuckDBConnection conn, string sql)
    {
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        return await cmd.ExecuteScalarAsync();
    }
}
