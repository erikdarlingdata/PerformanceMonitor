/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Darling rung that adds the store's statement history (#5097): two plain collect tables and one config
/// baseline table. Every fact finds the rung by NAME. The live fact mints its own scratch store.
/// </summary>
[Collection("live-postgres")]
public sealed class StoreStatementHistoryRungTests
{
    public const string RungName = "store-statement-history";

    private const int ProbeOrdinal = 138;

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static PgMigrations.Migration Rung => PgMigrations.Scripts.Single(m => m.Name == RungName);

    private static string Statements() =>
        Regex.Replace(Rung.Sql, @"/\*.*?\*/", " ", RegexOptions.Singleline).Replace("\r\n", "\n", StringComparison.Ordinal);

    private static List<(string Name, string Type)> ColumnsOf(string table)
    {
        var sql = Statements();
        var header = "CREATE TABLE IF NOT EXISTS " + table + "\n(";
        var start = sql.IndexOf(header, StringComparison.Ordinal);
        Assert.True(start >= 0, "no CREATE TABLE for " + table);
        var end = sql.IndexOf("\n)", start, StringComparison.Ordinal);
        var columns = new List<(string, string)>();
        foreach (var raw in sql[(start + header.Length)..end].Split('\n'))
        {
            var line = raw.Trim().TrimEnd(',');
            if (line.Length == 0 || line.StartsWith("CONSTRAINT", StringComparison.Ordinal))
            {
                continue;
            }

            var space = line.IndexOf(' ', StringComparison.Ordinal);
            columns.Add((line[..space], line[(space + 1)..].Replace(" NOT NULL", string.Empty, StringComparison.Ordinal).Trim()));
        }

        return columns;
    }

    [Fact]
    public void TheRungIsRegisteredOnce_InADenseLadder()
    {
        var versions = PgMigrations.Scripts.Select(s => s.Version).ToList();

        Assert.Equal(163, Rung.Version);
        Assert.Single(PgMigrations.Scripts, m => m.Name == RungName);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
        Assert.Equal(versions.Distinct().OrderBy(v => v), versions);
        Assert.Contains(Rung.Version - 1, versions);
    }

    [Fact]
    public void TheTablesArePlain_SchemaQualified_AndGuarded()
    {
        var sql = Statements();

        Assert.DoesNotContain("create_hypertable", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("timescaledb", sql, StringComparison.OrdinalIgnoreCase);
        foreach (var match in Regex.Matches(sql, @"(?:CREATE TABLE IF NOT EXISTS|ON)\s+(\S+)").Cast<Match>())
        {
            Assert.Matches(@"^(collect|config)\.", match.Groups[1].Value);
        }

        foreach (var create in Regex.Matches(sql, @"CREATE\s+(?:UNIQUE\s+)?(TABLE|INDEX)\s+(?!IF NOT EXISTS)", RegexOptions.IgnoreCase).Cast<Match>())
        {
            Assert.Fail("an unguarded CREATE: " + create.Value);
        }

        foreach (var verb in new[] { "ALTER ", "UPDATE ", "DELETE ", "GRANT ", "DROP ", "INSERT " })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.OrdinalIgnoreCase);
        }

        foreach (var index in new[]
        {
            "idx_store_statement_captures_time\n    ON collect.store_statement_captures(capture_time)",
            "idx_store_statement_history_time\n    ON collect.store_statement_history(capture_time)",
            "idx_store_statement_history_query\n    ON collect.store_statement_history(queryid, capture_time)",
        })
        {
            Assert.Contains("CREATE INDEX IF NOT EXISTS " + index, sql, StringComparison.Ordinal);
        }

        Assert.Contains("CONSTRAINT pk_store_statement_baseline PRIMARY KEY (role_name, queryid)", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheColumnsAndTypes_ArePinned()
    {
        Assert.Equal(
            ["capture_time:timestamp", "interval_seconds:integer", "stats_reset:timestamp", "dealloc:bigint", "dealloc_delta:bigint",
             "statements_seen:integer", "statements_kept:integer", "hidden_statements:integer", "outcome:text"],
            ColumnsOf("collect.store_statement_captures").Select(c => c.Name + ":" + c.Type).ToArray());

        Assert.Equal(
            ["capture_time:timestamp", "interval_seconds:integer", "role_name:text", "queryid:bigint", "delta_calls:bigint",
             "delta_total_exec_ms:double precision", "delta_rows:bigint", "delta_shared_blks_hit:bigint", "delta_shared_blks_read:bigint",
             "delta_temp_blks_written:bigint", "max_exec_ms:double precision", "first_seen:boolean", "entry_restarted:boolean",
             "reset_in_interval:boolean"],
            ColumnsOf("collect.store_statement_history").Select(c => c.Name + ":" + c.Type).ToArray());

        Assert.Equal(
            ["role_name:text", "queryid:bigint", "calls:bigint", "total_exec_ms:double precision", "rows_returned:bigint",
             "shared_blks_hit:bigint", "shared_blks_read:bigint", "temp_blks_written:bigint", "captured_at:timestamp"],
            ColumnsOf("config.store_statement_baseline").Select(c => c.Name + ":" + c.Type).ToArray());
    }

    [Fact]
    public void NoneOfTheTables_IsACollectorTable()
    {
        foreach (var table in new[] { "store_statement_captures", "store_statement_history", "store_statement_baseline" })
        {
            Assert.DoesNotContain(CollectorCatalog.All, c => c.TargetTable == table);
        }
    }

    [Fact]
    public void TheProbeCarriesTheHistoryTable_AsTheTopArm_AndMapsFullyMigratedToThisRung()
    {
        var probe = ViewerDataService.StoreSchemaProbeSql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var arm = "to_regclass('collect.store_statement_history') IS NOT NULL";
        Assert.Contains(arm, probe, StringComparison.Ordinal);
        Assert.True(probe.LastIndexOf("EXISTS", StringComparison.Ordinal) < probe.IndexOf(arm, StringComparison.Ordinal),
            "the new arm is the probe's last EXISTS, so it reads at the next ordinal");

        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.cs");
        Assert.Contains($"reader.GetBoolean({ProbeOrdinal})", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain($"reader.GetBoolean({ProbeOrdinal + 1})", viewer, StringComparison.Ordinal);

        var method = typeof(ViewerDataService).GetMethod("MapProbedSchemaVersion", BindingFlags.NonPublic | BindingFlags.Static)!;
        var parameters = method.GetParameters();
        Assert.Equal(ProbeOrdinal, parameters.Length - 1);
        Assert.Equal("hasStoreStatementHistory", parameters[ProbeOrdinal].Name);

        var all = Enumerable.Repeat((object)true, parameters.Length).ToArray();
        Assert.Equal(Rung.Version, (int)method.Invoke(null, all)!);
        Assert.Equal(StorageVersion.SchemaVersion, ViewerDataService.RequiredStoreSchemaVersion);

        var behind = (object[])all.Clone();
        behind[ProbeOrdinal] = false;
        Assert.Equal(Rung.Version - 1, (int)method.Invoke(null, behind)!);

        var thisArm = viewer.IndexOf("if (hasStoreStatementHistory)", StringComparison.Ordinal);
        var previousArm = viewer.IndexOf("if (hasSlowReads)", StringComparison.Ordinal);
        Assert.True(thisArm >= 0 && thisArm < previousArm, "this arm sits above the previous rung's");
        Assert.Contains($"return {Rung.Version};", viewer[thisArm..previousArm], StringComparison.Ordinal);

        var armProseStart = viewer.LastIndexOf("/* V163 (#5097)", thisArm, StringComparison.Ordinal);
        Assert.True(armProseStart >= 0, "the arm has no comment block saying why it exists");
        Assert.DoesNotContain("store_statement_history", viewer[armProseStart..thisArm], StringComparison.Ordinal);
    }

    [Fact]
    public async Task MigratingAFreshStore_CreatesThePlainTablesAndIndexes_AndRerunningChangesNothing()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the statement history rung's live pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        var bodySucceeded = false;
        try
        {
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));
            Assert.Equal(3L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND c.relname IN ('store_statement_captures','store_statement_history','store_statement_baseline')", ct));
            Assert.Equal(3L, await ScalarAsync(connection,
                "SELECT count(*) FROM pg_indexes WHERE indexname IN ('idx_store_statement_captures_time','idx_store_statement_history_time','idx_store_statement_history_query')", ct));

            await ExecAsync(connection, "DELETE FROM darling_schema_version WHERE version >= 163", ct);
            await PgMigrations.MigrateAsync(connection, ct);
            Assert.Equal(StorageVersion.SchemaVersion, Convert.ToInt32(await ScalarAsync(connection, "SELECT MAX(version) FROM darling_schema_version", ct)));

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return await command.ExecuteScalarAsync(ct);
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
