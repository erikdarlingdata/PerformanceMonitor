/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and touches nothing on the
   shared one, so it is not serialized against the live-postgres collection. */

/// <summary>
/// Drift guard for the web UI smoke check (#5701). <c>tools/ui-smoke/seed.sql</c> writes straight into the collector
/// tables, so a migration that renames or drops a column, or adds a NOT NULL one, breaks the seed. Without this
/// class that shows up only in the separate UI smoke workflow (which a Darling change does not trigger); here it
/// fails the PG shards on the very pull request that changed the schema.
/// <para>The test migrates a scratch store, registers the two servers the smoke config registers
/// (<c>tools/ui-smoke/darling.ui-smoke.json</c>), applies the seed with the runtime's own <c>psql</c> (the seed uses
/// psql variables, which Npgsql cannot run), and asserts the row counts <c>tools/ui-smoke/pages.json</c> depends on.
/// A count here changing means the page expectations change with it.</para>
/// </summary>
public sealed class UiSmokeSeedAppliesLiveTests
{
    [Fact]
    public async Task SeedSql_AppliesAfterMigrate_AndWritesTheRowsThePagesExpect()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the UI smoke seed drift guard (it mints its own scratch database).");
        var runtime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        var psql = string.IsNullOrEmpty(runtime) ? null : Path.Combine(runtime, "pgsql", "bin", "psql.exe");
        Assert.SkipWhen(psql is null || !File.Exists(psql),
            "Set DARLING_TEST_PGRUNTIME to the extracted pg-runtime folder: the seed is applied with its psql.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var csb = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
        await using var source = NpgsqlDataSource.Create(csb.ConnectionString);
        await using (var connection = new NpgsqlConnection(csb.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, null, ct);
        }

        /* What the service writes from darling.ui-smoke.json when it starts: the config registry rows the seed reads. */
        await using (var command = source.CreateCommand(@"
INSERT INTO config.config_monitored_servers (server_id, name, host, auth, is_enabled, created_at, engine)
VALUES (101, 'ui-sql', '127.0.0.1,1', 'integrated', TRUE, now() AT TIME ZONE 'UTC', 'sqlserver'),
       (102, 'ui-pg',  '127.0.0.1',   'sql',        TRUE, now() AT TIME ZONE 'UTC', 'postgres')"))
        {
            await command.ExecuteNonQueryAsync(ct);
        }

        var seed = Path.Combine(RepoRoot(), "tools", "ui-smoke", "seed.sql");
        Assert.True(File.Exists(seed), "tools/ui-smoke/seed.sql not found at " + seed);

        var info = new ProcessStartInfo(psql!)
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var a in new[] { "-h", csb.Host!, "-p", csb.Port.ToString(System.Globalization.CultureInfo.InvariantCulture), "-U", csb.Username!,
                     "-d", scratch.DatabaseName, "-v", "ON_ERROR_STOP=1", "-X", "-q", "-t", "-A", "-f", seed })
        {
            info.ArgumentList.Add(a);
        }
        if (!string.IsNullOrEmpty(csb.Password))
        {
            info.Environment["PGPASSWORD"] = csb.Password;
        }

        using var process = Process.Start(info)!;
        var stderrTask = process.StandardError.ReadToEndAsync(ct);
        var stdoutTask = process.StandardOutput.ReadToEndAsync(ct);
        await process.WaitForExitAsync(ct);
        var stderr = await stderrTask;
        _ = await stdoutTask;
        Assert.True(process.ExitCode == 0, "seed.sql failed against a freshly migrated store (a schema change broke it?): " + stderr.Trim());

        async Task<long> CountAsync(string table, string where)
        {
            await using var count = source.CreateCommand($"SELECT count(*) FROM {table} WHERE {where}");
            return (long)(await count.ExecuteScalarAsync(ct))!;
        }

        const string sql = "server_id = (SELECT server_id FROM config.config_monitored_servers WHERE name = 'ui-sql')";
        const string pg = "server_id = (SELECT server_id FROM config.config_monitored_servers WHERE name = 'ui-pg')";

        /* The counts pages.json relies on. */
        Assert.Equal(360, await CountAsync("collect.cpu_utilization_stats", sql));
        Assert.Equal(360, await CountAsync("collect.memory_stats", sql));
        Assert.Equal(4 * 360, await CountAsync("collect.wait_stats", sql));
        Assert.Equal(2 * 360, await CountAsync("collect.file_io_stats", sql));
        Assert.Equal(12 * 36, await CountAsync("collect.query_stats", sql));
        Assert.Equal(1, await CountAsync("collect.server_properties", sql));
        Assert.Equal(6, await CountAsync("collect.database_size_stats", sql));
        Assert.Equal(5 * 72, await CountAsync("collect.collection_log", sql));
        Assert.Equal(2, await CountAsync("collect.blocked_process_reports", sql));
        Assert.Equal(1, await CountAsync("collect.deadlocks", sql));
        Assert.Equal(2, await CountAsync("collect.analysis_findings", sql));
        Assert.Equal(1, await CountAsync("config.config_alert_log", sql));

        Assert.Equal(2 * 360, await CountAsync("collect.pg_database_stats", pg));
        Assert.Equal(3 * 360, await CountAsync("collect.pg_wait_stats", pg));
        Assert.Equal(6 * 36, await CountAsync("collect.pg_statement_stats", pg));
        Assert.Equal(6, await CountAsync("collect.pg_statement_text", pg));
        Assert.Equal(3 * 72, await CountAsync("collect.collection_log", pg));
        Assert.Equal(2, await CountAsync("collect.servers", "engine_kind IN ('sqlserver', 'postgres')"));
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !Directory.Exists(Path.Combine(dir, "tools", "ui-smoke")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        return dir ?? throw new InvalidOperationException("repo root not found from " + thisFile);
    }
}
