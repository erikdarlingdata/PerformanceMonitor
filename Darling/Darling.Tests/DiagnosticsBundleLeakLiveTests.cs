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
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: this class mints its own scratch database through ScratchPostgres and never touches another
   test's rows, so it is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #5097, the acceptance test for <c>--diagnostics-bundle</c>. A scratch store is seeded with distinctive synthetic names
/// in every source the bundle reads (registry, hosts, an FQDN with its domain suffix, an IP, databases, a login, wait
/// summaries, collection errors, slow-read arguments and sources, statement labels, statement history and service-log
/// lines), the verb writes a bundle, and no seeded string may appear anywhere in the file: not in the bytes, not
/// case-folded, not in the JSON-unescaped text. The aliases and every seeded section must be there, so the pass is not vacuous.
/// </summary>
public sealed class DiagnosticsBundleLeakLiveTests
{
    private const string ServerOne = "zeta-07";
    private const string HostOne = @"zeta-07.example.test\QXINST";
    private const string ServerTwo = "omega";
    private const string HostTwo = "203.0.113.40,1433";
    private const string DisplayTwo = "Epsilon Display";
    private const string DbOne = "DeltaLedgerDb";
    private const string DbTwo = "gamma_orders";
    private const string Login = "betaowner";
    private const string Password = "Sup3rS3cret!";
    private const string Domain = "example.test";
    private const string StoreRole = "qxreporter";
    private const string TcpServer = "qxtcpname-alpha";
    private const string TcpHostName = "qxtcphost-alpha.qxzone.test";
    private const string StoreLogSecretHost = "qxlogged-12";
    private const string RemovedServer = "qxgone-21";

    /// <summary>Everything that must not appear, including the pieces a splitter produces.</summary>
    internal static readonly string[] Forbidden =
    {
        ServerOne, "zeta-07.example.test", "QXINST", ServerTwo, "203.0.113.40", DisplayTwo, "Epsilon", DbOne, DbTwo, Login, Password, Domain,
        "zeta-07x", RemovedServer, StoreLogSecretHost, "QXCORP", "svc_zeta", StoreRole, TcpServer, TcpHostName, "qxtcphost-alpha", "qxzone",
    };

    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task DropRoleAsync(string connectionString)
    {
        await using var c = new NpgsqlConnection(connectionString);
        await c.OpenAsync(CancellationToken.None);
        await ExecAsync(c, $"DROP ROLE IF EXISTS {StoreRole}");
    }

    private static async Task ExecAsync(NpgsqlConnection c, string sql, params object?[] values)
    {
        await using var command = new NpgsqlCommand(sql, c);
        foreach (var v in values)
        {
            command.Parameters.AddWithValue(v ?? DBNull.Value);
        }

        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }

    private static async Task SeedAsync(NpgsqlConnection c)
    {
        var now = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified);
        foreach (var (id, name, host, display, db) in new[] { (701, ServerOne, HostOne, ServerOne, DbOne), (702, ServerTwo, HostTwo, DisplayTwo, DbTwo) })
        {
            await ExecAsync(c, "INSERT INTO config_monitored_servers (server_id, name, host, database, username, is_enabled) VALUES ($1, $2, $3, $4, $5, TRUE) ON CONFLICT (server_id) DO NOTHING",
                id, name, host, db, Login);
            await ExecAsync(c, "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ($1, $2, $3, TRUE, 15, $4, $4) ON CONFLICT (server_id) DO NOTHING",
                id, name, display, now);
            await ExecAsync(c, "INSERT INTO database_states (collection_id, collection_time, server_id, server_name, database_name, database_id, state_desc, is_in_standby) VALUES ($1, $2, $3, $4, $5, 5, 'ONLINE', FALSE)",
                CollectionIdGenerator.Next(), now.AddMinutes(-5), id, name, db);
        }

        /* A server registered with a protocol prefix (the Azure portal's connection form), a store login that is in no
           connection string, and a Windows account named only in an error message. */
        await ExecAsync(c, "INSERT INTO config_monitored_servers (server_id, name, host, database, username, is_enabled) VALUES (703, $1, $2, $3, $4, TRUE) ON CONFLICT (server_id) DO NOTHING",
            TcpServer, "tcp:" + TcpHostName + ",1433", DbTwo, Login);
        await ExecAsync(c, "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES (703, $1, $1, TRUE, 15, $2, $2) ON CONFLICT (server_id) DO NOTHING",
            TcpServer, now);
        /* A server removed from the registry keeps its rows in the collected-data tables, and the service log still names it. */
        await ExecAsync(c, "INSERT INTO servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES (704, $1, $1, FALSE, 15, $2, $2) ON CONFLICT (server_id) DO NOTHING",
            RemovedServer, now);
        await ExecAsync(c, $"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{StoreRole}') THEN CREATE ROLE {StoreRole} NOLOGIN; END IF; END $$");
        await ExecAsync(c, @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1, 701, $2, 'file_io_stats', $3, 3000, 'ERROR', $4, 0)",
            CollectionIdGenerator.Next(), ServerOne, now.AddMinutes(-12), $"Login failed for user 'QXCORP\\svc_zeta'. connecting to tcp:{TcpHostName},1433 (resolved {TcpHostName})");

        /* A retained store-log row whose text names a host nobody registered, with its capture denominator. */
        await ExecAsync(c, "INSERT INTO collect.store_log_captures VALUES ($1, 'postgresql-Mon.log', 1000, 0, 10, 4, FALSE, 0)", now.AddMinutes(-4));
        await ExecAsync(c, "INSERT INTO collect.store_log_events VALUES ($1, 'unrecognised', 'ERROR', 2, $2, $3)",
            now.AddMinutes(-4), $"connection to server at \"{StoreLogSecretHost}.example.test\" failed for user \"{StoreRole}\"", $"{now:yyyy-MM-dd HH:mm:ss} UTC [123] ERROR:  connection to server at \"{StoreLogSecretHost}.example.test\" failed for user \"{StoreRole}\"");

        /* A failed collection run whose error text names the server and a database. */
        await ExecAsync(c, @"INSERT INTO collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, error_message, rows_collected)
VALUES ($1, 702, $2, 'wait_stats', $3, 4000, 'ERROR', $4, 0)",
            CollectionIdGenerator.Next(), ServerTwo, now.AddMinutes(-10), $"Login failed for user '{Login}' on {ServerTwo} ({HostTwo}); database {DbTwo} unavailable");

        /* A stall probe whose wait summary names the server's FQDN and a client host. */
        await ExecAsync(c, @"INSERT INTO collect.collector_stall_probes
(probe_time, server_id, server_name, collector_name, outcome, budget_ms, trigger_elapsed_ms, waiting_task_count, top_wait_type, wait_summary, error_message)
VALUES ($1, 701, $2, 'query_store', 'sampled', 60000, 20000, 3, 'PAGEIOLATCH_SH', $3, $4)",
            now.AddMinutes(-20), ServerOne,
            $"PAGEIOLATCH_SH x3 (ours: client host zeta-07.example.test app 'Darling' db {DbOne})", $"connect to zeta-07.example.test failed as {Login}");

        /* Slow reads: arguments carry a database name, the source reason names the server, a statement label names a database. */
        await ExecAsync(c, @"INSERT INTO collect.slow_reads
(read_time, surface, route, outcome, total_ms, server_id, arguments, arguments_truncated, source, source_reason, statements, statement_count, statements_truncated, error_class, row_count)
VALUES ($1, 'mcp', 'get_query_store_top', 'ok', 9000, 701, $2::jsonb, FALSE, 'raw', $3, $4::jsonb, 1, FALSE, NULL, 12)",
            now.AddMinutes(-30),
            $$"""{"server_name":"zeta-07","database_name":"DeltaLedgerDb","note":"for omega at 203.0.113.40"}""",
            $"routed raw for zeta-07 (database {DbOne})",
            $$"""[{"ordinal":1,"label":"query_store_stats @ gamma_orders 1a2b3c","ms":8800.5,"rows":12}]""");

        /* The statement history, in the shape V163 gives it (the names match V163 as merged); created here only when the store lacks them. */
        await ExecAsync(c, @"CREATE TABLE IF NOT EXISTS collect.store_statement_captures
(capture_time timestamp NOT NULL, interval_seconds integer, stats_reset timestamp, dealloc bigint, dealloc_delta bigint, statements_seen integer NOT NULL, statements_kept integer NOT NULL, hidden_statements integer NOT NULL, outcome text NOT NULL)");
        await ExecAsync(c, @"CREATE TABLE IF NOT EXISTS collect.store_statement_history
(capture_time timestamp NOT NULL, interval_seconds integer NOT NULL, role_name text NOT NULL, queryid bigint NOT NULL, delta_calls bigint NOT NULL, delta_total_exec_ms double precision NOT NULL, delta_rows bigint NOT NULL, delta_shared_blks_hit bigint NOT NULL, delta_shared_blks_read bigint NOT NULL, delta_temp_blks_written bigint NOT NULL, max_exec_ms double precision, first_seen boolean NOT NULL, entry_restarted boolean NOT NULL, reset_in_interval boolean NOT NULL)");
        await ExecAsync(c, "INSERT INTO collect.store_statement_captures VALUES ($1, 60, NULL, 0, 0, 10, 10, 0, 'ok')", now.AddMinutes(-3));
        await ExecAsync(c, "INSERT INTO collect.store_statement_history VALUES ($1, 60, $2, 4242, 5, 12.5, 5, 10, 1, 0, 3.5, FALSE, FALSE, FALSE)", now.AddMinutes(-3), Login);
    }

    private static string Config(string connectionString, string dir)
    {
        var path = Path.Combine(dir, "darling.json");
        File.WriteAllText(path, JsonSerializer.Serialize(new
        {
            postgres = new { connectionString },
            servers = new object[]
            {
                new { name = ServerOne, host = HostOne, database = DbOne, username = Login },
                new { name = ServerTwo, host = HostTwo, database = DbTwo, username = Login },
                new { name = TcpServer, host = "tcp:" + TcpHostName + ",1433", database = DbTwo, username = Login },
            },
        }));
        return path;
    }

    private static void WriteServiceLog(string dir)
    {
        Directory.CreateDirectory(dir);
        File.WriteAllText(
            Path.Combine(dir, $"darling-service_{DateTime.Now:yyyyMMdd}.log"),
            $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [ERROR] [Connector] connect failed Server=zeta-07.example.test;Database={DbOne};User ID={Login};Password={Password}\n"
            + $"    at Connector.Open(zeta-07x, {ServerTwo})\n"
            + $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [WARN ] [Probe] slow answer from 203.0.113.40 for {DbTwo}\n"
            + $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [ERROR] [Connector] Login failed for user 'QXCORP\\svc_zeta' on tcp:{TcpHostName},1433\n"
            + $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [WARN ] [Probe] {RemovedServer} no longer answers\n");
    }

    /// <summary>The first forbidden string found in the text, in the bytes, case-folded, or in the JSON-unescaped text; null when none.</summary>
    internal static string? FirstLeak(string path)
    {
        var bytes = File.ReadAllBytes(path);
        var raw = Encoding.UTF8.GetString(bytes);
        var unescaped = new StringBuilder();
        using (var doc = JsonDocument.Parse(bytes))
        {
            Walk(doc.RootElement, unescaped);
        }

        foreach (var token in Forbidden)
        {
            var asBytes = Encoding.UTF8.GetBytes(token.ToUpperInvariant());
            var upperBytes = Encoding.UTF8.GetBytes(raw.ToUpperInvariant());
            if (raw.Contains(token, StringComparison.OrdinalIgnoreCase)
                || unescaped.ToString().Contains(token, StringComparison.OrdinalIgnoreCase)
                || upperBytes.AsSpan().IndexOf(asBytes) >= 0)
            {
                return token;
            }
        }

        return null;
    }

    private static void Walk(JsonElement element, StringBuilder sink)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var property in element.EnumerateObject())
                {
                    sink.Append(property.Name).Append('\n');
                    Walk(property.Value, sink);
                }

                break;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    Walk(item, sink);
                }

                break;
            case JsonValueKind.String:
                sink.Append(element.GetString()).Append('\n');
                break;
        }
    }

    [Fact]
    public async Task Bundle_ContainsNoSeededName_AnywhereInTheFile_AndTheSectionsAreNotEmpty()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live bundle leak test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var root = Directory.CreateTempSubdirectory("darling-bundle-leak-");
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
                await SeedAsync(connection);

                /* Store statement TEXT: the extension is installed in the scratch database and a statement naming seeded
                   names runs, as a literal (the module normalizes it away) and as an identifier (it keeps that). The
                   run must happen: a missing extension fails here, it does not skip. */
                var ensured = await StoreStatementStats.EnsureAsync(connection, "config", Array.Empty<string>(), Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, ct);
                Assert.Equal(StoreStatementStats.SetupOutcome.Ready, ensured);
                for (var i = 0; i < 3; i++)
                {
                    await using var statement = new NpgsqlCommand($"SELECT '{ServerOne}' AS {DbOne}, '{RemovedServer}' AS lit_col, pg_sleep(0.3)", connection);
                    await statement.ExecuteNonQueryAsync(ct);
                }
            }

            var logs = Path.Combine(root.FullName, "logs");
            WriteServiceLog(logs);
            var config = Config(scratch.ConnectionString, root.FullName);
            var first = Path.Combine(root.FullName, "first.json");
            var second = Path.Combine(root.FullName, "second.json");
            var map = Path.Combine(root.FullName, "map.txt");

            var error = new StringWriter();
            var exit = await DarlingCliCommands.DiagnosticsBundleAsync(new[] { first, "--config", config, "--log-dir", logs, "--alias-map", map }, new StringWriter(), error, ct);
            Assert.True(exit == DarlingCliCommands.DiagnosticsBundleExitCode.Ok, $"exit {exit}: {error}");
            Assert.True(File.Exists(first), error.ToString());

            var leaked = FirstLeak(first);
            Assert.True(leaked is null, $"the bundle contains the seeded string '{leaked}'");

            var text = await File.ReadAllTextAsync(first, ct);
            Assert.Contains("server-1", text, StringComparison.Ordinal);
            Assert.Contains("db-1", text, StringComparison.Ordinal);
            Assert.Contains("ip-1", text, StringComparison.Ordinal);

            var tree = JsonNode.Parse(text)!;
            var sections = tree["sections"]!.AsObject();
            foreach (var name in new[] { "store", "collection", "stall_probes", "slow_reads", "store_statements", "service_log", "config_shape" })
            {
                Assert.True(sections.ContainsKey(name), "missing section " + name);
            }

            /* Every seeded section was read: none may have failed, and the collection section holds the seeded run. */
            foreach (var (sectionName, node) in sections)
            {
                Assert.NotEqual("error", node?["status"]?.GetValue<string>());
            }

            foreach (var entry in tree["manifest"]!["sections"]!.AsArray())
            {
                Assert.NotEqual("error", entry!["status"]!.GetValue<string>());
            }

            Assert.NotEmpty(sections["collection"]!["slowest_runs"]!["runs"]!.AsArray());
            Assert.NotEmpty(sections["collection"]!["health_by_server"]!.AsArray());
            Assert.NotEmpty(sections["store_log"]!["retained_events"]!.AsArray());
            Assert.NotEmpty(sections["stall_probes"]!["probes"]!.AsArray());
            Assert.NotEmpty(sections["slow_reads"]!["reads"]!.AsArray());
            Assert.NotEmpty(sections["service_log"]!["entries"]!.AsArray());
            Assert.Equal("ok", sections["store_statements"]!["history"]!["status"]!.GetValue<string>());
            Assert.NotEmpty(sections["store_statements"]!["history"]!["top_statement_rows"]!.AsArray());
            Assert.Contains("DeltaLedgerDb", await File.ReadAllTextAsync(map, ct), StringComparison.Ordinal);

            /* The statement ran and its text reached the bundle, with the seeded identifier aliased. */
            var cumulative = sections["store_statements"]!["cumulative"]!.ToJsonString();
            Assert.Contains("lit_col", cumulative, StringComparison.Ordinal);
            Assert.Contains("db-", cumulative, StringComparison.Ordinal);

            /* A second run uses the same aliases. */
            var again = await DarlingCliCommands.DiagnosticsBundleAsync(new[] { second, "--config", config, "--log-dir", logs }, new StringWriter(), new StringWriter(), ct);
            Assert.True(again is DarlingCliCommands.DiagnosticsBundleExitCode.Ok or DarlingCliCommands.DiagnosticsBundleExitCode.PartialBundle);
            Assert.Equal(
                sections["stall_probes"]!["probes"]![0]!["server_name"]!.GetValue<string>(),
                JsonNode.Parse(await File.ReadAllTextAsync(second, ct))!["sections"]!["stall_probes"]!["probes"]![0]!["server_name"]!.GetValue<string>());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            await DropRoleAsync(baseConnectionString!);
            root.Delete(recursive: true);
        }
    }

    [Fact]
    public async Task SmallStoreOverTheCap_StaysUnderFourMiB()
    {
        var baseConnectionString = BaseConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to run the live bundle size test.");

        var ct = TestContext.Current.CancellationToken;
        var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var root = Directory.CreateTempSubdirectory("darling-bundle-size-");
        var bodySucceeded = false;
        try
        {
            await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
            {
                await connection.OpenAsync(ct);
                await PgMigrations.MigrateAsync(connection, ct);
            }

            var logs = Path.Combine(root.FullName, "logs");
            Directory.CreateDirectory(logs);
            var line = $"{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff} [ERROR] [Cat] {new string('x', 1_800)}\n";
            File.WriteAllText(Path.Combine(logs, $"darling-service_{DateTime.Now:yyyyMMdd}.log"), string.Concat(Enumerable.Repeat(line, 3_000)));
            var bundle = Path.Combine(root.FullName, "b.json");
            var exit = await DarlingCliCommands.DiagnosticsBundleAsync(
                new[] { bundle, "--config", Config(scratch.ConnectionString, root.FullName), "--log-dir", logs }, new StringWriter(), new StringWriter(), ct);
            Assert.True(exit is 0 or 3);
            Assert.True(new FileInfo(bundle).Length <= DiagnosticsBundle.TotalCapBytes);
            var serviceLog = JsonNode.Parse(await File.ReadAllTextAsync(bundle, ct))!["sections"]!["service_log"]!;
            Assert.True(new FileInfo(bundle).Length > 100_000);
            Assert.NotEmpty(serviceLog["entries"]!.AsArray());
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, async (_, _) => await Task.CompletedTask);
            await scratch.DisposeAsync();
            root.Delete(recursive: true);
        }
    }
}
