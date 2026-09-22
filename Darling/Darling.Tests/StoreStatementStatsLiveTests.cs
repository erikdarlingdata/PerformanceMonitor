/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The store's own statement statistics (#3899) against real servers, in the two states a store can be in.
///
/// <para><b>Loaded, on the product's own conf.</b> Boots a managed cluster from the bundled runtime, so the
/// preload comes from the conf's v13 block exactly as it does in the field, then proves the claims the feature
/// rests on: statements are attributed to the role that ran them and their constants are normalized; a role
/// password in role DDL never reaches the view; a role outside the grant is told so; and a password-bearing
/// statement recorded with utility tracking forced on (the state of a store that loaded the module before this
/// build) is refused by the reader and removed by the next pass. The verbatim recording that makes the scrub
/// necessary was measured on the bundled 18.4 / pg_stat_statements 1.12 before this was written.</para>
///
/// <para><b>Not loaded, on the shared test cluster.</b> Which preloads TimescaleDB only, so the setup lands in
/// the restart-pending state, and the tool must say so with its remedy rather than return an error. The test
/// reads the cluster's preload list and asserts the state that list implies, so it proves the loaded branch too
/// when pointed at a cluster that preloads the module.</para>
///
/// <para><b>#1776 own-store</b> — the second test mints a scratch database through <c>ScratchPostgres</c>
/// (the extension and the reader functions must never land in the shared fixture's database, where other live
/// tests use the cluster as a monitored PostgreSQL target and depend on the extension being absent), and the
/// first boots its own cluster, so neither is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class StoreStatementStatsLiveTests
{
    private const string ReaderPassword = "ReaderLiteral3899";
    private const string OutsiderPassword = "OutsiderLiteral3899";
    private const string ScrubPassword = "ScrubLiteral3899";

    [Fact]
    public async Task OnTheProductsOwnConf_StatementsRankByRole_AndARolePasswordNeverReachesTheView_Gated()
    {
        var runtimeRoot = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(runtimeRoot),
            "Set DARLING_TEST_PGRUNTIME to an assembled pg-runtime directory (the folder containing pgsql\\bin\\pg_ctl.exe) to run the #3899 proof on the product's own conf.");
        Assert.SkipUnless(OperatingSystem.IsWindows(), "The bundled runtime is Windows-only.");
        Assert.SkipUnless(File.Exists(Path.Combine(runtimeRoot!, "pgsql", "bin", "pg_ctl.exe")),
            $"DARLING_TEST_PGRUNTIME={runtimeRoot} does not contain pgsql\\bin\\pg_ctl.exe.");

        var root = Directory.CreateTempSubdirectory("darling-pgss-");
        var config = new PostgresConfig
        {
            Managed = true,
            Port = DarlingManagedPostgresTests.FindFreeTcpPort(),
            DataDirectory = Path.Combine(root.FullName, "pg"),
        };
        var owner = new DarlingManagedPostgres(config, NullLogger.Instance, runtimeRoot);
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(5));
            var ct = timeout.Token;
            var ownerCs = new NpgsqlConnectionStringBuilder(await owner.EnsureRunningAsync(ct)) { Pooling = false };

            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                /* The v13 block loaded the library on the start that wrote it, beside TimescaleDB, with utility
                   tracking off. */
                var preload = await ScalarAsync<string>(c, "SHOW shared_preload_libraries", ct);
                Assert.Contains("timescaledb", preload, StringComparison.Ordinal);
                Assert.Contains("pg_stat_statements", preload, StringComparison.Ordinal);
                Assert.Equal("off", await ScalarAsync<string>(c, "SHOW pg_stat_statements.track_utility", ct));

                await ExecAsync(c, "CREATE SCHEMA IF NOT EXISTS config", ct);
                await ExecAsync(c, $"CREATE ROLE pgss_reader LOGIN PASSWORD '{ReaderPassword}'", ct);
                await ExecAsync(c, $"CREATE ROLE pgss_outsider LOGIN PASSWORD '{OutsiderPassword}'", ct);

                /* Schema USAGE for both, as provisioning gives the product's roles: the outsider's refusal below
                   must come from the missing EXECUTE grant, not from the schema. */
                await ExecAsync(c, "GRANT USAGE ON SCHEMA config TO pgss_reader, pgss_outsider", ct);

                Assert.Equal(
                    StoreStatementStats.SetupOutcome.Ready,
                    await StoreStatementStats.EnsureAsync(c, "config", ["pgss_reader", "pgss_absent"], NullLogger.Instance, ct));

                /* Both CREATE ROLEs above carried a password literal, and neither was recorded. */
                Assert.Equal(0L, await CountRecordedAsync(c, "Literal3899", ct));
            }

            var readerCs = new NpgsqlConnectionStringBuilder(ownerCs.ConnectionString) { Username = "pgss_reader", Password = ReaderPassword };
            await using var reader = NpgsqlDataSource.Create(readerCs.ConnectionString);
            await using (var r = await reader.OpenConnectionAsync(ct))
            {
                for (var i = 0; i < 3; i++)
                {
                    await ScalarAsync<int>(r, "SELECT 3899 AS pgss_probe_marker", ct);
                }
            }

            /* The tool, as the reader: the probe is attributed to its role, counted, and normalized. */
            using (var doc = JsonDocument.Parse(await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(reader, top: 1000, full_text: true)))
            {
                var statements = doc.RootElement.GetProperty("statements").EnumerateArray().ToList();
                var probe = Assert.Single(statements, s => QueryOf(s).Contains("pgss_probe_marker", StringComparison.Ordinal));
                Assert.Equal("pgss_reader", probe.GetProperty("role").GetString());
                Assert.Equal(3L, probe.GetProperty("calls").GetInt64());
                Assert.Contains("$1 AS pgss_probe_marker", QueryOf(probe), StringComparison.Ordinal);
                Assert.DoesNotContain(statements, s => QueryOf(s).Contains("Literal3899", StringComparison.Ordinal));
                Assert.Contains(doc.RootElement.GetProperty("by_role").EnumerateArray(), row => row.GetProperty("role").GetString() == "owner");
                Assert.Contains(doc.RootElement.GetProperty("by_role").EnumerateArray(), row => row.GetProperty("role").GetString() == "pgss_reader");
                Assert.False(string.IsNullOrEmpty(doc.RootElement.GetProperty("stats_since").GetString()));
            }

            /* The role filter keeps only that role. */
            using (var doc = JsonDocument.Parse(await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(reader, role: "owner", top: 1000)))
            {
                var owned = doc.RootElement.GetProperty("statements").EnumerateArray().ToList();
                Assert.NotEmpty(owned);
                Assert.All(owned, s => Assert.Equal("owner", s.GetProperty("role").GetString()));
            }

            /* A role outside the grant is told what it lacks, not handed an error. */
            var outsiderCs = new NpgsqlConnectionStringBuilder(ownerCs.ConnectionString) { Username = "pgss_outsider", Password = OutsiderPassword };
            await using (var outsider = NpgsqlDataSource.Create(outsiderCs.ConnectionString))
            {
                var denied = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(outsider);
                Assert.StartsWith("{\"status\":\"unavailable\"", denied, StringComparison.Ordinal);
                Assert.Contains("may not read", denied, StringComparison.Ordinal);
            }

            /* The scrub. With utility tracking forced on for one statement, the state of a store that loaded the
               module before this build, the password is recorded VERBATIM. The reader refuses it, and the next
               pass removes it. */
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await ExecAsync(c, "SET pg_stat_statements.track_utility = on", ct);
                await ExecAsync(c, $"ALTER ROLE pgss_outsider PASSWORD '{ScrubPassword}'", ct);
                await ExecAsync(c, "RESET pg_stat_statements.track_utility", ct);
                Assert.Equal(1L, await CountRecordedAsync(c, ScrubPassword, ct));
            }

            Assert.DoesNotContain(
                ScrubPassword,
                await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(reader, top: 1000, full_text: true),
                StringComparison.Ordinal);

            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                Assert.Equal(
                    StoreStatementStats.SetupOutcome.Ready,
                    await StoreStatementStats.EnsureAsync(c, "config", ["pgss_reader"], NullLogger.Instance, ct));
                Assert.Equal(0L, await CountRecordedAsync(c, ScrubPassword, ct));
            }
        }
        finally
        {
            await owner.StopIfStartedByThisProcessAsync();
            DarlingManagedPostgresTests.TryDeleteRecursive(root.FullName);
        }
    }

    [Fact]
    public async Task OnTheSharedCluster_SetupMatchesThePreloadState_AndTheToolNamesTheState_AgainstScratchPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(baseConnectionString),
            "Set DARLING_TEST_PG to a superuser connection string to run the #3899 setup proof in a scratch database.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var c = await OpenAsync(scratch.ConnectionString, ct);

        var preloaded = (await ScalarAsync<string>(c, "SHOW shared_preload_libraries", ct))
            .Contains("pg_stat_statements", StringComparison.OrdinalIgnoreCase);
        await ExecAsync(c, "CREATE SCHEMA IF NOT EXISTS config", ct);

        var outcome = await StoreStatementStats.EnsureAsync(c, "config", ["pgss_role_that_does_not_exist"], NullLogger.Instance, ct);
        Assert.Equal(preloaded ? StoreStatementStats.SetupOutcome.Ready : StoreStatementStats.SetupOutcome.NotPreloaded, outcome);

        /* Whatever the preload state: both readers exist as definers with a pinned path, and PUBLIC cannot run them. */
        var definers = new List<(string Name, bool Definer, string Config)>();
        await using (var cmd = new NpgsqlCommand(@"
SELECT p.proname::text, p.prosecdef, COALESCE(pg_catalog.array_to_string(p.proconfig, ','), '')
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = p.pronamespace
WHERE n.nspname = 'config'
AND   p.proname LIKE 'store_statement_stats%'
ORDER BY 1", c))
        await using (var rows = await cmd.ExecuteReaderAsync(ct))
        {
            while (await rows.ReadAsync(ct))
            {
                definers.Add((rows.GetString(0), rows.GetBoolean(1), rows.GetString(2)));
            }
        }

        Assert.Equal(
            new[] { StoreStatementStats.FunctionName, StoreStatementStats.InfoFunctionName },
            definers.Select(d => d.Name).ToArray());
        Assert.All(definers, d => Assert.True(d.Definer, $"{d.Name} is not SECURITY DEFINER"));
        Assert.All(definers, d => Assert.Contains("search_path=pg_catalog, pg_temp", d.Config, StringComparison.Ordinal));
        Assert.False(await ScalarAsync<bool>(c, $"SELECT pg_catalog.has_function_privilege('public', 'config.{StoreStatementStats.FunctionName}()', 'EXECUTE')", ct));

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var result = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(dataSource);
        if (preloaded)
        {
            using var doc = JsonDocument.Parse(result);
            Assert.True(doc.RootElement.TryGetProperty("statements", out _), result);
        }
        else
        {
            Assert.StartsWith("{\"status\":\"unavailable\"", result, StringComparison.Ordinal);
            Assert.Contains("installed but not loaded", result, StringComparison.Ordinal);
        }

        /* A second pass, the hourly tick's shape, reports the same state. */
        Assert.Equal(outcome, await StoreStatementStats.EnsureAsync(c, "config", ["pgss_role_that_does_not_exist"], NullLogger.Instance, ct));
    }

    private static string QueryOf(JsonElement statement) => statement.GetProperty("query").GetString() ?? "";

    private static async Task<NpgsqlConnection> OpenAsync(string connectionString, CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<T> ScalarAsync<T>(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (T)(await command.ExecuteScalarAsync(ct))!;
    }

    private static async Task<long> CountRecordedAsync(NpgsqlConnection connection, string fragment, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pg_catalog.count(*) FROM public.pg_stat_statements WHERE query LIKE '%' || $1 || '%'", connection);
        command.Parameters.AddWithValue(fragment);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}
