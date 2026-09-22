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
/// The store's own statement statistics (#3899) against real servers, in the states a store can be in.
///
/// <para><b>Loaded, on the product's own conf.</b> Boots a managed cluster from the bundled runtime, so the
/// preload comes from the conf's v13 block exactly as it does in the field, then proves the claims the feature
/// rests on: statements are attributed to the role that ran them and their constants are normalized; a role
/// password in role DDL never reaches the view; a role outside the grant is told so; a password-bearing statement
/// recorded with utility tracking forced on is refused by the reader, even by a caller that switches
/// <c>standard_conforming_strings</c> off (#3904's review reproduced that bypass against the first version), and
/// removed by the next pass, which warns once about the tracking; and a store owner that is NOT a superuser can
/// run the setup (the first version's probe failed at plan time for one) and has its hidden text reported. The
/// verbatim recording that makes the scrub necessary was measured on the bundled 18.4 / pg_stat_statements 1.12
/// before this was written.</para>
///
/// <para><b>On the shared test cluster.</b> Which preloads TimescaleDB only, so the setup lands in the
/// restart-pending state, and the tool must say so with its remedy rather than return an error. The tests read
/// the cluster's preload list and assert the state that list implies, so they prove the loaded branch too when
/// pointed at a cluster that preloads the module. The version test installs the extension at 1.7 and walks it up
/// (<c>pg_upgrade</c> leaves an old version in place, and 1.8 has no <c>pg_stat_statements_info</c>).</para>
///
/// <para><b>#1776 own-store</b> — the shared-cluster tests mint scratch databases through <c>ScratchPostgres</c>
/// (the extension and the reader functions must never land in the shared fixture's database, where other live
/// tests use the cluster as a monitored PostgreSQL target and depend on the extension being absent), and the
/// first boots its own cluster, so none is in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class StoreStatementStatsLiveTests
{
    private const string ReaderPassword = "ReaderLiteral3899";
    private const string OutsiderPassword = "OutsiderLiteral3899";
    private const string PlainOwnerPassword = "PlainOwnerLiteral3899";
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

            /* The tool, as the reader (a non-superuser, so the catalog precondition read works without
               pg_read_all_settings): the probe is attributed to its role, counted, and normalized; its query id
               is a STRING (#2548's rule); the store says what it tracks and who it read as. */
            using (var doc = JsonDocument.Parse(await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(reader, top: 1000, full_text: true)))
            {
                var root0 = doc.RootElement;
                var statements = root0.GetProperty("statements").EnumerateArray().ToList();
                var probe = Assert.Single(statements, s => QueryOf(s).Contains("pgss_probe_marker", StringComparison.Ordinal));
                Assert.Equal("pgss_reader", probe.GetProperty("role").GetString());
                Assert.Equal(3L, probe.GetProperty("calls").GetInt64());
                Assert.Contains("$1 AS pgss_probe_marker", QueryOf(probe), StringComparison.Ordinal);
                Assert.Equal(JsonValueKind.String, probe.GetProperty("query_id").ValueKind);
                Assert.True(long.TryParse(probe.GetProperty("query_id").GetString(), out _));
                Assert.DoesNotContain(statements, s => QueryOf(s).Contains("Literal3899", StringComparison.Ordinal));
                Assert.Contains(root0.GetProperty("by_role").EnumerateArray(), row => row.GetProperty("role").GetString() == "owner");
                Assert.Contains(root0.GetProperty("by_role").EnumerateArray(), row => row.GetProperty("role").GetString() == "pgss_reader");
                Assert.False(string.IsNullOrEmpty(root0.GetProperty("stats_since").GetString()));
                Assert.False(root0.GetProperty("utility_statements_tracked").GetBoolean());
                Assert.False(root0.GetProperty("connected_as_owner").GetBoolean());
                Assert.Equal(0L, root0.GetProperty("hidden_text_statements").GetInt64());
                Assert.Equal(JsonValueKind.Number, root0.GetProperty("eviction_passes").ValueKind);
                Assert.Contains("COPY", root0.GetProperty("note").GetString(), StringComparison.Ordinal);
            }

            /* The role filter keeps only that role. */
            using (var doc = JsonDocument.Parse(await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(reader, role: "owner", top: 1000)))
            {
                var owned = doc.RootElement.GetProperty("statements").EnumerateArray().ToList();
                Assert.NotEmpty(owned);
                Assert.All(owned, s => Assert.Equal("owner", s.GetProperty("role").GetString()));
            }

            /* A role outside the grant is told what it lacks, from the catalog, not handed an error. */
            var outsiderCs = new NpgsqlConnectionStringBuilder(ownerCs.ConnectionString) { Username = "pgss_outsider", Password = OutsiderPassword };
            await using (var outsider = NpgsqlDataSource.Create(outsiderCs.ConnectionString))
            {
                var denied = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(outsider);
                Assert.StartsWith("{\"status\":\"precondition\"", denied, StringComparison.Ordinal);
                Assert.Contains("may not read", denied, StringComparison.Ordinal);
            }

            /* The scrub. With utility tracking forced on, a store's state before this build or a bring-your-own
               store's default, the password is recorded VERBATIM. */
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await ExecAsync(c, "SET pg_stat_statements.track_utility = on", ct);
                await ExecAsync(c, $"ALTER ROLE pgss_outsider PASSWORD '{ScrubPassword}'", ct);
                await ExecAsync(c, "RESET pg_stat_statements.track_utility", ct);
                Assert.Equal(1L, await CountRecordedAsync(c, ScrubPassword, ct));
            }

            /* The reader refuses it, through the tool and to a caller that turns standard_conforming_strings off
               first: the first version's backslash pattern degraded to letters under that one SET and returned the
               password (#3904's review). */
            Assert.DoesNotContain(
                ScrubPassword,
                await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(reader, top: 1000, full_text: true),
                StringComparison.Ordinal);
            await using (var r = await reader.OpenConnectionAsync(ct))
            {
                await ExecAsync(r, "SET standard_conforming_strings = off", ct);
                Assert.Equal(0L, await ScalarAsync<long>(r,
                    $"SELECT pg_catalog.count(*) FROM config.{StoreStatementStats.FunctionName}() AS f WHERE f.query LIKE '%{ScrubPassword}%'", ct));
            }

            /* The next pass, on a session that still tracks utility statements, warns ONCE per process about the
               tracking and removes the password; a second pass in the same process says it at Debug only. */
            StoreStatementStats.ResetUtilityTrackingWarning();
            var firstPass = new CapturingTestLogger();
            var secondPass = new CapturingTestLogger();
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await ExecAsync(c, "SET pg_stat_statements.track_utility = on", ct);
                Assert.Equal(
                    StoreStatementStats.SetupOutcome.Ready,
                    await StoreStatementStats.EnsureAsync(c, "config", ["pgss_reader"], firstPass, ct));
                Assert.Equal(0L, await CountRecordedAsync(c, ScrubPassword, ct));
                Assert.Equal(
                    StoreStatementStats.SetupOutcome.Ready,
                    await StoreStatementStats.EnsureAsync(c, "config", ["pgss_reader"], secondPass, ct));
            }

            Assert.Contains("Warning: Statement statistics: pg_stat_statements.track_utility is on", firstPass.Joined, StringComparison.Ordinal);
            Assert.Contains("Warning: Statement statistics: removed 1 statement(s)", firstPass.Joined, StringComparison.Ordinal);
            Assert.DoesNotContain("Warning: ", secondPass.Joined, StringComparison.Ordinal);

            /* A store owner that is NOT a superuser (a bring-your-own store's usual shape): its own database, the
               extension created for it by a superuser as the README says. The first version's probe failed for
               this login at plan time, on every pass; now setup completes, and because the readers run with
               that owner's rights, other roles' text reads <insufficient privilege> and the tool says how many
               and what fixes it, until pg_read_all_stats is granted. */
            await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
            {
                await ExecAsync(c, $"CREATE ROLE pgss_plainowner LOGIN NOSUPERUSER PASSWORD '{PlainOwnerPassword}'", ct);
                await ExecAsync(c, "CREATE DATABASE pgss_plain OWNER pgss_plainowner", ct);
            }

            var plainSuperCs = new NpgsqlConnectionStringBuilder(ownerCs.ConnectionString) { Database = "pgss_plain" };
            await using (var c = await OpenAsync(plainSuperCs.ConnectionString, ct))
            {
                await ExecAsync(c, "CREATE EXTENSION pg_stat_statements", ct);
                await ExecAsync(c, "CREATE SCHEMA config AUTHORIZATION pgss_plainowner", ct);
                await ExecAsync(c, "GRANT USAGE ON SCHEMA config TO pgss_reader", ct);
                await ScalarAsync<int>(c, "SELECT 3899 AS pgss_hidden_marker", ct);
            }

            var plainOwnerCs = new NpgsqlConnectionStringBuilder(plainSuperCs.ConnectionString) { Username = "pgss_plainowner", Password = PlainOwnerPassword };
            await using (var c = await OpenAsync(plainOwnerCs.ConnectionString, ct))
            {
                Assert.False(await ScalarAsync<bool>(c, "SELECT pg_catalog.pg_has_role(current_user, 'pg_read_all_settings', 'USAGE')", ct));
                Assert.Equal(
                    StoreStatementStats.SetupOutcome.Ready,
                    await StoreStatementStats.EnsureAsync(c, "config", ["pgss_reader"], NullLogger.Instance, ct));
            }

            var plainReaderCs = new NpgsqlConnectionStringBuilder(readerCs.ConnectionString) { Database = "pgss_plain" };
            await using (var plainReader = NpgsqlDataSource.Create(plainReaderCs.ConnectionString))
            {
                using (var doc = JsonDocument.Parse(await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(plainReader, top: 1000, full_text: true)))
                {
                    Assert.True(doc.RootElement.GetProperty("hidden_text_statements").GetInt64() >= 1, doc.RootElement.ToString());
                    Assert.Contains("pg_read_all_stats", doc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
                }

                await using (var c = await OpenAsync(ownerCs.ConnectionString, ct))
                {
                    await ExecAsync(c, "GRANT pg_read_all_stats TO pgss_plainowner", ct);
                }

                using (var doc = JsonDocument.Parse(await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(plainReader, top: 1000, full_text: true)))
                {
                    Assert.Equal(0L, doc.RootElement.GetProperty("hidden_text_statements").GetInt64());
                    Assert.Contains(doc.RootElement.GetProperty("statements").EnumerateArray(), s => QueryOf(s).Contains("pgss_hidden_marker", StringComparison.Ordinal));
                }
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

        var preloaded = await IsPreloadedAsync(c, ct);
        await ExecAsync(c, "CREATE SCHEMA IF NOT EXISTS config", ct);

        var outcome = await StoreStatementStats.EnsureAsync(c, "config", ["pgss_role_that_does_not_exist"], NullLogger.Instance, ct);
        Assert.Equal(preloaded ? StoreStatementStats.SetupOutcome.Ready : StoreStatementStats.SetupOutcome.NotPreloaded, outcome);

        /* Whatever the preload state: both readers exist as definers with a pinned path and string mode, and
           PUBLIC cannot run them. */
        var definers = await DefinersAsync(c, ct);
        Assert.Equal(
            new[] { StoreStatementStats.FunctionName, StoreStatementStats.InfoFunctionName },
            definers.Select(d => d.Name).ToArray());
        Assert.All(definers, d => Assert.True(d.Definer, $"{d.Name} is not SECURITY DEFINER"));
        Assert.All(definers, d => Assert.Contains("search_path=pg_catalog, pg_temp", d.Config, StringComparison.Ordinal));
        Assert.All(definers, d => Assert.Contains("standard_conforming_strings=on", d.Config, StringComparison.Ordinal));
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
            Assert.StartsWith("{\"status\":\"precondition\"", result, StringComparison.Ordinal);
            Assert.Contains("installed but not loaded", result, StringComparison.Ordinal);
        }

        /* A second pass, the hourly tick's shape, reports the same state. */
        Assert.Equal(outcome, await StoreStatementStats.EnsureAsync(c, "config", ["pgss_role_that_does_not_exist"], NullLogger.Instance, ct));
    }

    /// <summary>
    /// The version gates (#3904's review), on a real extension walked up from 1.7: pg_upgrade leaves an old
    /// version in place, the first version's function batch failed on every pass against 1.8 (no
    /// <c>pg_stat_statements_info</c>) and the tool then advised a CREATE EXTENSION that already existed. At 1.7
    /// the readers are not built and the tool names ALTER EXTENSION UPDATE; at 1.8 they are, with an info reader
    /// that answers nulls; at the current version the info reader reads the real view.
    /// </summary>
    [Fact]
    public async Task AnOldExtensionVersion_IsNamed_AndTheReadersFollowItUp_AgainstScratchPostgres()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(baseConnectionString),
            "Set DARLING_TEST_PG to a superuser connection string to run the #3899 version proof in a scratch database.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var c = await OpenAsync(scratch.ConnectionString, ct);
        var preloaded = await IsPreloadedAsync(c, ct);
        await ExecAsync(c, "CREATE SCHEMA IF NOT EXISTS config", ct);
        await ExecAsync(c, "CREATE EXTENSION pg_stat_statements VERSION '1.7'", ct);

        Assert.Equal(StoreStatementStats.SetupOutcome.Outdated, await StoreStatementStats.EnsureAsync(c, "config", [], NullLogger.Instance, ct));
        Assert.Empty(await DefinersAsync(c, ct));

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        var outdated = await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(dataSource);
        Assert.StartsWith("{\"status\":\"precondition\"", outdated, StringComparison.Ordinal);
        Assert.Contains("ALTER EXTENSION pg_stat_statements UPDATE", outdated, StringComparison.Ordinal);

        await ExecAsync(c, "ALTER EXTENSION pg_stat_statements UPDATE TO '1.8'", ct);
        var loadedOutcome = preloaded ? StoreStatementStats.SetupOutcome.Ready : StoreStatementStats.SetupOutcome.NotPreloaded;
        Assert.Equal(loadedOutcome, await StoreStatementStats.EnsureAsync(c, "config", [], NullLogger.Instance, ct));
        Assert.DoesNotContain("pg_stat_statements_info", await InfoDefinitionAsync(c, ct), StringComparison.Ordinal);
        if (preloaded)
        {
            using var doc = JsonDocument.Parse(await DarlingMcpStoreQueryStatsTools.GetStoreQueryStats(dataSource));
            Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("stats_since").ValueKind);
            Assert.Contains("predates pg_stat_statements_info", doc.RootElement.GetProperty("note").GetString(), StringComparison.Ordinal);
        }

        await ExecAsync(c, "ALTER EXTENSION pg_stat_statements UPDATE", ct);
        Assert.Equal(loadedOutcome, await StoreStatementStats.EnsureAsync(c, "config", [], NullLogger.Instance, ct));
        Assert.Contains("pg_stat_statements_info", await InfoDefinitionAsync(c, ct), StringComparison.Ordinal);
    }

    private static string QueryOf(JsonElement statement) => statement.GetProperty("query").GetString() ?? "";

    private static async Task<bool> IsPreloadedAsync(NpgsqlConnection connection, CancellationToken ct) =>
        (await ScalarAsync<string>(connection, "SHOW shared_preload_libraries", ct))
            .Contains("pg_stat_statements", StringComparison.OrdinalIgnoreCase);

    private static async Task<List<(string Name, bool Definer, string Config)>> DefinersAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var definers = new List<(string Name, bool Definer, string Config)>();
        await using var cmd = new NpgsqlCommand(@"
SELECT p.proname::text, p.prosecdef, COALESCE(pg_catalog.array_to_string(p.proconfig, ','), '')
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = p.pronamespace
WHERE n.nspname = 'config'
AND   p.proname LIKE 'store_statement_stats%'
ORDER BY 1", connection);
        await using var rows = await cmd.ExecuteReaderAsync(ct);
        while (await rows.ReadAsync(ct))
        {
            definers.Add((rows.GetString(0), rows.GetBoolean(1), rows.GetString(2)));
        }

        return definers;
    }

    private static Task<string> InfoDefinitionAsync(NpgsqlConnection connection, CancellationToken ct) =>
        ScalarAsync<string>(connection, $"SELECT pg_catalog.pg_get_functiondef('config.{StoreStatementStats.InfoFunctionName}()'::regprocedure)", ct);

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
