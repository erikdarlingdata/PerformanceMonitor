/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PostgreSQL arm of <c>kill_session</c> (#5632), against a throwaway cluster: <c>DARLING_TEST_PG</c> names it, and it plays both the store
/// (a scratch database per test) and the monitored PostgreSQL server (roles made per test, dropped after). Skips without it. The victim is a real
/// backend, named by the <c>backend_id</c> the collector's own expression builds, and the tool is driven end to end through the gate and the journal.
/// </summary>
/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres and its own roles on the cluster. */
public sealed class PostgresKillLiveTests
{
    private static string? Cluster => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>The collector's own expression for <c>backend_id</c> (PgBlockingCollector, PgSessionStatesCollector), run on the server for one pid.</summary>
    private const string CollectorBackendIdSql =
        "SELECT (extract(epoch FROM coalesce(a.backend_start, pg_postmaster_start_time()))::bigint::text || to_char(a.pid, 'FM0000000'))::bigint::text FROM pg_stat_activity AS a WHERE a.pid = $1";

    private enum Grants { SignalAndStats, StatsOnly, Nothing }

    private enum Without { Nothing, KillSwitch }

    private sealed class Roles
    {
        private readonly string _tag = Guid.NewGuid().ToString("N")[..12];

        public string Remediation => "pmk_rem_" + _tag;

        public string Victim => "pmk_vic_" + _tag;

        public string Secret { get; } = "Aa1" + Guid.NewGuid().ToString("N");

        public string EnvName => "PM_PGK_" + _tag;
    }

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Store, NpgsqlConnection Admin, Roles Roles, string Arg, CancellationToken Ct)
    {
        public Task<string> Kill(string? backendId, bool dryRun, int? session = null) =>
            DarlingMcpPlanForceTools.RunKillAsync(Source, Source, null, "pg-7", session, null, backendId, Arg, dryRun, Ct);

        public async Task<(NpgsqlConnection Connection, int Pid, string BackendId)> OpenVictimAsync(string? role = null)
        {
            var builder = new NpgsqlConnectionStringBuilder(Cluster!) { Username = role ?? Roles.Victim, Pooling = false, Database = "postgres", ApplicationName = "PerformanceMonitorDarling-KillVictim" };
            var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(Ct);
            var pid = Convert.ToInt32(await Scalar(connection, "SELECT pg_backend_pid()"), CultureInfo.InvariantCulture);
            await using var read = new NpgsqlCommand(CollectorBackendIdSql, Admin);
            read.Parameters.Add(new NpgsqlParameter { Value = pid, NpgsqlDbType = NpgsqlDbType.Integer });
            return (connection, pid, (string)(await read.ExecuteScalarAsync(Ct))!);
        }

        public async Task<bool> AliveAsync(int pid)
        {
            for (var i = 0; i < 100; i++)
            {
                if (await Scalar(Admin, $"SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}") is 0L)
                    return false;
                await Task.Delay(100, Ct);
            }

            return true;
        }

        public async Task<long> JournalAsync(string eventName) =>
            Convert.ToInt64(await Scalar(Store, $"SELECT count(*) FROM config.investigation_journal WHERE event = '{eventName}'"), CultureInfo.InvariantCulture);

        private async Task<object?> Scalar(NpgsqlConnection connection, string sql)
        {
            await using var command = new NpgsqlCommand(sql, connection);
            return await command.ExecuteScalarAsync(Ct);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task RunAsync(Grants grants, Without without, Func<Ctx, Task> body)
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Cluster), "Set DARLING_TEST_PG to run the PostgreSQL kill live tests.");
        var ct = TestContext.Current.CancellationToken;
        var roles = new Roles();
        await using var scratch = await ScratchPostgres.CreateAsync(Cluster!, ct);
        var bodySucceeded = false;
        await using var admin = new NpgsqlConnection(new NpgsqlConnectionStringBuilder(Cluster!) { Pooling = false }.ConnectionString);
        await admin.OpenAsync(ct);
        try
        {
            await ExecAsync(admin, $"CREATE ROLE \"{roles.Victim}\" LOGIN PASSWORD 'x'", ct);
            await ExecAsync(admin, $"CREATE ROLE \"{roles.Remediation}\" LOGIN PASSWORD '{roles.Secret}'", ct);
            if (grants == Grants.SignalAndStats)
                await ExecAsync(admin, $"GRANT pg_signal_backend TO \"{roles.Remediation}\"", ct);
            if (grants != Grants.Nothing)
                await ExecAsync(admin, $"GRANT pg_read_all_stats TO \"{roles.Remediation}\"", ct);
            Environment.SetEnvironmentVariable(roles.EnvName, roles.Secret);

            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            var cluster = new NpgsqlConnectionStringBuilder(Cluster!);
            await ExecAsync(store,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, port, encrypt_mode, trust_server_certificate, remediation_username, remediation_encrypted_password) "
                + $"VALUES (7, 'pg-7', '{cluster.Host}', {cluster.Port}, 'Optional', TRUE, '{roles.Remediation}', 'env:{roles.EnvName}')", ct);
            await ExecAsync(store, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, engine_kind, created_date, modified_date) "
                + "VALUES (7, 'pg-7', 'pg-7', TRUE, NULL, 'postgres', now(), now())", ct);
            await ExecAsync(store, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', 3)", ct);
            if (without != Without.KillSwitch)
                await ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'kill_session', true, 'test')", ct);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            var requests = new InvestigationRequestStore(source, 30);
            var alert = new InvestigationAlert(1, 7, "pg-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
                LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 3 };
            var opened = await requests.OpenOrJoinAsync(alert, ct);
            var claim = await requests.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString(), reason: "the blocker holds a transaction open");
            await body(new Ctx(source, store, admin, roles, arg, ct));
            bodySucceeded = true;
        }
        finally
        {
            Environment.SetEnvironmentVariable(roles.EnvName, null);
            try
            {
                await ExecAsync(admin, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename IN ('{roles.Victim}', '{roles.Remediation}')", CancellationToken.None);
                await ExecAsync(admin, $"DROP ROLE IF EXISTS \"{roles.Victim}\"", CancellationToken.None);
                await ExecAsync(admin, $"DROP ROLE IF EXISTS \"{roles.Remediation}\"", CancellationToken.None);
            }
            catch (PostgresException)
            {
                /* A role that cannot be dropped is named for this test alone; the assertions already reported what went wrong. */
            }

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    [Fact]
    public async Task ADryRunKill_ShowsTheSession_KillsNothing_JournalsNothing_AndTheKillIsDryByDefault()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            var json = Parse(await c.Kill(id, dryRun: true));
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.Equal(id, (string?)json["session"]!["backend_id"]);
            Assert.Equal(c.Roles.Victim, (string?)json["session"]!["login_name"]);
            Assert.Equal("client backend", (string?)json["session"]!["backend_type"]);
            Assert.True(await c.AliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.KillSession))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
        });
    }

    [Fact]
    public async Task ARealKill_EndsTheBackendNamedByTheCollectorsBackendId_AndJournalsCalledAndDone()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            await using (var open = new NpgsqlCommand("BEGIN; CREATE TEMP TABLE kill_probe (a int);", connection))
                await open.ExecuteNonQueryAsync(c.Ct);

            /* The id the read returned unpacks to the pid and the start second the collector derived: the C# packing matches the collector's expression. */
            Assert.True(PostgresBackendId.TryUnpack(id, out var unpackedPid, out var epoch));
            Assert.Equal(pid, unpackedPid);
            Assert.Equal(id, PostgresBackendId.Pack(epoch, pid).ToString(CultureInfo.InvariantCulture));

            var dry = Parse(await c.Kill(id, dryRun: true));
            Assert.True((bool)dry["session"]!["open_transaction"]!);
            Assert.Contains("rolls that transaction back", (string?)Assert.Single(dry["warnings"]!.AsArray()), StringComparison.Ordinal);

            var json = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("done", (string?)json["status"]);
            Assert.Null(json["notice"]);
            Assert.Contains("There is no undo", (string?)json["message"], StringComparison.Ordinal);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal(1, await c.JournalAsync("action_called"));
            Assert.Equal(1, await c.JournalAsync("action_done"));
            Assert.Equal(0, await c.JournalAsync("action_failed"));
            Assert.True(Convert.ToInt64(await new NpgsqlCommand("SELECT count(*) FROM config.investigation_journal WHERE event LIKE 'action_%' AND detail::text LIKE '%session_kill%'", c.Store).ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture) > 0);
        });
    }

    [Fact]
    public async Task WithTheKillSwitchOff_TheCallIsRefused_AndTheBackendLives()
    {
        await RunAsync(Grants.SignalAndStats, Without.KillSwitch, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            var json = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("server_action_off", (string?)json["reason"]);
            Assert.True(await c.AliveAsync(pid));
        });
    }

    [Fact]
    public async Task APidTheSystemGaveToANewBackend_HasAnotherStartSecond_IsRefusedAsReused_AndNothingIsKilled()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            PostgresBackendId.TryUnpack(id, out _, out var epoch);
            var wrong = PostgresBackendId.Pack(epoch - 1, pid).ToString(CultureInfo.InvariantCulture);
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await c.Kill(wrong, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("session_reused", (string?)json["reason"]);
                Assert.Equal(id, (string?)json["actual_backend_id"]);
            }

            Assert.True(await c.AliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));
        });
    }

    [Fact]
    public async Task AGonePid_IsRefusedAsGone()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await connection.DisposeAsync();
            Assert.False(await c.AliveAsync(pid));
            var json = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("session_gone", (string?)json["reason"]);
        });
    }

    [Fact]
    public async Task ALoginWithoutPgSignalBackend_IsRefusedAsNotPermitted_ForAnotherRolesBackend_AndNothingIsKilled()
    {
        await RunAsync(Grants.StatsOnly, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            foreach (var dryRun in new[] { true, false })
            {
                var json = Parse(await c.Kill(id, dryRun));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("not_permitted", (string?)json["reason"]);
                Assert.Contains("pg_signal_backend", (string?)json["message"], StringComparison.Ordinal);
            }

            Assert.True(await c.AliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));
        });
    }

    [Fact]
    public async Task ALoginThatCannotSeeTheStartTime_IsRefusedAsNotPermitted_AndNothingIsKilled()
    {
        await RunAsync(Grants.Nothing, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            var json = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("not_permitted", (string?)json["reason"]);
            Assert.Contains("pg_read_all_stats", (string?)json["message"], StringComparison.Ordinal);
            Assert.True(await c.AliveAsync(pid));
        });
    }

    [Fact]
    public async Task ASuperusersBackend_IsRefusedForALoginThatIsNotASuperuser_EvenWithPgSignalBackend()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync(new NpgsqlConnectionStringBuilder(Cluster!).Username);
            await using var victimConnection = connection;
            var json = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal("not_permitted", (string?)json["reason"]);
            Assert.Contains("superuser", (string?)json["message"], StringComparison.Ordinal);
            Assert.True(await c.AliveAsync(pid));
        });
    }

    [Fact]
    public async Task ABackendIdThatIsNotDigits_OrAPidThatDoesNotMatchIt_IsRefusedBeforeTheGate()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            foreach (var bad in new[] { null, "", "abc", "-5", "12345", "1.5e9" })
                Assert.Equal("bad_backend_id", (string?)Parse(await c.Kill(bad, dryRun: false))["reason"]);
            Assert.Equal("bad_backend_id", (string?)Parse(await c.Kill(id, dryRun: false, session: pid + 1))["reason"]);
            Assert.True(await c.AliveAsync(pid));
        });
    }

    [Fact]
    public void TheToolsOwnBackend_AndANonClientBackend_AreRefused_WhateverTheLoginMayDo()
    {
        PostgresRemediationExecutor.PgBackendRow Row(int pid, string type, long epoch = 1_700_000_000) =>
            new(new PgKillSessionFacts(pid, null, epoch, "u", "app", "127.0.0.1", "active", type, "postgres", null, null),
                OwnPid: 42, TargetIsSuperuser: false, CallerHasTargetRole: true, CallerHasSignalBackend: true, CallerIsSuperuser: true);

        Assert.Equal(RemediationResult.Reasons.OwnSession, PostgresRemediationExecutor.KillRefusal("kill_session", false, 42, 1_700_000_000, Row(42, "client backend"))!.Reason);
        Assert.Equal(RemediationResult.Reasons.NotUserSession, PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, Row(43, "walsender"))!.Reason);
        Assert.Equal(RemediationResult.Reasons.SessionReused, PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_001, Row(43, "client backend"))!.Reason);
        Assert.Null(PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, Row(43, "client backend")));
    }
}
