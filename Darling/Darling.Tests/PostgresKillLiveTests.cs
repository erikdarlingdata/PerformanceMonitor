/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The PostgreSQL arm of <c>kill_session</c> (#5632). The store is a scratch database on the cluster <c>DARLING_TEST_PG</c> names. The monitored
/// PostgreSQL server is a cluster of this class's own (<see cref="ScramTlsCluster"/>), booted from the runtime <c>DARLING_TEST_PGRUNTIME</c> names,
/// because the remediation connection signs in only with SCRAM over TLS and the shared cluster has neither. Skips without both. Roles are made
/// per test and dropped after. The victim is a real backend, named by the <c>backend_id</c> the collector's own expression builds, and the tool is
/// driven end to end through the gate and the journal.
/// </summary>
/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres and its own roles on the cluster. */
public sealed class PostgresKillLiveTests(PostgresKillLiveTests.ScramTlsCluster target) : IClassFixture<PostgresKillLiveTests.ScramTlsCluster>
{
    private static string? Cluster => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    /// <summary>
    /// The monitored server for this class: a cluster initialised with SCRAM-SHA-256 for every sign-in, TLS on with a certificate made for the run,
    /// and pg_hba lines for 127.0.0.1 only. A role whose name starts <c>pmk_md5_</c> signs in with MD5 instead, for the refusal test. Stopped and
    /// deleted when the class ends. Its superuser connection pins search_path, so a test's own reads never run a planted operator.
    /// </summary>
    public sealed class ScramTlsCluster : IAsyncLifetime
    {
        public const string Superuser = "darling";

        private string? _root;
        private string? _bin;

        public string? SkipReason { get; private set; }

        public int Port { get; private set; }

        public string SuperuserPassword { get; } = "Aa1" + Guid.NewGuid().ToString("N");

        private string Data => Path.Combine(_root!, "data");

        public string ConnectionString(string user, string password, bool pinSearchPath = false)
        {
            var builder = new NpgsqlConnectionStringBuilder
            {
                Host = "127.0.0.1", Port = Port, Username = user, Password = password, Database = "postgres", Pooling = false, SslMode = SslMode.Require,
            };
            if (pinSearchPath)
                builder.Options = "-c search_path=pg_catalog";
            return builder.ConnectionString;
        }

        public string SuperuserConnectionString => ConnectionString(Superuser, SuperuserPassword, pinSearchPath: true);

        public async ValueTask InitializeAsync()
        {
            var runtime = Environment.GetEnvironmentVariable("DARLING_TEST_PGRUNTIME");
            if (string.IsNullOrEmpty(Cluster) || string.IsNullOrWhiteSpace(runtime) || !File.Exists(Path.Combine(runtime, "pgsql", "bin", "initdb.exe")))
            {
                SkipReason = "Set DARLING_TEST_PG and DARLING_TEST_PGRUNTIME (a runtime folder holding pgsql\\bin\\initdb.exe) to run the PostgreSQL kill live tests.";
                return;
            }

            _bin = Path.Combine(runtime, "pgsql", "bin");
            _root = Path.Combine(Path.GetTempPath(), "pmk-" + Guid.NewGuid().ToString("N")[..12]);
            Directory.CreateDirectory(_root);
            var passwordFile = Path.Combine(_root, "pw.txt");
            await File.WriteAllTextAsync(passwordFile, SuperuserPassword);
            await RunAsync("initdb.exe", $"-D \"{Data}\" -U {Superuser} -A scram-sha-256 --pwfile=\"{passwordFile}\" --encoding=UTF8 --no-instructions");
            File.Delete(passwordFile);

            using (var key = RSA.Create(2048))
            {
                var request = new CertificateRequest("CN=127.0.0.1", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
                using var certificate = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(2));
                await File.WriteAllTextAsync(Path.Combine(Data, "server.crt"), certificate.ExportCertificatePem());
                await File.WriteAllTextAsync(Path.Combine(Data, "server.key"), key.ExportPkcs8PrivateKeyPem());
            }

            using (var listener = new TcpListener(IPAddress.Loopback, 0))
            {
                listener.Start();
                Port = ((IPEndPoint)listener.LocalEndpoint).Port;
            }

            await File.AppendAllTextAsync(Path.Combine(Data, "postgresql.conf"),
                $"\nport = {Port}\nlisten_addresses = '127.0.0.1'\nssl = on\nmax_connections = 40\ntimezone = 'UTC'\nlog_timezone = 'UTC'\n");
            await File.WriteAllTextAsync(Path.Combine(Data, "pg_hba.conf"),
                "hostssl all /^pmk_md5_ 127.0.0.1/32 md5\nhost all all 127.0.0.1/32 scram-sha-256\n");
            /* Not through RunToolAsync: the server inherits a redirected output pipe, which then never closes. Its output goes to the log. */
            using var start = Process.Start(new ProcessStartInfo(Path.Combine(_bin, "pg_ctl.exe"), $"start -D \"{Data}\" -l \"{Path.Combine(_root, "server.log")}\" -w -t 60")
            {
                UseShellExecute = false,
                CreateNoWindow = true,
            })!;
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(90));
            await start.WaitForExitAsync(timeout.Token);
            if (start.ExitCode != 0)
                throw new InvalidOperationException($"pg_ctl start exited {start.ExitCode}: {await File.ReadAllTextAsync(Path.Combine(_root, "server.log"))}");
        }

        public async ValueTask DisposeAsync()
        {
            if (_root is null)
                return;
            if (File.Exists(Path.Combine(Data, "postmaster.pid")))
                await RunAsync("pg_ctl.exe", $"stop -D \"{Data}\" -m immediate -w -t 60", throwOnError: false);
            DarlingManagedPostgresTests.TryDeleteRecursive(_root);
        }

        /// <summary>Turns TLS off or on at the server and waits until a new connection sees it.</summary>
        public async Task SetSslAsync(bool on, CancellationToken ct)
        {
            var prefer = new NpgsqlConnectionStringBuilder(SuperuserConnectionString) { SslMode = SslMode.Prefer }.ConnectionString;
            await using (var admin = new NpgsqlConnection(prefer))
            {
                await admin.OpenAsync(ct);
                await ExecAsync(admin, on ? "ALTER SYSTEM RESET ssl" : "ALTER SYSTEM SET ssl = off", ct);
                await ExecAsync(admin, "SELECT pg_reload_conf()", ct);
            }

            for (var i = 0; i < 100; i++)
            {
                await using var connection = new NpgsqlConnection(prefer);
                await connection.OpenAsync(ct);
                await using var command = new NpgsqlCommand("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()", connection);
                if ((bool)(await command.ExecuteScalarAsync(ct))! == on)
                    return;
                await Task.Delay(100, ct);
            }

            throw new InvalidOperationException($"The test cluster did not turn TLS {(on ? "on" : "off")} after a reload.");
        }

        private async Task RunAsync(string tool, string arguments, bool throwOnError = true)
        {
            var (exitCode, output) = await DarlingManagedPostgres.RunToolAsync(Path.Combine(_bin!, tool), arguments, TimeSpan.FromSeconds(90), CancellationToken.None);
            if (exitCode != 0 && throwOnError)
                throw new InvalidOperationException($"{tool} {arguments} exited {exitCode}: {output}");
        }
    }

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

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Store, NpgsqlConnection Admin, ScramTlsCluster Target, Roles Roles, string Arg, CancellationToken Ct)
    {
        /// <summary>The kill as an agent calls it: backend_id and, unless <paramref name="omitPid"/>, the pid (by default the one inside backend_id).</summary>
        public Task<string> Kill(string? backendId, bool dryRun, int? session = null, bool omitPid = false) =>
            DarlingMcpPlanForceTools.RunKillAsync(Source, Source, null, "pg-7",
                omitPid ? null : session ?? (PostgresBackendId.TryUnpack(backendId, out var pid, out _) ? pid : null),
                null, backendId, Arg, dryRun, Ct);

        public async Task<(NpgsqlConnection Connection, int Pid, string BackendId)> OpenVictimAsync(string? role = null, string? password = null)
        {
            var builder = new NpgsqlConnectionStringBuilder(Target.ConnectionString(role ?? Roles.Victim, password ?? VictimPassword)) { ApplicationName = "PerformanceMonitorDarling-KillVictim" };
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

    private const string VictimPassword = "Victim-1x";

    private async Task RunAsync(Grants grants, Without without, Func<Ctx, Task> body)
    {
        Assert.SkipWhen(target.SkipReason is not null, target.SkipReason ?? "");
        var ct = TestContext.Current.CancellationToken;
        var roles = new Roles();
        await using var scratch = await ScratchPostgres.CreateAsync(Cluster!, ct);
        var bodySucceeded = false;
        await using var admin = new NpgsqlConnection(target.SuperuserConnectionString);
        await admin.OpenAsync(ct);
        try
        {
            await ExecAsync(admin, $"CREATE ROLE \"{roles.Victim}\" LOGIN PASSWORD '{VictimPassword}'", ct);
            await ExecAsync(admin, $"CREATE ROLE \"{roles.Remediation}\" LOGIN PASSWORD '{roles.Secret}'", ct);
            if (grants == Grants.SignalAndStats)
                await ExecAsync(admin, $"GRANT pg_signal_backend TO \"{roles.Remediation}\"", ct);
            if (grants != Grants.Nothing)
                await ExecAsync(admin, $"GRANT pg_read_all_stats TO \"{roles.Remediation}\"", ct);
            Environment.SetEnvironmentVariable(roles.EnvName, roles.Secret);

            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, port, encrypt_mode, trust_server_certificate, remediation_username, remediation_encrypted_password) "
                + $"VALUES (7, 'pg-7', '127.0.0.1', {target.Port}, 'Optional', TRUE,'{roles.Remediation}', 'env:{roles.EnvName}')", ct);
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
            await body(new Ctx(source, store, admin, target, roles, arg, ct));
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
            /* The read after the signal: an idle session in a transaction ends at once, so it is gone when read. */
            Assert.False((bool)json["still_there"]!);
            Assert.Contains("signalled to end, and it was gone when read right after", (string?)json["message"], StringComparison.Ordinal);
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
                var answer = await c.Kill(wrong, dryRun);
                var json = Parse(answer);
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal("session_reused", (string?)json["reason"]);
                /* Nothing about the session that has the pid now: not its id, not its facts. */
                Assert.Null(json["actual_backend_id"]);
                Assert.Null(json["session"]);
                Assert.DoesNotContain(id, answer, StringComparison.Ordinal);
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
            var (connection, pid, id) = await c.OpenVictimAsync(ScramTlsCluster.Superuser, c.Target.SuperuserPassword);
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
            /* The pid is required alongside backend_id, so an id changed on the way is caught rather than naming a neighbour. */
            foreach (var dryRun in new[] { true, false })
                Assert.Equal("bad_backend_id", (string?)Parse(await c.Kill(id, dryRun, omitPid: true))["reason"]);
            /* backend_id one off (what a conversion through a double can do): the pid passed with it no longer agrees. */
            var nudged = (long.Parse(id, CultureInfo.InvariantCulture) + 1).ToString(CultureInfo.InvariantCulture);
            Assert.Equal("bad_backend_id", (string?)Parse(await c.Kill(nudged, dryRun: false, session: pid))["reason"]);
            Assert.True(await c.AliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));
        });
    }

    [Fact]
    public async Task AKillWhoseReplyIsLost_AnswersUnknown_JournalsNoFailure_AndTheListSaysResultUnknown()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            /* The real statement runs and ends the backend; then the reply is lost, the way a dropped connection loses it. */
            PostgresRemediationExecutor.KillStatementSeam.Value = async (command, token) =>
            {
                await command.ExecuteScalarAsync(token);
                throw new NpgsqlException("Exception while reading from stream", new System.IO.IOException("The connection was reset."));
            };
            string answer;
            try
            {
                answer = await c.Kill(id, dryRun: false);
            }
            finally
            {
                PostgresRemediationExecutor.KillStatementSeam.Value = null;
            }

            var json = Parse(answer);
            Assert.Equal("unknown", (string?)json["status"]);
            Assert.Equal("outcome_unknown", (string?)json["reason"]);
            Assert.Contains("may have ended", (string?)json["message"], StringComparison.Ordinal);
            Assert.NotNull(json["journal_id"]);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal(1, await c.JournalAsync("action_called"));
            Assert.Equal(0, await c.JournalAsync("action_failed"));
            Assert.Equal(0, await c.JournalAsync("action_done"));
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, c.Ct)).Changes);
            Assert.Equal(AgentChangeResult.Unknown, change.Result);
        });
    }

    [Fact]
    public async Task AServerErrorOnTheKillStatement_IsAFailure_NotUnknown_AndTheBackendLives()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            /* The server answers with an error instead of running the statement: it did not run, so this is a failure. */
            PostgresRemediationExecutor.KillStatementSeam.Value = async (command, token) =>
            {
                await using var other = new NpgsqlCommand("SELECT 1/0", command.Connection);
                return await other.ExecuteScalarAsync(token);
            };
            string answer;
            try
            {
                answer = await c.Kill(id, dryRun: false);
            }
            finally
            {
                PostgresRemediationExecutor.KillStatementSeam.Value = null;
            }

            Assert.Equal("failed", (string?)Parse(answer)["status"]);
            Assert.True(await c.AliveAsync(pid));
            Assert.Equal(1, await c.JournalAsync("action_failed"));
        });
    }

    [Fact]
    public async Task ABackendIdThatHoldsTheServersStartTime_IsRefusedNamingTheGrant_NotCalledReused()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            /* The collector's own expression, run as a monitoring login without pg_read_all_stats: backend_start is NULL to it, so the id holds
               the server's start time. */
            var monitor = "pmk_mon_" + Guid.NewGuid().ToString("N")[..12];
            await ExecAsync(c.Admin, $"CREATE ROLE \"{monitor}\" LOGIN PASSWORD '{VictimPassword}'", c.Ct);
            try
            {
                string collected;
                await using (var seen = new NpgsqlConnection(c.Target.ConnectionString(monitor, VictimPassword)))
                {
                    await seen.OpenAsync(c.Ct);
                    await using var read = new NpgsqlCommand(CollectorBackendIdSql, seen);
                    read.Parameters.Add(new NpgsqlParameter { Value = pid, NpgsqlDbType = NpgsqlDbType.Integer });
                    collected = (string)(await read.ExecuteScalarAsync(c.Ct))!;
                }

                Assert.NotEqual(id, collected);
                foreach (var dryRun in new[] { true, false })
                {
                    var json = Parse(await c.Kill(collected, dryRun));
                    Assert.Equal("refused", (string?)json["status"]);
                    Assert.Equal("backend_id_has_no_session_start", (string?)json["reason"]);
                    Assert.Contains("pg_read_all_stats", (string?)json["message"], StringComparison.Ordinal);
                    Assert.DoesNotContain("reused", (string?)json["message"], StringComparison.Ordinal);
                }

                Assert.True(await c.AliveAsync(pid));
                Assert.Equal(0, await c.JournalAsync("action_called"));
            }
            finally
            {
                await ExecAsync(c.Admin, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{monitor}'", CancellationToken.None);
                await ExecAsync(c.Admin, $"DROP ROLE IF EXISTS \"{monitor}\"", CancellationToken.None);
            }
        });
    }

    [Fact]
    public void ASessionWaitingForASynchronousStandby_IsWarnedItsCommitStays_NotThatItRollsBack()
    {
        var waiting = new PgKillSessionFacts(43, null, 1_700_000_000, "u", "app", "127.0.0.1", "active", "client backend", "postgres",
            new DateTime(2026, 10, 19, 12, 0, 0), null, "SyncRep");
        var warning = Assert.Single(DarlingMcpPlanForceTools.PgKillWarnings(waiting));
        Assert.Contains("already committed", warning, StringComparison.Ordinal);
        Assert.DoesNotContain("rolls that transaction back", warning, StringComparison.Ordinal);
        Assert.Contains("rolls that transaction back",
            Assert.Single(DarlingMcpPlanForceTools.PgKillWarnings(waiting with { WaitEvent = "ClientRead" })), StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("57014", true)]
    [InlineData("57P01", true)]
    [InlineData("42501", false)]
    [InlineData("22012", false)]
    public void AServerErrorResponse_IsAFailure_ExceptACancelOrTheConnectionEnding(string sqlState, bool unknown) =>
        Assert.Equal(unknown, PostgresRemediationExecutor.IsOutcomeUnknown(new PostgresException("m", "ERROR", "ERROR", sqlState)));

    [Fact]
    public void ALostConnectionOrATimeout_IsUnknown_AndACancelByTheCallerIsNeither()
    {
        Assert.True(PostgresRemediationExecutor.IsOutcomeUnknown(new NpgsqlException("Exception while reading from stream", new TimeoutException())));
        Assert.True(PostgresRemediationExecutor.IsOutcomeUnknown(new System.IO.IOException("reset")));
        Assert.False(PostgresRemediationExecutor.IsOutcomeUnknown(new OperationCanceledException()));
    }

    /// <summary>
    /// What the owner of a database plants: operators for = and &lt;&gt; on integer and = on bigint that match every row and record each call.
    /// The functions run as their owner, so a call by any login is recorded.
    /// </summary>
    private const string PlantSql = """
        CREATE SCHEMA planted;
        GRANT USAGE ON SCHEMA planted TO PUBLIC;
        CREATE TABLE planted.calls (at timestamptz NOT NULL DEFAULT now());
        CREATE FUNCTION planted.eq(integer, integer) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
            AS $$ BEGIN INSERT INTO planted.calls DEFAULT VALUES; RETURN true; END $$;
        CREATE FUNCTION planted.ne(integer, integer) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
            AS $$ BEGIN INSERT INTO planted.calls DEFAULT VALUES; RETURN true; END $$;
        CREATE FUNCTION planted.eq8(bigint, bigint) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
            AS $$ BEGIN INSERT INTO planted.calls DEFAULT VALUES; RETURN true; END $$;
        CREATE OPERATOR planted.= (LEFTARG = integer, RIGHTARG = integer, FUNCTION = planted.eq);
        CREATE OPERATOR planted.<> (LEFTARG = integer, RIGHTARG = integer, FUNCTION = planted.ne);
        CREATE OPERATOR planted.= (LEFTARG = bigint, RIGHTARG = bigint, FUNCTION = planted.eq8);
        """;

    [Fact]
    public async Task AnOperatorPlantedInTheDatabasesSearchPath_NeverRuns_AndOnlyTheNamedBackendEnds_OnADryRunAndAKill()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var owner = "pmk_own_" + Guid.NewGuid().ToString("N")[..12];
            await ExecAsync(c.Admin, $"CREATE ROLE \"{owner}\" LOGIN PASSWORD '{VictimPassword}'", c.Ct);
            await ExecAsync(c.Admin, $"ALTER DATABASE postgres OWNER TO \"{owner}\"", c.Ct);
            try
            {
                /* A database owner who is not a superuser can do all of this. */
                await using (var asOwner = new NpgsqlConnection(c.Target.ConnectionString(owner, VictimPassword)))
                {
                    await asOwner.OpenAsync(c.Ct);
                    await ExecAsync(asOwner, PlantSql, c.Ct);
                    await ExecAsync(asOwner, "ALTER DATABASE postgres SET search_path = planted, pg_catalog", c.Ct);
                }

                /* The plant works: a session that takes the database's setting runs it, and its pid filter matches every row. */
                await using (var plain = new NpgsqlConnection(c.Target.ConnectionString(c.Roles.Victim, VictimPassword)))
                {
                    await plain.OpenAsync(c.Ct);
                    await using var probe = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE pid = 0", plain);
                    Assert.True(Convert.ToInt64(await probe.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture) > 0);
                }

                Assert.True(await CallsAsync(c) > 0);
                await ExecAsync(c.Admin, "TRUNCATE planted.calls", c.Ct);

                var (connection, pid, id) = await c.OpenVictimAsync();
                await using var victimConnection = connection;
                var (bystanderConnection, bystanderPid, _) = await c.OpenVictimAsync();
                await using var bystander = bystanderConnection;

                var dry = Parse(await c.Kill(id, dryRun: true));
                Assert.Equal("dry_run", (string?)dry["status"]);
                Assert.Equal(0, await CallsAsync(c));

                var json = Parse(await c.Kill(id, dryRun: false));
                Assert.Equal("done", (string?)json["status"]);
                Assert.False(await c.AliveAsync(pid));
                Assert.True(await c.AliveAsync(bystanderPid));
                Assert.Equal(0, await CallsAsync(c));
            }
            finally
            {
                await ExecAsync(c.Admin, "ALTER DATABASE postgres RESET search_path", CancellationToken.None);
                await ExecAsync(c.Admin, "DROP SCHEMA IF EXISTS planted CASCADE", CancellationToken.None);
                await ExecAsync(c.Admin, $"ALTER DATABASE postgres OWNER TO \"{ScramTlsCluster.Superuser}\"", CancellationToken.None);
                await ExecAsync(c.Admin, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{owner}'", CancellationToken.None);
                await ExecAsync(c.Admin, $"DROP ROLE IF EXISTS \"{owner}\"", CancellationToken.None);
            }
        });
    }

    private static async Task<long> CallsAsync(Ctx c)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM planted.calls", c.Admin);
        return Convert.ToInt64(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture);
    }

    [Fact]
    public async Task AServerThatAsksForMd5_OrHasNoTls_IsRefused_WithWhatToChange_AndNothingIsKilled()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;

            /* MD5: a login whose password is stored as MD5, and a pg_hba line that asks it for MD5. */
            var md5 = "pmk_md5_" + Guid.NewGuid().ToString("N")[..12];
            await ExecAsync(c.Admin, $"SET password_encryption = 'md5'; CREATE ROLE \"{md5}\" LOGIN PASSWORD '{c.Roles.Secret}'; RESET password_encryption;", c.Ct);
            await ExecAsync(c.Admin, $"GRANT pg_signal_backend, pg_read_all_stats TO \"{md5}\"", c.Ct);
            try
            {
                /* A TLS mode that requires TLS: under Optional, Npgsql tries again without TLS after a failed sign-in, and the server then
                   offers SCRAM without channel binding, which is refused for that reason instead. */
                await ExecAsync(c.Store, $"UPDATE config.config_monitored_servers SET remediation_username = '{md5}', encrypt_mode = 'Mandatory' WHERE server_id = 7", c.Ct);
                foreach (var dryRun in new[] { true, false })
                {
                    var json = Parse(await c.Kill(id, dryRun));
                    Assert.Equal("refused", (string?)json["status"]);
                    Assert.Equal(RemediationResult.Reasons.SignInNotStrongEnough, (string?)json["reason"]);
                    Assert.Contains("\"MD5\" authentication method is not allowed", (string?)json["message"], StringComparison.Ordinal);
                    Assert.Contains("password_encryption = 'scram-sha-256'", (string?)json["message"], StringComparison.Ordinal);
                }
            }
            finally
            {
                await ExecAsync(c.Store, $"UPDATE config.config_monitored_servers SET remediation_username = '{c.Roles.Remediation}', encrypt_mode = 'Optional' WHERE server_id = 7", CancellationToken.None);
                await ExecAsync(c.Admin, $"DROP ROLE IF EXISTS \"{md5}\"", CancellationToken.None);
            }

            /* No TLS: SCRAM is offered, but channel binding needs TLS. */
            await c.Target.SetSslAsync(on: false, c.Ct);
            try
            {
                foreach (var dryRun in new[] { true, false })
                {
                    var json = Parse(await c.Kill(id, dryRun));
                    Assert.Equal("refused", (string?)json["status"]);
                    Assert.Equal(RemediationResult.Reasons.SignInNotStrongEnough, (string?)json["reason"]);
                    Assert.Contains("ssl = on", (string?)json["message"], StringComparison.Ordinal);
                }
            }
            finally
            {
                await c.Target.SetSslAsync(on: true, CancellationToken.None);
            }

            Assert.True(await c.AliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));

            /* With TLS back, the same call goes through. */
            Assert.Equal("dry_run", (string?)Parse(await c.Kill(id, dryRun: true))["status"]);
        });
    }

    [Fact]
    public async Task ALoginThatMayNotOpenThePostgresDatabase_IsRefused_NamingThatDatabase()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            await ExecAsync(c.Admin, "REVOKE CONNECT ON DATABASE postgres FROM PUBLIC", c.Ct);
            try
            {
                var json = Parse(await c.Kill(id, dryRun: false));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal(RemediationResult.Reasons.SignInFailed, (string?)json["reason"]);
                Assert.Contains("GRANT CONNECT ON DATABASE postgres", (string?)json["message"], StringComparison.Ordinal);
            }
            finally
            {
                await ExecAsync(c.Admin, "GRANT CONNECT ON DATABASE postgres TO PUBLIC", CancellationToken.None);
            }

            Assert.True(await c.AliveAsync(pid));
        });
    }

    [Theory]
    [InlineData("Optional", true, SslMode.Prefer, ChannelBinding.Require)]
    [InlineData("Optional", false, SslMode.Prefer, ChannelBinding.Require)]
    [InlineData("Mandatory", true, SslMode.Require, ChannelBinding.Require)]
    [InlineData("Mandatory", false, SslMode.VerifyFull, ChannelBinding.Prefer)]
    [InlineData(null, false, SslMode.VerifyFull, ChannelBinding.Prefer)]
    public void TheRemediationConnection_OpensPostgres_PinsSearchPath_AndRequiresScram_WithChannelBindingUnlessTheCertificateIsVerified(
        string? encryptMode, bool trust, SslMode sslMode, ChannelBinding channelBinding)
    {
        var server = new RemediationServer("db.example.test", Database: "app", EncryptMode: encryptMode, TrustServerCertificate: trust, Port: 5433);
        var built = new NpgsqlConnectionStringBuilder(RemediationConnection.BuildPostgresConnectionString(server, new RemediationLogin("rem", "pw")));
        Assert.Equal("postgres", built.Database);
        Assert.Equal("-c search_path=pg_catalog,pg_temp", built.Options);
        Assert.Equal("ScramSHA256", built.RequireAuth);
        Assert.Equal(sslMode, built.SslMode);
        Assert.Equal(channelBinding, built.ChannelBinding);
    }

    [Fact]
    public void TheToolsOwnBackend_AndANonClientBackend_AreRefused_WhateverTheLoginMayDo()
    {
        var facts = new PgKillSessionFacts(43, null, 1_700_000_000, "u", "app", "127.0.0.1", "active", "client backend", "postgres", null, null);
        Assert.Equal(RemediationResult.Reasons.NotUserSession,
            PostgresRemediationExecutor.AfterAnswer("kill_session", 43, 1_700_000_000, facts, found: true, actualEpoch: 1_700_000_000, own: false, backendType: "walsender").Reason);

        PostgresRemediationExecutor.PgBackendRow Row(int pid, string type, long epoch = 1_700_000_000) =>
            new(new PgKillSessionFacts(pid, null, epoch, "u", "app", "127.0.0.1", "active", type, "postgres", null, null),
                OwnPid: 42, TargetIsSuperuser: false, CallerHasTargetRole: true, CallerHasSignalBackend: true, CallerIsSuperuser: true);

        Assert.Equal(RemediationResult.Reasons.OwnSession, PostgresRemediationExecutor.KillRefusal("kill_session", false, 42, 1_700_000_000, Row(42, "client backend"))!.Reason);
        Assert.Equal(RemediationResult.Reasons.NotUserSession, PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, Row(43, "walsender"))!.Reason);
        Assert.Equal(RemediationResult.Reasons.SessionReused, PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_001, Row(43, "client backend"))!.Reason);
        Assert.Null(PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, Row(43, "client backend")));
    }
}
