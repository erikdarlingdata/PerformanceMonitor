/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
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
                $"\nport = {Port}\nlisten_addresses = '127.0.0.1'\nssl = on\nmax_connections = 40\nfsync = off\nfull_page_writes = off\ntimezone = 'UTC'\nlog_timezone = 'UTC'\n");
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
    internal const string CollectorBackendIdSql =
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

    /// <summary>#6963: runs superuser cleanup on an open connection. The shared admin connection may be the thing the failure broke, and a cleanup on
    /// it throws "Connection is not open" over the body's own exception, so a closed one is replaced by a fresh one. As in
    /// <see cref="LiveStoreCleanup.RunOwnedAsync"/>, a cleanup failure after a failed body is dropped and one after a passing body fails the test.</summary>
    private static Task AdminCleanupAsync(NpgsqlConnection admin, ScramTlsCluster target, bool bodySucceeded, Func<NpgsqlConnection, Task> cleanup) =>
        LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
        {
            if (admin.State == ConnectionState.Open)
            {
                await cleanup(admin);
                return;
            }

            await using var fresh = new NpgsqlConnection(target.SuperuserConnectionString);
            await fresh.OpenAsync(CancellationToken.None);
            await cleanup(fresh);
        });

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Store, NpgsqlConnection Admin, ScramTlsCluster Target, Roles Roles, string Arg, CancellationToken Ct)
    {
        public Task CleanupAsync(bool bodySucceeded, Func<NpgsqlConnection, Task> cleanup) => AdminCleanupAsync(Admin, Target, bodySucceeded, cleanup);

        /// <summary>The kill as an agent calls it: backend_id and, unless <paramref name="omitPid"/>, the pid (by default the one inside backend_id).</summary>
        public Task<string> Kill(string? backendId, bool dryRun, int? session = null, bool omitPid = false) =>
            DarlingMcpPlanForceTools.RunKillAsync(Source, Source, null, "pg-7",
                omitPid ? null : session ?? (PostgresBackendId.TryUnpack(backendId, out var pid, out _) ? pid : null),
                null, backendId, Arg, dryRun, Ct);

        /// <summary>The permit the gate issues for a kill of <paramref name="pid"/> on server 7, as the tool gets it before the prepare step (#7108).</summary>
        public async Task<ServerActionPermit> PermitAsync(int pid)
        {
            var store = new InvestigationRequestStore(Source, 30, agentActionSource: Source);
            var decision = await ServerActionGate.AuthorizeAsync(store, Source, NullLogger.Instance, DarlingMcpPlanForceTools.KillToolName, ServerActionNames.KillSession,
                Arg, new AgentActionTarget(7, "pg-7", pid.ToString(CultureInfo.InvariantCulture), null), Ct);
            return decision.Permit!;
        }

        public async Task<(NpgsqlConnection Connection, int Pid, string BackendId)> OpenVictimAsync(string? role = null, string? password = null, string? application = null)
        {
            var builder = new NpgsqlConnectionStringBuilder(Target.ConnectionString(role ?? Roles.Victim, password ?? VictimPassword)) { ApplicationName = application ?? "pm5632-victim" };
            var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(Ct);
            var pid = Convert.ToInt32(await Scalar(connection, "SELECT pg_backend_pid()"), CultureInfo.InvariantCulture);
            await using var read = new NpgsqlCommand(CollectorBackendIdSql, Admin);
            read.Parameters.Add(new NpgsqlParameter { Value = pid, NpgsqlDbType = NpgsqlDbType.Integer });
            return (connection, pid, (string)(await read.ExecuteScalarAsync(Ct))!);
        }

        /// <summary>
        /// Waits up to <paramref name="polls"/> x 100 ms for the backend to leave pg_stat_activity: false the moment it
        /// does, true when it is still there at the end. A caller that expects a kill gives it the full 10 s
        /// (<see cref="AliveAsync"/>); a caller that expects no kill uses <see cref="StillAliveAsync"/>.
        /// </summary>
        public async Task<bool> AliveAsync(int pid, int polls = 100)
        {
            for (var i = 0; i < polls; i++)
            {
                if (await Scalar(Admin, $"SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}") is 0L)
                    return false;
                await Task.Delay(100, Ct);
            }

            return true;
        }

        /// <summary>
        /// "The backend was not terminated" after a refused or dry-run kill (#6972). pg_terminate_backend takes effect in
        /// milliseconds on an idle backend, so a wrongly sent kill shows within 1 s (10 polls of 100 ms) and the 10 s
        /// that <see cref="AliveAsync"/> keeps for the expected kill only added 9 s to each of these checks.
        /// </summary>
        public Task<bool> StillAliveAsync(int pid) => AliveAsync(pid, polls: 10);

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

    private async Task RunAsync(Grants grants, Without without, Func<Ctx, Task> body, string approval = "off")
    {
        Assert.SkipWhen(target.SkipReason is not null, target.SkipReason ?? "");
        var ct = TestContext.Current.CancellationToken;
        var roles = new Roles();
        await using var scratch = await ScratchPostgres.CreateAsync(Cluster!, ct, emptyStore: true);
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
            await ExecAsync(store, $"INSERT INTO config.config_notification_routes (metric_match, agent_level, action_approval) VALUES ('Blocking', 3, '{approval}')", ct);
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
            await AdminCleanupAsync(admin, target, bodySucceeded, async cleanup =>
            {
                try
                {
                    await ExecAsync(cleanup, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename IN ('{roles.Victim}', '{roles.Remediation}')", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{roles.Victim}\"", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{roles.Remediation}\"", CancellationToken.None);
                }
                catch (PostgresException)
                {
                    /* A role that cannot be dropped is named for this test alone; the assertions already reported what went wrong. */
                }
            });

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
            Assert.True(await c.StillAliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));
            var parameter = typeof(DarlingMcpPlanForceTools).GetMethod(nameof(DarlingMcpPlanForceTools.KillSession))!.GetParameters().Single(p => p.Name == "dry_run");
            Assert.True((bool)parameter.DefaultValue!);
        });
    }

    [Fact]
    public async Task ThePrepareStep_ReturnsTheBindingFacts_KillsNothing_AndTheWriteStepEndsTheBackend()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            await using (var open = new NpgsqlCommand("BEGIN; CREATE TEMP TABLE kill_probe (a int);", connection))
                await open.ExecuteNonQueryAsync(c.Ct);
            Assert.True(PostgresBackendId.TryUnpack(id, out _, out var epoch));
            await using var read = new NpgsqlCommand("SELECT xact_start, state_change, state FROM pg_stat_activity WHERE pid = $1", c.Admin);
            read.Parameters.Add(new NpgsqlParameter { Value = pid, NpgsqlDbType = NpgsqlDbType.Integer });
            await using var reader = await read.ExecuteReaderAsync(c.Ct);
            Assert.True(await reader.ReadAsync(c.Ct));
            var xactStart = reader.GetDateTime(0);
            var stateChange = reader.GetDateTime(1);
            var state = reader.GetString(2);
            await reader.CloseAsync();

            var permit = await c.PermitAsync(pid);
            var prepared = await DarlingMcpPlanForceTools.PrepareKillBackendAsync(c.Source, NullLogger.Instance, permit, 7, "pg-7", pid, epoch, c.Ct);
            Assert.True(prepared.Proceed);
            var binding = prepared.Binding!;
            Assert.Equal(pid, binding.Pid);
            Assert.Equal(id, binding.BackendId);
            Assert.Equal(epoch, binding.BackendStartEpoch);
            Assert.Equal(xactStart.Ticks, binding.XactStartUtc!.Value.Ticks);
            Assert.Equal(stateChange.Ticks, binding.StateChangeUtc!.Value.Ticks);
            Assert.Equal(state, binding.State);
            Assert.Equal("idle in transaction", binding.State);
            Assert.True(await c.StillAliveAsync(pid));

            var written = await DarlingMcpPlanForceTools.WriteKillBackendAsync(prepared.Executor!, permit, binding, c.Ct);
            Assert.Equal(RemediationOutcome.Done, written.Outcome);
            Assert.True(written.Wrote);
            Assert.False(await c.AliveAsync(pid));
        });
    }

    /// <summary>
    /// #7109: the write checks <c>xact_start</c> and <c>state_change</c> from the binding, in the statement that ends the backend. A backend that
    /// committed and opened another transaction after the dry run is refused as session_changed and stays alive. So is a backend that held no
    /// transaction (NULL <c>xact_start</c>) and opened one: a NULL equals only a NULL. The unchanged case ends the backend in the test above.
    /// </summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task TheWriteStep_RefusesABackendWhoseTransactionChangedSinceThePrepare_AndEndsNothing(bool hadTransaction)
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            if (hadTransaction)
            {
                await using var open = new NpgsqlCommand("BEGIN; CREATE TEMP TABLE kill_probe (a int);", connection);
                await open.ExecuteNonQueryAsync(c.Ct);
            }

            Assert.True(PostgresBackendId.TryUnpack(id, out _, out var epoch));
            var permit = await c.PermitAsync(pid);
            var prepared = await DarlingMcpPlanForceTools.PrepareKillBackendAsync(c.Source, NullLogger.Instance, permit, 7, "pg-7", pid, epoch, c.Ct);
            Assert.True(prepared.Proceed);
            Assert.Equal(hadTransaction, prepared.Binding!.XactStartUtc is not null);

            /* The session commits, runs a new batch, and opens a new transaction. */
            await Task.Delay(50, c.Ct);
            await using (var again = new NpgsqlCommand(hadTransaction ? "COMMIT; BEGIN; CREATE TEMP TABLE kill_probe2 (a int);" : "BEGIN; CREATE TEMP TABLE kill_probe2 (a int);", connection))
                await again.ExecuteNonQueryAsync(c.Ct);

            var written = await DarlingMcpPlanForceTools.WriteKillBackendAsync(prepared.Executor!, permit, prepared.Binding, c.Ct);
            Assert.Equal(RemediationOutcome.Refused, written.Outcome);
            Assert.Equal(RemediationResult.Reasons.SessionChanged, written.Reason);
            Assert.False(written.Wrote);
            Assert.Contains("run something new since the dry run", written.Message, StringComparison.Ordinal);
            Assert.True(await c.StillAliveAsync(pid));
        });
    }

    [Fact]
    public async Task ThePrepareStep_OfASessionThatIsGone_StopsWithTheReasonToRecord_AndNoBinding()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            Assert.True(PostgresBackendId.TryUnpack(id, out _, out var epoch));
            await connection.DisposeAsync();
            Assert.False(await c.AliveAsync(pid));

            var permit = await c.PermitAsync(pid);
            var prepared = await DarlingMcpPlanForceTools.PrepareKillBackendAsync(c.Source, NullLogger.Instance, permit, 7, "pg-7", pid, epoch, c.Ct);
            Assert.False(prepared.Proceed);
            Assert.Null(prepared.Binding);
            Assert.Equal(RemediationResult.Reasons.SessionGone, prepared.RecordReason);
            Assert.NotNull(prepared.NotDone);
        });
    }

    /// <summary>
    /// #7109: a route set to <c>required</c> turns a real kill into a proposal. The tool answers <c>proposed</c>, the backend stays alive, no
    /// <c>action_called</c> row is written, and the proposal row holds the binding the prepare step read. A retry for the same request and target gets the
    /// same proposal id and adds no row. <c>nobody_can_approve</c> is true until a mapped person holds <c>approve</c> for the kind on the server,
    /// and the retry then answers without the sentence.
    /// </summary>
    [Fact]
    public async Task ARouteThatRequiresApproval_AnswersProposed_EndsNothing_StoresTheBinding_AndARetryGetsTheSameProposal()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            await using (var open = new NpgsqlCommand("BEGIN; CREATE TEMP TABLE kill_probe (a int);", connection))
                await open.ExecuteNonQueryAsync(c.Ct);

            var first = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("proposed", (string?)first["status"]);
            Assert.Equal("kill_session", (string?)first["kind"]);
            Assert.Equal("pg-7", (string?)first["server"]);
            Assert.False((bool)first["dry_run"]!);
            Assert.Equal(id, (string?)first["session"]!["backend_id"]);
            Assert.True(Guid.TryParse((string?)first["proposal_id"], out var proposalId));
            Assert.True((bool)first["nobody_can_approve"]!);
            Assert.EndsWith("Nobody can approve this. Map a person, or turn on single-person approval.", (string?)first["message"], StringComparison.Ordinal);

            Assert.True(await c.StillAliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));
            Assert.Equal(0, await c.JournalAsync("action_done"));

            Assert.True(PostgresBackendId.TryUnpack(id, out _, out var epoch));
            await using (var read = new NpgsqlCommand(
                "SELECT state, tool, server_id, target->>'pid', binding->>'pid', binding->>'backend_id', binding->>'backend_start_epoch', binding->>'state', "
                + "(binding->>'xact_start') IS NOT NULL, (binding->>'state_change') IS NOT NULL, reason FROM config.agent_action_proposals WHERE proposal_id = $1", c.Store))
            {
                read.Parameters.Add(new NpgsqlParameter { Value = proposalId, NpgsqlDbType = NpgsqlDbType.Uuid });
                await using var reader = await read.ExecuteReaderAsync(c.Ct);
                Assert.True(await reader.ReadAsync(c.Ct));
                Assert.Equal("pending", reader.GetString(0));
                Assert.Equal("kill_session", reader.GetString(1));
                Assert.Equal(7, reader.GetInt32(2));
                Assert.Equal(pid.ToString(CultureInfo.InvariantCulture), reader.GetString(3));
                Assert.Equal(pid.ToString(CultureInfo.InvariantCulture), reader.GetString(4));
                Assert.Equal(id, reader.GetString(5));
                Assert.Equal(epoch.ToString(CultureInfo.InvariantCulture), reader.GetString(6));
                Assert.Equal("idle in transaction", reader.GetString(7));
                Assert.True(reader.GetBoolean(8));
                Assert.True(reader.GetBoolean(9));
                Assert.Equal("the blocker holds a transaction open", reader.GetString(10));
            }

            /* A person who can approve it appears; the retry is the same proposal, and the sentence is gone. */
            var actor = "person:alice";
            var ann = (await PersonDirectory.AddPersonAsync(c.Source, "Ann", actor, ServerActionSurfaces.Viewer, c.Ct)).PersonId!.Value;
            var key = DarlingWebSeat.ComputeOwnerKey("https://issuer.example.test", "sub-ann")!;
            Assert.True((await PersonDirectory.AddIdentityAsync(c.Source, ann, "web", key, actor, ServerActionSurfaces.Web, c.Ct)).Succeeded);
            Assert.True((await PersonDirectory.AddGrantAsync(c.Source, ann, 7, null, "approve", [ServerActionNames.KillSession], actor, ServerActionSurfaces.Viewer, c.Ct)).Succeeded);

            var retry = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("proposed", (string?)retry["status"]);
            Assert.Equal(proposalId.ToString(), (string?)retry["proposal_id"]);
            Assert.False((bool)retry["nobody_can_approve"]!);
            Assert.DoesNotContain("Nobody can approve", (string?)retry["message"], StringComparison.Ordinal);
            Assert.Contains("already waiting", (string?)retry["message"], StringComparison.Ordinal);
            Assert.Equal(1L, await new NpgsqlCommand("SELECT count(*) FROM config.agent_action_proposals", c.Store).ExecuteScalarAsync(c.Ct));
            Assert.True(await c.StillAliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_called"));
        }, approval: "required");
    }

    [Fact]
    public async Task ARouteThatRequiresApproval_StillAnswersADryRunAsADryRun_AndStoresNoProposal()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            var json = Parse(await c.Kill(id, dryRun: true));
            Assert.Equal("dry_run", (string?)json["status"]);
            Assert.True(await c.StillAliveAsync(pid));
            Assert.Equal(0L, await new NpgsqlCommand("SELECT count(*) FROM config.agent_action_proposals", c.Store).ExecuteScalarAsync(c.Ct));
        }, approval: "required");
    }

    /// <summary>#7109: with single-person approval on, a person can approve alone, so the proposal does not say nobody can approve.</summary>
    [Fact]
    public async Task ARouteWithSinglePersonApproval_ProposesWithoutTheNobodyCanApproveSentence()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            await ExecAsync(c.Store, "UPDATE config.config_notification_routes SET approval_single_person = TRUE", c.Ct);
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            var json = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("proposed", (string?)json["status"]);
            Assert.False((bool)json["nobody_can_approve"]!);
            Assert.DoesNotContain("Nobody can approve", (string?)json["message"], StringComparison.Ordinal);
            Assert.True(await c.StillAliveAsync(pid));
        }, approval: "required");
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
            Assert.True(await c.StillAliveAsync(pid));
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

            Assert.True(await c.StillAliveAsync(pid));
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

            Assert.True(await c.StillAliveAsync(pid));
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
            Assert.True(await c.StillAliveAsync(pid));
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
            Assert.True(await c.StillAliveAsync(pid));
        });
    }

    /// <summary>
    /// #5632: a backend of Darling itself is never ended, dry run or real: one whose application name starts with PerformanceMonitorDarling, and
    /// another backend of the remediation role. The refusal returns no facts about the backend. Nothing is signalled.
    /// </summary>
    [Fact]
    public async Task ABackendOfDarlingsApplicationAndTheMonitoringRole_OrOfTheRemediationRole_IsRefused_WithNoFacts_AndNothingIsEnded()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            /* The server entry's monitoring role is the victim role: Darling's application name together with that role is Darling's own. */
            await ExecAsync(c.Store, $"UPDATE config.config_monitored_servers SET username = '{c.Roles.Victim}' WHERE server_id = 7", c.Ct);
            foreach (var (role, password, application) in new[]
            {
                (c.Roles.Victim, VictimPassword, "PerformanceMonitorDarling-Collector"),
                (c.Roles.Remediation, c.Roles.Secret, "pm5632-other"),
            })
            {
                var (connection, pid, id) = await c.OpenVictimAsync(role, password, application);
                await using var victimConnection = connection;
                foreach (var dry in new[] { true, false })
                {
                    var json = Parse(await c.Kill(id, dryRun: dry));
                    Assert.Equal("refused", (string?)json["status"]);
                    Assert.Equal(RemediationResult.Reasons.ToolLoginSession, (string?)json["reason"]);
                    Assert.Null(json["session"]);
                    Assert.True(await c.StillAliveAsync(pid));
                }
            }

            Assert.Equal(0, await c.JournalAsync("action_called"));
        });
    }

    /// <summary>#5632: any client can name its own application, so an ordinary role with Darling's application name is not Darling's: it is ended like any other.</summary>
    [Fact]
    public async Task ABackendOfDarlingsApplication_UnderAnOrdinaryRole_IsNotRefusedAsDarlings_AndIsEnded()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            await ExecAsync(c.Store, "UPDATE config.config_monitored_servers SET username = 'pm5632_someone_else' WHERE server_id = 7", c.Ct);
            var (connection, pid, id) = await c.OpenVictimAsync(application: "PerformanceMonitorDarling-Collector");
            await using var victimConnection = connection;
            var dry = Parse(await c.Kill(id, dryRun: true));
            Assert.Equal("dry_run", (string?)dry["status"]);
            Assert.True(await c.StillAliveAsync(pid));
            var json = Parse(await c.Kill(id, dryRun: false));
            Assert.Equal("done", (string?)json["status"]);
            Assert.False(await c.AliveAsync(pid));
        });
    }

    /// <summary>
    /// #5632: a backend that takes Darling's application name between the read before and the kill statement (here: the statement is held back
    /// and the name is set first) is matched by nothing in the statement. That is a refusal as Darling's own, with no facts and nothing ended, not
    /// a failure.
    /// </summary>
    [Fact]
    public async Task ABackendThatTakesDarlingsApplicationNameBeforeTheKillStatement_IsRefusedAsDarlings_NotFailed_AndLives()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            /* The monitoring role is the victim's, so its Darling application name counts it as Darling's. */
            await ExecAsync(c.Store, $"UPDATE config.config_monitored_servers SET username = '{c.Roles.Victim}' WHERE server_id = 7", c.Ct);
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            PostgresRemediationExecutor.KillStatementSeam.Value = async (command, token) =>
            {
                await ExecAsync(victimConnection, "SET application_name = 'PerformanceMonitorDarling-Collector'", token);
                return await command.ExecuteScalarAsync(token);
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
            Assert.Equal("refused", (string?)json["status"]);
            Assert.Equal(RemediationResult.Reasons.ToolLoginSession, (string?)json["reason"]);
            Assert.Null(json["session"]);
            Assert.True(await c.StillAliveAsync(pid));
            Assert.Equal(0, await c.JournalAsync("action_done"));
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
            Assert.True(await c.StillAliveAsync(pid));
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
            Assert.Equal("outcome_unknown", (string?)json["status"]);
            Assert.Contains("may have ended", (string?)json["message"], StringComparison.Ordinal);
            Assert.NotNull(json["journal_id"]);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal(1, await c.JournalAsync("action_called"));
            Assert.Equal(0, await c.JournalAsync("action_failed"));
            Assert.Equal(0, await c.JournalAsync("action_done"));
            var change = Assert.Single((await new AgentChangeUndo(c.Source, 30).ReadAsync(50, cancellationToken: c.Ct)).Changes);
            Assert.Equal(AgentChangeResult.Unknown, change.Result);
        });
    }

    [Fact]
    public async Task AKillWhoseReplyIsLost_AndWhoseStoreRollbackFails_StillAnswersUnknown_AndJournalsOnlyTheCall()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            /* The real statement runs and ends the backend; the same fault then ends the store's open transaction too, so its rollback
               fails, and the reply is lost. */
            PostgresRemediationExecutor.KillStatementSeam.Value = async (command, token) =>
            {
                await command.ExecuteScalarAsync(token);
                const string open = "FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid() AND state = 'idle in transaction'";
                await ExecAsync(c.Store, "SELECT pg_terminate_backend(pid) " + open, token);
                for (var i = 0; i < 100; i++)
                {
                    await using var left = new NpgsqlCommand("SELECT count(*) " + open, c.Store);
                    if (await left.ExecuteScalarAsync(token) is 0L)
                        break;
                    await Task.Delay(50, token);
                }

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

            Assert.Equal("outcome_unknown", (string?)Parse(answer)["status"]);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal(1, await c.JournalAsync("action_called"));
            Assert.Equal(0, await c.JournalAsync("action_failed"));
            Assert.Equal(0, await c.JournalAsync("action_done"));
        });
    }

    [Fact]
    public async Task ACancelDuringTheReadAfterAConfirmedKill_StillJournalsDone_AndTheCancelGoesOn()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(c.Ct);
            /* pg_terminate_backend answers true; the caller cancels before the read that follows it. */
            PostgresRemediationExecutor.KillStatementSeam.Value = async (command, token) =>
            {
                var answer = await command.ExecuteScalarAsync(token);
                Assert.Equal(true, answer);
                await cts.CancelAsync();
                return answer;
            };
            try
            {
                await Assert.ThrowsAnyAsync<OperationCanceledException>(() => (c with { Ct = cts.Token }).Kill(id, dryRun: false));
            }
            finally
            {
                PostgresRemediationExecutor.KillStatementSeam.Value = null;
            }

            Assert.False(await c.AliveAsync(pid));
            Assert.Equal(1, await c.JournalAsync("action_called"));
            Assert.Equal(1, await c.JournalAsync("action_done"));
            Assert.Equal(0, await c.JournalAsync("action_failed"));
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
            Assert.True(await c.StillAliveAsync(pid));
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
            var monitorDone = false;
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

                Assert.True(await c.StillAliveAsync(pid));
                Assert.Equal(0, await c.JournalAsync("action_called"));
                monitorDone = true;
            }
            finally
            {
                await c.CleanupAsync(monitorDone, async cleanup =>
                {
                    await ExecAsync(cleanup, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{monitor}'", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{monitor}\"", CancellationToken.None);
                });
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
    [InlineData("57014", "ERROR", true)]
    [InlineData("57P01", "FATAL", true)]
    [InlineData("57P01", "ERROR", true)]
    [InlineData("57P02", "FATAL", true)]
    [InlineData("57P03", "FATAL", true)]
    [InlineData("57P04", "FATAL", true)]
    [InlineData("40001", "ERROR", true)]
    [InlineData("XX000", "FATAL", true)]
    [InlineData("XX000", "PANIC", true)]
    [InlineData("53200", "FATAL", true)]
    [InlineData("42501", "ERROR", false)]
    [InlineData("22012", "ERROR", false)]
    [InlineData("XX000", "ERROR", false)]
    [InlineData("53200", "ERROR", false)]
    public void AServerErrorResponse_IsAFailure_ExceptOneThatCanEndTheStatementAfterTheKillRan(string sqlState, string severity, bool unknown) =>
        Assert.Equal(unknown, PostgresRemediationExecutor.IsOutcomeUnknown(new PostgresException("m", severity, severity, sqlState)));

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
            var ownerDone = false;
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
                Assert.True(await c.StillAliveAsync(bystanderPid));
                Assert.Equal(0, await CallsAsync(c));
                ownerDone = true;
            }
            finally
            {
                await c.CleanupAsync(ownerDone, async cleanup =>
                {
                    await ExecAsync(cleanup, "ALTER DATABASE postgres RESET search_path", CancellationToken.None);
                    await ExecAsync(cleanup, "DROP SCHEMA IF EXISTS planted CASCADE", CancellationToken.None);
                    await ExecAsync(cleanup, $"ALTER DATABASE postgres OWNER TO \"{ScramTlsCluster.Superuser}\"", CancellationToken.None);
                    await ExecAsync(cleanup, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{owner}'", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{owner}\"", CancellationToken.None);
                });
            }
        });
    }

    private static async Task<long> CallsAsync(Ctx c)
    {
        await using var command = new NpgsqlCommand("SELECT count(*) FROM planted.calls", c.Admin);
        return Convert.ToInt64(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// What the type-loading and search_path tests add to <see cref="PlantSql"/>: an <c>=</c> for oid, which is what Npgsql's own pg_type read
    /// compares, so a connection that loads types under the planted search_path records a call. It answers as the built-in operator does, so
    /// that read still finishes.
    /// </summary>
    private const string PlantOidSql = """
        CREATE FUNCTION planted.eq_oid(oid, oid) RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog
            AS $$ BEGIN INSERT INTO planted.calls DEFAULT VALUES; RETURN $1 OPERATOR(pg_catalog.=) $2; END $$;
        CREATE OPERATOR planted.= (LEFTARG = oid, RIGHTARG = oid, FUNCTION = planted.eq_oid);
        """;

    /// <summary>Plants <see cref="PlantSql"/> and <see cref="PlantOidSql"/> in the postgres database as its owner, puts the planted schema first in the
    /// database's search_path, runs <paramref name="body"/>, and removes all of it.</summary>
    private static async Task WithPlantedDatabaseAsync(Ctx c, Func<Task> body)
    {
        var owner = "pmk_own_" + Guid.NewGuid().ToString("N")[..12];
        await ExecAsync(c.Admin, $"CREATE ROLE \"{owner}\" LOGIN PASSWORD '{VictimPassword}'", c.Ct);
        await ExecAsync(c.Admin, $"ALTER DATABASE postgres OWNER TO \"{owner}\"", c.Ct);
        var bodySucceeded = false;
        try
        {
            await using (var asOwner = new NpgsqlConnection(c.Target.ConnectionString(owner, VictimPassword)))
            {
                await asOwner.OpenAsync(c.Ct);
                await ExecAsync(asOwner, PlantSql, c.Ct);
                await ExecAsync(asOwner, PlantOidSql, c.Ct);
                await ExecAsync(asOwner, "ALTER DATABASE postgres SET search_path = planted, pg_catalog", c.Ct);
            }

            await body();
            bodySucceeded = true;
        }
        finally
        {
            await c.CleanupAsync(bodySucceeded, async cleanup =>
            {
                await ExecAsync(cleanup, "ALTER DATABASE postgres RESET search_path", CancellationToken.None);
                await ExecAsync(cleanup, "DROP SCHEMA IF EXISTS planted CASCADE", CancellationToken.None);
                await ExecAsync(cleanup, $"ALTER DATABASE postgres OWNER TO \"{ScramTlsCluster.Superuser}\"", CancellationToken.None);
                await ExecAsync(cleanup, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename = '{owner}'", CancellationToken.None);
                await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{owner}\"", CancellationToken.None);
            });
        }
    }

    /// <summary>#6143: the kill connection loads no types. Opened without the search_path option, as behind a pooler that drops it, against a
    /// database whose search_path puts a planted <c>=</c> for oid first, the connection runs none of the planted schema. The control is the same
    /// string with Npgsql's type loading left on, which does record calls, so the plant does catch what it is meant to catch.</summary>
    [Fact]
    public async Task TheKillConnection_LoadsNoTypes_SoAPlantedOperatorNeverRunsOnTheFirstOpen()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            await WithPlantedDatabaseAsync(c, async () =>
            {
                var server = new RemediationServer("127.0.0.1", EncryptMode: "Optional", TrustServerCertificate: true, Port: c.Target.Port);
                var built = new NpgsqlConnectionStringBuilder(RemediationConnection.BuildPostgresConnectionString(server, new RemediationLogin(c.Roles.Remediation, c.Roles.Secret)))
                {
                    Options = null,
                };

                /* The type read compares oids many times, and each comparison records a row, so it runs far past the 30-second budget. */
                var control = new NpgsqlConnectionStringBuilder(built.ConnectionString) { CommandTimeout = 600, Timeout = 120 };
                control.Remove("Server Compatibility Mode");
                await using (var withTypes = new NpgsqlConnection(control.ConnectionString))
                {
                    await withTypes.OpenAsync(c.Ct);
                }

                Assert.True(await CallsAsync(c) > 0, "the control connection did not run the planted operator, so this test would not notice type loading");
                await ExecAsync(c.Admin, "TRUNCATE planted.calls", c.Ct);

                await using (var connection = new NpgsqlConnection(built.ConnectionString))
                {
                    await connection.OpenAsync(c.Ct);
                    await using var show = new NpgsqlCommand("SHOW search_path", connection);
                    Assert.Contains("planted", (string?)await show.ExecuteScalarAsync(c.Ct), StringComparison.Ordinal);
                }

                Assert.Equal(0, await CallsAsync(c));
            });
        });
    }

    /// <summary>#6143: a kill connection whose server-side search_path is not the pinned one is refused before the facts are read, on a dry run and
    /// on a kill. The sign-in option is dropped through the seam, the way a connection pooler drops it, so the database's planted setting applies.</summary>
    [Fact]
    public async Task AKillConnectionWithoutThePinnedSearchPath_IsRefused_BeforeAnyFactsRead_AndNothingIsKilled()
    {
        await RunAsync(Grants.SignalAndStats, Without.Nothing, async c =>
        {
            /* The victim signs in before the plant: its own plain sign-in would load types under the planted search_path, slowly. */
            var (connection, pid, id) = await c.OpenVictimAsync();
            await using var victimConnection = connection;
            await WithPlantedDatabaseAsync(c, async () =>
            {
                PostgresRemediationExecutor.ConnectionStringSeam.Value = static s => new NpgsqlConnectionStringBuilder(s) { Options = null }.ConnectionString;
                try
                {
                    foreach (var dryRun in new[] { true, false })
                    {
                        var json = Parse(await c.Kill(id, dryRun));
                        Assert.Equal("refused", (string?)json["status"]);
                        Assert.Equal(RemediationResult.Reasons.SearchPathNotPinned, (string?)json["reason"]);
                        Assert.Contains("search_path", (string?)json["message"], StringComparison.Ordinal);
                        Assert.Contains("planted", (string?)json["message"], StringComparison.Ordinal);
                        Assert.Contains("Nothing was run", (string?)json["message"], StringComparison.Ordinal);
                    }
                }
                finally
                {
                    PostgresRemediationExecutor.ConnectionStringSeam.Value = null;
                }

                Assert.True(await c.StillAliveAsync(pid));
                Assert.Equal(0, await CallsAsync(c));
                Assert.Equal(0, await c.JournalAsync("action_called"));

                /* Without the seam the sign-in option applies, and the same call goes through. */
                Assert.Equal("dry_run", (string?)Parse(await c.Kill(id, dryRun: true))["status"]);
            });
        });
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
            var md5Done = false;
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

                md5Done = true;
            }
            finally
            {
                await ExecAsync(c.Store, $"UPDATE config.config_monitored_servers SET remediation_username = '{c.Roles.Remediation}', encrypt_mode = 'Optional' WHERE server_id = 7", CancellationToken.None);
                await c.CleanupAsync(md5Done, async cleanup => await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{md5}\"", CancellationToken.None));
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

            Assert.True(await c.StillAliveAsync(pid));
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
            var refused = false;
            try
            {
                var json = Parse(await c.Kill(id, dryRun: false));
                Assert.Equal("refused", (string?)json["status"]);
                Assert.Equal(RemediationResult.Reasons.SignInFailed, (string?)json["reason"]);
                Assert.Contains("GRANT CONNECT ON DATABASE postgres", (string?)json["message"], StringComparison.Ordinal);
                refused = true;
            }
            finally
            {
                await c.CleanupAsync(refused, async cleanup => await ExecAsync(cleanup, "GRANT CONNECT ON DATABASE postgres TO PUBLIC", CancellationToken.None));
            }

            Assert.True(await c.StillAliveAsync(pid));
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

        /* #5632: the application name together with the monitoring role, and the remediation role's backends, are refused, with no facts, whatever the login may signal. */
        var darling = Row(43, "client backend") with { Facts = Row(43, "client backend").Facts with { ApplicationName = "PerformanceMonitorDarling-Remediation" } };
        var refusedProgram = PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, darling, monitoringRole: "u")!;
        Assert.Equal(RemediationResult.Reasons.ToolLoginSession, refusedProgram.Reason);
        Assert.Null(refusedProgram.PgSession);
        /* An ordinary role with the Darling application name is not Darling's; with no monitoring role known, the application name alone decides. */
        Assert.Null(PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, darling, monitoringRole: "someone_else"));
        Assert.Equal(RemediationResult.Reasons.ToolLoginSession, PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, darling)!.Reason);
        /* The same decision after the statement: a backend that took the name since the read is refused, with no facts, not failed. */
        var afterDarling = PostgresRemediationExecutor.AfterAnswer("kill_session", 43, 1_700_000_000, darling.Facts, found: true, actualEpoch: 1_700_000_000, own: false,
            backendType: "client backend", applicationName: "PerformanceMonitorDarling-Remediation", isCallerRole: false, isMonitoringRole: true);
        Assert.Equal(RemediationResult.Reasons.ToolLoginSession, afterDarling.Reason);
        Assert.Null(afterDarling.PgSession);
        Assert.Equal(RemediationOutcome.Failed, PostgresRemediationExecutor.AfterAnswer("kill_session", 43, 1_700_000_000, darling.Facts, found: true, actualEpoch: 1_700_000_000, own: false,
            backendType: "client backend", applicationName: "PerformanceMonitorDarling-Remediation", isCallerRole: false, isMonitoringRole: false).Outcome);
        var ownRole = PostgresRemediationExecutor.KillRefusal("kill_session", false, 43, 1_700_000_000, Row(43, "client backend") with { TargetIsCallerRole = true })!;
        Assert.Equal(RemediationResult.Reasons.ToolLoginSession, ownRole.Reason);
        Assert.Null(ownRole.PgSession);
        Assert.True(PostgresRemediationExecutor.IsDarlingProgram("performancemonitordarling"));
        Assert.False(PostgresRemediationExecutor.IsDarlingProgram("PerformanceMonitor"));
        Assert.False(PostgresRemediationExecutor.IsDarlingProgram(null));
    }
}
