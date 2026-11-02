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
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The approval path of #7109 end to end on a throwaway PostgreSQL target: the agent's kill call stores a proposal, a person presses twice, and
/// the kill runs through <see cref="InvestigationRequestStore.RunApprovedActionAsync"/> with no claim. Each test names the part it covers.
/// </summary>
/* #1776 own-store: each test mints its own scratch store database through ScratchPostgres and its own roles on the cluster. */
public sealed partial class ApprovalPathLiveTests(PostgresKillLiveTests.ScramTlsCluster target) : IClassFixture<PostgresKillLiveTests.ScramTlsCluster>
{
    private static string? Cluster => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private const string VictimPassword = "Victim-1x";

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Store, NpgsqlConnection Admin, PostgresKillLiveTests.ScramTlsCluster Target,
        string Victim, string Arg, Guid RequestId, CancellationToken Ct)
    {
        public ActionProposalPress Press(Func<DateTime>? utcNow = null) => new(Source, NullLogger.Instance, utcNow);

        public InvestigationRequestStore Requests => new(Source, 30, agentActionSource: Source);

        public ApprovedActionHooks Hooks => new()
        {
            LoadRoutes = (connection, _, token) => DarlingAgentActionGate.LoadRoutesAsync(connection, NullLogger.Instance, token),
            ReadCredential = (serverId, token) => Requests.ReadServerCredentialStateAsync(Source, serverId, token),
        };

        /// <summary>Opens a victim backend and has the agent propose its kill. Answers the backend and the proposal id.</summary>
        public async Task<(NpgsqlConnection Connection, int Pid, Guid ProposalId)> ProposeKillAsync()
        {
            var builder = new NpgsqlConnectionStringBuilder(Target.ConnectionString(Victim, VictimPassword)) { ApplicationName = "pm7109-victim" };
            var connection = new NpgsqlConnection(builder.ConnectionString);
            await connection.OpenAsync(Ct);
            await using var pidCommand = new NpgsqlCommand("SELECT pg_backend_pid()", connection);
            var pid = Convert.ToInt32(await pidCommand.ExecuteScalarAsync(Ct), CultureInfo.InvariantCulture);
            await using var read = new NpgsqlCommand(PostgresKillLiveTests.CollectorBackendIdSql, Admin);
            read.Parameters.Add(new NpgsqlParameter { Value = pid, NpgsqlDbType = NpgsqlDbType.Integer });
            var backendId = (string)(await read.ExecuteScalarAsync(Ct))!;
            var answer = JsonNode.Parse(await DarlingMcpPlanForceTools.RunKillAsync(Source, Source, null, "pg-7", pid, null, backendId, Arg, false, Ct))!;
            Assert.Equal("proposed", (string?)answer["status"]);
            return (connection, pid, Guid.Parse((string)answer["proposal_id"]!));
        }

        /// <summary>A mapped person (one web identity) with <c>approve</c> for <c>kill_session</c> on server 7, unless <paramref name="grant"/> is false.</summary>
        public async Task<ProposalApprover> PersonAsync(string name, bool grant = true)
        {
            var personId = (long)(await ScalarAsync($"INSERT INTO config.person (display_name, created_by, updated_by) VALUES ('{name}', 'test', 'test') RETURNING person_id"))!;
            var external = (Guid.NewGuid().ToString("N") + "ABCDEFGHIJK")[..43];
            var identityId = (long)(await ScalarAsync($"INSERT INTO config.person_identity (person_id, kind, external_id, created_by) VALUES ({personId}, 'web', '{external}', 'test') RETURNING identity_id"))!;
            if (grant)
                await ScalarAsync($"INSERT INTO config.person_grant (person_id, scope_server_id, capability, kinds, created_by) VALUES ({personId}, 7, 'approve', ARRAY['kill_session'], 'test') RETURNING grant_id");
            return ProposalApprover.Person(personId, identityId, "web");
        }

        public async Task<object?> ScalarAsync(string sql)
        {
            await using var command = new NpgsqlCommand(sql, Store);
            return await command.ExecuteScalarAsync(Ct);
        }

        public async Task<string> StateAsync(Guid proposalId) => (string)(await ScalarAsync($"SELECT state FROM config.agent_action_proposals WHERE proposal_id = '{proposalId}'"))!;

        public async Task<long> JournalAsync(string eventName) =>
            Convert.ToInt64(await ScalarAsync($"SELECT count(*) FROM config.investigation_journal WHERE event = '{eventName}'"), CultureInfo.InvariantCulture);

        /// <summary>Up to <paramref name="polls"/> x 100 ms for the backend to leave pg_stat_activity: false the moment it does.</summary>
        public async Task<bool> AliveAsync(int pid, int polls = 100)
        {
            for (var i = 0; i < polls; i++)
            {
                await using var command = new NpgsqlCommand($"SELECT count(*) FROM pg_stat_activity WHERE pid = {pid}", Admin);
                if (await command.ExecuteScalarAsync(Ct) is 0L)
                    return false;
                await Task.Delay(100, Ct);
            }

            return true;
        }
    }

    private async Task RunAsync(bool singlePerson, Func<Ctx, Task> body)
    {
        Assert.SkipWhen(target.SkipReason is not null, target.SkipReason ?? "");
        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N")[..12];
        var victim = "pma_vic_" + tag;
        var remediation = "pma_rem_" + tag;
        var secret = "Aa1" + Guid.NewGuid().ToString("N");
        var envName = "PM_PGA_" + tag;
        await using var scratch = await ScratchPostgres.CreateAsync(Cluster!, ct, emptyStore: true);
        var bodySucceeded = false;
        await using var admin = new NpgsqlConnection(target.SuperuserConnectionString);
        await admin.OpenAsync(ct);
        try
        {
            await ExecAsync(admin, $"CREATE ROLE \"{victim}\" LOGIN PASSWORD '{VictimPassword}'", ct);
            await ExecAsync(admin, $"CREATE ROLE \"{remediation}\" LOGIN PASSWORD '{secret}'", ct);
            await ExecAsync(admin, $"GRANT pg_signal_backend TO \"{remediation}\"", ct);
            await ExecAsync(admin, $"GRANT pg_read_all_stats TO \"{remediation}\"", ct);
            Environment.SetEnvironmentVariable(envName, secret);

            await using var store = new NpgsqlConnection(scratch.ConnectionString);
            await store.OpenAsync(ct);
            await PgMigrations.MigrateAsync(store, ct);
            await ExecAsync(store,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, port, encrypt_mode, trust_server_certificate, remediation_username, remediation_encrypted_password) "
                + $"VALUES (7, 'pg-7', '127.0.0.1', {target.Port}, 'Optional', TRUE,'{remediation}', 'env:{envName}')", ct);
            await ExecAsync(store, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, engine_kind, created_date, modified_date) "
                + "VALUES (7, 'pg-7', 'pg-7', TRUE, NULL, 'postgres', now(), now())", ct);
            await ExecAsync(store, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(store, "INSERT INTO config.config_notification_routes (metric_match, agent_level, action_approval, approval_single_person) "
                + $"VALUES ('Blocking', 3, 'required', {(singlePerson ? "TRUE" : "FALSE")})", ct);
            await ExecAsync(store, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'kill_session', true, 'test')", ct);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            var requests = new InvestigationRequestStore(source, 30);
            var alert = new InvestigationAlert(1, 7, "pg-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
                LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = 3 };
            var opened = await requests.OpenOrJoinAsync(alert, ct);
            var claim = await requests.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString(), reason: "the blocker holds a transaction open");
            await body(new Ctx(source, store, admin, target, victim, arg, opened.RequestId.Value, ct));
            bodySucceeded = true;
        }
        finally
        {
            Environment.SetEnvironmentVariable(envName, null);
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, async () =>
            {
                await using var cleanup = new NpgsqlConnection(target.SuperuserConnectionString);
                await cleanup.OpenAsync(CancellationToken.None);
                try
                {
                    await ExecAsync(cleanup, $"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE usename IN ('{victim}', '{remediation}')", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{victim}\"", CancellationToken.None);
                    await ExecAsync(cleanup, $"DROP ROLE IF EXISTS \"{remediation}\"", CancellationToken.None);
                }
                catch (PostgresException)
                {
                    /* A role that cannot be dropped is named for this test alone; the assertions already reported what went wrong. */
                }
            });

            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    /// <summary>End to end, and a double confirm: proposal, first press, confirm, the backend ended, the proposal <c>run</c>, <c>action_called</c>
    /// with the agent as the actor and the approver in its detail. A second confirm is refused and runs nothing more.</summary>
    [Fact]
    public async Task AProposedKill_RunsAfterTwoPressesByOnePerson_EndsTheBackend_AndASecondConfirmRunsNothing()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var approver = await c.PersonAsync("Approver A");
            var press = c.Press();

            var first = await press.FirstPressAsync(proposalId, approver, c.Ct);
            Assert.Equal("first_pressed", first.Status);
            Assert.Equal(pid, (int)first.Facts!["pid"]!);
            Assert.True(await c.AliveAsync(pid, polls: 5));

            var confirmed = await press.ConfirmAsync(proposalId, approver, c.Ct);
            Assert.Equal("run", confirmed.Status);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal("run", await c.StateAsync(proposalId));
            Assert.Equal("agent:agent-a", (string?)await c.ScalarAsync("SELECT actor FROM config.investigation_journal WHERE event = 'action_called'"));
            Assert.Equal(approver.PersonId, Convert.ToInt64(await c.ScalarAsync(
                "SELECT detail->'approved_by'->>'person_id' FROM config.investigation_journal WHERE event = 'action_called'"), CultureInfo.InvariantCulture));
            Assert.Equal(1, await c.JournalAsync("action_done"));

            var again = await press.ConfirmAsync(proposalId, approver, c.Ct);
            Assert.Equal("refused", again.Status);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NotPending, again.Code);
            Assert.Equal(1, await c.JournalAsync("action_called"));
        });
    }

    /// <summary>The same approver makes both presses: a confirm with no first press is refused, and so is a confirm by a second person.</summary>
    [Fact]
    public async Task AConfirmWithNoFirstPress_AndAConfirmByASecondPerson_AreRefused_AndNothingRuns()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var a = await c.PersonAsync("Approver A");
            var b = await c.PersonAsync("Approver B");
            var press = c.Press();

            var early = await press.ConfirmAsync(proposalId, a, c.Ct);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NoFirstPress, early.Code);

            Assert.Equal("first_pressed", (await press.FirstPressAsync(proposalId, a, c.Ct)).Status);
            var other = await press.ConfirmAsync(proposalId, b, c.Ct);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.OtherApprover, other.Code);

            /* The store's own first transaction refuses it too, so a caller that skips the press class cannot confirm for another person. */
            var direct = await c.Requests.ApproveProposalForRunAsync(proposalId, b, c.Hooks, c.Ct);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.OtherApprover, direct.Refusal?.Reason);

            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.Equal(0, await c.JournalAsync("action_called"));
            Assert.True(await c.AliveAsync(pid, polls: 10));
        });
    }

    /// <summary>The binding: the session ran something new between the presses, so the confirm ends the proposal superseded and nothing runs.</summary>
    [Fact]
    public async Task ASessionThatChangedBetweenThePresses_EndsSuperseded_AndNothingRuns()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var approver = await c.PersonAsync("Approver A");
            var press = c.Press();
            Assert.Equal("first_pressed", (await press.FirstPressAsync(proposalId, approver, c.Ct)).Status);

            await ExecAsync(victim, "BEGIN; SELECT 1;", c.Ct);

            var confirmed = await press.ConfirmAsync(proposalId, approver, c.Ct);
            Assert.Equal("superseded", confirmed.Status);
            Assert.Contains("state", confirmed.Changed!);
            Assert.Equal("superseded", await c.StateAsync(proposalId));
            Assert.Equal(0, await c.JournalAsync("action_called"));
            Assert.True(await c.AliveAsync(pid, polls: 10));
        });
    }

    /// <summary>The grant is checked in the confirming press's transaction: a removed grant, then a disabled person, is refused, and the proposal stays pending.</summary>
    [Fact]
    public async Task ARemovedGrant_OrADisabledPerson_AtTheConfirm_IsRefused_AndTheProposalStaysPending()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var approver = await c.PersonAsync("Approver A");
            var press = c.Press();
            Assert.Equal("first_pressed", (await press.FirstPressAsync(proposalId, approver, c.Ct)).Status);

            await c.ScalarAsync($"DELETE FROM config.person_grant WHERE person_id = {approver.PersonId}");
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NotApprover, (await press.ConfirmAsync(proposalId, approver, c.Ct)).Code);

            await c.ScalarAsync($"INSERT INTO config.person_grant (person_id, scope_server_id, capability, kinds, created_by) VALUES ({approver.PersonId}, 7, 'approve', ARRAY['kill_session'], 'test')");
            await c.ScalarAsync($"UPDATE config.person SET enabled = FALSE WHERE person_id = {approver.PersonId}");
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.NotApprover, (await press.ConfirmAsync(proposalId, approver, c.Ct)).Code);

            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.Equal(0, await c.JournalAsync("action_called"));
            Assert.True(await c.AliveAsync(pid, polls: 10));
        });
    }

    /// <summary>Open rule 1 on a two-person route: a person who asked in the request cannot approve, and an administrator path cannot either.</summary>
    [Fact]
    public async Task OnATwoPersonRoute_AnAskerAndAnAdministratorPath_AreRefused()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, _, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var asker = await c.PersonAsync("Asker");
            await c.ScalarAsync("INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, detail) "
                + $"VALUES (now(), '{c.RequestId}', 7, '{InvestigationRequestStore.AskerJournalEvent}', 'person:asker', jsonb_build_object('person_id', {asker.PersonId}))");
            var press = c.Press();

            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.AskerCannotApprove, (await press.FirstPressAsync(proposalId, asker, c.Ct)).Code);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.AdministratorNeedsSinglePerson,
                (await press.FirstPressAsync(proposalId, ProposalApprover.Administrator("viewer"), c.Ct)).Code);
            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.Equal(0L, await c.ScalarAsync($"SELECT count(first_press_by) FROM config.agent_action_proposals WHERE proposal_id = '{proposalId}'"));
        });
    }

    /// <summary>Open rule 1 on a single-person route: an administrator path with no person approves, and the kill runs.</summary>
    [Fact]
    public async Task OnASinglePersonRoute_AnAdministratorPath_Approves_AndTheKillRuns()
    {
        await RunAsync(singlePerson: true, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var admin = ProposalApprover.Administrator("web_token");
            var press = c.Press();
            Assert.Equal("first_pressed", (await press.FirstPressAsync(proposalId, admin, c.Ct)).Status);
            Assert.Equal("run", (await press.ConfirmAsync(proposalId, admin, c.Ct)).Status);
            Assert.False(await c.AliveAsync(pid));
            Assert.Equal("admin:web_token", (string?)await c.ScalarAsync($"SELECT decided_by FROM config.agent_action_proposals WHERE proposal_id = '{proposalId}'"));
        });
    }

    /// <summary>Transaction 2 runs the shared conditions again: the service switch turned off after the approval marks the proposal refused,
    /// journals action_failed, and the write never runs.</summary>
    [Fact]
    public async Task TheServiceSwitchTurnedOffBeforeTheWrite_MarksTheProposalRefused_AndTheWriteNeverRuns()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var approver = await c.PersonAsync("Approver A");
            Assert.Equal("first_pressed", (await c.Press().FirstPressAsync(proposalId, approver, c.Ct)).Status);

            var approval = await c.Requests.ApproveProposalForRunAsync(proposalId, approver, c.Hooks, c.Ct);
            Assert.Null(approval.Refusal);
            await c.ScalarAsync("UPDATE config.config_service SET agent_actions_enabled = FALSE WHERE id = 1");

            var wrote = false;
            var run = await c.Requests.RunApprovedActionAsync(approval.Permit!, c.Hooks, _ =>
            {
                wrote = true;
                return Task.FromResult(new ApprovedWriteOutcome("run", "written"));
            }, c.Ct);
            Assert.Equal("refused", run.State);
            Assert.Equal(AgentActionCodes.ActionsOff, run.Code);
            Assert.False(wrote);
            Assert.Equal("refused", await c.StateAsync(proposalId));
            Assert.Equal(1, await c.JournalAsync("action_failed"));
            Assert.True(await c.AliveAsync(pid, polls: 10));

            /* A permit runs once. */
            var second = await c.Requests.RunApprovedActionAsync(approval.Permit!, c.Hooks, _ => Task.FromResult(new ApprovedWriteOutcome("run", "x")), c.Ct);
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.AlreadyRun, second.Code);
        });
    }

    /// <summary>The proposal's expiry: a press after it is refused, and nothing is recorded.</summary>
    [Fact]
    public async Task AnExpiredProposal_IsRefused()
    {
        await RunAsync(singlePerson: false, async c =>
        {
            var (connection, pid, proposalId) = await c.ProposeKillAsync();
            await using var victim = connection;
            var approver = await c.PersonAsync("Approver A");
            var late = c.Press(() => DateTime.UtcNow.AddMinutes(ProposalRules.ExpiryMinutes + 1));
            Assert.Equal(InvestigationRequestStore.ProposalPressCodes.Expired, (await late.FirstPressAsync(proposalId, approver, c.Ct)).Code);
            Assert.Equal("pending", await c.StateAsync(proposalId));
            Assert.True(await c.AliveAsync(pid, polls: 10));
        });
    }
}
