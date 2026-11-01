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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// The checks after an agent forces a plan (#5632, step 8) on a live store. The force is seeded as the journal rows the tool writes
/// (<c>action_called</c> then <c>action_done</c>); the sweep is driven with an explicit clock; <c>collect.collection_log</c> and
/// <c>query_store_stats</c> hold what the collector wrote. Nothing here touches a monitored server: the sweep reads the store.
/// </summary>
public sealed class PlanForceCheckLiveTests
{
    private const int ServerId = 7;
    private const string Database = "Db1";
    private const long QueryId = 5;
    private const long NewPlan = 22;
    private const long OldPlan = 21;
    private const long OtherPlan = 23;

    private static readonly DateTime T = LiveClock.Now().Date.AddDays(-1).AddHours(10);

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static DateTime Utc(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Utc);

    private sealed record Force(Guid Request, Guid Claim, long CalledId);

    private static InvestigationAlert Alert(DateTime at, string family = "query_performance", string metric = "Blocking") =>
        new(1, ServerId, "srv-7", family, metric, at, [], null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 600) { AutonomyLevel = 3 };

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct, params object?[] values)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        foreach (var v in values)
            command.Parameters.AddWithValue(v ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return Convert.ToString(await command.ExecuteScalarAsync(ct), CultureInfo.InvariantCulture);
    }

    private static async Task<long> JournalAsync(NpgsqlConnection connection, string journalEvent, Guid request, DateTime at, JsonObject detail, CancellationToken ct, string actor = "agent:agent-a")
    {
        await using var command = new NpgsqlCommand(
            "INSERT INTO config.investigation_journal (event_time, request_id, server_id, event, actor, detail) VALUES ($1, $2, $3, $4, $5, $6::jsonb) RETURNING journal_id", connection);
        command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.TimestampTz, Utc(at));
        command.Parameters.AddWithValue(request);
        command.Parameters.AddWithValue(ServerId);
        command.Parameters.AddWithValue(journalEvent);
        command.Parameters.AddWithValue(actor);
        command.Parameters.AddWithValue(detail.ToJsonString());
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }

    private static JsonObject CalledDetail(string kind, long queryId, long planId, long? forcedBefore) => new()
    {
        ["tool"] = kind == "plan_force" ? "force_query_store_plan" : "unforce_query_store_plan",
        ["kind"] = kind,
        ["target"] = new JsonObject { ["id"] = planId.ToString(CultureInfo.InvariantCulture), ["server_id"] = ServerId, ["database"] = Database },
        ["claim_no"] = 1,
        ["before"] = new JsonObject
        {
            ["database"] = Database, ["query_id"] = queryId, ["plan_id"] = planId,
            ["requested_plan"] = new JsonObject { ["plan_id"] = planId },
            ["forced_plan"] = forcedBefore is null ? null : new JsonObject { ["plan_id"] = forcedBefore, ["forcing_type"] = "MANUAL" },
        },
    };

    /// <summary>Opens and claims the request that places the force, then journals the force: called, and done unless <paramref name="done"/> is false.</summary>
    private static async Task<Force> ForceAsync(InvestigationRequestStore store, NpgsqlConnection connection, DateTime at, CancellationToken ct,
        string kind = "plan_force", long planId = NewPlan, long? forcedBefore = null, bool done = true, string actor = "agent:agent-a")
    {
        /* One request per force: an alert joins a live request of its server and family, so each force takes a family no request holds yet. */
        var families = new[] { "blocking", "query_performance", "resources", "availability", "jobs", "self_monitor", "other" };
        var taken = new HashSet<string>();
        await using (var used = new NpgsqlCommand("SELECT DISTINCT family FROM config.investigation_requests WHERE force_check_of IS NULL", connection))
        await using (var rows = await used.ExecuteReaderAsync(ct))
        {
            while (await rows.ReadAsync(ct))
                taken.Add(rows.GetString(0));
        }

        var opened = await store.OpenOrJoinAsync(Alert(at, families.First(f => !taken.Contains(f))), ct);
        var request = opened.RequestId!.Value;
        var claim = (await store.ClaimAsync(request, "agent-a", ct)).ClaimId!.Value;
        var called = await JournalAsync(connection, "action_called", request, at, CalledDetail(kind, QueryId, planId, forcedBefore), ct, actor);
        if (done)
            await JournalAsync(connection, "action_done", request, at, new JsonObject { ["called_id"] = called, ["after"] = new JsonObject { ["state"] = "forced" } }, ct, actor);
        return new Force(request, claim, called);
    }

    private static Task PassAsync(NpgsqlConnection connection, DateTime finishedAt, CancellationToken ct, string status = "SUCCESS", int durationMs = 20_000) =>
        ExecAsync(connection,
            "INSERT INTO collect.collection_log (log_id, server_id, server_name, collector_name, collection_time, duration_ms, status, rows_collected) VALUES ($1, $2, 'srv-7', 'query_store', $3, $4, $5, 10)",
            ct, CollectionIdGenerator.Next(), ServerId, DateTime.SpecifyKind(finishedAt, DateTimeKind.Unspecified), durationMs, status);

    /// <summary>One Query Store interval of one plan, collected at <paramref name="collected"/>.</summary>
    private static Task StatsAsync(NpgsqlConnection connection, long plan, DateTime intervalStart, DateTime lastRun, DateTime collected, long executions,
        long avgCpu, long avgDuration, long maxDuration, CancellationToken ct, long intervalId = 0) =>
        ExecAsync(connection, @"INSERT INTO query_store_stats
(collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, runtime_stats_interval_id,
 interval_start_time_utc, first_execution_time, last_execution_time, execution_count, avg_cpu_time_us, avg_duration_us, max_duration_us, avg_rowcount, is_forced_plan, force_failure_count)
VALUES ($1,$2,$3,'srv-7',$4,$5,$6,'Regular',$7,$8,$8,$9,$10,$11,$12,$13,1,FALSE,0)",
            ct, CollectionIdGenerator.Next(), DateTime.SpecifyKind(collected, DateTimeKind.Unspecified), ServerId, Database, QueryId, plan,
            intervalId == 0 ? plan * 100 + intervalStart.Hour : intervalId, DateTime.SpecifyKind(intervalStart, DateTimeKind.Unspecified),
            DateTime.SpecifyKind(lastRun, DateTimeKind.Unspecified), executions, avgCpu, avgDuration, maxDuration);

    private static Task CaptureAsync(NpgsqlConnection connection, DateTime at, CancellationToken ct, bool forced = true, string? reason = null, long failures = 0)
    {
        const string sql = @"INSERT INTO collect.query_store_forced_plans
(collection_id, collection_time, server_id, server_name, database_name, plan_id, query_id, query_plan_hash, plan_forcing_type, force_failure_count, last_force_failure_reason)
VALUES ($1,$2,$3,'srv-7',$4,$5,$6,$7,$8,$9,$10)";
        var when = DateTime.SpecifyKind(at, DateTimeKind.Unspecified);
        return Task.Run(async () =>
        {
            await ExecAsync(connection, sql, ct, CollectionIdGenerator.Next(), when, ServerId, Database, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value, DBNull.Value);
            if (forced)
                await ExecAsync(connection, sql, ct, CollectionIdGenerator.Next(), when, ServerId, Database, NewPlan, QueryId, "0x16", "MANUAL", failures, reason);
        }, ct);
    }

    private static Task ArmAsync(NpgsqlConnection connection, CancellationToken ct) =>
        Task.Run(async () =>
        {
            await ExecAsync(connection, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            await ExecAsync(connection,
                "INSERT INTO config.config_monitored_servers (server_id, name, host, remediation_username, remediation_encrypted_password) VALUES (7, 'srv-7', 'host-7', 'svc_fix', 'env:FIX_PASSWORD')", ct);
            await ExecAsync(connection, "INSERT INTO config.config_server_actions (server_id, action, enabled, set_by) VALUES (7, 'force_plan', true, 'test')", ct);
        }, ct);

    private static async Task<List<(Guid Id, int No, string State)>> ChecksAsync(NpgsqlConnection connection, long calledId, CancellationToken ct)
    {
        var rows = new List<(Guid, int, string)>();
        await using var command = new NpgsqlCommand(
            $"SELECT request_id, force_check_no, state FROM config.investigation_requests WHERE force_check_of = {calledId} ORDER BY force_check_no", connection);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
            rows.Add((reader.GetGuid(0), reader.GetInt16(1), reader.GetString(2)));
        return rows;
    }

    private static async Task<JsonObject> EvidenceAsync(NpgsqlConnection connection, Guid check, CancellationToken ct) =>
        (JsonObject)JsonNode.Parse((await ScalarAsync(connection,
            $"SELECT content::text FROM config.investigation_evidence WHERE request_id = '{check}' AND section = 'plan_force_check'", ct))!)!;

    private static async Task RunAsync(Func<NpgsqlDataSource, NpgsqlConnection, Func<DateTime, InvestigationRequestStore>, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the plan-force check live tests.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await LiveTimescaleProbe.TryEnableAsync(scratch.ConnectionString, ct);
        await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, "srv-7", ct);

        await using var dataSource = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(dataSource, connection, now => new InvestigationRequestStore(dataSource, 30, () => now), ct);
    }

    [Fact]
    public async Task CheckOne_OpensAfterTheFirstSuccessfulPassThatStartedAfterTheForce_AndCheckTwoAfterTheSecond()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);

            /* A pass that finished after the force but began before it, a failed pass, and a pass before the force open nothing. */
            await PassAsync(connection, T.AddSeconds(10), ct, durationMs: 30_000);
            await PassAsync(connection, T.AddMinutes(2), ct, status: "ERROR");
            await PassAsync(connection, T.AddMinutes(-3), ct);
            Assert.Equal(0, (await at(T.AddMinutes(3)).SweepAsync(ct)).FollowUps);
            Assert.Empty(await ChecksAsync(connection, force.CalledId, ct));

            /* The first pass that began at or after the force opens check 1. */
            await PassAsync(connection, T.AddMinutes(5), ct);
            Assert.Equal(1, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            var one = Assert.Single(await ChecksAsync(connection, force.CalledId, ct));
            Assert.Equal(1, one.No);
            Assert.Equal("first_pass", (await EvidenceAsync(connection, one.Id, ct))["check"]!["trigger"]!.GetValue<string>());

            /* The second pass waits while check 1 is open, then opens check 2 once it ended. */
            await PassAsync(connection, T.AddMinutes(10), ct);
            Assert.Equal(0, (await at(T.AddMinutes(11)).SweepAsync(ct)).FollowUps);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE request_id = '{one.Id}'", ct);
            Assert.Equal(1, (await at(T.AddMinutes(12)).SweepAsync(ct)).FollowUps);
            var checks = await ChecksAsync(connection, force.CalledId, ct);
            Assert.Equal([1, 2], checks.Select(c => c.No));
            Assert.Equal("second_pass", (await EvidenceAsync(connection, checks[1].Id, ct))["check"]!["trigger"]!.GetValue<string>());

            /* The same sweep again opens nothing: one check per number. */
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE request_id = '{checks[1].Id}'", ct);
            Assert.Equal(0, (await at(T.AddMinutes(13)).SweepAsync(ct)).FollowUps);
        });
    }

    [Fact]
    public async Task HourlyChecks_OpenAtEachHour_SkipTheOnesInBetween_AndNothingOpensAfter25Hours()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            Assert.Equal(1, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE force_check_of = {force.CalledId}", ct);
            await PassAsync(connection, T.AddMinutes(10), ct);
            Assert.Equal(1, (await at(T.AddMinutes(11)).SweepAsync(ct)).FollowUps);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE force_check_of = {force.CalledId}", ct);

            /* At T + 1 h check 3 opens. */
            Assert.Equal(1, (await at(T.AddHours(1)).SweepAsync(ct)).FollowUps);
            Assert.Equal(3, (await ChecksAsync(connection, force.CalledId, ct)).Max(c => c.No));
            Assert.Equal("hourly", (await EvidenceAsync(connection, (await ChecksAsync(connection, force.CalledId, ct)).Last().Id, ct))["check"]!["trigger"]!.GetValue<string>());

            /* Check 3 stays claimed or open until T + 5 h: nothing else opens, the overdue checks are not a burst, and once it ends check 7 opens alone. */
            for (var h = 2; h <= 5; h++)
                Assert.Equal(0, (await at(T.AddHours(h)).SweepAsync(ct)).FollowUps);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE force_check_of = {force.CalledId}", ct);
            Assert.Equal(1, (await at(T.AddHours(5).AddMinutes(1)).SweepAsync(ct)).FollowUps);
            Assert.Equal([1, 2, 3, 7], (await ChecksAsync(connection, force.CalledId, ct)).Select(c => c.No));

            /* Check 26 is due at T + 24 h, and nothing opens after T + 25 h. */
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE force_check_of = {force.CalledId}", ct);
            Assert.Equal(1, (await at(T.AddHours(24)).SweepAsync(ct)).FollowUps);
            Assert.Equal(26, (await ChecksAsync(connection, force.CalledId, ct)).Max(c => c.No));
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE force_check_of = {force.CalledId}", ct);
            Assert.Equal(0, (await at(T.AddHours(25).AddMinutes(1)).SweepAsync(ct)).FollowUps);
        });
    }

    [Fact]
    public async Task AtMostOneCheckPerForce_IsOpenOrClaimedAtATime()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            await PassAsync(connection, T.AddMinutes(10), ct);
            Assert.Equal(1, (await at(T.AddMinutes(11)).SweepAsync(ct)).FollowUps);

            /* Claimed counts as open. */
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct));
            Assert.Equal(1, (await at(T.AddMinutes(11)).ClaimAsync(check.Id, "agent-b", ct) is { ClaimId: not null } ? 1 : 0));
            Assert.Equal(0, (await at(T.AddHours(3)).SweepAsync(ct)).FollowUps);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_requests WHERE force_check_of = {force.CalledId} AND state IN ('open', 'claimed')", ct));
        });
    }

    [Fact]
    public async Task ALaterUnforce_ALaterForce_AFailedForce_AnUndo_AndANonAgentForce_GiveNoFurtherChecks()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            await PassAsync(connection, T.AddMinutes(5), ct);

            /* A failed force: no action_done, so no check. */
            var failed = await ForceAsync(at(T), connection, T, ct, done: false);
            /* A bot or person actor is not an agent: no check. */
            var bot = await ForceAsync(at(T), connection, T, ct, planId: 31, actor: "service");
            /* An undone force: the schedule stops. */
            var undone = await ForceAsync(at(T), connection, T, ct, planId: 32);
            await JournalAsync(connection, "action_undone", undone.Request, T.AddMinutes(1), new JsonObject { ["called_id"] = undone.CalledId }, ct, "person:erik");
            Assert.Equal(0, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            foreach (var none in new[] { failed, bot, undone })
                Assert.Empty(await ChecksAsync(connection, none.CalledId, ct));

            /* An agent force of another query, then a later unforce of its own query: the schedule stops at the unforce. */
            var first = await ForceAsync(at(T.AddMinutes(20)), connection, T.AddMinutes(20), ct);
            await PassAsync(connection, T.AddMinutes(25), ct);
            Assert.Equal(1, (await at(T.AddMinutes(26)).SweepAsync(ct)).FollowUps);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE force_check_of = {first.CalledId}", ct);
            var unforce = await ForceAsync(at(T.AddMinutes(30)), connection, T.AddMinutes(30), ct, kind: "plan_unforce");
            Assert.NotEqual(0, unforce.CalledId);
            await PassAsync(connection, T.AddMinutes(35), ct);
            Assert.Equal(0, (await at(T.AddHours(2)).SweepAsync(ct)).FollowUps);
            Assert.Equal([1], (await ChecksAsync(connection, first.CalledId, ct)).Select(c => c.No));
        });
    }

    [Fact]
    public async Task ALaterForceOfTheSameQuery_StartsItsOwnSchedule_AndEndsTheEarlierOne()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var first = await ForceAsync(at(T), connection, T, ct);
            var second = await ForceAsync(at(T.AddMinutes(30)), connection, T.AddMinutes(30), ct, planId: 24, forcedBefore: NewPlan);
            await PassAsync(connection, T.AddMinutes(40), ct);
            Assert.Equal(1, (await at(T.AddMinutes(41)).SweepAsync(ct)).FollowUps);
            Assert.Empty(await ChecksAsync(connection, first.CalledId, ct));
            Assert.Single(await ChecksAsync(connection, second.CalledId, ct));
        });
    }

    [Fact]
    public async Task ACheck_CopiesTheForcesRequest_KeepsLevel3_IsNotAFollowUp_AndDoesNotCountAgainstTheRouteCap()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            Assert.Equal(1, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;

            Assert.Equal("1|7|3|true", await ScalarAsync(connection, $@"SELECT c.route_id || '|' || c.server_id || '|' || c.autonomy_level || '|' || (c.family = r.family)
FROM config.investigation_requests AS c JOIN config.investigation_requests AS r ON r.request_id = '{force.Request}' WHERE c.request_id = '{check}'", ct));
            Assert.Equal("0", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_requests WHERE follow_up_of IS NOT NULL", ct));
            Assert.Equal("1", await ScalarAsync(connection, $"SELECT count(*) FROM config.investigation_request_alerts WHERE request_id = '{check}'", ct));

            /* The route count: the force's request counts, the check does not (the count is the route's two window counts). */
            await using var command = new NpgsqlCommand(InvestigationRequestStore.CountRouteSql, connection);
            command.Parameters.AddWithValue(1);
            command.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.TimestampTz, Utc(T.AddMinutes(7)));
            await using var reader = await command.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal(1, reader.GetInt64(1));
        });
    }

    [Fact]
    public async Task ACheckRequest_CanBeAuthorizedToUnforce_WhenTheSwitchAndTheCredentialAreOn()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            await ArmAsync(connection, ct);
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            var store = at(T.AddMinutes(6));
            Assert.Equal(1, (await store.SweepAsync(ct)).FollowUps);
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;
            var claim = (await store.ClaimAsync(check, "agent-b", ct)).ClaimId!.Value;

            var call = new ServerActionCall("unforce_query_store_plan", ServerActionNames.ForcePlan,
                new AgentAction(check, claim, "agent-b", "the plan is not helping",
                    "[{\"tool\":\"get_forced_plans\",\"arguments\":{},\"key_numbers\":\"12\"}]"),
                new AgentActionTarget(ServerId, "srv-7", NewPlan.ToString(CultureInfo.InvariantCulture), Database));
            var hooks = new ServerActionHooks
            {
                LoadLevels = (_, _, _) => Task.FromResult<Func<string, int, PerformanceMonitor.Notifications.AlertSeverityLevel?, int?>>((_, _, _) => 3),
                ReadCredential = (serverId, token) => store.ReadServerCredentialStateAsync(dataSource, serverId, token),
            };
            var decision = await store.AuthorizeServerActionAsync(call, hooks, ct);
            Assert.NotNull(decision.Permit);
            Assert.Null(decision.Refusal);
        });
    }

    [Fact]
    public async Task TheNumbers_AreFrozenAtOpen_ForTheForcedPlan_TheOldPlan_AndAnotherPlanThatRan()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            /* Before the force: the old plan (21) ran 100 + 300 times in two intervals, plan 23 ran 50; the forced plan did not run. */
            await StatsAsync(connection, OldPlan, T.AddHours(-5), T.AddHours(-4).AddMinutes(-1), T.AddHours(-4), 100, 4000, 8000, 20_000, ct);
            await StatsAsync(connection, OldPlan, T.AddHours(-2), T.AddHours(-1), T.AddHours(-1).AddMinutes(1), 300, 8000, 12_000, 90_000, ct);
            await StatsAsync(connection, OtherPlan, T.AddHours(-3), T.AddHours(-2).AddMinutes(-1), T.AddHours(-2), 50, 100, 200, 300, ct);
            /* The open interval is stored twice: only its newest snapshot counts. */
            await StatsAsync(connection, OldPlan, T.AddMinutes(-40), T.AddMinutes(-30), T.AddMinutes(-35), 5, 1, 1, 1, ct, intervalId: 9001);
            await StatsAsync(connection, OldPlan, T.AddMinutes(-40), T.AddMinutes(-20), T.AddMinutes(-25), 10, 6000, 9000, 50_000, ct, intervalId: 9001);

            var force = await ForceAsync(at(T), connection, T, ct);
            await CaptureAsync(connection, T.AddMinutes(-4), ct, forced: false);
            await CaptureAsync(connection, T.AddMinutes(1), ct, failures: 2, reason: "GENERAL_FAILURE");

            /* After the force: the forced plan ran 40 + 60 times, and plan 23 ran 7 (the engine ran another plan: ignored). */
            await StatsAsync(connection, NewPlan, T.AddMinutes(3), T.AddMinutes(8), T.AddMinutes(9), 40, 1000, 2000, 30_000, ct, intervalId: 7001);
            await StatsAsync(connection, NewPlan, T.AddMinutes(10), T.AddMinutes(14), T.AddMinutes(15), 60, 3000, 4000, 40_000, ct, intervalId: 7002);
            await StatsAsync(connection, OtherPlan, T.AddMinutes(11), T.AddMinutes(12), T.AddMinutes(13), 7, 500, 600, 700, ct, intervalId: 7003);
            /* #6221: plan 23 is a later plan of the query, so what it is comes from its stored XML (an ordinary plan: no UsePlan). */
            await StoredPlanSeed.AddAsync(connection, ct, ServerId, Database, OtherPlan, StoredPlanSeed.Plain);
            await PassAsync(connection, T.AddMinutes(20), ct);

            Assert.Equal(1, (await at(T.AddMinutes(21)).SweepAsync(ct)).FollowUps);
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;
            var evidence = await EvidenceAsync(connection, check, ct);

            Assert.Equal("failing", evidence["state"]!.GetValue<string>());
            Assert.Equal(1, evidence["check"]!["no"]!.GetValue<int>());
            Assert.Equal(26, evidence["check"]!["of"]!.GetValue<int>());
            Assert.Equal(NewPlan, evidence["force"]!["plan_id"]!.GetValue<long>());
            Assert.Equal(2, evidence["failures"]!["force_failure_count"]!.GetValue<int>());
            Assert.Equal("GENERAL_FAILURE", evidence["failures"]!["last_force_failure_reason"]!.GetValue<string>());

            var forced = evidence["since_force"]!["forced_plan"]!;
            Assert.Equal(100, forced["executions"]!.GetValue<long>());
            /* (1000 * 40 + 3000 * 60) / 100 = 2200 us CPU, (2000 * 40 + 4000 * 60) / 100 = 3200 us duration, max 40,000 us. */
            Assert.Equal(2.2, forced["avg_cpu_ms"]!.GetValue<double>(), 3);
            Assert.Equal(3.2, forced["avg_duration_ms"]!.GetValue<double>(), 3);
            Assert.Equal(40.0, forced["max_duration_ms"]!.GetValue<double>(), 3);
            Assert.True(evidence["since_force"]!["other_plans_ran"]!.GetValue<bool>());
            Assert.Equal(7, evidence["since_force"]!["plans"]!.AsArray().Single(p => p!["plan_id"]!.GetValue<long>() == OtherPlan)!["executions"]!.GetValue<long>());

            var beforeForce = evidence["before_force"]!;
            Assert.Equal(OldPlan, beforeForce["old_plan_id"]!.GetValue<long>());
            Assert.Equal("most_executions", beforeForce["old_plan_reason"]!.GetValue<string>());
            var old = beforeForce["old_plan"]!;
            /* 100 + 300 + 10 (the open interval's newest snapshot, not its first) = 410 executions. */
            Assert.Equal(410, old["executions"]!.GetValue<long>());
            Assert.Equal((4000.0 * 100 + 8000 * 300 + 6000 * 10) / 410 / 1000, old["avg_cpu_ms"]!.GetValue<double>(), 3);
            Assert.Equal(90.0, old["max_duration_ms"]!.GetValue<double>(), 3);
            Assert.NotNull(evidence["duration_gap"]!["since_force_max_over_avg"]);
            Assert.Contains("interval", evidence["interval_caveat"]!.GetValue<string>(), StringComparison.Ordinal);

            /* Frozen: the numbers do not move when more stats arrive. */
            await StatsAsync(connection, NewPlan, T.AddMinutes(16), T.AddMinutes(17), T.AddMinutes(18), 999, 1, 1, 1, ct, intervalId: 7004);
            Assert.Equal(100, (await EvidenceAsync(connection, check, ct))["since_force"]!["forced_plan"]!["executions"]!.GetValue<long>());
        });
    }

    /// <summary>#6221: a later plan of the forced query ran after the force, and its XML is stored or not, as the Query Store collector would have it.</summary>
    private static async Task<(Force Force, JsonObject Evidence)> CheckWithALaterPlanAsync(NpgsqlConnection connection, Func<DateTime, InvestigationRequestStore> at, string? storedXml, CancellationToken ct)
    {
        var force = await ForceAsync(at(T), connection, T, ct);
        await CaptureAsync(connection, T.AddMinutes(1), ct);
        await StatsAsync(connection, NewPlan, T.AddMinutes(3), T.AddMinutes(8), T.AddMinutes(9), 5, 1000, 2000, 30_000, ct, intervalId: 7001);
        await StatsAsync(connection, OtherPlan, T.AddMinutes(11), T.AddMinutes(12), T.AddMinutes(13), 40, 500, 600, 700, ct, intervalId: 7003);
        if (storedXml is not null) await StoredPlanSeed.AddAsync(connection, ct, ServerId, Database, OtherPlan, storedXml);
        await PassAsync(connection, T.AddMinutes(20), ct);
        Assert.Equal(1, (await at(T.AddMinutes(21)).SweepAsync(ct)).FollowUps);
        var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;
        return (force, await EvidenceAsync(connection, check, ct));
    }

    [Fact]
    public async Task ALaterPlanWhoseXmlIsNotStored_MakesTheCheckUnknown_NotIgnored_AndTheScheduleKeepsGoing()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var (force, evidence) = await CheckWithALaterPlanAsync(connection, at, null, ct);

            Assert.Equal("unknown_plan_not_collected", evidence["state"]!.GetValue<string>());
            Assert.False(evidence["since_force"]!["other_plans_ran"]!.GetValue<bool>());
            Assert.True(evidence["since_force"]!["unknown_plans_ran"]!.GetValue<bool>());
            Assert.Contains("23", evidence["since_force"]!["unknown_reason"]!.GetValue<string>(), StringComparison.Ordinal);
            var unknown = evidence["since_force"]!["plans"]!.AsArray().Single(p => p!["plan_id"]!.GetValue<long>() == OtherPlan)!;
            Assert.True(unknown["plan_xml_not_stored"]!.GetValue<bool>());

            /* An unknown does not close the schedule (only a removed force does): the next pass opens check 2, and by then the XML may be stored. */
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE force_check_of = {force.CalledId}", ct);
            await StoredPlanSeed.AddAsync(connection, ct, ServerId, Database, OtherPlan, StoredPlanSeed.Variant);
            await PassAsync(connection, T.AddMinutes(30), ct);
            Assert.Equal(1, (await at(T.AddMinutes(31)).SweepAsync(ct)).FollowUps);
            var second = (await ChecksAsync(connection, force.CalledId, ct)).Last();
            Assert.Equal(2, second.No);
            Assert.Equal("working", (await EvidenceAsync(connection, second.Id, ct))["state"]!.GetValue<string>());
        });
    }

    [Fact]
    public async Task AnIgnoredForce_ShowsItsOtherPlan_EvenWhenMoreUnknownLaterPlansRanThanTheCheckShows()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await CaptureAsync(connection, T.AddMinutes(1), ct);
            /* The plan forced before ran again after the force: a definite other plan, so the force is IGNORED. */
            await StatsAsync(connection, OldPlan, T.AddMinutes(3), T.AddMinutes(4), T.AddMinutes(5), 50, 900, 1000, 1100, ct, intervalId: 7101);
            /* More later plans with no stored XML ran than the check shows: they must not push the other plan out of the evidence. */
            for (var i = 0; i < ForceCheckPlansShownForTest + 2; i++)
                await StatsAsync(connection, 100 + i, T.AddMinutes(6), T.AddMinutes(7), T.AddMinutes(8), 10 + i, 100, 200, 300, ct, intervalId: 7200 + i);
            await PassAsync(connection, T.AddMinutes(20), ct);
            Assert.Equal(1, (await at(T.AddMinutes(21)).SweepAsync(ct)).FollowUps);
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;
            var evidence = await EvidenceAsync(connection, check, ct);

            Assert.Equal("ignored", evidence["state"]!.GetValue<string>());
            Assert.True(evidence["since_force"]!["other_plans_ran"]!.GetValue<bool>());
            Assert.True(evidence["since_force"]!["unknown_plans_ran"]!.GetValue<bool>());
            var plans = evidence["since_force"]!["plans"]!.AsArray();
            Assert.Contains(plans, p => p!["plan_id"]!.GetValue<long>() == OldPlan);
            Assert.True(plans.Count <= ForceCheckPlansShownForTest);
        });
    }

    private const int ForceCheckPlansShownForTest = InvestigationRequestStore.ForceCheckPlansShown;

    [Fact]
    public async Task ALaterPlanWhoseStoredXmlIsAVariant_MakesTheCheckWorking_AndMarksTheVariant()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var (_, evidence) = await CheckWithALaterPlanAsync(connection, at, StoredPlanSeed.Variant, ct);

            Assert.Equal("working", evidence["state"]!.GetValue<string>());
            Assert.False(evidence["since_force"]!["other_plans_ran"]!.GetValue<bool>());
            Assert.True(evidence["since_force"]!["variant_plans_ran"]!.GetValue<bool>());
            Assert.False(evidence["since_force"]!["unknown_plans_ran"]!.GetValue<bool>());
            Assert.True(evidence["since_force"]!["plans"]!.AsArray().Single(p => p!["plan_id"]!.GetValue<long>() == OtherPlan)!["variant_of_forced"]!.GetValue<bool>());
        });
    }

    [Fact]
    public async Task TheOldPlan_IsThePlanForcedBefore_WhenThereWasOne_AndAForceTheCaptureShowsRemoved_EndsTheSchedule()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            await StatsAsync(connection, OtherPlan, T.AddHours(-3), T.AddHours(-2), T.AddHours(-2), 900, 1, 1, 1, ct);
            await StatsAsync(connection, OldPlan, T.AddHours(-4), T.AddHours(-3), T.AddHours(-3), 20, 1, 1, 1, ct);
            var force = await ForceAsync(at(T), connection, T, ct, forcedBefore: OldPlan);
            /* The force was captured, then a later capture of the database no longer lists it: removed. */
            await CaptureAsync(connection, T.AddMinutes(1), ct);
            await CaptureAsync(connection, T.AddMinutes(6), ct, forced: false);
            await PassAsync(connection, T.AddMinutes(8), ct);

            Assert.Equal(1, (await at(T.AddMinutes(9)).SweepAsync(ct)).FollowUps);
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;
            var evidence = await EvidenceAsync(connection, check, ct);
            Assert.Equal("removed", evidence["state"]!.GetValue<string>());
            Assert.Equal(OldPlan, evidence["before_force"]!["old_plan_id"]!.GetValue<long>());
            Assert.Equal("forced_before", evidence["before_force"]!["old_plan_reason"]!.GetValue<string>());

            /* The removed check is the last one. */
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported' WHERE request_id = '{check}'", ct);
            await PassAsync(connection, T.AddMinutes(10), ct);
            Assert.Equal(0, (await at(T.AddHours(2)).SweepAsync(ct)).FollowUps);
        });
    }

    [Fact]
    public async Task EachCheck_JournalsOneFollowUpEvent_WithItsDetailMembers_AndAReplayWritesNoSecond()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            Assert.Equal(1, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;

            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE request_id = '{check}' AND event = 'follow_up' AND once_key = 'force_check:{force.CalledId}:1'", ct));
            var detail = await ScalarAsync(connection, $"SELECT detail::text FROM config.investigation_journal WHERE request_id = '{check}' AND event = 'follow_up'", ct);
            var published = InvestigationEvents.PublicDetail(InvestigationEvents.FollowUp, detail)!;
            using var document = JsonDocument.Parse(published);
            Assert.Equal(force.CalledId, document.RootElement.GetProperty("force_check_of").GetInt64());
            Assert.Equal(1, document.RootElement.GetProperty("check_no").GetInt32());
            Assert.Equal("force_check", document.RootElement.GetProperty("trigger").GetString());

            /* A replay of the open: the unique pair refuses a second request, and the once key a second event. */
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'expired' WHERE request_id = '{check}'", ct);
            await ExecAsync(connection, $"DELETE FROM config.investigation_requests WHERE request_id = '{check}'", ct);
            Assert.Equal(1, (await at(T.AddMinutes(8)).SweepAsync(ct)).FollowUps);
            Assert.Equal("1", await ScalarAsync(connection,
                $"SELECT count(*) FROM config.investigation_journal WHERE event = 'follow_up' AND once_key LIKE 'force_check:{force.CalledId}:%'", ct));
        });
    }

    [Fact]
    public async Task TheBriefSourceReader_FillsTheForceCheck_ForACheckRequest_AndNullForAnyOther()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            Assert.Equal(1, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            var check = Assert.Single(await ChecksAsync(connection, force.CalledId, ct)).Id;
            var reader = new PerformanceMonitor.Darling.Service.Investigations.PgInvestigationBriefSourceReader(dataSource, utcNow: () => T.AddMinutes(7));

            var forCheck = (await reader.ReadAsync(check, ct))!.Request.ForceCheck;
            Assert.NotNull(forCheck);
            Assert.Equal(force.CalledId, forCheck!.CalledId);
            Assert.Equal(1, forCheck.CheckNo);
            Assert.Equal(Database, forCheck.Database);
            Assert.Equal(QueryId, forCheck.QueryId);
            Assert.Equal(NewPlan, forCheck.PlanId);
            Assert.Equal(force.Request, forCheck.ForceRequestId);
            Assert.Equal(T, forCheck.ForcedAtUtc);

            Assert.Null((await reader.ReadAsync(force.Request, ct))!.Request.ForceCheck);
        });
    }

    [Fact]
    public async Task TheAgentChangesList_MarksAForce_AnsweredOf_NotReviewedYet_AndNotReviewed()
    {
        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            await PassAsync(connection, T.AddMinutes(10), ct);
            string Review(AgentChangesRead read) =>
                (string)PerformanceMonitor.Darling.Service.DarlingWebEndpoints.AgentChangesBody(read, true, T)["changes"]!.AsArray()
                    .Single(c => (long)c!["called_id"]! == force.CalledId)!["force_review"]!;
            async Task<AgentChangesRead> ReadAt(DateTime now) => await new AgentChangeUndo(dataSource, 30, () => now).ReadAsync(200, cancellationToken: ct);

            /* The schedule runs and no check is opened: not reviewed yet. */
            Assert.Equal("not reviewed yet", Review(await ReadAt(T.AddMinutes(3))));

            /* One check opened and not answered: still not reviewed yet. */
            Assert.Equal(1, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            Assert.Equal("not reviewed yet", Review(await ReadAt(T.AddMinutes(7))));

            /* One answered, one opened: "checks answered 1 of 2". The report stamps reported_at, as the submit path does. */
            var reportedAt = T.AddMinutes(8).ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'reported', reported_at = '{reportedAt}+00' WHERE force_check_of = {force.CalledId}", ct);
            Assert.Equal(1, (await at(T.AddMinutes(11)).SweepAsync(ct)).FollowUps);
            Assert.Equal("checks answered 1 of 2", Review(await ReadAt(T.AddMinutes(12))));

            /* A person closes the answered check in the Viewer (state closed, reported_at kept): it was still answered. */
            await ExecAsync(connection, $"UPDATE config.investigation_requests SET state = 'closed', closed_at = now() WHERE force_check_of = {force.CalledId} AND reported_at IS NOT NULL", ct);
            Assert.Equal("checks answered 1 of 2", Review(await ReadAt(T.AddMinutes(13))));

            /* The schedule ended (past T + 25 h) with one answered: the mark still counts it. */
            Assert.Equal("checks answered 1 of 2", Review(await ReadAt(T.AddHours(26))));
        });

        await RunAsync(async (dataSource, connection, at, ct) =>
        {
            var force = await ForceAsync(at(T), connection, T, ct);
            await PassAsync(connection, T.AddMinutes(5), ct);
            Assert.Equal(1, (await at(T.AddMinutes(6)).SweepAsync(ct)).FollowUps);
            string Review(AgentChangesRead read) =>
                (string)PerformanceMonitor.Darling.Service.DarlingWebEndpoints.AgentChangesBody(read, true, T)["changes"]!.AsArray()
                    .Single(c => (long)c!["called_id"]! == force.CalledId)!["force_review"]!;

            /* Nothing answered and the schedule ended: not reviewed. */
            Assert.Equal("not reviewed", Review(await new AgentChangeUndo(dataSource, 30, () => T.AddHours(26)).ReadAsync(200, cancellationToken: ct)));
            /* Nothing answered and an unforce of the query was done: the schedule stopped, so not reviewed. */
            await ForceAsync(at(T.AddMinutes(30)), connection, T.AddMinutes(30), ct, kind: "plan_unforce");
            Assert.Equal("not reviewed", Review(await new AgentChangeUndo(dataSource, 30, () => T.AddHours(2)).ReadAsync(200, cancellationToken: ct)));
        });
    }

    [Fact]
    public void TheCheckSchedule_FollowsTheRules()
    {
        var t = new DateTime(2026, 10, 18, 10, 0, 0, DateTimeKind.Utc);
        DateTime[] passes(int n) => Enumerable.Range(0, n).Select(i => t.AddMinutes(5 * (i + 1))).ToArray();
        Assert.Null(InvestigationRequestStore.DueForceCheck(t, t.AddMinutes(3), passes(0), 0));
        Assert.Equal((1, "first_pass"), InvestigationRequestStore.DueForceCheck(t, t.AddMinutes(6), passes(1), 0));
        Assert.Equal((2, "second_pass"), InvestigationRequestStore.DueForceCheck(t, t.AddMinutes(11), passes(2), 1));
        Assert.Equal((3, "hourly"), InvestigationRequestStore.DueForceCheck(t, t.AddHours(1), passes(2), 2));
        Assert.Equal((7, "hourly"), InvestigationRequestStore.DueForceCheck(t, t.AddHours(5), passes(2), 3));
        Assert.Equal((26, "hourly"), InvestigationRequestStore.DueForceCheck(t, t.AddHours(24), passes(2), 7));
        Assert.Null(InvestigationRequestStore.DueForceCheck(t, t.AddHours(24), passes(2), 26));
        Assert.Null(InvestigationRequestStore.DueForceCheck(t, t.AddHours(25).AddMinutes(1), passes(2), 7));
    }
}
