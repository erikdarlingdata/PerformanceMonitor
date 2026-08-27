/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The control-plane Stage 2 command executor. The pure tests pin the two correctness-critical pieces with
/// no database — the atomic-claim SQL shape (FOR UPDATE SKIP LOCKED, oldest-pending, mark in_progress) and
/// the PURE dispatch (<see cref="DarlingCommandExecutor.ResolvePlan"/>: one plan per command_type, argument
/// validation, unknown -> fail) — plus the test_connect result mapping. The live tests (gated on
/// DARLING_TEST_PG) prove the claim/execute/report round-trip against real Postgres and are transaction-
/// rolled-back (or sentinel-scoped + cleaned up) so they never clobber a shared dev store's singleton rows.
/// </summary>
/* Live-fixture tests share one Postgres store; the collection serializes them so cross-test row churn
   (inserts/deletes on config_command / config_collector_schedules) cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class DarlingCommandExecutorTests
{
    /// <summary>A fake host — the store-write / probe branches never call it; asserts if they do.</summary>
    private sealed class ThrowingHost : IDarlingCommandHost
    {

        /* #2612: not exercised by these dispatch tests — the pin that the command REACHES the host
           lives beside the other worker-delegated verbs. */
        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");
        public Task<CommandOutcome> SnapshotNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");
    }

    /// <summary>Records the custom-retention argument a <c>purge_now</c> dispatch passed the host — for the
    /// custom-N-vs-default parsing round-trip (no live store or SQL Server).</summary>
    private sealed class PurgeRecordingHost : IDarlingCommandHost
    {

        /* #2612: not exercised by these dispatch tests — the pin that the command REACHES the host
           lives beside the other worker-delegated verbs. */
        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");
        public bool WasCalled { get; private set; }
        public int? ReceivedCustomRetentionDays { get; private set; }

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
        {
            WasCalled = true;
            ReceivedCustomRetentionDays = customRetentionDays;
            return Task.FromResult(new CommandOutcome(true, "purge complete",
                "{\"success\":true,\"tablesPurged\":3,\"rowsPurged\":42}"));
        }

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");
    }

    /// <summary>A host that records the fetch_plan request it received and returns a canned plan — for the
    /// fetch_plan claim/execute/report round-trip (the store path, without a live SQL Server).</summary>
    private sealed class PlanReturningHost : IDarlingCommandHost
    {

        /* #2612: not exercised by these dispatch tests — the pin that the command REACHES the host
           lives beside the other worker-delegated verbs. */
        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");
        public PlanFetchRequest? Received { get; private set; }
        public int ReceivedServerId { get; private set; }

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
        {
            Received = request;
            ReceivedServerId = serverId;
            return Task.FromResult(new CommandOutcome(true, "plan fetched", "{\"success\":true,\"planXml\":\"<ShowPlanXML/>\"}"));
        }

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");
    }

    /// <summary>A host that records the fetch_active_queries call it received and returns a canned rows payload —
    /// for the fetch_active_queries claim/execute/report round-trip (the store path, without a live SQL Server).</summary>
    private sealed class ActiveQueriesReturningHost : IDarlingCommandHost
    {

        /* #2612: not exercised by these dispatch tests — the pin that the command REACHES the host
           lives beside the other worker-delegated verbs. */
        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("host should not be called for this command");
        public int ReceivedServerId { get; private set; } = int.MinValue;

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
        {
            ReceivedServerId = serverId;
            return Task.FromResult(new CommandOutcome(true, "active queries fetched",
                "{\"success\":true,\"fetchedAtUtc\":\"2026-07-13T12:00:00Z\",\"rows\":[{\"SessionId\":57,\"QueryText\":\"SELECT 1\"}]}"));
        }
    }

    private static ClaimedCommand Command(string type, int? target = null, string? args = null) =>
        new(1, type, target, args, "tester");

    /* ---------------- pure: the atomic-claim SQL shape (pin) ---------------- */

    [Fact]
    public void ClaimCommandSql_IsAtomicOldestPendingSkipLocked()
    {
        var sql = DarlingCommandExecutor.ClaimCommandSql;

        /* Multi-instance safety: the sub-select locks the chosen row and SKIPs rows another instance holds. */
        Assert.Contains("FOR UPDATE SKIP LOCKED", sql, StringComparison.Ordinal);
        /* Claims only pending rows and marks the claimed one in_progress (so it is never re-claimed). */
        Assert.Contains("status = 'pending'", sql, StringComparison.Ordinal);
        Assert.Contains("status = 'in_progress'", sql, StringComparison.Ordinal);
        /* FIFO, exactly one, and returns the row so the executor can dispatch it. */
        Assert.Contains("ORDER BY command_id", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT 1", sql, StringComparison.Ordinal);
        Assert.Contains("RETURNING", sql, StringComparison.Ordinal);
        /* Records which instance claimed it. */
        Assert.Contains("service_instance = $1", sql, StringComparison.Ordinal);
    }

    /* ---------------- pure: reaper + terminal secret-scrub SQL shape (pin) ---------------- */

    [Fact]
    public void ReclaimStaleCommandsSql_MarksFailed_NullsArgs_FiltersStaleInProgress()
    {
        var sql = DarlingCommandExecutor.ReclaimStaleCommandsSql;

        /* Reclaim = terminal failed with a clear reason (so the Viewer's poll completes) — NOT re-queued to
           pending (which would re-run a non-idempotent snapshot_now or loop on a poison command). */
        Assert.Contains("status = 'failed'", sql, StringComparison.Ordinal);
        Assert.Contains("result_status = 'reclaimed", sql, StringComparison.Ordinal);
        /* Terminal secret scrub: a reclaimed test_connect's credential args are nulled too. */
        Assert.Contains("args_json = NULL", sql, StringComparison.Ordinal);
        /* Only genuinely-abandoned rows: in_progress AND claimed before the parameterized threshold. */
        Assert.Contains("WHERE status = 'in_progress'", sql, StringComparison.Ordinal);
        Assert.Contains("claimed_at <", sql, StringComparison.Ordinal);
        Assert.Contains("$1", sql, StringComparison.Ordinal);

        /* A generous margin over the slowest command (test_connect ~15-30s, analyze_now caps 120s), so the
           reaper can never catch a command that is merely slow. */
        Assert.Equal(TimeSpan.FromMinutes(5), DarlingCommandExecutor.StaleCommandTimeout);
    }

    [Fact]
    public void ReportCommandSql_NullsArgsJson_OnTerminalState()
    {
        /* The terminal report scrubs the credential-carrying args_json (#1262 Low retention follow-up); the
           Viewer reads result_status/result_json on completion, never args, so this is transparent. */
        Assert.Contains("args_json = NULL", DarlingCommandExecutor.ReportCommandSql, StringComparison.Ordinal);
    }

    /* ---------------- pure: dispatch (one plan per command_type) ---------------- */

    [Fact]
    public void ResolvePlan_PauseResume_WriteConfigServicePaused()
    {
        var pause = DarlingCommandExecutor.ResolvePlan(Command("pause"));
        Assert.Equal(CommandKind.StoreWrite, pause.Kind);
        Assert.Contains("config_service", pause.Sql, StringComparison.Ordinal);
        Assert.Contains("paused = TRUE", pause.Sql, StringComparison.Ordinal);
        Assert.Equal("paused", pause.SuccessStatus);

        var resume = DarlingCommandExecutor.ResolvePlan(Command("resume"));
        Assert.Equal(CommandKind.StoreWrite, resume.Kind);
        Assert.Contains("paused = FALSE", resume.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolvePlan_Reload_BumpsConfigServiceWithoutStateChange()
    {
        var plan = DarlingCommandExecutor.ResolvePlan(Command("reload"));
        Assert.Equal(CommandKind.StoreWrite, plan.Kind);
        Assert.Contains("UPDATE config.config_service", plan.Sql, StringComparison.Ordinal);
        /* Only touches updated_at — the self-bump trigger increments config_version. */
        Assert.DoesNotContain("paused", plan.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ResolvePlan_EnableDisableServer_RequireTarget_ThenParameterizeIt()
    {
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("enable_server")).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("disable_server")).Kind);

        var enable = DarlingCommandExecutor.ResolvePlan(Command("enable_server", target: 42));
        Assert.Equal(CommandKind.StoreWrite, enable.Kind);
        Assert.Contains("is_enabled = TRUE", enable.Sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1", enable.Sql, StringComparison.Ordinal);
        Assert.Equal(42, enable.Parameters![0]);

        var disable = DarlingCommandExecutor.ResolvePlan(Command("disable_server", target: 42));
        Assert.Contains("is_enabled = FALSE", disable.Sql, StringComparison.Ordinal);
        Assert.Equal(42, disable.Parameters![0]);
    }

    [Fact]
    public void ResolvePlan_EnableCollector_FleetVsPerServer_ValidatesName()
    {
        /* Missing collector_name -> fail. */
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("enable_collector", args: "{}")).Kind);

        /* Unknown collector -> fail (no junk override row). */
        Assert.Equal(CommandKind.Fail,
            DarlingCommandExecutor.ResolvePlan(Command("enable_collector", args: "{\"collector_name\":\"not_a_collector\"}")).Kind);

        /* Fleet-wide (no server scope) -> the server_id IS NULL partial-index arbiter, one param (the name). */
        var fleet = DarlingCommandExecutor.ResolvePlan(Command("enable_collector", args: "{\"collector_name\":\"wait_stats\"}"));
        Assert.Equal(CommandKind.StoreWrite, fleet.Kind);
        Assert.Contains("VALUES (NULL, $1, TRUE)", fleet.Sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (collector_name) WHERE server_id IS NULL", fleet.Sql, StringComparison.Ordinal);
        Assert.Single(fleet.Parameters!);
        Assert.Equal("wait_stats", fleet.Parameters![0]);

        /* Per-server (target scope) -> the (server_id, collector_name) arbiter, two params. */
        var perServer = DarlingCommandExecutor.ResolvePlan(Command("disable_collector", target: 7, args: "{\"collector_name\":\"wait_stats\"}"));
        Assert.Contains("VALUES ($1, $2, FALSE)", perServer.Sql, StringComparison.Ordinal);
        Assert.Contains("ON CONFLICT (server_id, collector_name) WHERE server_id IS NOT NULL", perServer.Sql, StringComparison.Ordinal);
        Assert.Equal(7, perServer.Parameters![0]);
        Assert.Equal("wait_stats", perServer.Parameters![1]);
    }

    [Fact]
    public void ResolvePlan_TestConnect_RequiresArgsJson_ElseProbe()
    {
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("test_connect")).Kind);
        Assert.Equal(CommandKind.Probe, DarlingCommandExecutor.ResolvePlan(Command("test_connect", args: "{\"host\":\"sql01\"}")).Kind);
    }

    [Fact]
    public void ResolvePlan_SnapshotAndAnalyze_RequireTarget()
    {
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("snapshot_now")).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("analyze_now")).Kind);
        Assert.Equal(CommandKind.Snapshot, DarlingCommandExecutor.ResolvePlan(Command("snapshot_now", target: 3)).Kind);
        Assert.Equal(CommandKind.Analyze, DarlingCommandExecutor.ResolvePlan(Command("analyze_now", target: 3)).Kind);
    }

    [Fact]
    public void ResolvePlan_PurgeNow_ResolvesToPurge_WithNoTargetAndOptionalArgs()
    {
        /* Fleet-wide over the shared tables — NO target_server_id required (unlike snapshot_now/analyze_now). */
        Assert.Equal(CommandKind.Purge, DarlingCommandExecutor.ResolvePlan(Command("purge_now")).Kind);

        /* An optional custom-retention arg doesn't change the plan kind (it's read at execution time). */
        Assert.Equal(CommandKind.Purge,
            DarlingCommandExecutor.ResolvePlan(Command("purge_now", args: "{\"retention_days\":7}")).Kind);

        /* Case-insensitive like every other command_type. */
        Assert.Equal(CommandKind.Purge, DarlingCommandExecutor.ResolvePlan(Command("Purge_Now")).Kind);
    }

    [Fact]
    public async Task ExecuteAndReport_PurgeNow_PassesCustomRetentionToHost_ElseNull()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the purge_now dispatch round-trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);

        /* Custom-N days -> the host receives the parsed value. Build the args with the VIEWER's own builder so
           this pins the two ends agree on the retention_days key, not just a hand-written literal. */
        var customHost = new PurgeRecordingHost();
        var customExecutor = new DarlingCommandExecutor(postgres, customHost, "test-instance", null);
        await customExecutor.ExecuteAndReportAsync(
            new ClaimedCommand(-1, "purge_now", null, PerformanceMonitor.Darling.Viewer.ViewerDataService.BuildPurgeNowArgs(7), "sentinel-test"), ct);
        Assert.True(customHost.WasCalled);
        Assert.Equal(7, customHost.ReceivedCustomRetentionDays);

        /* No args -> the host receives null (the service falls back to the configured fleet horizons). */
        var defaultHost = new PurgeRecordingHost();
        var defaultExecutor = new DarlingCommandExecutor(postgres, defaultHost, "test-instance", null);
        await defaultExecutor.ExecuteAndReportAsync(
            new ClaimedCommand(-1, "purge_now", null, null, "sentinel-test"), ct);
        Assert.True(defaultHost.WasCalled);
        Assert.Null(defaultHost.ReceivedCustomRetentionDays);

        using (var cleanup = new NpgsqlCommand("DELETE FROM config.config_command WHERE requested_by = 'sentinel-test'", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    [Fact]
    public void ResolvePlan_FetchPlan_RequiresTargetAndAHandleInArgs()
    {
        /* No target -> fail (needs a server whose live cache to read). */
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("fetch_plan")).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("fetch_plan", args: "{\"planHandle\":\"0x06\"}")).Kind);

        /* Target but no args, or args without any handle -> fail (nothing to fetch). */
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("fetch_plan", target: 7)).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("fetch_plan", target: 7, args: "{}")).Kind);
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("fetch_plan", target: 7, args: "{\"databaseName\":\"tempdb\"}")).Kind);

        /* Target + a plan_handle (query-grid path) -> FetchPlan. */
        Assert.Equal(CommandKind.FetchPlan,
            DarlingCommandExecutor.ResolvePlan(Command("fetch_plan", target: 7, args: "{\"planHandle\":\"0x06000100\"}")).Kind);

        /* Target + a sql_handle (deadlock / blocked path) -> FetchPlan. */
        Assert.Equal(CommandKind.FetchPlan,
            DarlingCommandExecutor.ResolvePlan(Command("fetch_plan", target: 7, args: "{\"sqlHandle\":\"0x0200\",\"statementStartOffset\":0,\"statementEndOffset\":-1}")).Kind);
    }

    [Fact]
    public void ResolvePlan_FetchPlan_IsCaseInsensitive()
    {
        Assert.Equal(CommandKind.FetchPlan,
            DarlingCommandExecutor.ResolvePlan(Command("Fetch_Plan", target: 1, args: "{\"planHandle\":\"0x06\"}")).Kind);
    }

    [Fact]
    public void ResolvePlan_FetchActiveQueries_RequiresTarget_NeedsNoArgs()
    {
        /* No target -> fail (needs a server whose LIVE DMVs to read). */
        Assert.Equal(CommandKind.Fail, DarlingCommandExecutor.ResolvePlan(Command("fetch_active_queries")).Kind);

        /* Target present -> FetchActiveQueries, with NO args required (the whole request is "this server, now"). */
        var plan = DarlingCommandExecutor.ResolvePlan(Command("fetch_active_queries", target: 7));
        Assert.Equal(CommandKind.FetchActiveQueries, plan.Kind);
        Assert.Equal("active queries fetched", plan.SuccessStatus);

        /* Case-insensitive like every other command_type. */
        Assert.Equal(CommandKind.FetchActiveQueries,
            DarlingCommandExecutor.ResolvePlan(Command("Fetch_Active_Queries", target: 7)).Kind);
    }

    /// <summary>
    /// Pins the command constant viewer↔executor: the viewer's <see cref="ViewerDataService.CommandFetchActiveQueries"/>
    /// (what it enqueues) must resolve to <see cref="CommandKind.FetchActiveQueries"/> in the service executor, so the
    /// two ends can never drift apart (the same guarantee the fetch_plan / purge_now round-trips give their constants).
    /// </summary>
    [Fact]
    public void CommandFetchActiveQueries_ConstantMatchesExecutorCase()
    {
        var plan = DarlingCommandExecutor.ResolvePlan(
            Command(PerformanceMonitor.Darling.Viewer.ViewerDataService.CommandFetchActiveQueries, target: 7));
        Assert.Equal(CommandKind.FetchActiveQueries, plan.Kind);
    }

    [Theory]
    [InlineData("bogus")]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("PAUSE_NOW")]
    public void ResolvePlan_UnknownType_FailsWithClearReason(string type)
    {
        var plan = DarlingCommandExecutor.ResolvePlan(Command(type));
        Assert.Equal(CommandKind.Fail, plan.Kind);
        Assert.Equal("unknown command_type", plan.FailReason);
    }

    [Fact]
    public void ResolvePlan_CommandTypeIsCaseInsensitive()
    {
        Assert.Equal(CommandKind.StoreWrite, DarlingCommandExecutor.ResolvePlan(Command("PAUSE")).Kind);
        Assert.Equal(CommandKind.Snapshot, DarlingCommandExecutor.ResolvePlan(Command("Snapshot_Now", target: 1)).Kind);
    }

    /* ---------------- pure: test_connect result mapping ---------------- */

    [Fact]
    public void MapProbeResult_Success_CarriesTheProbeFacts()
    {
        var probe = new ConnectionProbeResult(
            Success: true, MajorVersion: 16, EngineEdition: 3, EngineEditionDescription: "Enterprise",
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null);

        var (status, json) = DarlingCommandExecutor.MapProbeResult(probe);

        Assert.Equal("connected", status);
        Assert.Contains("\"success\":true", json, StringComparison.Ordinal);
        Assert.Contains("\"majorVersion\":16", json, StringComparison.Ordinal);
        Assert.Contains("\"engineEdition\":3", json, StringComparison.Ordinal);
        Assert.Contains("Enterprise", json, StringComparison.Ordinal);
        Assert.Contains("\"hasMsdbAccess\":true", json, StringComparison.Ordinal);
    }

    [Fact]
    public void MapProbeResult_Failure_CarriesTheError_NoFacts()
    {
        var probe = new ConnectionProbeResult(
            Success: false, MajorVersion: 0, EngineEdition: 0, EngineEditionDescription: null,
            IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: false,
            Error: "A network-related or instance-specific error occurred");

        var (status, json) = DarlingCommandExecutor.MapProbeResult(probe);

        Assert.Equal("connection_failed", status);
        Assert.Contains("\"success\":false", json, StringComparison.Ordinal);
        Assert.Contains("network-related", json, StringComparison.Ordinal);
    }

    /* ---------------- live (DARLING_TEST_PG): claim + execute + report ---------------- */

    /// <summary>Distinctive sentinel — a real server_id is a storage-name hash, never this.</summary>
    private const int SentinelServerId = -515151;

    [Fact]
    public async Task ClaimCommandSql_ClaimsOldestPendingFifo_MarksInProgress_RolledBack()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the command-claim test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var tx = await connection.BeginTransactionAsync(ct);

        /* Neutralize any pre-existing pending rows WITHIN this rolled-back tx so the claim sees only ours. */
        await ExecAsync(connection, tx, "UPDATE config.config_command SET status = 'in_progress' WHERE status = 'pending'", ct);

        await ExecAsync(connection, tx,
            "INSERT INTO config.config_command (command_type, status) VALUES ('reload', 'pending'), ('pause', 'pending')", ct);

        /* First claim -> the OLDEST pending (reload), marked in_progress. */
        var first = await ClaimAsync(connection, tx, ct);
        Assert.NotNull(first);
        Assert.Equal("reload", first!.Value.Type);

        var second = await ClaimAsync(connection, tx, ct);
        Assert.NotNull(second);
        Assert.Equal("pause", second!.Value.Type);
        Assert.True(second.Value.Id > first.Value.Id, "second claim must be a later command_id (FIFO)");

        /* Queue drained -> nothing left to claim. */
        var third = await ClaimAsync(connection, tx, ct);
        Assert.Null(third);

        /* Both are now in_progress (claimed), none still pending. */
        var pending = Convert.ToInt64(await ScalarAsync(connection, tx,
            $"SELECT COUNT(*) FROM config.config_command WHERE command_id IN ({first.Value.Id}, {second.Value.Id}) AND status = 'pending'", ct));
        Assert.Equal(0, pending);

        await tx.RollbackAsync(ct);
    }

    [Fact]
    public async Task PauseResume_FlipsConfigServicePaused_AndBumpsVersion_RolledBack()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the pause/resume command test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var tx = await connection.BeginTransactionAsync(ct);
        await ExecAsync(connection, tx,
            "INSERT INTO config.config_service (id, updated_at) VALUES (1, now() AT TIME ZONE 'UTC') ON CONFLICT (id) DO NOTHING", ct);

        /* Drive the executor's ACTUAL resolved SQL — the same string the running service runs. */
        var pauseSql = DarlingCommandExecutor.ResolvePlan(Command("pause")).Sql!;
        var resumeSql = DarlingCommandExecutor.ResolvePlan(Command("resume")).Sql!;

        var beforeVersion = Convert.ToInt64(await ScalarAsync(connection, tx, "SELECT config_version FROM config.config_service WHERE id = 1", ct));

        await ExecAsync(connection, tx, pauseSql, ct);
        Assert.True((bool)(await ScalarAsync(connection, tx, "SELECT paused FROM config.config_service WHERE id = 1", ct))!);
        var afterPause = Convert.ToInt64(await ScalarAsync(connection, tx, "SELECT config_version FROM config.config_service WHERE id = 1", ct));
        Assert.True(afterPause > beforeVersion, "pause must bump config_version (so the loop reloads and honors it)");

        await ExecAsync(connection, tx, resumeSql, ct);
        Assert.False((bool)(await ScalarAsync(connection, tx, "SELECT paused FROM config.config_service WHERE id = 1", ct))!);

        await tx.RollbackAsync(ct);
    }

    [Fact]
    public async Task ExecuteAndReport_EnableCollector_RoundTrip_AgainstDevPostgres()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the command execute/report round-trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await CleanupSentinelAsync(connection, ct);

        /* A real pending command row for the executor to claim, execute, and report on. */
        long commandId;
        using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (command_type, target_server_id, args_json, status) " +
            "VALUES ('enable_collector', $1, '{\"collector_name\":\"wait_stats\"}'::jsonb, 'pending') RETURNING command_id", connection))
        {
            insert.Parameters.AddWithValue(SentinelServerId);
            commandId = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var executor = new DarlingCommandExecutor(postgres, new ThrowingHost(), "test-instance", null);

        /* Claim through the executor's own path (the queue holds only our sentinel row now), then execute+report. */
        var claimed = await executor.ClaimNextAsync(ct);
        Assert.NotNull(claimed);
        Assert.Equal(commandId, claimed!.CommandId);
        Assert.Equal("enable_collector", claimed.CommandType);
        Assert.Equal(SentinelServerId, claimed.TargetServerId);

        await executor.ExecuteAndReportAsync(claimed, ct);

        /* The store effect: a per-server schedule override row with enabled = TRUE. */
        using (var read = new NpgsqlCommand(
            "SELECT enabled FROM config.config_collector_schedules WHERE server_id = $1 AND collector_name = 'wait_stats'", connection))
        {
            read.Parameters.AddWithValue(SentinelServerId);
            Assert.Equal(true, await read.ExecuteScalarAsync(ct));
        }

        /* The reported terminal outcome on the command row, including the args_json secret scrub. */
        using (var read = new NpgsqlCommand(
            "SELECT status, result_status, result_json::text, completed_at, args_json FROM config.config_command WHERE command_id = $1", connection))
        {
            read.Parameters.AddWithValue(commandId);
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("succeeded", reader.GetString(0));
            Assert.Equal("collector enabled", reader.GetString(1));
            /* result_json is jsonb, so its ::text is normalized with a space after the colon (key order is
               jsonb's, not the serializer's) — assert on the stored/normalized form, not the raw serializer output. */
            Assert.Contains("\"success\": true", reader.GetString(2), StringComparison.Ordinal);
            Assert.False(reader.IsDBNull(3), "completed_at must be stamped");
            Assert.True(reader.IsDBNull(4), "args_json must be nulled on terminal state (#1262 secret retention)");
        }

        await CleanupSentinelAsync(connection, ct);
    }

    [Fact]
    public async Task ReclaimStaleCommands_FailsStaleInProgress_LeavesFreshUntouched_RolledBack()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the stale-command reaper test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);

        await using var tx = await connection.BeginTransactionAsync(ct);

        /* One command claimed 10 minutes ago (past the 5-minute stale timeout) and still carrying a
           test_connect credential blob, plus one claimed just now (a command legitimately in flight). */
        long staleId, freshId;
        using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (command_type, args_json, status, claimed_at, service_instance) " +
            "VALUES ('test_connect', '{\"password\":\"secret\"}'::jsonb, 'in_progress', (now() AT TIME ZONE 'UTC') - interval '10 minutes', 'crashed-instance') RETURNING command_id", connection, tx))
        {
            staleId = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
        }

        using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (command_type, args_json, status, claimed_at, service_instance) " +
            "VALUES ('test_connect', '{\"password\":\"secret\"}'::jsonb, 'in_progress', (now() AT TIME ZONE 'UTC'), 'live-instance') RETURNING command_id", connection, tx))
        {
            freshId = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
        }

        /* Drive the executor's ACTUAL reaper SQL, with the real 5-minute timeout bound as a PG interval. */
        using (var reap = new NpgsqlCommand(DarlingCommandExecutor.ReclaimStaleCommandsSql, connection, tx))
        {
            reap.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Interval, Value = DarlingCommandExecutor.StaleCommandTimeout });
            await reap.ExecuteNonQueryAsync(ct);
        }

        /* The stale row is reclaimed: terminal failed, a clear result_status, credential args scrubbed,
           completed_at stamped. */
        using (var read = new NpgsqlCommand(
            "SELECT status, result_status, args_json IS NULL, completed_at IS NOT NULL FROM config.config_command WHERE command_id = $1", connection, tx))
        {
            read.Parameters.AddWithValue(staleId);
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("failed", reader.GetString(0));
            Assert.Contains("reclaimed", reader.GetString(1), StringComparison.Ordinal);
            Assert.True(reader.GetBoolean(2), "stale row's args_json must be nulled");
            Assert.True(reader.GetBoolean(3), "stale row's completed_at must be stamped");
        }

        /* The fresh row (a command still legitimately running) is untouched — still in_progress, args kept. */
        using (var read = new NpgsqlCommand(
            "SELECT status, args_json IS NULL FROM config.config_command WHERE command_id = $1", connection, tx))
        {
            read.Parameters.AddWithValue(freshId);
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("in_progress", reader.GetString(0));
            Assert.False(reader.GetBoolean(1), "fresh row's args_json must be preserved (still executing)");
        }

        await tx.RollbackAsync(ct);
    }

    [Fact]
    public async Task ExecuteAndReport_FetchPlan_RoundTrip_DispatchesToHost_ReportsPlanXml()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the fetch_plan command round-trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupSentinelAsync(connection, ct);

        /* A real pending fetch_plan row carrying a plan_handle in args_json (the query-grid path). */
        long commandId;
        using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (command_type, target_server_id, args_json, status) " +
            "VALUES ('fetch_plan', $1, '{\"planHandle\":\"0x0600AB\",\"databaseName\":\"master\"}'::jsonb, 'pending') RETURNING command_id", connection))
        {
            insert.Parameters.AddWithValue(SentinelServerId);
            commandId = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var host = new PlanReturningHost();
        var executor = new DarlingCommandExecutor(postgres, host, "test-instance", null);

        var claimed = await executor.ClaimNextAsync(ct);
        Assert.NotNull(claimed);
        Assert.Equal("fetch_plan", claimed!.CommandType);
        await executor.ExecuteAndReportAsync(claimed, ct);

        /* The executor parsed args_json and dispatched the structured request to the host with the target id. */
        Assert.Equal(SentinelServerId, host.ReceivedServerId);
        Assert.NotNull(host.Received);
        Assert.Equal("0x0600AB", host.Received!.PlanHandle);
        Assert.Equal("master", host.Received.DatabaseName);
        Assert.True(host.Received.UsePlanHandle);

        /* The reported terminal outcome carries the plan XML; args_json is scrubbed on terminal state. */
        using (var read = new NpgsqlCommand(
            "SELECT status, result_status, result_json::text, args_json FROM config.config_command WHERE command_id = $1", connection))
        {
            read.Parameters.AddWithValue(commandId);
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("succeeded", reader.GetString(0));
            Assert.Equal("plan fetched", reader.GetString(1));
            Assert.Contains("ShowPlanXML", reader.GetString(2), StringComparison.Ordinal);
            Assert.True(reader.IsDBNull(3), "args_json must be nulled on terminal state");
        }

        await CleanupSentinelAsync(connection, ct);
    }

    [Fact]
    public async Task ExecuteAndReport_FetchActiveQueries_RoundTrip_DispatchesToHost_ReportsRows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the fetch_active_queries command round-trip.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupSentinelAsync(connection, ct);

        /* A real pending fetch_active_queries row — target-only, no args_json (the whole request is the server id). */
        long commandId;
        using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (command_type, target_server_id, status) " +
            "VALUES ('fetch_active_queries', $1, 'pending') RETURNING command_id", connection))
        {
            insert.Parameters.AddWithValue(SentinelServerId);
            commandId = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var host = new ActiveQueriesReturningHost();
        var executor = new DarlingCommandExecutor(postgres, host, "test-instance", null);

        var claimed = await executor.ClaimNextAsync(ct);
        Assert.NotNull(claimed);
        Assert.Equal("fetch_active_queries", claimed!.CommandType);
        Assert.Equal(SentinelServerId, claimed.TargetServerId);
        await executor.ExecuteAndReportAsync(claimed, ct);

        /* The executor dispatched to the host with the target id (no args to parse). */
        Assert.Equal(SentinelServerId, host.ReceivedServerId);

        /* The reported terminal outcome carries the rows payload. */
        using (var read = new NpgsqlCommand(
            "SELECT status, result_status, result_json::text FROM config.config_command WHERE command_id = $1", connection))
        {
            read.Parameters.AddWithValue(commandId);
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("succeeded", reader.GetString(0));
            Assert.Equal("active queries fetched", reader.GetString(1));
            Assert.Contains("\"rows\"", reader.GetString(2), StringComparison.Ordinal);
        }

        await CleanupSentinelAsync(connection, ct);
    }

    [Fact]
    public async Task ExecuteAndReport_UnknownType_ReportsFailed_NeverThrows()
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the unknown-command test.");

        var ct = TestContext.Current.CancellationToken;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await CleanupSentinelAsync(connection, ct);

        long commandId;
        using (var insert = new NpgsqlCommand(
            "INSERT INTO config.config_command (command_type, requested_by, status) VALUES ('nonsense_command', 'sentinel-test', 'in_progress') RETURNING command_id", connection))
        {
            commandId = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
        }

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var executor = new DarlingCommandExecutor(postgres, new ThrowingHost(), "test-instance", null);

        /* Directly report on the row (bypass ClaimNextAsync so we never claim an unrelated command). */
        await executor.ExecuteAndReportAsync(
            new ClaimedCommand(commandId, "nonsense_command", null, null, "sentinel-test"), ct);

        using (var read = new NpgsqlCommand("SELECT status, result_status FROM config.config_command WHERE command_id = $1", connection))
        {
            read.Parameters.AddWithValue(commandId);
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("failed", reader.GetString(0));
            Assert.Equal("unknown command_type", reader.GetString(1));
        }

        using (var cleanup = new NpgsqlCommand("DELETE FROM config.config_command WHERE requested_by = 'sentinel-test'", connection))
        {
            await cleanup.ExecuteNonQueryAsync(ct);
        }
    }

    private static async Task CleanupSentinelAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var cleanup = new NpgsqlCommand(
            $"DELETE FROM config.config_collector_schedules WHERE server_id = {SentinelServerId}; " +
            $"DELETE FROM config.config_command WHERE target_server_id = {SentinelServerId} OR requested_by = 'sentinel-test'", connection);
        await cleanup.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(long Id, string Type)?> ClaimAsync(NpgsqlConnection connection, NpgsqlTransaction tx, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(DarlingCommandExecutor.ClaimCommandSql, connection, tx);
        command.Parameters.AddWithValue("claim-test");
        using var reader = await command.ExecuteReaderAsync(ct);
        if (!await reader.ReadAsync(ct))
        {
            return null;
        }

        return (reader.GetInt64(0), reader.GetString(1));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, NpgsqlTransaction tx, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection, tx);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<object?> ScalarAsync(NpgsqlConnection connection, NpgsqlTransaction tx, string sql, CancellationToken ct)
    {
        using var command = new NpgsqlCommand(sql, connection, tx);
        return await command.ExecuteScalarAsync(ct);
    }
}
