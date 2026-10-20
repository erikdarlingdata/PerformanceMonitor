/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Security.Cryptography;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres and a role with a per-run name,
   and touches nothing on the shared one, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// The MCP incident actions (#6230, part B) as the <c>mcp</c> role runs them: <c>test_connection</c>, <c>analyze_now</c> and <c>purge_now</c>
/// queue a command through <c>config.request_mcp_command</c>, and <c>get_command_result</c> reads what the service wrote back. The role's
/// grants come from <see cref="DarlingManagedRoles.BuildProvisioningSql"/> (through <see cref="ViewerGrantReplay"/>) and the function is the
/// product's own builder, so a table grant added to the product's SQL, or a dropped EXECUTE grant, fails these facts. No SQL Server is
/// involved: the service's part is played by the test (it completes the row the way the executor reports it).
/// </summary>
public sealed class McpIncidentActionsLiveTests
{
    private static readonly string RolePassword = Convert.ToHexString(RandomNumberGenerator.GetBytes(16));

    private const int ServerId = 41;

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    [Fact]
    public async Task EachAction_IsQueuedAsTheFixedCommand_AndTheExecutorResolvesItsPlanOnTheNextPass()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            foreach (var (answer, type, plan) in new[]
            {
                (await DarlingMcpIncidentActionTools.TestConnection(asRole, "snap-target", ct), "test_connect", CommandKind.Probe),
                (await DarlingMcpIncidentActionTools.AnalyzeNow(asRole, "snap-target", ct), "analyze_now", CommandKind.Analyze),
                (await DarlingMcpIncidentActionTools.PurgeNow(asRole, confirm: await ConfirmValueAsync(asRole, 30, ct), retention_days: 30, cancellationToken: ct), "purge_now", CommandKind.Purge),
            })
            {
                var node = Parse(answer);
                Assert.Equal("queued", (string?)node["status"]);
                var id = (long)node["command_id"]!;
                Assert.Equal(id, (long)node["check_with"]!["command_id"]!);
                Assert.False(string.IsNullOrWhiteSpace((string?)node["undo"]));

                await using var read = owner.CreateCommand(
                    "SELECT command_type, status, target_server_id, requested_by, args_json::text FROM config.config_command WHERE command_id = $1");
                read.Parameters.AddWithValue(id);
                await using var reader = await read.ExecuteReaderAsync(ct);
                Assert.True(await reader.ReadAsync(ct));
                Assert.Equal(type, reader.GetString(0));
                Assert.Equal("pending", reader.GetString(1));
                int? target = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                Assert.Equal(type == "purge_now" ? null : ServerId, target);
                Assert.Equal("mcp", reader.GetString(3));
                var args = reader.IsDBNull(4) ? null : reader.GetString(4);

                /* The service's own plan for that row is a real one (not a refusal), so it runs on its next poll. */
                Assert.Equal(plan, DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(id, type, target, args, "mcp")).Kind);
                if (type == "purge_now")
                {
                    Assert.Contains("30", args, StringComparison.Ordinal);
                    Assert.Contains("cannot be brought back", (string?)node["undo"], StringComparison.Ordinal);
                }

                if (type == "test_connect")
                {
                    /* Built from the stored row: the host, no plain password slot, and the stored secret as the service reads it. */
                    var built = JsonNode.Parse(args!)!.AsObject();
                    Assert.Equal("snap-target", (string?)built["host"]);
                    Assert.Equal("blob-from-the-store", (string?)built["encryptedPassword"]);
                    Assert.False(built.ContainsKey("password"));
                }
            }
        });
    }

    [Fact]
    public async Task TestConnection_ReportsSuccessAndFailure_ThroughGetCommandResult()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            var first = Parse(await DarlingMcpIncidentActionTools.TestConnection(asRole, "snap-target", ct));
            var id = (long)first["command_id"]!;

            var waiting = Parse(await DarlingMcpIncidentActionTools.GetCommandResult(asRole, id, ct));
            Assert.Equal("pending", (string?)waiting["command_status"]);

            var ok = DarlingCommandExecutor.MapProbeResult(new ConnectionProbeResult(
                Success: true, MajorVersion: 16, EngineEdition: 3, EngineEditionDescription: "Enterprise",
                IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: true, Error: null));
            await CompleteAsync(owner, id, "succeeded", ok.ResultStatus, ok.ResultJson, ct);
            var done = Parse(await DarlingMcpIncidentActionTools.GetCommandResult(asRole, id, ct));
            Assert.Equal("succeeded", (string?)done["command_status"]);
            Assert.Equal("connected", (string?)done["result_status"]);
            Assert.Equal(16, (int)done["result"]!["majorVersion"]!);
            Assert.DoesNotContain("args", done.ToJsonString(), StringComparison.OrdinalIgnoreCase);

            await ExecAsync(owner, "UPDATE config.config_command SET created_at = created_at - interval '1 minute'", ct);
            var second = Parse(await DarlingMcpIncidentActionTools.TestConnection(asRole, "snap-target", ct));
            var failedId = (long)second["command_id"]!;
            var bad = DarlingCommandExecutor.MapProbeResult(new ConnectionProbeResult(
                Success: false, MajorVersion: 0, EngineEdition: 0, EngineEditionDescription: null,
                IsAzureSqlDb: false, IsAzureManagedInstance: false, IsAwsRds: false, HasMsdbAccess: false, Error: "A network-related error occurred"));
            await CompleteAsync(owner, failedId, "failed", bad.ResultStatus, bad.ResultJson, ct);
            var failed = Parse(await DarlingMcpIncidentActionTools.GetCommandResult(asRole, failedId, ct));
            Assert.Equal("failed", (string?)failed["command_status"]);
            Assert.Equal("connection_failed", (string?)failed["result_status"]);
            Assert.Contains("network-related", failed["result"]!.ToJsonString(), StringComparison.Ordinal);

            Assert.Equal("not_found", (string?)Parse(await DarlingMcpIncidentActionTools.GetCommandResult(asRole, 999999, ct))["status"]);
        });
    }

    private static async Task<string> ConfirmValueAsync(NpgsqlDataSource asRole, int? days, CancellationToken ct) =>
        (string)Parse(await DarlingMcpIncidentActionTools.PurgeNow(asRole, retention_days: days, cancellationToken: ct))["confirm_value"]!;

    private static async Task<long> CommandCountAsync(NpgsqlDataSource owner, CancellationToken ct)
    {
        await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
        return Convert.ToInt64(await count.ExecuteScalarAsync(ct));
    }

    /// <summary>The purge is two steps: the first call queues nothing and shows what it would delete (collector retention and the
    /// oldest time kept) with a confirm value; a wrong echo is refused; only the right echo queues it.</summary>
    [Fact]
    public async Task PurgeNow_ShowsFirst_RefusesAWrongEcho_AndQueuesOnlyOnTheRightOne()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            var shown = Parse(await DarlingMcpIncidentActionTools.PurgeNow(asRole, retention_days: 7, cancellationToken: ct));
            Assert.Equal("confirm_required", (string?)shown["status"]);
            Assert.Contains("cannot be brought back", (string?)shown["message"], StringComparison.Ordinal);
            var groups = shown["would_delete"]!.AsArray();
            Assert.NotEmpty(groups);
            Assert.All(groups, g =>
            {
                Assert.True((int)g!["retention_days"]! >= 7);
                Assert.False(string.IsNullOrWhiteSpace((string?)g["oldest_kept_utc"]));
                Assert.NotEmpty(g["collectors"]!.AsArray());
            });
            var confirm = (string)shown["confirm_value"]!;
            Assert.Equal(0L, await CommandCountAsync(owner, ct));

            foreach (var wrong in new[] { "true", "yes", "purge-000000000000", confirm + "x" })
            {
                var refused = Parse(await DarlingMcpIncidentActionTools.PurgeNow(asRole, confirm: wrong, retention_days: 7, cancellationToken: ct));
                Assert.Equal("refused", (string?)refused["status"]);
                Assert.Equal("confirm_mismatch", (string?)refused["reason"]);
            }

            /* A value shown for one retention does not confirm another. */
            var otherRetention = Parse(await DarlingMcpIncidentActionTools.PurgeNow(asRole, confirm: confirm, retention_days: 8, cancellationToken: ct));
            Assert.Equal("confirm_mismatch", (string?)otherRetention["reason"]);
            Assert.Equal(0L, await CommandCountAsync(owner, ct));

            var queued = Parse(await DarlingMcpIncidentActionTools.PurgeNow(asRole, confirm: confirm, retention_days: 7, cancellationToken: ct));
            Assert.Equal("queued", (string?)queued["status"]);
            Assert.Equal(1L, await CommandCountAsync(owner, ct));
        });
    }

    [Fact]
    public async Task PurgeNow_WithABadRetention_IsRefused_AndQueuesNothing()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            foreach (var days in new[] { 0, -5, DarlingMcpIncidentActionTools.MaxPurgeRetentionDays + 1 })
            {
                var bad = Parse(await DarlingMcpIncidentActionTools.PurgeNow(asRole, confirm: "purge-000000000000", retention_days: days, cancellationToken: ct));
                Assert.Equal("refused", (string?)bad["status"]);
                Assert.Equal("invalid", (string?)bad["reason"]);
            }

            Assert.Equal(0L, await CommandCountAsync(owner, ct));
        });
    }

    /// <summary>get_command_result answers only for the command types the MCP tools queue: a command another client queued (the
    /// Viewer's live reads, a plan fetch) answers as not found, and the person's name it carries is withheld under metadataOnly.</summary>
    [Fact]
    public async Task GetCommandResult_RefusesAViewerOnlyCommand_AndMetadataOnlyWithholdsRequestedBy()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            await ExecAsync(owner,
                "INSERT INTO config.config_command (command_id, command_type, target_server_id, status, requested_by) OVERRIDING SYSTEM VALUE VALUES " +
                "(9001, 'fetch_active_queries', 41, 'succeeded', 'dana.reyes (viewer)'), (9002, 'test_connect', 41, 'succeeded', 'dana.reyes (viewer)')", ct);

            var viewerOnly = Parse(await DarlingMcpIncidentActionTools.GetCommandResult(asRole, 9001, ct));
            Assert.Equal("not_found", (string?)viewerOnly["status"]);
            Assert.DoesNotContain("dana.reyes", viewerOnly.ToJsonString(), StringComparison.Ordinal);
            Assert.DoesNotContain("fetch_active_queries", viewerOnly.ToJsonString(), StringComparison.Ordinal);

            var answer = await DarlingMcpIncidentActionTools.GetCommandResult(asRole, 9002, ct);
            Assert.Equal("test_connect", (string?)Parse(answer)["command_type"]);
            Assert.Equal("dana.reyes (viewer)", (string?)Parse(answer)["requested_by"]);

            var masked = McpDataDetailMasker.MaskJson(answer, McpDataDetail.MetadataOnly, key: PseudonymTestKey.Key);
            Assert.DoesNotContain("dana.reyes", masked, StringComparison.Ordinal);
        });
    }

    [Fact]
    public async Task ASecondRequest_IsRefusedInPlainWords_WhileOneIsPending_AndAnUnknownServerIsRefused()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            Assert.Equal("queued", (string?)Parse(await DarlingMcpIncidentActionTools.AnalyzeNow(asRole, "snap-target", ct))["status"]);
            var again = Parse(await DarlingMcpIncidentActionTools.AnalyzeNow(asRole, "snap-target", ct));
            Assert.Equal("refused", (string?)again["status"]);
            Assert.Equal("already_queued", (string?)again["reason"]);
            Assert.Contains("Nothing was queued", (string?)again["message"], StringComparison.Ordinal);

            var unknown = await DarlingMcpIncidentActionTools.AnalyzeNow(asRole, "no-such-server", ct);
            Assert.DoesNotContain("\"queued\"", unknown, StringComparison.Ordinal);

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(1L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    [Theory]
    [InlineData("pause", ServerId, null)]
    [InlineData("resume", null, 30)]
    [InlineData("disable_server", ServerId, null)]
    [InlineData("snapshot_now", ServerId, null)]
    [InlineData("undo_plan_change", null, null)]
    [InlineData("execute_actual_plan", ServerId, null)]
    [InlineData("", null, null)]
    [InlineData("analyze_now", ServerId, 30)]
    [InlineData("purge_now", ServerId, null)]
    [InlineData("purge_now", null, 0)]
    [InlineData("purge_now", null, 3651)]
    [InlineData("reload", ServerId, null)]
    [InlineData("reload", null, null)]
    public async Task TheFunction_RefusesACommandTypeOutsideItsFiveOrABadArgument_AndQueuesNothing(string type, int? serverId, int? retentionDays)
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var call = asRole.CreateCommand("SELECT config.request_mcp_command($1, $2, $3, 'mcp')");
                call.Parameters.AddWithValue(type);
                call.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = (object?)serverId ?? DBNull.Value });
                call.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer, Value = (object?)retentionDays ?? DBNull.Value });
                await call.ExecuteScalarAsync(ct);
            });
            Assert.Equal("DL400", refused.SqlState);

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    [Theory]
    [InlineData("pause")]
    [InlineData("resume")]
    public async Task PauseAndResume_AreQueuedThroughTheFunction_WithNoServer_StampedMcpWhateverTheCallerSends(string type)
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            await using var call = asRole.CreateCommand("SELECT config.request_mcp_command($1, NULL, NULL, 'someone (viewer)')");
            call.Parameters.AddWithValue(type);
            var id = Convert.ToInt64(await call.ExecuteScalarAsync(ct));
            await using var read = owner.CreateCommand(
                "SELECT command_type || '/' || COALESCE(args_json::text, 'null') || '/' || requested_by || '/' || status FROM config.config_command WHERE command_id = $1");
            read.Parameters.AddWithValue(id);
            Assert.Equal(type + "/null/mcp/pending", (string)(await read.ExecuteScalarAsync(ct))!);
            Assert.Equal(CommandKind.StoreWrite, DarlingCommandExecutor.ResolvePlan(new ClaimedCommand(id, type, null, null, "mcp")).Kind);
        });
    }

    [Fact]
    public async Task PauseCollection_QueuesThePauseCommand_AndTheMcpRoleCanNoLongerWriteThePausedFlag()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            await ExecAsync(owner, "INSERT INTO config.config_service (id) VALUES (1) ON CONFLICT DO NOTHING", ct);
            var queued = Parse(await DarlingMcpCollectionStateTools.PauseCollection(asRole, cancellationToken: ct));
            Assert.Equal("queued", (string?)queued["status"]);
            var id = (long)queued["command_id"]!;
            Assert.Equal(id, (long)queued["check_with"]!["command_id"]!);
            Assert.Equal("resume_collection", (string?)queued["undo"]!["tool"]);
            await using (var read = owner.CreateCommand("SELECT command_type || '/' || requested_by FROM config.config_command WHERE command_id = $1"))
            {
                read.Parameters.AddWithValue(id);
                Assert.Equal("pause/mcp", (string)(await read.ExecuteScalarAsync(ct))!);
            }

            /* Nothing wrote the flag: only the service does, when it runs the command. */
            await using (var flag = owner.CreateCommand("SELECT paused FROM config.config_service WHERE id = 1"))
            {
                Assert.False((bool)(await flag.ExecuteScalarAsync(ct))!);
            }

            var again = Parse(await DarlingMcpCollectionStateTools.PauseCollection(asRole, cancellationToken: ct));
            Assert.Equal("refused", (string?)again["status"]);
            Assert.Equal("already_queued", (string?)again["reason"]);

            var denied = await Assert.ThrowsAsync<PostgresException>(() => ExecAsync(asRole, "UPDATE config.config_service SET paused = TRUE WHERE id = 1", ct));
            Assert.Equal("42501", denied.SqlState);
        });
    }

    [Fact]
    public async Task TestConnect_IsRefusedForADisabledServer_AndQueuedOnceItIsEnabled()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            await ExecAsync(owner, $"UPDATE config.config_monitored_servers SET is_enabled = FALSE WHERE server_id = {ServerId}", ct);
            var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var call = asRole.CreateCommand($"SELECT config.request_mcp_command('test_connect', {ServerId}, NULL, 'mcp')");
                await call.ExecuteScalarAsync(ct);
            });
            Assert.Equal("DL400", refused.SqlState);
            await using (var count = owner.CreateCommand("SELECT count(*) FROM config.config_command"))
            {
                Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
            }

            await ExecAsync(owner, $"UPDATE config.config_monitored_servers SET is_enabled = TRUE WHERE server_id = {ServerId}", ct);
            await using var ok = asRole.CreateCommand($"SELECT config.request_mcp_command('test_connect', {ServerId}, NULL, 'mcp')");
            Assert.True(Convert.ToInt64(await ok.ExecuteScalarAsync(ct)) > 0);
        });
    }

    [Fact]
    public async Task TheFunction_RefusesARepeatableReadTransaction_AndQueuesNothing()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            await using var connection = await asRole.OpenConnectionAsync(ct);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, ct);
            var refused = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var call = new NpgsqlCommand("SELECT config.request_mcp_command('pause', NULL, NULL, 'mcp')", connection, transaction);
                await call.ExecuteScalarAsync(ct);
            });
            Assert.Equal("DL400", refused.SqlState);
            Assert.Contains("READ COMMITTED", refused.MessageText, StringComparison.Ordinal);
            await transaction.RollbackAsync(ct);

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    [Fact]
    public async Task TheFunction_AnswersBusyAtOnce_WhenAnotherSessionHoldsItsLock_AndNeverBlocks()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            await using var holder = await owner.OpenConnectionAsync(ct);
            await using (var hold = new NpgsqlCommand("SELECT pg_advisory_lock(6230, hashtext('pause:'))", holder))
            {
                await hold.ExecuteNonQueryAsync(ct);
            }

            var busy = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var call = asRole.CreateCommand("SELECT config.request_mcp_command('pause', NULL, NULL, 'mcp')");
                call.CommandTimeout = 20; /* a request that waited on the lock would end in 57014 (query canceled), not DL409 */
                await call.ExecuteScalarAsync(ct);
            });
            Assert.Equal("DL409", busy.SqlState);
            Assert.Contains("busy", busy.MessageText, StringComparison.Ordinal);

            await using (var release = new NpgsqlCommand("SELECT pg_advisory_unlock(6230, hashtext('pause:'))", holder))
            {
                await release.ExecuteNonQueryAsync(ct);
            }

            await using var after = asRole.CreateCommand("SELECT config.request_mcp_command('pause', NULL, NULL, 'mcp')");
            Assert.True(Convert.ToInt64(await after.ExecuteScalarAsync(ct)) > 0);
        });
    }

    [Fact]
    public async Task TheMcpRole_CannotInsertACommandRowDirectly_AndAnotherRoleCannotCallTheFunction()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var insert = asRole.CreateCommand(
                    "INSERT INTO config.config_command (requested_by, command_type, target_server_id, args_json, status, created_at) " +
                    "VALUES ('x', 'pause', NULL, NULL, 'pending', now() AT TIME ZONE 'UTC')");
                await insert.ExecuteNonQueryAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);
        });

        await WithRoleAsync("viewer", grantFunction: false, async (owner, asRole, ct) =>
        {
            var denied = await Assert.ThrowsAsync<PostgresException>(async () =>
            {
                await using var call = asRole.CreateCommand("SELECT config.request_mcp_command('purge_now', NULL, NULL, 'x')");
                await call.ExecuteScalarAsync(ct);
            });
            Assert.Equal("42501", denied.SqlState);

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.config_command");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    [Fact]
    public async Task UndoPlanChange_RefusesAnUnknownChange_AndABadId_WritingNothing_AndGetAgentChangesReadsTheJournal()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            var empty = Parse(await DarlingMcpIncidentActionTools.GetAgentChanges(asRole, cancellationToken: ct));
            Assert.Equal("empty", (string?)empty["status"]);

            var unknown = Parse(await DarlingMcpPlanUndoTools.RunAsync(asRole, null, null, 424242, confirm: true, shown: "x", ct));
            Assert.Equal("refused", (string?)unknown["status"]);
            Assert.Equal("not_found", (string?)unknown["code"]);

            var bad = Parse(await DarlingMcpPlanUndoTools.RunAsync(asRole, null, null, 0, confirm: false, shown: null, ct));
            Assert.Equal("refused", (string?)bad["status"]);
            Assert.Equal("invalid", (string?)bad["code"]);

            await using var count = owner.CreateCommand("SELECT count(*) FROM config.investigation_journal WHERE event = 'action_undone'");
            Assert.Equal(0L, Convert.ToInt64(await count.ExecuteScalarAsync(ct)));
        });
    }

    /// <summary>M1 (#6230): the fleet roll-up the web status bar reads says whether collection is paused, and who paused it and when
    /// while the command queue still holds that pause (newer than any resume).</summary>
    [Fact]
    public async Task TheFleetRollup_CarriesThePauseFlag_WithWhoAndWhen()
    {
        await WithRoleAsync("mcp", grantFunction: true, async (owner, asRole, ct) =>
        {
            async Task<FleetCollectionState?> ReadAsync() =>
                (await DarlingFleetReader.GetFleetOverviewAsync(asRole, DateTime.UtcNow.AddHours(-1), DateTime.UtcNow, cancellationToken: ct)).Collection;

            await ExecAsync(owner, "INSERT INTO config.config_service (id, paused) VALUES (1, FALSE) ON CONFLICT (id) DO UPDATE SET paused = FALSE", ct);
            Assert.False((await ReadAsync())!.Paused);

            await ExecAsync(owner, "UPDATE config.config_service SET paused = TRUE WHERE id = 1", ct);
            var unknownWho = (await ReadAsync())!;
            Assert.True(unknownWho.Paused);
            Assert.Null(unknownWho.PausedBy);

            await ExecAsync(owner,
                "INSERT INTO config.config_command (command_type, status, requested_by, claimed_at, completed_at) VALUES " +
                "('pause', 'succeeded', 'dana.reyes (viewer)', TIMESTAMP '2026-10-20 10:00', TIMESTAMP '2026-10-20 10:00:05')", ct);
            var named = (await ReadAsync())!;
            Assert.True(named.Paused);
            Assert.Equal("dana.reyes (viewer)", named.PausedBy);
            Assert.Equal(new DateTime(2026, 10, 20, 10, 0, 5), named.PausedUtc);

            /* A later resume means the pause the flag now holds was not that one: nobody is named. */
            await ExecAsync(owner,
                "INSERT INTO config.config_command (command_type, status, requested_by, completed_at) VALUES ('resume', 'succeeded', 'dana.reyes (viewer)', TIMESTAMP '2026-10-20 11:00')", ct);
            Assert.Null((await ReadAsync())!.PausedBy);
        });
    }

    private static Task CompleteAsync(NpgsqlDataSource owner, long id, string status, string resultStatus, string resultJson, CancellationToken ct) =>
        ExecAsync(owner,
            $"UPDATE config.config_command SET status = '{status}', result_status = '{resultStatus}', result_json = '{resultJson.Replace("'", "''", StringComparison.Ordinal)}'::jsonb, " +
            $"claimed_at = now() AT TIME ZONE 'UTC', completed_at = now() AT TIME ZONE 'UTC', args_json = NULL WHERE command_id = {id}", ct);

    private static async Task WithRoleAsync(
        string managedRole, bool grantFunction, Func<NpgsqlDataSource, NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the MCP incident action live tests (they mint their own scratch database and role).");

        var ct = TestContext.Current.CancellationToken;
        var roleName = "mcp_inc_" + Guid.NewGuid().ToString("N")[..8];
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using (var connection = new NpgsqlConnection(scratch.ConnectionString))
        {
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            await ExecAsync(owner,
                $"INSERT INTO config.config_monitored_servers (server_id, name, host, encrypted_password, is_enabled) VALUES ({ServerId}, 'snap-target', 'snap-target', 'blob-from-the-store', TRUE)", ct);
            await ExecAsync(owner,
                $"INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) VALUES ({ServerId}, 'snap-target', 'snap-target', TRUE, 15, now(), now())", ct);

            await ExecAsync(owner, DarlingManagedRoles.BuildRequestMcpCommandFunctionSql("config"), ct);
            await ExecAsync(owner, $"CREATE ROLE {roleName} LOGIN NOSUPERUSER PASSWORD '{RolePassword}'", ct);
            foreach (var statement in ViewerGrantReplay.StatementsFor(roleName, scratch.DatabaseName, managedRole))
            {
                await ExecAsync(owner, statement, ct);
            }

            /* The product's own EXECUTE grant for the function, retargeted: mcp holds it, no other role does. */
            var execute = ViewerGrantReplay.BatchStatements().Single(s =>
                s.StartsWith("GRANT EXECUTE ON FUNCTION config.request_mcp_command", StringComparison.Ordinal));
            var at = execute.LastIndexOf(" TO ", StringComparison.Ordinal);
            Assert.Equal("mcp", execute[(at + 4)..].Trim());
            if (grantFunction)
            {
                await ExecAsync(owner, execute[..at] + " TO " + roleName, ct);
            }

            await using var asRole = NpgsqlDataSource.Create(new NpgsqlConnectionStringBuilder(scratch.ConnectionString)
            {
                Username = roleName,
                Password = RolePassword,
                Pooling = false,
                SearchPath = PgSchemaGenerator.SearchPath,
            }.ConnectionString);

            await body(owner, asRole, ct);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunOwnedAsync(bodySucceeded, () => ExecAsync(owner, $"DROP OWNED BY {roleName}; DROP ROLE IF EXISTS {roleName};", CancellationToken.None));
        }
    }

    private static async Task ExecAsync(NpgsqlDataSource source, string sql, CancellationToken ct)
    {
        await using var command = source.CreateCommand(sql);
        await command.ExecuteNonQueryAsync(ct);
    }
}
