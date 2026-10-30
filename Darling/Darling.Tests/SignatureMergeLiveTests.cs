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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #6006 against a live store: <c>merge_deadlock_signatures</c> through the Level 2 gate. One test per behaviour the issue names:
/// merged signatures share counts, undo splits them again, a Level 1 route is refused, and the merge is journaled with its reason
/// and shows in the Agent changes list. One more holds the one-level rule.
/// </summary>
public sealed class SignatureMergeLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingAgentActionSeat AgentSeat = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));

    private const string SigA = "dl1-aaaaaaaaaaaa";
    private const string SigB = "dl1-bbbbbbbbbbbb";
    private const string SigC = "dl1-cccccccccccc";
    private const string Login = "erik";

    private sealed record Ctx(NpgsqlDataSource Source, NpgsqlConnection Connection, DeadlockSignatureStore Signatures, AgentChangeUndo Undo,
        string Arg, DateOnly Today, CancellationToken Ct);

    /// <param name="agentLevel">The route's agent level: 2 lets an agent change settings, 1 only investigate.</param>
    private static async Task RunAsync(int agentLevel, Func<Ctx, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the signature merge live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await ExecAsync(connection, "INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'h7')", ct);
            await ExecAsync(connection, "INSERT INTO collect.servers (server_id, server_name, display_name, is_enabled, sql_major_version, created_date, modified_date) "
                + "VALUES (7, 'srv-7', 'srv-7', TRUE, 15, now(), now())", ct);
            await ExecAsync(connection, $"INSERT INTO config.config_notification_routes (metric_match, agent_level) VALUES ('Blocking', {agentLevel.ToString(CultureInfo.InvariantCulture)})", ct);

            var builder = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            var store = new InvestigationRequestStore(source, 30);
            var alert = new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100,
                LeaseMinutes: 15, ExpiryMinutes: 240) { AutonomyLevel = agentLevel };
            var opened = await store.OpenOrJoinAsync(alert, ct);
            var claim = await store.ClaimAsync(opened.RequestId!.Value, "agent-a", ct);
            var arg = AgentActionArgumentTests.Arg(opened.RequestId.Value.ToString(), claim.ClaimId!.Value.ToString(), reason: "same tables, same lock order");

            /* Counts are on the day the rollup would write, so the test holds at any hour. The signatures are seeded for the same day. */
            var today = DateOnly.FromDateTime(DateTime.UtcNow);
            var signatures = new DeadlockSignatureStore(source, 30);
            await signatures.AddAsync(7, new Dictionary<string, int> { [SigA] = 3, [SigB] = 5, [SigC] = 1 }, today, ct);
            await body(new Ctx(source, connection, signatures, new AgentChangeUndo(source, 30), arg, today, ct));
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<string?> ScalarAsync(Ctx c, string sql)
    {
        await using var command = new NpgsqlCommand(sql, c.Connection);
        return Convert.ToString(await command.ExecuteScalarAsync(c.Ct), CultureInfo.InvariantCulture);
    }

    private static JsonObject Parse(string answer) => JsonNode.Parse(answer)!.AsObject();

    private static Task<string> Merge(Ctx c, string signature, string into) =>
        DarlingMcpSignatureMergeTools.MergeAsync(c.Source, AgentSeat, null, "srv-7", signature, into, null, c.Arg, c.Ct);

    private static async Task<Dictionary<string, int>> TodayAsync(Ctx c) =>
        (await c.Signatures.StatsAsync(7, c.Today, c.Ct)).ToDictionary(s => s.Signature, s => s.Today, StringComparer.Ordinal);

    [Fact]
    public async Task MergedSignatures_ShareCounts()
    {
        await RunAsync(2, async c =>
        {
            Assert.Equal(new Dictionary<string, int> { [SigA] = 3, [SigB] = 5, [SigC] = 1 }, await TodayAsync(c));

            var answer = Parse(await Merge(c, SigA, SigB));
            Assert.Equal("merged", (string?)answer["status"]);
            /* #6403: no tool call undoes a merge, so the answer carries the previous value, a null undo and a note saying where the undo is. */
            Assert.Equal(SigA, (string?)answer["previous"]!["counted_as"]);
            Assert.Null(answer["previous"]!["merged_into"]);
            Assert.Null(answer["undo"]);
            Assert.Contains("Agent changes", (string?)answer["undo_note"]);

            /* A is counted as B: one row, the two days added, and A has none of its own. */
            Assert.Equal(new Dictionary<string, int> { [SigB] = 8, [SigC] = 1 }, await TodayAsync(c));
            /* A fresh A count joins B's, and a signature merged into B is known, not new. */
            var fresh = await c.Signatures.AddAsync(7, new Dictionary<string, int> { [SigA] = 1 }, c.Today, c.Ct);
            Assert.Empty(fresh);
            Assert.Equal(9, (await TodayAsync(c))[SigB]);
        });
    }

    [Fact]
    public async Task UndoingAMerge_SplitsTheCountsAgain_AndKeepsTheRow()
    {
        await RunAsync(2, async c =>
        {
            Assert.Equal("merged", (string?)Parse(await Merge(c, SigA, SigB))["status"]);
            var called = long.Parse((await ScalarAsync(c, "SELECT max(journal_id) FROM config.investigation_journal WHERE event = 'action_called' AND detail->>'tool' = 'merge_deadlock_signatures'"))!, CultureInfo.InvariantCulture);

            var undone = await c.Undo.UndoAsync(called, Login, c.Ct);
            Assert.True(undone.Undone, undone.Words);

            Assert.Equal(new Dictionary<string, int> { [SigA] = 3, [SigB] = 5, [SigC] = 1 }, await TodayAsync(c));
            /* The row is the record of the decision: stamped, not deleted. */
            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_signature_merges WHERE undone_at IS NOT NULL AND undone_by = 'person:erik'"));
            Assert.Equal("person:erik", await ScalarAsync(c, $"SELECT actor FROM config.investigation_journal WHERE event = 'action_undone' AND detail->>'called_id' = '{called}'"));
            Assert.Equal("already_undone", (await c.Undo.UndoAsync(called, Login, c.Ct)).Code);

            /* With the merge undone the same pair can be merged again, the other way round. */
            Assert.Equal("merged", (string?)Parse(await Merge(c, SigB, SigA))["status"]);
            Assert.Equal(new Dictionary<string, int> { [SigA] = 8, [SigC] = 1 }, await TodayAsync(c));
        });
    }

    [Fact]
    public async Task ALevel1Route_IsRefused_AndNothingIsMerged()
    {
        await RunAsync(1, async c =>
        {
            var answer = Parse(await Merge(c, SigA, SigB));
            Assert.Equal("refused", (string?)answer["status"]);
            Assert.Equal(AgentActionCodes.NotLevel2, (string?)answer["reason"]);
            Assert.Equal("0", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_signature_merges"));
            Assert.Equal(new Dictionary<string, int> { [SigA] = 3, [SigB] = 5, [SigC] = 1 }, await TodayAsync(c));
        });
    }

    [Fact]
    public async Task AMerge_IsJournaledWithItsReason_AndShowsInTheAgentChangesList()
    {
        await RunAsync(2, async c =>
        {
            Assert.Equal("merged", (string?)Parse(await Merge(c, SigA, SigB))["status"]);

            var read = await c.Undo.ReadAsync(200, c.Ct);
            var change = Assert.Single(read.Changes, ch => ch.Tool == "merge_deadlock_signatures");
            Assert.Equal(AgentChangeKinds.SignatureMerge, change.Kind);
            Assert.Equal("agent:agent-a", change.Actor);
            Assert.Equal("same tables, same lock order", change.Reason);
            Assert.Equal(AgentChangeResult.Done, change.Result);
            Assert.True(change.CanTryUndo(DateTime.UtcNow));
            Assert.Equal(SigA, JsonNode.Parse(change.DetailJson)!["target"]!["id"]!.GetValue<string>());

            /* The table row carries the same reason and the agent. */
            Assert.Equal("same tables, same lock order", await ScalarAsync(c, "SELECT reason FROM config.investigation_signature_merges"));
            Assert.Equal("agent:agent-a", await ScalarAsync(c, "SELECT merged_by FROM config.investigation_signature_merges"));
        });
    }

    [Fact]
    public async Task Merges_AreOneLevelDeep_AndNeedCountedSignatures()
    {
        await RunAsync(2, async c =>
        {
            Assert.Equal("merged", (string?)Parse(await Merge(c, SigA, SigB))["status"]);
            Assert.Equal("merge_chain", (string?)Parse(await Merge(c, SigC, SigA))["reason"]);
            Assert.Equal("merge_chain", (string?)Parse(await Merge(c, SigB, SigC))["reason"]);
            Assert.Equal("already_merged", (string?)Parse(await Merge(c, SigA, SigC))["reason"]);
            Assert.Equal("unknown_signature", (string?)Parse(await Merge(c, SigC, "dl1-never-counted"))["reason"]);
            Assert.Equal("invalid", (string?)Parse(await Merge(c, SigC, SigC))["reason"]);
            Assert.Equal("1", await ScalarAsync(c, "SELECT count(*) FROM config.investigation_signature_merges"));
            /* A second signature may still join the same target. */
            Assert.Equal("merged", (string?)Parse(await Merge(c, SigC, SigB))["status"]);
            Assert.Equal(new Dictionary<string, int> { [SigB] = 9 }, await TodayAsync(c));
        });
    }
}
