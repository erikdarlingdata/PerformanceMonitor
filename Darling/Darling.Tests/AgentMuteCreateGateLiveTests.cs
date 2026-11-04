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
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: every fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT
   [Collection("live-postgres")]. */

/// <summary>
/// #7414: lock order between an agent's mute create and an operator's. The agent's write locks the agent switch row; an operator's insert
/// reaches that same row through the mute rules trigger and holds the in-process create gate meanwhile. PostgreSQL cannot see the gate, so an
/// agent that locked the row and then waited on the gate waited on an operator who waited on the row, until the operator's 10 second timeout.
/// The gate now comes first on the agent side too, so the agent never holds the row while it waits on the gate.
/// </summary>
public sealed class AgentMuteCreateGateLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static readonly DarlingAgentActionSeat Agent = new(new DarlingMcpWritePolicy.Effective(false, false, Investigations: true));

    private static string Iso(DateTime utc) => utc.ToString("o", CultureInfo.InvariantCulture);

    /// <summary>An operator's create that is inside the create gate and stays there until <see cref="Release"/> is set.</summary>
    private sealed class HoldingMuteRuleStore : IMuteRuleStore
    {
        private readonly FakeMuteRuleStore _inner = new();
        public TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);
        public TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<IReadOnlyList<MuteRule>> LoadAllAsync(CancellationToken cancellationToken = default)
        {
            Entered.TrySetResult();
            await Release.Task;
            return await _inner.LoadAllAsync(cancellationToken);
        }

        public Task InsertAsync(MuteRule rule) => _inner.InsertAsync(rule);
        public Task UpdateAsync(MuteRule rule) => _inner.UpdateAsync(rule);
        public Task SetEnabledAsync(string ruleId, bool enabled) => _inner.SetEnabledAsync(ruleId, enabled);
        public Task DeleteAsync(string ruleId) => _inner.DeleteAsync(ruleId);
        public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds) => _inner.DeleteExpiredAsync(expiredIds);
    }

    private static async Task<bool> SwitchRowIsFreeAsync(NpgsqlDataSource source, CancellationToken ct)
    {
        await using var probe = await source.OpenConnectionAsync(ct);
        await using var transaction = await probe.BeginTransactionAsync(ct);
        try
        {
            await using var command = new NpgsqlCommand("SELECT agent_actions_enabled FROM config.config_service WHERE id = 1 FOR NO KEY UPDATE NOWAIT", probe, transaction);
            await command.ExecuteScalarAsync(ct);
            return true;
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.LockNotAvailable)
        {
            return false;
        }
    }

    private static async Task RunAsync(Func<NpgsqlDataSource, CancellationToken, Task> body)
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the agent mute create gate live tests.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await using var source = NpgsqlDataSource.Create(scratch.ConnectionString);
        await body(source, ct);
    }

    /// <summary>An operator's create is inside the gate. The agent's create must wait at the gate WITHOUT the switch row (a row an agent holds
    /// blocks the operator's insert), then complete once the operator's create lets go.</summary>
    [Fact]
    public async Task AnAgentCreate_WaitsAtTheGateWithoutHoldingTheSwitchRow_WhileAnOperatorsCreateIsInsideIt()
    {
        await RunAsync(async (source, ct) =>
        {
            /* One agent create first, so its code is compiled and its connection is open: the scenario's agent then reaches its row lock in
               milliseconds, and the wait below is long enough to see the old order take it. */
            var warm = await DarlingMcpAlertTools.CreateMuteRule(source, metric_name: "Warm Up", reason: "warm up",
                expires_at: Iso(DateTime.UtcNow.AddDays(1)), action: "digest", agentSeat: Agent);
            Assert.Equal("created", (string?)JsonNode.Parse(warm)!["status"]);

            var holding = new HoldingMuteRuleStore();
            var operatorCreate = DarlingMcpAlertTools.CreateMuteRuleOver(holding, null, "Operator Metric", null, null, null, null, "operator rule", null);
            await holding.Entered.Task.WaitAsync(TimeSpan.FromSeconds(30), ct);

            var agentCreate = DarlingMcpAlertTools.CreateMuteRule(source, metric_name: "Agent Metric", reason: "alert storm after the collector change",
                expires_at: Iso(DateTime.UtcNow.AddDays(1)), action: "digest", agentSeat: Agent);

            /* The new order never locks the row while it waits on the gate, so this loop cannot fail on a slow machine. It does not prove the
               old stall: on the old order this test still passed, because the probe did not catch the held row inside its window. */
            for (var i = 0; i < 75; i++)
            {
                Assert.True(await SwitchRowIsFreeAsync(source, ct), "At " + i + ": The agent's create holds the agent switch row while it waits on the create gate.");
                await Task.Delay(40, ct);
            }

            Assert.False(agentCreate.IsCompleted);
            holding.Release.SetResult();
            Assert.Equal("created", (string?)JsonNode.Parse(await operatorCreate.WaitAsync(TimeSpan.FromSeconds(30), ct))!["status"]);
            Assert.Equal("created", (string?)JsonNode.Parse(await agentCreate.WaitAsync(TimeSpan.FromSeconds(30), ct))!["status"]);
        });
    }

    /// <summary>Operator creates and agent creates started together all finish, in either order, with every rule written.</summary>
    [Fact]
    public async Task AgentAndOperatorCreatesStartedTogether_AllComplete()
    {
        await RunAsync(async (source, ct) =>
        {
            var tasks = new List<Task<string>>();
            for (var i = 0; i < 8; i++)
            {
                var n = i;
                tasks.Add(Task.Run(() => DarlingMcpAlertTools.CreateMuteRuleOver(new PgMuteRuleStore(source), null, "Operator Metric " + n, null, null, null, null,
                    "operator rule", null), ct));
                tasks.Add(Task.Run(() => DarlingMcpAlertTools.CreateMuteRule(source, metric_name: "Agent Metric " + n, reason: "alert storm",
                    expires_at: Iso(DateTime.UtcNow.AddDays(1)), action: "digest", agentSeat: Agent), ct));
            }

            var answers = await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(60), ct);
            Assert.All(answers, a => Assert.Equal("created", (string?)JsonNode.Parse(a)!["status"]));
        });
    }
}
