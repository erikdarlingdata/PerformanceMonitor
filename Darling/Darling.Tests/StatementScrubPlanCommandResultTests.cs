/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5320 (the plan viewers half of #4348), the Darling service's side. The <c>fetch_plan</c> and
/// <c>execute_actual_plan</c> commands read a plan live from the monitored server and report it in
/// <c>config.config_command.result_json</c>. The row stays in the store until the terminal-command purge when the
/// viewer never reads it, so the plan is judged by the statement filter BEFORE the result is written
/// (<c>DarlingWorker.PlanResultOutcome</c>): the stored result never holds the raw text.
/// </summary>
/* The round-trip rows go through config.config_command, the shared live store: the collection serializes this class
   with the other live classes so its inserts and deletes cannot race another class's assertions. */
[Collection("live-postgres")]
public sealed class StatementScrubPlanCommandResultTests
{
    /// <summary>Distinctive sentinel, like the other command round-trip tests: a real server_id is a hash.</summary>
    private const int SentinelServerId = -515152;

    private static string PlanXmlOf(string? resultJson)
    {
        using var document = JsonDocument.Parse(resultJson!);
        Assert.True(document.RootElement.GetProperty("success").GetBoolean());
        return document.RootElement.GetProperty("planXml").GetString()!;
    }

    private static void AssertFiltered(string plan)
    {
        foreach (var needle in StatementScrubCanary.SecretNeedles)
        {
            Assert.DoesNotContain(needle, plan, StringComparison.Ordinal);
        }

        foreach (var needle in StatementScrubCanary.KeptNeedles)
        {
            Assert.Contains(needle, plan, StringComparison.Ordinal);
        }
    }

    [Theory]
    [InlineData("plan fetched")]
    [InlineData("actual plan captured")]
    public void PlanResultOutcome_JudgesThePlanBeforeItIsSerialized(string resultStatus)
    {
        var raw = StatementScrubCanary.CanaryPlan();

        var outcome = DarlingWorker.PlanResultOutcome(resultStatus, raw);

        Assert.True(outcome.Success);
        Assert.Equal(resultStatus, outcome.ResultStatus);
        AssertFiltered(PlanXmlOf(outcome.ResultJson));
    }

    [Fact]
    public void PlanResultOutcome_ANamedPlanThatCannotBeJudged_IsStoredAsTheMarker_AndAPlainPlanIsUnchanged()
    {
        var plain = "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"><BatchSequence/></ShowPlanXML>";
        Assert.Equal(plain, PlanXmlOf(DarlingWorker.PlanResultOutcome("plan fetched", plain).ResultJson));

        /* What the filter hands back for this text is what is stored, so whatever it withholds whole is the marker. */
        var odd = "<ShowPlanXML><unclosed";
        Assert.Equal(SensitiveStatements.Xml(odd), PlanXmlOf(DarlingWorker.PlanResultOutcome("plan fetched", odd).ResultJson));
    }

    /// <summary>A host that answers each plan command the way the worker's handlers do after the live read: through
    /// <c>DarlingWorker.PlanResultOutcome</c>, with the raw canary plan the monitored server would have returned.</summary>
    private sealed class RawPlanHost : IDarlingCommandHost
    {
        public Task<CommandOutcome> FetchPlanAsync(int serverId, PlanFetchRequest request, CancellationToken cancellationToken)
            => Task.FromResult(DarlingWorker.PlanResultOutcome("plan fetched", StatementScrubCanary.CanaryPlan()));

        public Task<CommandOutcome> ExecuteActualPlanAsync(int serverId, ActualPlanRequest request, CancellationToken cancellationToken)
            => Task.FromResult(DarlingWorker.PlanResultOutcome("actual plan captured", StatementScrubCanary.CanaryPlan()));

        public Task<CommandOutcome> SnapshotNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> AnalyzeNowAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> PurgeNowAsync(int? customRetentionDays, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> TestHypotheticalIndexAsync(int serverId, HypotheticalIndexRequest request, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");

        public Task<CommandOutcome> FetchActiveQueriesLiveAsync(int serverId, CancellationToken cancellationToken)
            => throw new InvalidOperationException("not this command");
    }

    [Theory]
    [InlineData("fetch_plan", "{\"planHandle\":\"0x0600AB\",\"databaseName\":\"master\"}", "plan fetched")]
    [InlineData("execute_actual_plan", "{\"queryHash\":\"0x0102030405060708\",\"databaseName\":\"master\"}", "actual plan captured")]
    public async Task ThePlanCommandPath_NeverWritesTheRawPlanIntoResultJson(string commandType, string argsJson, string expectedStatus)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the plan command result_json round-trip.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DeleteSentinelRowsAsync(connection, ct);

            long commandId;
            using (var insert = new NpgsqlCommand(
                "INSERT INTO config.config_command (command_type, target_server_id, args_json, status) " +
                "VALUES ($1, $2, $3::jsonb, 'pending') RETURNING command_id", connection))
            {
                insert.Parameters.AddWithValue(commandType);
                insert.Parameters.AddWithValue(SentinelServerId);
                insert.Parameters.AddWithValue(argsJson);
                commandId = Convert.ToInt64(await insert.ExecuteScalarAsync(ct));
            }

            await using var postgres = NpgsqlDataSource.Create(connectionString!);
            var executor = new DarlingCommandExecutor(postgres, new RawPlanHost(), "test-instance", null);

            var claimed = await executor.ClaimNextAsync(ct);
            Assert.NotNull(claimed);
            Assert.Equal(commandType, claimed!.CommandType);
            await executor.ExecuteAndReportAsync(claimed, ct);

            using var read = new NpgsqlCommand(
                "SELECT status, result_status, result_json::text FROM config.config_command WHERE command_id = $1", connection);
            read.Parameters.AddWithValue(commandId);
            using var reader = await read.ExecuteReaderAsync(ct);
            Assert.True(await reader.ReadAsync(ct));
            Assert.Equal("succeeded", reader.GetString(0));
            Assert.Equal(expectedStatus, reader.GetString(1));

            /* The stored result holds the filtered plan: the secret values the raw plan carried are not in the row. */
            var stored = reader.GetString(2);
            AssertFiltered(PlanXmlOf(stored));
            foreach (var needle in StatementScrubCanary.SecretNeedles)
            {
                Assert.DoesNotContain(needle, stored, StringComparison.Ordinal);
            }

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
            {
                await DeleteSentinelRowsAsync(cleanup, cleanupCt);
            });
        }
    }

    private static async Task DeleteSentinelRowsAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        using var delete = new NpgsqlCommand(
            $"DELETE FROM config.config_command WHERE target_server_id = {SentinelServerId}", connection);
        await delete.ExecuteNonQueryAsync(ct);
    }
}
