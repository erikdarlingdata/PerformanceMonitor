/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7528: a session row whose duration the collector did not measure (an autovacuum worker in state
/// <c>starting</c> has no query start) answered <c>peak_query_duration_ms: -1</c>. The numeric field now answers
/// null, the <c>*_duration</c> text twins keep saying "not measured", and a measured duration is unchanged.
/// </summary>
/* #1776 own-store: this test mints its own scratch store database through ScratchPostgres. */
[Collection("live-postgres")]
public sealed class PgSessionStatesUnmeasuredDurationsLiveTests
{
    private const int ServerId = -752800;
    private const string ServerName = "pg-session-unmeasured-durations";

    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task GetPgSessionStates_AnswersNullForUnmeasuredDurations_AndKeepsMeasuredOnes()
    {
        var baseConnectionString = ConnectionString;
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString), "Set DARLING_TEST_PG to a Postgres connection string to run the live session states test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        var bodySucceeded = false;
        try
        {
            await using var connection = new NpgsqlConnection(scratch.ConnectionString);
            await connection.OpenAsync(ct);
            await PgMigrations.MigrateAsync(connection, ct);
            await DarlingMcpTestData.RegisterServerAsync(connection, ServerId, ServerName, ct);

            var at = DateTime.UtcNow.AddMinutes(-10);
            var collectionId = CollectionIdGenerator.Next();
            const string insert =
                "INSERT INTO pg_session_states (collection_id, collection_time, server_id, server_name, backend_id, pid, state, xact_duration_ms, state_duration_ms, query_duration_ms, backend_duration_ms, horizon_age, is_idle_in_transaction, is_horizon_holder, state_is_redacted, total_sessions, active_sessions, idle_in_transaction_sessions, reportable_sessions) "
                + "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 5, TRUE, TRUE, FALSE, 10, 2, 4, 4)";

            /* Backend 1: a measured session. Backend 2: an autovacuum worker in 'starting', every duration NULL. */
            await DarlingMcpTestData.ExecAsync(connection, ct, insert, collectionId, DarlingMcpTestData.Naive(at), ServerId, ServerName,
                1L, 101, "idle in transaction", 60000L, 60000L, 1000L, 120000L);
            await DarlingMcpTestData.ExecAsync(connection, ct, insert, collectionId, DarlingMcpTestData.Naive(at), ServerId, ServerName,
                2L, 102, "starting", null, null, null, null);

            await using var postgres = NpgsqlDataSource.Create(scratch.ConnectionString);
            var sessions = JsonDocument.Parse(await DarlingMcpPgSessionStatesTools.GetPgSessionStates(postgres, ServerName, 24, 25, null, false, cancellationToken: ct))
                .RootElement.GetProperty("sessions");
            Assert.Equal(2, sessions.GetArrayLength());

            JsonElement Row(int pid)
            {
                foreach (var row in sessions.EnumerateArray())
                {
                    if (row.GetProperty("pid").GetInt32() == pid)
                    {
                        return row;
                    }
                }

                throw new InvalidOperationException("No session row for pid " + pid);
            }

            var measured = Row(101);
            Assert.Equal(60000, measured.GetProperty("peak_xact_duration_ms").GetInt64());
            Assert.Equal(60000, measured.GetProperty("peak_state_duration_ms").GetInt64());
            Assert.Equal(1000, measured.GetProperty("peak_query_duration_ms").GetInt64());
            Assert.Equal(120000, measured.GetProperty("backend_age_ms").GetInt64());

            var unmeasured = Row(102);
            foreach (var field in new[] { "peak_xact_duration_ms", "peak_state_duration_ms", "peak_query_duration_ms", "backend_age_ms" })
            {
                Assert.Equal(JsonValueKind.Null, unmeasured.GetProperty(field).ValueKind);
            }

            Assert.Equal("not measured", unmeasured.GetProperty("peak_state_duration").GetString());
            Assert.Equal("not measured", unmeasured.GetProperty("backend_age").GetString());

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
