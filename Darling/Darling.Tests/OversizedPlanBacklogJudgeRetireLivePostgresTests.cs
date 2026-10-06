/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the one test here mints its own scratch database through ScratchPostgres and touches nothing
   on the shared one. */

/// <summary>
/// #5320: a plan the judging budget keeps failing to cover ends. The store half of that is the part a unit test of
/// <see cref="OversizedPlanBacklogSweep.JudgeFetchedPlan"/> cannot reach: the claim carries <c>attempt_count</c>, a
/// first failure leaves the row claimable, and the terminal marker is stored once and the claim never returns the
/// row again.
/// </summary>
public sealed class OversizedPlanBacklogJudgeRetireLivePostgresTests
{
    private const int ServerId = 4101;
    private const string PlanHandle = "0xOVERRUN";

    [Fact]
    public async Task APlanThatKeepsOverrunningTheJudgingBudget_IsClaimedOnce_ThenStoredAsTheMarker_AndNeverClaimedAgain()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live oversized-plan judging-retire test (it mints its own scratch database).");

        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        await using var connection = new NpgsqlConnection(scratch.ConnectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        await ExecuteAsync(connection,
            @"INSERT INTO collect.servers (server_id, server_name, is_enabled) VALUES (4101, 'fixture-server', TRUE);
INSERT INTO collect.oversized_plan_backlog
(server_id, collector_name, plan_handle, sql_handle, statement_start_offset, statement_end_offset,
 database_name, query_hash, observed_bytes, first_seen_at, last_seen_at)
VALUES (4101, 'query_stats', '0xOVERRUN', '0xSQL', 0, 400, 'fixture_db', '0xHASH',
        900000, '2026-09-10 08:00:00', '2026-09-13 08:00:00');", ct);

        var plan = StatementScrubCanary.CanaryPlan();

        /* Pass 1: the row has never been attempted, the plan overruns the budget, and the verdict leaves it claimable. */
        var first = await ClaimAsync(connection, ct);
        Assert.Equal(0, Assert.Single(first));

        var (firstVerdict, _, _) = OversizedPlanBacklogSweep.JudgeFetchedPlan(OverrunningSession(), plan, first[0]);
        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.JudgeTimedOut, firstVerdict);
        await RecordAsync(connection, OversizedPlanBacklogSweep.OutcomeSql(firstVerdict), null, ct);

        /* A connect failure between the passes, and an expiry that a re-sighting then clears, add no attempt (#5367
           review, A-M1): the row still has one judge timeout against it, so the next overrun is the terminal one. */
        await RecordAsync(connection, OversizedPlanBacklogSweep.OutcomeSql(OversizedPlanBacklogSweep.PlanFetchVerdict.Failed), null, ct);
        Assert.Equal(1, Assert.Single(await ClaimAsync(connection, ct)));
        await RecordAsync(connection, OversizedPlanBacklogSweep.OutcomeSql(OversizedPlanBacklogSweep.PlanFetchVerdict.Expired), null, ct);
        await ExecuteAsync(connection, "UPDATE collect.oversized_plan_backlog SET expired_at = NULL WHERE plan_handle = '0xOVERRUN';", ct);

        /* Pass 2: the claim hands the row back with its attempt count, and the second overrun is terminal. */
        var second = await ClaimAsync(connection, ct);
        Assert.Equal(1, Assert.Single(second));

        var (secondVerdict, marker, _) = OversizedPlanBacklogSweep.JudgeFetchedPlan(OverrunningSession(), plan, second[0]);
        Assert.Equal(OversizedPlanBacklogSweep.PlanFetchVerdict.Captured, secondVerdict);
        Assert.Equal(SensitiveStatements.PlaceholderText, marker);
        await RecordAsync(connection, OversizedPlanBacklog.RecordCaptureSql, marker, ct);

        /* Pass 3: resolved. Nothing to claim, and the stored content is the marker and nothing else. */
        Assert.Empty(await ClaimAsync(connection, ct));

        await using var read = new NpgsqlCommand(
            "SELECT plan_xml, captured_at IS NOT NULL, attempt_count FROM collect.oversized_plan_backlog WHERE plan_handle = '0xOVERRUN';",
            connection);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct));
        Assert.Equal(SensitiveStatements.PlaceholderText, reader.GetString(0));
        Assert.True(reader.GetBoolean(1));
        Assert.Equal(2, reader.GetInt32(2));
    }

    private static SensitiveStatements.Session OverrunningSession()
    {
        var now = TimeSpan.Zero;
        return new SensitiveStatements.Session(
            _ =>
            {
                now += TimeSpan.FromSeconds(1);
                return SensitiveStatements.Verdict.Clean;
            },
            () => now,
            TimeSpan.FromMilliseconds(1500),
            new SensitiveStatements.TimedOutMemo());
    }

    private static async Task<List<int>> ClaimAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        var attempts = new List<int>();
        await using var command = new NpgsqlCommand(
            OversizedPlanBacklog.ClaimSql(OversizedPlanBacklogSweep.MaxPlansPerServerPerTick), connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            /* The same ordinal the sweep's own claim reads. */
            attempts.Add(reader.GetInt32(7));
        }

        return attempts;
    }

    private static async Task RecordAsync(NpgsqlConnection connection, string sql, string? content, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = ServerId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = "query_stats" });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = PlanHandle });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = "0xSQL" });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 0 });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = 400 });
        command.Parameters.Add(new NpgsqlParameter
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Unspecified),
        });
        if (content is not null)
        {
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = content });
        }

        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }
}
