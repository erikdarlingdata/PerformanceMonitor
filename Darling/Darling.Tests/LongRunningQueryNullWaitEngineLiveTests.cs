/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the test mints its own scratch database through ScratchPostgres and touches nothing on the shared one, so the class is
   not serialized against the live-postgres collection. */

/// <summary>
/// The Long-Running Query alert sample by sample on a real PostgreSQL store (#9081): seeded <c>query_snapshots</c>, the real
/// <see cref="DarlingAlertReadAdapter"/> read with every opt-out on, and the real <see cref="AlertEngine"/> on an injected clock. A request
/// that is over the threshold in a wait, then running with no wait, then gone, fires once and clears once, and the clear belongs to the
/// sample that no longer has the request.
/// </summary>
public sealed class LongRunningQueryNullWaitEngineLiveTests
{
    private const int ServerId = 9081001;
    private const string ServerName = "lrq-null-wait-live";
    private const string FireMetric = "Long-Running Query";
    private const string ClearTitle = "Long-Running Queries Cleared";

    private static string? Pg => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    private static async Task InsertRequestAsync(
        NpgsqlConnection connection, long collectionId, DateTime collectionTime, int sessionId, long elapsedMs, string? waitType, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            @"INSERT INTO query_snapshots (collection_id, collection_time, server_id, server_name, session_id, database_name, query_text, total_elapsed_time_ms, cpu_time_ms, reads, writes, wait_type, blocking_session_id, query_hash, program_name, login_name)
              VALUES ($1, $2, $3, $4, $5, 'StackOverflow', 'SELECT COUNT(*) FROM Posts', $6, 1000, 10, 2, $7, 0, '0x9AAF0129E4E9AD07', 'HammerDB', 'app_user')", connection);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, collectionId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, collectionTime);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, ServerId);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, ServerName);
        command.Parameters.AddWithValue(NpgsqlDbType.Integer, sessionId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bigint, elapsedMs);
        command.Parameters.AddWithValue(NpgsqlDbType.Text, (object?)waitType ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(ct);
    }

    [Fact]
    public async Task ARequestThatStopsWaiting_StaysInTheAlert_AndClearsOnceWhenItIsGone()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(Pg), "Set DARLING_TEST_PG to a Postgres connection string to run the live long-running sequence test.");
        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(Pg!, ct);
        var ownerString = new NpgsqlConnectionStringBuilder(scratch.ConnectionString) { SearchPath = PgSchemaGenerator.SearchPath }.ConnectionString;
        await using var postgres = NpgsqlDataSource.Create(ownerString);
        await using var connection = new NpgsqlConnection(ownerString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, null, ct);

        /* The read keeps the newest collection that is under 10 minutes old on the store's clock, so the three samples sit 3, 2 and 1
           minutes before the instant the test starts. The engine's clock is the harness's and moves a minute a sweep, inside the
           5-minute cooldown, so the one fire below is the only one the sequence can produce. */
        var started = DateTime.UtcNow;
        var anchor = new DateTime(started.Ticks - (started.Ticks % TimeSpan.TicksPerSecond), DateTimeKind.Unspecified);
        var sample1 = anchor.AddMinutes(-3);
        var sample2 = anchor.AddMinutes(-2);
        var sample3 = anchor.AddMinutes(-1);

        var h = new AlertEngineTests.Harness { Now = DateTime.SpecifyKind(anchor, DateTimeKind.Utc) };
        h.Settings.LongRunningQueryEnabled = true;
        var engine = new AlertEngine(h.Settings, new DarlingAlertReadAdapter(postgres), h.StateStore, h.Deliverer,
            isAlertMuted: _ => false,
            resolutionCallback: (resolution, _) => { h.Resolutions.Add(resolution); return Task.CompletedTask; },
            utcNow: () => h.Now);
        var key = ServerId.ToString(CultureInfo.InvariantCulture);

        async Task SweepAsync()
        {
            await engine.EvaluateServerAsync(
                new AlertServerSnapshot(key, ServerName, true, null, null, false, false, null) { ServerId = ServerId }, ct);
            h.Now = h.Now.AddMinutes(1);
        }

        int Fires() => h.Deliverer.Outcomes.Count(o => o.MetricName == FireMetric);
        int Clears() => h.Resolutions.Count(r => r.Title == ClearTitle);

        /* Sample 1: the request is 40 minutes in, over the 30-minute threshold, and in a wait. */
        await InsertRequestAsync(connection, 1, sample1, sessionId: 71, elapsedMs: 2_400_000, waitType: "CXPACKET", ct);
        await SweepAsync();
        Assert.Equal(1, Fires());
        Assert.Equal(40d, h.Deliverer.Outcomes.Single(o => o.MetricName == FireMetric).NumericCurrentValue);
        Assert.Equal(0, Clears());

        /* The next sweep comes before the next sample and reads sample 1 again. */
        await SweepAsync();
        Assert.Equal(1, Fires());
        Assert.Equal(0, Clears());

        /* Sample 2: the same request, 41 minutes in, running with no wait. It is still over the threshold. */
        await InsertRequestAsync(connection, 2, sample2, sessionId: 71, elapsedMs: 2_460_000, waitType: null, ct);
        await SweepAsync();
        Assert.Equal(1, Fires());
        Assert.Equal(0, Clears());

        /* Sample 3: the request is gone. The sample holds one other request, 5 seconds in, so it is the newest collection. */
        await InsertRequestAsync(connection, 3, sample3, sessionId: 72, elapsedMs: 5_000, waitType: null, ct);
        await SweepAsync();
        Assert.Equal(1, Fires());
        Assert.Equal(1, Clears());
        Assert.Equal(ClearTitle, Assert.Single(h.Resolutions).Title);
    }
}
