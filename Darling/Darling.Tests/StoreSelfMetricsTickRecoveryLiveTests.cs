/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Data;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3971: a store with no readable server-log directory does not lose the hourly collector-cost flush.
///
/// <para><b>The reproduction, against a real store.</b> A fresh PostgreSQL cluster with
/// <c>logging_collector</c> off (the default, and the Linux compose store's shape) has no <c>log</c>
/// directory at all — <c>pg_ls_logdir()</c>, which <see cref="StoreLogSweep.SweepAsync"/> lists the capture
/// candidates through, fails with <c>58P01 could not open directory "log"</c> before this test does anything
/// to force it. And Npgsql does not leave the connection idle-but-usable after that ERROR: it closes it
/// outright, which is the fact <c>DarlingWorker.SweepStoreSelfMetricsAsync</c>'s next two passes — the
/// store-log re-mask (#3915) and the collector-cost flush (#2674) — ran into every hour, on the same
/// connection, before the fix beside this test.</para>
///
/// <para><b>Why this test does not call <c>SweepStoreSelfMetricsAsync</c> itself.</b> That method is a
/// private instance member of a <c>BackgroundService</c> with a full worker's worth of DI behind it, so —
/// like the re-mask pass beside it (<see cref="StoreLogRemaskLiveTests"/>) — the live behaviour is proven
/// against the real primitives it calls (<see cref="StoreLogSweep.SweepAsync"/> and
/// <see cref="CollectorCostAccumulator.FlushAsync"/>) on one connection, in the same order, while
/// <c>StoreLogSelfMonitoringStoreTests.TheSweepRidesTheHourlySelfMetricsTick</c> pins that the worker method
/// actually wires the reopen between them.</para>
///
/// <para><b>#1776 own-store</b>: it mints and migrates a scratch database through <c>ScratchPostgres</c>, so
/// it is not in the <c>live-postgres</c> collection.</para>
/// </summary>
public sealed class StoreSelfMetricsTickRecoveryLiveTests
{
    [Fact]
    public async Task AFailedStoreLogCapture_ClosesTheConnection_AndReopeningItLetsTheCollectorCostFlushWrite()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrWhiteSpace(baseConnectionString),
            "Set DARLING_TEST_PG to a superuser connection string to run the #3971 capture-recovery proof "
            + "in a scratch database.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);
        await using var c = new NpgsqlConnection(scratch.ConnectionString);
        await c.OpenAsync(ct);
        await PgMigrations.MigrateAsync(c, ct);

        /* The reproduction. No ALTER SYSTEM needed: logging_collector defaults to off, so a fresh cluster
           never creates its log directory, which is exactly the Linux compose store's shape (#3971's own
           evidence) rather than a condition this test manufactures. */
        var capture = await Assert.ThrowsAsync<PostgresException>(
            () => StoreLogSweep.SweepAsync(c, DateTime.UtcNow, NullLogger.Instance, ct));
        Assert.Equal("58P01", capture.SqlState);
        Assert.Contains("could not open directory", capture.Message, StringComparison.Ordinal);

        /* #3971's central claim: Npgsql does not leave this connection idle-but-usable after that ERROR. If
           this ever reads Open on a future Npgsql/PostgreSQL pairing, the reopen this test and the worker
           both do below becomes a no-op, not a wrong fix — but the claim itself needs to keep being true, or
           the rest of this test proves nothing. */
        Assert.Equal(ConnectionState.Closed, c.State);

        /* The fix (DarlingWorker.SweepStoreSelfMetricsAsync carries the same check): reopen before anything
           else on this connection runs. Disable this block (`if (false && ...)`) and the very next
           assertion reds with Expected Open / Actual Closed; remove that assertion too and the run reaches
           the collector-cost flush below and throws InvalidOperationException: "Connection is not open" —
           the exact failure #3971 reported for the re-mask and the collector-cost flush. Both are how this
           test was proven to fail on the code before this PR. */
        if (c.State != ConnectionState.Open)
        {
            await c.OpenAsync(ct);
        }

        Assert.Equal(ConnectionState.Open, c.State);

        /* The collector-cost flush still writes its row on the reopened connection — the outcome #3971 says
           was lost every hour on a store shaped like this one. */
        var accumulator = new CollectorCostAccumulator();
        accumulator.Record(serverId: 397_100, collectorName: "test_collector_3971", rows: 10, sqlMs: 5, storageMs: 2);
        await accumulator.FlushAsync(c, DateTime.UtcNow, NullLogger.Instance, ct);

        await using var read = new NpgsqlCommand(
            "SELECT run_count, total_sql_ms, max_sql_ms, total_storage_ms, total_rows "
            + "FROM collect.collector_cost WHERE server_id = 397100 AND collector_name = 'test_collector_3971'",
            c);
        await using var reader = await read.ExecuteReaderAsync(ct);
        Assert.True(await reader.ReadAsync(ct), "the collector-cost flush wrote no row for the reopened connection");
        Assert.Equal(1, reader.GetInt32(0));
        Assert.Equal(5L, reader.GetInt64(1));
        Assert.Equal(5L, reader.GetInt64(2));
        Assert.Equal(2L, reader.GetInt64(3));
        Assert.Equal(10L, reader.GetInt64(4));
        Assert.False(await reader.ReadAsync(ct), "more than one row landed for one Record call");
    }
}
