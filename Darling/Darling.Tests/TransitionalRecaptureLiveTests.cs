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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;
using static Darling.Tests.ChangeWatchDetectionLiveTests;
using static Darling.Tests.ChangeWatchLiveTests;

namespace Darling.Tests;

/// <summary>
/// #9270: the early-recapture rule read from the stored <c>database_config</c> captures on a live store
/// (<see cref="DarlingWorker.QueryRecaptureDueAsync"/>), the Darling half of the rule the Lite tests run over DuckDB.
/// </summary>
/* #1776 own-store: every fact mints its own scratch database through ChangeWatchLiveTests.RunAsync, so the class is deliberately NOT [Collection("live-postgres")]. */
public sealed class TransitionalRecaptureLiveTests
{
    private static Task CaptureAsync(NpgsqlConnection c, DateTime at, string database, string state, CancellationToken ct) =>
        SeedStateAsync(c, at, database, state, "ON", ct);

    [Fact]
    public async Task ANewlyTransitionalDatabase_IsDueIn_TenMinutes()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-20), "Sales", "ONLINE", ct);
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-1), "Sales", "RESTORING", ct);

            Assert.True(await DarlingWorker.QueryRecaptureDueAsync(h.Source, ServerId, ct));
        });
    }

    [Fact]
    public async Task TheSeventhCaptureOfARun_KeepsTheNormalInterval_AndTheSixthDoesNot()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-100), "Sales", "ONLINE", ct);
            for (var i = 1; i <= TransitionalDatabaseRuns.MaxRecaptures; i++)
            {
                await CaptureAsync(h.Connection, h.Now.AddMinutes(-100 + (i * 10)), "Sales", "RESTORING", ct);
                Assert.True(await DarlingWorker.QueryRecaptureDueAsync(h.Source, ServerId, ct), "capture " + i + " of the run");
            }

            await CaptureAsync(h.Connection, h.Now.AddMinutes(-100 + ((TransitionalDatabaseRuns.MaxRecaptures + 1) * 10)), "Sales", "RESTORING", ct);

            Assert.False(await DarlingWorker.QueryRecaptureDueAsync(h.Source, ServerId, ct));
        });
    }

    [Fact]
    public async Task ADatabaseTransitionalSinceFirstSeen_KeepsTheNormalInterval()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-30), "Standby", "RESTORING", ct);
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-1), "Standby", "RESTORING", ct);

            Assert.False(await DarlingWorker.QueryRecaptureDueAsync(h.Source, ServerId, ct));
        });
    }

    [Fact]
    public async Task ARestartWithAStuckDatabasePresent_IsDueIn_TenMinutes_ForAnotherDatabaseNewlyRecovering()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await CaptureAsync(h.Connection, h.Now.AddDays(-3), "Stuck", "ONLINE", ct);
            await CaptureAsync(h.Connection, h.Now.AddDays(-3), "Other", "ONLINE", ct);
            for (var i = 0; i < TransitionalDatabaseRuns.MaxRecaptures + 2; i++)
            {
                await CaptureAsync(h.Connection, h.Now.AddDays(-2).AddMinutes(i * 10), "Stuck", "RECOVERY_PENDING", ct);
            }

            await CaptureAsync(h.Connection, h.Now.AddMinutes(-1), "Stuck", "RECOVERY_PENDING", ct);
            await CaptureAsync(h.Connection, h.Now.AddMinutes(-1), "Other", "RECOVERING", ct);

            Assert.True(await DarlingWorker.QueryRecaptureDueAsync(h.Source, ServerId, ct));
        });
    }

    [Fact]
    public async Task AStuckDatabaseAlone_KeepsTheNormalInterval_AndAnotherServersDatabaseDoesNotCount()
    {
        await RunAsync(true, async (h, ct) =>
        {
            await CaptureAsync(h.Connection, h.Now.AddDays(-3), "Stuck", "ONLINE", ct);
            for (var i = 0; i < TransitionalDatabaseRuns.MaxRecaptures + 2; i++)
            {
                await CaptureAsync(h.Connection, h.Now.AddDays(-2).AddMinutes(i * 10), "Stuck", "RECOVERY_PENDING", ct);
            }

            Assert.False(await DarlingWorker.QueryRecaptureDueAsync(h.Source, ServerId, ct));
            Assert.False(await DarlingWorker.QueryRecaptureDueAsync(h.Source, ServerId + 1, ct));
        });
    }
}
