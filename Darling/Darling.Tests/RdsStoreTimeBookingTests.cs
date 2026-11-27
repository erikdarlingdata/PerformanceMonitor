/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9523: an AWS-API collector run books its store calls as storage time and the rest of the run, which is the AWS
/// calls, as the fetch. A run booked whole as storage time reads as a slow store when the time is spent waiting on
/// AWS.
///
/// <para><b>No timing.</b> The ingestors read their store time from a tick source the test owns
/// (<c>StoreClock</c>). The fake RDS client moves it by a stated amount inside each AWS call and the row writer
/// moves it by a stated amount inside the write, so each expected figure is an exact number.</para>
/// </summary>
public sealed class RdsStoreTimeBookingTests
{
    /* A store nothing can connect to: every test here writes through RowWriter, so reaching it would be a fault. */
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=15";

    private const string Host = "solo.abc123.us-east-1.rds.amazonaws.com";

    private const long ListingMs = 300;
    private const long DownloadMs = 400;
    private const long WriteMs = 40;

    /* The PostgreSQL 17.11 deadlock report RdsDeadlockIngestorTests reads, which parses to one deadlock. */
    private const string DeadlockText =
        "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151ERROR:  deadlock detected\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
        + "\tProcess 1549: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=1; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=2; COMMIT;\n"
        + "\tProcess 1556: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=2; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=1; COMMIT;\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151HINT:  See server log for query details.\n";

    /* Log traffic with nothing in it to store. */
    private const string QuietText =
        "2026-08-26 22:30:00.000 UTC [1600] LOG:  checkpoint starting: time\n"
        + "2026-08-26 22:30:12.000 UTC [1600] LOG:  checkpoint complete: wrote 42 buffers\n";

    /// <summary>A tick source the test moves, in <see cref="Stopwatch.Frequency"/> ticks a second.</summary>
    private sealed class ManualClock
    {
        public long Ticks;

        public long ElapsedMs => Ticks * 1000 / Stopwatch.Frequency;

        public void Advance(long milliseconds) => Ticks += milliseconds * Stopwatch.Frequency / 1000;
    }

    /// <summary>An RDS client whose listing and download each take a stated time on the test's clock.</summary>
    private sealed class SlowRds : AmazonRDSClient
    {
        private readonly ManualClock _clock;
        private readonly string _body;

        public SlowRds(ManualClock clock, string body)
            : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1)
        {
            _clock = clock;
            _body = body;
        }

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
        {
            _clock.Advance(ListingMs);

            return Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails>
                {
                    new() { LogFileName = "error/postgresql.log.2026-08-26-22", LastWritten = 9999 },
                },
            });
        }

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            _clock.Advance(DownloadMs);

            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = _body,
                Marker = "MARKER-1",
                AdditionalDataPending = false,
            });
        }
    }

    [Fact]
    public async Task ADeadlockCycle_ReportsTheRowWriteAsItsStoreTime_AndNoneOfTheAwsCalls()
    {
        var clock = new ManualClock();
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var ingestor = new RdsDeadlockIngestor(store, new RdsLogSource(_ => new SlowRds(clock, DeadlockText)))
        {
            StoreClock = () => clock.Ticks,
            RowWriter = (rows, _) =>
            {
                clock.Advance(WriteMs);
                return Task.FromResult(rows.Count);
            },
        };

        var outcome = await ingestor.IngestAsync(1, "target-a", Host);

        Assert.Equal(1, outcome.Rows);
        Assert.Equal(WriteMs, outcome.StoreMs);

        /* The AWS calls did run and did take their time: the cycle is 740 ms, of which 40 is the store's. */
        Assert.Equal(ListingMs + DownloadMs + WriteMs, clock.ElapsedMs);
    }

    [Fact]
    public async Task ACycleWithNothingToStore_ReportsNoStoreTime_HoweverLongAwsTook()
    {
        var clock = new ManualClock();
        var writes = 0;
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var ingestor = new RdsDeadlockIngestor(store, new RdsLogSource(_ => new SlowRds(clock, QuietText)))
        {
            StoreClock = () => clock.Ticks,
            RowWriter = (rows, _) =>
            {
                writes++;
                clock.Advance(WriteMs);
                return Task.FromResult(rows.Count);
            },
        };

        var outcome = await ingestor.IngestAsync(1, "target-a", Host);

        Assert.True(outcome.SourceReached);
        Assert.Equal(0, outcome.Rows);
        Assert.Equal(0, writes);
        Assert.Equal(0, outcome.StoreMs);
        Assert.Equal(ListingMs + DownloadMs, clock.ElapsedMs);
    }

    [Fact]
    public async Task ALogEventCycle_ReportsTheRowWriteAsItsStoreTime()
    {
        var clock = new ManualClock();
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var ingestor = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, new RdsLogSource(_ => new SlowRds(clock, DeadlockText)))
        {
            StoreClock = () => clock.Ticks,
            RowWriter = (events, _) =>
            {
                clock.Advance(WriteMs);
                return Task.FromResult(events.Count);
            },
        };

        var outcome = await ingestor.IngestAsync(1, "target-a", Host, logTimezoneIsUtc: true);

        Assert.True(outcome.Rows > 0, "the deadlock report classifies to at least one log event");
        Assert.Equal(WriteMs, outcome.StoreMs);
    }

    [Fact]
    public async Task APlanCycle_ReportsTheRowWriteAsItsStoreTime()
    {
        var planText = File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "auto_explain_real_block.txt"));
        var clock = new ManualClock();
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var ingestor = new RdsPlanIngestor(store, new RdsLogSource(_ => new SlowRds(clock, planText)))
        {
            StoreClock = () => clock.Ticks,
            RowWriter = (rows, _) =>
            {
                clock.Advance(WriteMs);
                return Task.FromResult(rows.Count);
            },
        };

        var outcome = await ingestor.IngestAsync(1, "target-a", Host);

        Assert.True(outcome.Rows > 0, "the auto_explain capture holds at least one plan");
        Assert.Equal(WriteMs, outcome.StoreMs);
    }

    [Fact]
    public async Task AStoreCallThatThrows_StillCountsItsTime()
    {
        var clock = new ManualClock();
        var phase = new RdsStorePhase(() => clock.Ticks);

        await Assert.ThrowsAsync<InvalidOperationException>(() => phase.TimeAsync(() =>
        {
            clock.Advance(25);
            throw new InvalidOperationException("the store refused the write");
        }));

        var kept = await phase.TimeAsync(() =>
        {
            clock.Advance(15);
            return Task.FromResult(7);
        });

        Assert.Equal(7, kept);
        Assert.Equal(40, phase.ElapsedMs);
    }

    /// <summary>
    /// The runner's booking: the store time is storage time, the rest of the run is the fetch, and the two add up to
    /// the run. A 740 ms cycle whose store calls take 40 ms books 700 ms of fetch and 40 ms of storage.
    /// </summary>
    [Theory]
    [InlineData(740, 40, 700, 40)]
    [InlineData(700, 0, 700, 0)]
    [InlineData(55, 55, 0, 55)]
    public void ARun_IsBookedAsTheFetchPlusTheStoreTime(long runMs, long storeMs, long expectedSqlMs, long expectedStorageMs)
    {
        var (sqlMs, storageMs) = DarlingCollectorRunner.RdsRunPhases(runMs, storeMs);

        Assert.Equal(expectedSqlMs, sqlMs);
        Assert.Equal(expectedStorageMs, storageMs);
        Assert.Equal(runMs, sqlMs + storageMs);
    }

    /// <summary>The two figures come off separate clocks, so a store time past the run, or below zero, is held to the run.</summary>
    [Theory]
    [InlineData(100, 250, 0, 100)]
    [InlineData(100, -5, 100, 0)]
    [InlineData(-3, 10, 0, 0)]
    public void AStoreTimeOutsideTheRun_IsHeldToIt(long runMs, long storeMs, long expectedSqlMs, long expectedStorageMs)
    {
        var (sqlMs, storageMs) = DarlingCollectorRunner.RdsRunPhases(runMs, storeMs);

        Assert.Equal(expectedSqlMs, sqlMs);
        Assert.Equal(expectedStorageMs, storageMs);
    }
}
