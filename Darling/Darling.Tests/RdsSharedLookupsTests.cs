/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */


using System;
using System.Collections.Generic;
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
/// The AWS calls the RDS readers of one runner make, counted on a fake client (#9522): what a cycle costs when each
/// reader asks AWS itself, and what it costs when they share one <see cref="RdsLookupCache"/>.
///
/// <para>The clock is the cache's own, moved by the test, so a lifetime ends when the test says so. No test waits.
/// The store is a data source on a closed loopback port, as in <c>RdsDeadlockIngestorTests</c>: a write through the
/// ingestor's row hook succeeds, and a read of the store fails at the connection.</para>
/// </summary>
public sealed class RdsSharedLookupsTests
{
    private const string DeadStore =
        "Host=127.0.0.1;Port=1;Username=none;Password=none;Database=none;Timeout=15";

    private const string ClusterHost = "shared.cluster-abc123.us-east-1.rds.amazonaws.com";

    private static readonly DateTime Start = new(2026, 11, 25, 1, 0, 0, DateTimeKind.Utc);

    /* The PostgreSQL 17.11 deadlock report RdsDeadlockIngestorTests reads. It parses to one deadlock, and the log
       event reader classifies the same lines. */
    private const string DeadlockText =
        "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151ERROR:  deadlock detected\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151DETAIL:  Process 1549 waits for ShareLock on transaction 809; blocked by process 1556.\n"
        + "\tProcess 1556 waits for ShareLock on transaction 808; blocked by process 1549.\n"
        + "\tProcess 1549: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=1; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=2; COMMIT;\n"
        + "\tProcess 1556: \n"
        + "\tBEGIN; UPDATE dl SET v=v+1 WHERE id=2; SELECT pg_sleep(2); UPDATE dl SET v=v+1 WHERE id=1; COMMIT;\n"
        + "2026-08-26 22:25:24.100 UTC [1549] 322048460535975151HINT:  See server log for query details.\n";

    /// <summary>
    /// An RDS client that counts every call. Its one log file ends at <see cref="End"/>, and a download answers with
    /// that end as the marker, so a reader that commits it asks for "from there on" next time.
    /// </summary>
    private sealed class CountingRds : AmazonRDSClient
    {
        public CountingRds()
            : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1)
        {
        }

        /// <summary>The instance the cluster names as its writer.</summary>
        public string Writer { get; set; } = "writer-1";

        /// <summary>Instances that no longer exist: every call addressed to one is refused as AWS refuses it.</summary>
        public HashSet<string> Gone { get; } = new(StringComparer.Ordinal);

        public int End { get; set; } = 100;

        public string Body { get; set; } = "log body\n";

        /// <summary>When set, answers a download itself, for a file one read does not finish.</summary>
        public Func<DownloadDBLogFilePortionRequest, DownloadDBLogFilePortionResponse>? DownloadAnswer { get; set; }

        public int ClusterCalls { get; private set; }

        public int ListingCalls { get; private set; }

        public List<string> InstancesDescribed { get; } = new();

        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();

        public (int Cluster, int Listing, int Download) Calls => (ClusterCalls, ListingCalls, Downloads.Count);

        public override Task<DescribeDBClustersResponse> DescribeDBClustersAsync(
            DescribeDBClustersRequest request, CancellationToken cancellationToken = default)
        {
            ClusterCalls++;

            return Task.FromResult(new DescribeDBClustersResponse
            {
                DBClusters = new List<DBCluster>
                {
                    new()
                    {
                        DBClusterMembers = new List<DBClusterMember>
                        {
                            new() { DBInstanceIdentifier = "reader-1", IsClusterWriter = false },
                            new() { DBInstanceIdentifier = Writer, IsClusterWriter = true },
                        },
                    },
                },
            });
        }

        public override Task<DescribeDBInstancesResponse> DescribeDBInstancesAsync(
            DescribeDBInstancesRequest request, CancellationToken cancellationToken = default)
        {
            InstancesDescribed.Add(request.DBInstanceIdentifier);
            RefuseIfGone(request.DBInstanceIdentifier);

            return Task.FromResult(new DescribeDBInstancesResponse
            {
                DBInstances = new List<DBInstance> { new() { DbiResourceId = "db-" + request.DBInstanceIdentifier } },
            });
        }

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
        {
            ListingCalls++;
            RefuseIfGone(request.DBInstanceIdentifier);

            return Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails>
                {
                    new() { LogFileName = "error/postgresql.log.2026-11-25-01", LastWritten = 9999 },
                },
            });
        }

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads.Add(request);
            RefuseIfGone(request.DBInstanceIdentifier);

            return Task.FromResult(DownloadAnswer?.Invoke(request) ?? new DownloadDBLogFilePortionResponse
            {
                LogFileData = Body,
                Marker = End.ToString(System.Globalization.CultureInfo.InvariantCulture),
                AdditionalDataPending = false,
            });
        }

        private void RefuseIfGone(string instanceId)
        {
            if (Gone.Contains(instanceId))
            {
                throw new DBInstanceNotFoundException($"DBInstance {instanceId} not found.");
            }
        }
    }

    /// <summary>A clock the test moves.</summary>
    private sealed class Clock
    {
        public DateTime Now { get; set; } = Start;

        public void Advance(TimeSpan by) => Now += by;
    }

    /* The endpoint check is RdsEndpointVerifierTests' subject. It is off here so the calls counted are the readers'. */
    private static RdsLogSource Reader(CountingRds rds, RdsLookupCache? lookups)
        => new(_ => rds, verifier: RdsEndpointVerifier.ForTests(enforce: false), lookups: lookups);

    private static async Task<RdsLogSource.LogChunk> ReadAsync(RdsLogSource reader)
    {
        var chunk = await reader.ReadNewestAsync(ClusterHost);

        Assert.NotNull(chunk);

        return chunk.Value;
    }

    /// <summary>
    /// The count the issue is about. Three readers of one cluster in one cycle make three writer lookups, three
    /// listings and three downloads when each asks AWS itself, and one of each when they share a cache. Every reader
    /// still gets the text and its own position to commit.
    /// </summary>
    [Fact]
    public async Task ACycleOfThreeReaders_MakesOneOfEachCallThroughASharedCache_AndThreeWithout()
    {
        var alone = new CountingRds();

        for (var reader = 0; reader < 3; reader++)
        {
            await ReadAsync(Reader(alone, lookups: null));
        }

        Assert.Equal((3, 3, 3), alone.Calls);

        var shared = new CountingRds();
        var lookups = new RdsLookupCache(() => Start);
        var chunks = new List<RdsLogSource.LogChunk>();

        for (var reader = 0; reader < 3; reader++)
        {
            chunks.Add(await ReadAsync(Reader(shared, lookups)));
        }

        Assert.Equal((1, 1, 1), shared.Calls);
        Assert.All(chunks, chunk => Assert.Equal("log body\n", chunk.Text));
        Assert.All(chunks, chunk => Assert.Equal("100", chunk.Resume.Marker));
    }

    /// <summary>
    /// A file one read does not finish: the first reader makes two reads of it, and a second reader that starts
    /// where the first started follows both from the held answers.
    /// </summary>
    [Fact]
    public async Task AReaderThatFollowsAnother_SharesEveryReadOfTheCycle()
    {
        var rds = new CountingRds
        {
            DownloadAnswer = request => request.Marker is null
                ? new DownloadDBLogFilePortionResponse { LogFileData = "first part\n", Marker = "50", AdditionalDataPending = true }
                : new DownloadDBLogFilePortionResponse { LogFileData = "second part\n", Marker = "100", AdditionalDataPending = false },
        };

        var lookups = new RdsLookupCache(() => Start);
        var texts = new List<string>();

        for (var reader = 0; reader < 2; reader++)
        {
            var source = Reader(rds, lookups);

            var first = await ReadAsync(source);
            source.CommitResume(first.Resume);

            var second = await ReadAsync(source);
            source.CommitResume(second.Resume);

            texts.Add(first.Text + second.Text);
        }

        Assert.Equal(2, rds.Downloads.Count);
        Assert.Equal(new[] { "first part\nsecond part\n", "first part\nsecond part\n" }, texts);
    }

    /// <summary>
    /// The positions stay separate. A reader that did not store its rows keeps its position, and its next read asks
    /// AWS for its own window while the reader that committed asks for the bytes after its own. Once both have
    /// committed the same marker they share a download again.
    /// </summary>
    [Fact]
    public async Task AReaderLeftBehind_ReadsItsOwnWindow_AndTheOtherMovesOn()
    {
        var rds = new CountingRds();
        var clock = new Clock();
        var lookups = new RdsLookupCache(() => clock.Now);
        var ahead = Reader(rds, lookups);
        var behind = Reader(rds, lookups);

        /* Cycle 1: one download serves both. Only the first reader's rows reach the store. */
        ahead.CommitResume((await ReadAsync(ahead)).Resume);
        await ReadAsync(behind);

        Assert.Single(rds.Downloads);

        /* Cycle 2: the file has grown. Each reader asks from its own position. */
        clock.Advance(RdsLookupCache.PortionLifetime);
        rds.End = 250;

        var aheadChunk = await ReadAsync(ahead);
        var behindChunk = await ReadAsync(behind);

        Assert.Equal(3, rds.Downloads.Count);
        Assert.Equal("100", rds.Downloads[1].Marker);
        Assert.Null(rds.Downloads[2].Marker);

        ahead.CommitResume(aheadChunk.Resume);
        behind.CommitResume(behindChunk.Resume);

        /* Cycle 3: both hold the marker cycle 2 returned, so the second reader's request is the first one's. */
        clock.Advance(RdsLookupCache.PortionLifetime);
        rds.End = 400;

        await ReadAsync(ahead);
        await ReadAsync(behind);

        Assert.Equal(4, rds.Downloads.Count);
        Assert.Equal("250", rds.Downloads[3].Marker);
    }

    /// <summary>
    /// A rotation between two cycles, for two readers at one position. The rest of the old file is downloaded once
    /// and the new file's first read is downloaded once, and each reader commits both under its own position.
    /// </summary>
    [Fact]
    public async Task ARotatedFile_IsFinishedOnceForTheReadersAtOnePosition()
    {
        const string instanceHost = "solo.abc123.us-east-1.rds.amazonaws.com";
        const string oldFile = "error/postgresql.log.2026-11-25-00";
        const string newFile = "error/postgresql.log.2026-11-25-01";

        var rds = new RdsRotationFakeRds(oldFile) { Body = "log body\n" };
        var clock = new Clock();
        var lookups = new RdsLookupCache(() => clock.Now);

        var readers = new[]
        {
            new RdsLogSource(_ => rds, verifier: RdsEndpointVerifier.ForTests(enforce: false), lookups: lookups),
            new RdsLogSource(_ => rds, verifier: RdsEndpointVerifier.ForTests(enforce: false), lookups: lookups),
        };

        /* Cycle 1: both readers reach the same position in the only file. */
        foreach (var reader in readers)
        {
            var chunk = await reader.ReadNewestAsync(instanceHost);

            Assert.NotNull(chunk);
            reader.CommitResume(chunk.Value.Resume);
        }

        Assert.Single(rds.Downloads);

        /* The log rotates before cycle 2. */
        clock.Advance(RdsLookupCache.ListingLifetime);
        rds.Listed = new[] { oldFile, newFile };

        foreach (var reader in readers)
        {
            var rest = await reader.ReadNewestAsync(instanceHost);

            Assert.NotNull(rest);
            Assert.True(rest.Value.ReadAgain);
            reader.CommitResume(rest.Value.Resume);

            var first = await reader.ReadNewestAsync(instanceHost);

            Assert.NotNull(first);
            Assert.True(first.Value.StartsAtFileStart);
            reader.CommitResume(first.Value.Resume);
        }

        Assert.Equal(3, rds.Downloads.Count);
        Assert.Equal((oldFile, "M1"), (rds.Downloads[1].LogFileName, rds.Downloads[1].Marker));
        Assert.Equal((newFile, RdsLogSource.StartOfFileMarker), (rds.Downloads[2].LogFileName, rds.Downloads[2].Marker));
    }

    /// <summary>
    /// The failover that removes the old writer. The read after it goes to the held writer and fails, which is the
    /// one failed run. That failure drops the held writer, so the next read asks the cluster again and reads the new
    /// writer, with the lifetime still running.
    /// </summary>
    [Fact]
    public async Task AFailedCallOnTheHeldWriter_DropsIt_AndTheNextReadResolvesTheNewWriter()
    {
        var rds = new CountingRds();
        var clock = new Clock();
        var source = Reader(rds, new RdsLookupCache(() => clock.Now));

        source.CommitResume((await ReadAsync(source)).Resume);

        Assert.Equal((1, 1, 1), rds.Calls);

        rds.Writer = "writer-2";
        rds.Gone.Add("writer-1");
        clock.Advance(TimeSpan.FromSeconds(1));

        await Assert.ThrowsAsync<DBInstanceNotFoundException>(() => source.ReadNewestAsync(ClusterHost));

        Assert.Equal(1, rds.ClusterCalls);
        Assert.Equal("writer-1", rds.Downloads[^1].DBInstanceIdentifier);

        clock.Advance(TimeSpan.FromSeconds(1));

        await ReadAsync(source);

        Assert.Equal(2, rds.ClusterCalls);
        Assert.Equal(2, rds.ListingCalls);
        Assert.Equal("writer-2", rds.Downloads[^1].DBInstanceIdentifier);
        Assert.Null(rds.Downloads[^1].Marker);
    }

    /// <summary>
    /// The failover no call fails on: the old writer stays up as a reader, so its log calls still succeed. The held
    /// writer is used until its lifetime ends and not after it, which is how long the readers can follow the old one.
    /// </summary>
    [Fact]
    public async Task AWriterThatMovedWithoutAFailure_IsFollowedWhenTheLifetimeEnds()
    {
        var rds = new CountingRds();
        var clock = new Clock();
        var source = Reader(rds, new RdsLookupCache(() => clock.Now));

        source.CommitResume((await ReadAsync(source)).Resume);

        rds.Writer = "writer-2";
        clock.Advance(RdsLookupCache.WriterLifetime - TimeSpan.FromSeconds(1));

        source.CommitResume((await ReadAsync(source)).Resume);

        Assert.Equal(1, rds.ClusterCalls);
        Assert.Equal("writer-1", rds.Downloads[^1].DBInstanceIdentifier);

        clock.Advance(TimeSpan.FromSeconds(1));

        await ReadAsync(source);

        Assert.Equal(2, rds.ClusterCalls);
        Assert.Equal("writer-2", rds.Downloads[^1].DBInstanceIdentifier);
    }

    /// <summary>
    /// The two readers on one cadence, through their ingestors. A cycle is one writer lookup, one listing and one
    /// download for both. The log event reader's store write then fails: the deadlock reader has stored and
    /// committed already, and the next cycle gives each its own window.
    /// </summary>
    [Fact]
    public async Task ALogReaderWhoseStoreWriteFails_DoesNotHoldTheOtherReaderBack()
    {
        var rds = new CountingRds { Body = DeadlockText };
        var clock = new Clock();
        var lookups = new RdsLookupCache(() => clock.Now);
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var deadlocksStored = 0;
        var eventWritesFail = true;

        var deadlocks = new RdsDeadlockIngestor(store, Reader(rds, lookups))
        {
            RowWriter = (rows, _) =>
            {
                deadlocksStored += rows.Count;
                return Task.FromResult(rows.Count);
            },
        };

        var logEvents = new RdsLogEventIngestor(store, TestLogHashKeys.Fixed, Reader(rds, lookups))
        {
            RowWriter = (events, _) => eventWritesFail
                ? throw new InvalidOperationException("the store refused the write")
                : Task.FromResult(events.Count),
        };

        await deadlocks.IngestAsync(1, "target-a", ClusterHost);
        await Assert.ThrowsAnyAsync<InvalidOperationException>(
            () => logEvents.IngestAsync(1, "target-a", ClusterHost, logTimezoneIsUtc: true));

        Assert.Equal((1, 1, 1), rds.Calls);
        Assert.Equal(1, deadlocksStored);

        clock.Advance(RdsLookupCache.PortionLifetime);
        rds.End = 250;
        rds.Body = "2026-08-26 22:30:00.000 UTC [1600] LOG:  checkpoint starting: time\n";
        eventWritesFail = false;

        await deadlocks.IngestAsync(1, "target-a", ClusterHost);
        await logEvents.IngestAsync(1, "target-a", ClusterHost, logTimezoneIsUtc: true);

        Assert.Equal(3, rds.Downloads.Count);
        Assert.Equal("100", rds.Downloads[1].Marker);
        Assert.Null(rds.Downloads[2].Marker);
        Assert.Equal(1, deadlocksStored);
    }

    /// <summary>
    /// The Performance Insights reader. Two runs make two writer lookups and two resource id lookups when each run
    /// asks, and one of each through a cache. The resource id is kept past the writer: a moved writer is looked up
    /// under its own name, and the first writer's id is asked for again only after its own lifetime.
    /// Each run ends at the watermark read, which the closed store refuses, after both lookups.
    /// </summary>
    [Fact]
    public async Task TheCpuReader_LooksUpTheWriterAndTheResourceIdOnceThroughACache_AndEveryRunWithout()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);

        async Task RunAsync(CountingRds rds, RdsLookupCache? lookups)
        {
            var ingestor = new RdsCpuIngestor(
                store, rdsClientFactory: _ => rds, verifier: RdsEndpointVerifier.ForTests(enforce: false), lookups: lookups);

            var refused = await Assert.ThrowsAsync<PiMetricsUnavailableException>(
                () => ingestor.IngestAsync(1, "target-a", ClusterHost));

            Assert.IsAssignableFrom<NpgsqlException>(refused.InnerException);
        }

        var alone = new CountingRds();

        await RunAsync(alone, lookups: null);
        await RunAsync(alone, lookups: null);

        Assert.Equal(2, alone.ClusterCalls);
        Assert.Equal(new[] { "writer-1", "writer-1" }, alone.InstancesDescribed);

        var shared = new CountingRds();
        var clock = new Clock();
        var cache = new RdsLookupCache(() => clock.Now);

        await RunAsync(shared, cache);
        await RunAsync(shared, cache);

        Assert.Equal(1, shared.ClusterCalls);
        Assert.Equal(new[] { "writer-1" }, shared.InstancesDescribed);

        /* The writer's lifetime ends and the cluster names another instance. */
        clock.Advance(RdsLookupCache.WriterLifetime);
        shared.Writer = "writer-2";

        await RunAsync(shared, cache);

        Assert.Equal(2, shared.ClusterCalls);
        Assert.Equal(new[] { "writer-1", "writer-2" }, shared.InstancesDescribed);

        /* The writer moves back: the first instance's resource id is still held. */
        clock.Advance(RdsLookupCache.WriterLifetime);
        shared.Writer = "writer-1";

        await RunAsync(shared, cache);

        Assert.Equal(3, shared.ClusterCalls);
        Assert.Equal(new[] { "writer-1", "writer-2" }, shared.InstancesDescribed);

        clock.Advance(RdsLookupCache.ResourceIdLifetime);

        await RunAsync(shared, cache);

        Assert.Equal(new[] { "writer-1", "writer-2", "writer-1" }, shared.InstancesDescribed);
    }

    /// <summary>
    /// The Performance Insights reader after the failover that removes the old writer: the lookup addressed to the
    /// held writer fails, the held writer is dropped, and the next run resolves the new one inside the lifetime.
    /// </summary>
    [Fact]
    public async Task ACpuRunThatFailsOnTheHeldWriter_DropsIt_AndTheNextRunResolvesTheNewWriter()
    {
        await using var store = NpgsqlDataSource.Create(DeadStore);

        var rds = new CountingRds();
        var clock = new Clock();
        var cache = new RdsLookupCache(() => clock.Now);

        /* A log reader of the same cycle resolves the writer first, and the CPU reader takes it from the cache. */
        await ReadAsync(Reader(rds, cache));

        rds.Writer = "writer-2";
        rds.Gone.Add("writer-1");

        RdsCpuIngestor Cpu() => new(
            store, rdsClientFactory: _ => rds, verifier: RdsEndpointVerifier.ForTests(enforce: false), lookups: cache);

        var failed = await Assert.ThrowsAsync<PiMetricsUnavailableException>(() => Cpu().IngestAsync(1, "target-a", ClusterHost));

        Assert.IsType<DBInstanceNotFoundException>(failed.InnerException);
        Assert.Equal(1, rds.ClusterCalls);

        var next = await Assert.ThrowsAsync<PiMetricsUnavailableException>(() => Cpu().IngestAsync(1, "target-a", ClusterHost));

        Assert.IsAssignableFrom<NpgsqlException>(next.InnerException);
        Assert.Equal(2, rds.ClusterCalls);
        Assert.Equal(new[] { "writer-1", "writer-2" }, rds.InstancesDescribed);
    }

    /// <summary>
    /// A held portion is dropped when its lifetime ends, with no later request to replace it: the cache waits the
    /// lifetime out and then removes what has ended on its clock. A wait that ends early removes nothing.
    /// </summary>
    [Fact]
    public async Task AHeldPortion_IsDroppedWhenItsLifetimeEnds_WithoutALaterRequest()
    {
        var clock = new Clock();
        var waits = new List<TaskCompletionSource>();

        /* The wait is the test's to end. Its continuation runs inside SetResult, so the drop has happened when
           SetResult returns. */
        var cache = new RdsLookupCache(
            () => clock.Now,
            _ =>
            {
                var wait = new TaskCompletionSource();
                waits.Add(wait);
                return wait.Task;
            });

        var rds = new CountingRds();

        await ReadAsync(Reader(rds, cache));

        /* The writer, the listing and the portion. */
        Assert.Equal(3, cache.HeldCount);
        Assert.Equal(3, waits.Count);

        waits[0].SetResult();

        Assert.Equal(3, cache.HeldCount);

        clock.Advance(RdsLookupCache.PortionLifetime);
        waits[1].SetResult();

        Assert.Equal(0, cache.HeldCount);
    }

    /// <summary>
    /// The cache itself: two callers that ask at the same moment wait on one call, and a call that fails is not
    /// kept, so the caller that was waiting on it makes its own.
    /// </summary>
    [Fact]
    public async Task CallersThatArriveTogether_ShareOneCall_AndAFailedCallIsNotKept()
    {
        var cache = new RdsLookupCache(() => Start);
        var calls = 0;
        var held = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

        Task<string> Resolve()
        {
            calls++;
            return held.Task;
        }

        var first = cache.WriterAsync(null, "us-east-1", "shared", Resolve, CancellationToken.None);
        var second = cache.WriterAsync(null, "us-east-1", "shared", Resolve, CancellationToken.None);

        held.SetResult("writer-1");

        Assert.Equal("writer-1", await first);
        Assert.Equal("writer-1", await second);
        Assert.Equal(1, calls);

        var failing = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var owner = cache.WriterAsync(null, "us-east-1", "other", () => failing.Task, CancellationToken.None);
        var waiter = cache.WriterAsync(null, "us-east-1", "other", () => Task.FromResult("writer-9"), CancellationToken.None);

        failing.SetException(new DBClusterNotFoundException("no such cluster"));

        await Assert.ThrowsAsync<DBClusterNotFoundException>(() => owner);
        Assert.Equal("writer-9", await waiter);

        /* Nothing was kept for the failed call: the next caller asks again. */
        Assert.Equal(
            "writer-3",
            await cache.WriterAsync(null, "us-east-1", "other", () => Task.FromResult("writer-3"), CancellationToken.None));
    }
}
