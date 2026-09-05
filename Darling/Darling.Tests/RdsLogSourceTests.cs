/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Collectors;

using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The RDS log transport (#2538), against a fake client. No AWS call is made and none is needed: what is
/// worth pinning is the DECISION-MAKING — which identifier gets used, which endpoints are refused, and
/// whether the resume marker advances — and all of that is ours rather than the SDK's.
/// </summary>
public class RdsLogSourceTests
{
    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"),
            Amazon.RegionEndpoint.USEast1) { }

        public List<DownloadDBLogFilePortionRequest> Downloads { get; } = new();
        public string? WriterId { get; init; }
        public string LogName { get; init; } = "error/postgresql.log.2026-08-25-18";
        public string? NextMarker { get; set; } = "MARKER-1";

        /* The three shapes a real answer takes that the populated fixtures above cannot produce. The AWS SDK
           leaves a response collection NULL rather than empty when the service omits it, so every fixture
           that sets its lists exercises only the populated half — which is why a null-source crash reached a
           production fleet past a green suite (#2996). */
        public bool OmitClusters { get; init; }
        public bool OmitClusterMembers { get; init; }
        public bool OmitLogFiles { get; init; }

        /// <summary>An empty-but-present list, which is the same FACT as an absent one and has to reach the
        /// same named branch — pre-#2996 the two diverged, one crashing and one answering silently.</summary>
        public bool EmptyLogFiles { get; init; }

        public override Task<DescribeDBClustersResponse> DescribeDBClustersAsync(
            DescribeDBClustersRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBClustersResponse
            {
                DBClusters = OmitClusters
                    ? null
                    : new List<DBCluster>
                    {
                        new()
                        {
                            DBClusterMembers = OmitClusterMembers
                                ? null
                                : new List<DBClusterMember>
                                {
                                    new() { DBInstanceIdentifier = "reader-1", IsClusterWriter = false },
                                    new() { DBInstanceIdentifier = WriterId ?? "writer-1", IsClusterWriter = true },
                                },
                        },
                    },
            });

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = OmitLogFiles
                    ? null
                    : EmptyLogFiles
                        ? new List<DescribeDBLogFilesDetails>()
                        : new List<DescribeDBLogFilesDetails>
                        {
                            new() { LogFileName = "error/postgresql.log.2026-08-09-01", LastWritten = 1000 },
                            new() { LogFileName = LogName, LastWritten = 9999 },
                        },
            });

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads.Add(request);
            return Task.FromResult(new DownloadDBLogFilePortionResponse
            {
                LogFileData = "log body",
                Marker = NextMarker,
                AdditionalDataPending = false,
            });
        }
    }

    private static (RdsLogSource Source, FakeRds Client) Build(FakeRds? client = null)
    {
        var fake = client ?? new FakeRds();
        return (new RdsLogSource(_ => fake), fake);
    }

    /// <summary>
    /// A cluster endpoint is resolved to its WRITER. <c>DownloadDBLogFilePortion</c> takes an instance
    /// identifier, so passing the cluster name through would fail with <c>DBInstanceNotFound</c> against a
    /// perfectly healthy cluster — which reads like a permissions problem and is not one.
    /// </summary>
    [Fact]
    public async Task AClusterEndpointResolvesToItsWriter()
    {
        var (source, client) = Build(new FakeRds { WriterId = "prod-writer" });

        await source.ReadNewestAsync("shared.cluster-abc123.us-east-1.rds.amazonaws.com");

        Assert.Single(client.Downloads);
        Assert.Equal("prod-writer", client.Downloads[0].DBInstanceIdentifier);
    }

    /// <summary>An instance endpoint is used directly — no cluster lookup to get wrong.</summary>
    [Fact]
    public async Task AnInstanceEndpointIsUsedAsIs()
    {
        var (source, client) = Build();

        await source.ReadNewestAsync("solo.abc123.us-east-1.rds.amazonaws.com");

        Assert.Equal("solo", client.Downloads[0].DBInstanceIdentifier);
    }

    /// <summary>
    /// Reader and custom endpoints are REFUSED, with a reason. They round-robin across replicas, so the
    /// instance behind one is not stable between calls — plans pulled through one would be attributed to
    /// whichever replica happened to answer, which is worse than having none.
    /// </summary>
    [Theory]
    [InlineData("shared.cluster-ro-abc123.us-east-1.rds.amazonaws.com")]
    [InlineData("shared.cluster-custom-abc.us-east-1.rds.amazonaws.com")]
    public async Task ReaderAndCustomEndpointsAreRefused(string host)
    {
        var (source, client) = Build();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => source.ReadNewestAsync(host));

        Assert.Contains("does not resolve to a stable instance", ex.Message, StringComparison.Ordinal);
        Assert.Empty(client.Downloads);
    }

    /// <summary>
    /// The newest file is chosen by LAST-WRITTEN, not by name. RDS filenames embed a date and sorting them
    /// as text puts <c>2026-08-09</c> after <c>2026-08-25</c>, which would pin the collector to a stale log
    /// forever without ever erroring.
    /// </summary>
    [Fact]
    public async Task TheNewestLogIsChosenByTimeNotByName()
    {
        var (source, client) = Build();

        await source.ReadNewestAsync("solo.abc123.us-east-1.rds.amazonaws.com");

        Assert.Equal("error/postgresql.log.2026-08-25-18", client.Downloads[0].LogFileName);
    }

    /// <summary>
    /// First read asks for a bounded TAIL; the next resumes from the marker. Without the tail, a first read
    /// against a rotated multi-GB log would pull all of it across the network — #2565 measured 772 MB in
    /// twenty seconds at capture-everything.
    /// </summary>
    [Fact]
    public async Task FirstReadIsBounded_ThenItResumesFromTheMarker()
    {
        var (source, client) = Build();

        await source.ReadNewestAsync("solo.abc123.us-east-1.rds.amazonaws.com");
        await source.ReadNewestAsync("solo.abc123.us-east-1.rds.amazonaws.com");

        Assert.Null(client.Downloads[0].Marker);
        Assert.True(client.Downloads[0].NumberOfLines > 0);

        Assert.Equal("MARKER-1", client.Downloads[1].Marker);
        Assert.Equal(0, client.Downloads[1].NumberOfLines);
    }

    /// <summary>A non-RDS host is not this transport's problem: null, so the caller uses pg_read_file.</summary>
    [Fact]
    public async Task ANonRdsHostReturnsNull()
    {
        var (source, client) = Build();

        Assert.Null(await source.ReadNewestAsync("db.internal.example.com"));
        Assert.Empty(client.Downloads);
    }

    /// <summary>
    /// An instance that lists no PostgreSQL log file is NAMED, whether the SDK omitted the collection or
    /// handed back an empty one. Both mean no log was opened, and neither may reach the runner as
    /// <c>Value cannot be null. (Parameter 'source')</c> — the whole message the null form produced on a
    /// live fleet, which names no call, no branch and no instance (#2996).
    /// </summary>
    [Theory]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public async Task AnInstanceListingNoPostgresLogFileIsNamed(bool omit, bool empty)
    {
        var (source, client) = Build(new FakeRds { OmitLogFiles = omit, EmptyLogFiles = empty });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => source.ReadNewestAsync("solo.abc123.us-east-1.rds.amazonaws.com"));

        Assert.Contains("listed no PostgreSQL server log file", ex.Message, StringComparison.Ordinal);
        Assert.Contains("solo", ex.Message, StringComparison.Ordinal);
        Assert.Contains("NO LOG WAS OPENED", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Value cannot be null", ex.Message, StringComparison.Ordinal);

        /* And nothing was downloaded — the branch is decided before a byte is asked for. */
        Assert.Empty(client.Downloads);
    }

    /// <summary>
    /// A cluster the API returns nothing for gets the "was not found" answer that was already written for
    /// it. It was unreachable: the SDK's absent collection made LINQ raise first, so a target pointed at a
    /// cluster that does not exist reported the same seven words as a failover.
    /// </summary>
    [Fact]
    public async Task AnAbsentClusterListNamesTheMissingCluster()
    {
        var (source, _) = Build(new FakeRds { OmitClusters = true });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => source.ReadNewestAsync("shared.cluster-abc123.us-east-1.rds.amazonaws.com"));

        Assert.Contains("was not found", ex.Message, StringComparison.Ordinal);
        Assert.Contains("DescribeDBClusters returned no cluster", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Value cannot be null", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// A cluster with no member list is the FAILOVER answer, and it says so — the distinction the
    /// "was not found" message above must not absorb, because one is worth retrying and the other is a
    /// target to repoint.
    /// </summary>
    [Fact]
    public async Task AClusterWithNoMemberListReportsTheFailoverState()
    {
        var (source, _) = Build(new FakeRds { OmitClusterMembers = true });

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => source.ReadNewestAsync("shared.cluster-abc123.us-east-1.rds.amazonaws.com"));

        Assert.Contains("reports no writer", ex.Message, StringComparison.Ordinal);
        Assert.Contains("during a failover", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Value cannot be null", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The SDK contract the three guards above exist for, pinned rather than remembered: a response
    /// collection is <c>null</c> when the service omits it, and LINQ over one names only <c>source</c>.
    /// An SDK upgrade that went back to empty collections would make the guards look like dead defence;
    /// this says they are not.
    /// </summary>
    [Fact]
    public void AnOmittedResponseCollectionIsNullAndLinqOverItNamesOnlySource()
    {
        Assert.Null(new DescribeDBLogFilesResponse().DescribeDBLogFiles);
        Assert.Null(new DescribeDBClustersResponse().DBClusters);
        Assert.Null(new DBCluster().DBClusterMembers);
        Assert.Null(new DescribeDBInstancesResponse().DBInstances);

        var ex = Assert.Throws<ArgumentNullException>(
            () => new DescribeDBLogFilesResponse().DescribeDBLogFiles.FirstOrDefault());

        Assert.Equal("source", ex.ParamName);
        Assert.Equal("Value cannot be null. (Parameter 'source')", ex.Message);
    }
}

/// <summary>
/// What the deadlock ingestor hands the runner for each of <see cref="RdsLogSource"/>'s named branches —
/// the layer that decides what <c>collection_log</c> says (#2996).
///
/// <para>The classification matters as much as the message. <c>IsAuthorizationFailure</c> false is what
/// keeps a transient log-file absence out of the PERMISSIONS bucket, whose text tells an operator to go
/// add an IAM grant; and the message is what the runner stores, so it is the difference between a row that
/// names the instance and the branch and a row reading only <c>Value cannot be null. (Parameter
/// 'source')</c>.</para>
/// </summary>
public class RdsDeadlockBranchClassificationTests
{
    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"),
            Amazon.RegionEndpoint.USEast1) { }

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBLogFilesResponse());
    }

    /* Never opened: IngestAsync reads the log before it touches the store, and this branch throws there.
       NpgsqlDataSource.Create does not connect eagerly, the same stand-in RdsCpuIngestorTests uses. */
    private static NpgsqlDataSource UnusedDataSource()
        => NpgsqlDataSource.Create("Host=localhost;Port=1;Database=unused");

    [Fact]
    public async Task AnAbsentLogFileListReachesTheRunnerNamedAndNotAsAPermissionsSkip()
    {
        var ingestor = new RdsDeadlockIngestor(
            UnusedDataSource(), new RdsLogSource(_ => new FakeRds()));

        var ex = await Assert.ThrowsAsync<RdsLogUnavailableException>(
            () => ingestor.IngestAsync(1, "srv", "solo.abc123.us-east-1.rds.amazonaws.com"));

        Assert.Contains("listed no PostgreSQL server log file", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Value cannot be null", ex.Message, StringComparison.Ordinal);

        /* Not an authorization refusal, so the runner does not append the "the role needs
           rds:DescribeDBLogFiles" sentence to a fault no grant fixes. */
        Assert.False(ex.IsAuthorizationFailure);
    }
}

/// <summary>
/// The route split (#2538): one table, two transports, and neither target type left without a road.
/// </summary>
public class PlanCaptureRouteTests
{
    private static CollectorTargetInfo Target(bool aurora = false, bool rds = false)
        => new() { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = aurora, IsAwsRds = rds };


    /// <summary>
    /// Both routes write through the SAME definition, so the column order, the COPY command and the
    /// standard prefix have one owner. A second writer with its own opinion about the table is how the two
    /// halves would drift into storing subtly different rows.
    /// </summary>
    [Fact]
    public void BothRoutesShareOneTableDefinition()
    {
        Assert.Equal("pg_plan_capture", PgPlanCaptureCollector.Instance.TargetTable);

        /* The ingestor builds PgPlanCaptureCollector.Row values and hands them to the definition's own
           WritePayload; if the payload shape moved, this count moves with it rather than silently
           disagreeing. */
        Assert.Equal(6, PgPlanCaptureCollector.Instance.PayloadColumns.Count);
    }

    /// <summary>
    /// The collector is NOT gated on the engine, and that is deliberate. Gating would make the capability
    /// model report plan capture as a PERMANENT GAP on Aurora, which is false - those targets do capture
    /// plans, through the RDS log API. The ROUTE is chosen at dispatch instead, so the definition never
    /// executes against a managed target and cannot raise the permission failure a gate would have been
    /// avoiding. Four capability tests caught the first attempt at this.
    /// </summary>
    [Fact]
    public void TheCollectorIsNotAPermanentGapOnManagedTargets()
    {
        Assert.True(PgPlanCaptureCollector.Instance.AppliesTo(Target()));
        Assert.True(PgPlanCaptureCollector.Instance.AppliesTo(Target(aurora: true)));
        Assert.True(PgPlanCaptureCollector.Instance.AppliesTo(Target(rds: true)));
    }
}
