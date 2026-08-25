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

        public override Task<DescribeDBClustersResponse> DescribeDBClustersAsync(
            DescribeDBClustersRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBClustersResponse
            {
                DBClusters = new List<DBCluster>
                {
                    new()
                    {
                        DBClusterMembers = new List<DBClusterMember>
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
                DescribeDBLogFiles = new List<DescribeDBLogFilesDetails>
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
}

/// <summary>
/// The route split (#2538): one table, two transports, and neither target type left without a road.
/// </summary>
public class PlanCaptureRouteTests
{
    private static CollectorTargetInfo Target(bool aurora = false, bool rds = false)
        => new() { Engine = CollectorTargetEngine.PostgreSql, PostgresMajorVersion = 17, IsAurora = aurora, IsAwsRds = rds };

    /// <summary>
    /// The SQL collector is excluded from managed targets, and that is a ROUTE decision rather than a
    /// capability one. Aurora and RDS have no filesystem, <c>pg_read_server_files</c> is not grantable and
    /// <c>pg_read_file</c> is denied — so left enabled it would record a permission failure every cycle
    /// forever, on a target where nothing is wrong and no grant exists to fix it.
    /// </summary>
    [Fact]
    public void TheFileRouteIsSelfHostedOnly()
    {
        Assert.True(PgPlanCaptureCollector.Instance.AppliesTo(Target()));
        Assert.False(PgPlanCaptureCollector.Instance.AppliesTo(Target(aurora: true)));
        Assert.False(PgPlanCaptureCollector.Instance.AppliesTo(Target(rds: true)));
    }

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
}
