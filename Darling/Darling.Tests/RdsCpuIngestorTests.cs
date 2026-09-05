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
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The two decision points in <see cref="RdsCpuIngestor.IngestAsync"/> that are reachable without a store
/// connection — endpoint parsing and the reader/custom refusal — mirroring <see cref="RdsLogSourceTests"/>'s
/// scope for the same shared decisions. Both return or throw BEFORE the method ever touches
/// <see cref="NpgsqlDataSource"/>, so a data source built from a connection string that is never actually
/// opened is a legitimate stand-in here: <see cref="NpgsqlDataSource.Create(string)"/> does not connect
/// eagerly.
///
/// <para>The DB-touching path (watermark read, PI call construction, COPY write) is NOT covered by an
/// automated test here — it needs a real Postgres and a fake AWS PI/RDS client together, and no ingestor in
/// this family (<c>RdsDeadlockIngestor</c>, <c>RdsPlanIngestor</c>) has that today either. It was verified
/// manually against a real local PostgreSQL 17 before this file was written: the V106 DDL accepts real rows,
/// and <see cref="DarlingPgCpuUtilizationReaderTests"/> pins the resulting read SQL's shape.</para>
/// </summary>
public class RdsCpuIngestorTests
{
    private static NpgsqlDataSource UnusedDataSource()
        /* Never opened by either test below — both return/throw before RdsCpuIngestor.IngestAsync reaches
           its first _postgres call. */
        => NpgsqlDataSource.Create("Host=localhost;Port=1;Database=unused");

    /// <summary>Self-hosted PostgreSQL has no CPU route at all — a non-RDS host is this transport's ordinary
    /// answer, not an error, matching <see cref="RdsLogSource.ReadNewestAsync"/>'s identical null-return for
    /// the same case.</summary>
    [Fact]
    public async Task ANonRdsHostReturnsZero()
    {
        var awsCalled = false;
        var ingestor = new RdsCpuIngestor(
            UnusedDataSource(),
            rdsClientFactory: _ => { awsCalled = true; return null!; });

        var rows = await ingestor.IngestAsync(1, "srv", "db.internal.example.com");

        Assert.Equal(0, rows);
        Assert.False(awsCalled, "a non-RDS host must never reach an AWS client factory");
    }

    /// <summary>Reader and custom endpoints round-robin across replicas, so the instance behind one is not
    /// stable call to call — a CPU series attributed through one would jump between physical instances.
    /// Same refusal <see cref="RdsLogSource"/> gives, restated here because this is a separate code path, not
    /// a shared one.</summary>
    [Theory]
    [InlineData("shared.cluster-ro-abc123.us-east-1.rds.amazonaws.com")]
    [InlineData("shared.cluster-custom-abc.us-east-1.rds.amazonaws.com")]
    public async Task ReaderAndCustomEndpointsAreRefused(string host)
    {
        var ingestor = new RdsCpuIngestor(UnusedDataSource());

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(
            () => ingestor.IngestAsync(1, "srv", host));

        Assert.Contains("does not resolve to a stable instance", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Its copy of <see cref="RdsLogSource"/>'s writer/instance resolution carries the same three named
    /// branches, and reaches the runner with the branch in the message rather than the SDK's
    /// <c>Value cannot be null. (Parameter 'source')</c> (#2996). Wrapped in
    /// <see cref="PiMetricsUnavailableException"/> because <c>IngestAsync</c>'s tolerant catch owns
    /// everything from the first AWS call onwards — which is exactly why the inner message has to be the
    /// classified one: it is what the runner stores.
    /// </summary>
    [Theory]
    [InlineData(true, false, false, "DescribeDBClusters returned no cluster")]
    [InlineData(false, true, false, "reports no writer among its members")]
    [InlineData(false, false, true, "DescribeDBInstances returned no instance")]
    public async Task EachAbsentResolutionCollectionNamesItsOwnBranch(
        bool omitClusters, bool omitMembers, bool omitInstances, string expected)
    {
        var ingestor = new RdsCpuIngestor(
            UnusedDataSource(),
            rdsClientFactory: _ => new FakeRds
            {
                OmitClusters = omitClusters,
                OmitClusterMembers = omitMembers,
                OmitInstances = omitInstances,
            });

        var ex = await Assert.ThrowsAsync<PiMetricsUnavailableException>(
            () => ingestor.IngestAsync(1, "srv", "shared.cluster-abc123.us-east-1.rds.amazonaws.com"));

        Assert.Contains(expected, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("Value cannot be null", ex.Message, StringComparison.Ordinal);
        Assert.False(ex.IsAuthorizationFailure);
    }

    /* Only the RDS half is faked: every case above throws inside ResolveWriterAsync or
       ResolveDbiResourceIdAsync, both of which run before the watermark read, so no PI client and no store
       connection are ever reached. The PI leg (an omitted MetricList, which means "no sample in this
       window" and is coalesced to empty rather than raised) needs a real store for the watermark read and
       is not covered here — the same gap this file's class comment already records for the write path. */
    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"),
            Amazon.RegionEndpoint.USEast1) { }

        public bool OmitClusters { get; init; }
        public bool OmitClusterMembers { get; init; }
        public bool OmitInstances { get; init; }

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
                                    new() { DBInstanceIdentifier = "writer-1", IsClusterWriter = true },
                                },
                        },
                    },
            });

        public override Task<DescribeDBInstancesResponse> DescribeDBInstancesAsync(
            DescribeDBInstancesRequest request, CancellationToken cancellationToken = default)
            => Task.FromResult(new DescribeDBInstancesResponse
            {
                DBInstances = OmitInstances
                    ? null
                    : new List<DBInstance> { new() { DbiResourceId = "db-EXAMPLE" } },
            });
    }
}
