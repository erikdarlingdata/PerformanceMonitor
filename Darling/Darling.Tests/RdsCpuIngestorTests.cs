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
using Amazon.RDS;
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
}
