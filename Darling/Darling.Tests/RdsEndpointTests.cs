/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Endpoint parsing for the RDS log-download route (#2538). All fixtures are synthetic — the shapes come
/// from AWS's documented endpoint formats, not from any fleet.
/// </summary>
public class RdsEndpointTests
{
    [Theory]
    [InlineData("alpha.abc123xyz.us-east-1.rds.amazonaws.com", "alpha", "us-east-1", RdsEndpointKind.Instance)]
    [InlineData("beta-01.c9ukews.eu-west-2.rds.amazonaws.com", "beta-01", "eu-west-2", RdsEndpointKind.Instance)]
    [InlineData("gamma.cluster-abc123.us-east-2.rds.amazonaws.com", "gamma", "us-east-2", RdsEndpointKind.ClusterWriter)]
    [InlineData("gamma.cluster-ro-abc123.us-east-2.rds.amazonaws.com", "gamma", "us-east-2", RdsEndpointKind.ClusterReader)]
    [InlineData("gamma.cluster-custom-abc.ap-southeast-2.rds.amazonaws.com", "gamma", "ap-southeast-2", RdsEndpointKind.ClusterCustom)]
    public void ItReadsTheIdentifierRegionAndShape(string host, string id, string region, RdsEndpointKind kind)
    {
        var parsed = RdsEndpoint.TryParse(host);

        Assert.NotNull(parsed);
        Assert.Equal(id, parsed!.Value.Identifier);
        Assert.Equal(region, parsed.Value.Region);
        Assert.Equal(kind, parsed.Value.Kind);
    }

    /// <summary>
    /// <b>The distinction the whole route depends on.</b> <c>DownloadDBLogFilePortion</c> takes an INSTANCE
    /// identifier. Reading a cluster endpoint as one yields <c>DBInstanceNotFound</c> against a perfectly
    /// healthy cluster — a confusing way to fail, and one that would look like a permissions problem.
    /// </summary>
    [Fact]
    public void AClusterEndpointIsNotMistakenForAnInstance()
    {
        var cluster = RdsEndpoint.TryParse("shared.cluster-abc123.us-east-1.rds.amazonaws.com");
        var instance = RdsEndpoint.TryParse("shared.abc123.us-east-1.rds.amazonaws.com");

        Assert.Equal(RdsEndpointKind.ClusterWriter, cluster!.Value.Kind);
        Assert.Equal(RdsEndpointKind.Instance, instance!.Value.Kind);

        /* Same name, same region — only the second label separates them, which is why it is matched
           exactly while the suffix is matched loosely. */
        Assert.Equal(cluster.Value.Identifier, instance.Value.Identifier);
    }

    /// <summary>
    /// Non-RDS hosts return null rather than throwing. That is an ordinary answer — a self-hosted server, a
    /// pooler, or an IP — and the caller falls back to the <c>pg_read_file</c> route.
    /// </summary>
    [Theory]
    [InlineData("localhost")]
    [InlineData("10.0.0.5")]
    [InlineData("db.internal.example.com")]
    [InlineData("pgbouncer.svc.cluster.local")]
    [InlineData("")]
    [InlineData(null)]
    public void ANonRdsHostIsNotAnError(string? host)
    {
        Assert.Null(RdsEndpoint.TryParse(host));
    }

    /// <summary>
    /// #6143: an RDS Proxy endpoint (default or one of the proxy's own endpoints, in any partition) is recognised, so
    /// the connector can ask for the instance or cluster endpoint instead. Instance and cluster endpoints are not.
    /// </summary>
    [Theory]
    [InlineData("the-proxy.proxy-abc123xyz.us-east-1.rds.amazonaws.com", true)]
    [InlineData("reader-ep.endpoint.proxy-abc123xyz.eu-west-2.rds.amazonaws.com", true)]
    [InlineData("p.proxy-abc.cn-north-1.rds.amazonaws.com.cn", true)]
    [InlineData("10.0.0.5, the-proxy.proxy-abc.us-east-1.rds.amazonaws.com", true)]
    [InlineData("alpha.abc123xyz.us-east-1.rds.amazonaws.com", false)]
    [InlineData("gamma.cluster-abc123.us-east-2.rds.amazonaws.com", false)]
    [InlineData("proxy-db.internal.example.com", false)]
    [InlineData("", false)]
    [InlineData(null, false)]
    public void AnRdsProxyEndpointIsRecognised(string? host, bool expected)
    {
        Assert.Equal(expected, RdsEndpoint.IsProxyHost(host));
    }

    /// <summary>
    /// The suffix is matched loosely so China and GovCloud parse. Pinning <c>.rds.amazonaws.com</c> exactly
    /// would silently refuse a valid endpoint in either partition, and refusing to parse looks identical to
    /// "this is not RDS" — so the failure would present as the wrong route being chosen.
    /// </summary>
    [Theory]
    [InlineData("delta.abc123.cn-north-1.rds.amazonaws.com.cn", "cn-north-1")]
    [InlineData("delta.cluster-abc.us-gov-west-1.rds.amazonaws.com", "us-gov-west-1")]
    public void OtherPartitionsParse(string host, string region)
    {
        var parsed = RdsEndpoint.TryParse(host);

        Assert.NotNull(parsed);
        Assert.Equal(region, parsed!.Value.Region);
    }
}
