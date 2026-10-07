/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An RDS target's host has to be the endpoint AWS reports for the instance or cluster id parsed from it before any RDS
/// or Performance Insights read runs. Every case here builds the verifier with <c>enforce: true</c>, because the test
/// assembly's default verifier skips the check for the older fixtures that predate it.
/// </summary>
public class RdsEndpointVerifierTests
{
    private const string InstanceHost = "solo.abc123.us-east-1.rds.amazonaws.com";
    private const string WriterHost = "shared.cluster-abc123.us-east-1.rds.amazonaws.com";
    private const string ReaderHost = "shared.cluster-ro-abc123.us-east-1.rds.amazonaws.com";
    private const string CustomHost = "shared.cluster-custom-abc.us-east-1.rds.amazonaws.com";
    private const string LookAlikeHost = "solo.abc123.us-east-1.rds.example.invalid";
    private const string ReportedElsewhere = "elsewhere.zzz999.us-east-1.rds.amazonaws.com";

    private static RdsEndpointVerifier Enforcing(Func<DateTime>? clock = null) => new(clock, enforce: true);

    private static RdsEndpoint.Parsed Parse(string host) => RdsEndpoint.TryParse(host)!.Value;

    private static NpgsqlDataSource UnusedDataSource()
        => NpgsqlDataSource.Create("Host=localhost;Port=1;Database=unused");

    private sealed class FakeRds : AmazonRDSClient
    {
        public FakeRds() : base(new Amazon.Runtime.BasicAWSCredentials("a", "b"), Amazon.RegionEndpoint.USEast1) { }

        public string? InstanceAddress { get; init; } = InstanceHost;
        public string? ClusterEndpoint { get; init; } = WriterHost;
        public string? ReaderEndpoint { get; init; } = ReaderHost;
        public List<string>? CustomEndpoints { get; init; } = [CustomHost];
        public Exception? DescribeFailure { get; init; }

        public int InstanceDescribes;
        public int ClusterDescribes;
        public int LogListings;
        public int Downloads;

        public override Task<DescribeDBInstancesResponse> DescribeDBInstancesAsync(
            DescribeDBInstancesRequest request, CancellationToken cancellationToken = default)
        {
            InstanceDescribes++;

            if (DescribeFailure is not null)
            {
                throw DescribeFailure;
            }

            return Task.FromResult(new DescribeDBInstancesResponse
            {
                DBInstances = [new DBInstance { Endpoint = new Endpoint { Address = InstanceAddress }, DbiResourceId = "db-EXAMPLE" }],
            });
        }

        public override Task<DescribeDBClustersResponse> DescribeDBClustersAsync(
            DescribeDBClustersRequest request, CancellationToken cancellationToken = default)
        {
            ClusterDescribes++;

            if (DescribeFailure is not null)
            {
                throw DescribeFailure;
            }

            return Task.FromResult(new DescribeDBClustersResponse
            {
                DBClusters =
                [
                    new DBCluster
                    {
                        Endpoint = ClusterEndpoint,
                        ReaderEndpoint = ReaderEndpoint,
                        CustomEndpoints = CustomEndpoints,
                        DBClusterMembers = [new DBClusterMember { DBInstanceIdentifier = "writer-1", IsClusterWriter = true }],
                    },
                ],
            });
        }

        public override Task<DescribeDBLogFilesResponse> DescribeDBLogFilesAsync(
            DescribeDBLogFilesRequest request, CancellationToken cancellationToken = default)
        {
            LogListings++;

            return Task.FromResult(new DescribeDBLogFilesResponse
            {
                DescribeDBLogFiles = [new DescribeDBLogFilesDetails { LogFileName = "error/postgresql.log.2026-08-25-18", LastWritten = 1000 }],
            });
        }

        public override Task<DownloadDBLogFilePortionResponse> DownloadDBLogFilePortionAsync(
            DownloadDBLogFilePortionRequest request, CancellationToken cancellationToken = default)
        {
            Downloads++;

            return Task.FromResult(new DownloadDBLogFilePortionResponse { LogFileData = string.Empty, Marker = "M" });
        }
    }

    [Theory]
    [InlineData("SOLO.ABC123.US-EAST-1.RDS.AMAZONAWS.COM")]
    [InlineData("solo.abc123.us-east-1.rds.amazonaws.com.")]
    [InlineData("  Solo.Abc123.us-east-1.rds.amazonaws.com..  ")]
    public void Matches_IgnoresCaseAndTrailingDot(string host)
    {
        Assert.True(RdsEndpointVerifier.Matches(host, [InstanceHost]));
        Assert.True(RdsEndpointVerifier.Matches(InstanceHost, [host]));
    }

    [Fact]
    public void Matches_RefusesAnEmptyOrAbsentReportedAddress()
    {
        Assert.False(RdsEndpointVerifier.Matches(InstanceHost, [null, string.Empty, "  ", "."]));
        Assert.False(RdsEndpointVerifier.Matches(string.Empty, [string.Empty]));
        Assert.False(RdsEndpointVerifier.Matches(InstanceHost, []));
    }

    [Fact]
    public async Task Instance_HostEqualToTheReportedAddress_Proceeds()
    {
        var rds = new FakeRds();

        await Enforcing().EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None);

        Assert.Equal(1, rds.InstanceDescribes);
    }

    [Fact]
    public async Task Instance_HostThatIsNotTheReportedAddress_Throws()
    {
        var rds = new FakeRds { InstanceAddress = ReportedElsewhere };

        var ex = await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => Enforcing().EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None));

        Assert.Contains(InstanceHost, ex.Message, StringComparison.Ordinal);
        Assert.Contains("'solo'", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(ReportedElsewhere, ex.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(WriterHost)]
    [InlineData(ReaderHost)]
    [InlineData(CustomHost)]
    public async Task Cluster_HostEqualToAnyReportedEndpoint_Proceeds(string host)
    {
        var rds = new FakeRds();

        await Enforcing().EnsureAsync(rds, Parse(host), host, 1, CancellationToken.None);

        Assert.Equal(1, rds.ClusterDescribes);
    }

    [Theory]
    [InlineData(WriterHost)]
    [InlineData(ReaderHost)]
    [InlineData(CustomHost)]
    public async Task Cluster_HostThatIsNotAReportedEndpoint_Throws(string host)
    {
        var rds = new FakeRds
        {
            ClusterEndpoint = "other.cluster-zzz.us-east-1.rds.amazonaws.com",
            ReaderEndpoint = "other.cluster-ro-zzz.us-east-1.rds.amazonaws.com",
            CustomEndpoints = null,
        };

        var ex = await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => Enforcing().EnsureAsync(rds, Parse(host), host, 1, CancellationToken.None));

        Assert.Contains("cluster 'shared'", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("zzz", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AMatchIsCachedPerServerHostAndId_AndAnEditedHostAsksAgain()
    {
        var rds = new FakeRds();
        var verifier = Enforcing();

        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None);
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost + ".", 1, CancellationToken.None);

        Assert.Equal(1, rds.InstanceDescribes);

        /* Another server id, with the same host, is its own key. */
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 2, CancellationToken.None);

        Assert.Equal(2, rds.InstanceDescribes);

        /* The host edited to a look-alike: a new key, so AWS is asked again, and now it does not match. */
        await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => verifier.EnsureAsync(rds, Parse(LookAlikeHost), LookAlikeHost, 1, CancellationToken.None));

        Assert.Equal(3, rds.InstanceDescribes);
    }

    [Fact]
    public async Task AMismatchIsKeptBriefly_ThenAsksAgainAndCanRecover()
    {
        var now = new DateTime(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);
        var verifier = Enforcing(() => now);
        var wrong = new FakeRds { InstanceAddress = ReportedElsewhere };

        await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => verifier.EnsureAsync(wrong, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None));
        await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => verifier.EnsureAsync(wrong, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None));

        Assert.Equal(1, wrong.InstanceDescribes);

        now = now.AddMinutes(6);
        var right = new FakeRds();

        await verifier.EnsureAsync(right, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None);

        Assert.Equal(1, right.InstanceDescribes);
    }

    [Fact]
    public async Task ADeniedDescribe_IsNotCached_AndNamesTheAction()
    {
        var denied = new FakeRds
        {
            DescribeFailure = new AmazonRDSException(
                "User: arn:aws:sts::1:assumed-role/x is not authorized to perform: rds:DescribeDBInstances"),
        };
        var verifier = Enforcing();

        var ex = await Assert.ThrowsAsync<AmazonRDSException>(
            () => verifier.EnsureAsync(denied, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None));

        Assert.Contains("rds:DescribeDBInstances", ex.Message, StringComparison.Ordinal);
        Assert.True(RdsLogUnavailableException.IsAuthorizationRefusal(ex));

        await Assert.ThrowsAsync<AmazonRDSException>(
            () => verifier.EnsureAsync(denied, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None));

        Assert.Equal(2, denied.InstanceDescribes);
    }

    [Fact]
    public async Task LogSource_HostEqualToTheReportedAddress_ReadsTheLog()
    {
        var rds = new FakeRds();
        var source = new RdsLogSource(_ => rds, verifier: Enforcing());

        var chunk = await source.ReadNewestAsync(InstanceHost, RdsLogSource.LogFileKind.Stderr, 7, CancellationToken.None);

        Assert.NotNull(chunk);
        Assert.Equal(1, rds.Downloads);
    }

    [Fact]
    public async Task HostThatIsNotTheReportedEndpoint_ReadsNothing()
    {
        var rds = new FakeRds { InstanceAddress = ReportedElsewhere };
        var source = new RdsLogSource(_ => rds, verifier: Enforcing());

        var ex = await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => source.ReadNewestAsync(LookAlikeHost, RdsLogSource.LogFileKind.Stderr, 7, CancellationToken.None));

        Assert.Contains(LookAlikeHost, ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, rds.LogListings);
        Assert.Equal(0, rds.Downloads);
    }

    [Fact]
    public async Task HostThatIsNotTheReportedEndpoint_ReachesTheRunnerUnwrapped_ThroughEachLogIngestor()
    {
        var rds = new FakeRds { InstanceAddress = ReportedElsewhere };
        var source = new RdsLogSource(_ => rds, verifier: Enforcing());

        await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => new RdsPlanIngestor(UnusedDataSource(), source).IngestAsync(7, "srv", LookAlikeHost));
        await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => new RdsDeadlockIngestor(UnusedDataSource(), source).IngestAsync(7, "srv", LookAlikeHost));
        await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => new RdsLogEventIngestor(UnusedDataSource(), TestLogHashKeys.Fixed, source).IngestAsync(7, "srv", LookAlikeHost));

        Assert.Equal(0, rds.LogListings);
        Assert.Equal(0, rds.Downloads);
    }

    [Fact]
    public async Task ADeniedDescribe_ThroughALogIngestor_IsTheAuthorizationOutcome()
    {
        var rds = new FakeRds
        {
            DescribeFailure = new AmazonRDSException(
                "User: arn:aws:sts::1:assumed-role/x is not authorized to perform: rds:DescribeDBInstances"),
        };
        var source = new RdsLogSource(_ => rds, verifier: Enforcing());

        var ex = await Assert.ThrowsAsync<RdsLogUnavailableException>(
            () => new RdsPlanIngestor(UnusedDataSource(), source).IngestAsync(7, "srv", InstanceHost));

        Assert.True(ex.IsAuthorizationFailure);
        Assert.Contains("rds:DescribeDBInstances", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, rds.LogListings);
    }

    [Fact]
    public async Task Cpu_HostThatIsNotTheReportedEndpoint_MakesNoPerformanceInsightsCall()
    {
        var rds = new FakeRds { InstanceAddress = ReportedElsewhere };
        var piCalled = false;
        var ingestor = new RdsCpuIngestor(
            UnusedDataSource(),
            rdsClientFactory: _ => rds,
            piClientFactory: _ => { piCalled = true; return null!; },
            verifier: Enforcing());

        var ex = await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => ingestor.IngestAsync(7, "srv", LookAlikeHost));

        Assert.Contains(LookAlikeHost, ex.Message, StringComparison.Ordinal);
        Assert.False(piCalled);
    }

    [Fact]
    public async Task Cpu_ADeniedDescribe_IsTheAuthorizationOutcome()
    {
        var rds = new FakeRds
        {
            DescribeFailure = new AmazonRDSException(
                "User: arn:aws:sts::1:assumed-role/x is not authorized to perform: rds:DescribeDBInstances"),
        };
        var ingestor = new RdsCpuIngestor(
            UnusedDataSource(),
            rdsClientFactory: _ => rds,
            piClientFactory: _ => null!,
            verifier: Enforcing());

        var ex = await Assert.ThrowsAsync<PiMetricsUnavailableException>(() => ingestor.IngestAsync(7, "srv", InstanceHost));

        Assert.True(ex.IsAuthorizationFailure);
    }

    /// <summary>
    /// The skip switch the older fixtures rely on stays out of product code: it may be named in the verifier's own file
    /// and nowhere else in the service.
    /// </summary>
    [Fact]
    public void TheTestSkipSwitch_IsNamedInNoOtherServiceFile()
    {
        var service = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        var offenders = Directory.EnumerateFiles(service, "*.cs", SearchOption.AllDirectories)
            .Where(path => !path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                && !path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                && Path.GetFileName(path) != "RdsEndpointVerifier.cs"
                && File.ReadAllText(path).Contains("TestOnlySkipCheck", StringComparison.Ordinal))
            .ToList();

        Assert.Empty(offenders);
    }

    /// <summary>The runner builds its sources and ingestors without a verifier, so each one enforces.</summary>
    [Fact]
    public void TheRunner_BuildsTheRdsReadersWithTheDefaultEnforcingVerifier()
    {
        var runner = File.ReadAllText(Path.Combine(
            RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", "DarlingCollectorRunner.cs"));

        Assert.DoesNotContain("RdsEndpointVerifier(", runner, StringComparison.Ordinal);
        Assert.DoesNotContain("enforce:", runner, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}

/* #1776 own-store: this class never connects to any store. It sets one in-memory switch when the assembly loads, so the
   older RDS fixtures (whose fakes answer neither Describe call) build a verifier that skips the check. */
internal static class RdsEndpointVerifierTestSwitch
{
    [ModuleInitializer]
    internal static void SkipTheCheckForOlderFixtures() => RdsEndpointVerifier.TestOnlySkipCheck = true;
}
