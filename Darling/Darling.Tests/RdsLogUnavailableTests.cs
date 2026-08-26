/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2633: "could not read the log" and "read the log, it held nothing" were the same value, and the store
/// reported the second one.
///
/// <para>
/// Measured on the PostgreSQL monitoring host after deploying the RDS log-API route. <c>collection_log</c>
/// said <c>SUCCESS | rows=0 | no new auto_explain plans in the RDS log window</c>. The app log said
/// <c>rds:DescribeDBLogFiles</c> was denied by IAM. Nothing had been read; the row asserted the log was
/// opened and was empty.
/// </para>
///
/// <para>
/// It was also a REGRESSION against the route it replaced: <c>pg_read_file</c> answers the same situation
/// with <c>PERMISSIONS</c> and a message naming the grant. The managed path — the one a real fleet is on —
/// was the one that went quiet, and the app-log warning is not a substitute for the column collection
/// health is actually read from.
/// </para>
/// </summary>
public sealed class RdsLogUnavailableTests
{
    /// <summary>
    /// The message shape measured on the fleet, verbatim past the identifiers. Matched on the SENTENCE and
    /// not only on an SDK exception type, because the refusal arrives in more than one shape depending on
    /// the call.
    /// </summary>
    private const string FleetDenial =
        "User: arn:aws:sts::000000000000:assumed-role/example-monitor-role/i-0example is not authorized to "
        + "perform: rds:DescribeDBLogFiles on resource: arn:aws:rds:us-east-1:000000000000:db:example-1 "
        + "because no identity-based policy allows the rds:DescribeDBLogFiles action";

    [Fact]
    public void TheFleetsOwnDenialMessage_IsRecognisedAsAnAuthorizationRefusal()
        => Assert.True(RdsLogUnavailableException.IsAuthorizationRefusal(new InvalidOperationException(FleetDenial)));

    [Theory]
    [InlineData("AccessDenied")]
    [InlineData("AccessDeniedException: not authorised")]
    [InlineData("User: x is not authorized to perform: rds:DownloadDBLogFilePortion")]
    public void TheOtherShapesTheSdkUses_AreRecognisedToo(string message)
        => Assert.True(RdsLogUnavailableException.IsAuthorizationRefusal(new InvalidOperationException(message)));

    /// <summary>
    /// It is found through the INNER exception too — the SDK wraps, and a refusal that arrives nested must
    /// not be classified as an unknown fault and reported as a hard error.
    /// </summary>
    [Fact]
    public void ARefusalNestedInsideAWrapper_IsStillFound()
        => Assert.True(RdsLogUnavailableException.IsAuthorizationRefusal(
            new InvalidOperationException("reading the log failed", new InvalidOperationException(FleetDenial))));

    /// <summary>
    /// And everything else stays LOUD. A throttle, a failover or an endpoint that stopped resolving is not
    /// a configuration choice, and giving it a permanent-sounding status is how a real outage gets read as
    /// one. The store's own rule: an unclassified failure must be loud rather than quietly swallowed.
    /// </summary>
    [Theory]
    [InlineData("Rate exceeded")]
    [InlineData("The DB instance is currently in a failover state")]
    [InlineData("The specified DB instance was not found")]
    [InlineData("A connection attempt failed")]
    public void ATransientOrUnknownFailure_IsNotAnAuthorizationRefusal(string message)
        => Assert.False(RdsLogUnavailableException.IsAuthorizationRefusal(new InvalidOperationException(message)));

    [Fact]
    public void TheExceptionCarriesTheClassificationAndTheOriginalMessage()
    {
        var inner = new InvalidOperationException(FleetDenial);

        var ex = new RdsLogUnavailableException(
            inner.Message, RdsLogUnavailableException.IsAuthorizationRefusal(inner), inner);

        Assert.True(ex.IsAuthorizationFailure);
        Assert.Same(inner, ex.InnerException);
        Assert.Contains("rds:DescribeDBLogFiles", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// The source pin on the half that caused the defect. The ingestor must not turn a failure into a row
    /// COUNT: returning zero there is indistinguishable from an empty log one frame later, and the runner
    /// stamps that with a sentence claiming the log was read.
    /// </summary>
    [Fact]
    public void TheIngestorRethrows_RatherThanReturningZeroRowsOnFailure()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "Targets", "RdsPlanIngestor.cs"));

        var catchIndex = source.IndexOf("catch (Exception ex) when (ex is not OperationCanceledException)", StringComparison.Ordinal);
        Assert.True(catchIndex >= 0, "The ingestor's tolerant catch is gone — this pin needs re-anchoring.");

        var body = source[catchIndex..];
        var close = body.IndexOf("\n        }", StringComparison.Ordinal);
        body = close > 0 ? body[..close] : body;

        Assert.Contains("throw new RdsLogUnavailableException", body, StringComparison.Ordinal);
        Assert.DoesNotMatch(new Regex(@"return\s+0\s*;"), body);
    }

    /// <summary>
    /// And the pin on the second defect: <c>IsAwsRds</c> was never assigned on the PostgreSQL connect path,
    /// so <c>pg_plan_capture</c>'s <c>IsAurora || IsAwsRds</c> dispatch had an unreachable half and plain
    /// RDS PostgreSQL — managed, no filesystem, not Aurora — fell to the <c>pg_read_file</c> route.
    ///
    /// <para>The fleet could never have shown this: every PostgreSQL target on it is Aurora, so
    /// <c>IsAurora</c> carried the routing and the dead half was never load-bearing.</para>
    /// </summary>
    [Fact]
    public void ThePostgresConnectPath_SetsIsAwsRds()
    {
        var source = File.ReadAllText(Path.Combine(RepoRoot(),
            "Darling", "PerformanceMonitor.Darling.Service", "DarlingServerConnector.cs"));

        var pgIndex = source.IndexOf("Engine = CollectorTargetEngine.PostgreSql,", StringComparison.Ordinal);
        Assert.True(pgIndex >= 0, "The PostgreSQL target construction moved — this pin needs re-anchoring.");

        var block = source[pgIndex..];
        var close = block.IndexOf("\n            },", StringComparison.Ordinal);
        block = close > 0 ? block[..close] : block;

        Assert.Contains("IsAwsRds", block, StringComparison.Ordinal);
    }

    /// <summary>
    /// The derivation itself, against the endpoint shapes it has to separate. A managed host is RDS; a
    /// self-hosted one is not, and must keep the file route that works there.
    /// </summary>
    [Theory]
    [InlineData("example-1.abcdefghijkl.us-east-1.rds.amazonaws.com", true)]
    [InlineData("example-cluster.cluster-abcdefghijkl.us-east-1.rds.amazonaws.com", true)]
    [InlineData("localhost", false)]
    [InlineData("db.internal.example.com", false)]
    public void TheEndpointDecidesWhetherATargetIsManaged(string host, bool expected)
        => Assert.Equal(expected, RdsEndpoint.TryParse(host) is not null);

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }
}
