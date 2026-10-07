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
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon.RDS.Model;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: these cases never connect to any store; the login probe is an in-memory fake. */
public partial class RdsEndpointVerifierTests
{
    private static IEnumerable<string> ServiceFiles()
    {
        var service = Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service");

        return Directory.EnumerateFiles(service, "*.cs", SearchOption.AllDirectories)
            .Where(path => !path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                && !path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal));
    }

    private static string ServiceFile(string name)
        => File.ReadAllText(Path.Combine(RepoRoot(), "Darling", "PerformanceMonitor.Darling.Service", name)).Replace("\r\n", "\n");

    /// <summary>
    /// The skip switch the older fixtures rely on stays out of product code: it may be named in the verifier's own file
    /// and nowhere else in the service.
    /// </summary>
    [Fact]
    public void TheTestSkipSwitch_IsNamedInNoOtherServiceFile()
    {
        var offenders = ServiceFiles()
            .Where(path => Path.GetFileName(path) != "RdsEndpointVerifier.cs"
                && File.ReadAllText(path).Contains("TestOnlySkipCheck", StringComparison.Ordinal))
            .ToList();

        Assert.Empty(offenders);
    }

    /// <summary>
    /// No service file builds a verifier that skips the check: outside the verifier's file the only construction forms are
    /// the parameterless one (the runner's shared verifier) and the log source's clock-only one, and none names
    /// <c>enforce</c>. The constructor that takes it is internal.
    /// </summary>
    [Fact]
    public void NoServiceFile_BuildsAVerifierThatSkipsTheCheck()
    {
        var construction = new Regex(@"new\s+RdsEndpointVerifier\s*\(([^)]*)\)");
        var offenders = new List<string>();

        foreach (var path in ServiceFiles().Where(path => Path.GetFileName(path) != "RdsEndpointVerifier.cs"))
        {
            var code = File.ReadAllText(path);

            if (code.Contains("enforce:", StringComparison.Ordinal) || code.Contains("enforce =", StringComparison.Ordinal))
            {
                offenders.Add(path + " names enforce");
            }

            foreach (Match match in construction.Matches(code))
            {
                var arguments = match.Groups[1].Value.Trim();

                if (arguments.Length > 0 && arguments != "clock, null")
                {
                    offenders.Add(path + ": " + match.Value);
                }
            }
        }

        Assert.Empty(offenders);

        var publicConstructors = typeof(RdsEndpointVerifier).GetConstructors(
            System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance);

        Assert.All(publicConstructors, c => Assert.DoesNotContain(c.GetParameters(), p => p.Name == "enforce"));
    }

    /// <summary>The runner builds one verifier, hands it to the four RDS readers, and gives each the target's connection string.</summary>
    [Fact]
    public void TheRunner_SharesOneVerifierWithTheFourRdsReaders()
    {
        var runner = ServiceFile("DarlingCollectorRunner.cs");

        Assert.Single(Regex.Matches(runner, @"RdsEndpointVerifier _rdsVerifier = new\(\);"));
        Assert.Equal(4, Regex.Matches(runner, @"verifier: _rdsVerifier").Count);
        Assert.Equal(4, Regex.Matches(runner, @"server\.ConnectionString, cancellationToken\);").Count);
    }

    /// <summary>The reload clears a server's verdicts when its definition changed, and the dispatch arm for the refusal records PERMISSIONS.</summary>
    [Fact]
    public void TheWorker_ClearsVerdictsOnAChangedDefinition_AndHasTheRefusalArm()
    {
        var worker = ServiceFile("DarlingWorker.cs");

        var changed = worker.IndexOf("if (connectionChanged)", StringComparison.Ordinal);
        var forget = worker.IndexOf("_runner?.ForgetRdsVerdicts(id);", changed, StringComparison.Ordinal);

        Assert.True(changed > 0 && forget > changed, "the changed-definition branch clears the verdicts");
        Assert.True(forget - changed < 6000, "and it does so inside that branch");

        var arm = worker.IndexOf("catch (RdsEndpointMismatchException ex)", StringComparison.Ordinal);
        var general = worker.IndexOf("catch (RdsLogUnavailableException", arm, StringComparison.Ordinal);

        Assert.True(arm > 0 && general > arm);
        Assert.Contains("\"PERMISSIONS\"", worker[arm..general], StringComparison.Ordinal);
        Assert.Contains("ex.Message", worker[arm..general], StringComparison.Ordinal);

        /* The denied-Describe text on the log route names both Describe actions, as the AWS refusal does. */
        Assert.Contains("the role needs rds:DescribeDBInstances, rds:DescribeDBClusters, \"", worker, StringComparison.Ordinal);
    }

    /// <summary>A refused login (28000, 28P01) is a connection-level fault, so the runtime is dropped and reconnects.</summary>
    [Theory]
    [InlineData("28000")]
    [InlineData("28P01")]
    [InlineData("57P01")]
    public void ARefusedLogin_IsAConnectionFatalFault(string sqlState)
    {
        var fault = new PostgresException("boom", "FATAL", "FATAL", sqlState);

        Assert.Equal(CollectorTargetFault.ConnectionFatal, PostgresTargetProvider.Instance.Classify(fault, yieldsOnLockTimeout: false));
    }

    [Fact]
    public void AnOtherwiseUnclassifiedFault_StaysUnclassified()
    {
        var fault = new PostgresException("boom", "ERROR", "ERROR", "42000");

        Assert.Equal(CollectorTargetFault.Unclassified, PostgresTargetProvider.Instance.Classify(fault, yieldsOnLockTimeout: false));
    }

    [Fact]
    public async Task ACachedMatch_NeedsAFreshLogin_NoOlderThanTheMatchLifetime()
    {
        var now = new DateTime(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);
        var probes = 0;
        string? answer = null;
        var verifier = Enforcing(() => now, (_, _) => { probes++; return Task.FromResult(answer); });
        var rds = new FakeRds();

        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);

        Assert.Equal(1, rds.InstanceDescribes);
        Assert.Equal(1, probes);

        /* Inside the hour the login holds. Past it the next read logs in again, and a refused login ends the reads. */
        now = now.AddMinutes(59);
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);
        Assert.Equal(1, probes);

        now = now.AddMinutes(2);
        answer = "SQLSTATE 28P01";

        var ex = await Assert.ThrowsAsync<RdsTargetLoginException>(
            () => verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString));

        Assert.Equal(2, probes);
        Assert.Contains(InstanceHost, ex.Message, StringComparison.Ordinal);
        Assert.Contains("SQLSTATE 28P01", ex.Message, StringComparison.Ordinal);
        Assert.IsAssignableFrom<RdsEndpointMismatchException>(ex);

        /* A refusal is not cached: the next read tries the login again, and goes on when it is accepted. */
        answer = null;
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);
        Assert.Equal(3, probes);
    }

    [Fact]
    public async Task NoConnectionString_IsNoFreshLogin_AndNothingIsRead()
    {
        var rds = new FakeRds();
        var verifier = new RdsEndpointVerifier(null, enforce: true);

        var ex = await Assert.ThrowsAsync<RdsTargetLoginException>(
            () => verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None));

        Assert.Contains("no connection string", ex.Message, StringComparison.Ordinal);
        Assert.NotNull(await RdsEndpointVerifier.ProbeLoginAsync(null, CancellationToken.None));
    }

    [Fact]
    public async Task ARefusedLogin_ThroughTheLogSource_ReadsNothing()
    {
        var rds = new FakeRds();
        var source = new RdsLogSource(
            _ => rds, verifier: Enforcing(loginProbe: (_, _) => Task.FromResult<string?>("SQLSTATE 28000")));

        await Assert.ThrowsAsync<RdsTargetLoginException>(
            () => source.ReadNewestAsync(InstanceHost, RdsLogSource.LogFileKind.Stderr, 7, CancellationToken.None, ConnectionString));

        Assert.Equal(0, rds.LogListings);
        Assert.Equal(0, rds.Downloads);
    }

    [Fact]
    public async Task TheCredentialScope_IsPartOfTheVerdictKey_AndAClearedServerAsksAgain()
    {
        var rds = new FakeRds();
        var verifier = Enforcing();

        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);
        Assert.Equal(1, rds.InstanceDescribes);

        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString, "scope-b");
        Assert.Equal(2, rds.InstanceDescribes);

        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 2, CancellationToken.None, ConnectionString);
        Assert.Equal(3, rds.InstanceDescribes);

        verifier.ClearServer(1);

        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString, "scope-b");
        Assert.Equal(5, rds.InstanceDescribes);

        /* Another server's verdict was kept. */
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 2, CancellationToken.None, ConnectionString);
        Assert.Equal(5, rds.InstanceDescribes);
    }

    private static async Task<string> MismatchTextAsync(FakeRds rds, string host, RdsEndpointVerifier? verifier = null)
    {
        var ex = await Assert.ThrowsAsync<RdsEndpointMismatchException>(
            () => (verifier ?? Enforcing()).EnsureAsync(rds, Parse(host), host, 1, CancellationToken.None, ConnectionString));

        return ex.Message;
    }

    [Fact]
    public async Task MissingId_ReadsAsMismatch_WithTheSameTextAsAMismatchedId()
    {
        var mismatched = await MismatchTextAsync(new FakeRds { InstanceAddress = ReportedElsewhere }, InstanceHost);

        Assert.Equal(mismatched, await MismatchTextAsync(new FakeRds { NoResults = true }, InstanceHost));
        Assert.Equal(mismatched, await MismatchTextAsync(
            new FakeRds { DescribeFailure = new DBInstanceNotFoundException("DBInstance solo not found.") }, InstanceHost));

        var clusterMismatch = await MismatchTextAsync(
            new FakeRds { ClusterEndpoint = ReportedElsewhere, ReaderEndpoint = null, CustomEndpoints = null }, WriterHost);

        Assert.Equal(clusterMismatch, await MismatchTextAsync(new FakeRds { NoResults = true }, WriterHost));
        Assert.Equal(clusterMismatch, await MismatchTextAsync(
            new FakeRds { DescribeFailure = new DBClusterNotFoundException("DBCluster shared not found.") }, WriterHost));
    }

    [Fact]
    public async Task MissingId_IsCachedAsAMismatch()
    {
        var rds = new FakeRds { DescribeFailure = new DBInstanceNotFoundException("not found") };
        var verifier = Enforcing();

        await MismatchTextAsync(rds, InstanceHost, verifier);
        await MismatchTextAsync(rds, InstanceHost, verifier);

        Assert.Equal(1, rds.InstanceDescribes);
    }

    [Theory]
    [InlineData("hosK.abc123.us-east-1.rds.amazonaws.com", "hosk.abc123.us-east-1.rds.amazonaws.com")]
    [InlineData("höst.abc123.us-east-1.rds.amazonaws.com", "höst.abc123.us-east-1.rds.amazonaws.com")]
    [InlineData("solo.abc123.us-east-1.rds.amazonaws.comİ", "solo.abc123.us-east-1.rds.amazonaws.comİ")]
    public void ANonAsciiHost_MatchesNothing(string host, string reported)
    {
        Assert.False(RdsEndpointVerifier.Matches(host, [reported]));
        Assert.False(RdsEndpointVerifier.Matches(reported, [host]));
        Assert.Equal(string.Empty, RdsEndpointVerifier.Normalize(host));
    }

    [Fact]
    public void AnAsciiHost_StillComparesIgnoringCase()
    {
        Assert.True(RdsEndpointVerifier.Matches("SOLO.ABC123.us-east-1.RDS.amazonaws.com", [InstanceHost]));
    }

    [Theory]
    [InlineData("solo.abc123.us-east-1.rds.amazonaws.com,other.abc123.us-east-1.rds.amazonaws.com")]
    [InlineData("solo.abc123.us-east-1.rds.amazonaws.com:5432")]
    [InlineData("solo.abc123.us-east-1.rds.amazonaws.comK")]
    [InlineData("sölo.abc123.us-east-1.rds.amazonaws.com")]
    [InlineData("solo.abc123.us-east-1.rds.amazonaws.com extra")]
    public async Task AHostThatIsNotOneEndpointName_ReadsNothing(string host)
    {
        var rds = new FakeRds();
        var source = new RdsLogSource(_ => rds, verifier: Enforcing());

        try
        {
            var chunk = await source.ReadNewestAsync(host, RdsLogSource.LogFileKind.Stderr, 7, CancellationToken.None, ConnectionString);

            Assert.Null(chunk);
        }
        catch (RdsEndpointMismatchException)
        {
            /* The other acceptable end: refused as a mismatch. Either way nothing below was read. */
        }

        Assert.Equal(0, rds.LogListings);
        Assert.Equal(0, rds.Downloads);
    }

    [Fact]
    public async Task AnExpiredEntry_IsDroppedOnRead_AndEachCacheIsCapped()
    {
        var now = new DateTime(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc);
        var verifier = Enforcing(() => now);

        await MismatchTextAsync(new FakeRds { InstanceAddress = ReportedElsewhere }, InstanceHost, verifier);
        Assert.Equal(1, verifier.CachedVerdictCount);

        now = now.AddMinutes(6);
        var rds = new FakeRds();
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);

        /* The expired mismatch was replaced by the new match under the same key, not kept beside it. */
        Assert.Equal(1, verifier.CachedVerdictCount);

        var last = 100 + RdsEndpointVerifier.MaxEntries + 49;

        for (var serverId = 100; serverId <= last; serverId++)
        {
            now = now.AddSeconds(1);
            await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, serverId, CancellationToken.None, ConnectionString);
        }

        Assert.Equal(RdsEndpointVerifier.MaxEntries, verifier.CachedVerdictCount);
        Assert.Equal(RdsEndpointVerifier.MaxEntries, verifier.CachedLoginCount);

        /* The oldest entry went first: the newest is kept, and server 1 (the oldest) asks AWS again. */
        var before = rds.InstanceDescribes;
        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, last, CancellationToken.None, ConnectionString);
        Assert.Equal(before, rds.InstanceDescribes);

        await verifier.EnsureAsync(rds, Parse(InstanceHost), InstanceHost, 1, CancellationToken.None, ConnectionString);
        Assert.Equal(before + 1, rds.InstanceDescribes);
    }
}
