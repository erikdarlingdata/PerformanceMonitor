/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The external ID of a per-server AWS role is a secret (#5452): it never appears in a message, an exception's text, a
/// key's text or a log line, whatever the SDK repeats back. Each test builds a fake STS or credential source whose
/// error text repeats a sentinel ID (as written, URL-encoded and in other case) and then looks for it everywhere the
/// code under test can put it.
/// </summary>
public sealed class AwsExternalIdNeverPrintedTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";

    /// <summary>Uses every character the external ID alphabet allows, so an encoder changes it.</summary>
    private const string Sentinel = "sent+inel=id,7.4@x:y/z-9";

    private static string Echo(string text) =>
        $"{text} [request externalId={Sentinel} encoded={Uri.EscapeDataString(Sentinel)} upper={Sentinel.ToUpperInvariant()}]";

    private static void AssertClean(string? text)
    {
        Assert.NotNull(text);
        Assert.DoesNotContain(Sentinel, text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain(Uri.EscapeDataString(Sentinel), text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("sent+inel", text, StringComparison.OrdinalIgnoreCase);
    }

    private static void AssertClean(AwsRoleAssumeException ex)
    {
        AssertClean(ex.Message);
        AssertClean(ex.ToString());
        AssertClean(ex.SourceErrorCode ?? string.Empty);
        AssertClean(ex.SourceExceptionType ?? string.Empty);
        AssertClean(ex.StackTrace ?? string.Empty);
        Assert.Null(ex.InnerException);
        Assert.Empty(ex.Data);
    }

    private static AwsAssumedRoleCredentials Make(
        FakeSts sts, ManualTimeProvider clock, CapturingAwsLogger? logger = null, Func<RegionEndpoint, Task<AWSCredentials>>? source = null) =>
        new(new AwsRoleKey(Role, Sentinel), RegionEndpoint.USEast1, sts.Factory, source ?? FakeSts.Source, null, clock, null, logger);

    /* ---- the key ------------------------------------------------------------------------------------------- */

    [Fact]
    public void TheKeysText_NamesTheRole_AndSaysWhetherAnIdIsSet()
    {
        var withId = new AwsRoleKey(Role, Sentinel);
        var withoutId = new AwsRoleKey(Role, null);

        Assert.Equal("AwsRoleKey { RoleArn = " + Role + ", ExternalId = set }", withId.ToString());
        Assert.Equal("AwsRoleKey { RoleArn = " + Role + ", ExternalId = none }", withoutId.ToString());
        AssertClean($"{withId}");
        AssertClean(string.Format(System.Globalization.CultureInfo.InvariantCulture, "{0}", withId));
        AssertClean(new[] { withId }.Select(k => k.ToString()).Single());
    }

    [Fact]
    public void TheKeyStillComparesByTheValueOfTheId()
    {
        Assert.Equal(new AwsRoleKey(Role, Sentinel), new AwsRoleKey(Role, Sentinel));
        Assert.NotEqual(new AwsRoleKey(Role, Sentinel), new AwsRoleKey(Role, Sentinel.ToUpperInvariant()));
        Assert.NotEqual(new AwsRoleKey(Role, Sentinel), new AwsRoleKey(Role, null));
        Assert.Equal(Sentinel, new AwsRoleKey(Role, Sentinel).ExternalId);
    }

    /* ---- every failure path -------------------------------------------------------------------------------- */

    [Theory]
    [InlineData("AccessDenied")]
    [InlineData("RegionDisabledException")]
    [InlineData("ExpiredToken")]
    [InlineData("InvalidClientTokenId")]
    [InlineData("SignatureDoesNotMatch")]
    [InlineData("ValidationError")]
    [InlineData("Throttling")]
    public async Task AnStsErrorThatRepeatsTheId_LeavesItOutOfEverythingTheCodePrints(string code)
    {
        var logger = new CapturingAwsLogger();
        var sts = new FakeSts((_, _) => throw FakeSts.Error(code, Echo("Request failed for the call")));
        var credentials = Make(sts, new ManualTimeProvider(), logger);

        var first = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());
        var cached = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());

        AssertClean(first);
        AssertClean(cached);
        AssertClean(logger.All);
        Assert.True(first.ExternalIdSet);
        Assert.Equal(1, sts.Calls);
    }

    [Fact]
    public async Task AnErrorCodeThatRepeatsTheId_IsScrubbedToo()
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error(Echo("InvalidClientTokenId"), "denied"));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => Make(sts, new ManualTimeProvider()).GetCredentialsAsync());

        Assert.Equal(AwsRoleAssumeKind.SourceCredentials, ex.Kind);
        AssertClean(ex);
    }

    [Fact]
    public async Task ASourceFailureThatRepeatsTheId_LeavesItOutOfTheMessage()
    {
        var logger = new CapturingAwsLogger();
        var sts = new FakeSts();
        var credentials = Make(sts, new ManualTimeProvider(), logger,
            source: _ => throw new AmazonClientException(Echo("Unable to get credentials")));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());

        Assert.Equal(AwsRoleAssumeKind.NoSourceIdentity, ex.Kind);
        AssertClean(ex);
        AssertClean(logger.All);
    }

    [Fact]
    public async Task ANonServiceExceptionThatRepeatsTheId_IsScrubbed()
    {
        var sts = new FakeSts((_, _) => throw new InvalidOperationException(Echo("socket closed")));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => Make(sts, new ManualTimeProvider()).GetCredentialsAsync());

        Assert.Equal(AwsRoleAssumeKind.Transient, ex.Kind);
        AssertClean(ex);
        Assert.Contains("socket closed", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheCredentialsHandedToAClient_CarryNoIdInTheirText()
    {
        var sts = new FakeSts();
        var credentials = Make(sts, new ManualTimeProvider());

        var granted = await credentials.GetCredentialsAsync();

        AssertClean(credentials.ToString());
        AssertClean(credentials.ForRegion(RegionEndpoint.USEast1).ToString());
        Assert.Equal(Sentinel, Assert.Single(sts.Requests).ExternalId);
        Assert.NotNull(granted);
    }

    [Fact]
    public async Task ThroughARealRdsClient_TheExceptionThatReachesTheCaller_IsClean()
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error("AccessDenied", Echo("User is not authorized")));
        var credentials = Make(sts, new ManualTimeProvider());

        using var rds = new Amazon.RDS.AmazonRDSClient(credentials.ForRegion(RegionEndpoint.USEast1), RegionEndpoint.USEast1);
        var thrown = await Record.ExceptionAsync(() => rds.DescribeDBInstancesAsync(new Amazon.RDS.Model.DescribeDBInstancesRequest()));

        Assert.NotNull(thrown);
        AssertClean(thrown!.ToString());
        AssertClean(thrown.Message);
        var found = AwsRoleAssumeException.Find(thrown);
        Assert.NotNull(found);
        AssertClean(found!);
    }

    [Fact]
    public void TheTwoRefusalsThatNeverReachStsAreClean()
    {
        var cache = new AwsRoleCredentialCache(AwsRoleAllowlist.Empty);
        var notAllowed = Assert.Throws<AwsRoleAssumeException>(() => cache.For(new AwsRoleKey(Role, Sentinel), "us-east-1"));
        AssertClean(notAllowed);

        var allowed = new AwsRoleCredentialCache(AwsRoleAllowlist.From(new[] { "123456789012" }));
        var mismatch = Assert.Throws<AwsRoleAssumeException>(() => allowed.For(new AwsRoleKey(Role, Sentinel), "cn-north-1"));
        AssertClean(mismatch);
        Assert.True(notAllowed.ExternalIdSet);
        Assert.True(mismatch.ExternalIdSet);
    }

    [Fact]
    public void ScrubRemovesEveryFormOfTheId_AndLeavesTextWithoutOneAlone()
    {
        Assert.Equal("none here", AwsRoleAssumeException.Scrub("none here", Sentinel));
        Assert.Equal("anything", AwsRoleAssumeException.Scrub("anything", null));
        Assert.Equal("anything", AwsRoleAssumeException.Scrub("anything", string.Empty));
        AssertClean(AwsRoleAssumeException.Scrub(Echo("text"), Sentinel));
        AssertClean(AwsRoleAssumeException.Scrub("value " + System.Net.WebUtility.HtmlEncode(Sentinel) + " end", Sentinel));
    }

    /* ---- the worker's own outputs --------------------------------------------------------------------------- */

    private const string InstanceHost = "solo.abc123.us-east-1.rds.amazonaws.com";

    [Theory]
    [InlineData("AccessDenied")]
    [InlineData("RegionDisabledException")]
    [InlineData("ExpiredToken")]
    [InlineData("Throttling")]
    public async Task TheWorkersCollectionLogTextAndWarningLine_NeverCarryTheId(string code)
    {
        // An STS error that repeats the external ID in its message, driven through the real RDS reader and a real client.
        var sts = new FakeSts((_, _) => throw FakeSts.Error(code, Echo("denied for role " + Role)));
        var cacheLog = new CapturingAwsLogger();
        var cache = new AwsRoleCredentialCache(AwsRoleAllowlist.From(new[] { Role }), null, sts.Factory, FakeSts.Source, new ManualTimeProvider(), null, cacheLog);
        var ingestor = new RdsCpuIngestor(
            Npgsql.NpgsqlDataSource.Create("Host=localhost;Port=1;Database=unused"),
            verifier: RdsEndpointVerifier.ForTests(enforce: true, loginProbe: (_, _) => Task.FromResult<string?>(null)),
            roles: cache);

        var thrown = await Record.ExceptionAsync(
            () => ingestor.IngestAsync(1, "s", InstanceHost, "Host=" + InstanceHost, new AwsRoleKey(Role, Sentinel), System.Threading.CancellationToken.None));

        var fault = Assert.IsType<AwsRoleAssumeException>(thrown);
        AssertClean(fault);

        // What the worker writes for a refused role: the collection_log text and the warning line.
        var workerLog = new CapturingAwsLogger();
        var rowText = PerformanceMonitor.Darling.Service.DarlingWorker.AwsRoleFaultNote(workerLog, "alpha-pg-01", "pg_plan_capture", fault);
        AssertClean(rowText);
        AssertClean(workerLog.All);

        // The ERROR arm writes the exception's message too; the credentials class logged one warning for the failed refresh.
        AssertClean(fault.Message);
        AssertClean(cacheLog.All);
    }
}
