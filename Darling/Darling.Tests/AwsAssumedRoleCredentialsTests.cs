/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.Endpoints;
using Amazon.SecurityToken;
using Amazon.SecurityToken.Model;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The credentials class for a per-server AWS role (#5452): how often it calls STS, when it refreshes, what it serves
/// while a refresh runs or fails, what it sends, and which exception each failure becomes. No AWS and no network: a
/// fake STS and a clock the test moves.
/// </summary>
public sealed class AwsAssumedRoleCredentialsTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string Sentinel = "sentinel-external-id-4417";

    private static readonly RegionEndpoint UsEast1 = RegionEndpoint.USEast1;

    private static AwsAssumedRoleCredentials Make(
        FakeSts sts, ManualTimeProvider clock, string? externalId = null, TimeSpan? deadline = null,
        Func<RegionEndpoint, Task<AWSCredentials>>? source = null, CapturingAwsLogger? logger = null, Func<string?>? installId = null) =>
        new(new AwsRoleKey(Role, externalId), UsEast1, sts.Factory, source ?? FakeSts.Source, installId, clock, deadline, logger);

    /* ---- how many STS calls ------------------------------------------------------------------------------ */

    [Fact]
    public async Task ManyCallersAtOnce_MakeOneStsCall()
    {
        var sts = new FakeSts(async (_, token) =>
        {
            await Task.Delay(50, token);
            return FakeSts.Grant();
        });
        var credentials = Make(sts, new ManualTimeProvider());

        var all = await Task.WhenAll(Enumerable.Range(0, 20).Select(_ => credentials.GetCredentialsAsync()));

        Assert.Equal(1, sts.Calls);
        Assert.All(all, c => Assert.Equal("ASIAEXAMPLEASSUMED", c.AccessKey));
    }

    [Fact]
    public async Task ValidCredentials_AreServedWithNoNewCall_UntilFifteenMinutesBeforeExpiry()
    {
        var clock = new ManualTimeProvider();
        var sts = new FakeSts();
        var credentials = Make(sts, clock);

        await credentials.GetCredentialsAsync();
        clock.Advance(TimeSpan.FromMinutes(44));
        await credentials.GetCredentialsAsync();
        Assert.Equal(1, sts.Calls);

        clock.Advance(TimeSpan.FromMinutes(2));
        await credentials.GetCredentialsAsync();
        Assert.Equal(2, sts.Calls);
    }

    [Fact]
    public async Task ExpiryIsReceiptPlusDuration_NotTheResponsesExpiration()
    {
        var clock = new ManualTimeProvider();

        /* An Expiration a minute away on AWS's clock would force a call per request if it were read; one in the past
           likewise. Neither is read. */
        foreach (DateTime? expiration in new DateTime?[] { null, DateTime.UtcNow.AddMinutes(1), DateTime.UtcNow.AddDays(-1) })
        {
            var sts = new FakeSts((_, _) => Task.FromResult(FakeSts.Grant(expiration: expiration)));
            var credentials = Make(sts, clock);

            await credentials.GetCredentialsAsync();
            clock.Advance(TimeSpan.FromMinutes(30));
            await credentials.GetCredentialsAsync();
            Assert.Equal(1, sts.Calls);
            clock.Advance(TimeSpan.FromMinutes(-30));
        }
    }

    [Fact]
    public void TheRefreshPointIsNeverSoonerThanTheMinimumIntervalAfterReceipt()
    {
        var received = new DateTimeOffset(2026, 10, 7, 12, 0, 0, TimeSpan.Zero);

        Assert.Equal(received.AddMinutes(45), AwsAssumedRoleCredentials.RefreshPointFor(received, received.AddHours(1)));
        Assert.Equal(received.AddMinutes(1), AwsAssumedRoleCredentials.RefreshPointFor(received, received.AddMinutes(5)));
        Assert.Equal(received.AddMinutes(1), AwsAssumedRoleCredentials.RefreshPointFor(received, received.AddSeconds(10)));
    }

    /* ---- serving without blocking ------------------------------------------------------------------------ */

    [Fact]
    public async Task PastTheRefreshPoint_OneCallerRefreshes_AndTheOthersKeepUsingTheValidCredentials()
    {
        var clock = new ManualTimeProvider();
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var blockNext = false;
        var sts = new FakeSts(async (_, _) =>
        {
            if (blockNext)
            {
                entered.TrySetResult();
                await release.Task;
                return FakeSts.Grant("second-token");
            }

            return FakeSts.Grant("first-token");
        });
        var credentials = Make(sts, clock);

        var first = await credentials.GetCredentialsAsync();
        Assert.Equal("first-token", first.Token);

        clock.Advance(TimeSpan.FromMinutes(50));
        blockNext = true;
        var refresher = credentials.GetCredentialsAsync();
        await entered.Task.WaitAsync(TimeSpan.FromSeconds(10));

        /* The refresh is in flight and holds the gate: these must come back at once, with the old credentials. */
        var served = await Task.WhenAll(Enumerable.Range(0, 5).Select(_ => credentials.GetCredentialsAsync()))
            .WaitAsync(TimeSpan.FromSeconds(5));
        Assert.All(served, c => Assert.Equal("first-token", c.Token));
        Assert.False(refresher.IsCompleted);

        release.SetResult();
        Assert.Equal("second-token", (await refresher).Token);
        Assert.Equal(2, sts.Calls);
    }

    [Fact]
    public async Task ACallerWithNothingValid_WaitsForTheRefresh_AndSharesItsAnswer()
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var sts = new FakeSts(async (_, _) =>
        {
            await release.Task;
            return FakeSts.Grant();
        });
        var credentials = Make(sts, new ManualTimeProvider());

        var callers = Enumerable.Range(0, 4).Select(_ => credentials.GetCredentialsAsync()).ToList();
        await Task.Delay(100);
        Assert.All(callers, c => Assert.False(c.IsCompleted));

        release.SetResult();
        await Task.WhenAll(callers).WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(1, sts.Calls);
    }

    /* ---- what is sent -------------------------------------------------------------------------------------- */

    [Fact]
    public async Task TheRequest_CarriesTheRole_AnHourOfDuration_AndTheSessionName()
    {
        var sts = new FakeSts();
        var credentials = Make(sts, new ManualTimeProvider(), installId: () => "0123abcd-4567-89ef-0123-456789abcdef");

        await credentials.GetCredentialsAsync();

        var request = Assert.Single(sts.Requests);
        Assert.Equal(Role, request.RoleArn);
        Assert.Equal(3600, request.DurationSeconds);
        Assert.Equal("darling-0123abcd4567", request.RoleSessionName);
        Assert.Null(request.ExternalId);
    }

    [Fact]
    public async Task TheExternalId_IsSentOnlyWhenSet()
    {
        var withId = new FakeSts();
        await Make(withId, new ManualTimeProvider(), Sentinel).GetCredentialsAsync();
        Assert.Equal(Sentinel, Assert.Single(withId.Requests).ExternalId);

        var blank = new FakeSts();
        await Make(blank, new ManualTimeProvider(), externalId: string.Empty).GetCredentialsAsync();
        Assert.Null(Assert.Single(blank.Requests).ExternalId);
    }

    [Fact]
    public async Task TheStsClientIsGivenASnapshotOfTheSourceCredentials()
    {
        var sts = new FakeSts();
        await Make(sts, new ManualTimeProvider()).GetCredentialsAsync();

        var seen = Assert.Single(sts.SourceCredentialsSeen);
        Assert.Equal("AKIAEXAMPLESOURCE", seen.AccessKey);
        Assert.Equal("example-source-secret", seen.SecretKey);
    }

    [Theory]
    [InlineData("0123abcd-4567-89ef-0123-456789abcdef", "darling-0123abcd4567")]
    [InlineData("0123ABCD456789EF0123456789ABCDEF", "darling-0123abcd4567")]
    [InlineData("  0123abcd-4567-89ef-0123-456789abcdef  ", "darling-0123abcd4567")]
    [InlineData(null, "darling-collector")]
    [InlineData("", "darling-collector")]
    [InlineData("abc", "darling-collector")]
    [InlineData("not-a-hex-install-id-at-all", "darling-collector")]
    public void SessionName_IsDarlingAndTwelveHexDigits_OrTheFallback(string? installId, string expected)
    {
        var name = AwsAssumedRoleCredentials.SessionNameFor(installId);

        Assert.Equal(expected, name);
        Assert.Matches(new Regex(@"^[\w+=,.@-]{2,64}$"), name);
    }

    /* ---- the region of the call that triggers a refresh --------------------------------------------------- */

    [Fact]
    public async Task ARefresh_CallsStsInTheRegionOfTheCallThatTriggeredIt()
    {
        var clock = new ManualTimeProvider();
        var sts = new FakeSts();
        var credentials = Make(sts, clock);

        await credentials.ForRegion(RegionEndpoint.GetBySystemName("eu-west-1")).GetCredentialsAsync();
        clock.Advance(TimeSpan.FromMinutes(50));
        await credentials.ForRegion(RegionEndpoint.GetBySystemName("ap-southeast-2")).GetCredentialsAsync();

        Assert.Equal(new[] { "eu-west-1", "ap-southeast-2" }, sts.Regions);
    }

    [Theory]
    [InlineData("us-east-1", "https://sts.us-east-1.amazonaws.com")]
    [InlineData("cn-north-1", "https://sts.cn-north-1.amazonaws.com.cn")]
    [InlineData("us-gov-west-1", "https://sts.us-gov-west-1.amazonaws.com")]
    public void TheStsClientConfig_PointsAtTheRegionalEndpointOfTheTargetsOwnPartition(string region, string expectedUrl)
    {
        var config = AwsAssumedRoleCredentials.CreateStsConfig(RegionEndpoint.GetBySystemName(region));

        var endpoint = config.DetermineServiceOperationEndpoint(new ServiceOperationEndpointParameters(new AssumeRoleRequest()));

        Assert.StartsWith(expectedUrl, endpoint.URL, StringComparison.Ordinal);
        Assert.Equal(AwsAssumedRoleCredentials.StsTimeout, config.Timeout);
        Assert.Equal(AwsAssumedRoleCredentials.StsMaxErrorRetry, config.MaxErrorRetry);
    }

    /* ---- failure ------------------------------------------------------------------------------------------- */

    [Fact]
    public async Task AFailedRefresh_IsRememberedForAMinute_AndEachThrowIsANewException()
    {
        var clock = new ManualTimeProvider();
        var sts = new FakeSts((_, _) => throw FakeSts.Error("AccessDenied", "User is not authorized to perform: sts:AssumeRole"));
        var credentials = Make(sts, clock);

        var first = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());
        var second = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());
        var third = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());

        Assert.Equal(1, sts.Calls);
        Assert.NotSame(first, second);
        Assert.NotSame(second, third);
        Assert.Equal(first.Message, second.Message);
        Assert.Equal(AwsRoleAssumeKind.Denied, second.Kind);
        Assert.Equal("AccessDenied", second.SourceErrorCode);

        clock.Advance(TimeSpan.FromSeconds(61));
        await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());
        Assert.Equal(2, sts.Calls);
    }

    [Fact]
    public async Task WhileAFailureIsRemembered_OldCredentialsWithMoreThanAMinuteLeftAreServed()
    {
        var clock = new ManualTimeProvider();
        var logger = new CapturingAwsLogger();
        var sts = new FakeSts();
        var credentials = Make(sts, clock, logger: logger);
        await credentials.GetCredentialsAsync();

        sts.Handler = (_, _) => throw FakeSts.Error("Throttling", "Rate exceeded");

        /* Past the refresh point with ten minutes left: the refresh fails, the old credentials are served, and the failure is only logged. */
        clock.Advance(TimeSpan.FromMinutes(50));
        Assert.Equal("ASIAEXAMPLEASSUMED", (await credentials.GetCredentialsAsync()).AccessKey);
        Assert.Equal(2, sts.Calls);
        Assert.Contains("Warning", logger.All, StringComparison.Ordinal);

        /* Inside the minute of the remembered failure there is no new STS call. */
        clock.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal("ASIAEXAMPLEASSUMED", (await credentials.GetCredentialsAsync()).AccessKey);
        Assert.Equal(2, sts.Calls);

        /* The failure has expired and the credentials have under a minute left: a new attempt, which fails, and the failure is thrown. */
        clock.Advance(TimeSpan.FromMinutes(8) + TimeSpan.FromSeconds(45));
        var thrown = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());
        Assert.Equal(AwsRoleAssumeKind.Transient, thrown.Kind);
        Assert.Equal(3, sts.Calls);

        /* And the next throw, from the remembered failure, is a new exception with no new call. */
        clock.Advance(TimeSpan.FromSeconds(20));
        var again = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());
        Assert.NotSame(thrown, again);
        Assert.Equal(thrown.Message, again.Message);
        Assert.Equal(3, sts.Calls);
    }

    [Fact]
    public async Task ARememberedFailure_BelongsToItsRegion()
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error("RegionDisabledException", "STS is not active"));
        var credentials = Make(sts, new ManualTimeProvider());

        await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.ForRegion(RegionEndpoint.EUSouth1).GetCredentialsAsync());
        await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.ForRegion(RegionEndpoint.EUSouth1).GetCredentialsAsync());
        Assert.Equal(1, sts.Calls);

        sts.Handler = (_, _) => Task.FromResult(FakeSts.Grant());
        var other = await credentials.ForRegion(RegionEndpoint.USEast1).GetCredentialsAsync();
        Assert.Equal("ASIAEXAMPLEASSUMED", other.AccessKey);
        Assert.Equal(2, sts.Calls);
    }

    [Theory]
    [InlineData("AccessDenied", AwsRoleAssumeKind.Denied, true)]
    [InlineData("RegionDisabledException", AwsRoleAssumeKind.RegionDisabled, true)]
    [InlineData("ExpiredToken", AwsRoleAssumeKind.SourceCredentials, false)]
    [InlineData("InvalidClientTokenId", AwsRoleAssumeKind.SourceCredentials, false)]
    [InlineData("SignatureDoesNotMatch", AwsRoleAssumeKind.SourceCredentials, false)]
    [InlineData("Throttling", AwsRoleAssumeKind.Transient, false)]
    [InlineData("ServiceUnavailable", AwsRoleAssumeKind.Transient, false)]
    public async Task EachStsErrorCode_BecomesItsKind(string code, AwsRoleAssumeKind kind, bool isConfiguration)
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error(code, "raw message from AWS"));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => Make(sts, new ManualTimeProvider(), Sentinel).GetCredentialsAsync());

        Assert.Equal(kind, ex.Kind);
        Assert.Equal(isConfiguration, ex.IsConfiguration);
        Assert.Equal(Role, ex.RoleArn);
        Assert.True(ex.ExternalIdSet);
        Assert.Equal(code, ex.SourceErrorCode);
        Assert.Contains(Role, ex.Message, StringComparison.Ordinal);
        Assert.Null(ex.InnerException);
        Assert.Contains("AmazonSecurityTokenServiceException", ex.SourceExceptionType, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ARegionDisabledExceptionType_IsRegionDisabled_WhateverItsCode()
    {
        var sts = new FakeSts((_, _) => throw new RegionDisabledException("STS is off here"));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => Make(sts, new ManualTimeProvider()).GetCredentialsAsync());

        Assert.Equal(AwsRoleAssumeKind.RegionDisabled, ex.Kind);
        Assert.Contains("us-east-1", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task TheDeniedText_SaysWhetherAnExternalIdWasSet_AndNeverTheValue()
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error("AccessDenied", "denied"));

        var withId = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => Make(sts, new ManualTimeProvider(), Sentinel).GetCredentialsAsync());
        var withoutId = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => Make(sts, new ManualTimeProvider()).GetCredentialsAsync());

        Assert.Contains("(with the external ID set on this server)", withId.Message, StringComparison.Ordinal);
        Assert.Contains("(without an external ID)", withoutId.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Sentinel, withId.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AHostWithNoCredentials_IsNoSourceIdentity_AndStsIsNeverCalled()
    {
        var sts = new FakeSts();
        var credentials = Make(sts, new ManualTimeProvider(), source: _ => throw new AmazonClientException("Unable to get IAM security credentials"));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync());

        Assert.Equal(AwsRoleAssumeKind.NoSourceIdentity, ex.Kind);
        Assert.False(ex.IsConfiguration);
        Assert.Contains("The monitoring host has no AWS credentials to assume role " + Role + " with:", ex.Message, StringComparison.Ordinal);
        Assert.Equal(0, sts.Calls);
    }

    [Fact]
    public async Task ASlowSts_IsCutOffAtTheDeadline_AndIsATransientFailure()
    {
        var sts = new FakeSts(async (_, token) =>
        {
            await Task.Delay(Timeout.Infinite, token);
            return FakeSts.Grant();
        });
        var credentials = Make(sts, new ManualTimeProvider(), deadline: TimeSpan.FromMilliseconds(100));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => credentials.GetCredentialsAsync()).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(AwsRoleAssumeKind.Transient, ex.Kind);
        Assert.Contains("did not answer within", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task ACallerWhoCancels_GetsTheCancellation_NotARememberedFailure()
    {
        var sts = new FakeSts(async (_, token) =>
        {
            await Task.Delay(Timeout.Infinite, token);
            return FakeSts.Grant();
        });
        var credentials = Make(sts, new ManualTimeProvider());
        using var cts = new CancellationTokenSource();

        var pending = credentials.GetCredentialsAsync(UsEast1, cts.Token);
        await Task.Delay(50);
        cts.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);

        sts.Handler = (_, _) => Task.FromResult(FakeSts.Grant());
        Assert.Equal("ASIAEXAMPLEASSUMED", (await credentials.GetCredentialsAsync()).AccessKey);
    }

    [Fact]
    public async Task AnEmptyResponse_IsATransientFailure()
    {
        var sts = new FakeSts((_, _) => Task.FromResult(new AssumeRoleResponse()));

        var ex = await Assert.ThrowsAsync<AwsRoleAssumeException>(() => Make(sts, new ManualTimeProvider()).GetCredentialsAsync());

        Assert.Equal(AwsRoleAssumeKind.Transient, ex.Kind);
    }

    /* ---- the SDK seam -------------------------------------------------------------------------------------- */

    [Fact]
    public async Task ARealRdsClientOverTheCredentials_SurfacesTheExceptionToWhoeverWalksTheChain()
    {
        var sts = new FakeSts((_, _) => throw FakeSts.Error("AccessDenied", "denied"));
        var credentials = Make(sts, new ManualTimeProvider(), Sentinel);

        using var rds = new Amazon.RDS.AmazonRDSClient(credentials.ForRegion(UsEast1), UsEast1);
        var thrown = await Record.ExceptionAsync(() => rds.DescribeDBInstancesAsync(new Amazon.RDS.Model.DescribeDBInstancesRequest()));

        Assert.NotNull(thrown);
        var found = AwsRoleAssumeException.Find(thrown);
        Assert.NotNull(found);
        Assert.Equal(AwsRoleAssumeKind.Denied, found!.Kind);
        Assert.Equal(1, sts.Calls);
    }

    [Fact]
    public void Find_WalksTheInnerChain_AndAnAggregate()
    {
        var inner = new AwsRoleCredentialCache(AwsRoleAllowlist.Empty);
        var thrown = Assert.Throws<AwsRoleAssumeException>(() => inner.For(new AwsRoleKey(Role, null), "us-east-1"));

        Assert.Same(thrown, AwsRoleAssumeException.Find(thrown));
        Assert.Same(thrown, AwsRoleAssumeException.Find(new InvalidOperationException("wrapped", new InvalidOperationException("twice", thrown))));
        Assert.Same(thrown, AwsRoleAssumeException.Find(new AggregateException(new InvalidOperationException("a"), new InvalidOperationException("b", thrown))));
        Assert.Null(AwsRoleAssumeException.Find(new InvalidOperationException("none")));
        Assert.Null(AwsRoleAssumeException.Find(null));
    }
}
