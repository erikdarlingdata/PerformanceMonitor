/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The per-process cache of assumed-role credentials (#5452): the allow list and the partition are checked on every
/// call and before any AWS call, and one key means one entry. The example account is 123456789012.
/// </summary>
public sealed class AwsRoleCredentialCacheTests
{
    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string ChinaRole = "arn:aws-cn:iam::123456789012:role/darling-monitor";
    private const string GovRole = "arn:aws-us-gov:iam::123456789012:role/darling-monitor";

    private static AwsRoleCredentialCache Cache(FakeSts sts, AwsRoleAllowlist? list = null, ManualTimeProvider? clock = null) =>
        new(list ?? AwsRoleAllowlist.From(new[] { "123456789012", "aws-cn:123456789012", "aws-us-gov:123456789012" }), null, sts.Factory, FakeSts.Source, clock ?? new ManualTimeProvider());

    /* ---- the allow list, at use ---------------------------------------------------------------------------- */

    [Fact]
    public void ARoleTheListDoesNotAllow_IsNeverAssumed_WithTheSettledText()
    {
        var sts = new FakeSts();
        var cache = Cache(sts, AwsRoleAllowlist.Empty);

        var ex = Assert.Throws<AwsRoleAssumeException>(() => cache.For(new AwsRoleKey(Role, null), "us-east-1"));

        Assert.Equal(AwsRoleAssumeKind.NotAllowed, ex.Kind);
        Assert.True(ex.IsConfiguration);
        Assert.Equal(
            "The AWS role arn:aws:iam::123456789012:role/darling-monitor on this server is not in allowedAwsRoles in darling.json, so it was not used. "
            + "Nothing was read this cycle. Add the role or its account id to the list and restart the service, or clear the role on this server.",
            ex.Message);
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, sts.Calls);
        Assert.Empty(sts.Regions);
    }

    [Fact]
    public void ARoleOnAnotherAccountsList_IsNotAllowed()
    {
        var cache = Cache(new FakeSts(), AwsRoleAllowlist.From(new[] { "999999999999", "arn:aws:iam::123456789012:role/other-role" }));

        var ex = Assert.Throws<AwsRoleAssumeException>(() => cache.For(new AwsRoleKey(Role, null), "us-east-1"));

        Assert.Equal(AwsRoleAssumeKind.NotAllowed, ex.Kind);
    }

    [Theory]
    [InlineData("123456789012")]
    [InlineData("arn:aws:iam::123456789012:role/darling-monitor")]
    public void AnAccountIdOrTheExactArn_AllowsTheRole(string entry)
    {
        var cache = Cache(new FakeSts(), AwsRoleAllowlist.From(new[] { entry }));

        Assert.NotNull(cache.For(new AwsRoleKey(Role, null), "us-east-1"));
        Assert.True(cache.Contains(new AwsRoleKey(Role, null)));
    }

    [Fact]
    public async Task TheListIsCheckedOnEveryCall_EvenWhenTheRoleIsAlreadyCached()
    {
        var sts = new FakeSts();
        var cache = Cache(sts);
        var key = new AwsRoleKey(Role, null);

        var credentials = cache.For(key, "us-east-1");
        await credentials.GetCredentialsAsync();
        Assert.Equal(1, sts.Calls);

        var ex = Assert.Throws<AwsRoleAssumeException>(() => cache.For(key, "us-east-1", AwsRoleAllowlist.Empty));
        Assert.Equal(AwsRoleAssumeKind.NotAllowed, ex.Kind);
        Assert.Equal(1, sts.Calls);

        /* The cache's own list still allows it, so the next call without an override is served. */
        Assert.NotNull(cache.For(key, "us-east-1"));
    }

    [Fact]
    public void TheListIsCheckedBeforeThePartition()
    {
        var cache = Cache(new FakeSts(), AwsRoleAllowlist.Empty);

        var ex = Assert.Throws<AwsRoleAssumeException>(() => cache.For(new AwsRoleKey(Role, null), "cn-north-1"));

        Assert.Equal(AwsRoleAssumeKind.NotAllowed, ex.Kind);
    }

    /* ---- the partition, on every call ---------------------------------------------------------------------- */

    [Fact]
    public async Task TheRolePartitionMustBeTheRegionsPartition_OnEveryCall()
    {
        var sts = new FakeSts();
        var cache = Cache(sts);
        var key = new AwsRoleKey(Role, null);

        await cache.For(key, "us-east-1").GetCredentialsAsync();

        var china = Assert.Throws<AwsRoleAssumeException>(() => cache.For(key, "cn-north-1"));
        Assert.Equal(AwsRoleAssumeKind.PartitionMismatch, china.Kind);
        Assert.True(china.IsConfiguration);
        Assert.Equal(
            "Role arn:aws:iam::123456789012:role/darling-monitor is in AWS partition aws, but this target's region cn-north-1 is in partition aws-cn. "
            + "A role can be assumed only inside its own partition. Nothing was read this cycle. "
            + "Set a role from the target's partition on this server, or clear the role, or correct the server's host name if it names the wrong region.",
            china.Message);

        /* The entry exists and was used from us-east-1, and the mismatch is still refused: the check is per call. */
        var gov = Assert.Throws<AwsRoleAssumeException>(() => cache.For(key, "us-gov-west-1"));
        Assert.Equal(AwsRoleAssumeKind.PartitionMismatch, gov.Kind);
        Assert.NotNull(cache.For(key, "eu-west-1"));
        Assert.Equal(1, sts.Calls);
    }

    [Theory]
    [InlineData(ChinaRole, "cn-north-1")]
    [InlineData(GovRole, "us-gov-west-1")]
    [InlineData(Role, "ap-southeast-2")]
    public void ARoleInTheRegionsOwnPartition_IsServed(string role, string region)
    {
        var cache = Cache(new FakeSts());

        Assert.NotNull(cache.For(new AwsRoleKey(role, null), region));
    }

    [Fact]
    public void AnArnWithNoPartition_IsAMismatch_NotACrash()
    {
        var cache = Cache(new FakeSts(), AwsRoleAllowlist.From(new[] { "not-an-arn" }));

        Assert.Equal("", AwsRoleCredentialCache.PartitionOf("not-an-arn"));
        Assert.Equal("aws-cn", AwsRoleCredentialCache.PartitionOf(ChinaRole));
        Assert.Throws<ArgumentException>(() => cache.For(new AwsRoleKey(Role, null), " "));
    }

    /* ---- one entry per key --------------------------------------------------------------------------------- */

    [Fact]
    public async Task OneKey_OneEntry_OneStsCall_ForEveryServerThatNamesIt()
    {
        var sts = new FakeSts();
        var cache = Cache(sts);
        var key = new AwsRoleKey(Role, "shared-external-id");

        var first = cache.For(key, "us-east-1");
        var second = cache.For(new AwsRoleKey(Role, "shared-external-id"), "eu-west-1");
        await Task.WhenAll(first.GetCredentialsAsync(), second.GetCredentialsAsync());

        Assert.Equal(1, cache.Count);
        Assert.Equal(1, sts.Calls);
    }

    [Fact]
    public void ARoleWithAnotherExternalId_IsAnotherEntry()
    {
        var cache = Cache(new FakeSts());

        cache.For(new AwsRoleKey(Role, "first-external-id"), "us-east-1");
        cache.For(new AwsRoleKey(Role, "second-external-id"), "us-east-1");
        cache.For(new AwsRoleKey(Role, null), "us-east-1");

        Assert.Equal(3, cache.Count);
    }

    [Fact]
    public void Retain_DropsTheKeysNoEnabledServerNamesAnyMore()
    {
        var cache = Cache(new FakeSts());
        var keep = new AwsRoleKey(Role, "keep-external-id");
        var drop = new AwsRoleKey(Role, "drop-external-id");
        cache.For(keep, "us-east-1");
        cache.For(drop, "us-east-1");

        cache.Retain(new[] { keep });

        Assert.True(cache.Contains(keep));
        Assert.False(cache.Contains(drop));
        Assert.Equal(1, cache.Count);

        cache.Retain(Array.Empty<AwsRoleKey>());
        Assert.Equal(0, cache.Count);
    }
}
