/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The per-server AWS role rules (#5452): <see cref="AwsRoleSettings"/> is the one validator every surface that takes a
/// role ARN and an external ID shares, so its answers and its sentences are pinned here.
/// </summary>
public sealed class AwsRoleSettingsTests
{
    [Theory]
    [InlineData(null, null)]
    [InlineData("", null)]
    [InlineData("   ", null)]
    [InlineData("\t\n", null)]
    [InlineData("  arn:aws:iam::123456789012:role/darling-monitor  ", "arn:aws:iam::123456789012:role/darling-monitor")]
    public void Normalize_TrimsAndTurnsBlankIntoNull(string? input, string? expected)
    {
        Assert.Equal(expected, AwsRoleSettings.Normalize(input));
    }

    [Theory]
    [InlineData("arn:aws:iam::123456789012:role/darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/a")]
    [InlineData("arn:aws-cn:iam::123456789012:role/darling-monitor")]
    [InlineData("arn:aws-us-gov:iam::123456789012:role/darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/monitoring/darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/a/b/c/darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/name_with+=,.@-chars")]
    [InlineData("arn:aws:iam::000000000000:role/darling")]
    public void ValidateRole_AcceptsAnIamRoleArn(string role)
    {
        Assert.Null(AwsRoleSettings.ValidateRole(role));
    }

    [Fact]
    public void ValidateRole_AcceptsAnUnsetRole()
    {
        Assert.Null(AwsRoleSettings.ValidateRole(null));
    }

    [Theory]
    [InlineData("darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:user/darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/")]
    [InlineData("arn:aws:iam::12345678901:role/darling-monitor")]
    [InlineData("arn:aws:iam::1234567890123:role/darling-monitor")]
    [InlineData("arn:aws:iam::12345678901a:role/darling-monitor")]
    [InlineData("arn:aws:s3::123456789012:role/darling-monitor")]
    [InlineData("arn:azure:iam::123456789012:role/darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/has space")]
    [InlineData("arn:aws:iam::123456789012:role/monitoring//darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role//darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/darling-monitor/")]
    [InlineData("arn:aws:iam::123456789012:role/darlingé")]
    [InlineData("ARN:aws:iam::123456789012:role/darling-monitor")]
    [InlineData("arn:aws:iam::123456789012:role/darling-monitor\nextra")]
    public void ValidateRole_RefusesAnythingElse_WithTheSharedSentence(string role)
    {
        Assert.Equal(AwsRoleSettings.InvalidRoleMessage, AwsRoleSettings.ValidateRole(role));
    }

    [Fact]
    public void ValidateRole_RefusesANameOverSixtyFourCharacters_AndAPathOverFiveHundredTwelve()
    {
        var name64 = new string('n', 64);
        Assert.Null(AwsRoleSettings.ValidateRole($"arn:aws:iam::123456789012:role/{name64}"));
        Assert.Equal(
            AwsRoleSettings.InvalidRoleMessage,
            AwsRoleSettings.ValidateRole($"arn:aws:iam::123456789012:role/{name64}n"));

        /* A path is "/seg/seg/": 255 characters of one segment make a 257-character path, so two of them are 513. */
        var segment = new string('p', 255);
        Assert.Null(AwsRoleSettings.ValidateRole($"arn:aws:iam::123456789012:role/{segment}/darling"));
        Assert.Equal(
            AwsRoleSettings.InvalidRoleMessage,
            AwsRoleSettings.ValidateRole($"arn:aws:iam::123456789012:role/{segment}/{segment}/darling"));
    }

    [Fact]
    public void ValidateRole_RefusesAnArnOverTheLengthCap()
    {
        var role = "arn:aws:iam::123456789012:role/" + new string('p', 2048);
        Assert.True(role.Length > AwsRoleSettings.RoleArnMaxLength);
        Assert.Equal(AwsRoleSettings.InvalidRoleMessage, AwsRoleSettings.ValidateRole(role));
    }

    [Theory]
    [InlineData("ab")]
    [InlineData("0123456789")]
    [InlineData("tenant-1234:prod/us_east+1=x,y.z@corp")]
    [InlineData("A-b_C+d=e,f.g@h:i/j-k")]
    public void ValidateExternalId_AcceptsAwsCharactersAtTwoToTwelveTwentyFour(string externalId)
    {
        Assert.Null(AwsRoleSettings.ValidateExternalId(externalId));
    }

    [Fact]
    public void ValidateExternalId_AcceptsAnUnsetOneAndTheBounds()
    {
        Assert.Null(AwsRoleSettings.ValidateExternalId(null));
        Assert.Null(AwsRoleSettings.ValidateExternalId(new string('x', AwsRoleSettings.ExternalIdMaxLength)));
        Assert.Equal(
            AwsRoleSettings.InvalidExternalIdMessage,
            AwsRoleSettings.ValidateExternalId(new string('x', AwsRoleSettings.ExternalIdMaxLength + 1)));
    }

    [Theory]
    [InlineData("x")]
    [InlineData("has space")]
    [InlineData("tab\there")]
    [InlineData("semi;colon")]
    [InlineData("quote'quote")]
    [InlineData("café-id")]
    [InlineData("new\nline")]
    [InlineData("star*")]
    public void ValidateExternalId_RefusesTooShortAndAnyOtherCharacter(string externalId)
    {
        Assert.Equal(AwsRoleSettings.InvalidExternalIdMessage, AwsRoleSettings.ValidateExternalId(externalId));
    }

    [Fact]
    public void ValidatePair_ReportsFormatFirst_ThenAnExternalIdWithNoRole()
    {
        const string role = "arn:aws:iam::123456789012:role/darling-monitor";

        Assert.Null(AwsRoleSettings.ValidatePair(null, null));
        Assert.Null(AwsRoleSettings.ValidatePair(role, null));
        Assert.Null(AwsRoleSettings.ValidatePair(role, "tenant-1"));

        Assert.Equal(AwsRoleSettings.ExternalIdNeedsRoleMessage, AwsRoleSettings.ValidatePair(null, "tenant-1"));
        Assert.Equal(AwsRoleSettings.InvalidRoleMessage, AwsRoleSettings.ValidatePair("nope", "tenant-1"));
        Assert.Equal(AwsRoleSettings.InvalidExternalIdMessage, AwsRoleSettings.ValidatePair(role, "x"));
    }

    [Theory]
    /* old,                                           new,                                           stored, sent, needs */
    [InlineData(null, "arn:aws:iam::123456789012:role/b", true, false, true)]
    [InlineData("arn:aws:iam::123456789012:role/a", "arn:aws:iam::123456789012:role/b", true, false, true)]
    [InlineData("arn:aws:iam::123456789012:role/a", "arn:aws:iam::123456789012:role/b", true, true, false)]
    [InlineData("arn:aws:iam::123456789012:role/a", "arn:aws:iam::123456789012:role/b", false, false, false)]
    [InlineData("arn:aws:iam::123456789012:role/a", "arn:aws:iam::123456789012:role/a", true, false, false)]
    [InlineData("arn:aws:iam::123456789012:role/a", "arn:aws:iam::123456789012:role/A", true, false, true)]
    [InlineData("arn:aws:iam::123456789012:role/a", null, true, false, false)]
    [InlineData(null, null, true, false, false)]
    public void RoleChangeNeedsExternalId_OnlyForANewRoleOverAStoredIdThatWasNotSent(
        string? oldRole, string? newRole, bool stored, bool sent, bool expected)
    {
        Assert.Equal(expected, AwsRoleSettings.RoleChangeNeedsExternalId(oldRole, newRole, stored, sent));
    }

    [Fact]
    public void TheSentences_ArePinned_AndNameNoRealAccount()
    {
        Assert.Equal(
            "The AWS role must be an IAM role ARN, such as arn:aws:iam::123456789012:role/darling-monitor.",
            AwsRoleSettings.InvalidRoleMessage);
        Assert.Equal(
            "The external ID must be 2 to 1224 characters: letters, digits and _ + = , . @ : / - with no spaces.",
            AwsRoleSettings.InvalidExternalIdMessage);
        Assert.Equal(
            "An external ID needs an AWS role ARN. Set the role, or clear the external ID.",
            AwsRoleSettings.ExternalIdNeedsRoleMessage);
        Assert.Equal(
            "An AWS role applies only to a PostgreSQL target on Amazon RDS or Aurora.",
            AwsRoleSettings.RoleNeedsPostgresMessage);
        Assert.Equal(
            "Changing the AWS role needs the external ID with it: send the external ID again, or clear it.",
            AwsRoleSettings.RoleChangeNeedsExternalIdMessage);
    }
}
