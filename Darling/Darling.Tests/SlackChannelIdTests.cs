/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7656 (V252): the one shape rule for a route's Slack bot channel. A channel id starts with C or G and goes on with capital letters and
/// digits, 2 to 32 characters in all; blank is none. Everything else, a channel name first of all, is refused with the one sentence every
/// writer uses, and the value is trimmed before it is checked or stored.
/// </summary>
public sealed class SlackChannelIdTests
{
    private const string Sentence =
        "The Slack bot channel is a channel id: C or G, then capital letters and digits (for example C0123ABCDEF), or blank for none. Not the channel's name.";

    [Theory]
    [InlineData("C0123ABCDEF")]
    [InlineData("G9ZZZ")]
    [InlineData("CA")]
    [InlineData("  C0123ABCDEF  ")]
    [InlineData("\tG9ZZZ\n")]
    public void AChannelId_IsValid_AndIsTrimmed(string text)
    {
        Assert.True(SlackChannelId.IsValid(text));
        Assert.Null(SlackChannelId.Refusal(text));
        Assert.Equal(text.Trim(), SlackChannelId.Normalize(text));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Blank_IsNone_NotAnId_AndNotRefused(string? text)
    {
        Assert.False(SlackChannelId.IsValid(text));
        Assert.Null(SlackChannelId.Refusal(text));
        Assert.Equal("", SlackChannelId.Normalize(text));
    }

    [Theory]
    [InlineData("#ops")]
    [InlineData("ops")]
    [InlineData("c0123abc")]
    [InlineData("U0123ABC")]
    [InlineData("T0123ABC")]
    [InlineData("C")]
    [InlineData("C0123 ABC")]
    [InlineData("C0123-ABC")]
    public void AChannelName_ALowercaseId_AUserOrWorkspaceId_ABareLetter_OrPunctuation_IsRefusedWithTheSentence(string text)
    {
        Assert.False(SlackChannelId.IsValid(text));
        Assert.Equal(SlackChannelId.RefusalText, SlackChannelId.Refusal(text));
    }

    [Fact]
    public void TheLongestId_Is32Characters_AndOneMoreIsRefused()
    {
        var longest = "C" + new string('A', 31);
        Assert.Equal(32, longest.Length);
        Assert.True(SlackChannelId.IsValid(longest));
        Assert.Null(SlackChannelId.Refusal(longest));

        var tooLong = longest + "A";
        Assert.Equal(33, tooLong.Length);
        Assert.False(SlackChannelId.IsValid(tooLong));
        Assert.Equal(SlackChannelId.RefusalText, SlackChannelId.Refusal(tooLong));
    }

    [Fact]
    public void TheSentence_NamesTheShape_AnExample_AndThatANameIsNotOne() =>
        Assert.Equal(Sentence, SlackChannelId.RefusalText);

    [Theory]
    [InlineData("#ops")]
    [InlineData("C0123ABCDEF")]
    [InlineData("")]
    [InlineData(null)]
    public void TheRouteWriter_RefusesWithTheSameSentence_OrNotAtAll(string? text) =>
        Assert.Equal(SlackChannelId.Refusal(text), NotificationRouteWriter.SlackBotChannelRefusal(text));
}
