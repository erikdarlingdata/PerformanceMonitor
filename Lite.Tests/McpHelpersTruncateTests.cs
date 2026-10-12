/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// <see cref="McpHelpers.Truncate"/> cuts on a whole text element, so it never splits an emoji's surrogate pair or a
/// letter from its accent. A split pair reaches the caller as U+FFFD, because System.Text.Json writes that in place of
/// a lone surrogate. ASCII values are cut exactly where they were before.
/// </summary>
public sealed class McpHelpersTruncateTests
{
    [Fact]
    public void ACutInsideASurrogatePair_StopsBeforeTheWholeCharacter()
    {
        Assert.Equal("ab... (truncated)", McpHelpers.Truncate("ab\U0001F600cd", 3));
    }

    [Fact]
    public void ACutBetweenALetterAndItsAccent_StopsBeforeTheLetter()
    {
        Assert.Equal("ab... (truncated)", McpHelpers.Truncate("abécd", 3));
    }

    [Theory]
    [InlineData("abcdef", 3, "abc... (truncated)")]
    [InlineData("abc", 3, "abc")]
    [InlineData("ab\ncd", 3, "ab\n... (truncated)")]
    [InlineData(null, 3, null)]
    public void AnAsciiValue_IsCutExactlyAtTheLimit(string? value, int maxLength, string? expected)
    {
        Assert.Equal(expected, McpHelpers.Truncate(value, maxLength));
    }

    /// <summary>
    /// The shared cut keeps the #3625 rule: on every input it keeps exactly what
    /// <see cref="WebhookAlertService.SlackCutLength"/> keeps. The inputs mix ASCII, CR LF, accents, emoji with
    /// modifiers and joiners, flags, a prepended mark and bare combining marks, at every limit from 0 to past the end.
    /// </summary>
    [Fact]
    public void TheCut_KeepsWhatTheSlackCutKeeps_OnEveryInput()
    {
        var pieces = new[]
        {
            "a", "b", " ", "\r", "\n", "\r\n", "é", "é", "́", "\U0001F600", "\U0001F44D\U0001F3FD",
            "\U0001F468‍\U0001F469", "\U0001F1FA\U0001F1F8", "؀", "中", "#️⃣",
        };
        var random = new Random(4945);
        var failures = new List<string>();
        var checkedCount = 0;

        for (var i = 0; i < 4000; i++)
        {
            var text = new StringBuilder();
            var count = random.Next(0, 12);
            for (var j = 0; j < count; j++)
            {
                text.Append(pieces[random.Next(pieces.Length)]);
            }

            var value = text.ToString();
            for (var limit = 0; limit <= value.Length + 1; limit++)
            {
                checkedCount++;
                var expected = WebhookAlertService.SlackCutLength(value, limit);
                var actual = McpHelpers.TextElementCutLength(value, limit);
                if (expected != actual)
                {
                    failures.Add($"limit {limit}, {Escaped(value)}: kept {actual}, the Slack cut keeps {expected}");
                }
            }
        }

        Assert.True(failures.Count == 0, $"{failures.Count} of {checkedCount} cuts differ:\n" + string.Join("\n", failures.Take(20)));
    }

    private static string Escaped(string value) =>
        string.Concat(value.Select(c => c < 0x80 && !char.IsControl(c) ? c.ToString() : $"\\u{(int)c:X4}"));
}
