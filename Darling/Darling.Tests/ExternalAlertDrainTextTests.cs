/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The drain caps the title, detail and server name of an external alert in code points, the unit the submit function counts
/// in (<c>char_length</c>), so a surrogate pair is never cut in half (#5654).
/// </summary>
public sealed class ExternalAlertDrainTextTests
{
    private const string Emoji = "\U0001F600";

    [Fact]
    public void ATextAtTheCodePointCap_PassesWhole_EvenWhenItsUtf16UnitsExceedTheCap()
    {
        /* 1 + 199 emoji is 200 code points and 399 UTF-16 units: it passes the function's check. */
        var text = "a" + string.Concat(Enumerable.Repeat(Emoji, 199));

        Assert.Equal(text, ExternalAlertDrain.CleanText(text, 200));
    }

    [Fact]
    public void ATextOverTheCap_IsCutOnACodePointBoundary_AndNeverLeavesALoneSurrogate()
    {
        var text = "a" + string.Concat(Enumerable.Repeat(Emoji, 150));

        var cut = ExternalAlertDrain.CleanText(text, 100);

        Assert.Equal("a" + string.Concat(Enumerable.Repeat(Emoji, 99)), cut);
        Assert.Equal(100, cut.EnumerateRunes().Count());
        Assert.DoesNotContain('�', cut);
    }

    [Fact]
    public void AnEmojiStraddlingTheOldUtf16Cap_IsKeptWhole()
    {
        /* 199 letters, then an emoji: the 200th code point, units 199-200. A cap in units would keep one half of it. */
        var text = new string('x', 199) + Emoji + "tail";

        var cut = ExternalAlertDrain.CleanText(text, 200);

        Assert.Equal(new string('x', 199) + Emoji, cut);
    }

    [Fact]
    public void ADetail_KeepsItsLineBreaks_AsLineFeedsOnly()
    {
        Assert.Equal("a\nb\nc\nd", ExternalAlertDrain.CleanDetail("a\nb\r\nc\rd", 200));
        /* A carriage return then a line feed is one break, not two: no blank line appears. */
        Assert.Equal("a\nb", ExternalAlertDrain.CleanDetail("a\r\nb", 200));
        Assert.Equal(string.Empty, ExternalAlertDrain.CleanDetail(null, 200));
        Assert.Equal(string.Empty, ExternalAlertDrain.CleanDetail("  \n \r\n ", 200));
    }

    [Fact]
    public void ADetail_CollapsesRunsOfBlankLines_TrimsTrailingSpaces_AndKeepsIndentation()
    {
        Assert.Equal("a\n\nb", ExternalAlertDrain.CleanDetail("\n\na   \n\n\n \n\t\nb  \n\n", 200));
        Assert.Equal("a\n  b\n    c", ExternalAlertDrain.CleanDetail("a\n  b\n    c", 200));
    }

    [Fact]
    public void AOneLineDetail_ReadsExactlyAsTheOldTextCleanerReadsIt()
    {
        /* A tab, leading spaces and a run of spaces: the old cleaner makes each run one space and drops the leading ones. */
        const string Detail = "  disk\t\tfull   93%";
        Assert.Equal("disk full 93%", ExternalAlertDrain.CleanDetail(Detail, 200));
        Assert.Equal(ExternalAlertDrain.CleanText(Detail, 200), ExternalAlertDrain.CleanDetail(Detail, 200));

        /* A store still on the old function body keeps "x  \r\n  y" as five spaces. It is one line here and reads as one space. */
        Assert.Equal("x y", ExternalAlertDrain.CleanDetail("x     y", 200));

        /* Breaks that leave one line behind, and the cap, read as the old cleaner reads them. */
        Assert.Equal("abc", ExternalAlertDrain.CleanDetail("\n\n  abc  \r\n", 200));
        Assert.Equal(ExternalAlertDrain.CleanText("  ab  cd  ", 4), ExternalAlertDrain.CleanDetail("  ab  cd  ", 4));
    }

    [Fact]
    public void ADetail_StillTurnsOtherControlCharactersToSpaces_AndDropsInvisibleOnes()
    {
        /* The #5654 rules: a tab, NEL, the line and paragraph separators and DEL are spaces and never a break; a zero-width space
           and a right-to-left override are dropped. */
        Assert.Equal("a b c d e f", ExternalAlertDrain.CleanDetail("a\tb\u0085c\u2028d\u2029e\u007Ff", 200));
        Assert.Equal("abc", ExternalAlertDrain.CleanDetail("a\u200Bb\u202Ec", 200));
        Assert.Equal("a\nb", ExternalAlertDrain.CleanDetail("a\u202E\nb\u200B", 200));
    }

    [Fact]
    public void ADetail_IsCutAtFiftyLines_WithTheCutMarkedByALineOfDots()
    {
        var fifty = string.Join('\n', Enumerable.Range(1, ExternalAlertDrain.MaxDetailLines).Select(i => "l" + i));
        Assert.Equal(fifty, ExternalAlertDrain.CleanDetail(fifty, 2000));

        var sixty = string.Join('\n', Enumerable.Range(1, 60).Select(i => "l" + i));
        var cut = ExternalAlertDrain.CleanDetail(sixty, 2000).Split('\n');

        Assert.Equal(ExternalAlertDrain.MaxDetailLines, cut.Length);
        Assert.Equal("l1", cut[0]);
        Assert.Equal("l49", cut[^2]);
        Assert.Equal("...", cut[^1]);

        /* A blank line right at the cut does not leave the dots hanging below it. */
        var blankAtCut = string.Join('\n', Enumerable.Range(1, 48).Select(i => "l" + i)) + "\n\nl50\nl51\nl52";
        var tail = ExternalAlertDrain.CleanDetail(blankAtCut, 2000).Split('\n');
        Assert.Equal("l48", tail[^2]);
        Assert.Equal("...", tail[^1]);
    }

    [Fact]
    public void ADetail_KeepsTheCodePointCap_AndNeverSplitsASurrogatePair()
    {
        var text = "a\n" + string.Concat(Enumerable.Repeat(Emoji, 150));

        var cut = ExternalAlertDrain.CleanDetail(text, 100);

        Assert.Equal("a\n" + string.Concat(Enumerable.Repeat(Emoji, 98)), cut);
        Assert.Equal(100, cut.EnumerateRunes().Count());
    }

    [Fact]
    public void ATitleOrServerName_StillFlattensEveryBreak_AsBefore()
    {
        Assert.Equal("Hourly summary for web01", ExternalAlertDrain.CleanText("Hourly\nsummary\r\nfor\rweb01", 200));
        Assert.Equal("a b", ExternalAlertDrain.CleanText("a\n\n  b", 200));
    }

    [Fact]
    public void PlainText_KeepsTheSanitizersRules()
    {
        Assert.Equal("a b", ExternalAlertDrain.CleanText("  a\r\n b  ", 200));
        Assert.Equal(string.Empty, ExternalAlertDrain.CleanText(null, 200));
        Assert.Equal(string.Empty, ExternalAlertDrain.CleanText("   ", 200));
        Assert.Equal("abc", ExternalAlertDrain.CleanText("abcdef", 3));
        Assert.Equal("ab", ExternalAlertDrain.CleanText("ab cdef", 3));
    }
}
