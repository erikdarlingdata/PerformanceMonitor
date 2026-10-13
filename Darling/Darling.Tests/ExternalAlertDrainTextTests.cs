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
    public void PlainText_KeepsTheSanitizersRules()
    {
        Assert.Equal("a b", ExternalAlertDrain.CleanText("  a\r\n b  ", 200));
        Assert.Equal(string.Empty, ExternalAlertDrain.CleanText(null, 200));
        Assert.Equal(string.Empty, ExternalAlertDrain.CleanText("   ", 200));
        Assert.Equal("abc", ExternalAlertDrain.CleanText("abcdef", 3));
        Assert.Equal("ab", ExternalAlertDrain.CleanText("ab cdef", 3));
    }
}
