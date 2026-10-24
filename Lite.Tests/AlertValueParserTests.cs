/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the history stores' text fallback (#1830). The inputs are the REAL display strings the
/// alert producers emit — each row here names the producer that emits it — because the defect this
/// parser replaces was precisely a fallback whose happy-path tests used conveniently parseable
/// strings while production text carried labels.
/// </summary>
public sealed class AlertValueParserTests
{
    [Theory]
    [InlineData("87% (Total CPU)", 87)]      /* High CPU — the #1830 field case, stored 0 before */
    [InlineData("92% (SQL CPU)", 92)]        /* High CPU, SQL-only mode */
    [InlineData("+3 more incidents this cycle", 3)] /* per-event overflow trailer */
    [InlineData("92.5%", 92.5)]              /* plain percent — the old fallback's happy path */
    [InlineData("80%", 80)]
    [InlineData("1074", 1074)]               /* bare count (blocking/deadlocks) */
    [InlineData("-5", -5)]
    public void ParseOrDefault_ExtractsTheLeadingNumber(string text, double expected)
    {
        Assert.Equal(expected, AlertValueParser.ParseOrDefault(text));
    }

    [Theory]
    [InlineData("PRIMARY")]                  /* AG state strings — genuinely no number */
    [InlineData("Online")]
    [InlineData("caught up")]
    [InlineData("")]
    [InlineData(null)]
    public void ParseOrDefault_NoNumber_ReturnsTheFallback(string? text)
    {
        Assert.Equal(0, AlertValueParser.ParseOrDefault(text));
        Assert.Equal(-1, AlertValueParser.ParseOrDefault(text, fallback: -1));
    }

    [Theory]
    [InlineData("1,234", 1234)]              /* grouped count — stored 1 before (#1834 review) */
    [InlineData("1,234,567 reads", 1234567)] /* several groups, then a label */
    [InlineData("1,234.56%", 1234.56)]       /* groups AND a decimal */
    [InlineData("1234.56%", 1234.56)]        /* a long run before a DECIMAL point is fine - no group here */
    [InlineData("-1,024", -1024)]
    public void ParseOrDefault_ReadsGroupedNumbers_EnUs(string text, double expected)
    {
        /* No producer formats with "N0" today, but the alert code right next door does, and a
           truncated-but-plausible number is harder to notice than the ": 0" this parser replaced. */
        WithCulture("en-US", () => Assert.Equal(expected, AlertValueParser.ParseOrDefault(text)));
    }

    [Theory]
    [InlineData("1,5", 1)]                   /* not a group: only one digit follows */
    [InlineData("55,66", 55)]                /* not a group: only two */
    [InlineData("1,2,3", 1)]                 /* a comma-joined LIST must not fuse into 123 */
    [InlineData("1,2345", 1)]                /* not a group: four digits follow */
    [InlineData("3 sessions, 500 ms", 3)]    /* an ordinary space is not en-US's group separator */
    [InlineData("1234,567", 1234)]           /* leading run of 4 is no valid first group - must not fuse */
    [InlineData("12345,678,901", 12345)]     /* same, with more well-formed groups behind it */
    public void ParseOrDefault_RejectsMalformedGroups_EnUs(string text, double expected)
    {
        /* .NET's own AllowThousands does NOT validate group placement — double.Parse("1,2,3", en-US)
           returns 123 — so the scan has to require a whole group itself. These pin that it does. */
        WithCulture("en-US", () => Assert.Equal(expected, AlertValueParser.ParseOrDefault(text)));
    }

    [Theory]
    [InlineData("1.234", 1234)]              /* de-DE groups with '.', so this is one thousand */
    [InlineData("1.234,5 ms", 1234.5)]       /* '.' groups, ',' is the decimal */
    [InlineData("87.5%", 87)]                /* '.' with one digit after is no group: 87, as before */
    public void ParseOrDefault_ReadsGroupedNumbers_DeDe(string text, double expected)
    {
        /* The locale where getting this wrong is worst: the group separator IS the character every
           other locale decimal-points with. */
        WithCulture("de-DE", () => Assert.Equal(expected, AlertValueParser.ParseOrDefault(text)));
    }

    [Fact]
    public void ParseOrDefault_HonorsTheCurrentCulture_CommaDecimal()
    {
        /* The producers format with CurrentCulture, so the fallback must parse with it too — a
           de-DE "92,5%" is 92.5 there, and hardening to InvariantCulture would break exactly the
           locales the reporter of #1830 uses. */
        WithCulture("de-DE", () => Assert.Equal(92.5, AlertValueParser.ParseOrDefault("92,5% (SQL CPU)")));
    }

    private static void WithCulture(string name, Action assert)
    {
        var original = Thread.CurrentThread.CurrentCulture;
        try
        {
            Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo(name);
            assert();
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = original;
        }
    }
}
