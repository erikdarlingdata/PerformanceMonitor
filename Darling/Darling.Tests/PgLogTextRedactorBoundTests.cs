/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The log text and setting redactors read a field in time that grows with its length, whatever the field repeats:
/// a value slot ends at the first place the text after it begins and does not try every other split, a field
/// longer than <see cref="PgLogTextRedactor.MaxSqlFieldLength"/> is withheld without being matched, and a pattern that
/// still outruns its bound withholds the whole field. The class runs in <c>[Collection("timing")]</c>: its time
/// bounds are judged against the runner's load.
/// </summary>
[Collection("timing")]
public sealed class PgLogTextRedactorBoundTests
{
    private const string W = PgLogTextRedactor.WithheldValues;

    /// <summary>A DETAIL that repeats the exclusion form's separator a thousand times and never closes.</summary>
    private static readonly string RepeatedConflict =
        "Key (" + string.Concat(Enumerable.Repeat("a)=(b) conflicts with existing key (", 1000));

    private static TimeSpan Time(Action action)
    {
        var clock = Stopwatch.StartNew();
        action();
        return clock.Elapsed;
    }

    [Fact]
    public void ARepeatedKeySeparator_IsReadInAFixedTime_AndWithheld()
    {
        // The generous bound makes a pattern that outruns it visible as time spent: a pass here is the slots
        // being read in one pass, not a timeout that withheld the field.
        using var generous = GenerousRedactorTimeout.Begin();
        string? plain = null;
        string? classed = null;

        var elapsed = Time(() =>
        {
            plain = PgLogTextRedactor.RedactDetail(RepeatedConflict);
            classed = PgLogTextRedactor.RedactDetail(RepeatedConflict, sqlState: "23P01");
        });

        Assert.Equal(W, plain);
        Assert.Equal(W, classed);
        Assert.True(elapsed < TimeSpan.FromSeconds(4), $"two reads took {elapsed}");
    }

    [Fact]
    public void ARepeatedKeySeparator_ThatClosesProperly_IsMaskedInAFixedTime()
    {
        using var generous = GenerousRedactorTimeout.Begin();
        string? masked = null;

        var elapsed = Time(() => masked = PgLogTextRedactor.RedactDetail(RepeatedConflict + "a)=(b)."));

        Assert.Equal("Key (a)=(?) conflicts with existing key (a)=(?).", masked);
        Assert.True(elapsed < TimeSpan.FromSeconds(4), $"the read took {elapsed}");
    }

    [Fact]
    public void ARepeatedInputSyntaxQuote_IsReadInAFixedTime()
    {
        using var generous = GenerousRedactorTimeout.Begin();
        var closed = "invalid input syntax for type " + string.Concat(Enumerable.Repeat(": \"", 10_000));
        var open = "invalid input syntax for type " + string.Concat(Enumerable.Repeat(": \"x", 10_000));
        string? maskedClosed = null;
        string? maskedOpen = null;

        var elapsed = Time(() =>
        {
            maskedClosed = PgLogTextRedactor.RedactMessage(closed);
            maskedOpen = PgLogTextRedactor.RedactMessage(open, sqlState: "22P02");
        });

        Assert.Equal("invalid input syntax for type : \"?\"", maskedClosed);
        Assert.Equal(W, maskedOpen);
        Assert.True(elapsed < TimeSpan.FromSeconds(4), $"two reads took {elapsed}");
    }

    [Fact]
    public void ARepeatedCopyAndPartitionSeparator_IsReadInAFixedTime()
    {
        using var generous = GenerousRedactorTimeout.Begin();
        var context = "COPY t, line 1: \"" + string.Concat(Enumerable.Repeat("COPY t, line 1, column c: \"", 1500));
        var partition = "Partition key of the failing row contains (" + string.Concat(Enumerable.Repeat("a) = (b", 3000));
        string? maskedContext = null;
        string? maskedPartition = null;

        var elapsed = Time(() =>
        {
            maskedContext = PgLogTextRedactor.RedactContext(context);
            maskedPartition = PgLogTextRedactor.RedactDetail(partition);
        });

        Assert.Equal("COPY t, line 1: \"?\"", maskedContext);
        Assert.Equal(W, maskedPartition);
        Assert.True(elapsed < TimeSpan.FromSeconds(4), $"two reads took {elapsed}");
    }

    /// <summary>The value slots that run to the LAST place their separator appears (a foreign key's value before its
    /// table, a quoted value before its type) are one scan back from the field's end, not a retry per separator. The
    /// bound here is 250 ms, short of the time the old shape took on this field, so a slot that is not linear shows as
    /// a withheld field where a masked one is expected.</summary>
    [Fact]
    public void ARepeatedGreedySlotSeparator_IsReadInOnePass_UnderAShortBound()
    {
        var keySeparator = ") is not present in table \"";
        var literalSeparator = "\" is out of range for type ";
        var key = "Key (c)=(" + string.Concat(Enumerable.Repeat(keySeparator, 2_000)) + "t\".";
        var keyOpen = "Key (c)=(" + string.Concat(Enumerable.Repeat(keySeparator, 2_000)) + "t";
        var literal = "value \"" + string.Concat(Enumerable.Repeat(literalSeparator, 2_000)) + "integer";
        var repeatedQuote = "Key (c)=(" + string.Concat(Enumerable.Repeat(")\"x", 20_000));

        var prior = PgSettingRedactor.MatchTimeoutForTest;
        PgSettingRedactor.MatchTimeoutForTest = TimeSpan.FromMilliseconds(250);
        string? maskedKey = null;
        string? withheldKey = null;
        string? maskedLiteral = null;
        string? withheldQuote = null;
        try
        {
            var elapsed = Time(() =>
            {
                maskedKey = PgLogTextRedactor.RedactDetail(key, sqlState: "23503");
                withheldKey = PgLogTextRedactor.RedactDetail(keyOpen, sqlState: "23503");
                maskedLiteral = PgLogTextRedactor.RedactMessage(literal, sqlState: "22003");
                withheldQuote = PgLogTextRedactor.RedactDetail(repeatedQuote, sqlState: "23503");
            });
            Assert.True(elapsed < TimeSpan.FromSeconds(4), $"four reads took {elapsed}");
        }
        finally
        {
            PgSettingRedactor.MatchTimeoutForTest = prior;
        }

        // Masked, not timed out: the value runs to the LAST separator and the name after it is kept.
        Assert.Equal("Key (c)=(?) is not present in table \"t\".", maskedKey);
        Assert.Equal("value \"?\" is out of range for type integer", maskedLiteral);
        Assert.Equal(W, withheldKey);
        Assert.Equal(W, withheldQuote);
    }

    [Fact]
    public void AFieldLongerThanTheCap_IsWithheldWithoutBeingMatched()
    {
        var body = new string('x', PgLogTextRedactor.MaxSqlFieldLength);

        Assert.Equal(W, PgLogTextRedactor.RedactDetail("Key (e)=(" + body + ") already exists."));
        Assert.Equal(W, PgLogTextRedactor.RedactDetail(body, sqlState: "23505"));
        Assert.Equal(W, PgLogTextRedactor.RedactContext("COPY t, line 1: \"" + body + "\""));
        Assert.Equal(W, PgLogTextRedactor.RedactMessage("invalid input syntax for type integer: \"" + body + "\""));
        Assert.Equal(W, PgLogTextRedactor.RedactMessage(body, sqlState: "22P02"));

        // Prose that opens like none of the forms is kept as written, as it is at any length.
        var prose = "Customer " + body + " has an account.";
        Assert.Equal(prose, PgLogTextRedactor.RedactDetail(prose));
        Assert.Equal(prose, PgLogTextRedactor.RedactMessage(prose));
    }

    [Fact]
    public void AFieldThatOutrunsItsBound_IsWithheldWhole_NotKeptAndNotHalfMasked()
    {
        var detail = "Key (" + new string('a', 30_000) + ")=(Kept-5698) already exists.";
        var context = "COPY " + new string('r', 30_000) + ", line 1: \"Kept-5698\"";
        var message = "invalid input syntax for type " + new string('t', 30_000) + ": \"Kept-5698\"";

        // Read normally, the three come back with their value slot masked.
        Assert.Equal("Key (" + new string('a', 30_000) + ")=(?) already exists.", PgLogTextRedactor.RedactDetail(detail));
        Assert.Equal("COPY " + new string('r', 30_000) + ", line 1: \"?\"", PgLogTextRedactor.RedactContext(context));
        Assert.Equal("invalid input syntax for type " + new string('t', 30_000) + ": \"?\"", PgLogTextRedactor.RedactMessage(message));

        var prior = PgSettingRedactor.MatchTimeoutForTest;
        PgSettingRedactor.MatchTimeoutForTest = new TimeSpan(1);
        try
        {
            Assert.Equal(W, PgLogTextRedactor.RedactDetail(detail));
            Assert.Equal(W, PgLogTextRedactor.RedactContext(context));
            Assert.Equal(W, PgLogTextRedactor.RedactMessage(message));
            Assert.Equal(PgLogTextRedactor.WithheldPlan, PgLogTextRedactor.WithholdPlan("duration: 1.000 ms  plan:\n\tSeq Scan"));
            Assert.True(PgLogTextRedactor.IsPlanHead("duration: 1.000 ms  plan:"));
        }
        finally
        {
            PgSettingRedactor.MatchTimeoutForTest = prior;
        }
    }

    [Fact]
    public void EveryPatternInBothRedactors_RunsUnderAMatchTimeout()
    {
        var regexes = new List<Regex>();
        foreach (var type in new[] { typeof(PgLogTextRedactor), typeof(PgSettingRedactor) })
        {
            foreach (var field in type.GetFields(BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic))
            {
                regexes.AddRange(RegexesIn(field.GetValue(null)));
            }
        }

        // The eleven frame and head patterns, the nineteen of the setting rules, and every compiled form.
        Assert.True(regexes.Count > 60, $"found {regexes.Count} patterns");
        foreach (var regex in regexes)
        {
            Assert.NotEqual(Regex.InfiniteMatchTimeout, regex.MatchTimeout);
            Assert.True(regex.MatchTimeout <= TimeSpan.FromSeconds(5), $"{regex.MatchTimeout}: {regex}");
        }
    }

    private static IEnumerable<Regex> RegexesIn(object? value)
    {
        switch (value)
        {
            case null or string:
                yield break;
            case Regex regex:
                yield return regex;
                yield break;
            case IEnumerable items:
                foreach (var item in items)
                {
                    foreach (var found in RegexesIn(item))
                    {
                        yield return found;
                    }
                }

                yield break;
        }

        var owner = value.GetType().DeclaringType;
        if (owner != typeof(PgLogTextRedactor) && owner != typeof(PgSettingRedactor))
        {
            yield break;
        }

        foreach (var field in value.GetType().GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic))
        {
            foreach (var found in RegexesIn(field.GetValue(value)))
            {
                yield return found;
            }
        }
    }
}
