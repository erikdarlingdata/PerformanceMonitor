/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Common;
using PerformanceMonitorLite;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2444 — one badly-shaped value used to cost every setting after it.
///
/// <para>#2425 split the DOCUMENT fault out of <c>App.LoadAlertSettings</c>'s single <c>try</c>, so a
/// trailing comma is now reported rather than silently reverting everything. What it left behind was the
/// VALUE fault: a key holding a string where an int belongs threw on its own <c>Get*</c> call and abandoned
/// every read after it, so which settings survived depended on where the bad key happened to sit in the
/// file. One wrong value near the top cost almost everything; the same value near the bottom cost almost
/// nothing. That ordering is an implementation detail of the loader's line order, and it was invisible.</para>
///
/// <para>The position tests below are the ones that matter, and they are position tests on purpose: a
/// fixture with a single bad key and nothing after it passes against the OLD code too. Only a bad key with
/// good keys on BOTH sides can tell "this key fell back" from "this key and the rest of the file fell
/// back".</para>
///
/// <para>Shares the <c>app-alert-statics</c> collection with the other classes that drive
/// <c>App.LoadAlertSettings</c>: it rewrites the whole alert block, and xUnit runs classes in parallel.</para>
/// </summary>
[Collection("app-alert-statics")]
public class SettingsValueReadTests
{
    private static void FailOnWrite(string key, string value) =>
        Assert.Fail($"LoadAlertSettings wrote credential '{key}' when nothing should have been saved.");

    private static string WriteSettings(string tag, string json)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pmlite_{tag}_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        File.WriteAllText(Path.Combine(dir, "settings.json"), json);
        return dir;
    }

    private static SettingsReader ReaderOver(string json) =>
        new(JsonDocument.Parse(json).RootElement);

    /* ---- the defect, end to end through the real loader ---- */

    /// <summary>
    /// The headline. A quoted number sits between two perfectly good settings; the good ones both apply and
    /// only the bad key falls back. Against dev the LAST key is never reached at all, which is the whole of
    /// #2444 in one assertion.
    /// </summary>
    [Fact]
    public void ABadValue_CostsItsOwnKeyAndNothingAfterIt()
    {
        var dir = WriteSettings("badvalue",
            """
            {
              "alerts_enabled": true,
              "alert_cpu_threshold": "ninety",
              "analysis_timeout_seconds": 123
            }
            """);

        try
        {
            App.AlertsEnabled = false;
            App.AlertCpuThreshold = 42;
            App.AnalysisTimeoutSeconds = 99;

            App.LoadAlertSettings(dir, key => "stored:" + key, FailOnWrite);

            /* Before the bad key: always worked, still works. */
            Assert.True(App.AlertsEnabled);

            /* The bad key itself: keeps the value it had, which is the only correct answer. */
            Assert.Equal(42, App.AlertCpuThreshold);

            /* After the bad key: THIS is the regression. It was unreachable on dev. */
            Assert.Equal(123, App.AnalysisTimeoutSeconds);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// The same shape one level down, and a second throw site the old single <c>try</c> hid: an array whose
    /// ELEMENT is the wrong kind. <c>elem.GetString()</c> throws on the number, and on dev that one element
    /// cost every setting in the rest of the file.
    /// </summary>
    [Fact]
    public void AWrongShapedArrayElement_CostsNeitherTheListNorTheRestOfTheFile()
    {
        var dir = WriteSettings("badelement",
            """
            {
              "alert_excluded_databases": ["keep_me", 5, "keep_me_too"],
              "analysis_timeout_seconds": 321
            }
            """);

        try
        {
            App.AnalysisTimeoutSeconds = 99;

            App.LoadAlertSettings(dir, key => "stored:" + key, FailOnWrite);

            Assert.Equal(new[] { "keep_me", "keep_me_too" }, App.AlertExcludedDatabases);
            Assert.Equal(321, App.AnalysisTimeoutSeconds);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /* ---- the reader's own policy ---- */

    /// <summary>
    /// Reports the whole set, not the first. A user who hand-edited one line has one mistake; a user who
    /// pasted a block out of settings.sample.json has several, and stopping at the first means they fix,
    /// restart, and discover the next one — several times over.
    /// </summary>
    [Fact]
    public void EveryBadKeyIsReported_NotOnlyTheFirst()
    {
        var read = ReaderOver(
            """
            { "a": "no", "b": 1, "c": true, "d": [1], "e": null }
            """);

        /* Every one of the five is a different wrong shape, and every one hands back the caller's value. */
        Assert.True(read.TryGetProperty("a", out var a));
        Assert.True(a.Bool(fallback: true));

        Assert.True(read.TryGetProperty("b", out var b));
        Assert.Equal("kept", b.Text("kept"));

        Assert.True(read.TryGetProperty("c", out var c));
        Assert.Equal(5, c.Int(5));

        Assert.True(read.TryGetProperty("d", out var d));
        Assert.Equal(2.5, d.Double(2.5, 0, 10));

        Assert.True(read.TryGetProperty("e", out var e));
        Assert.Equal("kept", e.Text("kept"));

        Assert.Equal(new[] { "a", "b", "c", "d", "e" }, read.Problems.Select(p => p.Key).ToArray());

        /* The message names the kind that WAS there, which is what lets someone find the line. */
        Assert.Contains("string", read.Problems[0].Problem, StringComparison.Ordinal);
        Assert.Contains("true or false", read.Problems[0].Problem, StringComparison.Ordinal);
        Assert.Contains("null", read.Problems[4].Problem, StringComparison.Ordinal);
    }

    /// <summary>An absent key is how every default works and how an older settings.json behaves. It is never
    /// a problem, and reporting it would make the dialog fire on nearly every install.</summary>
    [Fact]
    public void AnAbsentKeyIsNotAProblem()
    {
        var read = ReaderOver("""{ "present": 1 }""");

        Assert.False(read.TryGetProperty("absent", out _));
        Assert.True(read.TryGetProperty("present", out var present));
        Assert.Equal(1, present.Int(0));
        Assert.Empty(read.Problems);
    }

    /// <summary>
    /// The latent bug the clamps' move made unreachable. The old inline form was
    /// <c>(int)Math.Max(0, v.GetInt64())</c>, which floors and then NARROWS — so a hand-typed value beyond
    /// int range wrapped, and a "bigger threshold" became a negative one. The reader clamps before it
    /// narrows, so the worst a huge number can do is land on the bound. This is not a shape fault, so it is
    /// deliberately NOT reported: the value was readable, it was just out of range.
    /// </summary>
    [Fact]
    public void AnOutOfRangeNumberClampsToTheBound_RatherThanWrappingNegative()
    {
        var read = ReaderOver("""{ "huge": 5000000000, "negative": -7 }""");

        Assert.True(read.TryGetProperty("huge", out var huge));
        Assert.Equal(int.MaxValue, huge.Int(1, 0, int.MaxValue));

        Assert.True(read.TryGetProperty("negative", out var negative));
        Assert.Equal(0, negative.Int(1, 0, 100));

        Assert.Empty(read.Problems);
    }

    /// <summary>
    /// Review found the boundary the clamp promise stopped at (#2453): the clamped readers go through
    /// <c>TryGetInt64</c>, so a number beyond Int64 fell out of the clamp and was reported as though it were
    /// not a whole number — a nonsense sentence about a number, and in a dialog. It clamps by SIGN instead,
    /// which makes the promise absolute rather than "up to Int64".
    /// </summary>
    [Fact]
    public void ANumberBeyondInt64_StillClampsToABound()
    {
        var read = ReaderOver("""{ "vast": 99999999999999999999, "vast_negative": -99999999999999999999 }""");

        Assert.True(read.TryGetProperty("vast", out var vast));
        Assert.Equal(100, vast.Int(1, 0, 100));

        Assert.True(read.TryGetProperty("vast_negative", out var negative));
        Assert.Equal(0, negative.Int(1, 0, 100));

        Assert.Empty(read.Problems);
    }

    /// <summary>
    /// The regression the FIRST cut of that fix introduced, and the sharper of the two review findings.
    /// <c>TryGetInt64</c> returns false for a number that is not a pure integer token as well as for one that
    /// is too big, so choosing the bound by SIGN turned <c>analysis_timeout_seconds: 30.0</c> into 600 — a
    /// value the user never asked for, never saw flagged, and could not have found. Reading the remainder as
    /// a double is what tells the two apart, and 30.0 is plainly 30.
    /// </summary>
    [Fact]
    public void AFractionalNumber_ReadsAsItsValue_RatherThanLandingOnTheMaximum()
    {
        var read = ReaderOver("""{ "whole": 30.0, "fraction": 5.5, "under": -0.5 }""");

        Assert.True(read.TryGetProperty("whole", out var whole));
        Assert.Equal(30, whole.Int(99, 30, 600));

        /* Truncated INTO the range, which is the same silent adjustment the clamp already is and is confined
           to the readers whose caller declared a range for exactly that. It is not 100. */
        Assert.True(read.TryGetProperty("fraction", out var fraction));
        Assert.Equal(5, fraction.Int(99, 0, 100));

        Assert.True(read.TryGetProperty("under", out var under));
        Assert.Equal(0, under.Int(99, 0, 100));

        Assert.Empty(read.Problems);
    }

    /// <summary>
    /// The other side of the boundary. An UNCLAMPED read has no range to put an unusable number into, so it
    /// must not invent one — and it names which of the two problems the value has, because "holds a JSON
    /// number where a whole number belongs" is a nonsense sentence about a number. A number that IS exact is
    /// still taken however it was written.
    /// </summary>
    [Fact]
    public void AnUnclampedRead_TakesAnExactNumber_AndNamesWhyItRejectsTheRest()
    {
        var exact = ReaderOver("""{ "n": 30.0 }""");
        Assert.True(exact.TryGetProperty("n", out var n));
        Assert.Equal(30, n.Int(7));
        Assert.Empty(exact.Problems);

        var fractional = ReaderOver("""{ "n": 90.7 }""");
        Assert.True(fractional.TryGetProperty("n", out var f));
        Assert.Equal(7, f.Int(7));
        Assert.Contains("not a whole number", Assert.Single(fractional.Problems).Problem, StringComparison.Ordinal);

        var vast = ReaderOver("""{ "n": 99999999999999999999 }""");
        Assert.True(vast.TryGetProperty("n", out var v));
        Assert.Equal(7, v.Int(7));

        var problem = Assert.Single(vast.Problems);
        Assert.Contains("out of range", problem.Problem, StringComparison.Ordinal);
        Assert.DoesNotContain("where a whole number belongs", problem.Problem, StringComparison.Ordinal);
    }

    /// <summary>
    /// The line between "wrong shape" and "wrong word", which decides what the startup dialog is allowed to
    /// complain about. A number where a theme name belongs is this file's business; a string that is simply
    /// not one of the three theme names is the caller's own vocabulary, has always been ignored quietly, and
    /// widening THAT into a dialog is a separate decision from this one.
    /// </summary>
    [Fact]
    public void OnlyTheShapeIsReported_NotAnUnrecognisedButWellShapedString()
    {
        var wrongShape = ReaderOver("""{ "color_theme": 5 }""");
        Assert.True(wrongShape.TryGetProperty("color_theme", out var number));
        Assert.Null(number.TextOrNull());
        Assert.Equal("color_theme", Assert.Single(wrongShape.Problems).Key);

        var wrongWord = ReaderOver("""{ "color_theme": "Chartreuse" }""");
        Assert.True(wrongWord.TryGetProperty("color_theme", out var text));
        Assert.Equal("Chartreuse", text.TextOrNull());
        Assert.Empty(wrongWord.Problems);
    }

    /// <summary>A value read twice cannot be reported twice — the dialog lists keys, and a key listed
    /// twice reads as two different problems with the same name.</summary>
    [Fact]
    public void OneKeyIsReportedOnce()
    {
        var read = ReaderOver("""{ "n": "not a number" }""");

        Assert.True(read.TryGetProperty("n", out var n));
        Assert.Equal(1, n.Int(1));
        Assert.Equal(2, n.Int(2, 0, 10));

        Assert.Equal("n", Assert.Single(read.Problems).Key);
    }
}
