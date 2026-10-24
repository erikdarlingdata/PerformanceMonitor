/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3303: a custom rule's human name reaches the delivered <see cref="AlertOutcome"/> (so notification
/// titles show the name, not "Custom:&lt;id&gt;"), the detail_text separator is a plain " - " not an em dash,
/// and, the security half, the user-authored name/description are newline-stripped + length-capped before
/// they enter detail_text so a crafted name cannot forge a mute-context label line that the viewer's
/// mute-from-history pre-fill (<see cref="AlertMuteContext.PopulateFromDetailText"/>) would parse.
/// </summary>
public class CustomAlertDisplayNameTests
{
    /// <summary>A valid Scalar metric over a real catalog measure (mirrors CustomAlertRuleDefinitionTests).</summary>
    private const string DefinitionJson =
        "{\"metric\":{\"source\":\"wait_stats\",\"measure\":\"wait_time_ms\",\"aggregate\":\"sum\",\"hours\":1}," +
        "\"predicate\":{\"op\":\"ge\",\"warnThreshold\":1000}}";

    private static CustomAlertRuleDefinition Definition()
    {
        var (def, error) = CustomAlertRuleDefinition.TryParse(DefinitionJson);
        Assert.Null(error);
        Assert.NotNull(def);
        return def!;
    }

    private static CustomAlertRule Rule(long id, string name, string? description = null) =>
        new(id, name, DefinitionJson, description, Enabled: true, Version: 1,
            CreatedAt: DateTime.UtcNow, UpdatedAt: DateTime.UtcNow, UpdatedBy: null);

    // ─────────────────────────── SanitizeDisplayText ───────────────────────────

    [Fact]
    public void Sanitize_StripsEveryLineBreak_ToSingleLine()
    {
        var result = CustomAlertEvaluator.SanitizeDisplayText("a\nb\r\nc\rd\te", 200);

        Assert.DoesNotContain('\n', result);
        Assert.DoesNotContain('\r', result);
        Assert.DoesNotContain('\t', result);
        Assert.Equal("a b c d e", result);
    }

    [Fact]
    public void Sanitize_CollapsesWhitespace_AndTrims()
    {
        Assert.Equal("Hello World", CustomAlertEvaluator.SanitizeDisplayText("   Hello    World   ", 200));
    }

    [Fact]
    public void Sanitize_CapsLength()
    {
        var result = CustomAlertEvaluator.SanitizeDisplayText(new string('x', 500), 200);
        Assert.Equal(200, result.Length);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("\n\r\t")]
    public void Sanitize_NullOrAllWhitespaceOrControl_ReturnsEmpty(string? input)
    {
        Assert.Equal(string.Empty, CustomAlertEvaluator.SanitizeDisplayText(input, 200));
    }

    [Fact]
    public void Sanitize_LeavesOrdinaryNameUnchanged()
    {
        Assert.Equal("Signal wait % high", CustomAlertEvaluator.SanitizeDisplayText("Signal wait % high", 200));
    }

    /// <summary>#5654: bidirectional controls, zero-width characters and C1 controls are dropped, one class per case; U+0085
    /// stays a line break (a space), and ordinary non-ASCII text is kept.</summary>
    [Theory]
    [InlineData("a‮b", "ab")]       // right-to-left override
    [InlineData("a‪b‫c‬d‭e", "abcde")]
    [InlineData("a⁦b⁧c⁨d⁩e", "abcde")] // isolates
    [InlineData("a\u200Bb\u200Ec\u200Fd", "abcd")] // zero-width space and direction marks
    [InlineData("a\u061Cb\u2061c\u2064d\u206Fe\uFFF9f\uFFFBg\u180Eh\u00ADi", "abcdefghi")] // other format characters
    [InlineData("a\U000E0001b\U000E0041c\U000E007Fd", "abcd")] // tag characters, which are surrogate pairs
    [InlineData("\U000E0001\U000E0049\U000E0067\U000E006E\U000E007Fhello", "hello")] // a tag-character run hiding text
    [InlineData("\u0645\u06CC\u200C\u062E\u0648\u0627\u0647\u0645", "\u0645\u06CC\u200C\u062E\u0648\u0627\u0647\u0645")] // a Persian name keeps its zero-width non-joiner
    [InlineData("\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67", "\uD83D\uDC68\u200D\uD83D\uDC69\u200D\uD83D\uDC67")] // a joined emoji sequence is unchanged
    [InlineData("a⁠b﻿c", "abc")] // word joiner, byte-order mark
    [InlineData("a\u0080b\u009Fc\u0090d", "abcd")] // C1 controls
    [InlineData("a\u0085b", "a b")]      // NEL stays a break
    [InlineData("café 中文", "café 中文")]
    public void Sanitize_DropsReorderingAndInvisibleCharacters(string input, string expected)
    {
        Assert.Equal(expected, CustomAlertEvaluator.SanitizeDisplayText(input, 200));
    }

    [Fact]
    public void Sanitize_TheCap_NeverSplitsASurrogatePair()
    {
        const string face = "\U0001F600";

        /* Three code points of two UTF-16 units each: a cap of 5 holds two of them, and a cap of 1 holds none. */
        Assert.Equal(face + face, CustomAlertEvaluator.SanitizeDisplayText(face + face + face, 5));
        Assert.Equal(string.Empty, CustomAlertEvaluator.SanitizeDisplayText(face, 1));
        Assert.Equal("a", CustomAlertEvaluator.SanitizeDisplayText("a" + face, 2));
    }

    [Fact]
    public void Sanitize_AnInvisibleOnlyValue_IsEmpty()
    {
        Assert.Equal(string.Empty, CustomAlertEvaluator.SanitizeDisplayText("​‮﻿", 200));
    }

    // ─────────────────────── the mute-pre-fill spoof guard ───────────────────────

    /// <summary>
    /// Proves the attack the guard defends: an UNSANITIZED detail_text carrying newline-led label lines
    /// makes PopulateFromDetailText harvest attacker-chosen mute-context fields. (The seam test — if this
    /// ever stops extracting, the guarded test below would pass vacuously.)
    /// </summary>
    [Fact]
    public void RawNewlineBearingText_WouldSpoofTheMutePreFill()
    {
        var ctx = new AlertMuteContext();
        ctx.PopulateFromDetailText("Innocent\nDatabase: master\nWait Type: PAGEIOLATCH_SH\nQuery: DROP TABLE x");

        Assert.Equal("master", ctx.DatabaseName);
        Assert.Equal("PAGEIOLATCH_SH", ctx.WaitType);
        Assert.Equal("DROP TABLE x", ctx.QueryText);
    }

    /// <summary>
    /// The mandated test: a malicious newline-bearing rule name + description, once they flow through
    /// <see cref="CustomAlertEvaluator.BuildFireOutcome"/> into the persisted detail_text, extract NOTHING
    /// when the viewer re-parses that detail_text for the mute-from-history pre-fill.
    /// </summary>
    [Fact]
    public void BuildFireOutcome_NewlineBearingName_DefeatsMutePreFillSpoof()
    {
        var row = Rule(
            42,
            name: "Innocent\nDatabase: master\nWait Type: PAGEIOLATCH_SH",
            description: "looks fine\nJob Name: sa_nightly_backup\nQuery: DROP TABLE dbo.Orders");

        var outcome = CustomAlertEvaluator.BuildFireOutcome(
            row, Definition(), serverKey: "7", serverDisplayName: "PROD01",
            value: 1500, severity: AlertSeverityLevel.Warning, muted: false);

        // The detail_text (and display name) are single-line.
        Assert.DoesNotContain('\n', outcome.DetailText!);
        Assert.DoesNotContain('\r', outcome.DetailText!);
        Assert.DoesNotContain('\n', outcome.DisplayName!);

        // Re-parsing the persisted detail_text harvests no attacker-controlled mute-context field.
        var ctx = new AlertMuteContext();
        ctx.PopulateFromDetailText(outcome.DetailText);
        Assert.Null(ctx.DatabaseName);
        Assert.Null(ctx.WaitType);
        Assert.Null(ctx.JobName);
        Assert.Null(ctx.QueryText);
    }

    // ─────────────────────── display name + separator wiring ───────────────────────

    [Fact]
    public void BuildFireOutcome_SetsDisplayNameToRuleName_AndKeepsImmutableMetricKey()
    {
        var outcome = CustomAlertEvaluator.BuildFireOutcome(
            Rule(42, "Signal wait % too high"), Definition(),
            serverKey: "7", serverDisplayName: "PROD01",
            value: 1500, severity: AlertSeverityLevel.Warning, muted: false);

        Assert.Equal("Signal wait % too high", outcome.DisplayName);
        // The dedup/mute/history key stays the immutable id, never the name.
        Assert.Equal("Custom:42", outcome.MetricName);
    }

    [Fact]
    public void BuildFireOutcome_DetailText_UsesPlainSeparator_NotEmDash()
    {
        var outcome = CustomAlertEvaluator.BuildFireOutcome(
            Rule(42, "CPU pressure", description: "watch the schedulers"), Definition(),
            serverKey: "7", serverDisplayName: "PROD01",
            value: 1500, severity: AlertSeverityLevel.Warning, muted: false);

        Assert.Equal("CPU pressure - watch the schedulers", outcome.DetailText);
        Assert.DoesNotContain('\u2014', outcome.DetailText!); // em dash
        Assert.DoesNotContain('\u2013', outcome.DetailText!); // en dash
    }

    [Fact]
    public void BuildFireOutcome_NoDescription_DetailTextIsJustTheName()
    {
        var outcome = CustomAlertEvaluator.BuildFireOutcome(
            Rule(42, "CPU pressure"), Definition(),
            serverKey: "7", serverDisplayName: "PROD01",
            value: 1500, severity: AlertSeverityLevel.Warning, muted: false);

        Assert.Equal("CPU pressure", outcome.DetailText);
    }

    // ─────────────────── #3309: the single-line-label residual, closed at the mute pre-fill ───────────────────

    /// <summary>
    /// #3309: a rule literally NAMED with a leading mute-label ("Database: master") produces a ONE-line
    /// detail_text that does lead with "Database: " — the residual the slice-1 sanitizer (which only defeats
    /// MULTI-line injection) cannot close on its own. A custom alert has no database dimension, so the
    /// mute-from-history pre-fill now skips parsing entirely when the metric is "Custom:&lt;id&gt;", closing it.
    /// </summary>
    [Fact]
    public void PopulateFromDetailText_CustomMetric_SkipsParsing_SoASingleLineLabelNameCannotForgeAField()
    {
        var outcome = CustomAlertEvaluator.BuildFireOutcome(
            Rule(42, "Database: master"), Definition(),
            serverKey: "7", serverDisplayName: "PROD01",
            value: 1500, severity: AlertSeverityLevel.Warning, muted: false);

        var ctx = new AlertMuteContext();
        ctx.PopulateFromDetailText(outcome.DetailText, outcome.MetricName); // metric is "Custom:42"

        Assert.Null(ctx.DatabaseName); // skipped: a custom alert has no database dimension to pre-fill
        Assert.Null(ctx.WaitType);
        Assert.Null(ctx.JobName);
        Assert.Null(ctx.QueryText);
    }

    /// <summary>
    /// The guard is METRIC-GATED, not blanket: a built-in alert (any non-"Custom:" metric, or none supplied)
    /// still pre-fills from its structured detail_text exactly as before, so #3309 never regresses built-in muting.
    /// </summary>
    [Fact]
    public void PopulateFromDetailText_BuiltInMetric_StillParses()
    {
        var ctx = new AlertMuteContext();
        ctx.PopulateFromDetailText("Database: master", "High CPU");
        Assert.Equal("master", ctx.DatabaseName);

        // The legacy single-argument call (no metric) is unchanged.
        var legacy = new AlertMuteContext();
        legacy.PopulateFromDetailText("Database: master");
        Assert.Equal("master", legacy.DatabaseName);
    }
}
