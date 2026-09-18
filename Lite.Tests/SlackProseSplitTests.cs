/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Notifications;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3493: Slack caps one text object at 3,000 characters and one message at 50 blocks, and
/// <c>BuildSlackPayload</c> rendered the alert's whole prose detail as ONE mrkdwn section — so the
/// Collector Cost Digest failed delivery (HTTP 400 <c>invalid_attachments</c>) on exactly the stores busy
/// enough to want it, while a Fleet Sweep Rollup delivered through the SAME webhook 80 milliseconds later.
/// The prose now splits on line boundaries across as many sections as it needs, yields to the message's
/// block budget, and degrades by STATED omission — never a silent truncation, never a line cut mid-thought.
///
/// <para><b>The two hazards this suite is built around.</b> A splitter is trivially satisfied by mangling:
/// every "each text fits" pin also passes when lines are dropped or cut, so the arms here assert
/// RECONSTRUCTION — joining the split sections back together must reproduce the original prose exactly —
/// and the omission arms assert the note names both the count and the first dropped line's own text,
/// because an omission note that is itself vague recreates the silent-truncation problem one level up.
/// And a splitter that changed the SMALL case would repaint every ordinary alert's payload, so the
/// one-section shape is pinned byte-for-byte.</para>
///
/// <para><b>#3622: the cuts land on whole characters.</b> Both of this splitter's cuts — the hard split
/// and the omission line's quoted fragment — were sized in UTF-16 units, so an emoji astride the boundary
/// was cut in half. <c>JsonSerializer</c> relaxes the unpaired half to U+FFFD rather than emitting a lone
/// escape (measured in the lane; the issue expected Slack to reject the payload), so the payload delivered
/// and the reader saw a replacement glyph that was never in the text — the hard split showed TWO of them
/// and no emoji. The arms below place a surrogate pair and a combining sequence exactly astride each
/// boundary and assert three things: no text a reader was sent contains U+FFFD, the cut landed one
/// character earlier, and reassembly still reproduces the original. The pathological single-element
/// input (a run of combining marks wider than a section) pins that the loop still advances.</para>
/// </summary>
public class SlackProseSplitTests
{
    private static AlertBranding Branding => EmailAlertService.Branding;

    /* The builder-side geometry, restated here so a drifted constant fails a test rather than silently
       resizing every assertion: the first prose section leads with this header, and every section's line
       capacity is the text-object cap minus the header's width. */
    private const string Header = "*Details*\n";
    private const int Capacity = 3000 - 10;

    /* ---------------- harness ---------------- */

    private static string Payload(string prose, AlertContext? context = null, string? triageUrl = null) =>
        WebhookAlertService.BuildSlackPayload(
            "Collector Cost Digest", "Monitor Store", "20", "no threshold (report)", Branding,
            context: context, triageUrl: triageUrl, detailText: prose);

    private static List<JsonElement> Blocks(JsonDocument doc) =>
        doc.RootElement.GetProperty("attachments")[0].GetProperty("blocks").EnumerateArray().ToList();

    /// <summary>The contiguous run of prose sections: from the one leading with the <c>*Details*</c>
    /// header until the first block that is not a text-carrying section (a divider, actions, or the
    /// context footer). Contiguity is itself part of the contract — a split renders seamlessly only when
    /// nothing interleaves.</summary>
    private static List<string> ProseTexts(List<JsonElement> blocks)
    {
        var texts = new List<string>();
        foreach (var block in blocks)
        {
            string? text = null;
            if (block.GetProperty("type").GetString() == "section"
                && block.TryGetProperty("text", out var t)
                && t.GetProperty("type").GetString() == "mrkdwn")
            {
                text = t.GetProperty("text").GetString();
            }

            if (texts.Count == 0)
            {
                if (text is not null && text.StartsWith(Header, StringComparison.Ordinal))
                {
                    texts.Add(text);
                }

                continue;
            }

            if (text is null)
            {
                break;
            }

            texts.Add(text);
        }

        return texts;
    }

    /// <summary>Every string that Slack counts against the per-text-object cap, wherever it sits in the
    /// payload — section texts, field texts, the header title, context elements, button labels.</summary>
    private static IEnumerable<string> AllTextStrings(JsonElement element)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    if (prop.NameEquals("text") && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        yield return prop.Value.GetString()!;
                        continue;
                    }

                    foreach (var s in AllTextStrings(prop.Value))
                    {
                        yield return s;
                    }
                }

                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    foreach (var s in AllTextStrings(item))
                    {
                        yield return s;
                    }
                }

                break;
        }
    }

    /// <summary>Strips the first section's header and re-joins — the packing only ever removes the
    /// newline BETWEEN two sections, so joining with a newline must reproduce the prose exactly.</summary>
    private static string Reassemble(List<string> proseTexts) =>
        string.Join('\n', proseTexts.Select((t, i) => i == 0 ? t[Header.Length..] : t));

    private static string Line(int i, int length)
    {
        var head = string.Create(CultureInfo.InvariantCulture, $"line-{i:D4} ");
        return head + new string('x', length - head.Length);
    }

    private static string ManyLines(int count, int length) =>
        string.Join('\n', Enumerable.Range(0, count).Select(i => Line(i, length)));

    /* #3622 fixtures: one character each to a reader, two or four UTF-16 units to the index arithmetic. */
    private const string Fire = "\U0001F525";              // 🔥, a surrogate pair
    private const string EAcute = "e\u0301";               // e + combining acute, two code points
    private const string ThumbsUpMedium = "\U0001F44D\U0001F3FD"; // 👍🏽, two pairs in one grapheme

    /// <summary>Every mrkdwn text a reader was sent, parsed — an unpaired surrogate never survives into
    /// the raw JSON (the serializer relaxes it to \uFFFD), so the only place it can be caught is the
    /// decoded text.</summary>
    private static void AssertNoReplacementGlyph(JsonDocument doc) =>
        Assert.All(AllTextStrings(doc.RootElement), t => Assert.DoesNotContain('\uFFFD', t));

    /* ---------------- the shape that must not change ---------------- */

    /// <summary>
    /// The regression pin: a prose inside one section's capacity keeps the exact pre-#3493 rendering —
    /// ONE section, serialized byte-for-byte as before — because 1-mover digests and every ordinary
    /// alert live on this path, and a fix for the oversized case has no business repainting them.
    /// </summary>
    [Fact]
    public void ASmallProse_KeepsItsSingleSection_ByteForByte()
    {
        const string prose = "Store query_stats retention is HELD PAUSED by the rollup-coverage gate.\n"
            + "Run the --backfill-rollups operator action, then RESTART the service.";

        var payload = Payload(prose);

        /* The whole section object, serialized the same way the builder serializes it. */
        var expected = JsonSerializer.Serialize(
            new { type = "section", text = new { type = "mrkdwn", text = $"*Details*\n{prose}" } });
        Assert.Contains(expected, payload, StringComparison.Ordinal);

        using var doc = JsonDocument.Parse(payload);
        var texts = ProseTexts(Blocks(doc));
        Assert.Single(texts);
    }

    /// <summary>The true boundary is the capacity a section has left after its header, not a round
    /// 3,000: a prose of exactly that size still fits one section, whose text object lands exactly ON
    /// the cap.</summary>
    [Fact]
    public void ProseExactlyAtTheSingleSectionCapacity_StaysOneSection()
    {
        var prose = new string('x', Capacity);

        using var doc = JsonDocument.Parse(Payload(prose));
        var texts = ProseTexts(Blocks(doc));

        var text = Assert.Single(texts);
        Assert.Equal(3000, text.Length);
    }

    /* ---------------- the split ---------------- */

    /// <summary>
    /// The boundary arms the live failure sat on: a multi-line prose at and just past the text-object
    /// ceiling splits on a LINE boundary, every resulting text object fits the cap, and reassembling the
    /// sections reproduces the prose exactly — no line dropped, none cut.
    /// </summary>
    [Theory]
    [InlineData(3000)]
    [InlineData(3001)]
    public void ProseJustPastTheTextObjectCeiling_SplitsOnALineBoundary(int totalLength)
    {
        /* 29 lines of 99 chars joined by newlines is 2,899; the first line is sized to hit the target
           total exactly. */
        var first = Line(0, totalLength - 2900);
        var prose = first + '\n' + string.Join('\n', Enumerable.Range(1, 29).Select(i => Line(i, 99)));
        Assert.Equal(totalLength, prose.Length);

        using var doc = JsonDocument.Parse(Payload(prose));
        var texts = ProseTexts(Blocks(doc));

        Assert.True(texts.Count >= 2, "a prose past one section's capacity must split");
        Assert.All(texts, t => Assert.True(t.Length <= 3000, $"a text object is {t.Length} chars"));
        Assert.Equal(prose, Reassemble(texts));
    }

    /// <summary>A digest-scale prose splits into several sections and reassembles exactly — whole lines
    /// only, order preserved, nothing dropped. Reconstruction is the strongest available statement of
    /// "split, not mangled".</summary>
    [Fact]
    public void AMultiLineProse_SplitsIntoWholeLines_AndReassemblesExactly()
    {
        /* Varied line lengths, deterministic: 37..217 chars, the digest's real range. */
        var prose = string.Join('\n', Enumerable.Range(0, 80).Select(i => Line(i, 37 + (i * 53) % 181)));
        Assert.True(prose.Length > 3000);

        using var doc = JsonDocument.Parse(Payload(prose));
        var texts = ProseTexts(Blocks(doc));

        Assert.True(texts.Count >= 2);
        Assert.All(texts, t => Assert.True(t.Length <= 3000));
        Assert.Equal(prose, Reassemble(texts));
    }

    /// <summary>
    /// The pathological arm: a single LINE past the ceiling hard-splits at character boundaries, each
    /// continuation piece visibly marked — the alternative is failing the whole delivery over one line,
    /// which is this bug again. Removing the markers reassembles the original line exactly.
    /// </summary>
    [Fact]
    public void ASingleLinePastTheCeiling_HardSplitsWithAContinuationMarker()
    {
        const string marker = "(cont.) ";
        var line = string.Concat(Enumerable.Range(0, 700).Select(i =>
            string.Create(CultureInfo.InvariantCulture, $"{i:D4}-06789")));
        Assert.Equal(7000, line.Length);

        using var doc = JsonDocument.Parse(Payload(line));
        var texts = ProseTexts(Blocks(doc));

        Assert.True(texts.Count >= 3, "7,000 chars cannot fit two sections");
        Assert.All(texts, t => Assert.True(t.Length <= 3000));
        Assert.All(texts.Skip(1), t => Assert.StartsWith(marker, t, StringComparison.Ordinal));

        var reassembled = texts[0][Header.Length..]
            + string.Concat(texts.Skip(1).Select(t => t[marker.Length..]));
        Assert.Equal(line, reassembled);
    }

    /* ---------------- #3622: the cuts land on whole characters ---------------- */

    /// <summary>
    /// The helper every Slack cut goes through, on its own: the whole text when it fits; the limit when the
    /// limit is already a boundary; one unit earlier when the limit falls between the halves of a surrogate
    /// pair; before the base letter when it falls between a letter and its combining accent; before a
    /// four-unit emoji at every interior offset; and — the one case a whole-element cut cannot serve — a
    /// single element wider than the limit falls back to the code-point boundary, so a caller that must
    /// advance always can.
    /// </summary>
    [Theory]
    [InlineData("abc", 5, 3)]                       // fits: the whole text
    [InlineData("abcdef", 3, 3)]                    // ASCII: the limit is a boundary
    [InlineData("ab\U0001F525cd", 3, 2)]           // pair astride: one earlier
    [InlineData("ab\U0001F525cd", 4, 4)]           // pair inside: the limit
    [InlineData("abe\u0301cd", 3, 2)]              // letter + accent astride: before the letter
    [InlineData("ab\U0001F44D\U0001F3FDcd", 3, 2)] // four-unit emoji, cut after its first unit
    [InlineData("ab\U0001F44D\U0001F3FDcd", 4, 2)] // ...after its first pair
    [InlineData("ab\U0001F44D\U0001F3FDcd", 5, 2)] // ...after its third unit
    [InlineData("ab\U0001F44D\U0001F3FDcd", 6, 6)] // ...after the whole emoji: the limit
    [InlineData("\u0301\u0301\u0301\u0301", 2, 2)]  // one element wider than the limit: code point
    [InlineData("\U0001F525\u0301\u0301\u0301", 3, 3)] // ...the limit itself when it is a code-point boundary
    [InlineData("\U0001F44D\U0001F3FD\u0301\u0301", 3, 2)] // ...and one earlier when it is mid-pair
    [InlineData("abc", 0, 0)]
    public void SlackCutLength_LandsOnTheLastWholeCharacterInsideTheLimit(string text, int limit, int expected)
    {
        Assert.Equal(expected, WebhookAlertService.SlackCutLength(text, limit));
    }

    /// <summary>
    /// A character astride the hard split's boundary is carried whole into the continuation piece rather
    /// than cut in half. Before #3622 the first section ended with the pair's high half and the second
    /// began with its low half after the marker: two replacement glyphs, no emoji, and a reassembly that
    /// no longer matched the line. Each arm sizes its line so the boundary (the section's capacity, 2,990
    /// after the header) falls one unit into the character.
    /// </summary>
    [Theory]
    [InlineData(Fire)]
    [InlineData(EAcute)]
    [InlineData(ThumbsUpMedium)]
    public void ACharacterAstrideTheHardSplit_MovesWholeIntoTheContinuation(string character)
    {
        const string marker = "(cont.) ";
        var line = new string('x', Capacity - 1) + character + new string('y', 100);

        using var doc = JsonDocument.Parse(Payload(line));
        AssertNoReplacementGlyph(doc);
        var texts = ProseTexts(Blocks(doc));

        Assert.Equal(2, texts.Count);
        Assert.All(texts, t => Assert.True(t.Length <= 3000));
        Assert.Equal(Header + new string('x', Capacity - 1), texts[0]);
        Assert.StartsWith(marker + character, texts[1], StringComparison.Ordinal);

        var reassembled = texts[0][Header.Length..] + string.Concat(texts.Skip(1).Select(t => t[marker.Length..]));
        Assert.Equal(line, reassembled);
    }

    /// <summary>
    /// A single line that is ONE text element wider than a section — combining marks with no base — cannot
    /// be cut on an element boundary at all. The cut falls back to the code point and the loop advances:
    /// the line still splits, every piece fits, and the pieces reassemble to the line. Without the fallback
    /// this input hangs the splitter, which is why it is pinned before any other property of it.
    /// </summary>
    [Fact]
    public void ASingleElementWiderThanASection_StillHardSplits_AndReassembles()
    {
        const string marker = "(cont.) ";
        var line = new string('\u0301', 7000);

        using var doc = JsonDocument.Parse(Payload(line));
        var texts = ProseTexts(Blocks(doc));

        Assert.True(texts.Count >= 3);
        Assert.All(texts, t => Assert.True(t.Length <= 3000));
        var reassembled = texts[0][Header.Length..] + string.Concat(texts.Skip(1).Select(t => t[marker.Length..]));
        Assert.Equal(line, reassembled);
    }

    /// <summary>
    /// The omission line quotes the first dropped line's leading 120 characters; a character astride that
    /// boundary is left out of the quote rather than cut in half. Every line carries the character at
    /// units 119–120 (one unit into the 120 cut), so whichever line is the first dropped, the quote ends
    /// one character earlier and the payload carries no replacement glyph.
    /// </summary>
    [Theory]
    [InlineData(Fire)]
    [InlineData(EAcute)]
    public void ACharacterAstrideTheOmissionFragment_IsLeftOutOfTheQuote(string character)
    {
        string LineWith(int i) => Line(i, 119) + character + new string('z', 179);
        var prose = string.Join('\n', Enumerable.Range(0, 600).Select(LineWith));

        using var doc = JsonDocument.Parse(Payload(prose));
        AssertNoReplacementGlyph(doc);
        var texts = ProseTexts(Blocks(doc));
        var lines = Reassemble(texts).Split('\n');
        var emitted = lines.Length - 1;

        Assert.Contains(
            $"first omitted: \"{Line(emitted, 119)}...\"", lines[^1], StringComparison.Ordinal);
        Assert.DoesNotContain(character, lines[^1], StringComparison.Ordinal);
    }

    /* ---------------- the block budget and the stated omission ---------------- */

    /// <summary>
    /// A prose the 50-block budget genuinely cannot hold degrades by STATED omission: whole lines that
    /// fit, then one closing line naming how many lines were dropped AND quoting the first of them — the
    /// long-prose producers order their lines most-significant-first, so the first dropped line is the
    /// headline of what the reader is not seeing. The counts are computed from the payload rather than
    /// restated from the fixture, so the assertion is against what a reader was actually told.
    /// </summary>
    [Fact]
    public void AProseTheBlockBudgetCannotHold_EndsWithAStatedOmission()
    {
        const int total = 5000;
        var prose = ManyLines(total, 60);

        var payload = Payload(prose);
        using var doc = JsonDocument.Parse(payload);
        var blocks = Blocks(doc);

        Assert.Equal(50, blocks.Count);
        Assert.All(AllTextStrings(doc.RootElement), t => Assert.True(t.Length <= 3000));

        var texts = ProseTexts(blocks);
        var lines = Reassemble(texts).Split('\n');
        var emitted = lines.Length - 1;
        var omission = lines[^1];

        /* Every delivered content line is intact and in order... */
        for (var i = 0; i < emitted; i++)
        {
            Assert.Equal(Line(i, 60), lines[i]);
        }

        /* ...and the omission line names the dropped count, quotes the first dropped line, and points
           at the surface that has always carried the full text. */
        Assert.Contains(
            string.Create(CultureInfo.InvariantCulture, $"... and {total - emitted:N0} more lines"),
            omission, StringComparison.Ordinal);
        Assert.Contains(
            $"first omitted: \"{Line(emitted, 60)}\"", omission, StringComparison.Ordinal);
        Assert.Contains(
            "see email or in-app Alert Details for the full text", omission, StringComparison.Ordinal);
    }

    /// <summary>The omission line must itself fit the ceiling it exists to respect, so a long first
    /// dropped line is quoted by its leading stretch only — enough to identify a digest mover (collector,
    /// server and the headline figures all sit at the front), bounded so the quote cannot blow the
    /// note.</summary>
    [Fact]
    public void TheOmissionLine_TruncatesItsQuotedFragment_WhenTheFirstDroppedLineIsLong()
    {
        var prose = ManyLines(600, 300);

        using var doc = JsonDocument.Parse(Payload(prose));
        var texts = ProseTexts(Blocks(doc));
        var lines = Reassemble(texts).Split('\n');
        var emitted = lines.Length - 1;

        Assert.Contains(
            $"first omitted: \"{Line(emitted, 300)[..120]}...\"", lines[^1], StringComparison.Ordinal);
    }

    /// <summary>
    /// The budget counts EVERYTHING the payload carries — title, fields, per-incident dividers and
    /// sections, the actions block, the footer — and it is the PROSE that yields, because the real
    /// payload shapes cannot collide: the long-prose producers fire with no structured context, and the
    /// alerts with heavy details carry prose that is short or suppressed as redundant. Every detail
    /// survives whole; the prose states its omission; the message lands exactly on the cap.
    /// </summary>
    [Fact]
    public void TheBlockBudget_CountsThePerIncidentDetails_AndTheProseYields()
    {
        var context = new AlertContext();
        for (var i = 0; i < 10; i++)
        {
            context.Details.Add(new AlertDetailItem
            {
                Heading = string.Create(CultureInfo.InvariantCulture, $"Incident {i + 1} of 10"),
                Body = "Investigation: look at the graph.\n\nRemediation: stop looking at the graph."
            });
        }

        var payload = Payload(ManyLines(5000, 60), context: context, triageUrl: "https://example.invalid/triage");
        using var doc = JsonDocument.Parse(payload);
        var blocks = Blocks(doc);

        Assert.Equal(50, blocks.Count);
        Assert.All(AllTextStrings(doc.RootElement), t => Assert.True(t.Length <= 3000));

        for (var i = 0; i < 10; i++)
        {
            Assert.Contains(
                string.Create(CultureInfo.InvariantCulture, $"*Incident {i + 1} of 10*"),
                payload, StringComparison.Ordinal);
        }

        Assert.Contains("first omitted:", payload, StringComparison.Ordinal);
        Assert.Contains("Open triage page", payload, StringComparison.Ordinal);
    }
}
