/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2814 (follow-on to #2781): the Darling web is headless (no system tray), so a <c>"tray"</c> alert -
/// Lite's delivered-without-email taxonomy - was never delivered on this surface and must read a single
/// neutral "Logged", NOT a Sent/Not-sent derived from <c>alert_sent</c>. No JS runner in this repo, so this
/// scans the frontend source the way the composer drift guards do.
///
/// <para>#3169 fixed the cause rather than this surface's reading of it. <c>alert_sent</c> no longer means
/// "no channel applies" on a resolution row, and <c>notification_type</c> now NAMES the state, so a headless
/// store records <c>unconfigured</c> / <c>none</c> where it used to borrow Lite's <c>tray</c>. Those values
/// reach these pages, and with no handling they would fall straight through to the very <c>alert_sent</c>
/// collapse #2814 removed - so the guard grew rather than being deleted.</para>
///
/// <para><b>What changed about the guard itself.</b> It used to scan for a
/// <c>notification_type === "tray"</c> branch, for the string "Logged" somewhere in the file, and for
/// <c>IndexOf("alert_sent")</c> preceding the branch. All three were satisfiable without the behaviour
/// holding: any mention of <c>alert_sent</c> anywhere anchored the ordering check, and a page pairing every
/// label with the wrong state would still have contained both strings. The mapping now lives in ONE
/// parseable declaration - <c>ALERT_STATE_LABELS</c> in <c>util.js</c>, which both pages read through
/// <c>alertDeliveryState</c> - and this file parses that object and compares it to
/// <see cref="AlertDeliveryStatus"/>'s constants key by key. A cross-language parity pin on the seam rather
/// than a text search, which is the shape the repo's other cross-surface guards use.</para>
/// </summary>
public class AlertTrayStatusLoggedTests
{
    /// <summary>
    /// The label each state-carrying channel owes an operator. The KEYS are not listed here - they come from
    /// <see cref="AlertDelivery.StateCarryingChannels"/>, so adding a state to the C# taxonomy without
    /// teaching the web dashboard about it fails
    /// <see cref="EveryChannelConstant_IsClassified_AndEveryStateHasALabel"/> rather than silently shipping
    /// a third vocabulary.
    /// </summary>
    private static readonly Dictionary<string, string> ExpectedStateLabels = new(StringComparer.Ordinal)
    {
        [AlertDelivery.ChannelNotApplicable] = AlertDeliveryStatus.NoChannel,
        [AlertDelivery.ChannelNoneConfigured] = AlertDeliveryStatus.NoChannelConfigured,
        [AlertDelivery.ChannelMuted] = AlertDeliveryStatus.Muted,
        [AlertDelivery.ChannelUndelivered] = AlertDeliveryStatus.NotSent,
        [AlertDelivery.ChannelTray] = AlertDeliveryStatus.Logged,
    };

    /// <summary>
    /// Every <c>Channel*</c> constant on <see cref="AlertDelivery"/> is classified as either state-carrying
    /// or delivering, the two lists are a partition, and this file has a label for every state-carrying one.
    ///
    /// <para>This is the reach property. Without it a new channel constant would be invisible to every check
    /// below - absent from both lists, absent from the web dashboard's map, and rendered by whatever
    /// fallback happened to catch it. Reflection over the constants is what makes "did anyone classify
    /// this?" answerable, instead of trusting that whoever added it also came here.</para>
    /// </summary>
    [Fact]
    public void EveryChannelConstant_IsClassified_AndEveryStateHasALabel()
    {
        var constants = typeof(AlertDelivery)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f.IsLiteral && !f.IsInitOnly && f.FieldType == typeof(string))
            .Where(f => f.Name.StartsWith("Channel", StringComparison.Ordinal))
            .Select(f => (f.Name, Value: (string)f.GetRawConstantValue()!))
            .ToList();

        Assert.NotEmpty(constants);

        var classified = AlertDelivery.StateCarryingChannels
            .Concat(AlertDelivery.DeliveringChannels)
            .ToHashSet(StringComparer.Ordinal);

        Assert.Empty(constants.Where(c => !classified.Contains(c.Value)).Select(c => c.Name));

        /* The two lists are a partition, not two overlapping opinions. */
        Assert.Empty(AlertDelivery.StateCarryingChannels
            .Intersect(AlertDelivery.DeliveringChannels, StringComparer.Ordinal));

        Assert.Equal(
            AlertDelivery.StateCarryingChannels.OrderBy(c => c, StringComparer.Ordinal).ToArray(),
            ExpectedStateLabels.Keys.OrderBy(c => c, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// The web dashboard's <c>ALERT_STATE_LABELS</c> is exactly the shared taxonomy, key for key and label
    /// for label. Three surfaces render this column and only two of them can share code, so this is the
    /// join for the third.
    /// </summary>
    [Fact]
    public void TheWebDashboardsStateLabels_MatchTheSharedConstants()
    {
        var actual = ParseStateLabels(FrontendSource("util.js"));

        Assert.Equal(
            ExpectedStateLabels.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToArray(),
            actual.OrderBy(kv => kv.Key, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// Both pages route through the shared lookup, and they do it BEFORE their own <c>alert_sent</c>
    /// collapse - otherwise a state-carrying row falls into the Sent/Not-sent reading #2814 removed and
    /// #3169 made avoidable.
    ///
    /// <para>The collapse is located by its full branch text, <c>(a.alert_sent)</c>, rather than by the
    /// first mention of <c>alert_sent</c> anywhere. The predecessor pin used the latter, and a composite
    /// condition reading <c>a.alert_sent &amp;&amp; a.notification_type === "tray"</c> would have anchored
    /// it - reporting the collapse as coming first and passing on a page that had lost every state
    /// branch.</para>
    /// </summary>
    [Theory]
    [InlineData("pages/alerts.js")]
    [InlineData("pages/triage.js")]
    public void BothPages_ConsultTheSharedLookup_BeforeTheAlertSentCollapse(string relPath)
    {
        var src = FrontendSource(relPath);

        var lookup = src.IndexOf("alertDeliveryState(a)", StringComparison.Ordinal);
        var collapse = src.IndexOf("(a.alert_sent)", StringComparison.Ordinal);

        Assert.True(lookup >= 0, $"{relPath}: the shared lookup is never called");
        Assert.True(collapse >= 0, $"{relPath}: the bare alert_sent collapse was not found - was it renamed?");
        Assert.True(
            lookup < collapse,
            $"{relPath}: the state lookup must precede the alert_sent collapse, or the row falls into it");
    }

    /// <summary>
    /// The one legacy signature that decodes lives in the shared lookup, once: <c>alert_sent</c> true
    /// alongside <c>tray</c> is unreachable for a row written after #3169 - a row that delivered names its
    /// channel - so it can only be the resolution builder's old hardcoded true, which meant "no send channel
    /// applies" and never meant a delivery.
    /// </summary>
    [Fact]
    public void TheLegacyResolutionSignature_IsDecodedOnceInTheSharedLookup()
    {
        Assert.Matches(
            new Regex(@"a\.alert_sent\s*&&\s*a\.notification_type\s*===\s*""tray"""),
            FrontendSource("util.js"));

        /* And neither page keeps a second copy of it, which is what "in lockstep" used to mean here. */
        foreach (var relPath in new[] { "pages/alerts.js", "pages/triage.js" })
        {
            Assert.DoesNotContain("=== \"tray\"", FrontendSource(relPath), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The channel chip names a real channel only - "No channel configured (unconfigured)" is the shape this
    /// prevents - and it derives its exclusions from the shared map rather than listing them again.
    /// </summary>
    [Fact]
    public void TheChannelChip_NamesARealChannelOnly()
    {
        var src = FrontendSource("pages/alerts.js");

        Assert.Contains("new Set(Object.keys(ALERT_STATE_LABELS))", src, StringComparison.Ordinal);
        Assert.Contains("!STATE_ONLY_CHANNELS.has(a.notification_type)", src, StringComparison.Ordinal);
    }

    /// <summary>
    /// Parses the <c>ALERT_STATE_LABELS</c> object literal out of <c>util.js</c>. Deliberately strict: every
    /// line inside the literal must be a flat <c>key: "value",</c> pair, so a form this cannot read fails
    /// loudly instead of yielding a short dictionary that would make
    /// <see cref="TheWebDashboardsStateLabels_MatchTheSharedConstants"/> pass by comparing less.
    /// </summary>
    private static Dictionary<string, string> ParseStateLabels(string util)
    {
        var open = util.IndexOf("export const ALERT_STATE_LABELS = {", StringComparison.Ordinal);
        Assert.True(open >= 0, "util.js: ALERT_STATE_LABELS was not found");

        var close = util.IndexOf("};", open, StringComparison.Ordinal);
        Assert.True(close > open, "util.js: ALERT_STATE_LABELS is not a closed object literal");

        var body = util.Substring(open, close - open);
        body = body.Substring(body.IndexOf('{') + 1);

        var pairs = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var rawLine in body.Split('\n'))
        {
            var line = rawLine.Trim().TrimEnd('\r');
            if (line.Length == 0)
            {
                continue;
            }

            var match = Regex.Match(line, @"^""?(?<key>[A-Za-z0-9_+]+)""?\s*:\s*""(?<label>[^""]*)"",$");
            Assert.True(
                match.Success,
                "util.js: ALERT_STATE_LABELS carries a line this parser cannot read, so the parity check "
                + $"would silently compare fewer keys: {line}");

            pairs[match.Groups["key"].Value] = match.Groups["label"].Value;
        }

        Assert.NotEmpty(pairs);
        return pairs;
    }

    private static string FrontendSource(string relPath, [CallerFilePath] string thisFile = "")
    {
        var path = Path.GetFullPath(Path.Combine(
            Path.GetDirectoryName(thisFile)!, "..", "PerformanceMonitor.Darling.Service", "wwwroot", "js", relPath));
        Assert.True(File.Exists(path), $"{relPath} not found at {path} (did the frontend move?)");
        return File.ReadAllText(path);
    }
}
