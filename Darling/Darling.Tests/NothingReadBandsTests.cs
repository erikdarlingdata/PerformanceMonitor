/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The named set that says which bands mean NOTHING WAS READ (#3017), pinned in BOTH directions.
///
/// <para><b>Why both directions.</b> The failure mode of a coverage denominator is over-exclusion as much as
/// under-exclusion: dropping FAILING, STALE or WARNING from what counts as read would shrink the denominator
/// and report a smaller fleet than exists — a new wrong number in place of the old one, and one that looks
/// more careful than the number it replaced. So the membership test sits beside a NON-membership test, and
/// the two together are a set-equality assertion that a fourth band cannot slip past in either direction.</para>
/// </summary>
public sealed class NothingReadBandsTests
{
    /// <summary>
    /// STOPPED and NO_PERMISSIONS are the two the field produces. NEVER_RUN is in the set on MEANING rather
    /// than on reachability: it is <c>totalRuns == 0</c>, which a GROUP BY over a run log cannot currently
    /// produce (no rows, no group), but that is a property of the QUERY and not of the band. A later outer
    /// join against the collector catalog — the natural way to make a never-invoked collector visible at all
    /// — makes it reachable, and a set that had left it out would then count the most completely unread
    /// server of all as read. Relying on a row shape to keep a band unreachable is how the next query change
    /// reintroduces the defect.
    /// </summary>
    [Theory]
    [InlineData(CollectorHealthClassifier.NoPermissions)]
    [InlineData(CollectorHealthClassifier.Stopped)]
    [InlineData(CollectorHealthClassifier.NeverRun)]
    public void ABandThatReadNothing_IsInTheSet(string band)
    {
        Assert.Contains(band, CollectorHealthClassifier.NothingReadBands);
        Assert.True(CollectorHealthClassifier.ReadNothing(band));
    }

    /// <summary>
    /// The other direction, and the one that keeps a denominator honest. A FAILING, STALE or WARNING
    /// collector DID read on some cycles — its rows really are in whatever total is built from them — so
    /// excluding it would understate coverage. Those states are not hidden by counting: they have their own
    /// loud surfaces (the health grid's band, the fleet's failing-collector counts).
    /// </summary>
    [Theory]
    [InlineData(CollectorHealthClassifier.Failing)]
    [InlineData(CollectorHealthClassifier.Stale)]
    [InlineData(CollectorHealthClassifier.Warning)]
    [InlineData(CollectorHealthClassifier.Healthy)]
    public void ABandThatDidRead_IsNotInTheSet(string band)
    {
        Assert.DoesNotContain(band, CollectorHealthClassifier.NothingReadBands);
        Assert.False(CollectorHealthClassifier.ReadNothing(band));
    }

    /// <summary>
    /// Set equality, so the two theories above cannot both keep passing while the set itself is widened or
    /// narrowed without a decision reaching them.
    /// </summary>
    [Fact]
    public void TheSetIsExactly_TheThreeBandsThatReadNothing()
    {
        Assert.Equal(
            ReadNothing.OrderBy(b => b, StringComparer.Ordinal).ToArray(),
            CollectorHealthClassifier.NothingReadBands.OrderBy(b => b, StringComparer.Ordinal).ToArray());
    }

    /// <summary>
    /// And the direction the set-equality test above cannot reach on its own: an EIGHTH band added to the
    /// classifier and to NEITHER list.
    ///
    /// <para>A test that compared the set against a hand-written triple would keep passing through that
    /// change, because nothing in it ever asks the classifier what bands EXIST — a decision would simply
    /// never be made, and the new band would count as read by default. So the band constants are read off
    /// the type and every one is required to sit in exactly one of the two lists.</para>
    ///
    /// <para>The band constants are told apart from the classifier's other <c>public const string</c>
    /// members (<c>EmptyEnumerationMarker</c>, <c>HasUserDatabasesQualifier</c> — lower-case prose) by their
    /// VALUE shape rather than by a name list, so a new band is picked up without this test being edited. A
    /// non-band constant that happened to be upper-case would be swept in and demand a decision it does not
    /// need, which is the safe direction: it fails loudly and gets one line of triage, where the miss it
    /// replaces is silent and wrong.</para>
    /// </summary>
    [Fact]
    public void EveryBandOnTheClassifier_HasADecision_InExactlyOneList()
    {
        var bands = typeof(CollectorHealthClassifier)
            .GetFields(BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy)
            .Where(f => f is { IsLiteral: true, IsInitOnly: false } && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Where(v => v.Length > 0 && v.All(ch => char.IsAsciiLetterUpper(ch) || ch == '_'))
            .ToList();

        /* The precondition, named so a reflection change reports itself instead of turning this into a
           vacuous pass over an empty sequence. */
        Assert.Equal(7, bands.Count);

        var decided = ReadNothing.Concat(DidRead).ToHashSet(StringComparer.Ordinal);

        Assert.Equal(
            bands.OrderBy(b => b, StringComparer.Ordinal).ToArray(),
            decided.OrderBy(b => b, StringComparer.Ordinal).ToArray());

        /* No band in both lists — a band that read nothing and also read is not a decision. */
        Assert.Empty(ReadNothing.Intersect(DidRead, StringComparer.Ordinal));
    }

    /* The two decisions, as data, so both tests above read from the same lists the theories enumerate. */
    private static readonly string[] ReadNothing =
    [
        CollectorHealthClassifier.NeverRun,
        CollectorHealthClassifier.NoPermissions,
        CollectorHealthClassifier.Stopped,
    ];

    private static readonly string[] DidRead =
    [
        CollectorHealthClassifier.Failing,
        CollectorHealthClassifier.Stale,
        CollectorHealthClassifier.Warning,
        CollectorHealthClassifier.Healthy,
    ];

    /// <summary>
    /// A null band answers FALSE, not TRUE. Absence of a band is not a claim that nothing was read, and a
    /// caller holding no band at all has to decide that case for itself — <c>DarlingFleetReader</c>'s
    /// deadlock coverage does, and treats it as uncovered, but it does so where a reader can see the
    /// reasoning rather than having this predicate decide it silently.
    /// </summary>
    [Fact]
    public void ANullOrUnknownBand_IsNotAClaimThatNothingWasRead()
    {
        Assert.False(CollectorHealthClassifier.ReadNothing(null));
        Assert.False(CollectorHealthClassifier.ReadNothing("SOMETHING_A_LATER_BUILD_WROTE"));
    }
}
