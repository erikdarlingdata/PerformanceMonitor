/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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
    /// Set equality, so the two theories above cannot both keep passing while an EIGHTH band arrives with no
    /// decision made about it. Every band constant on the classifier is accounted for by exactly one of the
    /// two lists.
    /// </summary>
    [Fact]
    public void TheSetIsExactly_TheThreeBandsThatReadNothing()
    {
        Assert.Equal(
            new[]
            {
                CollectorHealthClassifier.NeverRun,
                CollectorHealthClassifier.NoPermissions,
                CollectorHealthClassifier.Stopped,
            }.OrderBy(b => b, StringComparer.Ordinal).ToArray(),
            CollectorHealthClassifier.NothingReadBands.OrderBy(b => b, StringComparer.Ordinal).ToArray());
    }

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
