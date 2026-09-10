/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The cross-edition half of the findings-retention pin. Lite's own half lives in
/// <c>Lite.Tests.FindingsRetentionHorizonPinTests</c>, which pins the value and the wiring of the three
/// sites in its cleanup chain; this pins the claim the two editions make ABOUT EACH OTHER.
///
/// <para>Both SKUs purge <c>analysis_findings</c> on the same window on purpose — Lite's scheduler
/// comment says so in as many words, and the service's daily purge rides it. That was prose either side
/// of an assembly boundary, which is the weakest form the claim can take: two <c>const</c>s that happen
/// to read 30 today and no build step that notices when one moves.</para>
///
/// <para>This is the project that can see both symbols. <c>Darling.Tests</c> references
/// <c>PerformanceMonitor.Common</c> directly and is named in its <c>InternalsVisibleTo</c>, and it sees
/// <see cref="DarlingRetention"/>'s internals through the service's. <c>Lite.Tests</c> cannot reach the
/// service at all, so a compile-time assertion is only expressible here — the alternative was scraping
/// another edition's source text for a number, which drifts on a reformat.</para>
///
/// <para>If this goes red, nothing is broken yet: one edition's horizon has moved and the other's has
/// not. Move both, or split them deliberately and delete this pin with a reason — do not "fix" it by
/// copying whichever value happens to be under the cursor.</para>
/// </summary>
public sealed class FindingsRetentionCrossSkuPinTests
{
    [Fact]
    public void TheEditionsAgreeOnTheFindingsRetentionHorizon()
    {
        Assert.Equal(
            DarlingRetention.DataRetentionBaseDays,
            AnalysisRetentionDefaults.FindingsRetentionDays);
    }
}
