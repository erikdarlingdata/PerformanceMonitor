/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the Job History tab's row-cap label (#4478): the read's 2,000-row cap used to show as a bare "2000",
/// which a reader could mistake for the total instead of a cap. The label states the cap ONLY when the
/// underlying read actually reached it — a reader who sees fewer rows never wonders whether more were dropped.
/// </summary>
public sealed class JobHistoryCapLabelTests
{
    [Fact]
    public void BelowCap_NoLabel()
    {
        Assert.Equal("", JobHistoryTab.CapLabel(1500, 2000));
    }

    [Fact]
    public void AtCap_LabelsTheNewestN()
    {
        Assert.Equal("showing the newest 2,000", JobHistoryTab.CapLabel(2000, 2000));
    }

    [Fact]
    public void AboveCap_StillLabels()
    {
        /* Not reachable from LoadJobsAsync's own read (the store never returns more than the cap it was asked
           for), but the label logic itself must not require an exact match \u2014 "at or over" is the right test. */
        Assert.Equal("showing the newest 2,000", JobHistoryTab.CapLabel(2001, 2000));
    }
}
