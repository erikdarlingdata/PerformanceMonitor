/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Threading;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>The "Snapshot at &lt;time&gt;" label a newest-snapshot surface shows instead of a data-start note (#4966).</summary>
public sealed class ViewerSnapshotTimeLabelTests
{
    [Fact]
    public void TheLabel_IsTheDisplayZonesWallTime_ToTheSecond()
    {
        var zone = TimeZoneInfo.CreateCustomTimeZone("t", TimeSpan.FromHours(-5), "t", "t");
        var text = ViewerTimeHelper.FormatSnapshotLabel(new DateTime(2026, 3, 4, 15, 6, 7, DateTimeKind.Utc), zone);
        Assert.Equal("Snapshot at 2026-03-04 10:06:07", text);
    }

    [Fact]
    public void TheLabel_IsOnTheInvariantCulture_WhateverTheThreadsCalendar()
    {
        var saved = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = new CultureInfo("th-TH");
            var text = ViewerTimeHelper.FormatSnapshotLabel(new DateTime(2026, 3, 4, 15, 6, 7, DateTimeKind.Utc), TimeZoneInfo.Utc);
            Assert.Equal("Snapshot at 2026-03-04 15:06:07", text);
        }
        finally
        {
            CultureInfo.CurrentCulture = saved;
        }
    }
}
