/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>#7160: the Viewer's Dismiss All sends the window write only while the grid shows the whole window.</summary>
public sealed class AlertDismissScopeTests
{
    [Theory]
    [InlineData(false, 0, 500, false)]
    [InlineData(false, 499, 500, false)]
    [InlineData(false, 500, 500, true)]
    [InlineData(true, 3, 500, true)]
    [InlineData(true, 500, 500, true)]
    public void TheWindowWriteIsUsedOnlyWhenNothingNarrowsOrCapsTheGrid(bool filterActive, int readRows, int cap, bool expectShownRows)
        => Assert.Equal(expectShownRows, AlertDismissScope.UseShownRows(filterActive, readRows, cap));
}
