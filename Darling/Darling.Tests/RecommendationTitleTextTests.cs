/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8226: the words a recommendation card shows for a finding named only by its fact token, shared by Lite and the Darling Viewer. A
/// wait type keeps its name and says that it is a wait; a key that is not a wait type reads as its words.
/// </summary>
public sealed class RecommendationTitleTextTests
{
    [Theory]
    [InlineData("RESOURCE_MEMPHYSICAL_LOW", "Low physical memory")]
    [InlineData("RESOURCE_MEMPHYSICAL_HIGH", "Physical memory recovered")]
    [InlineData("RESOURCE_MEMVIRTUAL_LOW", "Low virtual memory")]
    [InlineData("RESOURCE_MEM_STEADY", "Memory steady")]
    [InlineData("RESOURCE_MEMPHYSICAL_SOMETHING_NEW", "Memphysical something new")]
    [InlineData("RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE waits")]
    [InlineData("SOS_SCHEDULER_YIELD", "SOS_SCHEDULER_YIELD waits")]
    [InlineData("CXPACKET", "CXPACKET waits")]
    [InlineData("PAGEIOLATCH_SH", "PAGEIOLATCH_SH waits")]
    [InlineData("LCK_M_S", "LCK_M_S waits")]
    [InlineData("WRITELOG", "WRITELOG waits")]
    [InlineData("CONFIG_MAXDOP_UNSET", "Config maxdop unset")]
    [InlineData("PG_AUTOVACUUM_LAG", "Pg autovacuum lag")]
    [InlineData("QUERY_HIGH_DOP", "Query high dop")]
    [InlineData("Plan cache bloat", "Plan cache bloat")]
    [InlineData("CPU", "CPU")]
    [InlineData("", "")]
    public void AFindingNamedByItsToken_ReadsAsWords(string title, string expected)
    {
        Assert.Equal(expected, RecommendationTitleText.Readable(title));
    }
}
