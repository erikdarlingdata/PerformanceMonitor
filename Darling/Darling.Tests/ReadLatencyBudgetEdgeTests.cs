/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The get_read_latency page never serializes past its byte budget, even when the greedy fill lands within a few
/// bytes of it (#5135): the envelope's reads_returned digits and truncated spelling depend on the final page.
/// </summary>
public sealed class ReadLatencyBudgetEdgeTests
{
    private static object[] Rows(int n) => Enumerable.Range(0, n)
        .Select(i => (object)new { surface = "web", route = "route_" + i.ToString("D4"), count = 1000 - i, pad = new string('x', 120) })
        .ToArray();

    [Fact]
    public void Response_NeverExceedsBudget_WhenTheFillLandsWithinAFewBytesOfIt()
    {
        var rows = Rows(150);
        var full = Encoding.UTF8.GetByteCount(DarlingMcpReadLatencyTools.BuildResponse(24, null, null, rows, 1000, int.MaxValue));

        var trimmedAtLeastOnce = false;
        for (var budget = full - 12; budget <= full + 2; budget++)
        {
            var json = DarlingMcpReadLatencyTools.BuildResponse(24, null, null, rows, 1000, budget);
            Assert.True(Encoding.UTF8.GetByteCount(json) <= budget, $"budget {budget}: got {Encoding.UTF8.GetByteCount(json)}");
            trimmedAtLeastOnce |= json.Contains("\"truncated\":true");
        }

        Assert.True(trimmedAtLeastOnce);
    }
}
