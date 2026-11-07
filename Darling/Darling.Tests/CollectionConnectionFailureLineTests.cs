/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8101. Each collector run opens its own connection, so a target that is unreachable for one cycle fails every collector due
/// in it: fifteen rows each read Errors 1 and HEALTHY at a 0.0 failure rate, and nothing on the tab says they are one event.
/// The Collection Health header words such a failure once, from the rows' newest errors, in both Lite and the Viewer. These
/// drive the shared summary through reflection so the file compiles whether or not it exists.
/// </summary>
public sealed class CollectionConnectionFailureLineTests
{
    private const string PreLoginHandshake =
        "A connection was successfully established with the server, but then an error occurred during the pre-login handshake. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)";

    private static readonly DateTime Cycle = new(2026, 11, 7, 10, 32, 5, DateTimeKind.Utc);

    [Fact]
    public void FifteenCollectorsFailingTheSameOpenInOneCycle_ReadAsOneConnectionFailure()
    {
        var rows = Enumerable.Range(0, 15)
            .Select(i => ($"collector_{i:00}", (DateTime?)Cycle.AddSeconds(i * 3), (string?)PreLoginHandshake, 1L))
            .ToList();

        var line = SummaryLine(rows);

        Assert.NotNull(line);
        Assert.StartsWith("One connection failure at 2026-11-07 10:32:05", line);
        Assert.Contains("reached 15 collectors", line);
        Assert.Contains("each missed that one run", line);
        Assert.Contains("Nothing to do unless it repeats", line);
    }

    [Fact]
    public void ACollectorWhoseWindowHoldsOtherErrors_IsNotSaidToHaveMissedOneRunAlone()
    {
        var rows = new List<(string, DateTime?, string?, long)>
        {
            ("wait_stats", Cycle, PreLoginHandshake, 1),
            ("query_stats", Cycle.AddSeconds(4), PreLoginHandshake, 3),
            ("blocking", Cycle.AddSeconds(8), PreLoginHandshake, 1),
        };

        var line = SummaryLine(rows);

        Assert.NotNull(line);
        Assert.Contains("reached 3 collectors", line);
        Assert.Contains("1 of them have other errors in the window as well", line);
        Assert.DoesNotContain("each missed that one run", line);
    }

    [Fact]
    public void AStatementErrorOnOneCollector_IsNotAConnectionFailure()
    {
        var rows = new List<(string, DateTime?, string?, long)>
        {
            ("query_store", Cycle, "SQL Error #207: Invalid column name 'plan_forcing_type'.", 2),
            ("wait_stats", null, null, 0),
            ("blocking", Cycle.AddSeconds(2), "SQL Error #1222: Lock request time out period exceeded.", 1),
        };

        Assert.Null(SummaryLine(rows));
    }

    [Fact]
    public void AConnectionFailureThatReachedOneCollectorAlone_IsThatRowsOwnLastError_NotAnEvent()
    {
        var rows = new List<(string, DateTime?, string?, long)>
        {
            ("wait_stats", Cycle, PreLoginHandshake, 1),
            ("query_stats", null, null, 0),
        };

        Assert.Null(SummaryLine(rows));
    }

    [Fact]
    public void TwoDropsHoursApart_ReadAsTheNewestWithTheEarlierCounted()
    {
        var earlier = Cycle.AddHours(-6);
        var rows = new List<(string, DateTime?, string?, long)>
        {
            ("wait_stats", Cycle, PreLoginHandshake, 2),
            ("query_stats", Cycle.AddSeconds(5), PreLoginHandshake, 2),
            ("blocking", earlier, "A network-related or instance-specific error occurred while establishing a connection to SQL Server.", 1),
            ("deadlocks", earlier.AddSeconds(7), "A network-related or instance-specific error occurred while establishing a connection to SQL Server.", 1),
        };

        var line = SummaryLine(rows);

        Assert.NotNull(line);
        Assert.StartsWith("One connection failure at 2026-11-07 10:32:05 reached 2 collectors", line);
        Assert.EndsWith("One earlier connection failure is in the window too.", line);
    }

    [Fact]
    public void ADropWhoseOpensWaitedOutTheConnectTimeout_IsWordedAsASpan()
    {
        var rows = Enumerable.Range(0, 6)
            .Select(i => ($"collector_{i}", (DateTime?)Cycle.AddSeconds(i * 15), (string?)"Connection Timeout Expired. The timeout period elapsed during the post-login phase.", 1L))
            .ToList();

        var line = SummaryLine(rows);

        Assert.NotNull(line);
        Assert.StartsWith("One connection failure between 2026-11-07 10:32:05 and 2026-11-07 10:33:20 reached 6 collectors", line);
    }

    /// <summary>
    /// Drives <c>CollectionConnectionFailures.SummaryLine</c> with the rows as (collector, newest error time, newest error text,
    /// error count), formatting instants as plain UTC. Fails at an assert, not a compile, where the shared summary is absent.
    /// </summary>
    private static string? SummaryLine(IReadOnlyList<(string Name, DateTime? At, string? Error, long Errors)> rows)
    {
        var assembly = typeof(CollectorHealthClassifier).Assembly;
        var summary = assembly.GetType("PerformanceMonitor.Common.CollectionConnectionFailures");
        var sample = assembly.GetType("PerformanceMonitor.Common.CollectorErrorSample");
        if (summary is null || sample is null)
        {
            Assert.Fail("No shared connection-failure summary exists: a drop that reached every due collector is left as one ERROR on each row with nothing to say they are one event.");
            return null;
        }

        var samples = Array.CreateInstance(sample, rows.Count);
        var constructor = sample.GetConstructor(new[] { typeof(string), typeof(DateTime?), typeof(string), typeof(long) });
        Assert.NotNull(constructor);
        for (var i = 0; i < rows.Count; i++)
        {
            samples.SetValue(constructor.Invoke(new object?[] { rows[i].Name, rows[i].At, rows[i].Error, rows[i].Errors }), i);
        }

        var method = summary.GetMethod("SummaryLine", BindingFlags.Public | BindingFlags.Static);
        Assert.NotNull(method);
        Func<DateTime, string> format = utc => utc.ToString("yyyy-MM-dd HH:mm:ss", System.Globalization.CultureInfo.InvariantCulture);
        return (string?)method.Invoke(null, new object?[] { samples, format });
    }
}
