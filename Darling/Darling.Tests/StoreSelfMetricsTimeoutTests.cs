/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The #2317 sweep timeout. The self-metrics sizing queries (<c>hypertable_local_size</c> across every
/// hypertable, <c>pg_database_size</c> over the whole store) outgrew Npgsql's default 30 seconds ~5x/day
/// on the dogfood fleet, and the cancel surfaces as "Exception while reading from stream" — a timeout
/// wearing a network-fault costume (the #2294 lesson one layer over). Every statement in the sweep must
/// carry the explicit timeout; a sixth statement added without one silently rides the 30s default and
/// reintroduces the fake fault, so this pins the count of constructions to the count of timeouts.
/// </summary>
public sealed class StoreSelfMetricsTimeoutTests
{
    [Fact]
    public void EverySweepStatementCarriesTheTimeout()
    {
        var source = ReadStoreSelfMetricsSource();

        var constructions = Regex.Matches(source, @"new NpgsqlCommand\(").Count;
        var timeouts = Regex.Matches(source, @"CommandTimeout = SweepTimeoutSeconds").Count;

        Assert.True(constructions > 0, "the sweep no longer constructs commands where this pin expects them — re-point it");
        Assert.Equal(constructions, timeouts);
    }

    /// <summary>Five minutes, matching DarlingRetention's destructive-statement budget — an hourly sweep
    /// on its own connection can afford patience, and a sweep that cannot finish in five minutes should
    /// skip the tick rather than retry into the same load.</summary>
    [Fact]
    public void TheTimeoutIsTheRetentionBudget()
        => Assert.Equal(300, StoreSelfMetrics.SweepTimeoutSeconds);

    private static string ReadStoreSelfMetricsSource([System.Runtime.CompilerServices.CallerFilePath] string thisFile = "")
    {
        var dir = System.IO.Path.GetDirectoryName(thisFile)!;
        var relative = System.IO.Path.Combine("Darling", "PerformanceMonitor.Darling.Storage", "StoreSelfMetrics.cs");
        while (dir is not null && !System.IO.File.Exists(System.IO.Path.Combine(dir, relative)))
        {
            dir = System.IO.Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return System.IO.File.ReadAllText(System.IO.Path.Combine(dir!, relative));
    }
}
