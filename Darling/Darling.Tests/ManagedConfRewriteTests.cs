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
using PerformanceMonitor.Darling.Service;
using Xunit;
using static PerformanceMonitor.Darling.Service.ManagedConfMigration;

namespace Darling.Tests;

/// <summary>
/// <c>ManagedConfMigration.Rewrite</c> (#4336 lane rewrite2, correcting the earlier lane mig-b build under
/// design ruling comment-5827624802 §3 rule 3): a non-Ours line for a managed key moves below the include
/// ONLY when it is the currently-effective assignment of that key; an already-overridden one stays exactly
/// where it is. Pure logic, no wiring — no live PostgreSQL, no disk.
/// </summary>
public sealed class ManagedConfRewriteTests
{
    private static readonly IReadOnlyDictionary<string, string> ManagedValues =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["max_connections"] = "200",
            ["work_mem"] = "16MB",
        };

    /// <summary>(a) An operator line for an owned key, currently effective (nothing later sets it), moves
    /// below the include: exactly one moved line, and the log carries the override with the real derived
    /// value from <see cref="ManagedValues"/> — never a literal placeholder.</summary>
    [Fact]
    public void Rewrite_EffectiveOperatorLineForOwnedKey_MovesBelowInclude_WithRealDerivedValueInLog()
    {
        var conf =
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "max_connections = 300\n" +
            DarlingManagedPostgres.ConfMarkerV7 + "\n" +
            "maintenance_work_mem = 2048MB\n";

        var result = Rewrite(conf, ManagedValues);

        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        Assert.True(includeIndex >= 0);
        Assert.Equal(MovedOperatorLinesComment, lines[includeIndex + 1]);
        Assert.Equal("max_connections = 300", lines[includeIndex + 2]);
        Assert.Equal(1, lines.Count(l => l == "max_connections = 300"));
        Assert.Contains("max_connections", result.ExcludedKeys);

        var overrideEntry = Assert.Single(result.Log, e => e.OverriddenKey == "max_connections");
        Assert.Equal(
            "max_connections: derived 200, overridden by operator line below the include (300), not applied",
            overrideEntry.Message);
    }

    /// <summary>(b) An operator line for an owned key that a LATER line overrides is not effective — it
    /// stays exactly where it is, and is not reported as excluded.</summary>
    [Fact]
    public void Rewrite_NonEffectiveOperatorLineForOwnedKey_StaysInPlace_NotExcluded()
    {
        var conf =
            "work_mem = 70MB\n" +
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "work_mem = 32MB\n";

        var result = Rewrite(conf, ManagedValues);

        Assert.Contains("work_mem = 70MB", result.NewConfText);
        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        Assert.True(Array.IndexOf(lines, "work_mem = 70MB") < includeIndex);
        Assert.DoesNotContain("work_mem", result.ExcludedKeys);
        Assert.DoesNotContain(result.Log, e => e.OverriddenKey == "work_mem");
    }

    /// <summary>(c) A line for a key the managed file does not own stays exactly where it is.</summary>
    [Fact]
    public void Rewrite_LineForUnownedKey_StaysInPlace()
    {
        var conf =
            "log_timezone = 'UTC'\n" +
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend();

        var result = Rewrite(conf, ManagedValues);

        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        Assert.True(Array.IndexOf(lines, "log_timezone = 'UTC'") < includeIndex);
        Assert.DoesNotContain("log_timezone", result.ExcludedKeys);
    }

    /// <summary>(d) Rule 6: running the rewrite twice is idempotent, and the second run's log is empty
    /// because there is nothing left to migrate — no product marker remains and the include is already
    /// there.</summary>
    [Fact]
    public void Rewrite_AppliedTwice_IsByteIdentical_SecondRunLogIsEmpty()
    {
        var conf =
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "max_connections = 300\n";

        var first = Rewrite(conf, ManagedValues);
        var second = Rewrite(first.NewConfText, ManagedValues);

        Assert.Equal(first.NewConfText, second.NewConfText);
        Assert.Empty(second.Log);
    }

    /// <summary>(e) Exactly one include line, including when the input already has one.</summary>
    [Fact]
    public void Rewrite_InputAlreadyHasInclude_StillExactlyOneIncludeLine()
    {
        var conf =
            ManagedConfFile.IncludeLine + "\n" +
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "max_connections = 300\n";

        var result = Rewrite(conf, ManagedValues);

        Assert.Equal(1, result.NewConfText.Split('\n').Count(l => l == ManagedConfFile.IncludeLine));
    }

    /// <summary>(f) A conf with no product blocks at all: only the include is appended, and nothing
    /// moves.</summary>
    [Fact]
    public void Rewrite_NoProductBlocks_OnlyAppendsInclude_NothingMoves()
    {
        const string conf = "log_timezone = 'UTC'\nmax_connections = 300\n";

        var result = Rewrite(conf, ManagedValues);

        Assert.Equal(
            "log_timezone = 'UTC'\nmax_connections = 300\n" + ManagedConfFile.IncludeLine + "\n",
            result.NewConfText);
        Assert.Empty(result.ExcludedKeys);
    }

    /// <summary>(g) Two effective operator lines for two different owned keys keep their original relative
    /// order below the include.</summary>
    [Fact]
    public void Rewrite_TwoEffectiveOperatorLines_KeepOriginalRelativeOrderBelowInclude()
    {
        var conf =
            "max_connections = 300\n" +
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "work_mem = 48MB\n";

        var result = Rewrite(conf, ManagedValues);

        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        Assert.Equal("max_connections = 300", lines[includeIndex + 2]);
        Assert.Equal("work_mem = 48MB", lines[includeIndex + 3]);
    }
}
