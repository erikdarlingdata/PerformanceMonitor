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
/// <c>ManagedConfMigration.Rewrite</c> (#4336): a non-Ours line for a managed key moves below the include
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
    /// stays exactly where it is, and is not reported as excluded. #4336:
    /// the ORIGINAL version of this test used <see cref="DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend"/>
    /// as the "later line", but that v14 block sets <c>maintenance_work_mem</c>, never <c>work_mem</c> —
    /// so the operator's <c>work_mem = 70MB</c> WAS the last (and only) assignment of that key in the file,
    /// correctly excluded by <c>Rewrite</c>. The test's premise was wrong, not the code. This version's
    /// "later line" is v3's memory-sizing block at 8 GB RAM, which DOES derive <c>work_mem</c> (16MB, per
    /// <see cref="DarlingManagedPostgres.DeriveMemorySettings"/>'s clamp(RAM/512, 16MB, 64MB)) — matching
    /// <see cref="ManagedValues"/>'s own 16MB so the log-suppression path ("this is a covered Ours line
    /// restating the same derived value, not a genuine override") is exercised too, same as the rehearsal
    /// fixture's v8-then-v8 case.</summary>
    [Fact]
    public void Rewrite_NonEffectiveOperatorLineForOwnedKey_StaysInPlace_NotExcluded()
    {
        var conf =
            "work_mem = 70MB\n" +
            DarlingManagedPostgres.BuildMemorySizingConfAppend(8L * 1024 * 1024 * 1024);

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

    /// <summary>(f) A conf with no product blocks at all and no line for an OWNED key: only the include is
    /// appended, and nothing moves. #4336: the ORIGINAL version of this test
    /// used <c>max_connections = 300</c>, an owned key — under rule 3, an effective operator line for an
    /// owned key moves below the include even with no product blocks present at all (there is nothing for
    /// it to be overridden BY), so <c>Rewrite</c> correctly moved it and the test's "nothing moves" name and
    /// assertion were wrong, not the code. This version uses only unowned keys
    /// (<c>log_timezone</c>, an arbitrary <c>something_else</c>) so "nothing moves" actually holds; the
    /// owned-key-with-no-product-blocks case is covered instead by (g)-adjacent coverage in
    /// <see cref="Rewrite_EffectiveOperatorLineForOwnedKey_MovesBelowInclude_WithRealDerivedValueInLog"/>
    /// (which does have a product block) — the design's rule 3 does not condition the move on a product
    /// block existing, only on the key being one <see cref="ManagedValues"/> owns and the line being the
    /// currently-effective assignment.</summary>
    [Fact]
    public void Rewrite_NoProductBlocks_NoOwnedKeyLines_OnlyAppendsInclude_NothingMoves()
    {
        const string conf = "log_timezone = 'UTC'\nsomething_else = 1\n";

        var result = Rewrite(conf, ManagedValues);

        Assert.Equal(
            "log_timezone = 'UTC'\nsomething_else = 1\n" + ManagedConfFile.IncludeLine + "\n",
            result.NewConfText);
        Assert.Empty(result.ExcludedKeys);
    }

    /// <summary>(f2) A conf with no product blocks at ALL but an operator line for an OWNED key: rule 3 still
    /// moves it below the include — there is no product block for it to be overridden by, so it is
    /// trivially the last (and only) assignment of that key.</summary>
    [Fact]
    public void Rewrite_NoProductBlocks_OwnedKeyLinePresent_MovesBelowInclude()
    {
        const string conf = "log_timezone = 'UTC'\nmax_connections = 300\n";

        var result = Rewrite(conf, ManagedValues);

        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        Assert.True(Array.IndexOf(lines, "log_timezone = 'UTC'") < includeIndex);
        Assert.True(Array.IndexOf(lines, "max_connections = 300") > includeIndex);
        Assert.Contains("max_connections", result.ExcludedKeys);
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
