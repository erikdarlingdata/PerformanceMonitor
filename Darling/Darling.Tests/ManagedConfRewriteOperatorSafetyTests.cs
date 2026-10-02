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
/// <c>ManagedConfMigration.Rewrite</c> (#4336, review finding): owned-key matching is case-insensitive,
/// the way PostgreSQL itself reads GUC names, and an operator's <c>include</c>/<c>include_if_exists</c>/
/// <c>include_dir</c> directive placed after the product blocks moves below the new include the same way
/// an effective operator assignment does — otherwise the operator's setting (or included file) is read
/// before the managed file's own include and is lost on the next start.
/// </summary>
public sealed class ManagedConfRewriteOperatorSafetyTests
{
    private static readonly IReadOnlyDictionary<string, string> ManagedValues =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["max_connections"] = "200",
            ["work_mem"] = "16MB",
        };

    /// <summary>(a) A case-variant operator line for an owned key (<c>Work_Mem</c>, not <c>work_mem</c>),
    /// currently effective, moves below the include with its casing intact, and <c>work_mem</c> is
    /// excluded.</summary>
    [Fact]
    public void Rewrite_CaseVariantOperatorLineForOwnedKey_MovesBelowInclude_AndIsExcluded()
    {
        var conf =
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "Work_Mem = 64MB\n";

        var result = Rewrite(conf, ManagedValues);

        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        Assert.True(includeIndex >= 0);
        Assert.True(Array.IndexOf(lines, "Work_Mem = 64MB") > includeIndex);
        Assert.Contains("work_mem", result.ExcludedKeys);
    }

    /// <summary>(b) An operator <c>include</c> directive placed after the product blocks moves below the
    /// new include, in original relative order with a moved assignment line.</summary>
    [Fact]
    public void Rewrite_OperatorIncludeAfterBlocks_MovesBelowInclude_InRelativeOrderWithMovedAssignment()
    {
        var conf =
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "max_connections = 300\n" +
            "include 'override.conf'\n";

        var result = Rewrite(conf, ManagedValues);

        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        var maxConnIndex = Array.IndexOf(lines, "max_connections = 300");
        var overrideIndex = Array.IndexOf(lines, "include 'override.conf'");

        Assert.True(includeIndex >= 0);
        Assert.True(maxConnIndex > includeIndex);
        Assert.True(overrideIndex > maxConnIndex);
        Assert.Contains(
            "include 'override.conf': operator include after the product settings, moved below the include",
            result.Log.Select(e => e.Message));
    }

    /// <summary>(c) An operator <c>include_dir</c> directive placed BEFORE the product blocks stays exactly
    /// where it is — it was already losing to the blocks, and stays losing to the managed file's own
    /// include in the same position.</summary>
    [Fact]
    public void Rewrite_OperatorIncludeDirBeforeBlocks_StaysInPlace()
    {
        var conf =
            "include_dir 'conf.d'\n" +
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend();

        var result = Rewrite(conf, ManagedValues);

        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        var includeDirIndex = Array.IndexOf(lines, "include_dir 'conf.d'");

        Assert.True(includeIndex >= 0);
        Assert.True(includeDirIndex >= 0);
        Assert.True(includeDirIndex < includeIndex);
    }

    /// <summary>(d) A second run over the case-variant fixture's output is byte-identical.</summary>
    [Fact]
    public void Rewrite_CaseVariantOperatorLine_AppliedTwice_IsByteIdentical()
    {
        var conf =
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "Work_Mem = 64MB\n";

        var first = Rewrite(conf, ManagedValues);
        var second = Rewrite(first.NewConfText, ManagedValues);

        Assert.Equal(first.NewConfText, second.NewConfText);
    }

    /// <summary>(e) A second run over the operator-include fixture's output is byte-identical.</summary>
    [Fact]
    public void Rewrite_OperatorIncludeAfterBlocks_AppliedTwice_IsByteIdentical()
    {
        var conf =
            DarlingManagedPostgres.BuildLegacyMaintenanceWorkMemCapConfAppend() +
            "max_connections = 300\n" +
            "include 'override.conf'\n";

        var first = Rewrite(conf, ManagedValues);
        var second = Rewrite(first.NewConfText, ManagedValues);

        Assert.Equal(first.NewConfText, second.NewConfText);
    }
}
