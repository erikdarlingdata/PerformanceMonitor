/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>ManagedConfMigration.ExtractOperatorLinesBelowInclude</c> (#4358): the pure text-only read of an
/// already-migrated <c>postgresql.conf</c>'s below-include region, which the major-upgrade path carries
/// into the new cluster's <c>postgresql.conf</c> alongside <c>CarryAutoConfAsync</c>'s auto.conf carry.
/// </summary>
public sealed class ManagedConfExtractOperatorLinesTests
{
    /// <summary>Pin 1: an operator line below the include is returned, in original order.</summary>
    [Fact]
    public void ExtractOperatorLinesBelowInclude_LineBelowInclude_IsReturned()
    {
        var conf =
            "max_connections = 200\n" +
            "include 'darling-managed.conf'\n" +
            "# operator settings kept from the previous postgresql.conf (#4215)\n" +
            "log_min_duration_statement = 250\n";

        var lines = ManagedConfMigration.ExtractOperatorLinesBelowInclude(conf);

        Assert.Equal(
            new[]
            {
                "# operator settings kept from the previous postgresql.conf (#4215)",
                "log_min_duration_statement = 250",
            },
            lines);
    }

    /// <summary>Pin 2: a line ABOVE the include (a stock/legacy setting) never appears in the extracted
    /// set — only the below-include region is returned.</summary>
    [Fact]
    public void ExtractOperatorLinesBelowInclude_LineAboveInclude_IsNotReturned()
    {
        var conf =
            "max_connections = 200\n" +
            "include 'darling-managed.conf'\n" +
            "log_min_duration_statement = 250\n";

        var lines = ManagedConfMigration.ExtractOperatorLinesBelowInclude(conf);

        Assert.DoesNotContain("max_connections = 200", lines);
        Assert.Equal(new[] { "log_min_duration_statement = 250" }, lines);
    }

    /// <summary>Pin 3: a Legacy conf — no working include at all — returns nothing; there is no
    /// below-the-include region to carry for a store that was never migrated (#4358's own scope).</summary>
    [Fact]
    public void ExtractOperatorLinesBelowInclude_NoInclude_ReturnsEmpty()
    {
        var conf =
            "max_connections = 200\n" +
            "work_mem = 64MB\n";

        var lines = ManagedConfMigration.ExtractOperatorLinesBelowInclude(conf);

        Assert.Empty(lines);
    }

    /// <summary>Pin 3b: the include line detection goes through PostgreSQL's own parser
    /// (<c>DarlingManagedPostgres.ParseConfText</c>), not a raw string match against
    /// <see cref="ManagedConfFile.IncludeLine"/> — an equivalent include naming the same file via a relative
    /// path prefix is still recognised (matched by file name, same as <c>HasManagedInclude</c>), and
    /// everything below it is still returned.</summary>
    [Fact]
    public void ExtractOperatorLinesBelowInclude_DifferentlyPathedInclude_IsStillFound()
    {
        var conf =
            "max_connections = 200\n" +
            "include './darling-managed.conf'\n" +
            "log_min_duration_statement = 250\n";

        var lines = ManagedConfMigration.ExtractOperatorLinesBelowInclude(conf);

        Assert.Equal(new[] { "log_min_duration_statement = 250" }, lines);
    }

    /// <summary>Pin: a conf ending exactly at the include line (nothing below it at all) returns empty,
    /// not a spurious trailing blank line.</summary>
    [Fact]
    public void ExtractOperatorLinesBelowInclude_NothingBelowInclude_ReturnsEmpty()
    {
        var conf =
            "max_connections = 200\n" +
            "include 'darling-managed.conf'\n";

        var lines = ManagedConfMigration.ExtractOperatorLinesBelowInclude(conf);

        Assert.Empty(lines);
    }
}
