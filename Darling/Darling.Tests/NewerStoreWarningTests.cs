/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5773: a store migrated by a newer build gets ONE warning at start, naming the rungs this build does not know and
/// pointing at the README's rollback list, and the start goes on. The version is planted through
/// <see cref="PgMigrations.WarnIfStoreNewerThanBuild"/>, the seam the migration applier calls with the version it
/// read, so no store is needed.
/// </summary>
public sealed class NewerStoreWarningTests
{
    private static int BuildTop => PgMigrations.Scripts[^1].Version;

    [Fact]
    public void A_store_several_rungs_ahead_gets_one_warning_naming_the_range()
    {
        var logger = new CapturingTestLogger();

        var warned = PgMigrations.WarnIfStoreNewerThanBuild(logger, BuildTop + 3, BuildTop);

        Assert.True(warned);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Single(logger.Lines);
        var line = logger.Lines[0];
        Assert.Contains(
            $"The store is at V{BuildTop + 3}; this build knows up to V{BuildTop}, so V{BuildTop + 1}-V{BuildTop + 3} are unknown to it.",
            line, StringComparison.Ordinal);
        Assert.Contains("\"Rolling back to an older build\" in Darling/README.md", line, StringComparison.Ordinal);
    }

    [Fact]
    public void A_store_one_rung_ahead_names_that_rung_alone()
    {
        var logger = new CapturingTestLogger();

        Assert.True(PgMigrations.WarnIfStoreNewerThanBuild(logger, BuildTop + 1, BuildTop));

        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains(
            $"The store is at V{BuildTop + 1}; this build knows up to V{BuildTop}, so V{BuildTop + 1} is unknown to it.",
            logger.Lines[0], StringComparison.Ordinal);
    }

    [Fact]
    public void A_store_at_the_builds_top_rung_gets_no_warning()
    {
        var logger = new CapturingTestLogger();

        Assert.False(PgMigrations.WarnIfStoreNewerThanBuild(logger, BuildTop, BuildTop));

        Assert.Empty(logger.Lines);
    }

    [Fact]
    public void A_store_below_the_builds_top_rung_gets_no_warning()
    {
        var logger = new CapturingTestLogger();

        Assert.False(PgMigrations.WarnIfStoreNewerThanBuild(logger, BuildTop - 1, BuildTop));
        Assert.False(PgMigrations.WarnIfStoreNewerThanBuild(logger, 0, BuildTop));

        Assert.Empty(logger.Lines);
    }

    /// <summary>
    /// The seam is only worth something if the start reaches it. Both paths that read the store's version at start
    /// call it with this build's top rung: the locked migrate, and the lock-timeout path that skips it.
    /// </summary>
    [Fact]
    public void Both_startup_version_reads_call_the_check_with_the_top_rung()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "PgMigrations.cs");

        Assert.Contains("WarnIfStoreNewerThanBuild(logger, currentVersion, Scripts[^1].Version);", source, StringComparison.Ordinal);
        Assert.Contains("WarnIfStoreNewerThanBuild(logger, storeVersion, Scripts[^1].Version);", source, StringComparison.Ordinal);
    }

    /// <summary>The warning names a README section; the section has to exist, with at least one rung in its list.</summary>
    [Fact]
    public void The_readme_carries_the_rollback_section_the_warning_names()
    {
        var readme = RepoFile.ReadRepoFile("Darling", "README.md");
        var lines = readme.Split('\n').Select(l => l.TrimEnd('\r')).ToList();

        var heading = lines.FindIndex(l => l.TrimStart('#').Trim() == "Rolling back to an older build" && l.StartsWith('#'));
        Assert.True(heading >= 0, "Darling/README.md has no \"Rolling back to an older build\" heading");
        Assert.Contains(lines.Skip(heading + 1).TakeWhile(l => !l.StartsWith('#')), l => l.StartsWith("- **V", StringComparison.Ordinal));
    }
}
