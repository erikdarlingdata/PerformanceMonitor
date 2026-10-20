/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>Help > About names the build (#6244): the informational version, with the commit when the build has one.</summary>
public sealed class AboutVersionTextTests
{
    [Fact]
    public void A_commit_suffix_is_shown_shortened()
    {
        Assert.Equal("Version 3.10.0 (commit 0993a1441)",
            AboutVersionText.Format("3.10.0+0993a1441f2c8d7e6b5a49382716abcdef012345"));
    }

    [Fact]
    public void A_short_commit_is_shown_whole()
    {
        Assert.Equal("Version 3.10.0 (commit abc1234)", AboutVersionText.Format("3.10.0+abc1234"));
    }

    [Fact]
    public void No_commit_shows_just_the_version()
    {
        Assert.Equal("Version 3.10.0", AboutVersionText.Format("3.10.0"));
    }

    [Fact]
    public void An_empty_commit_suffix_shows_just_the_version()
    {
        Assert.Equal("Version 3.10.0", AboutVersionText.Format("3.10.0+"));
    }

    [Fact]
    public void A_prerelease_tag_stays_in_the_version_part()
    {
        Assert.Equal("Version 3.10.0-nightly.20261020.5 (commit 0993a1441)",
            AboutVersionText.Format("3.10.0-nightly.20261020.5+0993a1441f2c8d7e6b5a49382716abcdef012345"));
        Assert.Equal("Version 3.10.0-nightly.20261020.5", AboutVersionText.Format("3.10.0-nightly.20261020.5"));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("+abc1234")]
    public void A_missing_version_reads_as_unknown(string? raw)
    {
        Assert.Equal("Version unknown", AboutVersionText.Format(raw));
    }

    [Fact]
    public void The_viewer_assembly_has_a_version_line()
    {
        var text = AboutVersionText.ForAssembly(typeof(AboutVersionText).Assembly);
        Assert.StartsWith("Version 3.", text);
    }
}
