/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Alerting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6112: the Settings windows' Excluded databases and Excluded volumes boxes hold one entry per line. A database
/// name with a comma has to survive a load then a save, which the old comma-joined box turned into two names.
/// </summary>
public sealed class SettingsListTextTests
{
    [Fact]
    public void A_database_name_with_a_comma_survives_load_then_save()
    {
        var names = new[] { "Sales,2024", "DBAtools", "a, b" };

        var saved = SettingsListText.SplitLines(SettingsListText.Join(names));

        Assert.Equal(names, saved);
        Assert.Equal(names, ExcludedDatabaseNames.Normalize(saved));
    }

    [Theory]
    [InlineData("one\r\ntwo\r\nthree")]
    [InlineData("one\ntwo\nthree")]
    [InlineData("one\rtwo\rthree")]
    [InlineData("one\r\ntwo\nthree")]
    public void Both_line_break_styles_split_database_names(string text)
    {
        Assert.Equal(new[] { "one", "two", "three" }, SettingsListText.SplitLines(text));
    }

    [Fact]
    public void Blank_lines_and_surrounding_spaces_are_dropped_from_database_names()
    {
        var saved = SettingsListText.SplitLines("  Sales,2024  \r\n\r\n   \n\tmsdb\t\n");

        Assert.Equal(new[] { "Sales,2024", "msdb" }, saved);
    }

    [Fact]
    public void An_empty_or_null_box_gives_an_empty_list_and_an_empty_list_gives_an_empty_box()
    {
        Assert.Empty(SettingsListText.SplitLines(""));
        Assert.Empty(SettingsListText.SplitLines(null));
        Assert.Empty(SettingsListText.SplitLinesAndCommas(null));
        Assert.Equal("", SettingsListText.Join(new string[0]));
        Assert.Equal("", SettingsListText.Join(null));
    }

    [Fact]
    public void Volumes_round_trip_one_per_line()
    {
        var volumes = new[] { "S:\\", "/mnt/scratch" };

        Assert.Equal(volumes, SettingsListText.SplitLinesAndCommas(SettingsListText.Join(volumes)));
    }

    [Fact]
    public void The_volumes_box_also_splits_on_commas_so_an_older_paste_still_works()
    {
        var saved = VolumeExclusions.Normalize(SettingsListText.SplitLinesAndCommas("S:\\, T:\\\r\n /mnt/scratch ,\n\n"));

        Assert.Equal(new[] { "S:\\", "T:\\", "/mnt/scratch" }, saved.ToArray());
    }
}
