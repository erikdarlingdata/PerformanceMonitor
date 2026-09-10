/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #2757: a customer's screenshot showed the Availability Groups grid's "Redo rate (KB/s)" column, sorted
/// descending, reading 73, 72'989, 7'275, 70'743, 70, 7, 7 — exact LEXICOGRAPHIC descending order ("73" >
/// "72989" as strings), not numeric. Root cause: each numeric column binds to a pre-formatted display STRING
/// (<c>RedoRateDisplay</c>, N0-formatted with a thousands separator) with no explicit <c>SortMemberPath</c>.
/// WPF's <c>DataGridBoundColumn.SortMemberPath</c> falls back to the column's own <c>Binding.Path</c> when
/// unset, so the app-wide <see cref="PerformanceMonitor.Ui.DataGridSortBehavior"/> (every column's first click
/// sorts descending) built its <c>SortDescription</c> against that same display string — sorting the text,
/// not the value it was formatted from.
///
/// <para>Both apps carry an identical, hand-duplicated copy of this grid (no shared XAML between SKUs), so
/// this pins both files rather than one — the exact drift shape <see cref="MainWindowAccessKeyTests"/>'
/// remarks describe for this same pair of files.</para>
/// </summary>
public sealed class AvailabilityGroupsGridSortTests
{
    private const string LiteGrid = "Lite/Controls/AvailabilityGroupsTab.xaml";
    private const string DarlingGrid = "Darling/PerformanceMonitor.Darling.Viewer/AvailabilityGroupsTab.xaml";

    /// <summary>
    /// Header text -&gt; the underlying numeric property each column's display string is formatted FROM (see
    /// <c>AgTopologyDatabase</c> in <c>PerformanceMonitor.Common/AgTopology.cs</c>). Database/Replica/Sync
    /// state/Data movement are genuinely text and are correctly absent here — sorting them as strings is
    /// correct, not a bug.
    /// </summary>
    private static readonly Dictionary<string, string> s_expectedSortMemberPath = new(System.StringComparer.Ordinal)
    {
        ["Send queue (KB)"] = "LogSendQueueKb",
        ["Redo queue (KB)"] = "RedoQueueKb",
        ["Send rate (KB/s)"] = "LogSendRateKbPerSec",
        ["Redo rate (KB/s)"] = "RedoRateKbPerSec",
        ["Lag"] = "SecondaryLagSeconds",
        ["Est drain (min)"] = "EstimatedSendDrainMinutes",
        ["Est redo (min)"] = "EstimatedRedoCompletionMinutes",
    };

    [Theory]
    [InlineData(LiteGrid)]
    [InlineData(DarlingGrid)]
    public void EveryNumericColumn_SortsOnItsRawValue_NotItsFormattedDisplayString(string gridPath)
    {
        var xaml = ParitySource.ReadFile(gridPath);

        foreach (var (header, expectedSortPath) in s_expectedSortMemberPath)
        {
            var match = Regex.Match(
                xaml,
                $@"<DataGridTextColumn Header=""{Regex.Escape(header)}""[^/]*?/>");

            Assert.True(match.Success, $"{gridPath}: no <DataGridTextColumn> found for header \"{header}\" — the column may have been renamed or removed.");

            var sortPathMatch = Regex.Match(match.Value, @"SortMemberPath=""([^""]+)""");
            Assert.True(sortPathMatch.Success,
                $"{gridPath}: \"{header}\" carries no SortMemberPath, so it falls back to sorting its Binding path — " +
                $"the exact #2757 regression (sorting the formatted display STRING instead of the numeric value).");

            Assert.Equal(expectedSortPath, sortPathMatch.Groups[1].Value);

            /* The regression's fingerprint: SortMemberPath must NOT be the *Display binding itself. */
            var bindingMatch = Regex.Match(match.Value, @"Binding=""\{Binding (\w+)\}""");
            Assert.True(bindingMatch.Success, $"{gridPath}: \"{header}\" has no simple Binding to compare against.");
            Assert.NotEqual(bindingMatch.Groups[1].Value, sortPathMatch.Groups[1].Value);
        }
    }

    /// <summary>
    /// The two apps' grids are hand-duplicated (see class remarks) — this is the parity half, so a fix landed
    /// in only one file fails here instead of shipping half-fixed.
    /// </summary>
    [Fact]
    public void BothApps_AssignTheSameSortMemberPath_ToEveryNumericColumn()
    {
        var lite = ParitySource.ReadFile(LiteGrid);
        var darling = ParitySource.ReadFile(DarlingGrid);

        foreach (var header in s_expectedSortMemberPath.Keys)
        {
            var liteSortPath = Regex.Match(
                Regex.Match(lite, $@"<DataGridTextColumn Header=""{Regex.Escape(header)}""[^/]*?/>").Value,
                @"SortMemberPath=""([^""]+)""").Groups[1].Value;
            var darlingSortPath = Regex.Match(
                Regex.Match(darling, $@"<DataGridTextColumn Header=""{Regex.Escape(header)}""[^/]*?/>").Value,
                @"SortMemberPath=""([^""]+)""").Groups[1].Value;

            Assert.Equal(darlingSortPath, liteSortPath);
        }
    }
}
