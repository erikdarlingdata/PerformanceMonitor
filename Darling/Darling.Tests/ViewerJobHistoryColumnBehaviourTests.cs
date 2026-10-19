/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Data;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6086: the Job History grid's Duration column sorted by its formatted text ("9s" above "5h 45m"), and its Message column
/// cut long text with no way to read the rest. These tests build the real tab, take the column the user clicks, and sort the
/// rows the way the grid's own collection view does, so they fail on the column's sort key and tooltip, not on a copy of them.
/// </summary>
public sealed class ViewerJobHistoryColumnBehaviourTests
{
    private static DataGridTextColumn Column(JobHistoryTab tab, string header) =>
        tab.JobHistoryDataGrid.Columns.OfType<DataGridTextColumn>().Single(c =>
            c.Header is StackPanel panel && panel.Children.OfType<TextBlock>().Any(t => t.Text == header));

    private static List<ViewerJobHistoryRow> Sorted(DataGridTextColumn column, IEnumerable<ViewerJobHistoryRow> rows)
    {
        var view = new ListCollectionView(rows.ToList());
        view.SortDescriptions.Add(new SortDescription(column.SortMemberPath, ListSortDirection.Ascending));
        return view.Cast<ViewerJobHistoryRow>().ToList();
    }

    [Fact]
    public void DurationColumn_SortsByTheSeconds_NotByTheFormattedText()
    {
        StaTestThread.Run(() =>
        {
            var tab = new JobHistoryTab();
            var rows = new[]
            {
                new ViewerJobHistoryRow { JobName = "nineSeconds", RunDurationSeconds = 9 },
                new ViewerJobHistoryRow { JobName = "fiveHours", RunDurationSeconds = (5 * 3600) + (45 * 60) },
                new ViewerJobHistoryRow { JobName = "twoMinutes", RunDurationSeconds = 125 },
                new ViewerJobHistoryRow { JobName = "tenMinutes", RunDurationSeconds = 600 },
            };

            var sorted = Sorted(Column(tab, "Duration"), rows);

            Assert.Equal(new[] { "nineSeconds", "twoMinutes", "tenMinutes", "fiveHours" }, sorted.Select(r => r.JobName));
        });
    }

    [Fact]
    public void StepAndRetriesColumns_SortByTheirNumbers_NotByText()
    {
        StaTestThread.Run(() =>
        {
            var tab = new JobHistoryTab();
            var rows = new[]
            {
                new ViewerJobHistoryRow { JobName = "step10", StepId = 10, StepName = "a", RetriesAttempted = 10 },
                new ViewerJobHistoryRow { JobName = "step2", StepId = 2, StepName = "b", RetriesAttempted = 2 },
                new ViewerJobHistoryRow { JobName = "step0", StepId = 0, RetriesAttempted = 0 },
            };

            Assert.Equal(new[] { "step0", "step2", "step10" }, Sorted(Column(tab, "Step"), rows).Select(r => r.JobName));
            Assert.Equal(new[] { "step0", "step2", "step10" }, Sorted(Column(tab, "Retries"), rows).Select(r => r.JobName));
        });
    }

    [Fact]
    public void MessageColumn_ShowsTheWholeMessageInATooltip()
    {
        StaTestThread.Run(() =>
        {
            var tab = new JobHistoryTab();
            var column = Column(tab, "Message");
            var longMessage = new string('x', 400) + " the end";
            var cell = new TextBlock { DataContext = new ViewerJobHistoryRow { Message = longMessage } };

            Assert.NotNull(column.ElementStyle);
            cell.Style = column.ElementStyle;

            Assert.Equal(longMessage, cell.ToolTip);
        });
    }
}
