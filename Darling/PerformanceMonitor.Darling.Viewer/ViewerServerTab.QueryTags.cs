/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #5988 part A: the Queries grids' query-tag filter. The choice is held per server tab and handed to each grid's read, which
/// applies it in the SQL before the top-N cut (<see cref="ViewerDataService.ResolveQueryTagFilterAsync"/>). Read only.
/// </summary>
public partial class ViewerServerTab
{
    private string? _queryTagChoice;
    private bool _queryTagChoicesFilling;

    /// <summary>The filter the Queries grids read with: every row when no tag is chosen.</summary>
    private Task<QueryTagFilter> CurrentQueryTagFilterAsync() => _dataService.ResolveQueryTagFilterAsync(_queryTagChoice);

    private async void QueryTagFilterCombo_DropDownOpened(object? sender, EventArgs e)
    {
        try
        {
            var choices = await _dataService.GetQueryTagFilterChoicesAsync();
            _queryTagChoicesFilling = true;
            try
            {
                QueryTagFilterCombo.ItemsSource = choices;
                var current = _queryTagChoice ?? ViewerDataService.AllTagsChoice;
                QueryTagFilterCombo.SelectedItem = choices.Contains(current) ? current : ViewerDataService.AllTagsChoice;
            }
            finally
            {
                _queryTagChoicesFilling = false;
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            System.Diagnostics.Trace.TraceWarning($"#5988 the query tag list did not load: {ex.Message}");
        }
    }

    private async void QueryTagFilterCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (_queryTagChoicesFilling || QueryTagFilterCombo.SelectedItem is not string choice)
        {
            return;
        }

        var next = choice == ViewerDataService.AllTagsChoice ? null : choice;
        if (next == _queryTagChoice)
        {
            return;
        }

        _queryTagChoice = next;
        await RefreshActiveInnerTabAsync();
    }
}
