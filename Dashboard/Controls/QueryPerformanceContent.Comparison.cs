/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;

namespace PerformanceMonitorDashboard.Controls
{
    public partial class QueryPerformanceContent : UserControl
    {
        private (DateTime From, DateTime To)? _comparisonRange;

        public void SetComparisonRange((DateTime From, DateTime To)? range)
        {
            _comparisonRange = range;
        }

        public async Task RefreshComparisonAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var currentEnd = _queryStatsToDate ?? DateTime.UtcNow;
                var currentStart = _queryStatsFromDate ?? currentEnd.AddHours(-_queryStatsHoursBack);

                await RefreshQueryStatsComparisonAsync(currentStart, currentEnd);
                await RefreshProcStatsComparisonAsync(currentStart, currentEnd);
                await RefreshQueryStoreComparisonAsync(currentStart, currentEnd);
            }
            catch (Exception ex)
            {
                _statusCallback?.Invoke($"Comparison failed: {ex.Message}");
            }
        }

        private void SetQueryStatsComparisonMode(bool active, (DateTime From, DateTime To)? baselineRange = null)
        {
            QueryStatsDataGrid.Visibility = active ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
            QueryStatsComparisonGrid.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
            QueryStatsComparisonBanner.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;

            if (active && baselineRange.HasValue)
            {
                var from = baselineRange.Value.From.ToString("yyyy-MM-dd HH:mm");
                var to = baselineRange.Value.To.ToString("yyyy-MM-dd HH:mm");
                QueryStatsComparisonBanner.Text = $"Comparing against baseline: {from} \u2192 {to}";
            }
        }

        private async Task RefreshQueryStatsComparisonAsync(DateTime currentStart, DateTime currentEnd)
        {
            if (_comparisonRange == null)
            {
                SetQueryStatsComparisonMode(false);
                return;
            }

            SetQueryStatsComparisonMode(true, _comparisonRange);

            var items = await _databaseService!.GetQueryStatsComparisonAsync(
                currentStart, currentEnd,
                _comparisonRange.Value.From, _comparisonRange.Value.To);

            var sorted = items
                .OrderBy(x => x.SortGroup)
                .ThenByDescending(x => x.SortableDurationDelta)
                .ToList();

            QueryStatsComparisonGrid.ItemsSource = sorted;
        }

        private void SetProcStatsComparisonMode(bool active, (DateTime From, DateTime To)? baselineRange = null)
        {
            ProcStatsDataGrid.Visibility = active ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
            ProcStatsComparisonGrid.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
            ProcStatsComparisonBanner.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;

            if (active && baselineRange.HasValue)
            {
                var from = baselineRange.Value.From.ToString("yyyy-MM-dd HH:mm");
                var to = baselineRange.Value.To.ToString("yyyy-MM-dd HH:mm");
                ProcStatsComparisonBanner.Text = $"Comparing against baseline: {from} \u2192 {to}";
            }
        }

        private async Task RefreshProcStatsComparisonAsync(DateTime currentStart, DateTime currentEnd)
        {
            if (_comparisonRange == null)
            {
                SetProcStatsComparisonMode(false);
                return;
            }

            SetProcStatsComparisonMode(true, _comparisonRange);

            var items = await _databaseService!.GetProcedureStatsComparisonAsync(
                currentStart, currentEnd,
                _comparisonRange.Value.From, _comparisonRange.Value.To);

            var sorted = items
                .OrderBy(x => x.SortGroup)
                .ThenByDescending(x => x.SortableDurationDelta)
                .ToList();

            ProcStatsComparisonGrid.ItemsSource = sorted;
        }

        private void SetQueryStoreComparisonMode(bool active, (DateTime From, DateTime To)? baselineRange = null)
        {
            QueryStoreDataGrid.Visibility = active ? System.Windows.Visibility.Collapsed : System.Windows.Visibility.Visible;
            QueryStoreComparisonGrid.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;
            QueryStoreComparisonBanner.Visibility = active ? System.Windows.Visibility.Visible : System.Windows.Visibility.Collapsed;

            if (active && baselineRange.HasValue)
            {
                var from = baselineRange.Value.From.ToString("yyyy-MM-dd HH:mm");
                var to = baselineRange.Value.To.ToString("yyyy-MM-dd HH:mm");
                QueryStoreComparisonBanner.Text = $"Comparing against baseline: {from} \u2192 {to}";
            }
        }

        private async Task RefreshQueryStoreComparisonAsync(DateTime currentStart, DateTime currentEnd)
        {
            if (_comparisonRange == null)
            {
                SetQueryStoreComparisonMode(false);
                return;
            }

            SetQueryStoreComparisonMode(true, _comparisonRange);

            var items = await _databaseService!.GetQueryStoreComparisonAsync(
                currentStart, currentEnd,
                _comparisonRange.Value.From, _comparisonRange.Value.To);

            var sorted = items
                .OrderBy(x => x.SortGroup)
                .ThenByDescending(x => x.SortableDurationDelta)
                .ToList();

            QueryStoreComparisonGrid.ItemsSource = sorted;
        }
    }
}
