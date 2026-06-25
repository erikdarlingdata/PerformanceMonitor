/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls
{
    /// <summary>
    /// Column maxima for the in-grid <see cref="PerformanceMonitor.Ui.BarChartCell"/>s on the
    /// top-queries grid (mirror of Dashboard's QueryPerformanceContent.BarCells). Each bar fills to
    /// <c>value / max</c> for its column; recomputed whenever the grid's ItemsSource changes (initial
    /// load + every filter) via a DependencyPropertyDescriptor so we don't hook each setter. The
    /// cells bind these by RelativeSource to the host ServerTab.
    /// </summary>
    public partial class ServerTab
    {
        public static readonly DependencyProperty BarMaxExecutionsProperty = DependencyProperty.Register(
            nameof(BarMaxExecutions), typeof(double), typeof(ServerTab), new PropertyMetadata(1.0));
        public double BarMaxExecutions
        {
            get => (double)GetValue(BarMaxExecutionsProperty);
            set => SetValue(BarMaxExecutionsProperty, value);
        }

        public static readonly DependencyProperty BarMaxTotalCpuProperty = DependencyProperty.Register(
            nameof(BarMaxTotalCpu), typeof(double), typeof(ServerTab), new PropertyMetadata(1.0));
        public double BarMaxTotalCpu
        {
            get => (double)GetValue(BarMaxTotalCpuProperty);
            set => SetValue(BarMaxTotalCpuProperty, value);
        }

        public static readonly DependencyProperty BarMaxTotalElapsedProperty = DependencyProperty.Register(
            nameof(BarMaxTotalElapsed), typeof(double), typeof(ServerTab), new PropertyMetadata(1.0));
        public double BarMaxTotalElapsed
        {
            get => (double)GetValue(BarMaxTotalElapsedProperty);
            set => SetValue(BarMaxTotalElapsedProperty, value);
        }

        public static readonly DependencyProperty BarMaxTotalReadsProperty = DependencyProperty.Register(
            nameof(BarMaxTotalReads), typeof(double), typeof(ServerTab), new PropertyMetadata(1.0));
        public double BarMaxTotalReads
        {
            get => (double)GetValue(BarMaxTotalReadsProperty);
            set => SetValue(BarMaxTotalReadsProperty, value);
        }

        private void SetupBarCellMaxes()
        {
            var dpd = DependencyPropertyDescriptor.FromProperty(ItemsControl.ItemsSourceProperty, typeof(DataGrid));
            dpd?.AddValueChanged(QueryStatsGrid, (_, _) => RecomputeBarCellMaxes());
        }

        private void RecomputeBarCellMaxes()
        {
            var items = (QueryStatsGrid.ItemsSource as IEnumerable)?.OfType<QueryStatsRow>().ToList();
            if (items == null || items.Count == 0)
            {
                BarMaxExecutions = BarMaxTotalCpu = BarMaxTotalElapsed = BarMaxTotalReads = 1.0;
                if (QueryStatsByDbCards != null) QueryStatsByDbCards.Items = null;
                return;
            }

            // Max(1,...) keeps the bar denominator positive even for an all-zero column.
            BarMaxExecutions = Math.Max(1.0, items.Max(i => (double)i.TotalExecutions));
            BarMaxTotalCpu = Math.Max(1.0, items.Max(i => i.TotalCpuMs));
            BarMaxTotalElapsed = Math.Max(1.0, items.Max(i => i.TotalElapsedMs));
            BarMaxTotalReads = Math.Max(1.0, items.Max(i => (double)i.TotalLogicalReads));

            RefreshByDbCards(items);
        }

        /// <summary>
        /// Builds the per-database CPU bar-cards (A.6) from the currently displayed query rows — no
        /// new query, just an aggregation of what the grid already holds. A database's color is a
        /// STABLE alphabetical index (D4), so it keeps its color no matter how the bars are ranked;
        /// the bars themselves are ordered by value (largest first). Mirror of Dashboard's.
        /// </summary>
        private void RefreshByDbCards(List<QueryStatsRow> items)
        {
            if (QueryStatsByDbCards == null) return;

            var byDb = items
                .GroupBy(i => i.DatabaseName ?? string.Empty)
                .Select(g => new { Db = g.Key, Cpu = g.Sum(x => x.TotalCpuMs) })
                .Where(x => x.Cpu > 0)
                .ToList();

            if (byDb.Count == 0)
            {
                QueryStatsByDbCards.Items = null;
                return;
            }

            var sortedNames = byDb.Select(x => x.Db)
                .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                .ToList();
            double max = byDb.Max(x => x.Cpu);

            QueryStatsByDbCards.Items = byDb
                .OrderByDescending(x => x.Cpu)
                .Select(x => new MetricBarItem
                {
                    Label = string.IsNullOrEmpty(x.Db) ? "(unknown)" : x.Db,
                    Value = x.Cpu,
                    Maximum = max,
                    ValueText = x.Cpu.ToString("N0"),
                    BarBrush = BrushFromHex(ChartPalette.CyclingColor(sortedNames.IndexOf(x.Db))),
                })
                .ToList();
        }

        private static Brush BrushFromHex(string hex)
        {
            var b = new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
            b.Freeze();
            return b;
        }
    }
}
