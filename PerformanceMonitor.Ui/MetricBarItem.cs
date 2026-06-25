/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using System.Windows.Media;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// One row of a <see cref="MetricBarCards"/> list: a label (e.g. a database name), a value
    /// rendered as a horizontal bar proportional to <see cref="Maximum"/>, the formatted value
    /// text, and the bar color (the entity's identity color via ChartPalette.CyclingColor).
    /// Items are rebuilt on each refresh, so the fill widths are simple computed getters with no
    /// change notification.
    /// </summary>
    public sealed class MetricBarItem
    {
        public string Label { get; set; } = string.Empty;
        public double Value { get; set; }
        public double Maximum { get; set; } = 1.0;
        public string ValueText { get; set; } = string.Empty;
        public Brush BarBrush { get; set; } = Brushes.SteelBlue;

        private double Ratio => Maximum > 0 ? Math.Clamp(Value / Maximum, 0.0, 1.0) : 0.0;

        /// <summary>Star width of the filled portion (proportional to Value/Maximum).</summary>
        public GridLength FillWidth => new GridLength(Ratio, GridUnitType.Star);

        /// <summary>Star width of the remaining track (kept &gt; 0 so the star math never divides by zero).</summary>
        public GridLength RestWidth => new GridLength(Math.Max(1.0 - Ratio, 0.0001), GridUnitType.Star);
    }
}
