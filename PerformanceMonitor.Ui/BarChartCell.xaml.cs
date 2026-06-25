/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// In-grid sparkline cell: a right-aligned value over a thin proportional fill bar.
    /// The bar length is <c>Value / Maximum</c>, where <see cref="Maximum"/> is the column max
    /// supplied by the host (so every bar in a column is relative to that column's largest value).
    /// The fill is rendered with star-sized grid columns, so it scales with the cell width with no
    /// width math or OS ProgressBar chrome. When <see cref="IsActiveSort"/> is true the bar uses
    /// <see cref="AccentBrush"/> instead of <see cref="BarBrush"/>. Shared by both apps (lives in
    /// .Ui) so the in-grid bars can't drift. Ported from PerformanceStudio's BarChartCell.
    /// </summary>
    public partial class BarChartCell : UserControl, INotifyPropertyChanged
    {
        public BarChartCell()
        {
            InitializeComponent();
        }

        /// <summary>The numeric value this cell represents (drives the bar length and, by default, the text).</summary>
        public static readonly DependencyProperty ValueProperty = DependencyProperty.Register(
            nameof(Value), typeof(double), typeof(BarChartCell),
            new PropertyMetadata(0.0, OnFillChanged));
        public double Value
        {
            get => (double)GetValue(ValueProperty);
            set => SetValue(ValueProperty, value);
        }

        /// <summary>The column maximum: the bar fills to <c>Value / Maximum</c>. Supplied by the host.</summary>
        public static readonly DependencyProperty MaximumProperty = DependencyProperty.Register(
            nameof(Maximum), typeof(double), typeof(BarChartCell),
            new PropertyMetadata(1.0, OnFillChanged));
        public double Maximum
        {
            get => (double)GetValue(MaximumProperty);
            set => SetValue(MaximumProperty, value);
        }

        /// <summary>The text shown above the bar (host-formatted so units/precision stay the host's call).</summary>
        public static readonly DependencyProperty TextProperty = DependencyProperty.Register(
            nameof(Text), typeof(string), typeof(BarChartCell),
            new PropertyMetadata(string.Empty));
        public string Text
        {
            get => (string)GetValue(TextProperty);
            set => SetValue(TextProperty, value);
        }

        /// <summary>When true, the bar uses <see cref="AccentBrush"/> (this column is the active sort).</summary>
        public static readonly DependencyProperty IsActiveSortProperty = DependencyProperty.Register(
            nameof(IsActiveSort), typeof(bool), typeof(BarChartCell),
            new PropertyMetadata(false, OnBrushChanged));
        public bool IsActiveSort
        {
            get => (bool)GetValue(IsActiveSortProperty);
            set => SetValue(IsActiveSortProperty, value);
        }

        /// <summary>The normal bar color.</summary>
        public static readonly DependencyProperty BarBrushProperty = DependencyProperty.Register(
            nameof(BarBrush), typeof(Brush), typeof(BarChartCell),
            new PropertyMetadata(MakeFrozen("#5B8DB8"), OnBrushChanged));
        public Brush BarBrush
        {
            get => (Brush)GetValue(BarBrushProperty);
            set => SetValue(BarBrushProperty, value);
        }

        /// <summary>The bar color used when this column is the active sort.</summary>
        public static readonly DependencyProperty AccentBrushProperty = DependencyProperty.Register(
            nameof(AccentBrush), typeof(Brush), typeof(BarChartCell),
            new PropertyMetadata(MakeFrozen("#4FC3F7"), OnBrushChanged));
        public Brush AccentBrush
        {
            get => (Brush)GetValue(AccentBrushProperty);
            set => SetValue(AccentBrushProperty, value);
        }

        /// <summary>The unfilled-track color behind the bar (default transparent).</summary>
        public static readonly DependencyProperty TrackBrushProperty = DependencyProperty.Register(
            nameof(TrackBrush), typeof(Brush), typeof(BarChartCell),
            new PropertyMetadata(Brushes.Transparent));
        public Brush TrackBrush
        {
            get => (Brush)GetValue(TrackBrushProperty);
            set => SetValue(TrackBrushProperty, value);
        }

        // ── Computed, INPC-bound layout/color ──────────────────────────────────────────────
        public GridLength FillWidth { get; private set; } = new GridLength(0, GridUnitType.Star);
        public GridLength RestWidth { get; private set; } = new GridLength(1, GridUnitType.Star);
        public Brush EffectiveBarBrush => IsActiveSort ? AccentBrush : BarBrush;

        private static void OnFillChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
            => ((BarChartCell)d).RecomputeFill();

        private static void OnBrushChanged(DependencyObject d, DependencyPropertyChangedEventArgs e)
            => ((BarChartCell)d).OnPropertyChanged(nameof(EffectiveBarBrush));

        private void RecomputeFill()
        {
            double ratio = Maximum > 0 ? Math.Clamp(Value / Maximum, 0.0, 1.0) : 0.0;
            FillWidth = new GridLength(ratio, GridUnitType.Star);
            // Keep a sliver of "rest" even at full so the star math never collapses to 0/0.
            RestWidth = new GridLength(Math.Max(1.0 - ratio, 0.0001), GridUnitType.Star);
            OnPropertyChanged(nameof(FillWidth));
            OnPropertyChanged(nameof(RestWidth));
        }

        private static Brush MakeFrozen(string hex)
        {
            var b = new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
            b.Freeze();
            return b;
        }

        public event PropertyChangedEventHandler? PropertyChanged;
        private void OnPropertyChanged(string name)
            => PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
    }
}
