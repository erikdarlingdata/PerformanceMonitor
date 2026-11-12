/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;
using PerformanceMonitor.Common;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Maps a <see cref="HealthSeverity"/> to Lite's frozen severity palette — the same four colours the
/// Darling viewer uses, so the two apps' AG tabs are visually identical. Exists so pre-banded models can live in
/// <c>PerformanceMonitor.Common</c> (which cannot reference WPF) while the view layer still paints them: the
/// model carries the VERDICT, the converter carries the colour.
/// </summary>
public sealed class SeverityBrushConverter : IValueConverter
{
    private static readonly SolidColorBrush s_critical = Frozen("#E57373");
    private static readonly SolidColorBrush s_warning = Frozen("#FFD54F");
    private static readonly SolidColorBrush s_healthy = Frozen("#81C784");
    private static readonly SolidColorBrush s_unknown = Frozen("#888888");

    private static SolidColorBrush Frozen(string hex)
    {
        var brush = new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
        brush.Freeze();
        return brush;
    }

    /// <summary>The palette lookup, also callable directly from code-behind. On Light and Cool Breeze each band takes the theme's own
    /// status brush (#8221): the pale Dark tones read at 1.4:1 to 2.5:1 as card text there. The card is bound again after a theme change.</summary>
    public static SolidColorBrush BrushFor(HealthSeverity severity) => severity switch
    {
        HealthSeverity.Critical => ThemedBandBrush.ForLightTheme("#E57373") ?? s_critical,
        HealthSeverity.Warning => ThemedBandBrush.ForLightTheme("#FFD54F") ?? s_warning,
        HealthSeverity.Healthy => ThemedBandBrush.ForLightTheme("#81C784") ?? s_healthy,
        _ => ThemedBandBrush.ForLightTheme("#888888") ?? s_unknown,
    };

    public object Convert(object value, Type targetType, object parameter, CultureInfo culture) =>
        BrushFor(value is HealthSeverity severity ? severity : HealthSeverity.Unknown);

    public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture) =>
        throw new NotSupportedException("SeverityBrushConverter is one-way: a brush cannot be turned back into a verdict.");
}
