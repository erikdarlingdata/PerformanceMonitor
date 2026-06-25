/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System;
using System.Globalization;
using System.Windows.Data;
using System.Windows.Media;

namespace PerformanceMonitorDashboard.Converters
{
    /// <summary>
    /// Shared threshold brushes used across converters
    /// </summary>
    internal static class ThresholdBrushes
    {
        internal static readonly SolidColorBrush DarkRed = new(Color.FromRgb(92, 38, 38));
        internal static readonly SolidColorBrush DarkOrange = new(Color.FromRgb(92, 58, 38));
        internal static readonly SolidColorBrush DarkYellow = new(Color.FromRgb(74, 74, 38));
        internal static readonly SolidColorBrush DarkBlue = new(Color.FromRgb(38, 58, 92));

        static ThresholdBrushes()
        {
            // Freeze brushes for performance and thread safety
            DarkRed.Freeze();
            DarkOrange.Freeze();
            DarkYellow.Freeze();
            DarkBlue.Freeze();
        }
    }

    /// <summary>
    /// Converts numeric values to colored brushes based on thresholds
    /// </summary>
    public class ValueToBrushConverter : IValueConverter
    {
        public Brush? LowBrush { get; set; }
        public Brush? MediumBrush { get; set; }
        public Brush? HighBrush { get; set; }
        public Brush? DefaultBrush { get; set; }

        public double LowThreshold { get; set; }
        public double HighThreshold { get; set; }

        public object? Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null || value == DBNull.Value)
                return DefaultBrush ?? Brushes.Transparent;

            if (!double.TryParse(value.ToString(), out double numValue))
                return DefaultBrush ?? Brushes.Transparent;

            if (numValue >= HighThreshold)
                return HighBrush ?? ThresholdBrushes.DarkRed;
            if (numValue >= LowThreshold)
                return MediumBrush ?? ThresholdBrushes.DarkYellow;

            return LowBrush ?? DefaultBrush ?? Brushes.Transparent;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts status strings to colored brushes
    /// </summary>
    public class StatusToBrushConverter : IValueConverter
    {
        public object? Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null)
                return Brushes.Transparent;

            string status = value.ToString()?.ToUpperInvariant() ?? "";

            return status switch
            {
                "ERROR" or "FAILING" => ThresholdBrushes.DarkRed,
                "STALE" or "WARNING" => ThresholdBrushes.DarkYellow,
                "HEALTHY" or "OK" => Brushes.Transparent,
                _ => Brushes.Transparent
            };
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts latency values to colored brushes (milliseconds)
    /// </summary>
    public class LatencyToBrushConverter : IValueConverter
    {
        public object? Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null || value == DBNull.Value)
                return Brushes.Transparent;

            if (!double.TryParse(value.ToString(), out double latencyMs))
                return Brushes.Transparent;

            // High latency threshold: 20ms+
            if (latencyMs >= 20.0)
                return ThresholdBrushes.DarkRed;

            // Medium latency threshold: 10-20ms
            if (latencyMs >= 10.0)
                return ThresholdBrushes.DarkYellow;

            return Brushes.Transparent;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts CPU percentage to colored brushes
    /// </summary>
    public class CpuToBrushConverter : IValueConverter
    {
        public object? Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null || value == DBNull.Value)
                return Brushes.Transparent;

            if (!double.TryParse(value.ToString(), out double cpuPercent))
                return Brushes.Transparent;

            // High CPU threshold: 80%+
            if (cpuPercent >= 80.0)
                return ThresholdBrushes.DarkRed;

            // Medium CPU threshold: 60-80%
            if (cpuPercent >= 60.0)
                return ThresholdBrushes.DarkOrange;

            return Brushes.Transparent;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts blocking duration (seconds) to colored brushes
    /// </summary>
    public class BlockingDurationToBrushConverter : IValueConverter
    {
        public object? Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null || value == DBNull.Value)
                return Brushes.Transparent;

            if (!double.TryParse(value.ToString(), out double durationSec))
                return Brushes.Transparent;

            // Severe blocking: 60+ seconds
            if (durationSec >= 60.0)
                return ThresholdBrushes.DarkRed;

            // Moderate blocking: 10-60 seconds
            if (durationSec >= 10.0)
                return ThresholdBrushes.DarkOrange;

            // Minor blocking: 1-10 seconds
            if (durationSec >= 1.0)
                return ThresholdBrushes.DarkYellow;

            return Brushes.Transparent;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts issue severity text to colored brushes (CRITICAL, HIGH, MEDIUM, LOW)
    /// </summary>
    public class IssueTextToBrushConverter : IValueConverter
    {
        public object? Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null)
                return Brushes.Transparent;

            string issue = value.ToString()?.ToUpperInvariant() ?? "";

            return issue switch
            {
                "CRITICAL" => ThresholdBrushes.DarkBlue,
                "HIGH" => ThresholdBrushes.DarkOrange,
                "MEDIUM" => ThresholdBrushes.DarkYellow,
                "LOW" => Brushes.Transparent,
                _ => Brushes.Transparent
            };
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    /// <summary>
    /// Converts pressure level text to colored brushes (CRITICAL, HIGH, MEDIUM, LOW, NONE)
    /// </summary>
    public class PressureLevelToBrushConverter : IValueConverter
    {
        public object? Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value == null)
                return Brushes.Transparent;

            string level = value.ToString()?.ToUpperInvariant() ?? "";

            return level switch
            {
                "CRITICAL" => ThresholdBrushes.DarkRed,
                "HIGH" => ThresholdBrushes.DarkOrange,
                "MEDIUM" => ThresholdBrushes.DarkYellow,
                "LOW" or "NONE" => Brushes.Transparent,
                _ => Brushes.Transparent
            };
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

}
