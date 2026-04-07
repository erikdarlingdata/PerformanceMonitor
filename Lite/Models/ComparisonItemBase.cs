/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows.Media;

namespace PerformanceMonitorLite.Models;

public abstract class ComparisonItemBase
{
    public string DatabaseName { get; set; } = "";

    // Current period
    public long ExecutionCount { get; set; }
    public double AvgDurationMs { get; set; }
    public double AvgCpuMs { get; set; }
    public double AvgReads { get; set; }

    // Baseline period
    public long BaselineExecutionCount { get; set; }
    public double BaselineAvgDurationMs { get; set; }
    public double BaselineAvgCpuMs { get; set; }
    public double BaselineAvgReads { get; set; }

    // Flags
    public bool IsNew => ExecutionCount > 0 && BaselineExecutionCount == 0;
    public bool IsGone => ExecutionCount == 0 && BaselineExecutionCount > 0;

    // Delta percentages (null when baseline is zero or item is new/gone)
    public double? DurationDeltaPct => ComputeDelta(AvgDurationMs, BaselineAvgDurationMs);
    public double? CpuDeltaPct => ComputeDelta(AvgCpuMs, BaselineAvgCpuMs);
    public double? ReadsDeltaPct => ComputeDelta(AvgReads, BaselineAvgReads);
    public double? ExecutionDeltaPct => ComputeDelta(ExecutionCount, BaselineExecutionCount);

    // Display helpers for grid binding
    public string DurationDeltaDisplay => FormatDelta(DurationDeltaPct);
    public string CpuDeltaDisplay => FormatDelta(CpuDeltaPct);
    public string ReadsDeltaDisplay => FormatDelta(ReadsDeltaPct);
    public string ExecutionDeltaDisplay => FormatDelta(ExecutionDeltaPct);

    public string StatusBadge => IsNew ? "NEW" : IsGone ? "GONE" : "";

    // Sort key: NEW at top (0), normal by delta (1), GONE at bottom (2)
    public int SortGroup => IsNew ? 0 : IsGone ? 2 : 1;
    public double SortableDurationDelta => DurationDeltaPct ?? (IsNew ? double.MaxValue : double.MinValue);

    // Color brushes for delta columns (red = regression, green = improvement)
    public Brush DurationDeltaBrush => GetDeltaBrush(DurationDeltaPct, 25);
    public Brush CpuDeltaBrush => GetDeltaBrush(CpuDeltaPct, 25);
    public Brush ReadsDeltaBrush => GetDeltaBrush(ReadsDeltaPct, 50, 25);
    public Brush ExecutionDeltaBrush => GetDeltaBrush(ExecutionDeltaPct, 100, 50);

    private static readonly Brush RedBrush = new SolidColorBrush(Color.FromRgb(0xFF, 0x6B, 0x6B));
    private static readonly Brush GreenBrush = new SolidColorBrush(Color.FromRgb(0x4E, 0xC9, 0xB0));
    private static readonly Brush NeutralBrush = Brushes.Transparent;

    static ComparisonItemBase()
    {
        RedBrush.Freeze();
        GreenBrush.Freeze();
    }

    private static Brush GetDeltaBrush(double? delta, double redThreshold, double greenThreshold = 25)
    {
        if (!delta.HasValue) return NeutralBrush;
        if (delta.Value > redThreshold) return RedBrush;
        if (delta.Value < -greenThreshold) return GreenBrush;
        return NeutralBrush;
    }

    private static double? ComputeDelta(double current, double baseline)
    {
        if (baseline == 0) return null;
        return (current - baseline) / baseline * 100.0;
    }

    private static string FormatDelta(double? delta)
    {
        if (!delta.HasValue) return "\u2014";
        var sign = delta.Value >= 0 ? "+" : "";
        return $"{sign}{delta.Value:N1}%";
    }
}
