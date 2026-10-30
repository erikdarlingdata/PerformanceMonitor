/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Formats baseline context from anomaly fact metadata into a structured dictionary
/// (deviation, ratio, baseline mean/stddev, time bucket, tier, samples).
/// <para>
/// Lifted from the per-app <c>Mcp/McpAnalysisTools.cs</c> (<c>ToolRecommendations</c>)
/// into shared analysis so both the MCP output and the shared
/// <c>AnalysisNotificationService</c> finding formatter can call one copy (plan §4.4).
/// </para>
/// </summary>
public static class BaselineContextFormatter
{
    private static readonly string[] DayNames = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

    /// <summary>
    /// Formats baseline context from anomaly fact metadata into a structured dictionary
    /// (deviation, ratio, baseline mean/stddev, time bucket, tier, samples). Used by MCP
    /// output and by the analysis notification path's finding formatter.
    /// <paramref name="plainWording"/> (#6925) renders the two deviations as "6.1 times its usual variation"
    /// for text a person reads (the notification path); the default keeps the compact "6.1σ" that MCP clients parse.
    /// </summary>
    public static Dictionary<string, object>? FormatBaselineContext(Dictionary<string, double> metadata, bool plainWording = false)
    {
        var result = new Dictionary<string, object>();

        if (metadata.TryGetValue("deviation_sigma", out var sigma))
            result["deviation"] = plainWording ? $"{sigma:F1} times its usual variation" : $"{sigma:F1}σ";

        /* #3653 (A8): the window MEAN's deviation beside the peak's, so the MCP baseline_context and the
           notification formatter carry the pair the gate now fires on. Absent on a pre-pair fact. */
        if (metadata.TryGetValue("mean_deviation_sigma", out var meanSigma))
            result["window_mean_deviation"] = plainWording ? $"{meanSigma:F1} times its usual variation" : $"{meanSigma:F1}σ";

        if (metadata.TryGetValue("ratio", out var ratio))
            result["ratio"] = $"{ratio:F1}x";

        if (metadata.TryGetValue("baseline_mean", out var mean))
            result["baseline_mean"] = Math.Round(mean, 2);

        if (metadata.TryGetValue("baseline_mean_ms", out var meanMs))
            result["baseline_mean"] = Math.Round(meanMs, 2);

        if (metadata.TryGetValue("baseline_stddev", out var stddev))
            result["baseline_stddev"] = Math.Round(stddev, 2);

        if (metadata.TryGetValue("baseline_hour", out var hour) &&
            metadata.TryGetValue("baseline_dow", out var dow))
        {
            var dowIdx = (int)dow;
            var dayName = dowIdx >= 0 && dowIdx < DayNames.Length ? DayNames[dowIdx] : "?";
            result["bucket"] = hour >= 0 ? $"{dayName} {(int)hour:00}:00" : "flat";
        }

        if (metadata.TryGetValue("baseline_tier", out var tier))
            result["tier"] = tier switch { 0 => "full", 1 => "hour_only", _ => "flat" };

        if (metadata.TryGetValue("baseline_samples", out var samples))
            result["baseline_samples"] = (int)samples;

        return result.Count > 0 ? result : null;
    }
}
