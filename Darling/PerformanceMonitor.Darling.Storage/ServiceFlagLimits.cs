/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The ranges the numeric service flags in <c>config.config_service</c> must meet before they are written (#6393): the Query
/// Store text budget (MB), the concurrent server sweeps and the Query Store fetch top N, and (#9638) the plan content retention days,
/// the compose statement timeout and the plan XML compression mode. One definition for every writer, so the
/// Viewer's Settings window and the <c>update_service_flags</c> MCP tool refuse the same values in the same words, and the service's
/// clamp-on-read (<c>StoreConfigProvider</c>) uses the same bounds. Pure. Each method returns the refusal text, or null when the value
/// is fine.
/// </summary>
public static class ServiceFlagLimits
{
    /// <summary>The smallest Query Store text budget, in MB.</summary>
    public const int TextBudgetMbMin = 4;

    /// <summary>The largest Query Store text budget, in MB.</summary>
    public const int TextBudgetMbMax = 256;

    /// <summary>The fewest server sweeps that run at once.</summary>
    public const int ConcurrentSweepsMin = 1;

    /// <summary>The most server sweeps that run at once.</summary>
    public const int ConcurrentSweepsMax = 16;

    /// <summary>The smallest Query Store fetch top N. 0 means every new plan.</summary>
    public const int FetchTopNMin = 0;

    /// <summary>The largest Query Store fetch top N the service honours.</summary>
    public const int FetchTopNMax = 100_000;

    /// <summary>The smallest plan content retention that is on, in days. 0 turns the horizon off.</summary>
    public const int PlanContentRetentionDaysMin = 7;

    /// <summary>The largest plan content retention, in days.</summary>
    public const int PlanContentRetentionDaysMax = 365;

    /// <summary>The shortest compose statement timeout, in seconds.</summary>
    public const int ComposeStatementTimeoutSecondsMin = 5;

    /// <summary>The longest compose statement timeout, in seconds.</summary>
    public const int ComposeStatementTimeoutSecondsMax = 600;

    /// <summary>The compose statement timeout a stored 0 or negative value reads as, in seconds.</summary>
    public const int ComposeStatementTimeoutSecondsDefault = 60;

    /// <summary>The plan XML compression mode that stores plans as gzip bytes.</summary>
    public const string PlanXmlCompressionGzip = "gzip";

    /// <summary>The plan XML compression mode that stores plans as plain text.</summary>
    public const string PlanXmlCompressionNone = "none";

    /// <summary>The refusal text for a Query Store text budget outside its range (also what the Settings window says for text that is not a number).</summary>
    public static readonly string TextBudgetMbMessage = $"Query Store text budget must be between {TextBudgetMbMin} and {TextBudgetMbMax} MB.";

    /// <summary>The refusal text for a concurrent-sweeps value outside its range.</summary>
    public static readonly string ConcurrentSweepsMessage = $"Concurrent server sweeps must be between {ConcurrentSweepsMin} and {ConcurrentSweepsMax}.";

    /// <summary>The refusal text for a Query Store fetch top N outside its range. 0 is valid: it fetches every new plan.</summary>
    public static readonly string FetchTopNMessage = $"Query Store fetch top N must be between {FetchTopNMin} and {FetchTopNMax} (0 fetches every new plan).";

    /// <summary>The refusal for a Query Store text budget outside its range, or null.</summary>
    public static string? TextBudgetMbError(int megabytes) =>
        megabytes is >= TextBudgetMbMin and <= TextBudgetMbMax ? null : TextBudgetMbMessage;

    /// <summary>The refusal for a concurrent-sweeps value outside its range, or null.</summary>
    public static string? ConcurrentSweepsError(int sweeps) =>
        sweeps is >= ConcurrentSweepsMin and <= ConcurrentSweepsMax ? null : ConcurrentSweepsMessage;

    /// <summary>The refusal for a Query Store fetch top N outside its range, or null.</summary>
    public static string? FetchTopNError(int topN) =>
        topN is >= FetchTopNMin and <= FetchTopNMax ? null : FetchTopNMessage;

    /// <summary>The refusal text for a plan content retention outside its range. 0 is valid: it turns the horizon off.</summary>
    public static readonly string PlanContentRetentionDaysMessage = $"Plan content retention must be 0 (off) or between {PlanContentRetentionDaysMin} and {PlanContentRetentionDaysMax} days.";

    /// <summary>The refusal text for a compose statement timeout outside its range.</summary>
    public static readonly string ComposeStatementTimeoutSecondsMessage = $"Compose statement timeout must be between {ComposeStatementTimeoutSecondsMin} and {ComposeStatementTimeoutSecondsMax} seconds.";

    /// <summary>The refusal text for a plan XML compression mode that is neither of the two the store accepts.</summary>
    public static readonly string PlanXmlCompressionMessage = $"Plan XML compression must be '{PlanXmlCompressionGzip}' or '{PlanXmlCompressionNone}'.";

    /// <summary>The refusal for a plan content retention outside its range, or null. 0 (off) passes.</summary>
    public static string? PlanContentRetentionDaysError(int days) =>
        days == 0 || days is >= PlanContentRetentionDaysMin and <= PlanContentRetentionDaysMax ? null : PlanContentRetentionDaysMessage;

    /// <summary>The refusal for a compose statement timeout outside its range, or null.</summary>
    public static string? ComposeStatementTimeoutSecondsError(int seconds) =>
        seconds is >= ComposeStatementTimeoutSecondsMin and <= ComposeStatementTimeoutSecondsMax ? null : ComposeStatementTimeoutSecondsMessage;

    /// <summary>
    /// The plan content retention the service uses for a stored value: 0 or less is off, anything else is held to the range. The one copy
    /// the service, the Viewer's Settings window and the store readers all call.
    /// </summary>
    public static int ClampPlanContentRetentionDays(int value) =>
        value <= 0 ? 0 : Math.Clamp(value, PlanContentRetentionDaysMin, PlanContentRetentionDaysMax);

    /// <summary>
    /// The compose statement timeout the service uses for a stored value: 0 or less is the default, anything else is held to the range.
    /// The one copy the service, the Viewer's Settings window and the Viewer's compose reads all call.
    /// </summary>
    public static int ClampComposeStatementTimeoutSeconds(int value) =>
        Math.Clamp(value <= 0 ? ComposeStatementTimeoutSecondsDefault : value, ComposeStatementTimeoutSecondsMin, ComposeStatementTimeoutSecondsMax);

    /// <summary>The refusal for a plan XML compression mode the store's CHECK constraint would reject, or null. Case and edge spaces are ignored.</summary>
    public static string? PlanXmlCompressionError(string? mode) =>
        NormalizePlanXmlCompression(mode) is null ? PlanXmlCompressionMessage : null;

    /// <summary>The stored spelling of a plan XML compression mode (<c>gzip</c> or <c>none</c>), or null when the text is neither.</summary>
    public static string? NormalizePlanXmlCompression(string? mode) =>
        string.Equals(mode?.Trim(), PlanXmlCompressionGzip, StringComparison.OrdinalIgnoreCase) ? PlanXmlCompressionGzip
        : string.Equals(mode?.Trim(), PlanXmlCompressionNone, StringComparison.OrdinalIgnoreCase) ? PlanXmlCompressionNone
        : null;
}
