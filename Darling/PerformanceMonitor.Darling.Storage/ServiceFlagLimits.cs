/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The ranges the three numeric service flags in <c>config.config_service</c> must meet before they are written (#6393): the Query
/// Store text budget (MB), the concurrent server sweeps and the Query Store fetch top N. One definition for every writer, so the
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
}
