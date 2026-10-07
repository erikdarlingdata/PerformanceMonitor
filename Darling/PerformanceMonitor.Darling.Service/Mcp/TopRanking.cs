/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #5226: what the web's Top Queries and Top Procedures lists rank by. The desktop ranks both by duration; the
/// web ranked by CPU only, so a long query that used little CPU (blocked, or waiting on I/O) never appeared in
/// the browser. <see cref="Cpu"/> is the default and is today's ranking: the same metric, with ties and NULL sums
/// now put in a fixed order (see <see cref="TopRankings"/>).
/// </summary>
public enum TopRanking
{
    /// <summary>Summed <c>delta_worker_time</c> — the tools' promise, and what every caller that names no choice gets.</summary>
    Cpu = 0,

    /// <summary>Summed <c>delta_elapsed_time</c> — the desktop's ranking.</summary>
    Duration = 1,

    /// <summary>Summed <c>delta_logical_reads</c>. Raw tier only: the hourly rollups carry no per-query or per-procedure reads.</summary>
    Reads = 2,

    /// <summary>Summed <c>delta_execution_count</c>.</summary>
    Executions = 3,
}

/// <summary>
/// The whitelist behind <see cref="TopRanking"/>: the wire spelling, and the FIXED metric each choice maps to for
/// each of the five top-N statements. PostgreSQL cannot parameterize a sort expression, so the choice is spliced
/// as text, and the text comes from this file only: a request value is parsed to the enum by
/// <see cref="TryParse"/> (an unknown spelling is refused, never carried along) and the enum picks a literal here.
/// Request text never reaches SQL.
///
/// <para>The five statements stay public consts, so the suite can pin their dialect and columns without a live
/// store. Each carries the anchor <see cref="RankAnchor"/> exactly once, in its ranking pass's select list as
/// <c>$RANK$ AS rank_metric</c>; every <c>ORDER BY</c> in the statement sorts on the <c>rank_metric</c> and
/// <c>rank_cpu</c> aliases and is written out in the const, with <c>NULLS LAST</c> and the group key at its end. So
/// the ranking choice is the ONE thing <see cref="Apply"/> varies, and the ordering rules (NULLs last, a total
/// order) are the same text for every choice and cannot be forgotten by one of them. <see cref="Apply"/> throws
/// when the anchor is missing or appears more than once, so a future edit to a const cannot turn a ranking choice
/// into a silent no-op, and it also expands the CPU default: a const is never run as it stands.</para>
/// </summary>
public static class TopRankings
{
    /// <summary>The accepted spellings, for the refusal message.</summary>
    public const string Accepted = "cpu, duration, reads or executions";

    /// <summary>
    /// The one anchor each top-N const carries, replaced by the chosen ranking's sum (<c>SUM(delta_logical_reads)</c>,
    /// <c>SUM(elapsed_time_sum)</c>, and so on). It sits in the select list of the pass that ranks, as
    /// <c>$RANK$ AS rank_metric</c>, and nowhere else.
    /// </summary>
    public const string RankAnchor = "$RANK$";

    /// <summary>
    /// Parses the wire value. Absent or blank is the default (<see cref="TopRanking.Cpu"/>); the match is
    /// case-insensitive. Returns false for anything else, so a caller refuses it rather than ranking by a guess.
    /// </summary>
    public static bool TryParse(string? value, out TopRanking ranking)
    {
        ranking = TopRanking.Cpu;
        if (string.IsNullOrWhiteSpace(value))
        {
            return true;
        }

        switch (value.Trim().ToLowerInvariant())
        {
            case "cpu":
                return true;
            case "duration":
                ranking = TopRanking.Duration;
                return true;
            case "reads":
                ranking = TopRanking.Reads;
                return true;
            case "executions":
                ranking = TopRanking.Executions;
                return true;
            default:
                return false;
        }
    }

    /// <summary>The wire spelling of a ranking.</summary>
    public static string WireName(TopRanking ranking) => ranking switch
    {
        TopRanking.Duration => "duration",
        TopRanking.Reads => "reads",
        TopRanking.Executions => "executions",
        _ => "cpu",
    };

    /// <summary>
    /// True when the hourly rollups carry the column the ranking sorts on. They keep worker time, elapsed time and
    /// execution counts per bucket and no logical reads (<c>s_stitchColumnsByLegacy</c> in <c>TimescaleSupport.cs</c>),
    /// so <see cref="TopRanking.Reads"/> is answered from the raw tier, the way the parallelism filter is.
    /// </summary>
    public static bool HourlyCarries(TopRanking ranking) => ranking != TopRanking.Reads;

    /// <summary>
    /// Expands one of the top-N consts for the chosen ranking: replaces its single <see cref="RankAnchor"/> with
    /// the ranking's sum. <paramref name="hourly"/> names the shape: the raw tables sum <c>delta_*</c> columns, the
    /// hourly rollups sum <c>*_sum</c> ones. The CPU choice is expanded too (to the worker-time sum), so every read
    /// goes through here and no const is run unexpanded. Throws <see cref="InvalidOperationException"/> when the
    /// anchor is absent or present more than once, and for a reads ranking on the hourly shape (it has no column).
    /// </summary>
    public static string Apply(string statement, TopRanking ranking, bool hourly)
    {
        if (hourly && !HourlyCarries(ranking))
        {
            throw new InvalidOperationException($"The hourly rollups carry no column to rank by {WireName(ranking)}; the caller must read raw.");
        }

        return ReplaceOnce(statement, RankAnchor, hourly ? HourlyMetric(ranking) : RawMetric(ranking));
    }

    private static string RawMetric(TopRanking ranking) => ranking switch
    {
        TopRanking.Cpu => "SUM(delta_worker_time)",
        TopRanking.Duration => "SUM(delta_elapsed_time)",
        TopRanking.Reads => "SUM(delta_logical_reads)",
        TopRanking.Executions => "SUM(delta_execution_count)",
        _ => throw new ArgumentOutOfRangeException(nameof(ranking)),
    };

    private static string HourlyMetric(TopRanking ranking) => ranking switch
    {
        TopRanking.Cpu => "SUM(worker_time_sum)",
        TopRanking.Duration => "SUM(elapsed_time_sum)",
        TopRanking.Executions => "SUM(execution_count_sum)",
        _ => throw new ArgumentOutOfRangeException(nameof(ranking)),
    };

    private static string ReplaceOnce(string sql, string find, string replacement)
    {
        var at = sql.IndexOf(find, StringComparison.Ordinal);
        if (at < 0 || sql.IndexOf(find, at + find.Length, StringComparison.Ordinal) >= 0)
        {
            throw new InvalidOperationException($"Expected exactly one '{find}' in the top-N statement (#5226); the ranking swap would not land.");
        }

        return string.Concat(sql.AsSpan(0, at), replacement, sql.AsSpan(at + find.Length));
    }

    /// <summary>The choices the page offers, in the order it lists them. Wire value and label.</summary>
    public static IReadOnlyList<(string Value, string Label)> Choices { get; } = new[]
    {
        ("cpu", "CPU"),
        ("duration", "Duration"),
        ("reads", "Reads"),
        ("executions", "Executions"),
    };
}
