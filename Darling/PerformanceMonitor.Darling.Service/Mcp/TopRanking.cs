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
/// the browser. <see cref="Cpu"/> is the default and is today's behaviour, byte for byte.
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
/// The whitelist behind <see cref="TopRanking"/>: the wire spelling, and the FIXED <c>ORDER BY</c> text each choice
/// maps to for each of the five top-N statements. PostgreSQL cannot parameterize <c>ORDER BY</c>, so the choice is
/// spliced as text, and the text comes from this file only: a request value is parsed to the enum by
/// <see cref="TryParse"/> (an unknown spelling is refused, never carried along) and the enum picks a literal here.
/// Request text never reaches SQL.
///
/// <para>The five statements stay public consts, so the suite can still pin their dialect and columns without a
/// live store, and each const still spells the CPU ranking. <see cref="Apply"/> swaps that CPU <c>ORDER BY</c> for
/// the chosen one, and throws when the text it expects is not exactly where it expects it, so a future edit to a
/// const cannot turn a ranking choice into a silent no-op.</para>
/// </summary>
public static class TopRankings
{
    /// <summary>The accepted spellings, for the refusal message.</summary>
    public const string Accepted = "cpu, duration, reads or executions";

    private const string CpuRawOrder = "ORDER BY SUM(delta_worker_time) DESC";
    private const string CpuHourlyOrder = "ORDER BY SUM(worker_time_sum) DESC";
    private const string CpuOuterOrder = "ORDER BY r.total_cpu_us DESC";

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
    /// Swaps the CPU <c>ORDER BY</c> in one of the top-N consts for the chosen ranking's. <paramref name="hourly"/>
    /// names the shape: the raw tables sum <c>delta_*</c> columns, the hourly rollups sum <c>*_sum</c> ones. Ties on
    /// the chosen metric fall back to CPU so the page is stable; <see cref="TopRanking.Cpu"/> returns the const
    /// unchanged, so the default read is byte-identical to before.
    /// </summary>
    public static string Apply(string cpuSql, TopRanking ranking, bool hourly)
    {
        if (ranking == TopRanking.Cpu)
        {
            return cpuSql;
        }

        if (hourly && !HourlyCarries(ranking))
        {
            throw new InvalidOperationException($"The hourly rollups carry no column to rank by {WireName(ranking)}; the caller must read raw.");
        }

        var inner = hourly ? HourlyOrder(ranking) : RawOrder(ranking);
        var sql = ReplaceOnce(cpuSql, hourly ? CpuHourlyOrder : CpuRawOrder, inner);
        /* The statements that re-rank an over-fetched page (the queries) carry a second, outer ORDER BY on the
           output column; the single-level procedures statement does not. */
        return cpuSql.Contains(CpuOuterOrder, StringComparison.Ordinal)
            ? ReplaceOnce(sql, CpuOuterOrder, OuterOrder(ranking))
            : sql;
    }

    private static string RawOrder(TopRanking ranking) => ranking switch
    {
        TopRanking.Duration => "ORDER BY SUM(delta_elapsed_time) DESC, SUM(delta_worker_time) DESC",
        TopRanking.Reads => "ORDER BY SUM(delta_logical_reads) DESC, SUM(delta_worker_time) DESC",
        TopRanking.Executions => "ORDER BY SUM(delta_execution_count) DESC, SUM(delta_worker_time) DESC",
        _ => throw new ArgumentOutOfRangeException(nameof(ranking)),
    };

    private static string HourlyOrder(TopRanking ranking) => ranking switch
    {
        TopRanking.Duration => "ORDER BY SUM(elapsed_time_sum) DESC, SUM(worker_time_sum) DESC",
        TopRanking.Executions => "ORDER BY SUM(execution_count_sum) DESC, SUM(worker_time_sum) DESC",
        _ => throw new ArgumentOutOfRangeException(nameof(ranking)),
    };

    private static string OuterOrder(TopRanking ranking) => ranking switch
    {
        TopRanking.Duration => "ORDER BY r.total_elapsed_us DESC, r.total_cpu_us DESC",
        TopRanking.Reads => "ORDER BY r.total_reads DESC, r.total_cpu_us DESC",
        TopRanking.Executions => "ORDER BY r.total_executions DESC, r.total_cpu_us DESC",
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
