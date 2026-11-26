/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The largest value of a <c>bytea</c> expression per group, as one SQL text that every supported store runs (#9802). PostgreSQL has
/// <c>max(bytea)</c> only from 18, and a Linux store runs 16 or 17, where <c>MAX</c> over a <c>bytea</c> fails with "function max(bytea)
/// does not exist". <c>max(anyarray)</c> is in every supported version, and it orders two one-element arrays by their elements with the
/// element type's own comparison, so the first element of the largest one-element array is the value <c>max(bytea)</c> returns. It is
/// built in, so the store needs no object of its own and no role needs a privilege, and like <c>MAX</c> it keeps one value per group and
/// has a combine step, so the aggregate stays partial and parallel where the planner wants it.
///
/// <para>An array orders a NULL element above every value, where <c>MAX</c> skips a NULL input. The <c>NULLIF</c> turns the array of a
/// NULL value into a NULL array, which the aggregate skips, so a group of NULLs, or of no rows, gives NULL as <c>MAX</c> does.</para>
/// </summary>
public static class ByteaMaxSql
{
    /// <summary>
    /// The aggregate over <paramref name="expression"/>, a <c>bytea</c> expression, restricted to the rows that pass
    /// <paramref name="filter"/> (a <c>FILTER (WHERE ...)</c> condition) when one is given. The answer is a <c>bytea</c>.
    /// </summary>
    public static string Of(string expression, string? filter = null) =>
        filter is null ? Before + expression + After : $"({AggregateBefore}{expression}{AggregateAfter} FILTER (WHERE {filter}))[1]";

    private const string AggregateBefore = "MAX(NULLIF(ARRAY[";

    private const string AggregateAfter = "], '{NULL}'::bytea[]))";

    /// <summary>The text before the expression of an unfiltered aggregate, for a statement that is a constant: <c>Before + expression + After</c> is <see cref="Of"/> with no filter.</summary>
    public const string Before = "(" + AggregateBefore;

    /// <summary>The text after the expression of an unfiltered aggregate. See <see cref="Before"/>.</summary>
    public const string After = AggregateAfter + ")[1]";

    /// <summary>
    /// The byte key of the newest text-bearing row of a group of <c>query_stats</c> rows, which the top-queries statements and the hourly
    /// host rollup pick a group's text by (#8136). The key sorts the way the text lookup does: collection time first
    /// (<c>timestamp_send</c> is its microseconds, big-endian, positive for any collection date), then collection id (the row collected
    /// last wins a tie), then the row's text as a flag byte and payload: 0x00 and the inline text when the row carries one, else 0x01 and
    /// the 32-byte digest. Only rows that carry a text or a digest take part. <paramref name="alias"/> is the rows' alias with its dot
    /// (<c>"q."</c>), or empty.
    /// </summary>
    public static string LatestTextKey(string alias) => Of(
        $"timestamp_send({alias}collection_time) || int8send({alias}collection_id)"
        + $" || CASE WHEN {alias}query_text IS NOT NULL THEN '\\x00'::bytea || convert_to({alias}query_text, 'UTF8') ELSE '\\x01'::bytea || {alias}query_text_digest END",
        $"{alias}query_text IS NOT NULL OR {alias}query_text_digest IS NOT NULL");
}
