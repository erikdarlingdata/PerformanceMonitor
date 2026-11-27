/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json;

namespace Darling.Tests;

/// <summary>
/// The buffer count a live test bounds from <c>EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)</c> (#8570).
/// <para>The root node's total is not that count. It also holds every catalog buffer the backend read to rebuild a cache entry
/// (a chunk's relation, an index's descriptor, TimescaleDB's chunk list), and those are charged to whichever node touches the
/// relation first, which is the ChunkAppend or one of its subplans. A catalog cache reset brings them all back, and another
/// test class creating or dropping a database on the same server causes one at any moment. The same plan over 300 chunks
/// counted 176 buffers on a warm backend and 4,402 after a reset, and a CI run counted 4,430 against a bound of 300, while the
/// pages of the tables and indexes the plan read stayed at 108 to 112 in every one of those runs.</para>
/// <para><see cref="DataPages(JsonElement)"/> adds up only the scan nodes that read a table or an index, so the figure is the pages
/// the statement reads and does not move with the backend's cache. A statement that walks every row it should have probed still
/// reads every page of the walk on every run, so the bound keeps catching it.</para>
/// </summary>
internal static class ExplainBuffers
{
    /// <summary>The plan node types whose own buffer counts are the heap and index pages they read. A bitmap heap scan's count already
    /// includes its bitmap index scans, so those are not added again. A parallel scan has the same node type with Parallel Aware set.</summary>
    private static readonly string[] ScanNodeTypes = ["Seq Scan", "Index Scan", "Index Only Scan", "Bitmap Heap Scan", "Tid Scan", "Tid Range Scan"];

    /// <summary>The data pages (shared hits plus reads) of the plan in <paramref name="explainJson"/>, the one-column result of
    /// <c>EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)</c>.</summary>
    public static long DataPages(object? explainJson)
    {
        using var document = JsonDocument.Parse(Convert.ToString(explainJson, CultureInfo.InvariantCulture)!);
        return DataPages(document.RootElement[0].GetProperty("Plan"));
    }

    /// <summary>The data pages (shared hits plus reads) of the scan nodes at and under <paramref name="plan"/>, the <c>Plan</c> property of the
    /// explain document.</summary>
    public static long DataPages(JsonElement plan)
    {
        var pages = 0L;
        if (plan.TryGetProperty("Node Type", out var type) && Array.IndexOf(ScanNodeTypes, type.GetString()) >= 0)
        {
            pages += Blocks(plan, "Shared Hit Blocks") + Blocks(plan, "Shared Read Blocks");
        }

        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                pages += DataPages(child);
            }
        }

        return pages;
    }

    /// <summary>The rows the scan nodes over <paramref name="relations"/> returned (actual rows times loops), summed over the plan in
    /// <paramref name="explainJson"/>, taken at the printed value. A compressed chunk's decompress node carries the uncompressed chunk's
    /// name, its compressed child does not, so a chunk is counted once. Rows, unlike pages, mean the same at every table size: a per-row
    /// index scan of an uncompressed chunk counts a buffer access per row, and a compressed read counts a few.
    /// <para>PostgreSQL 17 and earlier print <c>Actual Rows</c> as the per-loop average rounded to a whole number, so a node with several
    /// loops can stand half a row a loop away from what it returned (#9809). This figure is exact only where every loop returns the same
    /// whole number of rows, as a lookup that returns one row does. A bound check uses <see cref="RowsReadAtLeast"/> or
    /// <see cref="RowsReadAtMost"/> instead.</para></summary>
    public static long RowsRead(object? explainJson, ISet<string> relations) => RowsRead(explainJson, relations, Bound.Printed);

    /// <summary>The fewest rows the plan can have read and still print what it printed: the figure for a "reads no more than N" check. The
    /// printed value of a node is the per-loop average rounded to the digits it shows (whole rows on PostgreSQL 17 and earlier, hundredths
    /// on 18), so the node's total sits within half a unit of the last digit, per loop, of the printed value times its loops. The
    /// allowance applies only where it can matter, a node with more than one loop and a rounded value: a single loop prints its total, and
    /// the hundredths of 18 stay inside one row for up to a hundred loops, so the bound is as tight there as an exact count (#9809).</summary>
    public static long RowsReadAtLeast(object? explainJson, ISet<string> relations) => RowsRead(explainJson, relations, Bound.Least);

    /// <summary>The most rows the plan can have read and still print what it printed: the figure for a "reads at least N" check. The same
    /// allowance as <see cref="RowsReadAtLeast"/>, in the other direction (#9809).</summary>
    public static long RowsReadAtMost(object? explainJson, ISet<string> relations) => RowsRead(explainJson, relations, Bound.Most);

    private enum Bound
    {
        Printed,
        Least,
        Most,
    }

    private static long RowsRead(object? explainJson, ISet<string> relations, Bound bound)
    {
        using var document = JsonDocument.Parse(Convert.ToString(explainJson, CultureInfo.InvariantCulture)!);
        return RowsRead(document.RootElement[0].GetProperty("Plan"), relations, bound);
    }

    private static long RowsRead(JsonElement plan, ISet<string> relations, Bound bound)
    {
        var rows = 0L;
        if (plan.TryGetProperty("Relation Name", out var name) && relations.Contains(name.GetString() ?? string.Empty)
            && plan.TryGetProperty("Actual Rows", out var actual))
        {
            var loops = plan.TryGetProperty("Actual Loops", out var l) ? l.GetInt64() : 1L;
            rows += NodeRows(actual, loops, bound);
        }

        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                rows += RowsRead(child, relations, bound);
            }
        }

        return rows;
    }

    /// <summary>One node's rows. The rounding allowance is half of the last digit the number prints (read from the number's own text, so no
    /// server version is needed), taken per loop.</summary>
    private static long NodeRows(JsonElement actual, long loops, Bound bound)
    {
        var printed = actual.GetDecimal();
        if (bound == Bound.Printed || loops <= 1)
        {
            return (long)Math.Round(printed * loops);
        }

        var raw = actual.GetRawText();
        var point = raw.IndexOf('.', StringComparison.Ordinal);
        var half = 0.5m;
        for (var digits = point < 0 ? 0 : raw.Length - point - 1; digits > 0; digits--)
        {
            half /= 10m;
        }

        return bound == Bound.Least
            ? (long)Math.Ceiling(Math.Max(0m, printed - half) * loops)
            : (long)Math.Floor((printed + half) * loops);
    }

    private static long Blocks(JsonElement node, string name) => node.TryGetProperty(name, out var value) ? value.GetInt64() : 0;
}
