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
    /// <paramref name="explainJson"/>. A compressed chunk's decompress node carries the uncompressed chunk's name, its compressed child does
    /// not, so a chunk is counted once. Rows, unlike pages, mean the same at every table size: a per-row index scan of an uncompressed chunk
    /// counts a buffer access per row, and a compressed read counts a few.</summary>
    public static long RowsRead(object? explainJson, ISet<string> relations)
    {
        using var document = JsonDocument.Parse(Convert.ToString(explainJson, CultureInfo.InvariantCulture)!);
        return RowsRead(document.RootElement[0].GetProperty("Plan"), relations);
    }

    private static long RowsRead(JsonElement plan, ISet<string> relations)
    {
        var rows = 0L;
        if (plan.TryGetProperty("Relation Name", out var name) && relations.Contains(name.GetString() ?? string.Empty)
            && plan.TryGetProperty("Actual Rows", out var actual))
        {
            var loops = plan.TryGetProperty("Actual Loops", out var l) ? l.GetInt64() : 1L;
            rows += (long)Math.Round(actual.GetDouble() * loops);
        }

        if (plan.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray())
            {
                rows += RowsRead(child, relations);
            }
        }

        return rows;
    }

    private static long Blocks(JsonElement node, string name) => node.TryGetProperty(name, out var value) ? value.GetInt64() : 0;
}
