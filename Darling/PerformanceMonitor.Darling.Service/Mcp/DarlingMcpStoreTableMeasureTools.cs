/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// <c>measure_store_table</c> (#8787 part D2): the MEASURED empty space of one store table, the figure
/// <c>get_store_table_health</c> can only estimate. The read, the table-name checks and the verdict rule are
/// <see cref="DarlingStoreTableMeasureReader"/>; this wraps it as text for MCP and the web.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpStoreTableMeasureTools
{
    [McpServerTool(Name = "measure_store_table"), Description(
        "Measures the empty space of ONE table in the monitoring STORE (not a monitored server's), by adding up the size of its live rows "
        + "against the space of its file pages and counting which pages hold a live row: a measurement, where get_store_table_health gives an estimate. One call, one table, never a sweep. "
        + "Returns a one-line verdict: dense, mostly empty and only a rewrite returns the space, or mostly empty and VACUUM can truncate the tail. "
        + "<<GUIDE>> table is schema-qualified (for example collect.store_metrics). Accepted: an ordinary table in the collect or config "
        + "schema, a partition, or a chunk of a product hypertable. A hypertable or partitioned parent is refused with a sentence naming its "
        + "largest chunks or partitions to measure instead, and so is a view, a name outside the product schemas, an unknown name and a heap "
        + "over 20 GB. It scans the table once under a 60 second limit and says so when the limit trips. It adds up the stored size of each column "
        + "but does not read large stored values (TOAST), and returns no row data. Result fields: total_pages, pages_with_rows, empty_pages, empty_share "
        + "(pages with no live row over all pages), live_bytes (what the live rows take in pages, without column padding, so free space reads slightly high), "
        + "free_bytes and free_share (the space the live rows leave free, after the table's fillfactor reserve, which is kept free on purpose), fillfactor, rows, "
        + "rows_per_used_page, last_used_block against last_file_block (the pages after the last used block are all VACUUM can return), "
        + "trailing_empty_pages, the occupancy histogram (pages holding 1, 2 to 4, 5 to 10 and over 10 rows), vacuum_shrink_disabled, "
        + "verdict_kind (dense, vacuum_can_truncate, rewrite_needed, no_pages) and verdict. Mostly empty means the live rows leave at least half "
        + "the page space free. A rewrite is called for only when at least 64 MiB is free as well. A page holding only dead rows counts as empty, "
        + "because a VACUUM frees it. Nothing is written.")]
    public static async Task<string> MeasureStoreTable(
        NpgsqlDataSource postgres,
        [Description("The table, schema-qualified, for example collect.store_metrics.")] string? table = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await DarlingStoreTableMeasureReader.MeasureAsync(postgres, table, cancellationToken);
            switch (result.Outcome)
            {
                case DarlingStoreTableMeasureReader.StoreTableMeasureOutcome.Refused:
                    return McpHelpers.Refusal("table", result.Sentence!);
                case DarlingStoreTableMeasureReader.StoreTableMeasureOutcome.TimedOut:
                    return McpHelpers.Status("timed_out", result.Sentence!, new { timeout_seconds = DarlingStoreTableMeasureReader.TimeoutSeconds });
            }

            var m = result.Measure!;
            return JsonSerializer.Serialize(new
            {
                verdict = m.Verdict,
                verdict_kind = m.VerdictKind,
                table = m.QualifiedName,
                total_pages = m.TotalPages,
                pages_with_rows = m.PagesWithRows,
                empty_pages = m.EmptyPages,
                empty_share = Math.Round(m.EmptyShare, 4),
                live_bytes = m.LiveBytes,
                free_bytes = m.FreeBytes,
                free_share = Math.Round(m.FreeShare, 4),
                fillfactor = m.Fillfactor,
                rows = m.Rows,
                rows_per_used_page = m.RowsPerUsedPage,
                last_used_block = m.LastUsedBlock,
                last_file_block = m.LastFileBlock,
                trailing_empty_pages = m.TrailingEmptyPages,
                vacuum_shrink_disabled = m.VacuumTruncateDisabled,
                occupancy = new
                {
                    pages_with_1_row = m.PagesWith1Row,
                    pages_with_2_to_4_rows = m.PagesWith2To4Rows,
                    pages_with_5_to_10_rows = m.PagesWith5To10Rows,
                    pages_with_over_10_rows = m.PagesWithOver10Rows,
                },
                elapsed_ms = m.ElapsedMilliseconds,
            }, McpHelpers.JsonOptions);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return McpHelpers.FormatError("measure_store_table", ex);
        }
    }
}
