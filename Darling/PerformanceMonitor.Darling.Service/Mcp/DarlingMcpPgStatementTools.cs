/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using ModelContextProtocol.Server;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The MCP surface for PostgreSQL query statistics, paired with the <c>pg_statement_stats</c>
/// collector.
/// </summary>
[McpServerToolType]
public sealed class DarlingMcpPgStatementTools
{
    [McpServerTool(Name = "get_pg_top_queries"), Description("Gets top PostgreSQL query shapes by total execution time over the window ending at as_of (default 24h), from pg_stat_statements. Aurora adds an I/O source split, peak memory and WAL bytes; elsewhere null, not zero. query_text is null until hourly capture reaches that queryid (also true right after a major upgrade). queryid is a STRING: JSON doubles silently round a 64-bit value; compare and join as text. limit bounds the page only: pct_of_total_time divides by the WHOLE window's total_exec_time_ms, so shares never sum to 100%. <<GUIDE>> Gets the top PostgreSQL query shapes by total execution time over a time period, wherever pg_stat_statements is installed: on Amazon Aurora the collector reads aurora_stat_statements(), and on any other PostgreSQL the vanilla view, with the Aurora-only columns null rather than zero. On Aurora that includes the I/O source breakdown, which stock PostgreSQL cannot provide: a block 'read' may have come from the distributed storage volume or from the local NVMe Optimized Reads cache, and the two have very different costs. Also reports peak memory per statement (Aurora only), the closest PostgreSQL equivalent of a memory grant, and WAL bytes generated, which has no SQL Server DMV counterpart. Returns query_text for each statement, captured hourly and keyed on queryid, or null when none has been captured yet (a statement first seen minutes ago, or a queryid minted by a major-version upgrade). queryid itself is stable within a major version but changes across a major upgrade — which is exactly why the text is STORED rather than fetched live: after an upgrade the live view no longer holds the old ids, so their text would otherwise be unrecoverable and the history would read as a list of integers. queryid is returned as a STRING, not a number: it is a signed 64-bit value spread over the whole int8 range, so most ids exceed what a JSON number survives and a numeric wire form would be silently rounded by any parser that decodes numbers as IEEE-754 doubles — after which it matches nothing. Compare it and join on it as text. This is a separate tool from get_top_queries_by_cpu, which covers SQL Server. THE PAGE IS BOUNDED BY limit: queries_returned is how many shapes you got, truncated says the window held more, and the rows are the heaviest so the ones past the cap are lighter. SHARES ARE OF THE WINDOW, NOT OF THE PAGE: pct_of_total_time's denominator is total_exec_time_ms, the WHOLE window's execution time across every query shape, computed in the same statement as the rows - so a three-row page does not sum to 100%, and the gap between returned_exec_time_ms (what the page adds up to) and total_exec_time_ms is the work the cap left out; returned_pct_of_total is that ratio stated once.")]
    public static async Task<string> GetPgTopQueries(
        NpgsqlDataSource postgres,
        [Description("Server name or display name.")] string? server_name = null,
        [Description("Hours of history to analyze. Default 24.")] int hours_back = 24,
        [Description("Maximum query shapes to return, heaviest first. Default 20. Bounds the page: read truncated to know whether more exist; shares stay of the whole window regardless.")] int limit = 20,
        [Description(McpHelpers.AsOfDescription)] string? as_of = null)
    {
        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, server_name);
        if (error != null) return error;

        var validation = McpHelpers.ValidateWindow(hours_back, as_of, out var windowEnd);
        if (validation != null) return validation;
        validation = McpHelpers.ValidateTop(limit);
        if (validation != null) return validation;

        try
        {
            var now = windowEnd;
            /* #3541 A7 (and the A3 shape it sat on): the caller's limit + 1 as the fetch, the extra row as the
               observed truncation signal. The reader carried LIMIT 50 as a literal under a limit the tool
               accepts up to 1,000, and the tool's Take(limit) on top of it meant a share was computed over
               whichever of the two caps bit - never over the window. */
            var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(
                postgres, resolved.ServerId, now.AddHours(-hours_back), now, limit + 1);

            if (page.Rows.Count == 0)
            {
                /* The capability answer settles the dialect branch and the stock-PostgreSQL branch
                   whenever the store knows the engine (#2532), so this sentence is reached in exactly two
                   states: an Aurora target that produced no rows, which has one genuinely diagnosable
                   cause, and a row whose engine_kind is NULL, where no claim can be made. It names those
                   two rather than repeating the ones that can no longer reach this line. */
                return await DarlingEngineCapability.NotCollectedStatusAsync(
                    postgres, resolved.ServerId, resolved.ServerName, "pg_statement_stats")
                    /* #2546: the sentence below tells the reader to go and CHECK whether pg_stat_statements
                       is installed in the connected database. The collector has already checked — a 42P01
                       against a database where the extension was never created classifies as a non-fatal
                       skip and stores the CREATE EXTENSION to run. Asking for it here is the whole reason
                       the precondition vocabulary exists on the PostgreSQL side (#2545): the extension is
                       mutable at runtime, so nothing decided at connect time could report it and then stop
                       reporting it when somebody acts on the advice. */
                    ?? await DarlingRuntimePrecondition.StatusAsync(
                        postgres, resolved.ServerId, resolved.ServerName, "pg_statement_stats")
                    ?? McpHelpers.Status(
                        "unavailable",
                        "No PostgreSQL query statistics for this server and window. Check that "
                        + "pg_stat_statements is installed in the database the collector connects to — it is "
                        + "not installed by default, and on some clusters it exists only in the application "
                        + "database, not in postgres. Also expect an empty window from a SINGLE snapshot: the "
                        + "counters here are per-interval deltas, so the first collection after a restart has "
                        + "nothing to difference against and the window fills on the second. Otherwise the "
                        + "store has not recorded this server's engine yet, and a target it cannot classify may "
                        + "not be a PostgreSQL one at all — check list_servers.");
            }

            return BuildTopQueriesJson(resolved.ServerName, hours_back, page, limit);
        }
        catch (Exception ex)
        {
            /* #2554: a THROW is a miss too, and until now it was the one miss the capability answer could
               not reach. The gate sat inside `if (rows.Count == 0)`, so it only ever spoke when the query
               SUCCEEDED and returned nothing — and a caller on a target that excludes pg_statement_stats got
               a raw SQL error where its sibling get_pg_wait_stats gives the honest "does not run on that
               engine, and never will". (That caller was on stock PostgreSQL until #2625, which gave the
               collector a vanilla pg_stat_statements path; the gate now answers the DIALECT exclusion, and a
               stock-PostgreSQL fault correctly reads as a fault.)

               Consulted HERE rather than moved ahead of the read, which is what it looks like it should be.
               DarlingEngineCapability's contract is explicit that every call site asks AFTER its read came
               back empty, never before: a server whose registry row says one engine while its collected rows
               say another — a re-registration, a restored database — must still get its DATA rather than a
               confident explanation of why it cannot have any. Asking first would trade this defect for that
               one, across every read. On the throw path there is no data to prefer, so the same contract
               points the other way.

               And this stays narrow rather than swallowing failures: NotCollectedStatusAsync returns null
               unless the collector provably cannot run on this server's engine, so a transient error against
               an Aurora target still surfaces as an error. */
            var gated = await DarlingEngineCapability.NotCollectedStatusAsync(
                postgres, resolved.ServerId, resolved.ServerName, "pg_statement_stats");
            if (gated != null)
            {
                return gated;
            }

            return McpHelpers.FormatError("get_pg_top_queries", ex);
        }
    }

    /// <summary>
    /// The response body, split out from the tool so the WIRE SHAPE can be asserted directly (#2548). The
    /// tool itself needs a live store and a resolved server, neither of which a serialization guard has any
    /// business standing up — and a guard that re-implements the projection instead would keep passing while
    /// the shipped one drifted underneath it.
    ///
    /// <para><b>The denominator is the window's, not the page's</b> (#3541 A7). <paramref name="page"/> carries
    /// <c>WindowTotalExecTimeMs</c> from the same statement as its rows, and every <c>pct_of_total_time</c> divides
    /// by THAT. The previous shape divided by the sum of the rows fetched, so at <c>limit = 3</c> the three
    /// shares summed to 100% and read as "these three are everything" to any agent holding only the JSON.
    /// The page's own sum still travels, under a name that says what it is (<c>returned_exec_time_ms</c>), and
    /// the ratio of the two is stated once as <c>returned_pct_of_total</c> — the honest form of "the top N
    /// account for X%".</para>
    ///
    /// <para><paramref name="page"/> holds up to <c>limit + 1</c> rows; the extra one is the truncation signal
    /// and is cut before the projection. The window total is unaffected by the cut by construction.</para>
    /// </summary>
    internal static string BuildTopQueriesJson(
        string serverName,
        int hoursBack,
        DarlingPgStatementReader.PgTopQueriesPage page,
        int limit)
    {
        var truncated = page.Rows.Count > limit;
        var rows = truncated ? page.Rows.Take(limit).ToList() : page.Rows;

        var windowTotalMs = page.WindowTotalExecTimeMs;
        var returnedMs = rows.Sum(r => r.TotalExecTimeMs);

        var result = rows.Select(r =>
        {
            /* Null when the source did not report the split at all (self-hosted PostgreSQL, #2625), which
               is a different answer from zero and must not become a 0% cache-hit ratio. */
            var storageAndCache = r.StorageBlocksRead + r.OrcacheBlocksHit;
            return new
            {
                /* #2548: a STRING, not a number. queryid is a signed int8 whose values are spread over the
                   whole 64-bit range, so most of them are past 2^53 and every parser that decodes JSON
                   numbers as IEEE-754 doubles — JSON.parse, json.loads, most agent tooling — silently
                   rounds one. That is not the same failure as a rounded metric: queryid is the ONLY
                   identity a PostgreSQL statement has, and every use of it (SELECT … WHERE queryid = …,
                   matching this row against the operator's own screen, quoting it in a ticket) is an
                   equality join, which a rounded key loses entirely rather than approximates. A string
                   costs a type change once; a number costs the value on every read. It also matches how
                   SQL Server's query_hash already reaches this surface — as text, never as an integer.
                   database_id stays a number beside it because an oid is unsigned 32-bit and so cannot
                   reach the rounding range. Invariant culture, because a negative id must render with
                   ASCII '-' whatever the host's locale would prefer. */
                queryid = r.QueryId.ToString(CultureInfo.InvariantCulture),
                database_id = r.DatabaseId,
                calls = r.Calls,
                total_exec_time_ms = r.TotalExecTimeMs,
                avg_exec_time_ms = r.Calls > 0 ? Math.Round((double)r.TotalExecTimeMs / r.Calls, 3) : 0,
                max_exec_time_ms = Math.Round(r.MaxExecTimeMs, 3),
                rows_returned = r.RowsReturned,
                /* Of the WINDOW's total, never of the page's — see the remarks. */
                pct_of_total_time = windowTotalMs > 0 ? Math.Round((double)r.TotalExecTimeMs / windowTotalMs * 100, 1) : 0,
                /* Aurora's I/O split, which is the point of using aurora_stat_statements over the
                   vanilla view. A high orcache share means the reads were cheap local NVMe hits; a
                   high storage share means network round trips to the cluster volume. The community
                   cache-hit ratio cannot distinguish these and so overstates the cost of one and
                   understates the other. */
                shared_blks_hit = r.SharedBlocksHit,
                shared_blks_read = r.SharedBlocksRead,
                storage_blks_read = r.StorageBlocksRead,
                orcache_blks_hit = r.OrcacheBlocksHit,
                orcache_hit_pct_of_reads = storageAndCache > 0
                    ? Math.Round((double)r.OrcacheBlocksHit!.Value / storageAndCache.Value * 100, 1)
                    : (double?)null,
                /* Spills. temp blocks are sort/hash spill to disk, NOT temporary tables - those are
                   the local_blks_* family and a different problem. */
                temp_blks_read = r.TempBlocksRead,
                temp_blks_written = r.TempBlocksWritten,
                wal_bytes = r.WalBytes,
                max_exec_peakmem_bytes = r.MaxPeakMemBytes,
                // #2219: the statement text, or null when none has been captured for this queryid yet.
                // Null is honest rather than a placeholder — text refreshes hourly, so a statement first
                // seen minutes ago has none, and after a major-version upgrade re-keys queryid the new ids
                // have none until the next refresh. An empty string would read as "the query is blank".
                query_text = r.QueryText,
            };
        })
        .ToList();

        return JsonSerializer.Serialize(new
        {
            server = serverName,
            hours_back = hoursBack,
            /* #3541 A3 dialect: the page described as a page. No time bounds — the rows are per-shape
               aggregates over the whole window, so there is no page reach to report, only a cap. */
            queries_returned = result.Count,
            truncated,
            order = "total_exec_time_ms_desc",
            /* The WINDOW's execution time, across every shape that ran — the denominator of every
               pct_of_total_time below. It is NOT the sum of the rows; that is returned_exec_time_ms. */
            total_exec_time_ms = windowTotalMs,
            returned_exec_time_ms = returnedMs,
            returned_pct_of_total = windowTotalMs > 0 ? Math.Round((double)returnedMs / windowTotalMs * 100, 1) : 0,
            /* Every counter here covers the window, so a caller can safely divide one by another.
               Only the two high-water marks are not counters, and saying which is cheaper than
               letting someone assume max_exec_time_ms is a windowed total. */
            note = "All counters cover the requested window: calls, total_exec_time_ms and "
                 + "rows_returned from stored per-interval deltas, and the block and WAL figures "
                 + "differenced across the window's snapshots. max_exec_time_ms and "
                 + "max_exec_peakmem_bytes are high-water marks, not windowed totals. The top-level "
                 + "total_exec_time_ms is the WHOLE window's execution time across every query shape, "
                 + "computed in the same statement as the rows; each row's pct_of_total_time divides by it, "
                 + "so the shares on a page do not sum to 100 unless the page is the whole window "
                 + "(truncated = false). returned_exec_time_ms is what the rows returned add up to.",
            queries = result,
        }, McpHelpers.JsonOptions);
    }
}
