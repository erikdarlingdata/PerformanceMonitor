/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Analysis;

/// <summary>
/// Lite's twin of Darling's <c>PgPileupSnapshotReader</c>: reads the same-statement-pileup
/// evaluation window from <c>v_query_snapshots</c> — the newest snapshot instants plus the baseline
/// lookback behind them — and projects the rows the shared
/// <see cref="SameStatementPileupDetector"/> evaluates (#3467). Lite's active-query snapshots feed
/// the same seam as Darling's (one collector definition, one table shape), which is what makes the
/// finding shippable on both SKUs from one detector.
///
/// <para>One table, on purpose: the trigger must be computable where the Query Store readers are
/// disabled (#2296), so this read names <c>v_query_snapshots</c> alone — pinned from source by
/// <c>SameStatementPileupSourceCensusTests</c>. Plan-XML columns are deliberately not projected
/// (they expand ~30× on read — the ParquetCompaction lesson) and the text is capped: the identity
/// key is <c>query_hash</c>, the text serves only the null-hash surrogate and the drill-down
/// preview. Row filters mirror the long-running-query alert's read of this table (user sessions,
/// no CDC capture); the text/wait noise filters live in the shared detector.</para>
/// </summary>
public class PileupSnapshotReader
{
    /// <summary>The characters of a statement the detector keeps (the bound the read's <c>LEFT()</c> applied until #5320).</summary>
    internal const int StatementTextCharacters = 1500;

    /// <summary>The first <see cref="StatementTextCharacters"/> text elements of <paramref name="text"/>, unjudged: the cut
    /// DuckDB's <c>LEFT</c> over the text column made, for the identity surrogate and the noise test only (#5320).</summary>
    internal static string RawCut(string text) =>
        text.Length <= StatementTextCharacters ? text : text[..McpHelpers.TextElementCutLength(text, StatementTextCharacters)];

    /// <summary>
    /// The row cap of one window read, 50,000: about 900 qualifying sessions a snapshot across the 55-minute window the
    /// sweep reads at a one-minute cadence, past what a busy server sustains and well short of what a runaway snapshot
    /// would cost in statement text (2,000 blocked sessions a minute over that window is 110,000 rows). A tripwire, not a
    /// working bound: <see cref="ReadWindowAsync"/> tells a read that filled it short of the baseline floor from one that
    /// merely has that many rows (#8157). Darling's <c>PgPileupSnapshotReader</c> carries the same cap.
    /// </summary>
    public const int WindowRowCap = 50_000;

    private readonly DuckDbInitializer _duckDb;
    private readonly ILogger? _logger;
    private readonly int _rowCap;

    public PileupSnapshotReader(DuckDbInitializer duckDb, ILogger? logger = null, int rowCap = WindowRowCap)
    {
        _duckDb = duckDb;
        _logger = logger;
        _rowCap = rowCap;
    }

    /// <summary>
    /// The evaluation window's rows, newest first, or an empty list on a read fault — the pileup
    /// sweep is a per-collection best-effort evaluation; a failed read costs one log line and this
    /// cycle, never the collection loop. The row cap (<see cref="WindowRowCap"/>) is a tripwire against a
    /// runaway session count, not a working bound: a read that fills it short of the baseline floor the
    /// finding would state returns the empty list too, with one log line, rather than hand the detector a
    /// baseline shorter than the one its finding describes (#8157).
    /// </summary>
    public async Task<List<SameStatementPileupDetector.SnapshotRow>> ReadWindowAsync(
        int serverId, DateTime windowFloorUtc, CancellationToken cancellationToken)
    {
        var rows = new List<SameStatementPileupDetector.SnapshotRow>();
        try
        {
            /* #7256: this read runs on every collection with a window of minutes. It reads the hot table alone when the Parquet
               archive of query_snapshots holds nothing that new, instead of opening every archive file. */
            var capReached = false;
            rows.AddRange(await new LocalDataService(_duckDb).ReadWindowSkippingEmptyArchiveAsync(
                "query_snapshots", "collection_time", windowFloorUtc, ReadAsync, cancellationToken));

            if (capReached && !SameStatementPileupDetector.ReachesBaselineFloor(rows))
            {
                /* #8157: the cap tripped short of the baseline floor the finding would state. The detector would describe
                   a baseline it never read, so this sweep evaluates nothing; the next sweep re-reads. */
                _logger?.LogWarning(
                    "[PileupSnapshotReader] pileup window read for server {ServerId} filled its {RowCap}-row cap and reached back only to {OldestUtc:u}, short of the {Minutes}-minute baseline the finding would state — this sweep's evaluation is skipped",
                    serverId, _rowCap, rows.Min(r => r.CollectionTime), SameStatementPileupDetector.BaselineLookbackMinutes);
                rows.Clear();
            }

            async Task<List<SameStatementPileupDetector.SnapshotRow>> ReadAsync(string relation)
            {
            var window = new List<SameStatementPileupDetector.SnapshotRow>();
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = $@"
SELECT collection_time,
       session_id,
       query_hash,
       query_text,
       database_name,
       status,
       wait_type,
       wait_time_ms,
       total_elapsed_time_ms,
       cpu_time_ms,
       logical_reads,
       reads
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   session_id > 50
AND   COALESCE(is_cdc_capture, FALSE) = FALSE
AND   {PerformanceMonitor.Common.MonitorApplicationNames.StoreNotMonitorProgramNamePredicate}
ORDER BY collection_time DESC
LIMIT $3";
            cmd.CommandText = cmd.CommandText.Replace("v_query_snapshots", relation, StringComparison.Ordinal);
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = windowFloorUtc });
            cmd.Parameters.Add(new DuckDBParameter { Value = _rowCap });

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var rawText = reader.IsDBNull(3) ? null : reader.GetString(3);
                window.Add(new SameStatementPileupDetector.SnapshotRow(
                    CollectionTime: reader.GetDateTime(0),
                    SessionId: reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                    QueryHash: reader.IsDBNull(2) ? null : reader.GetString(2),
                    /* #5320: the raw cut feeds only the identity surrogate and the noise filter, exactly as the SQL cut
                       did; the text the finding prints is judged whole, then cut (PreviewText). */
                    QueryText: rawText is null ? null : RawCut(rawText),
                    DatabaseName: reader.IsDBNull(4) ? null : reader.GetString(4),
                    Status: reader.IsDBNull(5) ? null : reader.GetString(5),
                    WaitType: reader.IsDBNull(6) ? null : reader.GetString(6),
                    WaitTimeMs: reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7)),
                    ElapsedMs: reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                    CpuTimeMs: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                    LogicalReads: reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                    PhysicalReads: reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                    PreviewText: McpHelpers.StatementPreview(rawText, StatementTextCharacters)));
            }
            if (window.Count >= _rowCap)
            {
                capReached = true;
            }

            return window;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Shutdown — quiet and expected; the empty list ends this cycle's evaluation. */
            rows.Clear();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(
                "[PileupSnapshotReader] pileup window read failed for server {ServerId} — this cycle's evaluation is skipped, the next cycle re-reads: {Message}",
                serverId, ex.Message);
            rows.Clear();
        }

        return rows;
    }
}
