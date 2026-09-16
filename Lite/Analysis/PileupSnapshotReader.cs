/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Database;

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
    private readonly DuckDbInitializer _duckDb;
    private readonly ILogger? _logger;

    public PileupSnapshotReader(DuckDbInitializer duckDb, ILogger? logger = null)
    {
        _duckDb = duckDb;
        _logger = logger;
    }

    /// <summary>
    /// The evaluation window's rows, newest first, or an empty list on a read fault — the pileup
    /// sweep is a per-collection best-effort evaluation; a failed read costs one log line and this
    /// cycle, never the collection loop. The LIMIT is a tripwire against a misbehaving snapshot,
    /// not a working cap (the window is ~50–150 rows at the one-minute default cadence).
    /// </summary>
    public async Task<List<SameStatementPileupDetector.SnapshotRow>> ReadWindowAsync(
        int serverId, DateTime windowFloorUtc, CancellationToken cancellationToken)
    {
        var rows = new List<SameStatementPileupDetector.SnapshotRow>();
        try
        {
            using var readLock = _duckDb.AcquireReadLock(cancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT collection_time,
       session_id,
       query_hash,
       LEFT(query_text, 1500) AS query_text,
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
ORDER BY collection_time DESC
LIMIT 5000";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = windowFloorUtc });

            using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                rows.Add(new SameStatementPileupDetector.SnapshotRow(
                    CollectionTime: reader.GetDateTime(0),
                    SessionId: reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                    QueryHash: reader.IsDBNull(2) ? null : reader.GetString(2),
                    QueryText: reader.IsDBNull(3) ? null : reader.GetString(3),
                    DatabaseName: reader.IsDBNull(4) ? null : reader.GetString(4),
                    Status: reader.IsDBNull(5) ? null : reader.GetString(5),
                    WaitType: reader.IsDBNull(6) ? null : reader.GetString(6),
                    WaitTimeMs: reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7)),
                    ElapsedMs: reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                    CpuTimeMs: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                    LogicalReads: reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                    PhysicalReads: reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11))));
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
