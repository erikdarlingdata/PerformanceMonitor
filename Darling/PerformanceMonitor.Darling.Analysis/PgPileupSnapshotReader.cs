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
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

/// <summary>
/// Reads the same-statement-pileup evaluation window from <c>query_snapshots</c> — the newest
/// snapshot instants plus the baseline lookback behind them — and projects the rows the shared
/// <see cref="SameStatementPileupDetector"/> evaluates (#3467).
///
/// <para><b>This reader touches exactly one table, on purpose.</b> The pileup finding's trigger must
/// be computable where the Query Store readers are disabled (#2296), so this read names
/// <c>query_snapshots</c> alone — no query_store table, no view over one — and
/// <c>SameStatementPileupSourceCensusTests</c> pins that from source. The projection deliberately
/// skips the row's plan-XML columns (<c>query_plan</c> / <c>live_query_plan</c> expand ~30× on read —
/// the ParquetCompaction lesson) and caps <c>query_text</c>: the detector's identity key is
/// <c>query_hash</c>, the text serves only the null-hash surrogate and the drill-down preview.</para>
///
/// <para>Row filters mirror the long-running-query alert's read of this table: user sessions only
/// (<c>session_id &gt; 50</c>) and no CDC capture jobs; the text/wait noise filters
/// (sp_server_diagnostics, WAITFOR, backups) live in the shared detector so both SKUs get them from
/// one tested implementation. The staleness rule also lives detector-side
/// (<see cref="SameStatementPileupDetector.StaleSnapshotCutoffMinutes"/>) — this read's window floor
/// bounds the scan, not the freshness decision.</para>
/// </summary>
public sealed class PgPileupSnapshotReader
{
    /// <summary>The characters of a statement the detector keeps (the bound the read's <c>LEFT()</c> applied until #5320).</summary>
    internal const int StatementTextCharacters = 1500;

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    /// <summary>
    /// $1 server_id, $2 window floor (naive UTC — <c>collection_time</c> is stamped host-UTC by the
    /// collector runner and stored naive). The text comes back WHOLE (#5320): <see cref="ReadWindowAsync"/> cuts it to
    /// <see cref="StatementTextCharacters"/> code points, the bound identity fallback and the drill-down preview
    /// need, and judges the whole statement with the sensitive-statement filter before the cut that is printed. Bounded output: the window is
    /// <see cref="SameStatementPileupDetector.BaselineLookbackMinutes"/> of a one-minute-cadence
    /// collector, ~50–150 rows on a busy server; the LIMIT is a tripwire against a misbehaving
    /// snapshot (a runaway session count), not a working cap.
    /// </summary>
    public const string PileupWindowSql = @"
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
FROM query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   session_id > 50
AND   COALESCE(is_cdc_capture, FALSE) = FALSE
ORDER BY collection_time DESC
LIMIT 5000";

    public PgPileupSnapshotReader(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres;
        _logger = logger;
    }

    /// <summary>
    /// The evaluation window's rows, newest first, or an empty list on a read fault — the pileup
    /// sweep is a per-collection best-effort evaluation, and a failed read must cost one log line and
    /// this cycle's evaluation, never the sweep (the fact-collector degrade discipline). The next
    /// sweep re-reads a fresher window anyway. Shutdown residue is not a fault: it propagates past
    /// the filter (<see cref="AnalysisShutdown.IsExpectedAbandon"/>, #2299 — the same idiom every
    /// reader in this directory uses) instead of costing a log line for work we ourselves called off.
    /// </summary>
    public async Task<List<SameStatementPileupDetector.SnapshotRow>> ReadWindowAsync(
        int serverId, DateTime windowFloorUtc, CancellationToken cancellationToken)
    {
        var rows = new List<SameStatementPileupDetector.SnapshotRow>();
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            using var command = new NpgsqlCommand(PileupWindowSql, connection)
            {
                CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds,
            };
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(DateTime.SpecifyKind(windowFloorUtc, DateTimeKind.Unspecified));

            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var rawText = reader.IsDBNull(3) ? null : reader.GetString(3);
                rows.Add(new SameStatementPileupDetector.SnapshotRow(
                    CollectionTime: reader.GetDateTime(0),
                    SessionId: reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1)),
                    QueryHash: reader.IsDBNull(2) ? null : reader.GetString(2),
                    /* #5320: the raw cut feeds only the identity surrogate and the noise filter, exactly as the
                       SQL cut did; the text the finding prints is judged whole, then cut (PreviewText). */
                    QueryText: rawText is null ? null : AnalysisStatementText.CutCodePoints(rawText, StatementTextCharacters),
                    DatabaseName: reader.IsDBNull(4) ? null : reader.GetString(4),
                    Status: reader.IsDBNull(5) ? null : reader.GetString(5),
                    WaitType: reader.IsDBNull(6) ? null : reader.GetString(6),
                    WaitTimeMs: reader.IsDBNull(7) ? null : Convert.ToInt64(reader.GetValue(7)),
                    ElapsedMs: reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8)),
                    CpuTimeMs: reader.IsDBNull(9) ? 0L : Convert.ToInt64(reader.GetValue(9)),
                    LogicalReads: reader.IsDBNull(10) ? 0L : Convert.ToInt64(reader.GetValue(10)),
                    PhysicalReads: reader.IsDBNull(11) ? 0L : Convert.ToInt64(reader.GetValue(11)),
                    PreviewText: AnalysisStatementText.Preview(rawText, StatementTextCharacters)));
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, cancellationToken))
        {
            _logger?.LogWarning(
                "[PgPileupSnapshotReader] pileup window read failed for server {ServerId} — this sweep's evaluation is skipped, the next sweep re-reads: {Message}",
                serverId, ex.Message);
            rows.Clear();
        }

        return rows;
    }
}
