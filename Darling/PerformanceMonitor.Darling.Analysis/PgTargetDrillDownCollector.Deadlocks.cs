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
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetDrillDownCollector
{
    /// <summary>The drill-down section a deadlock-rooted finding carries: one object, never a bare list.</summary>
    internal const string DeadlockExemplarsSection = "pg_deadlock_exemplars";

    /// <summary>
    /// How many shapes the drill-down carries, most-recurrent first. Three, because the measured population
    /// (§B7 of the #3691 calibration: 50 clusters over 14 days, 7 with any captured report, 1–4 reports each,
    /// every graph distinct) never had more than four reports on one server in a fortnight; a fourth shape is
    /// <c>get_pg_deadlocks</c>' to show, and <c>distinct_shapes</c> says how many were not shown.
    /// </summary>
    internal const int DeadlockExemplarCap = 3;

    /// <summary>
    /// The line bound on a carried graph. A PostgreSQL deadlock DETAIL block is two lines per participant
    /// ("Process A waits for … blocked by process B." and "Process A: &lt;statement&gt;") — 24 lines holds a
    /// twelve-way cycle whole, and every graph in the measured population was two-participant (four to six
    /// lines). Past the bound the graph is cut at a line boundary and <c>graph_text_truncated</c> says so.
    /// </summary>
    internal const int GraphTextLineCap = 24;

    /// <summary>
    /// The character bound applied IN THE READ (<c>LEFT(graph_text, $n)</c>) before the line bound is applied
    /// in memory, so a graph whose statements are each a kilobyte long is not shipped whole to be cut here.
    /// Twice lane 7's <see cref="StatementTextCap"/>: a two-participant graph carries two statements.
    /// </summary>
    internal const int GraphTextCharCap = 2 * StatementTextCap;

    /// <summary>
    /// The prose fingerprint of a victim statement — whitespace collapsed, the first
    /// <see cref="VictimFingerprintCap"/> characters, an ellipsis when cut — the thing the advice sentence
    /// names ("with the victim <c>UPDATE orders SET …</c>"). Not a hash: a hash tells the reader nothing about
    /// the access pattern, which is what the exemplar exists to show. The bounded full text rides beside it.
    /// </summary>
    internal const int VictimFingerprintCap = 120;

    /// <summary>
    /// The anomaly's spelling of the counter delta — <c>PgTargetAnomalyDetector.DetectDeadlockRateAnomalies</c>
    /// stamps it as a literal, the same literal every anomaly detector in the repo and
    /// <c>PgTargetAdvice.Anomaly</c>'s reader use; repeated here by name so the reuse is visible.
    /// </summary>
    internal const string AnomalyCurrentCountKey = "current_count";

    /// <summary>
    /// The captured deadlock reports in the window, grouped into SHAPES and ranked by recurrence.
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the shape cap, <c>$5</c> the
    /// statement text cap, <c>$6</c> the graph text cap.
    ///
    /// <para><b>A shape is <c>(participant_count, lock_modes, resources)</c>, not <c>deadlock_hash</c>.</b>
    /// The hash is <c>PgDeadlockLogParser.HashOf(graph)</c> — SHA-256 over the DETAIL block, pids included —
    /// and exists so the same REPORT re-read from an overlapping log tail is stored once. Two deadlocks of
    /// the same two statements on the same two tables carry different pids and therefore different hashes:
    /// counting distinct hashes counts reports, and would call a shape that fired forty times "forty
    /// one-offs". <c>lock_modes</c> and <c>resources</c> are the parser's sorted, de-duplicated edge labels
    /// with no pid in them, so equal strings are the same cycle shape. <c>count(DISTINCT deadlock_hash)</c>
    /// per shape is the recurrence; <c>count(*)</c> beside it is how many rows the tail re-read left, and the
    /// two differ only on the <c>pg_read_file</c> route.</para>
    ///
    /// <para><b>Windowed on <c>collection_time</c></b>, the indexed chunk-partitioning column and the column
    /// the rate fact's <c>exemplar_count</c> was counted on, so the rows here are the rows that count named
    /// and <c>log_captured</c> in the payload agrees with the card. <c>DarlingPgDeadlockReader</c> windows on
    /// <c>occurred_at</c> for a browsing panel; the drill-down's job is to explain THIS pass's count.</para>
    ///
    /// <para>The exemplar of each shape is its LATEST report (<c>array_agg(… ORDER BY occurred_at DESC NULLS
    /// LAST)</c> — the first element), with its hash so <c>get_pg_deadlock_detail</c> can be pointed at it.
    /// The statement and graph are cut in the read to their caps and the untruncated lengths ride beside them
    /// for the flags. <c>count(*) OVER ()</c> and the two <c>SUM(…) OVER ()</c> put the window totals on every
    /// row of the capped result, so the shape count is known even though only <c>$4</c> shapes return.</para>
    /// </summary>
    public const string PgTargetDeadlockExemplarsSql = @"
WITH shapes AS (
    SELECT
        participant_count,
        lock_modes,
        resources,
        CAST(count(*) AS integer)                        AS rows_captured,
        CAST(count(DISTINCT deadlock_hash) AS integer)   AS reports,
        MIN(occurred_at)                                 AS first_seen,
        MAX(occurred_at)                                 AS last_seen,
        (array_agg(deadlock_hash     ORDER BY occurred_at DESC NULLS LAST, collection_time DESC))[1] AS latest_hash,
        (array_agg(victim_pid        ORDER BY occurred_at DESC NULLS LAST, collection_time DESC))[1] AS latest_victim_pid,
        (array_agg(victim_statement  ORDER BY occurred_at DESC NULLS LAST, collection_time DESC))[1] AS latest_victim_statement,
        (array_agg(graph_text        ORDER BY occurred_at DESC NULLS LAST, collection_time DESC))[1] AS latest_graph_text
    FROM pg_deadlocks
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY participant_count, lock_modes, resources
)
SELECT
    participant_count,
    lock_modes,
    resources,
    rows_captured,
    reports,
    first_seen,
    last_seen,
    latest_hash,
    latest_victim_pid,
    LEFT(latest_victim_statement, $5)                    AS victim_statement,
    length(latest_victim_statement)                      AS victim_statement_length,
    LEFT(latest_graph_text, $6)                          AS graph_text,
    length(latest_graph_text)                            AS graph_text_length,
    CAST(count(*) OVER () AS integer)                    AS distinct_shapes,
    CAST(SUM(reports) OVER () AS integer)                AS total_reports,
    CAST(SUM(rows_captured) OVER () AS integer)          AS total_rows
FROM shapes
ORDER BY reports DESC, last_seen DESC NULLS LAST, participant_count DESC
LIMIT $4";

    /// <summary>
    /// The deadlock exemplar drill-down (lane 16 of #3691), for a finding rooted on
    /// <see cref="PgTargetFactKeys.DeadlockRate"/> or <see cref="PgTargetFactKeys.AnomalyDeadlockRate"/>: what
    /// the log captured, grouped into shapes and ranked by recurrence, beside the counter the card was graded on.
    ///
    /// <para><b>The counter is REUSED from the root fact's metadata, never recomputed.</b> The rate fact stamps
    /// <c>counter_count</c> (the reset-aware counter delta) and <c>exemplar_count</c> (the log rows in the
    /// window); the anomaly stamps <c>current_count</c>. <see cref="AnalysisFinding.RootFactMetadata"/> carries
    /// them to this point, so <c>engine_counted</c> is the number the card already states. <c>log_captured</c>
    /// is the rate fact's own count when it stamped one; when the root is the anomaly (which never reads the
    /// log) or the fact's exemplar read degraded, it is this read's row total and
    /// <c>log_captured_source</c> says which.</para>
    ///
    /// <para><b>Both roots, behind the 0.5 gate like lane 7's.</b> A card's severity is its root's, and the
    /// engine only roots a fact at 0.5 or above, so the gate is satisfied by construction here; what is NOT
    /// automatic is which root. The measured population's deadlock rate never reached 5 per hour — the regular
    /// fact grades at most 0.2 there and never roots — and what fires is the first-occurrence anomaly at the
    /// 1-per-hour bar; a drill-down keyed on <c>PG_DEADLOCK_RATE</c> alone would run for nobody who has the
    /// problem. The read is one indexed range on <c>(server_id, collection_time)</c> over a table that held at
    /// most four rows per server per fortnight, capped at <see cref="DeadlockExemplarCap"/> shapes and
    /// <see cref="GraphTextCharCap"/> characters a graph.</para>
    ///
    /// <para><b>An empty capture is a sentence, never an empty array.</b> The counter is complete and the log
    /// tail is lossy, so "the engine counted 6 and the log captured none" is the ordinary state on a server
    /// whose logging posture does not write deadlock reports (<c>log_lock_waits</c> off, or
    /// <c>log_min_messages</c> above the report's level) or whose log volume out-ran the tail. The setting
    /// VALUES are not stated: lane 2's config snapshot does not read those three names, and naming a value the
    /// pass did not read would be the fabrication the advice rules forbid — the settings are named, and
    /// <c>get_pg_server_config</c> is the read.</para>
    ///
    /// <para>The same summary is folded into the finding's frozen advice: <see cref="FactAdvice.PopulateStoryText"/>
    /// composed the card's prose from facts alone (step 3.5 of the pass), before any drill-down ran, so the
    /// exemplar sentence is appended HERE — <see cref="PgTargetAdvice.WithDeadlockExemplars"/> writes the
    /// words, this method re-freezes them — and the persisted <c>StoryText</c> and the <c>advice</c> in
    /// <c>analyze_server</c>'s payload both carry it. A finding whose <c>StoryText</c> is empty or legacy is
    /// left as it was: the payload section still carries the numbers.</para>
    /// </summary>
    private async partial Task CollectDeadlockExemplarsAsync(AnalysisFinding finding, AnalysisContext context)
    {
        var exemplars = new List<PgTargetDeadlockExemplar>(DeadlockExemplarCap);
        int distinctShapes = 0, totalReports = 0, totalRows = 0;

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);
        using var cmd = new NpgsqlCommand(PgTargetDeadlockExemplarsSql, connection) { CommandTimeout = DarlingAnalysisService.AnalysisCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        cmd.Parameters.AddWithValue(DeadlockExemplarCap);
        cmd.Parameters.AddWithValue(StatementTextCap);
        cmd.Parameters.AddWithValue(GraphTextCharCap);

        using (var reader = await cmd.ExecuteReaderAsync(context.CancellationToken))
        {
            while (await reader.ReadAsync(context.CancellationToken))
            {
                distinctShapes = reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13));
                totalReports = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14));
                totalRows = reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15));

                var victimStatement = reader.IsDBNull(9) ? null : reader.GetString(9);
                var victimLength = reader.IsDBNull(10) ? 0 : Convert.ToInt32(reader.GetValue(10));
                var graphText = reader.IsDBNull(11) ? null : reader.GetString(11);
                var graphLength = reader.IsDBNull(12) ? 0 : Convert.ToInt32(reader.GetValue(12));
                var (boundedGraph, graphLines, graphTruncated) = BoundGraphText(graphText, graphLength);

                exemplars.Add(new PgTargetDeadlockExemplar(
                    Rank: exemplars.Count + 1,
                    ParticipantCount: reader.IsDBNull(0) ? null : Convert.ToInt32(reader.GetValue(0)),
                    LockModes: reader.IsDBNull(1) ? null : reader.GetString(1),
                    Resources: reader.IsDBNull(2) ? null : reader.GetString(2),
                    Reports: reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                    RowsCaptured: reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                    FirstSeen: reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                    LastSeen: reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                    DeadlockHash: reader.IsDBNull(7) ? null : reader.GetString(7),
                    VictimPid: reader.IsDBNull(8) ? null : Convert.ToInt32(reader.GetValue(8)),
                    VictimStatement: victimStatement,
                    VictimStatementMayBeTruncated: victimStatement is not null && victimLength > victimStatement.Length,
                    VictimStatementFingerprint: Fingerprint(victimStatement),
                    GraphText: boundedGraph,
                    GraphTextLinesTotal: graphLines,
                    GraphTextTruncated: graphTruncated));
            }
        }

        /* The counter, reused. The rate fact and the anomaly spell the delta differently; both are the same
           reset-aware difference over the same observed window. */
        var metadata = finding.RootFactMetadata;
        double? engineCounted = null;
        if (metadata is not null)
        {
            if (metadata.TryGetValue(PgTargetScorer.DeadlockCounterCountKey, out var counter)) engineCounted = counter;
            else if (metadata.TryGetValue(AnomalyCurrentCountKey, out var current)) engineCounted = current;
        }

        var factStampedCapture = metadata is not null && metadata.TryGetValue(PgTargetScorer.DeadlockExemplarCountKey, out var stamped) ? (int?)(int)stamped : null;
        var summary = new PgTargetDeadlockExemplarSummary(
            EngineCounted: engineCounted is { } c ? (int)Math.Round(c) : null,
            LogCaptured: factStampedCapture ?? totalRows,
            LogCapturedSource: factStampedCapture is not null
                ? $"{PgTargetFactKeys.DeadlockRate}.{PgTargetScorer.DeadlockExemplarCountKey}"
                : "drill-down read of pg_deadlocks",
            ReportsCaptured: totalReports,
            DistinctShapes: distinctShapes,
            Exemplars: exemplars);

        finding.DrillDown![DeadlockExemplarsSection] = new
        {
            engine_counted = summary.EngineCounted,
            log_captured = summary.LogCaptured,
            log_captured_source = summary.LogCapturedSource,
            /* Distinct deadlock_hash — the tail re-read de-duplicated; equal to log_captured off pg_read_file. */
            reports_captured = summary.ReportsCaptured,
            distinct_shapes = summary.DistinctShapes,
            exemplars_shown = exemplars.Count,
            exemplars = exemplars.Select(e => new
            {
                rank = e.Rank,
                participant_count = e.ParticipantCount,
                lock_modes = e.LockModes,
                resources = e.Resources,
                reports = e.Reports,
                rows_captured = e.RowsCaptured,
                first_seen = e.FirstSeen?.ToString("o", CultureInfo.InvariantCulture),
                last_seen = e.LastSeen?.ToString("o", CultureInfo.InvariantCulture),
                /* The LATEST report of this shape — what get_pg_deadlock_detail takes. */
                deadlock_hash = e.DeadlockHash,
                victim_pid = e.VictimPid,
                victim_statement_fingerprint = e.VictimStatementFingerprint,
                /* Null when the server could not recover the victim's text — never "". */
                victim_statement = e.VictimStatement,
                victim_statement_may_be_truncated = e.VictimStatementMayBeTruncated,
                graph_text = e.GraphText,
                graph_text_lines_total = e.GraphTextLinesTotal,
                graph_text_truncated = e.GraphTextTruncated,
            }).ToList(),
            shape_note = "A shape is (participant_count, lock_modes, resources); deadlock_hash identifies one REPORT (pids included), so recurrence is reports per shape, not distinct hashes.",
            note = PgTargetAdvice.DeadlockExemplarSentence(summary),
        };

        /* Re-freeze the card's prose with the exemplar sentence — see the summary. */
        var frozen = FactAdvice.TryReadStoryText(finding.StoryText);
        if (frozen is not null)
            finding.StoryText = FactAdvice.SerializeForStoryText(PgTargetAdvice.WithDeadlockExemplars(frozen, summary));
    }

    /// <summary>
    /// The graph, cut at <see cref="GraphTextLineCap"/> lines; the total line count and whether either bound
    /// (the read's characters, this method's lines) cut it. Tabs were stripped at capture; line ends are
    /// normalised here because the two log transports differ.
    /// </summary>
    internal static (string? Text, int Lines, bool Truncated) BoundGraphText(string? graphText, int untruncatedLength)
    {
        if (string.IsNullOrEmpty(graphText))
            return (graphText, 0, false);

        var charTruncated = untruncatedLength > graphText.Length;
        var lines = graphText.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        if (lines.Length <= GraphTextLineCap)
            return (string.Join('\n', lines), lines.Length, charTruncated);

        return (string.Join('\n', lines.Take(GraphTextLineCap)), lines.Length, true);
    }

    private static readonly Regex s_whitespaceRun = new(@"\s+", RegexOptions.Compiled);

    /// <summary>Whitespace collapsed, bounded to <see cref="VictimFingerprintCap"/>, an ellipsis when cut; null for no statement.</summary>
    internal static string? Fingerprint(string? statement)
    {
        if (string.IsNullOrWhiteSpace(statement))
            return null;

        var collapsed = s_whitespaceRun.Replace(statement, " ").Trim();
        return collapsed.Length <= VictimFingerprintCap
            ? collapsed
            : string.Concat(collapsed.AsSpan(0, VictimFingerprintCap).TrimEnd(), "…");
    }
}
