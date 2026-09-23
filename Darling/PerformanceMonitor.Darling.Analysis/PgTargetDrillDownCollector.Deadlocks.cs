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
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

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
    /// The character bound on a carried graph, applied before the line bound. Twice lane 7's
    /// <see cref="StatementTextCap"/>: a two-participant graph carries two statements. Applied in memory since
    /// #4005, after the graph is normalized, because a cut before it can land inside a literal and withhold the
    /// query it cut; the read's own bound is <c>PgDeadlockLogParser.NormalizeReadCap</c>.
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
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC), <c>$4</c> the shape cap, <c>$5</c> and
    /// <c>$6</c> how much of the statement and the graph are read, which since #4005 is what normalizing them
    /// needs (<c>PgDeadlockLogParser.NormalizeReadCap</c>) rather than the caps the drill-down shows.
    ///
    /// <para><b>A shape is <c>(participant_count, lock_modes, resources)</c>, not <c>deadlock_hash</c>.</b>
    /// The hash is <c>PgDeadlockLogParser.IdentityOf</c> — SHA-256 over the timestamp and the DETAIL block, pids
    /// included (over the raw block alone before #4005) — and exists so the same REPORT re-read from an
    /// overlapping log tail is stored once. Two deadlocks of
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
    /// LAST)</c> — the first element), with its identity so <c>get_pg_deadlock_detail</c> can be pointed at it:
    /// the hash, or the <c>PgDeadlockLogParser.ReportIdentity</c> of a report whose hash is over its raw graph
    /// (#4005), which the read tells apart by <c>latest_hash_is_raw</c>.
    /// The statement and graph are normalized in memory and then cut to their caps, which the flags report; the
    /// stored lengths ride beside them. <c>count(*) OVER ()</c> and the two <c>SUM(…) OVER ()</c> put the window totals on every
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
        (array_agg(graph_text        ORDER BY occurred_at DESC NULLS LAST, collection_time DESC))[1] AS latest_graph_text,
        (array_agg(occurred_at       ORDER BY occurred_at DESC NULLS LAST, collection_time DESC))[1] AS latest_occurred_at
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
    CAST(SUM(rows_captured) OVER () AS integer)          AS total_rows,
    latest_occurred_at,
    /* Whether the latest report's hash is over its raw graph (#4005), tested on the whole graph before the
       cut: PgDeadlockLogParser.RawGraphHashSql, spelled out because this is a constant; a test holds the two
       equal. */
    (upper(left(encode(sha256(convert_to(latest_graph_text, 'UTF8')), 'hex'), 32)) = latest_hash) AS latest_hash_is_raw
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
        /* #4005: the read cuts at what normalizing needs, and the caps are applied after it — see below. */
        cmd.Parameters.AddWithValue(PgDeadlockLogParser.NormalizeReadCap);
        cmd.Parameters.AddWithValue(PgDeadlockLogParser.NormalizeReadCap);

        using (var reader = await cmd.ExecuteReaderAsync(context.CancellationToken))
        {
            while (await reader.ReadAsync(context.CancellationToken))
            {
                distinctShapes = reader.IsDBNull(13) ? 0 : Convert.ToInt32(reader.GetValue(13));
                totalReports = reader.IsDBNull(14) ? 0 : Convert.ToInt32(reader.GetValue(14));
                totalRows = reader.IsDBNull(15) ? 0 : Convert.ToInt32(reader.GetValue(15));

                /* Normalized, then cut to the caps (#4005), as DarlingPgDeadlockReader normalizes every read: a row
                   stored before #4005 holds its SQL raw. In that order because a cut first can land inside a
                   literal, and the lexer then withholds the whole statement rather than keep what the cut left of
                   it. The read's own bound is past anything the lexer reads, so what it cuts is withheld whole
                   whichever way round. The flags say whether the caps cut what the reader is shown. */
                var victimRead = reader.IsDBNull(9) ? null : reader.GetString(9);
                var victimNormalized = PgDeadlockLogParser.NormalizeStatement(victimRead);
                var victimStatement = victimNormalized is { Length: > StatementTextCap } ? victimNormalized[..StatementTextCap] : victimNormalized;
                var graphNormalized = PgDeadlockLogParser.NormalizeGraph(reader.IsDBNull(11) ? null : reader.GetString(11));
                var graphCharCut = graphNormalized is { Length: > GraphTextCharCap };
                var (boundedGraph, graphLines, graphTruncated) = BoundGraphText(
                    graphCharCut ? graphNormalized![..GraphTextCharCap] : graphNormalized, readCut: graphCharCut);
                var latestPid = reader.IsDBNull(8) ? (int?)null : Convert.ToInt32(reader.GetValue(8));
                var latestHash = reader.IsDBNull(7) ? null : reader.GetString(7);

                exemplars.Add(new PgTargetDeadlockExemplar(
                    Rank: exemplars.Count + 1,
                    ParticipantCount: reader.IsDBNull(0) ? null : Convert.ToInt32(reader.GetValue(0)),
                    LockModes: reader.IsDBNull(1) ? null : reader.GetString(1),
                    Resources: reader.IsDBNull(2) ? null : reader.GetString(2),
                    Reports: reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                    RowsCaptured: reader.IsDBNull(3) ? 0 : Convert.ToInt32(reader.GetValue(3)),
                    FirstSeen: reader.IsDBNull(5) ? null : reader.GetDateTime(5),
                    LastSeen: reader.IsDBNull(6) ? null : reader.GetDateTime(6),
                    /* Never a hash over a raw graph (#4005, #4004): the identity get_pg_deadlock_detail takes. */
                    DeadlockHash: latestHash is null
                        ? null
                        : DarlingPgDeadlockReader.IdentityOf(
                            latestHash,
                            !reader.IsDBNull(17) && reader.GetBoolean(17),
                            reader.IsDBNull(16) ? default : reader.GetDateTime(16),
                            latestPid ?? 0),
                    VictimPid: latestPid,
                    VictimStatement: victimStatement,
                    VictimStatementMayBeTruncated: victimNormalized is not null && victimNormalized.Length > victimStatement!.Length,
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
            /* #4005: what tells a stored finding from one written before its SQL was normalized, which
               NormalizeStoredDeadlockExemplars rewrites on the way out. A statement this build cut to its cap
               can end inside a `'?'`, and read again it would be withheld, so this one is left as written. */
            sql_normalized = true,
            note = PgTargetAdvice.DeadlockExemplarSentence(summary),
        };

        /* Re-freeze the card's prose with the exemplar sentence — see the summary. */
        var frozen = FactAdvice.TryReadStoryText(finding.StoryText);
        if (frozen is not null)
            finding.StoryText = FactAdvice.SerializeForStoryText(PgTargetAdvice.WithDeadlockExemplars(frozen, summary));
    }

    /// <summary>
    /// The graph, cut at <see cref="GraphTextLineCap"/> lines; the total line count and whether either bound
    /// (the read's characters, which <paramref name="readCut"/> says, this method's lines) cut it. Tab
    /// indenting was stripped at capture; line ends are normalised here because the two log transports differ.
    /// </summary>
    internal static (string? Text, int Lines, bool Truncated) BoundGraphText(string? graphText, bool readCut)
    {
        if (string.IsNullOrEmpty(graphText))
            return (graphText, 0, false);

        var charTruncated = readCut;
        var lines = graphText.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        if (lines.Length <= GraphTextLineCap)
            return (string.Join('\n', lines), lines.Length, charTruncated);

        return (string.Join('\n', lines.Take(GraphTextLineCap)), lines.Length, true);
    }

    /// <summary>
    /// A stored finding's deadlock exemplars brought to #4005's rules on the way out, as
    /// <see cref="DarlingPgDeadlockReader"/> brings a stored report: a finding persists its drill-down and its
    /// prose (#2060), so one written before #4005 kept each exemplar's victim statement, its fingerprint in the
    /// advice sentence, and its graph with their literals, for the 30 days findings are kept. Each is normalized
    /// again, and the fingerprint replaced in the prose where the stored one stands. The stored
    /// <c>deadlock_hash</c> may be a hash over a raw graph, which a finding cannot tell from one that is not, so
    /// every one is replaced by the <see cref="PgDeadlockLogParser.ReportIdentity"/> of the exemplar's latest
    /// report, which <c>get_pg_deadlock_detail</c> takes. Idempotent, and a no-op on a finding with no exemplars
    /// and on one this build wrote, whose section says <c>sql_normalized</c>.
    /// </summary>
    internal static void NormalizeStoredDeadlockExemplars(AnalysisFinding finding)
    {
        if (finding.DrillDown is null
            || !finding.DrillDown.TryGetValue(DeadlockExemplarsSection, out var section)
            || section is not JsonElement { ValueKind: JsonValueKind.Object } element
            || JsonNode.Parse(element.GetRawText()) is not JsonObject node
            || node["exemplars"] is not JsonArray exemplars
            || (node["sql_normalized"] is JsonValue marker && marker.TryGetValue<bool>(out var normalized) && normalized))
            return;

        var fingerprints = new List<(string Stored, string Normalized)>();
        foreach (var exemplar in exemplars.OfType<JsonObject>())
        {
            var statement = PgDeadlockLogParser.NormalizeStatement(StringOf(exemplar["victim_statement"]));
            var storedFingerprint = StringOf(exemplar["victim_statement_fingerprint"]);
            var fingerprint = Fingerprint(statement);
            exemplar["victim_statement"] = statement;
            exemplar["victim_statement_fingerprint"] = fingerprint;
            exemplar["graph_text"] = PgDeadlockLogParser.NormalizeGraph(StringOf(exemplar["graph_text"]));
            if (storedFingerprint is not null && storedFingerprint != fingerprint)
                fingerprints.Add(($"`{storedFingerprint}`", $"`{fingerprint}`"));

            var identity = StringOf(exemplar["deadlock_hash"]);
            if (identity is not null && !PgDeadlockLogParser.TryParseReportIdentity(identity, out _, out _))
            {
                exemplar["deadlock_hash"] =
                    DateTime.TryParse(StringOf(exemplar["last_seen"]), CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var lastSeen)
                    && exemplar["victim_pid"] is JsonValue pid && pid.TryGetValue<int>(out var victimPid)
                        ? PgDeadlockLogParser.ReportIdentity(lastSeen, victimPid)
                        : null;
            }
        }

        if (StringOf(node["note"]) is { } note)
            node["note"] = Replace(note, fingerprints);

        finding.DrillDown[DeadlockExemplarsSection] = JsonSerializer.SerializeToElement(node);

        if (fingerprints.Count > 0 && FactAdvice.TryReadStoryText(finding.StoryText) is { } advice)
        {
            finding.StoryText = FactAdvice.SerializeForStoryText(advice with
            {
                Headline = Replace(advice.Headline, fingerprints),
                Investigation = Replace(advice.Investigation, fingerprints),
                Remediation = Replace(advice.Remediation, fingerprints),
            });
        }

        static string? StringOf(JsonNode? value) =>
            value is JsonValue text && text.TryGetValue<string>(out var s) ? s : null;

        static string Replace(string text, List<(string Stored, string Normalized)> pairs)
        {
            foreach (var (stored, normalized) in pairs)
                text = text.Replace(stored, normalized, StringComparison.Ordinal);
            return text;
        }
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
