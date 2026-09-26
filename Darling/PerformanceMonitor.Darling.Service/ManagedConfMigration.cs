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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Line-by-line classifier for an EXISTING <c>postgresql.conf</c> (#4215 A2): decides which lines the
/// v1-v15 appenders wrote (OURS) and which an operator added by hand (HAND-EDIT), so a later rewrite lane
/// can move hand edits below the new <c>include</c> line and drop everything else. Pure and read-only —
/// this class never writes a file.
///
/// <para><b>The rule (design §3 "What moves", rule 3; review L3; ruling comment-5836185470).</b> A line
/// inside a marker's block counts as ours only if it survives BOTH tests: it can be rebuilt byte-for-byte
/// from that block version's own builder (the rebuild test), and it is written in a form that builder
/// actually produces (the form test). A line that fails either test is a hand edit — misclassifying a hand
/// edit as ours would let a later rewrite delete an operator's setting; the reverse mistake only moves an
/// untouched line below the include, with its value intact. That asymmetry is why every path this class
/// cannot yet prove OURS for returns <see cref="ConfLineClassification.Unclassified"/> rather than
/// guessing — a caller must treat <see cref="ConfLineClassification.Unclassified"/> the same as
/// <see cref="ConfLineClassification.HandEdit"/> (left in place, never migrated as ours) until this
/// classifier covers that block version too.</para>
///
/// <para><b>Coverage today: v4, v6, v9, v10, v11, v13, v14, v15</b> (<see cref="CoveredMarkers"/>). Each
/// of these writes only FIXED constants with no derivation input at all — a block appended once with
/// the same bytes on every store, so the rebuild test is a plain string comparison (v14, v15, and v13's
/// <c>track_utility</c> line), or a round trip through a pure parse/format pair the codebase already has
/// (v13's preload-library merge). v1 is NOT covered despite writing mostly-fixed text, because its
/// <c>port = &lt;port&gt;</c> line depends on the store's configured port, an input this classifier is
/// never given (it takes only the conf text) — rebuilding it would require guessing or threading a new
/// parameter through every caller, which the design does not ask for. v2, v3, v5, v7, v8 and v12 are
/// RAM- or disk-derived and need more per-version rebuild machinery — v8 and v12 carry a
/// fingerprint/stamp that records their derivation inputs, v2/v3/v5/v7 only have a "within the formula's
/// range" test available — and are left for a follow-up lane; their marker and content lines classify as
/// <see cref="ConfLineClassification.Unclassified"/> here, never as ours.</para>
///
/// <para><b>The "older store" case (review M6).</b> A store built before v9-v15 existed has no line for
/// those keys at all, inside a block or outside one — there is nothing to misclassify, and this class does
/// nothing special for it: the absent blocks simply produce no spans, and any line elsewhere in the file
/// is classified exactly as it would be on a newer store. The M6 concern that migration rule 2 ("value on
/// the last service line") gives no answer for a missing key is the REWRITE lane's problem, not this
/// classifier's: rule 2 only fires for a key this class has already said is OURS.</para>
/// </summary>
internal static class ManagedConfMigration
{
    /// <summary>How one physical line of <c>postgresql.conf</c> was classified.</summary>
    internal enum ConfLineClassification
    {
        /// <summary>Written by a covered block's builder, unedited. Safe to drop once the block's value is
        /// carried into the new managed file (design rule 2).</summary>
        Ours,

        /// <summary>Failed the rebuild test, the form test, or sits outside every managed block. Moves
        /// verbatim below the new include line (design rule 3).</summary>
        HandEdit,

        /// <summary>Inside a block version this classifier does not cover yet, or is itself an uncovered
        /// marker line. MUST be treated as a hand edit by every caller until coverage is added — see the
        /// class doc comment for why the unproven direction is never "ours".</summary>
        Unclassified,
    }

    /// <summary>Why a line classified as <see cref="ConfLineClassification.HandEdit"/>, for logging and for
    /// tests that need to tell the three routes apart (ruling comment-5827624802 §3, review L3).</summary>
    internal enum HandEditReason
    {
        /// <summary>Not a hand edit — only set on <see cref="ConfLineClassification.Ours"/> and
        /// <see cref="ConfLineClassification.Unclassified"/> lines.</summary>
        None,

        /// <summary>Inside a covered block, in the right form, but a value that block's builder could not
        /// have produced from what it derives from (the design's rebuild test).</summary>
        RebuildMismatch,

        /// <summary>Inside a covered block, but written in a form that block's builder never emits — for
        /// example a unit or quoting the builder does not use, whatever the value means (review L3).</summary>
        FormMismatch,

        /// <summary>Not inside any managed block at all: an operator line added between, before, or after
        /// the appended blocks.</summary>
        OutsideBlock,
    }

    /// <summary>One physical line's verdict. <see cref="Text"/> is the raw line, without its line
    /// terminator — CRLF and LF files classify identically because every comparison here first strips a
    /// trailing <c>\r</c>, the same convention <see cref="DarlingManagedPostgres.FindHardwareSizingBlockEnd"/>
    /// already uses.</summary>
    internal readonly record struct ClassifiedConfLine(
        int LineNumber,
        string Text,
        ConfLineClassification Classification,
        string? BlockMarker,
        string? Key,
        HandEditReason Reason);

    /// <summary>The markers whose blocks this classifier can rule OURS on, in file-append order. Any other
    /// marker in <see cref="DarlingManagedPostgres.AllManagedConfMarkers"/> — v1 through v12 — is not yet
    /// covered; its marker line and its content classify as
    /// <see cref="ConfLineClassification.Unclassified"/>.</summary>
    internal static readonly string[] CoveredMarkers =
    [
        DarlingManagedPostgres.ConfMarkerV4,
        DarlingManagedPostgres.ConfMarkerV6,
        DarlingManagedPostgres.ConfMarkerV9,
        DarlingManagedPostgres.ConfMarkerV10,
        DarlingManagedPostgres.ConfMarkerV11,
        DarlingManagedPostgres.ConfMarkerV13,
        DarlingManagedPostgres.ConfMarkerV14,
        DarlingManagedPostgres.ConfMarkerV15,
    ];

    /// <summary>
    /// Classifies every physical line of <paramref name="conf"/>. Pure: the same text and inputs classify
    /// the same way every time, which is what lets the rebuild test in the pins compare this output against
    /// the exact builder call that (in the field) produced the line under test.
    /// </summary>
    internal static IReadOnlyList<ClassifiedConfLine> ClassifyLines(string conf)
    {
        var result = new List<ClassifiedConfLine>();
        if (string.IsNullOrEmpty(conf))
        {
            return result;
        }

        var rawLines = conf.Split('\n');
        var coveredSpans = FindCoveredSpans(conf, rawLines);
        var uncoveredMarkerLines = FindUncoveredMarkerLines(conf);
        var assignments = IndexAssignmentsByLine(conf);

        for (var i = 0; i < rawLines.Length; i++)
        {
            var lineNumber = i + 1;
            var text = TrimTrailingCarriageReturn(rawLines[i]);

            if (TryFindSpan(coveredSpans, lineNumber, out var span))
            {
                result.Add(ClassifyCoveredLine(lineNumber, text, span, assignments));
                continue;
            }

            if (uncoveredMarkerLines.Contains(lineNumber) || IsInsideAnyManagedSpan(conf, lineNumber))
            {
                result.Add(new ClassifiedConfLine(lineNumber, text, ConfLineClassification.Unclassified, BlockMarker: null, Key: null, HandEditReason.None));
                continue;
            }

            result.Add(new ClassifiedConfLine(lineNumber, text, ConfLineClassification.HandEdit, BlockMarker: null, Key: null, HandEditReason.OutsideBlock));
        }

        return result;
    }

    private static ClassifiedConfLine ClassifyCoveredLine(
        int lineNumber,
        string text,
        (int StartLine, int EndLine, string Marker) span,
        Dictionary<int, (string Key, string Value)> assignments)
    {
        if (string.Equals(text, span.Marker, StringComparison.Ordinal))
        {
            return new ClassifiedConfLine(lineNumber, text, ConfLineClassification.Ours, span.Marker, Key: null, HandEditReason.None);
        }

        if (!assignments.TryGetValue(lineNumber, out var assignment))
        {
            /* A comment or blank line inside a covered block's span. No covered builder emits one, so this
               is a hand edit spliced in without its own blank-line terminator (the #4207 edge case
               FindHardwareSizingBlockEnd's own doc comment names). */
            return new ClassifiedConfLine(lineNumber, text, ConfLineClassification.HandEdit, span.Marker, Key: null, HandEditReason.FormMismatch);
        }

        var (key, value) = assignment;
        var (isOurs, reason) = span.Marker switch
        {
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV4 => ClassifyV4Line(key, text),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV6 => ClassifyV6Line(key, text),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV9 => ClassifyFixedLine(
                key, text, "timezone", "timezone = 'UTC'"),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV10 => ClassifyFixedLine(
                key, text, "lc_messages", "lc_messages = 'C'"),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV11 => ClassifyFixedLine(
                key, text, PerformanceMonitor.Darling.Storage.StoreSelfMetrics.JobExecutionLoggingSetting,
                PerformanceMonitor.Darling.Storage.StoreSelfMetrics.JobExecutionLoggingSetting + " = on"),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV13 => ClassifyV13Line(key, value, text),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV14 => ClassifyFixedLine(
                key, text, DarlingManagedPostgres.MaintenanceWorkMemSetting,
                FormattableString.Invariant($"maintenance_work_mem = {DarlingManagedPostgres.MaintenanceWorkMemCapMb}MB")),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV15 => ClassifyFixedLine(
                key, text, "wal_compression", "wal_compression = lz4"),
            _ => (false, HandEditReason.FormMismatch),
        };

        return new ClassifiedConfLine(
            lineNumber, text, isOurs ? ConfLineClassification.Ours : ConfLineClassification.HandEdit, span.Marker, key, isOurs ? HandEditReason.None : reason);
    }

    /// <summary>v4's two fixed constants (#4214, BuildWriteThroughputConfAppend): <c>max_connections</c> at
    /// <see cref="DarlingManagedPostgres.TargetMaxConnections"/> and a fixed <c>max_wal_size = 4GB</c>.
    /// Neither depends on host RAM or disk, so both rebuild as plain constants.</summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV4Line(string key, string text)
    {
        if (string.Equals(key, "max_connections", StringComparison.OrdinalIgnoreCase))
        {
            return ClassifyFixedLine(
                key, text, "max_connections",
                FormattableString.Invariant($"max_connections = {DarlingManagedPostgres.TargetMaxConnections}"));
        }

        return ClassifyFixedLine(key, text, "max_wal_size", "max_wal_size = 4GB");
    }

    /// <summary>v6's six fixed log-rotation constants (BuildLogRotationConfAppend) — none derived from host
    /// inputs, so each key rebuilds as a plain constant.</summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV6Line(string key, string text)
        => key.ToUpperInvariant() switch
        {
            "LOGGING_COLLECTOR" => ClassifyFixedLine(key, text, "logging_collector", "logging_collector = on"),
            "LOG_DIRECTORY" => ClassifyFixedLine(key, text, "log_directory", "log_directory = 'log'"),
            "LOG_FILENAME" => ClassifyFixedLine(key, text, "log_filename", "log_filename = 'postgresql-%a.log'"),
            "LOG_ROTATION_AGE" => ClassifyFixedLine(key, text, "log_rotation_age", "log_rotation_age = 1d"),
            "LOG_ROTATION_SIZE" => ClassifyFixedLine(key, text, "log_rotation_size", "log_rotation_size = 0"),
            "LOG_TRUNCATE_ON_ROTATION" => ClassifyFixedLine(
                key, text, "log_truncate_on_rotation", "log_truncate_on_rotation = on"),
            _ => (false, HandEditReason.FormMismatch),
        };

    /// <summary>
    /// v13's two possible keys (design §3, review L3). <c>pg_stat_statements.track_utility</c> is a fixed
    /// constant, exactly like v14/v15 — <see cref="ClassifyFixedLine"/> covers it. <c>shared_preload_libraries</c>
    /// is a MERGE, not a constant (<see cref="DarlingManagedPostgres.MergePreloadLibraries"/>), so there is no
    /// single expected line to rebuild against; the test available is the form test alone (review L3's own
    /// carve-out for exactly this shape of builder): the value must round-trip through
    /// <see cref="DarlingManagedPostgres.ParsePreloadList"/> then
    /// <see cref="DarlingManagedPostgres.FormatPreloadList"/> unchanged. A hand edit in a form the builder
    /// would never emit — a literal list with no merge, different quoting, a trailing comma — fails that
    /// round trip and is a hand edit whatever libraries it names, exactly as L3 requires.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV13Line(string key, string value, string text)
    {
        if (string.Equals(key, DarlingManagedPostgres.PreloadSetting, StringComparison.OrdinalIgnoreCase))
        {
            var roundTripped = DarlingManagedPostgres.FormatPreloadList(DarlingManagedPostgres.ParsePreloadList(value));
            var expected = FormattableString.Invariant($"{DarlingManagedPostgres.PreloadSetting} = '{DarlingManagedPostgres.EscapeConfValue(roundTripped)}'");
            return string.Equals(text.Trim(), expected, StringComparison.Ordinal)
                ? (true, HandEditReason.None)
                : (false, HandEditReason.FormMismatch);
        }

        return ClassifyFixedLine(key, text, DarlingManagedPostgres.StatementStatisticsLibrary + ".track_utility", DarlingManagedPostgres.StatementStatisticsLibrary + ".track_utility = off");
    }

    /// <summary>
    /// The rebuild-plus-form test for a key whose covered block writes exactly one constant line (v14, v15,
    /// and v13's <c>track_utility</c> setting): the key must match AND the raw line, trimmed, must be
    /// byte-identical to the one line the builder can ever produce. There is only one way to fail this —
    /// <see cref="HandEditReason.RebuildMismatch"/> and <see cref="HandEditReason.FormMismatch"/> collapse
    /// into the same check for a constant, because any different value is also a different form; the two
    /// reasons stay distinct in <see cref="ConfLineClassification"/>'s doc only because v13's preload line
    /// (above) needs the distinction and shares this helper's key check.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyFixedLine(string key, string text, string expectedKey, string expectedLine)
    {
        if (!string.Equals(key, expectedKey, StringComparison.OrdinalIgnoreCase))
        {
            return (false, HandEditReason.FormMismatch);
        }

        return string.Equals(text.Trim(), expectedLine, StringComparison.Ordinal)
            ? (true, HandEditReason.None)
            : (false, HandEditReason.RebuildMismatch);
    }

    /// <summary>Every covered marker's [start, end) span, in 1-based LINE numbers (end exclusive), plus
    /// which marker started it — built from the same marker-agnostic char-offset walk
    /// <see cref="DarlingManagedPostgres.FindHardwareSizingBlockEnd"/> already applies to v8 (that function
    /// never mentions v8 specifically, so reusing it here cannot drift from what a block's end means
    /// anywhere else in this codebase), then converted to line numbers by counting newlines up to each
    /// char offset against <paramref name="rawLines"/>.</summary>
    private static List<(int StartLine, int EndLine, string Marker)> FindCoveredSpans(string conf, string[] rawLines)
    {
        var spans = new List<(int StartLine, int EndLine, string Marker)>();
        foreach (var marker in CoveredMarkers)
        {
            var searchFrom = 0;
            while (true)
            {
                var markerStart = conf.IndexOf(marker, searchFrom, StringComparison.Ordinal);
                if (markerStart < 0)
                {
                    break;
                }

                if (markerStart > 0 && conf[markerStart - 1] != '\n')
                {
                    searchFrom = markerStart + marker.Length;
                    continue;
                }

                var end = DarlingManagedPostgres.FindHardwareSizingBlockEnd(conf, markerStart);
                var startLine = CountNewlinesBefore(conf, markerStart) + 1;
                var endLine = end >= conf.Length ? rawLines.Length + 1 : CountNewlinesBefore(conf, end) + 1;
                spans.Add((startLine, endLine, marker));
                searchFrom = end;
            }
        }

        return spans;
    }

    private static int CountNewlinesBefore(string conf, int offset)
    {
        var count = 0;
        for (var i = 0; i < offset; i++)
        {
            if (conf[i] == '\n')
            {
                count++;
            }
        }

        return count;
    }

    /// <summary>The 1-based line numbers of every UNCOVERED marker's own marker line (v1-v12) — not its
    /// content, which <see cref="IsInsideAnyManagedSpan"/> already reaches through
    /// <see cref="DarlingStoreHostProfile.IsLineInsideManagedBlock"/>'s generic scan over
    /// <see cref="DarlingManagedPostgres.AllManagedConfMarkers"/>.</summary>
    private static HashSet<int> FindUncoveredMarkerLines(string conf)
    {
        var lines = new HashSet<int>();
        foreach (var marker in DarlingManagedPostgres.AllManagedConfMarkers)
        {
            if (Array.IndexOf(CoveredMarkers, marker) >= 0)
            {
                continue;
            }

            var searchFrom = 0;
            while (true)
            {
                var markerStart = conf.IndexOf(marker, searchFrom, StringComparison.Ordinal);
                if (markerStart < 0)
                {
                    break;
                }

                if (markerStart == 0 || conf[markerStart - 1] == '\n')
                {
                    var lineNumber = conf[..markerStart].Split('\n').Length;
                    lines.Add(lineNumber);
                }

                searchFrom = markerStart + marker.Length;
            }
        }

        return lines;
    }

    /// <summary>Whether 1-based <paramref name="lineNumber"/> sits inside SOME managed block, covered or
    /// not — reuses <see cref="DarlingStoreHostProfile.IsLineInsideManagedBlock"/> so "what counts as inside
    /// a block" cannot drift between the host-profile attribution and this migration classifier.</summary>
    private static bool IsInsideAnyManagedSpan(string conf, int lineNumber)
        => DarlingStoreHostProfile.IsLineInsideManagedBlock(conf, lineNumber);

    private static bool TryFindSpan(List<(int StartLine, int EndLine, string Marker)> spans, int lineNumber, out (int StartLine, int EndLine, string Marker) span)
    {
        foreach (var candidate in spans)
        {
            if (lineNumber >= candidate.StartLine && lineNumber < candidate.EndLine)
            {
                span = candidate;
                return true;
            }
        }

        span = default;
        return false;
    }

    /// <summary>Every active assignment's 1-based line, name and value, keyed by line — a thin wrapper over
    /// <see cref="DarlingManagedPostgres.ParseConfText"/> so this class never re-implements the conf lexer
    /// (quoting, escaping, the optional <c>=</c>) that function already gets right.</summary>
    private static Dictionary<int, (string Key, string Value)> IndexAssignmentsByLine(string conf)
    {
        var byLine = new Dictionary<int, (string Key, string Value)>();
        foreach (var (line, key, value) in DarlingManagedPostgres.ParseConfText(conf))
        {
            byLine[line] = (key, value);
        }

        return byLine;
    }

    private static string TrimTrailingCarriageReturn(string line)
        => line.EndsWith('\r') ? line[..^1] : line;
}
