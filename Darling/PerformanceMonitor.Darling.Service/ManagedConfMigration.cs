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

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Line-by-line classifier for an EXISTING <c>postgresql.conf</c> (#4215): decides which lines the
/// v1-v15 appenders wrote (OURS) and which an operator added by hand (HAND-EDIT), so a later rewrite step
/// can move hand edits below the new <c>include</c> line and drop everything else. Pure and read-only —
/// this class never writes a file.
///
/// <para><b>The rule (design §3 "What moves", rule 3).</b> A line
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
/// <para><b>Coverage today: v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15</b>
/// (<see cref="CoveredMarkers"/>). v4, v6, v9, v10, v11, v13, v14, v15 write only FIXED constants with no
/// derivation input at all — a block appended once with the same bytes on every store, so the rebuild test
/// is a plain string comparison (v14, v15, and v13's <c>track_utility</c> line), or a round trip through a
/// pure parse/format pair the codebase already has (v13's preload-library merge). v1 is NOT covered
/// despite writing mostly-fixed text, because its <c>port = &lt;port&gt;</c> line depends on the store's
/// configured port, an input this classifier is never given (it takes only the conf text) — rebuilding it
/// would require guessing or threading a new parameter through every caller, which the design does not ask
/// for. v2, v3, v5, v7, v8 and v12 are RAM- or disk-derived; v8 and v12 carry a fingerprint/stamp that
/// records their derivation inputs, while v2/v3/v5/v7 are classified by exact formula-image membership over
/// every generation that ever wrote that block (#4336 lanes c2b/c2c) rather than a fingerprint, since none
/// of them stamp their inputs into the block text. Their marker and content lines classify as
/// <see cref="ConfLineClassification.Ours"/> only when in-image, in-form, and (for v5/v7) introduced by a
/// generation reachable from the block's own introduction commit onward — never a generation that predates
/// the block, since a block cannot be older than its own introduction.</para>
///
/// <para><b>The "older store" case.</b> A store built before v9-v15 existed has no line for
/// those keys at all, inside a block or outside one — there is nothing to misclassify, and this class does
/// nothing special for it: the absent blocks simply produce no spans, and any line elsewhere in the file
/// is classified exactly as it would be on a newer store. The concern that migration rule 2 ("value on
/// the last service line") gives no answer for a missing key belongs to the rewrite step, not this
/// classifier: rule 2 only fires for a key this class has already said is OURS.</para>
/// </summary>
internal static class ManagedConfMigration
{
    /// <summary>How one physical line of <c>postgresql.conf</c> was classified.</summary>
    internal enum ConfLineClassification
    {
        /// <summary>Written by a covered block's builder, unedited. Safe to drop once the block's value is
        /// carried into the new managed file (design rule 2).</summary>
        Ours,

        /// <summary>Failed the rebuild test, the form test, or sits outside every managed block. Stays
        /// exactly where it is UNLESS it is the currently-effective assignment (the last one anywhere in
        /// the file) of a key the managed file will own — that one line moves, verbatim, below the new
        /// include line, under a comment (design rule 3).</summary>
        HandEdit,

        /// <summary>Inside a block version this classifier does not cover yet, or is itself an uncovered
        /// marker line. MUST be treated as a hand edit by every caller until coverage is added — see the
        /// class doc comment for why the unproven direction is never "ours".</summary>
        Unclassified,
    }

    /// <summary>Why a line classified as <see cref="ConfLineClassification.HandEdit"/>, for logging and for
    /// tests that need to tell the three routes apart.</summary>
    internal enum HandEditReason
    {
        /// <summary>Not a hand edit — only set on <see cref="ConfLineClassification.Ours"/> and
        /// <see cref="ConfLineClassification.Unclassified"/> lines.</summary>
        None,

        /// <summary>Inside a covered block, in the right form, but a value that block's builder could not
        /// have produced from what it derives from (the design's rebuild test).</summary>
        RebuildMismatch,

        /// <summary>Inside a covered block, but written in a form that block's builder never emits — for
        /// example a unit or quoting the builder does not use, whatever the value means.</summary>
        FormMismatch,

        /// <summary>Not inside any managed block at all: an operator line added between, before, or after
        /// the appended blocks.</summary>
        OutsideBlock,
    }

    /// <summary>One physical line's verdict. <see cref="Text"/> is the raw line, without its line
    /// terminator — CRLF and LF files classify identically because every comparison here first strips a
    /// trailing <c>\r</c>, the same convention <see cref="DarlingManagedPostgres.FindHardwareSizingBlockEnd"/>
    /// already uses. <see cref="Note"/> is set only for a v2/v3 line classified <see cref="ConfLineClassification.Ours"/>
    /// by the formula-image test — the caller logs it verbatim so an operator who set the
    /// value by hand, matching the product's own formula by coincidence, is told how to actually override it.</summary>
    internal readonly record struct ClassifiedConfLine(
        int LineNumber,
        string Text,
        ConfLineClassification Classification,
        string? BlockMarker,
        string? Key,
        HandEditReason Reason,
        string? Note = null);

    /// <summary>The markers whose blocks this classifier can rule OURS on, in file-append order. Every
    /// marker in <see cref="DarlingManagedPostgres.AllManagedConfMarkers"/> is covered, including
    /// v5 and v7.</summary>
    internal static readonly string[] CoveredMarkers =
    [
        DarlingManagedPostgres.ConfMarker,
        DarlingManagedPostgres.ConfMarkerV2,
        DarlingManagedPostgres.ConfMarkerV3,
        DarlingManagedPostgres.ConfMarkerV4,
        DarlingManagedPostgres.ConfMarkerV5,
        DarlingManagedPostgres.ConfMarkerV6,
        DarlingManagedPostgres.ConfMarkerV7,
        DarlingManagedPostgres.ConfMarkerV8,
        DarlingManagedPostgres.ConfMarkerV9,
        DarlingManagedPostgres.ConfMarkerV10,
        DarlingManagedPostgres.ConfMarkerV11,
        DarlingManagedPostgres.ConfMarkerV12,
        DarlingManagedPostgres.ConfMarkerV13,
        DarlingManagedPostgres.ConfMarkerV14,
        DarlingManagedPostgres.ConfMarkerV15,
    ];

    /// <summary>
    /// Classifies every physical line of <paramref name="conf"/>, with no configured port available — v1's
    /// <c>port = &lt;port&gt;</c> line therefore always classifies <see cref="ConfLineClassification.Unclassified"/>
    /// ("not guessable, not covered" becomes literally true only when the
    /// caller has nothing to guess with). Kept so every pre-existing caller and pin keeps compiling.
    /// </summary>
    internal static IReadOnlyList<ClassifiedConfLine> ClassifyLines(string conf)
        => ClassifyLines(conf, configuredPort: null);

    /// <summary>
    /// Classifies every physical line of <paramref name="conf"/>. Pure: the same text and inputs classify
    /// the same way every time, which is what lets the rebuild test in the pins compare this output against
    /// the exact builder call that (in the field) produced the line under test.
    ///
    /// <para><paramref name="configuredPort"/> is the store's configured port (#4336), an input
    /// <see cref="DarlingManagedPostgres.BuildConfAppend"/> needs to
    /// rebuild v1's <c>port = &lt;port&gt;</c> line — the one line in that block this class cannot re-derive
    /// from the conf text alone. Null (the default; see the overload above) leaves v1 at
    /// <see cref="ConfLineClassification.Unclassified"/>, exactly as before this parameter existed; a wrong
    /// port rebuilds to a line that does not match the host's actual <c>port = &lt;port&gt;</c> line and is a
    /// hand edit, never ours.</para>
    /// </summary>
    internal static IReadOnlyList<ClassifiedConfLine> ClassifyLines(string conf, int? configuredPort)
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
                result.Add(ClassifyCoveredLine(lineNumber, text, span, assignments, rawLines, configuredPort));
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
        Dictionary<int, (string Key, string Value)> assignments,
        string[] rawLines,
        int? configuredPort)
    {
        if (string.Equals(text, span.Marker, StringComparison.Ordinal))
        {
            return new ClassifiedConfLine(lineNumber, text, ConfLineClassification.Ours, span.Marker, Key: null, HandEditReason.None);
        }

        if (!assignments.TryGetValue(lineNumber, out var assignment))
        {
            /* A comment or blank line inside a covered block's span. No covered builder emits one, so this
               is a hand edit spliced in without its own blank-line terminator (the #4207 edge case
               FindHardwareSizingBlockEnd's own doc comment names) — UNLESS it is v8's fingerprint or v12's
               stamp comment line, which the rebuild helpers below read directly out of the block's own text. */
            if (span.Marker == DarlingManagedPostgres.ConfMarkerV8 && text.StartsWith(DarlingManagedPostgres.ConfHardwareFingerprintPrefix, StringComparison.Ordinal))
            {
                return new ClassifiedConfLine(lineNumber, text, ConfLineClassification.Ours, span.Marker, Key: null, HandEditReason.None);
            }

            if (span.Marker == DarlingManagedPostgres.ConfMarkerV12 &&
                (text.StartsWith(DarlingManagedPostgres.ConfWalSizingStampPrefix, StringComparison.Ordinal) || text.StartsWith("# derived from ", StringComparison.Ordinal)))
            {
                return new ClassifiedConfLine(lineNumber, text, ConfLineClassification.Ours, span.Marker, Key: null, HandEditReason.None);
            }

            return new ClassifiedConfLine(lineNumber, text, ConfLineClassification.HandEdit, span.Marker, Key: null, HandEditReason.FormMismatch);
        }

        var (key, value) = assignment;

        if (span.Marker == DarlingManagedPostgres.ConfMarker &&
            string.Equals(key, "port", StringComparison.OrdinalIgnoreCase) && configuredPort is null)
        {
            /* No port to rebuild against — not guessable, and not a hand edit either, since every managed
               store carries this line. See ClassifyV1Line's doc comment. */
            return new ClassifiedConfLine(lineNumber, text, ConfLineClassification.Unclassified, span.Marker, key, HandEditReason.None);
        }

        if (span.Marker == DarlingManagedPostgres.ConfMarkerV2)
        {
            var (v2IsOurs, v2Reason) = ClassifyV2Line(key, text, span, assignments);
            return new ClassifiedConfLine(
                lineNumber, text, v2IsOurs ? ConfLineClassification.Ours : ConfLineClassification.HandEdit, span.Marker, key,
                v2IsOurs ? HandEditReason.None : v2Reason,
                v2IsOurs ? FormattableString.Invariant($"legacy v2 {key}={value}: classified as product-derived (formula image); if you set this by hand, re-apply it with ALTER SYSTEM") : null);
        }

        if (span.Marker == DarlingManagedPostgres.ConfMarkerV3)
        {
            var (v3IsOurs, v3Reason) = ClassifyV3Line(key, value);
            return new ClassifiedConfLine(
                lineNumber, text, v3IsOurs ? ConfLineClassification.Ours : ConfLineClassification.HandEdit, span.Marker, key,
                v3IsOurs ? HandEditReason.None : v3Reason,
                v3IsOurs ? FormattableString.Invariant($"legacy v3 {key}={value}: classified as product-derived (formula image); if you set this by hand, re-apply it with ALTER SYSTEM") : null);
        }

        if (span.Marker == DarlingManagedPostgres.ConfMarkerV5)
        {
            var (v5IsOurs, v5Reason) = ClassifyV5Line(key, value);
            return new ClassifiedConfLine(
                lineNumber, text, v5IsOurs ? ConfLineClassification.Ours : ConfLineClassification.HandEdit, span.Marker, key,
                v5IsOurs ? HandEditReason.None : v5Reason,
                v5IsOurs ? FormattableString.Invariant($"legacy v5 {key}={value}: classified as product-derived (formula image); if you set this by hand, re-apply it with ALTER SYSTEM") : null);
        }

        if (span.Marker == DarlingManagedPostgres.ConfMarkerV7)
        {
            var (v7IsOurs, v7Reason) = ClassifyV7Line(key, value);
            return new ClassifiedConfLine(
                lineNumber, text, v7IsOurs ? ConfLineClassification.Ours : ConfLineClassification.HandEdit, span.Marker, key,
                v7IsOurs ? HandEditReason.None : v7Reason,
                v7IsOurs ? FormattableString.Invariant($"legacy v7 {key}={value}: classified as product-derived (formula image); if you set this by hand, re-apply it with ALTER SYSTEM") : null);
        }

        var (isOurs, reason) = span.Marker switch
        {
            _ when span.Marker == DarlingManagedPostgres.ConfMarker => ClassifyV1Line(key, text, configuredPort),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV4 => ClassifyV4Line(key, text),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV6 => ClassifyV6Line(key, text),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV8 => ClassifyV8Line(key, text, span, rawLines),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV9 => ClassifyFixedLine(
                key, text, "timezone", "timezone = 'UTC'"),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV10 => ClassifyFixedLine(
                key, text, "lc_messages", "lc_messages = 'C'"),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV11 => ClassifyFixedLine(
                key, text, PerformanceMonitor.Darling.Storage.StoreSelfMetrics.JobExecutionLoggingSetting,
                PerformanceMonitor.Darling.Storage.StoreSelfMetrics.JobExecutionLoggingSetting + " = on"),
            _ when span.Marker == DarlingManagedPostgres.ConfMarkerV12 => ClassifyV12Line(key, text, span, rawLines),
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

    /// <summary>
    /// v1's block (<see cref="DarlingManagedPostgres.BuildConfAppend"/>, #1681/#3175): three fixed lines
    /// (<c>shared_preload_libraries = 'timescaledb'</c>, <c>listen_addresses = '127.0.0.1'</c>,
    /// <c>default_toast_compression = lz4</c>) plus <c>port = &lt;port&gt;</c>, the one line that depends on
    /// an input this class is not always given (#4336). With
    /// <paramref name="configuredPort"/> null, the port line is <see cref="ConfLineClassification.Unclassified"/>
    /// — not guessable, not a hand edit either, since a port line always exists on every managed store and this
    /// class simply was not told what it should say. With a port, the line rebuilds exactly like any other
    /// fixed constant: match is ours, a different port is a hand edit.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV1Line(string key, string text, int? configuredPort)
    {
        if (string.Equals(key, "port", StringComparison.OrdinalIgnoreCase))
        {
            if (configuredPort is not { } port)
            {
                return (false, HandEditReason.None); /* caller maps this to Unclassified, see below */
            }

            return ClassifyFixedLine(key, text, "port", FormattableString.Invariant($"port = {port}"));
        }

        if (string.Equals(key, DarlingManagedPostgres.PreloadSetting, StringComparison.OrdinalIgnoreCase))
        {
            return ClassifyFixedLine(key, text, DarlingManagedPostgres.PreloadSetting, DarlingManagedPostgres.PreloadSetting + " = 'timescaledb'");
        }

        if (string.Equals(key, "listen_addresses", StringComparison.OrdinalIgnoreCase))
        {
            return ClassifyFixedLine(key, text, "listen_addresses", "listen_addresses = '127.0.0.1'");
        }

        return ClassifyFixedLine(key, text, "default_toast_compression", "default_toast_compression = lz4");
    }

    /// <summary>
    /// v2's block (worker sizing, introduced <c>ce45eed74</c>; today's <see cref="DarlingManagedPostgres.DeriveWorkerSettings"/>
    /// from <c>714af66f8</c> #2845 is the same formula) (#4336): the image is over
    /// hypertable counts, not RAM. <c>timescaledb.max_background_workers = N</c> is ours only if N is an integer
    /// &gt;= 2 (<c>hypertableCount + 2</c> for a non-negative hypertable count) read from the SAME block, and
    /// <c>max_worker_processes = M</c> is ours only if M == N + 11 (<see cref="DarlingManagedPostgres.DeriveWorkerSettings"/>
    /// with N-2 hypertables reproduces the same M) AND M is read from the same block as its N. Anything else —
    /// N &lt; 2, a non-integer, a mismatched M, or a value the builder's exact form never emits (a unit suffix,
    /// different spacing) — is a hand edit.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV2Line(
        string key, string text, (int StartLine, int EndLine, string Marker) span, Dictionary<int, (string Key, string Value)> assignments)
    {
        if (!TryFindAssignmentValue(span, assignments, "timescaledb.max_background_workers", out var backgroundWorkersText) ||
            !int.TryParse(backgroundWorkersText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var backgroundWorkers) ||
            backgroundWorkers < 2)
        {
            return (false, HandEditReason.RebuildMismatch);
        }

        if (!TryFindAssignmentValue(span, assignments, "max_worker_processes", out var workerProcessesText) ||
            !int.TryParse(workerProcessesText, NumberStyles.Integer, CultureInfo.InvariantCulture, out var workerProcesses) ||
            workerProcesses != backgroundWorkers + 11)
        {
            return (false, HandEditReason.RebuildMismatch);
        }

        var rebuilt = DarlingManagedPostgres.DeriveWorkerSettings(backgroundWorkers - 2);
        if (rebuilt.MaxBackgroundWorkers != backgroundWorkers || rebuilt.MaxWorkerProcesses != workerProcesses)
        {
            return (false, HandEditReason.RebuildMismatch);
        }

        if (string.Equals(key, "timescaledb.max_background_workers", StringComparison.OrdinalIgnoreCase))
        {
            return ClassifyFixedLine(
                key, text, "timescaledb.max_background_workers",
                FormattableString.Invariant($"timescaledb.max_background_workers = {backgroundWorkers}"));
        }

        return ClassifyFixedLine(
            key, text, "max_worker_processes",
            FormattableString.Invariant($"max_worker_processes = {workerProcesses}"));
    }

    /// <summary>The value text of the one assignment for <paramref name="key"/> inside <paramref name="span"/>,
    /// read straight from the parsed assignment index rather than re-parsed here — v2's N/M rebuild rule needs
    /// both keys from the SAME block (#4336).</summary>
    private static bool TryFindAssignmentValue(
        (int StartLine, int EndLine, string Marker) span, Dictionary<int, (string Key, string Value)> assignments, string key, out string value)
    {
        for (var lineNumber = span.StartLine; lineNumber < span.EndLine; lineNumber++)
        {
            if (assignments.TryGetValue(lineNumber, out var assignment) &&
                string.Equals(assignment.Key, key, StringComparison.OrdinalIgnoreCase))
            {
                value = assignment.Value;
                return true;
            }
        }

        value = string.Empty;
        return false;
    }

    /// <summary>
    /// v3's block (memory sizing, introduced <c>ce45eed74</c>) (#4336): a v3
    /// block is never rewritten once appended (<see cref="DarlingManagedPostgres.EnsureConfAppended"/> only
    /// appends it if the marker is absent), so a store provisioned under an OLDER formula generation legitimately
    /// still carries that generation's values. The image is the UNION over every generation's exact rounding and
    /// units, over RAM 1 GiB-1 TiB:
    /// <list type="bullet">
    /// <item><c>ce45eed74</c>: <c>shared_buffers = min(ram/4, 8GB)</c>, <c>effective_cache_size = ram/4*3</c>,
    ///   <c>maintenance_work_mem = min(ram/20, 1GB)</c>, <c>work_mem = clamp(ram/512, 16MB, 64MB)</c>.</item>
    /// <item><c>2b67bedeb</c>: the <c>shared_buffers</c> cap drops from 8 GB to 1 GB; the other three unchanged.</item>
    /// <item><c>f4c86ffa3</c> (#1777): <c>maintenance_work_mem = min(max(ram/20, 1536MB), ram/4, 2048MB)</c>;
    ///   <c>shared_buffers</c>/<c>effective_cache_size</c>/<c>work_mem</c> unchanged from <c>2b67bedeb</c>.</item>
    /// <item><c>e507aa2d1</c> (#3909): that cap becomes 2047 MB (<see cref="DarlingManagedPostgres.MaintenanceWorkMemCapMb"/>);
    ///   this is today's <see cref="DarlingManagedPostgres.DeriveMemorySettings"/>.</item>
    /// </list>
    /// <c>effective_cache_size</c> and <c>work_mem</c> never changed across all four generations, so any value
    /// today's formula produces is also every earlier generation's value — one membership test covers all four.
    /// <c>shared_buffers</c> and <c>maintenance_work_mem</c> each need testing against both the pre- and
    /// post-change formula because the change altered their image. Membership is computed exactly by inverting
    /// each formula's RAM-clamped cap/floor shape over the stated RAM range, not by testing "is this a plausible
    /// value": see <see cref="IsInSharedBuffersImage"/> and <see cref="IsInMaintenanceWorkMemImage"/>.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV3Line(string key, string value)
    {
        if (!TryParseWholeMb(value, out var valueMb))
        {
            return (false, HandEditReason.FormMismatch);
        }

        if (string.Equals(key, "shared_buffers", StringComparison.OrdinalIgnoreCase))
        {
            return IsInSharedBuffersImage(valueMb) ? (true, HandEditReason.None) : (false, HandEditReason.RebuildMismatch);
        }

        if (string.Equals(key, "effective_cache_size", StringComparison.OrdinalIgnoreCase))
        {
            return IsInEffectiveCacheSizeImage(valueMb) ? (true, HandEditReason.None) : (false, HandEditReason.RebuildMismatch);
        }

        if (string.Equals(key, "maintenance_work_mem", StringComparison.OrdinalIgnoreCase))
        {
            return IsInMaintenanceWorkMemImage(valueMb) ? (true, HandEditReason.None) : (false, HandEditReason.RebuildMismatch);
        }

        if (string.Equals(key, "work_mem", StringComparison.OrdinalIgnoreCase))
        {
            return IsInWorkMemImage(valueMb) ? (true, HandEditReason.None) : (false, HandEditReason.RebuildMismatch);
        }

        return (false, HandEditReason.FormMismatch);
    }

    /// <summary>
    /// v5's block (co-located sizing override, introduced <c>2b67bedeb</c>) (#4336): re-states ONLY
    /// <c>shared_buffers</c>, and is never rewritten once appended
    /// (<see cref="DarlingManagedPostgres.EnsureConfAppended"/> only appends it if the marker is absent), so
    /// the image is every generation's <c>shared_buffers</c> formula FROM <c>2b67bedeb</c> onward — unlike
    /// v3, this EXCLUDES the pre-<c>2b67bedeb</c> <c>min(ram/4, 8GB)</c> shape, since no generation before
    /// v5 existed could have written a v5 block at all. A single generation ever wrote this block
    /// (<c>min(ram/4, 1GB)</c>; no later commit re-derives <c>shared_buffers</c> — the #2845 v8 notes say it
    /// is deliberately NOT re-derived there), so the image is exactly <see cref="IsInMinRamQuarterImage"/>
    /// at the 1 GB cap, never the 8 GB cap.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV5Line(string key, string value)
    {
        if (!string.Equals(key, "shared_buffers", StringComparison.OrdinalIgnoreCase))
        {
            return (false, HandEditReason.FormMismatch);
        }

        if (!TryParseWholeMb(value, out var valueMb))
        {
            return (false, HandEditReason.FormMismatch);
        }

        return IsInMinRamQuarterImage(valueMb, 1024L) ? (true, HandEditReason.None) : (false, HandEditReason.RebuildMismatch);
    }

    /// <summary>
    /// v7's block (compression-memory override, introduced <c>f4c86ffa3</c> #1777) (#4336): re-states ONLY
    /// <c>maintenance_work_mem</c>, never rewritten once appended, so the
    /// image is every generation's <c>maintenance_work_mem</c> formula FROM <c>f4c86ffa3</c> onward — this
    /// EXCLUDES the pre-#1777 <c>min(ram/20, 1GB)</c> shape (no generation before v7 existed could have
    /// written a v7 block), unlike v3's union which does include it. Two generations wrote this block:
    /// <c>f4c86ffa3</c> itself (cap 2048 MB) and <c>e507aa2d1</c> #3909 (cap 2047 MB,
    /// <see cref="DarlingManagedPostgres.MaintenanceWorkMemCapMb"/>, today's formula) — both are in-image.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV7Line(string key, string value)
    {
        if (!string.Equals(key, "maintenance_work_mem", StringComparison.OrdinalIgnoreCase))
        {
            return (false, HandEditReason.FormMismatch);
        }

        if (!TryParseWholeMb(value, out var valueMb))
        {
            return (false, HandEditReason.FormMismatch);
        }

        var inImage = IsInPostFloorMaintenanceImage(valueMb, 2048L) || IsInPostFloorMaintenanceImage(valueMb, 2047L);
        return inImage ? (true, HandEditReason.None) : (false, HandEditReason.RebuildMismatch);
    }

    /// <summary>The RAM domain every v3 generation's formula was defined over (#4336, per the brief's
    /// "1 GiB-1 TiB" bound), in whole MB — the enumeration bound for the exact-membership tests below.</summary>
    private const long V3MinRamMb = 1024L;
    private const long V3MaxRamMb = 1024L * 1024L;

    /// <summary><c>min(ram/4, capMb)</c>'s image over RAM in [<see cref="V3MinRamMb"/>, <see cref="V3MaxRamMb"/>]
    /// MB, integer-MB truncating division exactly as <see cref="DarlingManagedPostgres.DeriveMemorySettings"/>
    /// computes it (division happens in BYTES there; here in MB the same truncation applies since 4 divides MB
    /// exactly at the byte level only when ram is a whole MB, which every enumerated RAM value in this domain
    /// is): every whole-MB value from 256 (1024/4) up to <paramref name="capMb"/>, plus <paramref name="capMb"/>
    /// itself for every RAM at or above 4*capMb.</summary>
    private static bool IsInMinRamQuarterImage(long valueMb, long capMb)
        => valueMb == capMb || (valueMb >= V3MinRamMb / 4 && valueMb <= capMb - 1 && valueMb == (valueMb * 4L));

    /// <summary>Whether <paramref name="valueMb"/> is <c>min(ram/4, capMb)</c> for SOME whole-MB RAM in
    /// [<see cref="V3MinRamMb"/>, <see cref="V3MaxRamMb"/>]: uncapped, ram/4 sweeps every integer from
    /// V3MinRamMb/4 up (ram/4 hits every integer as ram increases by 1 MB steps, since floor division by 4
    /// increases by 0 or 1 each MB step and never skips a value once past the first few MB) until the cap, so
    /// the image is exactly the integers in [V3MinRamMb/4, capMb - 1] union {capMb} (capped once ram/4 >= capMb,
    /// i.e. ram >= 4*capMb, which is well inside the domain for both cap values used here).</summary>
    private static bool IsInSharedBuffersImage(long valueMb)
        => IsInMinRamQuarterImage(valueMb, 8192L) || IsInMinRamQuarterImage(valueMb, 1024L);

    /// <summary><c>ram/4*3</c> never changed across any v3 generation — its image over the stated RAM domain is
    /// every multiple of 1 MB from <c>(V3MinRamMb/4)*3</c> to <c>(V3MaxRamMb/4)*3</c> that floor-division by 4
    /// followed by *3 can land on: since ram/4 sweeps every integer as RAM increases 1 MB at a time (see
    /// <see cref="IsInMinRamQuarterImage"/>'s reasoning), *3 sweeps every multiple of 3 in that range.</summary>
    private static bool IsInEffectiveCacheSizeImage(long valueMb)
    {
        if (valueMb % 3 != 0)
        {
            return false;
        }

        var quarter = valueMb / 3;
        return quarter >= V3MinRamMb / 4 && quarter <= V3MaxRamMb / 4;
    }

    /// <summary><c>clamp(ram/512, 16MB, 64MB)</c> never changed across any v3 generation — its image is exactly
    /// the integers 16 through 64 inclusive (ram/512 sweeps every integer in range as RAM increases, same
    /// reasoning as the other ram/N terms, and the domain 1 GiB-1 TiB easily reaches both the 16 and 64 MB
    /// clamp bounds).</summary>
    private static bool IsInWorkMemImage(long valueMb)
        => valueMb is >= 16L and <= 64L;

    /// <summary>The union of every v3 generation's <c>maintenance_work_mem</c> image: the pre-#1777 shape
    /// (<c>min(ram/20, 1GB)</c>, generations <c>ce45eed74</c> and <c>2b67bedeb</c> — identical to each other for
    /// this setting) union the post-#1777 shape (<c>min(max(ram/20, 1536MB), ram/4, capMb)</c> for capMb in
    /// {2048 (<c>f4c86ffa3</c>), 2047 (<c>e507aa2d1</c>, today's <see cref="DarlingManagedPostgres.MaintenanceWorkMemCapMb"/>)}).</summary>
    private static bool IsInMaintenanceWorkMemImage(long valueMb)
        => IsInPreFloorMaintenanceImage(valueMb) ||
           IsInPostFloorMaintenanceImage(valueMb, 2048L) ||
           IsInPostFloorMaintenanceImage(valueMb, 2047L);

    /// <summary><c>min(ram/20, 1GB)</c>'s image: every integer from V3MinRamMb/20 up to 1024 inclusive (ram/20
    /// sweeps every integer as RAM increases by whole-MB steps once ram/20 &gt;= 1, and the 1 GiB floor of the
    /// domain already clears that).</summary>
    private static bool IsInPreFloorMaintenanceImage(long valueMb)
        => valueMb >= V3MinRamMb / 20 && valueMb <= 1024L;

    /// <summary><c>min(max(ram/20, 1536), ram/4, capMb)</c>'s image: the value is always either exactly 1536
    /// (the floor, for every ram/20 &lt; 1536, i.e. ram &lt; 30720 MB, as long as ram/4 &gt;= 1536 too — true once
    /// ram &gt;= 6144 MB, comfortably inside the domain) or, once ram/20 &gt;= 1536, sweeps every integer from 1536
    /// up to capMb (ram/20 increases by whole integers as RAM increases, and ram/4 only binds ram/20's climb
    /// once ram/4 &lt; ram/20, which never happens for ram &gt; 0).</summary>
    private static bool IsInPostFloorMaintenanceImage(long valueMb, long capMb)
        => valueMb == 1536L || (valueMb >= 1536L && valueMb <= capMb);

    /// <summary>Parses a conf value of the whole-MB form <c>&lt;digits&gt;MB</c> that every v3 generation's
    /// builder emits — no other unit form (<c>GB</c>, bare digits meaning kB, a decimal) is one any v3 builder
    /// ever wrote, so any other form fails the form test outright.</summary>
    private static bool TryParseWholeMb(string value, out long mb)
    {
        mb = 0;
        if (!value.EndsWith("MB", StringComparison.Ordinal))
        {
            return false;
        }

        return long.TryParse(value[..^2], NumberStyles.Integer, CultureInfo.InvariantCulture, out mb);
    }

    /// <summary>
    /// v8's block (#2845/#4207): re-runs <see cref="DarlingManagedPostgres.BuildHardwareSizingConfAppend"/>
    /// with the RAM and hypertable count the block's OWN fingerprint line records — not the host's
    /// current inputs — and compares byte-for-byte. A block untouched since it was
    /// written matches at ITS OWN recorded inputs whatever the host is now (a resize since then does not make
    /// an old, unedited block a hand edit — it makes it stale, which is <see cref="DarlingManagedPostgres.ShouldAppendHardwareSizing"/>'s
    /// job to notice and replace, not this classifier's to punish). No fingerprint line in the span (an
    /// operator deleted it, or spliced in a hand-written block using the real marker) fails the rebuild outright.
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV8Line(
        string key, string text, (int StartLine, int EndLine, string Marker) span, string[] rawLines)
    {
        if (!TryFindFingerprintLine(span, rawLines, DarlingManagedPostgres.ConfHardwareFingerprintPrefix, out var fingerprint) ||
            !TryParseHardwareFingerprint(fingerprint, out var ramMb, out var hypertables))
        {
            return (false, HandEditReason.RebuildMismatch);
        }

        var rebuilt = DarlingManagedPostgres.BuildHardwareSizingConfAppend(ramMb * 1024L * 1024L, hypertables);
        return ClassifyAgainstRebuiltBlock(key, text, rebuilt);
    }

    /// <summary>
    /// v12's block (#3802): re-derives from the settings its OWN stamp line records — not from the
    /// host's current free-disk figure — the same recorded-input rule as v8. The stamp already carries the
    /// block's derived OUTPUTS (<c>max_wal_size_mb</c>, <c>min_wal_size_mb</c>, <c>pg_major</c>), so the two
    /// setting lines rebuild directly from it without needing the raw free/total disk bytes the comment line
    /// carries for a human reader only; that comment line itself is compared verbatim above (it is not an
    /// assignment, so it never reaches this method).
    /// </summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyV12Line(
        string key, string text, (int StartLine, int EndLine, string Marker) span, string[] rawLines)
    {
        if (!TryFindFingerprintLine(span, rawLines, DarlingManagedPostgres.ConfWalSizingStampPrefix, out var stamp) ||
            !TryParseWalSizingStamp(stamp, out var maxWalSizeMb, out var minWalSizeMb, out var pgMajor))
        {
            return (false, HandEditReason.RebuildMismatch);
        }

        if (string.Equals(key, "max_wal_size", StringComparison.OrdinalIgnoreCase))
        {
            return ClassifyFixedLine(key, text, "max_wal_size", FormattableString.Invariant($"max_wal_size = {maxWalSizeMb}MB"));
        }

        if (string.Equals(key, "min_wal_size", StringComparison.OrdinalIgnoreCase))
        {
            return ClassifyFixedLine(key, text, "min_wal_size", FormattableString.Invariant($"min_wal_size = {minWalSizeMb}MB"));
        }

        if (!DarlingManagedPostgres.PinsCheckpointCompletionTarget(pgMajor))
        {
            /* The stamp says this major does not get the pin, so a checkpoint_completion_target line inside
               the span cannot be this builder's output at these recorded inputs. */
            return (false, HandEditReason.RebuildMismatch);
        }

        return ClassifyFixedLine(
            key, text, "checkpoint_completion_target",
            "checkpoint_completion_target = " + DarlingManagedPostgres.CheckpointCompletionTargetPin);
    }

    /// <summary>The one line inside <paramref name="span"/> that starts with <paramref name="prefix"/> — v8's
    /// fingerprint or v12's stamp — read straight out of the block's own text rather than re-derived, which
    /// is the whole point of the recorded-inputs rebuild rule (#4336).</summary>
    private static bool TryFindFingerprintLine(
        (int StartLine, int EndLine, string Marker) span, string[] rawLines, string prefix, out string line)
    {
        for (var lineNumber = span.StartLine; lineNumber < span.EndLine; lineNumber++)
        {
            var text = TrimTrailingCarriageReturn(rawLines[lineNumber - 1]);
            if (text.StartsWith(prefix, StringComparison.Ordinal))
            {
                line = text;
                return true;
            }
        }

        line = string.Empty;
        return false;
    }

    /// <summary>Parses <see cref="DarlingManagedPostgres.BuildHardwareFingerprint"/>'s own format,
    /// <c>ram_mb=&lt;N&gt; hypertables=&lt;M&gt;</c>, back into the two inputs it recorded. Anything else fails,
    /// which the caller treats as a rebuild mismatch rather than throwing.</summary>
    private static bool TryParseHardwareFingerprint(string fingerprint, out long ramMb, out int hypertables)
    {
        ramMb = 0;
        hypertables = 0;
        var body = fingerprint[DarlingManagedPostgres.ConfHardwareFingerprintPrefix.Length..];
        var parts = body.Split(' ');
        if (parts.Length != 2 ||
            !parts[0].StartsWith("ram_mb=", StringComparison.Ordinal) ||
            !parts[1].StartsWith("hypertables=", StringComparison.Ordinal))
        {
            return false;
        }

        return long.TryParse(parts[0]["ram_mb=".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out ramMb) &&
               int.TryParse(parts[1]["hypertables=".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out hypertables);
    }

    /// <summary>Parses <see cref="DarlingManagedPostgres.BuildWalSizingStamp"/>'s own format,
    /// <c>max_wal_size_mb=&lt;N&gt; min_wal_size_mb=&lt;M&gt; pg_major=&lt;P&gt;</c>, back into the three
    /// recorded outputs.</summary>
    private static bool TryParseWalSizingStamp(string stamp, out int maxWalSizeMb, out int minWalSizeMb, out int pgMajor)
    {
        maxWalSizeMb = 0;
        minWalSizeMb = 0;
        pgMajor = 0;
        var body = stamp[DarlingManagedPostgres.ConfWalSizingStampPrefix.Length..];
        var parts = body.Split(' ');
        if (parts.Length != 3 ||
            !parts[0].StartsWith("max_wal_size_mb=", StringComparison.Ordinal) ||
            !parts[1].StartsWith("min_wal_size_mb=", StringComparison.Ordinal) ||
            !parts[2].StartsWith("pg_major=", StringComparison.Ordinal))
        {
            return false;
        }

        return int.TryParse(parts[0]["max_wal_size_mb=".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out maxWalSizeMb) &&
               int.TryParse(parts[1]["min_wal_size_mb=".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out minWalSizeMb) &&
               int.TryParse(parts[2]["pg_major=".Length..], NumberStyles.Integer, CultureInfo.InvariantCulture, out pgMajor);
    }

    /// <summary>Whether <paramref name="text"/>, keyed by <paramref name="key"/>, is one of the lines the
    /// freshly rebuilt block <paramref name="rebuiltBlock"/> contains — the shared byte-for-byte compare v8's
    /// rebuild test uses once it has re-run the builder at the block's own recorded inputs.</summary>
    private static (bool IsOurs, HandEditReason Reason) ClassifyAgainstRebuiltBlock(string key, string text, string rebuiltBlock)
    {
        foreach (var (_, rebuiltKey, _) in DarlingManagedPostgres.ParseConfText(rebuiltBlock))
        {
            if (!string.Equals(rebuiltKey, key, StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            foreach (var rebuiltLine in rebuiltBlock.Split('\n'))
            {
                if (rebuiltLine.TrimStart().StartsWith(key, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(rebuiltLine.Trim(), text.Trim(), StringComparison.Ordinal))
                {
                    return (true, HandEditReason.None);
                }
            }

            return (false, HandEditReason.RebuildMismatch);
        }

        return (false, HandEditReason.RebuildMismatch);
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
    /// v13's two possible keys (design §3). <c>pg_stat_statements.track_utility</c> is a fixed
    /// constant, exactly like v14/v15 — <see cref="ClassifyFixedLine"/> covers it. <c>shared_preload_libraries</c>
    /// is a MERGE, not a constant (<see cref="DarlingManagedPostgres.MergePreloadLibraries"/>), so there is no
    /// single expected line to rebuild against; the test available is the form test alone (a carve-out for
    /// exactly this shape of builder): the value must round-trip through
    /// <see cref="DarlingManagedPostgres.ParsePreloadList"/> then
    /// <see cref="DarlingManagedPostgres.FormatPreloadList"/> unchanged. A hand edit in a form the builder
    /// would never emit — a literal list with no merge, different quoting, a trailing comma — fails that
    /// round trip and is a hand edit whatever libraries it names.
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

    /// <summary>One key the rewrite either removed (an Ours line) or kept as an operator override, for the
    /// change log the rewrite writes (design rule 3).</summary>
    internal readonly record struct RewriteLogEntry(string Message, string? OverriddenKey);

    /// <summary>The rewritten <c>postgresql.conf</c> text plus its change log (#4336). Never reads
    /// or writes <c>postgresql.auto.conf</c> — that file is the operator's, per design section 4, and this
    /// pure function never touches disk at all.</summary>
    internal readonly record struct RewriteResult(
        string NewConfText,
        IReadOnlyList<RewriteLogEntry> Log,
        IReadOnlySet<string> ExcludedKeys);

    /// <summary>The comment written above every operator line this rewrite moves below the include, so a
    /// reader of the result finds them explained rather than orphaned (design §3 rule 3).</summary>
    internal const string MovedOperatorLinesComment = "# operator settings kept from the previous postgresql.conf (#4215)";

    /// <summary>
    /// Rewrites an existing <c>postgresql.conf</c> (#4336; design §3 "What moves"):
    /// <list type="bullet">
    /// <item>every line <see cref="ClassifyLines"/> marks <see cref="ConfLineClassification.Ours"/> is
    /// removed (rule 2 pairs with this: the product's own copy of a key goes away so the effective value
    /// comes only from the last service line, never from a stale block);</item>
    /// <item>a non-Ours line (<see cref="ConfLineClassification.HandEdit"/> or
    /// <see cref="ConfLineClassification.Unclassified"/>, per this class's own "unproven is never ours"
    /// rule) that assigns a key <paramref name="managedKeys"/> owns AND is the LAST assignment of that key
    /// anywhere in the original file (i.e. it is currently effective) moves, verbatim, below the include
    /// line, under <see cref="MovedOperatorLinesComment"/>, in original relative order — rule 3, "keep
    /// operator lines winning": moving it below the include is what keeps it in force once the managed
    /// file's own copy of that key is included above it;</item>
    /// <item>a non-Ours line that assigns a managed key but is NOT the last assignment (a later line
    /// overrides it) stays exactly where it is, still overridden, same as before this rewrite touched the
    /// file — rule 3's "a line that was already overridden stays where it is and stays overridden";</item>
    /// <item>every other non-Ours line — an unowned key, a comment, a blank line, a stock PostgreSQL
    /// default — stays exactly where it is, byte for byte;</item>
    /// <item>an <c>include</c>, <c>include_if_exists</c> or <c>include_dir</c> line that is not itself the
    /// managed file's own include, appearing AFTER the first product marker line, moves below the include
    /// the same way an effective operator assignment does, in original relative order with any moved
    /// assignment line (#4336). Before this rewrite touched the file, that line's own content was read
    /// AFTER the blocks and so won over them; moving it below the new include keeps it winning over the
    /// managed file the same way. One before the first product marker line stays exactly where it is — it
    /// was already losing to the blocks, and stays losing to the managed file's own include in the same
    /// position, so nothing about its effect changes;</item>
    /// <item>owning a key ownership test is case-insensitive, the same way PostgreSQL itself reads GUC
    /// names (#4336) — an operator line spelling an owned key with different casing, such as
    /// <c>Work_Mem = 64MB</c>, is still recognised as the currently-effective assignment of
    /// <c>work_mem</c> and moves, verbatim (its own casing untouched), below the include;</item>
    /// <item>exactly one <see cref="ManagedConfFile.IncludeLine"/> is written, once, right after the
    /// surviving non-block content and before the moved operator lines (design §2 "where the line goes: the
    /// end"; if one is already present and un-migrated — no opt-out — it is treated as ordinary
    /// text like any other line outside a covered span, and a fresh one is still appended, so re-running
    /// this function is idempotent on a file WITH markers left; rule 6 covers the no-marker case below);</item>
    /// <item>rule 6: if <paramref name="postgresqlConf"/> has no product marker left at all (nothing any
    /// <see cref="ClassifyLines"/> span or uncovered-marker scan finds) AND already carries the include
    /// line, this returns the input completely unchanged, with an empty log — a second run is a genuine
    /// no-op, not just byte-identical output from re-doing the same work.</item>
    /// </list>
    /// <paramref name="managedValues"/> is the managed file's own key/value pairs (<see cref="ManagedConfFile.RenderBody"/>'s
    /// output, parsed back) — used for the log line (the derived value the moved
    /// operator line overrides) and to decide which non-Ours lines are candidates to move; <see cref="ClassifyLines"/>
    /// alone still decides Ours vs. not.
    /// </summary>
    internal static RewriteResult Rewrite(string postgresqlConf, IReadOnlyDictionary<string, string> managedValues, int? configuredPort = null)
    {
        if (HasNoProductMarkers(postgresqlConf) && HasIncludeLine(postgresqlConf))
        {
            /* Rule 6: nothing left to migrate and the include is already there — a genuine no-op. */
            return new RewriteResult(postgresqlConf, Array.Empty<RewriteLogEntry>(), new HashSet<string>(StringComparer.OrdinalIgnoreCase));
        }

        var classified = ClassifyLines(postgresqlConf, configuredPort);
        var normalized = postgresqlConf.Replace("\r\n", "\n", StringComparison.Ordinal);
        var hadTrailingNewline = normalized.EndsWith('\n');
        var rawLines = normalized.Split('\n');
        if (hadTrailingNewline && rawLines.Length > 0 && rawLines[^1].Length == 0)
        {
            rawLines = rawLines[..^1];
        }

        /* Case-insensitive key ownership (#4336): PostgreSQL GUC names are case-insensitive, so
           'Work_Mem = 64MB' owns the same key as 'work_mem'. This maps any casing of an owned key to
           managedValues' own canonical casing, so a moved line's log entry and ExcludedKeys carry the
           canonical name — never whatever casing happened to be on the operator's line. */
        var managedKeys = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var key in managedValues.Keys)
        {
            managedKeys[key] = key;
        }

        var firstProductMarkerLine = FindFirstProductMarkerLine(normalized);

        /* Rule 3's "currently effective" test needs the LAST assignment of each managed key anywhere in the
           ORIGINAL file, Ours or not — an Ours line can be the effective one just as easily as a hand edit,
           and only the line index matters here, not its classification. */
        var lastAssignmentLineByKey = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var (line, key, _) in DarlingManagedPostgres.ParseConfText(normalized))
        {
            if (managedKeys.ContainsKey(key))
            {
                lastAssignmentLineByKey[key] = line;
            }
        }

        var kept = new List<string>();
        var movedOperatorLines = new List<string>();
        var log = new List<RewriteLogEntry>();
        var excluded = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        for (var i = 0; i < rawLines.Length; i++)
        {
            var line = classified[i];
            if (line.Classification == ConfLineClassification.Ours)
            {
                if (line.Note is not null)
                {
                    log.Add(new RewriteLogEntry(line.Note, OverriddenKey: null));
                }

                continue; /* dropped: the product's own line, superseded by the managed file's include. */
            }

            /* A pre-existing, un-migrated include of the managed file is ordinary text here — it is outside
               every covered span, so ClassifyLines already called it a HandEdit; drop it so a second run of
               this function does not duplicate the include line (the fresh one below still gets written). */
            if (string.Equals(rawLines[i], ManagedConfFile.IncludeLine, StringComparison.Ordinal))
            {
                continue;
            }

            var lineNumber = i + 1;

            if (IsOperatorIncludeDirective(rawLines[i], out var includeTarget) &&
                firstProductMarkerLine is { } markerLine && lineNumber > markerLine)
            {
                /* #4336: this include won over the blocks by file order before this rewrite touched the
                   file (it was read after them); moving it below the new include, in original relative
                   order with any moved assignment line, keeps it winning over the managed file the same
                   way. An include before the first product marker line is left where TryFindSpan/the loop
                   below already leaves it — it was already losing to the blocks, so nothing changes. */
                movedOperatorLines.Add(rawLines[i]);
                log.Add(new RewriteLogEntry(
                    FormattableString.Invariant($"include '{includeTarget}': operator include after the product settings, moved below the include"),
                    OverriddenKey: null));
                continue;
            }

            if (TryFindEffectiveManagedAssignment(rawLines[i], managedKeys, lastAssignmentLineByKey, lineNumber, out var effectiveKey))
            {
                /* Rule 3: this is the line currently in force for a key the managed file will own. It moves
                   below the include, verbatim, so it keeps winning once the managed file's own copy of that
                   key is included above it. */
                movedOperatorLines.Add(rawLines[i]);
                excluded.Add(effectiveKey);
                var derivedValue = managedValues[effectiveKey];
                var (_, _, operatorValue) = DarlingManagedPostgres.ParseConfText(rawLines[i]).First(a => string.Equals(a.Name, effectiveKey, StringComparison.OrdinalIgnoreCase));
                log.Add(new RewriteLogEntry(
                    FormattableString.Invariant($"{effectiveKey}: derived {derivedValue}, overridden by operator line below the include ({operatorValue}), not applied"),
                    OverriddenKey: effectiveKey));
                continue;
            }

            /* Not Ours, not the effective assignment of an owned key (either an unowned key, or a managed
               key's line that a LATER line already overrides) — stays exactly where it is (rule 3's "a line
               that was already overridden stays where it is and stays overridden"). */
            kept.Add(rawLines[i]);
        }

        var newLines = new List<string>(kept);
        newLines.Add(ManagedConfFile.IncludeLine);
        if (movedOperatorLines.Count > 0)
        {
            newLines.Add(MovedOperatorLinesComment);
            newLines.AddRange(movedOperatorLines);
        }

        var newText = string.Join('\n', newLines) + "\n";
        return new RewriteResult(newText, log, excluded);
    }

    /// <summary>Whether <paramref name="rawLine"/> is the line currently in force for SOME key in
    /// <paramref name="managedKeys"/> — it assigns a managed key (case-insensitively, per #4336; PostgreSQL
    /// GUC names are case-insensitive) and <paramref name="lastAssignmentLineByKey"/> (built once, over the
    /// whole file, before this loop runs) says <paramref name="lineNumber"/> is that key's last assignment
    /// anywhere. <paramref name="effectiveKey"/> comes back in <paramref name="managedKeys"/>' own canonical
    /// casing, never the operator line's, so callers can look it up in <c>managedValues</c> and
    /// <c>ExcludedKeys</c> by that one spelling regardless of how the operator wrote it. A line assigning
    /// more than one key (never emitted by any covered builder or by PostgreSQL's own <c>key = value</c>
    /// grammar) is not a case this needs to handle; the first managed, currently-effective key on the line
    /// wins.</summary>
    private static bool TryFindEffectiveManagedAssignment(
        string rawLine,
        Dictionary<string, string> managedKeys,
        Dictionary<string, int> lastAssignmentLineByKey,
        int lineNumber,
        out string effectiveKey)
    {
        foreach (var (_, name, _) in DarlingManagedPostgres.ParseConfText(rawLine))
        {
            if (managedKeys.TryGetValue(name, out var canonicalKey) &&
                lastAssignmentLineByKey.TryGetValue(name, out var lastLine) &&
                lastLine == lineNumber)
            {
                effectiveKey = canonicalKey;
                return true;
            }
        }

        effectiveKey = string.Empty;
        return false;
    }

    /// <summary>Whether <paramref name="rawLine"/> is an <c>include</c>, <c>include_if_exists</c> or
    /// <c>include_dir</c> directive — the three forms PostgreSQL itself follows when reading a conf file
    /// (#4336; <see cref="DarlingManagedPostgres.ReadConfAssignments"/> follows the same three). Its target
    /// path or directory comes back in <paramref name="target"/> exactly as
    /// <see cref="DarlingManagedPostgres.ParseConfText"/> reads it (unquoted).</summary>
    private static bool IsOperatorIncludeDirective(string rawLine, out string target)
    {
        foreach (var (_, name, value) in DarlingManagedPostgres.ParseConfText(rawLine))
        {
            if (name.Equals("include", StringComparison.OrdinalIgnoreCase) ||
                name.Equals("include_if_exists", StringComparison.OrdinalIgnoreCase) ||
                name.Equals("include_dir", StringComparison.OrdinalIgnoreCase))
            {
                target = value;
                return true;
            }
        }

        target = string.Empty;
        return false;
    }

    /// <summary>The 1-based line number of the FIRST product marker line anywhere in <paramref name="conf"/>
    /// (covered or not — a pending upgrade to a newer version still counts, same as
    /// <see cref="HasNoProductMarkers"/>), or null if none is present at all (#4336; decides which side of
    /// "before/after the blocks" an operator include directive falls on).</summary>
    private static int? FindFirstProductMarkerLine(string conf)
    {
        int? first = null;
        foreach (var marker in DarlingManagedPostgres.AllManagedConfMarkers)
        {
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
                    if (first is null || lineNumber < first)
                    {
                        first = lineNumber;
                    }
                }

                searchFrom = markerStart + marker.Length;
            }
        }

        return first;
    }

    /// <summary>Rule 6's cheap marker check: whether ANY managed marker (covered or not — a pending upgrade
    /// to a newer version still counts as "markers left") still appears anywhere in <paramref name="conf"/>.
    /// A file with none has nothing left for this rewrite to do.</summary>
    private static bool HasNoProductMarkers(string conf)
    {
        foreach (var marker in DarlingManagedPostgres.AllManagedConfMarkers)
        {
            if (conf.Contains(marker, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Whether <paramref name="conf"/> already carries the managed-file include line, on some line
    /// of its own (rule 6's other half: no markers left AND already included means nothing to do).</summary>
    private static bool HasIncludeLine(string conf)
    {
        foreach (var rawLine in conf.Split('\n'))
        {
            if (string.Equals(TrimTrailingCarriageReturn(rawLine), ManagedConfFile.IncludeLine, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }

}
