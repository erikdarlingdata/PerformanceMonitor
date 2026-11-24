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
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using static PerformanceMonitor.Darling.Storage.DarlingStoreMaintenanceReadinessReader;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableHealthReader;
using static PerformanceMonitor.Darling.Storage.DarlingStoreTableMeasureReader;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One row of the Store Health Tables grid (#8787). Sizes, shares and times are text made the way the PostgreSQL storage grids make
/// them (<see cref="PgDisplay"/>), and each numeric column carries the number its text came from under the property's name plus "Sort", so a
/// header click orders by the number. A missing estimate shows the reason phrase, never 0.</summary>
public sealed class StoreTableRow
{
    public string Table { get; init; } = "";
    public string Kind { get; init; } = "";
    public string Flag { get; init; } = "";
    public bool IsFlagged { get; init; }
    public string TotalSize { get; init; } = "";
    public long TotalSizeSort { get; init; }
    public string HeapSize { get; init; } = "";
    public long HeapSizeSort { get; init; }
    public string IndexSize { get; init; } = "";
    public long IndexSizeSort { get; init; }
    public string ToastSize { get; init; } = "";
    public long ToastSizeSort { get; init; }
    public string Rows { get; init; } = "";
    public long? RowsSort { get; init; }
    public string EmptySpace { get; init; } = "";
    public double? EmptySpaceSort { get; init; }
    public string VacuumCanReturn { get; init; } = "";
    public string DeadRows { get; init; } = "";
    public long DeadRowsSort { get; init; }
    public string Autovacuum { get; init; } = "";

    /// <summary>"Behind, no vacuum in 30 h" when a relation of the table is past its vacuum or insert trigger and no vacuum completed on it
    /// within <see cref="BehindWindow"/>; "No" otherwise. <see cref="BehindSort"/> orders the behind rows by how long, longest first.</summary>
    public string Behind { get; init; } = "";
    public double BehindSort { get; init; }
    public bool IsBehind { get; init; }

    /// <summary>Autovacuums of the table cancelled in the last <see cref="CancelWindow"/>, from the store log, and when the latest was.</summary>
    public string AutovacuumCancels { get; init; } = "";
    public long AutovacuumCancelsSort { get; init; }
    public bool IsCancelledRepeatedly { get; init; }
    public string LastCancel { get; init; } = "";
    public DateTime? LastCancelUtc { get; init; }

    /// <summary>How many of the table's relations (the table, its chunks or partitions) are past their vacuum or insert trigger.</summary>
    public string PastTrigger { get; init; } = "";
    public int PastTriggerSort { get; init; }
    public string AllVisible { get; init; } = "";
    public double? AllVisibleSort { get; init; }
    public string Turnover { get; init; } = "";
    public double? TurnoverSort { get; init; }

    /// <summary>When the churn counters began: the statistics reset, or "before" the last start when they were never reset (#8823).</summary>
    public string CountedSince { get; init; } = "";
    public DateTime? CountedSinceUtc { get; init; }
    public string Growth7d { get; init; } = "";
    public long? Growth7dSort { get; init; }
    public string LastVacuum { get; init; } = "";
    public DateTime? LastVacuumUtc { get; init; }
    public string LastAnalyze { get; init; } = "";
    public DateTime? LastAnalyzeUtc { get; init; }

    /// <summary>The schema-qualified name Measure sends, or null when the row is not a plain table (the reader measures plain tables only).</summary>
    public string? MeasureTarget { get; init; }
}

/// <summary>One row of the Store Health Indexes grid.</summary>
public sealed class StoreIndexRow
{
    public string Table { get; init; } = "";
    public string Index { get; init; } = "";
    public string Flags { get; init; } = "";
    public bool IsFlagged { get; init; }
    public string Size { get; init; } = "";
    public long SizeSort { get; init; }
    public string TableSize { get; init; } = "";
    public long TableSizeSort { get; init; }
    public string Ratio { get; init; } = "";
    public double? RatioSort { get; init; }
    public string Scans { get; init; } = "";
    public long ScansSort { get; init; }
    public string Reclaimable { get; init; } = "";
    public double? ReclaimableSort { get; init; }
    public string Note { get; init; } = "";
}

/// <summary>One row of the partition index coverage grid.</summary>
public sealed class StoreCoverageRow
{
    public string Table { get; init; } = "";
    public string Index { get; init; } = "";
    public string Leaves { get; init; } = "";
    public string WithIndex { get; init; } = "";
    public string Missing { get; init; } = "";
    public string Invalid { get; init; } = "";
    public string Detail { get; init; } = "";
    public bool IsFlagged { get; init; }
}

/// <summary>
/// Turns the Storage readers' rows into what the Store Health tab shows (#8787), with the web page's wording. A pure mapping, so a test
/// can run it without a window.
/// </summary>
internal static class StoreHealthRows
{
    internal const string MeasureHint = "Pick a plain table, then press Measure.";

    private static string Number(long value) => value.ToString("N0", CultureInfo.CurrentCulture);

    private static string Share(double share) => string.Create(CultureInfo.CurrentCulture, $"{share * 100:N1}%");

    /// <summary>A vacuum or analyze time as fleet-wide text with its zone (#8820). These rows describe the store's own tables, so there is no
    /// server clock to render them on: the cells read on the clock of the "Last refreshed" stamp beside them ("2026-11-15 15:13 UTC") instead of
    /// the active server's, which showed a vacuum from a minute ago hours old beside the stamp. Empty when the table has none.</summary>
    internal static string FleetWideTimestamp(DateTime? utc) =>
        utc is { } value ? ViewerTimeHelper.FormatFleetWide(value, "yyyy-MM-dd HH:mm") : string.Empty;

    private static DateTime? Latest(DateTime? a, DateTime? b) => a is null ? b : b is null ? a : a > b ? a : b;

    /// <summary>Why the per-day columns are empty when the statistics were never reset (#8823).</summary>
    internal const string RatesUnknownText =
        "The per-day columns are empty because the statistics were never reset: the counters are older than the last start, so their age is not known. A statistics reset gives rates.";

    /// <summary>The "Counted since" cell. A statistics reset dates the counters, so the rates beside it are worked out from it. With no reset the
    /// counters are older than the last start by an unknown amount: the cell reads "before" that start and the rates are empty (#8823).</summary>
    internal static string CountedSinceText(StoreTableHealthRow r) =>
        r.StatsSince is not { } since ? PgDisplay.NotApplicableText
        : r.RatesKnown ? FleetWideTimestamp(since)
        : "before " + FleetWideTimestamp(since);

    internal static string QualifiedName(string schema, string table) => $"{schema}.{table}";

    /// <summary>The empty space cell: the estimate and its share, or the reason there is none. Never 0 for a missing estimate.</summary>
    internal static string EmptySpaceText(StoreTableHealthRow r) =>
        r.EstimatedEmptyBytes is { } bytes
            ? $"{PgDisplay.Bytes(bytes)} ({Share(r.EstimatedEmptyShare ?? 0)})"
            : r.EstimateUnavailableReason is { } reason
                ? DescribeReason(reason)
                : "no estimate";

    /// <summary>The Flag cell: behind on autovacuum, autovacuum cancelled repeatedly, a rewrite candidate or a table past the empty space
    /// floors whose end could not be read, each that applies, or nothing.</summary>
    internal static string FlagText(StoreTableHealthRow r)
    {
        var flags = new List<string>();
        if (r.Behind)
        {
            flags.Add("Behind on autovacuum");
        }

        if (r.RepeatedlyCancelled)
        {
            flags.Add("Autovacuum cancelled repeatedly");
        }

        if (r.RewriteCandidate)
        {
            flags.Add("Rewrite candidate");
        }
        else if (r.PastRewriteFloors && r.TailOccupied is null)
        {
            flags.Add("Mostly empty, end not checked");
        }

        return string.Join(", ", flags);
    }

    /// <summary>A number of hours in whole hours below three days, then in whole days ("30 h", "4 days"), as the web page writes it.</summary>
    internal static string HoursText(double hours) =>
        hours < 72
            ? string.Create(CultureInfo.CurrentCulture, $"{Math.Floor(hours):N0} h")
            : string.Create(CultureInfo.CurrentCulture, $"{Math.Floor(hours / 24):N0} days");

    /// <summary>A window in whole hours as words: "24 hours".</summary>
    private static string WindowText(TimeSpan window) => CountText.Plural((long)window.TotalHours, "hour");

    /// <summary>The "Autovacuum behind" cell: how long the relation past its trigger has gone without a completed vacuum, or "No".</summary>
    internal static string BehindText(StoreTableHealthRow r) =>
        !r.Behind ? "No"
        : r.BehindHours is { } hours ? "Behind, no vacuum in " + HoursText(hours)
        : "Behind";

    /// <summary>Whether plain VACUUM can give the empty space back. Only read for a table past the floors.</summary>
    internal static string VacuumCanReturnText(StoreTableHealthRow r) => r.TailOccupied switch
    {
        true => "No, VACUUM cannot cut it shorter",
        false => "Yes",
        null => r.PastRewriteFloors ? "Not checked" : "",
    };

    internal static string AutovacuumText(StoreTableHealthRow r) =>
        r.AutovacuumDisabled ? "Off"
        : r.RelationsPastVacuumTrigger > 0 ? "Due now"
        : r.VacuumTriggerRows is { } trigger ? $"At {Number((long)trigger)} dead rows"
        : PgDisplay.NotApplicableText;

    internal static StoreTableRow Table(StoreTableHealthRow r)
    {
        var lastVacuum = Latest(r.LastVacuum, r.LastAutovacuum);
        var lastAnalyze = Latest(r.LastAnalyze, r.LastAutoanalyze);
        return new StoreTableRow
        {
            Table = QualifiedName(r.SchemaName, r.TableName),
            Kind = r.Kind.Replace('_', ' '),
            Flag = FlagText(r),
            IsFlagged = r.RewriteCandidate || r.Behind || r.RepeatedlyCancelled,
            TotalSize = PgDisplay.Bytes(r.TotalBytes),
            TotalSizeSort = r.TotalBytes,
            HeapSize = PgDisplay.Bytes(r.HeapBytes),
            HeapSizeSort = r.HeapBytes,
            IndexSize = PgDisplay.Bytes(r.IndexBytes),
            IndexSizeSort = r.IndexBytes,
            ToastSize = PgDisplay.Bytes(r.ToastBytes),
            ToastSizeSort = r.ToastBytes,
            Rows = r.EstimatedRows is { } rows ? Number(rows) : PgDisplay.NotApplicableText,
            RowsSort = r.EstimatedRows,
            EmptySpace = EmptySpaceText(r),
            EmptySpaceSort = r.EstimatedEmptyShare,
            VacuumCanReturn = VacuumCanReturnText(r),
            DeadRows = Number(r.DeadRows),
            DeadRowsSort = r.DeadRows,
            Autovacuum = AutovacuumText(r),
            Behind = BehindText(r),
            BehindSort = r.Behind ? 1 + (r.BehindHours ?? 0) : 0,
            IsBehind = r.Behind,
            AutovacuumCancels = Number(r.AutovacuumCancels),
            AutovacuumCancelsSort = r.AutovacuumCancels,
            IsCancelledRepeatedly = r.RepeatedlyCancelled,
            LastCancel = FleetWideTimestamp(r.LastAutovacuumCancel),
            LastCancelUtc = r.LastAutovacuumCancel,
            PastTrigger = Number(r.RelationsPastTrigger),
            PastTriggerSort = r.RelationsPastTrigger,
            AllVisible = r.PctAllVisible is { } pct ? string.Create(CultureInfo.CurrentCulture, $"{pct:N1}%") : PgDisplay.NotApplicableText,
            AllVisibleSort = r.PctAllVisible,
            Turnover = r.TurnoverPerDay is { } turnover ? Share(turnover) : PgDisplay.NotApplicableText,
            TurnoverSort = r.TurnoverPerDay,
            CountedSince = CountedSinceText(r),
            CountedSinceUtc = r.StatsSince,
            Growth7d = r.GrowthBytes7d is { } growth
                ? growth == 0 ? "flat" : (growth > 0 ? "+" : "-") + PgDisplay.Bytes(Math.Abs(growth))
                : PgDisplay.NotApplicableText,
            Growth7dSort = r.GrowthBytes7d,
            LastVacuum = FleetWideTimestamp(lastVacuum),
            LastVacuumUtc = lastVacuum,
            LastAnalyze = FleetWideTimestamp(lastAnalyze),
            LastAnalyzeUtc = lastAnalyze,
            MeasureTarget = r.Kind == KindTable ? QualifiedName(r.SchemaName, r.TableName) : null,
        };
    }

    /// <summary>The table rows, in the reader's order (behind on autovacuum first, then autovacuum cancelled repeatedly, then rewrite
    /// candidates).</summary>
    internal static List<StoreTableRow> Tables(IEnumerable<StoreTableHealthRow> rows) => rows.Select(Table).ToList();

    private static string IndexFlags(StoreIndexHealthRow r)
    {
        var flags = new List<string>();
        if (!r.Valid)
        {
            flags.Add("Invalid");
        }

        if (r.LargerThanHeap)
        {
            /* #8897: a rolled-up row's flag is about its copies, not the sum the ratio beside it is of, so it always says how many, even one. */
            flags.Add(r.IndexCount > 1
                ? $"{r.LargerThanHeapCount} of {r.IndexCount} copies larger than its table"
                : "Larger than its table");
        }

        if (r.RarelyUsed)
        {
            flags.Add("Rarely used");
        }

        return string.Join(", ", flags);
    }

    /// <summary>
    /// #8897: the share is of the size the estimate covered. When only some of a rolled-up row's copies have an estimate, that is less than the
    /// row's Size, so the cell names its basis ("6.0 MB of the 6.0 MB estimated (100.0%)") instead of sitting a bare 100% beside a bigger size.
    /// A row whose copies were all estimated keeps the plain text: the estimate and the Size are both pg_relation_size read in one statement, so the basis equals the Size.
    /// </summary>
    internal static string ReclaimableText(StoreIndexHealthRow r, long reclaimableBytes)
    {
        var share = Share(r.EstimatedBloatShare ?? 0);
        return r is { EstimatedBasisBytes: { } basis } && r.EstimateIndexesUsable < r.IndexCount && basis < r.Bytes
            ? $"{PgDisplay.Bytes(reclaimableBytes)} of the {PgDisplay.Bytes(basis)} estimated ({share})"
            : $"{PgDisplay.Bytes(reclaimableBytes)} ({share})";
    }

    internal static StoreIndexRow Index(StoreIndexHealthRow r)
    {
        var flags = IndexFlags(r);
        var notes = new List<string>();
        if (r.IndexCount > 1)
        {
            notes.Add($"{r.IndexCount} copies rolled up");
        }

        if (r.OnCompressedData)
        {
            notes.Add("on compressed data");
        }

        return new StoreIndexRow
        {
            Table = QualifiedName(r.SchemaName, r.TableName),
            Index = r.IndexName,
            Flags = flags,
            IsFlagged = flags.Length > 0,
            Size = PgDisplay.Bytes(r.Bytes),
            SizeSort = r.Bytes,
            TableSize = PgDisplay.Bytes(r.HeapBytes),
            TableSizeSort = r.HeapBytes,
            Ratio = r.IndexToHeapRatio is { } ratio ? string.Create(CultureInfo.CurrentCulture, $"{ratio:N2}") : PgDisplay.NotApplicableText,
            RatioSort = r.IndexToHeapRatio,
            Scans = Number(r.IndexScans),
            ScansSort = r.IndexScans,
            Reclaimable = r.EstimatedReclaimableBytes is { } bytes
                ? ReclaimableText(r, bytes)
                : r.EstimateSkippedReason is { Length: > 0 } skipped ? skipped : "no estimate",
            ReclaimableSort = r.EstimatedBloatShare,
            Note = string.Join(", ", notes),
        };
    }

    /// <summary>The index rows with the flagged ones first, then by size, largest first.</summary>
    internal static List<StoreIndexRow> Indexes(IEnumerable<StoreIndexHealthRow> rows) =>
        rows.OrderByDescending(r => IndexFlags(r).Length > 0)
            .ThenByDescending(r => r.Bytes)
            .Select(Index)
            .ToList();

    internal static StoreCoverageRow Coverage(StoreIndexCoverageRow r)
    {
        var detail = new List<string>();
        if (r.MissingLeaves.Count > 0)
        {
            detail.Add("missing on " + string.Join(", ", r.MissingLeaves)
                + (r.LeavesMissingIndex > r.MissingLeaves.Count ? $" and {r.LeavesMissingIndex - r.MissingLeaves.Count} more" : ""));
        }

        if (r.InvalidLeaves.Count > 0)
        {
            detail.Add("invalid on " + string.Join(", ", r.InvalidLeaves)
                + (r.LeavesInvalidIndex > r.InvalidLeaves.Count ? $" and {r.LeavesInvalidIndex - r.InvalidLeaves.Count} more" : ""));
        }

        if (!r.ParentIndexValid)
        {
            detail.Add("the parent index is not valid");
        }

        return new StoreCoverageRow
        {
            Table = QualifiedName(r.SchemaName, r.TableName),
            Index = r.IndexName,
            Leaves = Number(r.LeafCount),
            WithIndex = Number(r.LeavesWithIndex),
            Missing = Number(r.LeavesMissingIndex),
            Invalid = Number(r.LeavesInvalidIndex),
            Detail = string.Join("; ", detail),
            IsFlagged = detail.Count > 0,
        };
    }

    /// <summary>The coverage rows with the incomplete ones first.</summary>
    internal static List<StoreCoverageRow> CoverageRows(IEnumerable<StoreIndexCoverageRow> rows) =>
        rows.Select(Coverage).OrderByDescending(r => r.IsFlagged).ToList();

    /// <summary>The header's counts line: tables, total size, tables behind on autovacuum, tables with autovacuum cancelled repeatedly,
    /// rewrite candidates and tables with no usable estimate.</summary>
    internal static string HeaderText(IReadOnlyList<StoreTableHealthRow> rows)
    {
        var summary = Summarize(rows);
        var text = $"{CountText.Plural(summary.TableCount, "store table")}, {PgDisplay.Bytes(summary.TotalBytes)} in total. "
            + $"{CountText.Plural(summary.Behind, "table")} behind on autovacuum (past the start point, no vacuum in {WindowText(BehindWindow)}). "
            + $"{CountText.Plural(summary.RepeatedlyCancelled, "table")} with autovacuum cancelled {RepeatedCancelMinimum} or more times in {WindowText(CancelWindow)}. "
            + $"{CountText.Plural(summary.RewriteCandidates, "rewrite candidate")}. "
            + $"{CountText.Plural(summary.WithoutEstimate, "table")} without a usable empty space estimate";
        text += summary.WithoutEstimateByReason.Count == 0
            ? "."
            : " (" + string.Join(", ", summary.WithoutEstimateByReason.Select(p => $"{p.Value} {DescribeReason(p.Key)}")) + ").";
        var unread = rows.Count(r => r.PastRewriteFloors && r.TailOccupied is null);
        if (unread > 0)
        {
            text += $" {unread} mostly empty, with the end of the file not checked.";
        }

        if (rows.Any(r => !r.RatesKnown))
        {
            text += " " + RatesUnknownText;
        }

        return text;
    }

    /// <summary>A duration in seconds the way the web page writes it: "30 s", "1 min", "2 h".</summary>
    private static string SecondsText(int seconds) =>
        seconds < 60 ? $"{Number(seconds)} s"
        : seconds < 3600 ? $"{Number(seconds / 60)} min"
        : $"{Number(seconds / 3600)} h";

    /// <summary>
    /// The store's autovacuum settings line, from the read's own figures, with the web page's wording: the write budget first (the most
    /// all autovacuum workers together may dirty per second, or unthrottled when the cost delay is 0), then the settings in force, then
    /// the cancels the store log holds in the window. A setting that falls back to another (an <c>autovacuum_</c> setting of -1) names
    /// the one in force.
    /// </summary>
    internal static string AutovacuumSettingsText(StoreAutovacuumSettings s)
    {
        static string From(string setting, string own) => setting.Length > 0 && setting != own ? $" (from {setting})" : "";

        var budget = s.WriteBudgetMBps is { } mbps
            ? string.Create(CultureInfo.CurrentCulture, $"Autovacuum write budget {mbps:N1} MB/s: the most autovacuum may write per second. ")
            : "Autovacuum write budget: unthrottled, the cost delay is 0, so vacuum does not sleep. ";
        var parts = new[]
        {
            (s.AutovacuumOn ? "Autovacuum is on" : "Autovacuum is off") + $": {Number(s.WorkersRunning)} of {Number(s.MaxWorkers)} workers running",
            "it looks for work every " + SecondsText(s.NaptimeSeconds),
            $"cost limit {Number(s.CostLimit)}" + From(s.CostLimitSetting, "autovacuum_vacuum_cost_limit"),
            string.Create(CultureInfo.CurrentCulture, $"cost delay {s.CostDelayMs:0.##} ms") + From(s.CostDelaySetting, "autovacuum_vacuum_cost_delay"),
            PgDisplay.Bytes(s.WorkMemKb * 1024) + " of memory per worker" + From(s.WorkMemSetting, "autovacuum_work_mem"),
        };
        var cancels = s.CancelsInWindow == 0
            ? $"The store log has no cancelled autovacuum in the last {WindowText(CancelWindow)}."
            : $"The store log has {CountText.Plural(s.CancelsInWindow, "cancelled autovacuum")} in the last {WindowText(CancelWindow)}"
              + (s.CancelsUnresolved > 0
                  ? $", {Number(s.CancelsUnresolved)} for a relation the store no longer has or one in another database."
                  : ".");
        return budget + string.Join(", ", parts) + ". " + cancels;
    }

    /// <summary>The maintenance verdict and its reasons, one reason per line.</summary>
    internal static string ReadinessText(StoreMaintenanceReadiness readiness) =>
        readiness.Reasons.Count == 0
            ? readiness.Verdict
            : readiness.Verdict + Environment.NewLine + string.Join(Environment.NewLine, readiness.Reasons.Select(r => "- " + r));

    /// <summary>
    /// <see cref="ReadinessText"/> in two parts: the lead of the verdict (shown in a heavier weight) and everything after it (the
    /// caveat and note sentences, the reasons). The lead is a fixed phrase the reader sets, so no sentence is split on a full stop
    /// (a table name has dots in it). A verdict that starts with neither phrase has no lead.
    /// </summary>
    internal static (string Lead, string After) ReadinessParts(StoreMaintenanceReadiness readiness)
    {
        var text = ReadinessText(readiness);
        foreach (var lead in new[] { SafeLead, NotSafeLead })
        {
            if (text.StartsWith(lead, StringComparison.Ordinal))
            {
                return (lead, text[lead.Length..]);
            }
        }

        return (string.Empty, text);
    }

    /// <summary>The index section's header line.</summary>
    internal static string IndexHeaderText(IReadOnlyList<StoreIndexHealthRow> indexes) =>
        $"{CountText.Plural(indexes.Count, "store index", "store indexes")}. {Number(indexes.Count(r => r.LargerThanHeap))} larger than their table, "
        + $"{Number(indexes.Count(r => r.RarelyUsed))} rarely used, {Number(indexes.Count(r => !r.Valid))} invalid.";

    /// <summary>#9364: what a tables or indexes read did, written after its time in the slow-load line: how many relations it sized, and
    /// whether the store measured them for this call or the call took the measurement the Viewer already held, with that measurement's age.
    /// A read stamped at or after <paramref name="startedUtc"/> (the moment the call began) was taken for it, a call that joined a read already
    /// running included. An older stamp is a held measurement, and its age is counted to <paramref name="nowUtc"/>.</summary>
    internal static string ReadNote(int relations, DateTime readAtUtc, DateTime startedUtc, DateTime nowUtc)
    {
        var sized = $"sized {CountText.Plural(relations, "relation")}";
        if (readAtUtc >= startedUtc)
        {
            return sized + ", measured live";
        }

        var seconds = (long)Math.Max(0, (nowUtc - readAtUtc).TotalSeconds);
        var age = seconds < 60 ? $"{seconds} s" : $"{seconds / 60} min {seconds % 60} s";
        return $"{sized}, from the held measurement taken {age} ago";
    }

    /// <summary>#9374: the slow-load line's note for the first open's read of the store's last measured sizes: how many relations it sized and
    /// that the sizes are the store's last measurement, with its age at <paramref name="nowUtc"/>. The live read after it says "measured live".</summary>
    internal static string LastSizesNote(int relations, DateTime measuredAtUtc, DateTime nowUtc)
    {
        var seconds = (long)Math.Max(0, (nowUtc - measuredAtUtc).TotalSeconds);
        var age = seconds < 60 ? $"{seconds} s" : seconds < 3600 ? $"{seconds / 60} min {seconds % 60} s" : $"{seconds / 3600} h {seconds % 3600 / 60} min";
        return $"sized {CountText.Plural(relations, "relation")}, from the store's last measurement taken {age} ago";
    }

    /// <summary>#9364: how long before <paramref name="nowUtc"/> the store answered the rows the Viewer saved: "under 1 min", "12 min", "3 h 5 min"
    /// or "2 d 4 h". A time in the future (the store's clock ahead of this machine's) reads as no time.</summary>
    internal static string SnapshotAge(DateTime readAtUtc, DateTime nowUtc)
    {
        var seconds = (long)Math.Max(0, (nowUtc - readAtUtc).TotalSeconds);
        return seconds < 60 ? "under 1 min"
            : seconds < 3600 ? $"{seconds / 60} min"
            : seconds < 86400 ? $"{seconds / 3600} h {seconds % 3600 / 60} min"
            : $"{seconds / 86400} d {seconds % 86400 / 3600} h";
    }

    /// <summary>The measured result in plain words: the verdict, then the page counts, with the time the read took.</summary>
    internal static string MeasureText(string table, StoreTableMeasureResult result, TimeSpan elapsed)
    {
        var took = string.Create(CultureInfo.CurrentCulture, $"{elapsed.TotalSeconds:N1} s");
        if (result.Outcome != StoreTableMeasureOutcome.Measured || result.Measure is not { } m)
        {
            return $"{table}: {result.Sentence ?? "not measured"} ({took})";
        }

        var text = $"{table}: {m.Verdict} {CountText.Plural(m.Rows, "row")} in {CountText.Plural(m.TotalPages, "page")}.";
        if (m.TrailingEmptyPages > 0)
        {
            text += $" {CountText.Plural(m.TrailingEmptyPages, "empty page")} {(m.TrailingEmptyPages == 1 ? "sits" : "sit")} at the end of the file, which is the most VACUUM can return.";
        }

        text += $" ({took})";
        if (m.VacuumTruncateDisabled)
        {
            text += " VACUUM cannot cut the end of this table while its vacuum_truncate option is off.";
        }

        return text;
    }
}
