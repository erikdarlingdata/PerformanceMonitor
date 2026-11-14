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
    public string AllVisible { get; init; } = "";
    public double? AllVisibleSort { get; init; }
    public string Turnover { get; init; } = "";
    public double? TurnoverSort { get; init; }
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

    private static DateTime? Latest(DateTime? a, DateTime? b) => a is null ? b : b is null ? a : a > b ? a : b;

    internal static string QualifiedName(string schema, string table) => $"{schema}.{table}";

    /// <summary>The empty space cell: the estimate and its share, or the reason there is none. Never 0 for a missing estimate.</summary>
    internal static string EmptySpaceText(StoreTableHealthRow r) =>
        r.EstimatedEmptyBytes is { } bytes
            ? $"{PgDisplay.Bytes(bytes)} ({Share(r.EstimatedEmptyShare ?? 0)})"
            : r.EstimateUnavailableReason is { } reason
                ? DescribeReason(reason)
                : "no estimate";

    /// <summary>The Flag cell: a rewrite candidate, a table past the empty space floors whose end could not be read, or nothing.</summary>
    internal static string FlagText(StoreTableHealthRow r) =>
        r.RewriteCandidate ? "Rewrite candidate"
        : r.PastRewriteFloors && r.TailOccupied is null ? "Mostly empty, end not checked"
        : "";

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
            IsFlagged = r.RewriteCandidate,
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
            AllVisible = r.PctAllVisible is { } pct ? string.Create(CultureInfo.CurrentCulture, $"{pct:N1}%") : PgDisplay.NotApplicableText,
            AllVisibleSort = r.PctAllVisible,
            Turnover = r.TurnoverPerDay is { } turnover ? Share(turnover) : PgDisplay.NotApplicableText,
            TurnoverSort = r.TurnoverPerDay,
            Growth7d = r.GrowthBytes7d is { } growth
                ? growth == 0 ? "flat" : (growth > 0 ? "+" : "-") + PgDisplay.Bytes(Math.Abs(growth))
                : PgDisplay.NotApplicableText,
            Growth7dSort = r.GrowthBytes7d,
            LastVacuum = PgDisplay.Timestamp(lastVacuum),
            LastVacuumUtc = lastVacuum,
            LastAnalyze = PgDisplay.Timestamp(lastAnalyze),
            LastAnalyzeUtc = lastAnalyze,
            MeasureTarget = r.Kind == KindTable ? QualifiedName(r.SchemaName, r.TableName) : null,
        };
    }

    /// <summary>The table rows, in the reader's order (rewrite candidates first).</summary>
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
            flags.Add(r.LargerThanHeapCount > 1 ? $"Larger than its table ({r.LargerThanHeapCount})" : "Larger than its table");
        }

        if (r.RarelyUsed)
        {
            flags.Add("Rarely used");
        }

        return string.Join(", ", flags);
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
                ? $"{PgDisplay.Bytes(bytes)} ({Share(r.EstimatedBloatShare ?? 0)})"
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

    /// <summary>The header's counts line: tables, total size, rewrite candidates and tables with no usable estimate.</summary>
    internal static string HeaderText(IReadOnlyList<StoreTableHealthRow> rows)
    {
        var summary = Summarize(rows);
        var text = $"{CountText.Plural(summary.TableCount, "store table")}, {PgDisplay.Bytes(summary.TotalBytes)} in total. "
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

        return text;
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
