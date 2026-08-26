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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Stored PostgreSQL rows, projected into what a grid may show (#2530). Pure and WPF-free, so every rule
/// below is unit-testable without a window — which matters, because the rules ARE the content: each one
/// exists to stop the grid printing a number that would be read as a measurement when it is not one.
///
/// <list type="bullet">
/// <item><b><c>-1</c> is not a value.</b> It is the store's not-applicable sentinel for a duration, a WAL
/// size or a threshold. 0 was rejected for these deliberately — it reads as "started this instant",
/// "retains nothing", "already past its threshold" — so rendering <c>-1</c> raw would be the same mistake
/// one step later.</item>
/// <item><b>Absent is not zero.</b> <c>pg_stat_io</c>'s write side is NULL across the board on Aurora,
/// because backends there do not write data files. A blank cell says that; a <c>0</c> claims a measurement
/// that was never taken.</item>
/// <item><b>NULL recurrence is "cannot tell", not "once".</b> The blocking read returns NULL when the
/// root's own backend id did not resolve, and those are different claims.</item>
/// <item><b>Every timestamp is naive UTC in the store</b> and goes through
/// <see cref="ViewerTimeHelper.ForDisplay"/>, exactly like every other timestamp the viewer renders.</item>
/// <item><b>Blocks are not bytes.</b> <c>temp_blks_written</c> counts blocks, and the block size is a
/// compile-time setting of the server, not a constant. It is labelled in blocks rather than multiplied by
/// an assumed 8kB.</item>
/// </list>
/// </summary>
internal static class PgDisplay
{
    /// <summary>The store's not-applicable sentinel. Named rather than repeated, because reading it as a
    /// number anywhere is the defect this class exists to prevent.</summary>
    internal const long NotApplicable = -1;

    /// <summary>What an unmeasured / not-applicable cell shows. An em dash, not "0" and not blank: blank
    /// reads as "nothing here yet" and 0 reads as a measurement.</summary>
    internal const string NotApplicableText = "—";

    internal static string Bytes(long value)
    {
        if (value <= NotApplicable)
        {
            return NotApplicableText;
        }

        string[] units = { "B", "KB", "MB", "GB", "TB", "PB" };
        double scaled = value;
        var unit = 0;
        while (scaled >= 1024 && unit < units.Length - 1)
        {
            scaled /= 1024;
            unit++;
        }

        /* The unit has to be chosen against the value AS RENDERED, not against the exact quotient. One byte
           short of a megabyte the quotient is 1023.999…, which stays in KB and then rounds to "1024.0 KB" —
           a number expressed in its own next unit, which reads as a typo rather than as a size. Re-check
           once after rounding; one pass is enough, because the second division can only land on 1.0. */
        if (unit < units.Length - 1 && Math.Round(scaled, 1) >= 1024)
        {
            scaled /= 1024;
            unit++;
        }

        return unit == 0
            ? $"{value:N0} B"
            : string.Create(CultureInfo.CurrentCulture, $"{scaled:N1} {units[unit]}");
    }

    /// <summary>A signed byte delta, for the "is this growing" columns. Zero is stated rather than blanked:
    /// "flat" is an answer, and an empty cell would read as "not measured".</summary>
    internal static string ByteDelta(long from, long to)
    {
        if (from <= NotApplicable || to <= NotApplicable)
        {
            return NotApplicableText;
        }

        var delta = to - from;
        return delta == 0 ? "flat" : (delta > 0 ? "+" : "-") + Bytes(Math.Abs(delta));
    }

    /// <summary>A block count, LABELLED as blocks. PostgreSQL's block size is a compile-time setting of
    /// the server, so converting to bytes here would put a fabricated figure next to measured ones.</summary>
    internal static string Blocks(long value) =>
        value <= NotApplicable
            ? NotApplicableText
            : string.Create(CultureInfo.CurrentCulture, $"{value:N0} blocks");

    internal static string Count(long value) =>
        value <= NotApplicable ? NotApplicableText : value.ToString("N0", CultureInfo.CurrentCulture);

    internal static string CountDelta(long from, long to)
    {
        var delta = to - from;
        return delta == 0 ? "flat" : (delta > 0 ? "+" : "") + delta.ToString("N0", CultureInfo.CurrentCulture);
    }

    /// <summary>A duration in milliseconds, or the not-applicable dash for the <c>-1</c> sentinel.</summary>
    internal static string Milliseconds(long ms)
    {
        if (ms <= NotApplicable)
        {
            return NotApplicableText;
        }

        return ms < 1000
            ? $"{ms:N0} ms"
            : TimeSpan.FromMilliseconds(ms).ToString(ms < 3_600_000 ? @"mm\:ss" : @"d\.hh\:mm\:ss", CultureInfo.CurrentCulture);
    }

    internal static string Timestamp(DateTime? utc) =>
        utc is { } value
            ? ViewerTimeHelper.ForDisplay(value).ToString("yyyy-MM-dd HH:mm", CultureInfo.CurrentCulture)
            : string.Empty;

    /// <summary>A percentage of a total, or the dash when the total is zero — which is "nothing happened",
    /// not "0%".</summary>
    internal static string Percent(long part, long total) =>
        total <= 0
            ? NotApplicableText
            : string.Create(CultureInfo.CurrentCulture, $"{part * 100.0 / total:N1}%");

    // ── Vacuum ───────────────────────────────────────────────────────────────────────────────────

    internal sealed class XminRow
    {
        public string Source { get; init; } = "";
        public string IsWinnerText { get; init; } = "";
        public string XminAge { get; init; } = "";
        public string PeakXminAge { get; init; } = "";
        public string WinnerShare { get; init; } = "";
        public string MeasuredAt { get; init; } = "";
        public string Holder { get; init; } = "";
        public string Detail { get; init; } = "";
    }

    internal static XminRow Xmin(DarlingPgXminReader.PgXminRow row) => new()
    {
        Source = row.Source,
        IsWinnerText = row.IsWinner ? "yes" : "",
        XminAge = Count(row.XminAge),
        PeakXminAge = Count(row.PeakXminAge),
        /* The share, not the raw count. A slot that won 58 of 60 samples is a standing problem someone
           needs to own; a session that won twice is a query that ran long and finished. Reporting only the
           current holder makes those two look the same. */
        WinnerShare = row.Samples <= 0
            ? NotApplicableText
            : string.Create(CultureInfo.CurrentCulture, $"{row.SamplesAsWinner:N0} of {row.Samples:N0} samples"),
        MeasuredAt = Timestamp(row.MeasuredAt),
        Holder = row.Holder ?? "",
        Detail = row.Detail ?? "",
    };

    internal sealed class AutovacuumRow
    {
        public string DatabaseName { get; init; } = "";
        public string SchemaName { get; init; } = "";
        public string TableName { get; init; } = "";
        public string AutovacuumState { get; init; } = "";
        public bool AutovacuumDisabled { get; init; }
        public string DeadTuples { get; init; } = "";
        public string VacuumThreshold { get; init; } = "";
        public string ThresholdRatio { get; init; } = "";
        public string DeadTupleTrend { get; init; } = "";
        public string InsertsSinceVacuum { get; init; } = "";
        public string InsertVacuumThreshold { get; init; } = "";
        public string LastVacuum { get; init; } = "";
        public string LastAutovacuum { get; init; } = "";
        public string LastAnalyze { get; init; } = "";
        public string LastAutoanalyze { get; init; } = "";
        public string AutovacuumCount { get; init; } = "";
        public string LiveTuples { get; init; } = "";
        public string DeadTuplePct { get; init; } = "";
        public string ModsSinceAnalyze { get; init; } = "";
        public string AnalyzeThreshold { get; init; } = "";
        public string TotalSize { get; init; } = "";
        public string MeasuredAt { get; init; } = "";
    }

    internal static AutovacuumRow Autovacuum(DarlingPgAutovacuumReader.PgAutovacuumRow row) => new()
    {
        DatabaseName = row.DatabaseName ?? "",
        SchemaName = row.SchemaName ?? "",
        TableName = row.TableName ?? "",
        AutovacuumState = row.AutovacuumDisabled ? "DISABLED" : "enabled",
        AutovacuumDisabled = row.AutovacuumDisabled,
        DeadTuples = Count(row.DeadTuples),
        VacuumThreshold = Count(row.VacuumThreshold),
        /* Ratio to the table's OWN threshold, which is what makes a small hot table and a huge cold one
           comparable at all. A never-analyzed table can have threshold 0, where a ratio is undefined
           rather than infinite. */
        ThresholdRatio = row.VacuumThreshold <= 0
            ? NotApplicableText
            : string.Create(CultureInfo.CurrentCulture, $"{row.DeadTuples / (double)row.VacuumThreshold:N1}x"),
        /* Climbing dead tuples mean autovacuum is losing; a flat figure well past the threshold usually
           means it is blocked or switched off. Those need different fixes, so the trend is a column rather
           than something to work out from two visits. */
        DeadTupleTrend = CountDelta(row.FirstDeadTuples, row.DeadTuples),
        InsertsSinceVacuum = Count(row.InsertsSinceVacuum),
        /* -1 here is not "no threshold": it is a major with no autovacuum_vacuum_insert_threshold at all. */
        InsertVacuumThreshold = row.InsertVacuumThreshold <= NotApplicable
            ? "n/a on this version"
            : Count(row.InsertVacuumThreshold),
        /* Manual and automatic runs are separate columns because they answer different questions: a table
           whose only vacuums are manual is one somebody is nursing, and that is a finding about the
           configuration rather than about the workload. */
        LastVacuum = Timestamp(row.LastVacuum),
        LastAutovacuum = Timestamp(row.LastAutovacuum),
        LastAnalyze = Timestamp(row.LastAnalyze),
        LastAutoanalyze = Timestamp(row.LastAutoanalyze),
        /* Zero is the finding, not a missing value: a table autovacuum has NEVER processed is the classic
           wraparound route, because relfrozenxid never advances. */
        AutovacuumCount = Count(row.AutovacuumCount),
        LiveTuples = Count(row.LiveTuples),
        /* The dead-tuple SHARE, which is what bloat actually is. The raw count beside a threshold says
           whether vacuum is due; the share says how much of the table is already waste. */
        DeadTuplePct = Percent(row.DeadTuples, row.LiveTuples + row.DeadTuples),
        /* The ANALYZE half of the same story, and it is genuinely separate: a table can be vacuumed on
           schedule and still have statistics old enough to give the planner the wrong row counts. */
        ModsSinceAnalyze = Count(row.ModsSinceAnalyze),
        AnalyzeThreshold = Count(row.AnalyzeThreshold),
        TotalSize = Bytes(row.TotalBytes),
        MeasuredAt = Timestamp(row.MeasuredAt),
    };

    internal sealed class WraparoundRow
    {
        public string DatabaseName { get; init; } = "";
        public string FrozenXidAge { get; init; } = "";
        public double PctTowardEmergencyVacuum { get; init; }
        public double PctTowardWraparound { get; init; }
        public string XidsRemaining { get; init; } = "";
        public string MinMultiXidAge { get; init; } = "";
        public double PctTowardMultixactEmergency { get; init; }
        public double PctTowardMultixactWraparound { get; init; }
        public string MultiXidsRemaining { get; init; } = "";
        public string FreezeMaxAge { get; init; } = "";
        public string MultixactFreezeMaxAge { get; init; } = "";
        public string WindowPeakFrozenXidAge { get; init; } = "";
        public string WindowPeakMinMultiXidAge { get; init; } = "";
        public string ConnectionsAllowed { get; init; } = "";
        public string MeasuredAt { get; init; } = "";
    }

    internal static WraparoundRow Wraparound(DarlingPgWraparoundReader.PgWraparoundRow row) => new()
    {
        DatabaseName = row.DatabaseName,
        FrozenXidAge = Count(row.FrozenXidAge),
        PctTowardEmergencyVacuum = Math.Round(row.PctTowardEmergencyVacuum, 1),
        PctTowardWraparound = Math.Round(row.PctTowardWraparound, 1),
        XidsRemaining = Count(row.XidsRemaining),
        MinMultiXidAge = Count(row.MinMultiXidAge),
        PctTowardMultixactEmergency = Math.Round(row.PctTowardMultixactEmergency, 1),
        /* The multixact side has its OWN shutdown ceiling, reached independently of the XID one - a
           workload heavy on row-level share locks or subtransactions gets there first, and nothing on the
           XID columns would say so. */
        PctTowardMultixactWraparound = Math.Round(row.PctTowardMultixactWraparound, 1),
        MultiXidsRemaining = Count(row.MultiXidsRemaining),
        /* The percentages beside these are graded against THIS cluster's settings, not a constant, so the
           settings themselves are a column: two databases at "80% to emergency" are different distances from
           trouble when their freeze_max_age differs, and nothing else on the row would say so. */
        FreezeMaxAge = Count(row.AutovacuumFreezeMaxAge),
        MultixactFreezeMaxAge = Count(row.AutovacuumMultixactFreezeMaxAge),
        WindowPeakFrozenXidAge = Count(row.WindowPeakFrozenXidAge),
        WindowPeakMinMultiXidAge = Count(row.WindowPeakMinMultiXidAge),
        /* datallowconn=false is why a database can sit at 99% of wraparound and never be vacuumed by
           anything that connects to it. It is the finding, not a footnote. */
        ConnectionsAllowed = row.AllowsConnections ? "allowed" : "NOT ALLOWED",
        MeasuredAt = Timestamp(row.MeasuredAt),
    };

    // ── Waits, I/O, replication ──────────────────────────────────────────────────────────────────

    internal sealed class WaitRow
    {
        public string WaitType { get; init; } = "";
        public string WaitEvent { get; init; } = "";
        public string TotalWaits { get; init; } = "";
        public double TotalWaitTimeMs { get; init; }
        public double AvgWaitTimeMs { get; init; }
    }

    internal static WaitRow Wait(DarlingPgWaitReader.PgWaitRow row) => new()
    {
        WaitType = row.WaitType,
        WaitEvent = row.WaitEvent,
        TotalWaits = Count(row.TotalWaits),
        TotalWaitTimeMs = Math.Round(row.TotalWaitTimeMs, 1),
        AvgWaitTimeMs = Math.Round(row.AvgWaitTimeMs, 3),
    };

    internal sealed class IoRow
    {
        public string BackendType { get; init; } = "";
        public string ObjectType { get; init; } = "";
        public string Context { get; init; } = "";

        /// <summary>What this context MEANS, from the shared reader — the one copy the MCP surface also
        /// prints. Shown as the Context cell's tooltip rather than a column: it is a paragraph, and it is
        /// the dimension with no SQL Server counterpart, so an operator meeting `bulkread` for the first
        /// time needs it and one who knows it does not want it eating the grid.</summary>
        public string ContextMeaning { get; init; } = "";

        public string Reads { get; init; } = "";
        public double ReadTimeMs { get; init; }

        /// <summary>Per-read latency — the figure that separates "a lot of I/O" from "slow I/O", which have
        /// completely different remedies.</summary>
        public string AvgReadMs { get; init; } = "";

        public string Hits { get; init; } = "";

        /// <summary>A hit ratio scoped to THIS combination, the only scope where it means anything: a
        /// server-wide ratio averages bulkread's deliberate misses together with normal-context ones and
        /// understates both.</summary>
        public string HitPct { get; init; } = "";

        /// <summary>This combination's share of the window's total read TIME — how the grid's order was
        /// decided, made legible. A row at 4% of the reads and 60% of the read time is the finding.</summary>
        public string PctOfTotalReadTime { get; init; } = "";

        public string Extends { get; init; } = "";
        public string ExtendTimeMs { get; init; } = "";
        public string Evictions { get; init; } = "";
        public string Reuses { get; init; } = "";
        public string Writes { get; init; } = "";
        public string WriteTimeMs { get; init; } = "";
        public string OpSize { get; init; } = "";
        public string StatsReset { get; init; } = "";
    }

    /// <summary>
    /// Projects the whole I/O result at once, because two of its columns are SHARES of the window and a
    /// per-row projection cannot compute a denominator it never sees. Ordering the grid by read time and
    /// then not saying what share that is leaves the reader to divide by hand.
    /// </summary>
    internal static List<IoRow> IoRows(IReadOnlyList<DarlingPgIoReader.PgIoRow> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        var totalReadTime = rows.Sum(r => r.ReadTimeMs);
        return rows.Select(r => Io(r, totalReadTime)).ToList();
    }

    internal static IoRow Io(DarlingPgIoReader.PgIoRow row, double totalReadTimeMs = 0) => new()
    {
        BackendType = row.BackendType ?? "",
        ObjectType = row.ObjectType ?? "",
        Context = row.Context ?? "",
        ContextMeaning = DarlingPgIoReader.ContextMeaning(row.Context),
        Reads = Count(row.Reads),
        ReadTimeMs = Math.Round(row.ReadTimeMs, 1),
        AvgReadMs = row.Reads > 0
            ? string.Create(CultureInfo.CurrentCulture, $"{row.ReadTimeMs / row.Reads:N3}")
            : NotApplicableText,
        Hits = Count(row.Hits),
        HitPct = Percent(row.Hits, row.Hits + row.Reads),
        PctOfTotalReadTime = totalReadTimeMs > 0
            ? string.Create(CultureInfo.CurrentCulture, $"{row.ReadTimeMs / totalReadTimeMs * 100:N1}%")
            : NotApplicableText,
        Extends = Count(row.Extends),
        ExtendTimeMs = string.Create(CultureInfo.CurrentCulture, $"{Math.Round(row.ExtendTimeMs, 1):N1}"),
        Evictions = Count(row.Evictions),
        Reuses = Count(row.Reuses),
        /* "not tracked", not 0. On Aurora the whole pg_stat_io write side is NULL because backends there
           do not write data files, and the read carries WriteCountersTracked precisely so this cell can
           tell that apart from a server that wrote nothing. */
        Writes = row.WriteCountersTracked ? Count(row.Writes) : "not tracked",
        WriteTimeMs = row.WriteCountersTracked
            ? Math.Round(row.WriteTimeMs, 1).ToString("N1", CultureInfo.CurrentCulture)
            : "not tracked",
        OpSize = Bytes(row.OpBytes),
        StatsReset = Timestamp(row.StatsReset),
    };

    internal sealed class SlotRow
    {
        public string SlotName { get; init; } = "";
        public string SlotType { get; init; } = "";
        public string ActiveText { get; init; } = "";
        public string WalStatus { get; init; } = "";
        public string RetainedWal { get; init; } = "";
        public string RetainedWalTrend { get; init; } = "";
        public string SafeWalSize { get; init; } = "";
        public string XminAge { get; init; } = "";
        public string CatalogXminAge { get; init; } = "";
        public string InactiveSince { get; init; } = "";
        public string DatabaseName { get; init; } = "";
        public string Plugin { get; init; } = "";
        public string InvalidationReason { get; init; } = "";

        /// <summary>The stored <c>conflicting</c> flag: a logical slot whose needed rows were vacuumed away
        /// by a recovery conflict. It is a DIFFERENT failure from invalidation-by-WAL-size and needs a
        /// different response (the subscriber must be re-created either way, but the cause is
        /// <c>hot_standby_feedback</c> rather than <c>max_slot_wal_keep_size</c>), so it is its own column
        /// rather than folded into the invalidation reason.</summary>
        public string Conflicting { get; init; } = "";

        public string MeasuredAt { get; init; } = "";
        public bool IsInvalidated { get; init; }
        public bool IsInactive { get; init; }
    }

    internal static SlotRow Slot(DarlingPgSlotReader.PgSlotRow row) => new()
    {
        SlotName = row.SlotName,
        SlotType = row.SlotType ?? "",
        ActiveText = row.IsActive ? "active" : "INACTIVE",
        WalStatus = row.WalStatus ?? "",
        RetainedWal = Bytes(row.RetainedWalBytes),
        RetainedWalTrend = ByteDelta(row.FirstRetainedWalBytes, row.RetainedWalBytes),
        SafeWalSize = Bytes(row.SafeWalSizeBytes),
        XminAge = Count(row.XminAge),
        CatalogXminAge = Count(row.CatalogXminAge),
        InactiveSince = Timestamp(row.InactiveSince),
        DatabaseName = row.DatabaseName ?? "",
        Plugin = row.Plugin ?? "",
        InvalidationReason = row.InvalidationReason ?? "",
        Conflicting = row.Conflicting ? "CONFLICTING" : "",
        MeasuredAt = Timestamp(row.MeasuredAt),
        /* An invalidated slot has already lost its WAL: the replica behind it needs rebuilding, and that
           is a different day from an inactive slot that is merely accumulating. A CONFLICTING slot is in
           the same category — its subscriber cannot continue — so it shares the highlight even though the
           cause and the fix are different. */
        IsInvalidated = !string.IsNullOrEmpty(row.InvalidationReason)
                        || row.Conflicting
                        || string.Equals(row.WalStatus, "lost", StringComparison.OrdinalIgnoreCase),
        IsInactive = !row.IsActive,
    };

    // ── Activity ─────────────────────────────────────────────────────────────────────────────────

    internal sealed class ChainRow
    {
        public string CapturedAt { get; init; } = "";
        public int RootPid { get; init; }

        /// <summary>The collector's synthetic backend id, which is what "seen as root" is counted on — pids
        /// are reused, so two captures agreeing on a pid are not necessarily the same backend. <c>0</c> is
        /// the vanished-blocker sentinel and shows as unknown rather than as an id.</summary>
        public string RootBackendId { get; init; } = "";

        public string Databases { get; init; } = "";
        public int TotalVictims { get; init; }
        public int DirectVictims { get; init; }
        public string Depth { get; init; } = "";
        public string WorstVictimWait { get; init; } = "";
        public string RootState { get; init; } = "";
        public string RootXactDuration { get; init; } = "";
        public string RootUsername { get; init; } = "";
        public string RootApplicationName { get; init; } = "";
        public string SamplesAsRoot { get; init; } = "";
        public string RootQuery { get; init; } = "";
        public string WorstVictimQuery { get; init; } = "";
        public string Caveats { get; init; } = "";
    }

    internal static ChainRow Chain(DarlingPgBlockingReader.PgBlockingChainRow row)
    {
        var caveats = new List<string>();
        if (row.ChainMayBeTruncated)
        {
            caveats.Add("chain hit the 32-level walk cap");
        }
        if (row.QueryTextMayBeTruncated)
        {
            caveats.Add("query text truncated at track_activity_query_size");
        }

        return new ChainRow
        {
            CapturedAt = Timestamp(row.CapturedAt),
            RootPid = row.RootPid,
            RootBackendId = row.RootBackendId == 0
                ? "unknown"
                : row.RootBackendId.ToString("N0", CultureInfo.CurrentCulture),
            Databases = string.Join(", ", row.Databases.Where(d => !string.IsNullOrEmpty(d))),
            TotalVictims = row.TotalVictims,
            DirectVictims = row.DirectVictims,
            Depth = row.MaxDepth.ToString(CultureInfo.CurrentCulture) + (row.ChainMayBeTruncated ? " (capped)" : ""),
            WorstVictimWait = Milliseconds(row.WorstVictimWaitMs),
            /* idle in transaction is the state worth naming on its own: the root is not running anything,
               so nothing will finish and release the lock without intervention. */
            RootState = row.RootIsIdleInTransaction ? "idle in transaction" : row.RootState ?? "",
            RootXactDuration = Milliseconds(row.RootXactDurationMs),
            RootUsername = row.RootUsername ?? "",
            RootApplicationName = row.RootApplicationName ?? "",
            /* NULL is "cannot tell" — the blocker's own row had already left pg_stat_activity, so it landed
               on the collector's synthetic backend id and counting it would conflate unrelated incidents. */
            SamplesAsRoot = row.SamplesAsRoot is { } samples
                ? string.Create(CultureInfo.CurrentCulture, $"{samples:N0} captures")
                : "unknown",
            RootQuery = row.RootQuery ?? "",
            WorstVictimQuery = row.WorstVictimQuery ?? "",
            Caveats = string.Join("; ", caveats),
        };
    }

    internal sealed class CycleRow
    {
        public string CapturedAt { get; init; } = "";
        public int ParticipantCount { get; init; }
        public string Pids { get; init; } = "";
        public string DatabaseName { get; init; } = "";
        public string ApplicationName { get; init; } = "";
        public int BlockedBehindCount { get; init; }
        public string BlockedBehindPids { get; init; } = "";
    }

    internal static CycleRow Cycle(DarlingPgBlockingReader.PgBlockingCycleRow row) => new()
    {
        CapturedAt = Timestamp(row.CapturedAt),
        ParticipantCount = row.ParticipantCount,
        Pids = string.Join(", ", row.Pids),
        DatabaseName = row.DatabaseName ?? "",
        ApplicationName = row.ApplicationName ?? "",
        BlockedBehindCount = row.BlockedBehindCount,
        BlockedBehindPids = string.Join(", ", row.BlockedBehindPids),
    };

    internal sealed class StatementRow
    {
        /// <summary>A string, not a number. PostgreSQL <c>queryid</c> is an <c>int8</c> that routinely
        /// exceeds 2^53, and rendering it through anything that rounds is the one field whose entire
        /// purpose is joining back to <c>pg_stat_statements</c> (#2548).</summary>
        public string QueryId { get; init; } = "";

        /// <summary>The database OID, which is half the grain of this read — one queryid appears once per
        /// database it ran in, and two rows with the same statement text are not a duplicate.</summary>
        public string DatabaseId { get; init; } = "";

        public string Calls { get; init; } = "";
        public string TotalExecTimeMs { get; init; } = "";
        public string AvgExecTimeMs { get; init; } = "";
        public double MaxExecTimeMs { get; init; }
        public string RowsReturned { get; init; } = "";

        /// <summary>Buffer-cache hit share for this statement, over BOTH cache tiers Aurora reports — the
        /// shared buffers and the Optimized Read cache — against both miss sources. Reported as one ratio
        /// rather than four block counters, which are the same fact in a form nobody reads correctly.</summary>
        public string CacheHitPct { get; init; } = "";

        public string TempRead { get; init; } = "";
        public string TempWritten { get; init; } = "";
        public string PeakMemory { get; init; } = "";
        public string WalWritten { get; init; } = "";
        public string QueryText { get; init; } = "";
    }

    internal static StatementRow Statement(DarlingPgStatementReader.PgStatementRow row) => new()
    {
        QueryId = row.QueryId.ToString(CultureInfo.InvariantCulture),
        DatabaseId = row.DatabaseId.ToString("N0", CultureInfo.CurrentCulture),
        Calls = Count(row.Calls),
        TotalExecTimeMs = Count(row.TotalExecTimeMs),
        AvgExecTimeMs = row.Calls <= 0
            ? NotApplicableText
            : string.Create(CultureInfo.CurrentCulture, $"{row.TotalExecTimeMs / (double)row.Calls:N1}"),
        MaxExecTimeMs = Math.Round(row.MaxExecTimeMs, 1),
        RowsReturned = Count(row.RowsReturned),
        /* #2625: the Aurora-only halves are NULL on a self-hosted target, where there is no Optimized
           Reads tier and no storage volume to read from. Treating them as zero would still produce a
           NUMBER here — a cache-hit ratio computed over shared blocks alone, presented in the same column
           as Aurora's four-way one, with nothing saying the two mean different things. So the ratio is
           computed only when the split is present, and reads as not-applicable when it is not. */
        CacheHitPct = row.OrcacheBlocksHit is { } orcacheHit && row.StorageBlocksRead is { } storageRead
            ? Percent(
                row.SharedBlocksHit + orcacheHit,
                row.SharedBlocksHit + orcacheHit + row.SharedBlocksRead + storageRead)
            : NotApplicableText,
        /* BLOCKS, and labelled as such — both of them. The block size is a compile-time setting of the
           server, so multiplying by an assumed 8kB would put a fabricated byte count next to real ones.
           Temp READ sits beside temp written because a spill that is written and never read back is a
           different (and cheaper) event than one the query then re-reads. */
        TempRead = Blocks(row.TempBlocksRead),
        TempWritten = Blocks(row.TempBlocksWritten),
        /* Aurora-only too: core PostgreSQL has no per-statement peak-memory figure at all, so this is
           not-applicable rather than a zero-byte grant. */
        PeakMemory = row.MaxPeakMemBytes is { } peak ? Bytes(peak) : NotApplicableText,
        WalWritten = Bytes(row.WalBytes),
        /* Null is "no text captured for this queryid yet" — text refreshes hourly, and a major-version
           upgrade re-keys every queryid — which is a different statement from an empty query. */
        QueryText = row.QueryText ?? "(statement text not captured yet)",
    };

    internal sealed class DatabaseRow
    {
        public string DatabaseName { get; init; } = "";
        public string TempFiles { get; init; } = "";
        public string TempBytes { get; init; } = "";
        public string Deadlocks { get; init; } = "";
        public string CacheHitPct { get; init; } = "";
        public string XactCommit { get; init; } = "";
        public string XactRollback { get; init; } = "";
        public string RollbackPct { get; init; } = "";
        public int SampleCount { get; init; }

        /// <summary>The stored <c>stats_reset</c> timestamp. Shown beside the caveat that counts resets in
        /// the window, because "reset once" and "reset at 14:02" send you to different places.</summary>
        public string StatsReset { get; init; } = "";

        public string Caveats { get; init; } = "";
    }

    internal static DatabaseRow Database(DarlingPgDatabaseReader.PgDatabaseRow row)
    {
        var caveats = new List<string>();
        /* Every total beside it is a LOWER BOUND when the counters were reset inside the window, so this
           has to sit on the row rather than in a footnote nobody reads. */
        if (row.StatsResetCount > 0)
        {
            caveats.Add($"counters reset {row.StatsResetCount}x in window");
        }
        if (row.CounterRewindCount > 0)
        {
            caveats.Add($"counters rewound {row.CounterRewindCount}x");
        }

        return new DatabaseRow
        {
            DatabaseName = row.DatabaseName ?? "",
            TempFiles = Count(row.TempFiles),
            TempBytes = Bytes(row.TempBytes),
            Deadlocks = Count(row.Deadlocks),
            CacheHitPct = Percent(row.BlksHit, row.BlksHit + row.BlksRead),
            XactCommit = Count(row.XactCommit),
            XactRollback = Count(row.XactRollback),
            RollbackPct = Percent(row.XactRollback, row.XactCommit + row.XactRollback),
            SampleCount = row.SampleCount,
            StatsReset = Timestamp(row.StatsReset),
            Caveats = string.Join("; ", caveats),
        };
    }

    internal sealed class TableBloatRow
    {
        public string DatabaseName { get; init; } = "";
        public string SchemaName { get; init; } = "";
        public string TableName { get; init; } = "";
        public string HeapSize { get; init; } = "";

        /// <summary>The estimate, or the not-applicable dash when it was SUPPRESSED. Never the raw number
        /// with a caption beside it: a percentage rendered in a grid cell is read as a measurement whatever
        /// text sits next to it, and this one can be 81 percentage points wrong.</summary>
        public string BloatEstimate { get; init; } = "";

        public string BloatPctEstimate { get; init; } = "";

        /// <summary>The MEASURED fallback, from the server's own counters and needing no width model.
        /// Always populated, so a suppressed estimate still leaves the row saying something true.</summary>
        public string DeadTuplePct { get; init; } = "";

        public string ToastSize { get; init; } = "";
        public string IndexSize { get; init; } = "";
        public string HeapGrowth { get; init; } = "";

        /// <summary>Why the estimate is or is not shown, in a few words. The full reason is in the MCP
        /// read; this is what fits in a column and still stops somebody acting on a blank.</summary>
        public string Confidence { get; init; } = "";

        public bool EstimateSuppressed { get; init; }

        /// <summary>Only ever true for an estimate that was PUBLISHED - a suppressed row has no percentage
        /// to be high, and painting it red would assert the very thing the suppression denies.</summary>
        public bool IsHighBloat { get; init; }
    }

    /// <summary>
    /// Projects one bloat row for the grid.
    ///
    /// <para>The suppression decision comes from <see cref="DarlingPgTableBloatReader.EstimateIsUnpublishable"/>
    /// - the SAME call the MCP read makes - rather than from a second copy of the rule here. Two matching
    /// literals in two projects is exactly how the two surfaces would come to disagree about whether a
    /// number is publishable, silently and in the direction that shows a figure the other surface had
    /// already withheld.</para>
    /// </summary>
    internal static TableBloatRow TableBloat(DarlingPgTableBloatReader.PgTableBloatRow row)
    {
        var suppressed = DarlingPgTableBloatReader.EstimateIsUnpublishable(row);

        var confidence = row.EstimateUnavailable
            ? "no column statistics - check the monitoring login can SELECT this table"
            : row.LiveTuples < 0
                ? "never analyzed - no row count to compare against"
                : suppressed
                    ? "statistics stale - ANALYZE this table, then re-read"
                    : row.PgstattupleAvailable
                        ? "estimate; confirm exactly with pgstattuple"
                        : "estimate; pgstattuple is not installed here";

        return new TableBloatRow
        {
            DatabaseName = row.DatabaseName ?? "",
            SchemaName = row.SchemaName ?? "",
            TableName = row.TableName ?? "",
            HeapSize = Bytes(row.HeapBytes),
            BloatEstimate = suppressed ? NotApplicableText : Bytes(row.BloatBytesEstimate),
            BloatPctEstimate = suppressed
                ? NotApplicableText
                : string.Create(CultureInfo.CurrentCulture, $"{row.BloatPctEstimate:N2}%"),
            DeadTuplePct = row.LiveTuples < 0
                ? NotApplicableText
                : Percent(row.DeadTuples, row.LiveTuples + row.DeadTuples),
            ToastSize = Bytes(row.ToastBytes),
            IndexSize = Bytes(row.IndexBytes),
            HeapGrowth = ByteDelta(row.FirstHeapBytes, row.HeapBytes),
            Confidence = confidence,
            EstimateSuppressed = suppressed,
            IsHighBloat = !suppressed && row.BloatPctEstimate >= 50m,
        };
    }

    internal sealed class IndexUsageRow
    {
        public string DatabaseName { get; init; } = "";
        public string TableName { get; init; } = "";
        public string IndexName { get; init; } = "";
        public string IndexSize { get; init; } = "";
        public string ScansInWindow { get; init; } = "";

        /// <summary>The server's own lifetime counter, shown BESIDE the windowed figure rather than
        /// instead of it: they answer different questions, and an operator who checks in psql must not
        /// find this grid appearing to contradict the server.</summary>
        public string TotalScans { get; init; } = "";

        public string LastScan { get; init; } = "";
        public string BlockAccesses { get; init; } = "";
        public int SampleCount { get; init; }

        /// <summary>The short form of the droppability answer. Deliberately phrased as an answer to a
        /// question rather than as an instruction: no cell in this grid ever reads "drop it".</summary>
        public string Droppability { get; init; } = "";

        public string IndexDefinition { get; init; } = "";
        public bool IsInvalid { get; init; }

        /// <summary>Unscanned AND unblocked AND watched long enough - all three, so the amber never lands
        /// on a constraint index or on one we have only just met.</summary>
        public bool IsUnscanned { get; init; }
    }

    internal static IndexUsageRow IndexUsage(DarlingPgIndexUsageReader.PgIndexUsageRow row)
    {
        var blocked = row.IsPrimaryKey || row.SupportsConstraint || row.IsUnique || row.IsReplicaIdentity;
        var unscanned = row.IsValid && !blocked && row.ScansInWindow == 0 && row.SampleCount >= 2;

        var droppability = !row.IsValid
            ? "INVALID - never used by the planner, still maintained by writes"
            : row.IsPrimaryKey
                ? "no - backs the PRIMARY KEY"
                : blocked
                    ? (row.IsReplicaIdentity ? "no - is the REPLICA IDENTITY" : "no - backs a constraint")
                    : row.SampleCount < 2
                        ? "too early to say - only one sample"
                        : row.ScansInWindow > 0
                            ? "in use this window"
                            : row.IsPartial
                                ? "candidate - but PARTIAL, check the predicate still matches"
                                : row.IsExpression
                                    ? "candidate - but EXPRESSION, check the expression still matches"
                                    : "candidate - widen the window past your slowest job first";

        return new IndexUsageRow
        {
            DatabaseName = row.DatabaseName ?? "",
            TableName = row.TableName ?? "",
            IndexName = row.IndexName ?? "",
            IndexSize = Bytes(row.IndexBytes),
            ScansInWindow = Count(row.ScansInWindow),
            TotalScans = Count(row.TotalScans),
            /* NULL means two different things - PostgreSQL 15 and below do not record it at all, and on
               16+ it is an index never scanned since the reset - so the dash stands for "not recorded"
               rather than being filled with a fabricated date. The MCP read spells out which. */
            LastScan = Timestamp(row.LastScan),
            BlockAccesses = Count(row.BlocksHit + row.BlocksRead),
            SampleCount = row.SampleCount,
            Droppability = droppability,
            IndexDefinition = row.IndexDefinition ?? "",
            IsInvalid = !row.IsValid,
            IsUnscanned = unscanned,
        };
    }

    // ── Session states ───────────────────────────────────────────────────────────────────────────

    /// <summary>
    /// The share of the samples that saw a session in which it must have been the oldest xmin holder
    /// before the grid paints it as one.
    ///
    /// <para>Half, and it is a COPY of <c>DarlingMcpPgSessionStatesTools.SustainedHolderSampleShare</c>
    /// rather than a reference to it: the viewer does not reference the service project, so the constant
    /// cannot be reached from here. The duplication is deliberate and pinned —
    /// <c>ViewerPostgresTabsTests</c> asserts this projection's flags agree with the MCP's own severity
    /// band on the same row, in the one assembly that references both — because the alternative is two
    /// surfaces quietly disagreeing about whether a session is starving vacuum.</para>
    ///
    /// <para>The threshold itself is the argument from the read: every write transaction is momentarily
    /// the oldest holder, so a single sighting proves only that the instance does writes. Sustained
    /// holding is the finding.</para>
    /// </summary>
    internal const double SustainedHolderSampleShare = 0.5;

    /// <summary>
    /// How long a session must have sat idle in transaction, while pinning NOTHING, before the grid says
    /// anything about it. Five minutes, the copy of
    /// <c>DarlingMcpPgSessionStatesTools.IdleWithoutHorizonAttentionMs</c> — see above for why it is a copy.
    /// <para>It costs vacuum nothing, so the amber is not the vacuum argument: at five minutes it is holding
    /// a connection and whatever locks the transaction already took, which is a forgotten commit.</para>
    /// </summary>
    internal const long IdleWithoutHorizonAttentionMs = 5 * 60 * 1000;

    /// <summary>What the horizon-age cell shows for the <c>-1</c> sentinel. WORDS, not the em dash the rest
    /// of this class uses for a missing measurement, and that difference is the entire feature: -1 here is a
    /// positive finding — this session pinned nothing — and the dash would file it under "not measured",
    /// which is the reading that gets a harmless session killed.</summary>
    internal const string PinsNothingText = "pins nothing";

    internal sealed class SessionStateRow
    {
        public int Pid { get; init; }

        /// <summary>The collector's synthetic (backend_start, pid) identity, shown because a pid alone is
        /// not one: pids are reused, and this is the id that matches the same backend on the blocking
        /// grid.</summary>
        public long BackendId { get; init; }

        public string DatabaseName { get; init; } = "";
        public string Username { get; init; } = "";
        public string ApplicationName { get; init; } = "";
        public string ClientAddr { get; init; } = "";
        public string BackendType { get; init; } = "";
        public string LastState { get; init; } = "";
        public string LastWait { get; init; } = "";

        /// <summary>The leading SQL keyword, whitelisted at collection. Not a truncation of the statement:
        /// pg_stat_activity.query carries literal parameter values, so no raw text is stored anywhere.</summary>
        public string LastCommandTag { get; init; } = "";

        /// <summary>The normalised statement identity, to join against the Activity tab's query grid. Blank
        /// rather than 0 when absent — PostgreSQL 13 has no such column, and on 14+ it is NULL whenever
        /// compute_query_id is off, neither of which is a query whose id happens to be zero.</summary>
        public string LastQueryId { get; init; } = "";

        /// <summary><b>The column this panel exists for.</b> An age in transactions, and the words in
        /// <see cref="PinsNothingText"/> when the session held neither a snapshot nor a transaction id in
        /// any sample.</summary>
        public string PeakHorizonAge { get; init; } = "";

        public string PeakXminAge { get; init; } = "";
        public string PeakXidAge { get; init; } = "";
        public string PeakXactDuration { get; init; } = "";
        public string PeakStateDuration { get; init; } = "";
        public string PeakQueryDuration { get; init; } = "";

        /// <summary>How long the BACKEND has existed, against how long it has held its transaction. The
        /// pair separates two bugs that look identical in the transaction duration alone: a connection
        /// created ten minutes ago that has been idle in transaction for all ten is a pool handing out a
        /// session nobody finished with; a three-day-old worker that has held one for ten minutes is a
        /// code path that forgot to commit.</summary>
        public string BackendAge { get; init; } = "";

        /// <summary>How often this session was the oldest holder, as a fraction of the samples that saw it —
        /// never a bare count. One sighting in a hundred is normal write traffic; ninety-eight in a hundred
        /// is the reason vacuum reclaims nothing, and a count alone cannot tell them apart.</summary>
        public string HorizonHoldShare { get; init; } = "";

        public string IdleInTransactionShare { get; init; } = "";
        public int SampleCount { get; init; }
        public string FirstSeenAt { get; init; } = "";
        public string LastSeenAt { get; init; } = "";

        /// <summary>What the rest of the instance looked like in this backend's most recent sample. Two
        /// idle-in-transaction sessions out of six connections is a different server from two out of four
        /// thousand, and the row cannot be read without it.</summary>
        public string InstanceContext { get; init; } = "";

        /// <summary>Every qualifier on the row in one column — redaction, and a capture that hit the
        /// collector's per-capture cap so the stored set is a worst-first sample of a larger one.</summary>
        public string Caveats { get; init; } = "";

        /// <summary>Sustained holder: the red. Only ever true for a session seen holding the oldest xmin
        /// across at least <see cref="SustainedHolderSampleShare"/> of the samples that saw it, so a passing
        /// sighting — which every write transaction produces — is not painted as a cause.</summary>
        public bool IsSustainedHorizonHolder { get; init; }

        /// <summary>Idle in transaction past the attention threshold: the amber. A different finding from
        /// the red rather than a weaker one — the red is a session PROVEN to be setting the horizon, this
        /// is one holding a connection and its locks for minutes, so the two must never share a colour.
        /// <para>Not gated on whether it pinned anything: <c>is_horizon_holder</c> means OLDEST on the
        /// instance, so several real long transactions can each take turns being oldest and none of them
        /// reach the red. The horizon columns say separately whether this one pinned.</para></summary>
        public bool IsLongIdleTransaction { get; init; }

        /// <summary>The row's state columns came back NULL because the monitoring login lacks pg_monitor.
        /// Not a severity: nothing on this row is a trustworthy observation about the database, and painting
        /// it either healthy or critical would invent one.</summary>
        public bool StateUnknown { get; init; }
    }

    /// <summary>
    /// Projects one session-state row for the grid.
    ///
    /// <para><b><c>PeakHorizonAge</c> of <c>-1</c> renders as words, not as the dash.</b> Everywhere else in
    /// this class the <c>-1</c> sentinel means "not measured" and the em dash says so. Here it means the
    /// opposite of missing: the session held neither a snapshot nor a transaction id in any sample, which
    /// was MEASURED, and it is the finding that separates a session starving vacuum from one costing it
    /// nothing. Rendering it as a dash — or worse, as a number — is how a monitoring tool talks somebody
    /// into killing a harmless backend.</para>
    ///
    /// <para><b>The three flags are re-derived here rather than read off a severity string</b>, because the
    /// XAML row triggers bind to booleans and the MCP's band is a word. They are pinned against that band in
    /// the tests so the two surfaces cannot drift apart.</para>
    /// </summary>
    internal static SessionStateRow SessionState(DarlingPgSessionStatesReader.PgSessionStateRow r)
    {
        /* Redaction is asked FIRST and suppresses both other flags, for the same reason the read's finding
           does: with the state columns NULL, the inputs to every judgement below are missing, and a flag
           derived from them would report an absent GRANT as an observation about the workload. */
        var unknown = r.StateWasRedacted;

        var sustained = !unknown
            && r.SampleCount > 0
            && r.HorizonHolderSamples >= Math.Max(1, (int)Math.Ceiling(r.SampleCount * SustainedHolderSampleShare));

        /* Deliberately NOT gated on PeakHorizonAge < 0, and the gate that used to be here was a real
           cross-surface bug. is_horizon_holder means "the OLDEST holder on the instance", not "holds
           anything" - so on a busy instance several genuinely long idle transactions each take turns
           being oldest, none of them clears the sustained threshold, and every one of them pinned
           something. Requiring "pinned nothing" here painted all of them Healthy while the MCP band
           called them Warning. The band is "long idle transaction", full stop; whether it pins is an
           orthogonal fact the horizon columns already carry. */
        var longIdleTransaction = !unknown
            && r.IdleInTransactionSamples > 0
            && r.PeakStateDurationMs >= IdleWithoutHorizonAttentionMs;

        var caveats = new List<string>();
        if (r.StateWasRedacted)
        {
            caveats.Add("state columns REDACTED — the monitoring login lacks pg_monitor here");
        }

        if (r.CaptureWasTruncated)
        {
            caveats.Add("a capture hit the per-capture row cap, so this is a worst-first sample of more");
        }

        /* wait_event_type and wait_event are two halves of one name ("Lock: transactionid") and are worth
           little apart: the type alone is a category, the event alone repeats across categories. */
        var wait = string.Join(": ", new[] { r.LastWaitEventType, r.LastWaitEvent }
            .Where(part => !string.IsNullOrWhiteSpace(part)));

        return new SessionStateRow
        {
            Pid = r.Pid,
            BackendId = r.BackendId,
            DatabaseName = r.DatabaseName ?? "",
            Username = r.Username ?? "",
            ApplicationName = r.ApplicationName ?? "",
            ClientAddr = r.ClientAddr ?? "",
            BackendType = r.BackendType ?? "",
            LastState = r.LastState ?? "",
            LastWait = wait,
            LastCommandTag = r.LastCommandTag ?? "",
            LastQueryId = r.LastQueryId is { } queryId
                ? queryId.ToString("N0", CultureInfo.CurrentCulture)
                : string.Empty,
            PeakHorizonAge = r.PeakHorizonAge < 0
                ? PinsNothingText
                : string.Create(CultureInfo.CurrentCulture, $"{r.PeakHorizonAge:N0} transactions"),
            /* These two keep the ordinary dash. They are the components of the horizon age above, and an
               absent one is the narrower statement "this session held no snapshot" / "no transaction id" —
               which the horizon column has already said in words. */
            PeakXminAge = Count(r.PeakXminAge),
            PeakXidAge = Count(r.PeakXidAge),
            /* PEAKS, not averages. The finding is how far this went, and averaging a hundred samples of a
               transaction that grew monotonically reports about half of what actually happened. */
            PeakXactDuration = Milliseconds(r.PeakXactDurationMs),
            PeakStateDuration = Milliseconds(r.PeakStateDurationMs),
            PeakQueryDuration = Milliseconds(r.PeakQueryDurationMs),
            BackendAge = Milliseconds(r.PeakBackendDurationMs),
            HorizonHoldShare = r.SampleCount <= 0
                ? NotApplicableText
                : string.Create(CultureInfo.CurrentCulture,
                    $"{r.HorizonHolderSamples:N0} of {r.SampleCount:N0} samples"),
            IdleInTransactionShare = r.SampleCount <= 0
                ? NotApplicableText
                : string.Create(CultureInfo.CurrentCulture,
                    $"{r.IdleInTransactionSamples:N0} of {r.SampleCount:N0} samples"),
            SampleCount = r.SampleCount,
            FirstSeenAt = Timestamp(r.FirstSeenAt),
            LastSeenAt = Timestamp(r.LastSeenAt),
            InstanceContext = string.Create(CultureInfo.CurrentCulture,
                $"{r.IdleInTransactionSessions:N0} idle in xact / {r.ActiveSessions:N0} active / "
                + $"{r.TotalSessions:N0} sessions; {r.ReportableSessions:N0} reportable"),
            Caveats = string.Join("; ", caveats),
            IsSustainedHorizonHolder = sustained,
            IsLongIdleTransaction = longIdleTransaction,
            StateUnknown = unknown,
        };
    }

    /// <summary>One write-side metric, as a display row.</summary>
    internal sealed class WriteStatRow
    {
        public string Group { get; init; } = "";
        public string Metric { get; init; } = "";

        /// <summary>The change across the window, or an em dash when the value is NULL. The dash is
        /// deliberate and is NOT rendered as 0 — a metric can be null because this PostgreSQL major does not
        /// expose it, or because its statistics family was reset inside the window, and both of those are
        /// "we do not know" rather than "nothing happened".</summary>
        public string Value { get; init; } = "";

        public string Note { get; init; } = "";
    }

    /// <summary>
    /// Projects the single write-side row into one display row per metric (#2544), in causal reading order:
    /// checkpoints, then what the background writer did between them, then the WAL they were driven by.
    ///
    /// <para>A metric whose value is NULL is kept rather than dropped, and carries the reason in its note.
    /// Hiding it would answer the reader's next question ("where is buffers_backend?") with silence, and on
    /// a mixed-version fleet the answer differs per target: PostgreSQL 17 moved that counter to
    /// <c>pg_stat_io</c>, and 18 removed the WAL timing columns outright.</para>
    /// </summary>
    internal static List<WriteStatRow> WriteStatsRows(DarlingPgWriteStatsReader.PgWriteStatsRow row)
    {
        ArgumentNullException.ThrowIfNull(row);

        return new List<WriteStatRow>
        {
            Metric("Checkpoints", "Timed", row.CheckpointsTimed,
                "Checkpoints that began because checkpoint_timeout elapsed. This is the healthy kind."),
            Metric("Checkpoints", "Requested", row.CheckpointsRequested,
                "Began because WAL volume demanded one. Climbing against Timed is the classic "
                + "max_wal_size-too-small signal."),
            Metric("Checkpoints", "Completed", row.CheckpointsDone,
                "PostgreSQL 18+. Blank on earlier majors, which do not expose it."),
            MetricMs("Checkpoints", "Write Time (ms)", row.CheckpointWriteTimeMs,
                "Time spent writing buffers during checkpoints."),
            MetricMs("Checkpoints", "Sync Time (ms)", row.CheckpointSyncTimeMs,
                "Time spent in fsync during checkpoints. High here with low write time points at the "
                + "storage rather than at the volume."),
            Metric("Checkpoints", "Buffers Written", row.BuffersWrittenCheckpoint,
                "Buffers written by the checkpointer."),
            Metric("Checkpoints", "SLRU Written", row.SlruWritten,
                "PostgreSQL 18+. Blank on earlier majors."),
            Metric("Restartpoints", "Timed", row.RestartpointsTimed,
                "A standby's equivalent of a checkpoint. PostgreSQL 17+, and zero on a primary."),
            Metric("Restartpoints", "Requested", row.RestartpointsRequested,
                "PostgreSQL 17+, and zero on a primary."),
            Metric("Restartpoints", "Completed", row.RestartpointsDone,
                "PostgreSQL 17+. A standby where requested climbs and completed does not is falling behind "
                + "on replay."),
            Metric("Background Writer", "Buffers Cleaned", row.BuffersClean,
                "Buffers the background writer wrote out ahead of demand."),
            Metric("Background Writer", "Hit Max Written", row.MaxwrittenClean,
                "Cleaning rounds that stopped early because bgwriter_lru_maxpages was reached. Sustained "
                + "non-zero means the background writer is CAPPED rather than idle."),
            Metric("Background Writer", "Buffers Allocated", row.BuffersAlloc,
                "Buffers handed out — the denominator for how hard the pool is being churned."),
            Metric("Background Writer", "Buffers Written By Backends", row.BuffersBackend,
                "Queries writing their own buffers because nothing else kept up. Blank on PostgreSQL 17+, "
                + "which moved this to pg_stat_io — see the grid above, not a zero here."),
            Metric("Background Writer", "Backend fsyncs", row.BuffersBackendFsync,
                "Blank on PostgreSQL 17+ for the same reason."),
            Metric("WAL", "Records", row.WalRecords, "WAL records generated."),
            Metric("WAL", "Full Page Images", row.WalFpi,
                "Full-page writes. Spiking right after each checkpoint is the checkpoint_timeout-too-low "
                + "shape, and it is only legible next to the checkpoint counts above."),
            MetricBytes("WAL", "Bytes", row.WalBytes, "WAL volume generated across the window."),
            Metric("WAL", "Buffers Full", row.WalBuffersFull,
                "Times a backend had to flush WAL because wal_buffers was full."),
            Metric("WAL", "Writes", row.WalWrite,
                "Blank on PostgreSQL 18+, which removed the WAL write/sync counters."),
            Metric("WAL", "Syncs", row.WalSync, "Blank on PostgreSQL 18+."),
            MetricMs("WAL", "Write Time (ms)", row.WalWriteTimeMs, "Blank on PostgreSQL 18+."),
            MetricMs("WAL", "Sync Time (ms)", row.WalSyncTimeMs, "Blank on PostgreSQL 18+."),
        };

        /* Three overloads rather than one taking a formatted string, so a caller cannot accidentally pass
           "0" where the value was null. The em dash is produced HERE, once, from an actual null. */
        static WriteStatRow Metric(string group, string metric, long? value, string note) =>
            Row(group, metric, value?.ToString("N0", CultureInfo.CurrentCulture), note);

        static WriteStatRow MetricMs(string group, string metric, double? value, string note) =>
            Row(group, metric, value?.ToString("N1", CultureInfo.CurrentCulture), note);

        static WriteStatRow MetricBytes(string group, string metric, decimal? value, string note) =>
            Row(group, metric, value?.ToString("N0", CultureInfo.CurrentCulture), note);

        static WriteStatRow Row(string group, string metric, string? formatted, string note) => new()
        {
            Group = group,
            Metric = metric,
            /* EM DASH, never "0". A null here means either that this PostgreSQL major does not expose the
               counter or that its statistics family was reset inside the window - both "unknown", and both
               the opposite of "nothing happened". */
            Value = formatted ?? "\u2014",
            Note = note,
        };
    }
}
