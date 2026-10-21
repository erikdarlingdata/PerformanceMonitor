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

namespace PerformanceMonitor.Collectors;

/// <summary>Which server a Query Store read goes to (#6254).</summary>
public enum QueryStoreReadSource
{
    /// <summary>The server's own connection: today's read.</summary>
    Primary = 0,

    /// <summary>The readable secondary named by the server's <c>query_store_replica</c> setting.</summary>
    Replica = 1,
}

/// <summary>
/// Why a database reads the primary although the server has a replica set (#6254). <see cref="None"/> is the
/// healthy case (a replica read), and also the answer for a read nobody tried to route: no replica set,
/// Azure SQL Database, or a database that is not in an availability group.
/// </summary>
public enum QueryStoreRouteReason
{
    None = 0,

    /// <summary>The replica side is not a readable, NON-capturing secondary (<c>actual_state = 1 AND readonly_reason &amp; 8 = 8</c> fails).</summary>
    NotMirror,

    /// <summary>The database flushes Query Store slower than every 900 s.</summary>
    FlushTooSlow,

    /// <summary>Redo on the replica is more than 5 minutes behind the primary's last commit.</summary>
    RedoBehind,

    /// <summary>One side has no <c>sys.dm_hadr_database_replica_states</c> row for the database, or no commit time.</summary>
    NoRedoState,

    /// <summary>The two sides' <c>group_database_id</c> differ: the replica host holds a different database of that name.</summary>
    WrongDatabase,

    /// <summary>The two servers are not the same major version, and the read's shape depends on it.</summary>
    VersionMismatch,

    /// <summary>After a failover the named host is the primary for this database.</summary>
    ReplicaIsPrimary,

    /// <summary>The replica connection or enumeration failed, or this database's replica read failed mid-run.</summary>
    ReplicaUnavailable,
}

/// <summary>What the primary says about one database that passed the enumeration probe.</summary>
/// <param name="Name">Database name.</param>
/// <param name="FlushIntervalSeconds"><c>flush_interval_seconds</c>; null when unreadable.</param>
public sealed record QueryStorePrimaryDatabaseState(string Name, int? FlushIntervalSeconds);

/// <summary>What the replica says about one database in its replica-mode enumeration.</summary>
/// <param name="Name">Database name.</param>
/// <param name="ActualState">
/// <c>sys.database_query_store_options.actual_state</c>. A readable, NON-capturing secondary reads 1, or 0 (OFF) on
/// SQL Server 2025 CU9 (measured, though its Query Store tables are readable); a capturing secondary reads 4.
/// </param>
/// <param name="ReadonlyReason"><c>readonly_reason</c>; bit 8 is "this is a readable secondary".</param>
/// <param name="FlushIntervalSeconds"><c>flush_interval_seconds</c>; null when unreadable.</param>
public sealed record QueryStoreReplicaDatabaseState(
    string Name,
    int ActualState,
    int ReadonlyReason,
    int? FlushIntervalSeconds);

/// <summary>One <c>sys.dm_hadr_database_replica_states</c> row with <c>is_local = 1</c>, read on one side.</summary>
/// <param name="Name">Database name.</param>
/// <param name="GroupDatabaseId"><c>group_database_id</c>: the same on every replica of one AG database.</param>
/// <param name="LastCommitTime"><c>last_commit_time</c>; null when the row has none.</param>
/// <param name="IsPrimaryReplica"><c>is_primary_replica</c>: this side is the primary for the database.</param>
public sealed record QueryStoreAgRedoState(string Name, Guid GroupDatabaseId, DateTime? LastCommitTime, bool IsPrimaryReplica);

/// <summary>Everything the routing decision for ONE database needs. Built by the host from the reads on both connections.</summary>
/// <param name="Database">Database name.</param>
/// <param name="ReplicaConfigured">The server's <c>query_store_replica</c> setting is not empty.</param>
/// <param name="IsAzureSqlDb">Azure SQL Database never routes to a replica in this version.</param>
/// <param name="ReplicaUnavailable">The replica connection or its enumeration failed this run.</param>
/// <param name="PrimaryMajorVersion">Major version of the primary; 0 when unknown.</param>
/// <param name="ReplicaMajorVersion">Major version of the replica; 0 when unknown.</param>
/// <param name="Primary">The primary's enumeration facts for the database (its flush interval); null when not read.</param>
/// <param name="HasCapturingSecondary">A capturing secondary feeds this database (#6279): <see cref="QueryStoreCapturingSecondaryTracker.HasCapturingSecondary"/>.</param>
/// <param name="Replica">The replica's enumeration facts; null when the replica does not list the database.</param>
/// <param name="PrimaryRedo">The primary's local AG row; null for a database outside an availability group.</param>
/// <param name="ReplicaRedo">The replica's local AG row; null when it has none.</param>
public sealed record QueryStoreRoutingInput(
    string Database,
    bool ReplicaConfigured,
    bool IsAzureSqlDb,
    bool ReplicaUnavailable,
    int PrimaryMajorVersion,
    int ReplicaMajorVersion,
    QueryStorePrimaryDatabaseState? Primary,
    QueryStoreReplicaDatabaseState? Replica,
    QueryStoreAgRedoState? PrimaryRedo,
    QueryStoreAgRedoState? ReplicaRedo,
    bool HasCapturingSecondary = false);

/// <summary>
/// The answer for one database (#6254). The runners act on it: connection to read, cutoff (via
/// <see cref="QueryStoreReplicaRouting.ComputeCutoff"/>), whether to stage the <c>qsowm:</c> stamp, and
/// whether a failure counts toward the #2111 shrink.
/// </summary>
public sealed record QueryStoreRouteDecision
{
    public required string Database { get; init; }

    public required QueryStoreReadSource Source { get; init; }

    public required QueryStoreRouteReason Reason { get; init; }

    /// <summary>The flush interval L is built from; for a replica read the larger of the primary's and the replica's reported value, the primary's otherwise.</summary>
    public required int FlushIntervalSeconds { get; init; }

    /// <summary>L, the look-back (flush interval plus margin), or zero when this read keeps today's cutoff.</summary>
    public required TimeSpan LookBack { get; init; }

    /// <summary>The read uses cutoff W - L and goes through the ship guard.</summary>
    public bool UsesLookBack => LookBack > TimeSpan.Zero;

    /// <summary>A capturing secondary feeds this database's Query Store (#6279), so L carries the longer margin.</summary>
    public required bool HasCapturingSecondary { get; init; }

    /// <summary>The database is in an availability group. Databases outside one are left out of <c>read_source</c>.</summary>
    public required bool IsAgDatabase { get; init; }

    /// <summary>A replica read always includes the open interval; the 30-minute throttle belongs to primary reads.</summary>
    public bool IncludeOpenIntervalEveryRun => Source == QueryStoreReadSource.Replica;

    /// <summary>
    /// False for a replica read: it must NOT stage the <c>qsowm:</c> open-interval stamp, or a later primary
    /// fallback would skip the open interval for 30 minutes on top of a snapshot that is already old.
    /// </summary>
    public bool StageOpenIntervalStamp => Source == QueryStoreReadSource.Primary;

    /// <summary>A failed replica read counts toward neither the #2111 shrink nor the failure count; a primary read's does.</summary>
    public bool FailureCountsTowardShrink => Source == QueryStoreReadSource.Primary;

    /// <summary><see cref="Reason"/> as the value written to <c>read_source</c> text.</summary>
    public string ReasonText => QueryStoreReplicaRouting.ReasonText(Reason);
}

/// <summary>The cutoff for one read, and the hole the clamp opened (#6254).</summary>
/// <param name="Cutoff">The <c>@cutoff_time</c> for the read; null when no watermark is stored (the first-run window applies).</param>
/// <param name="HoleFrom">Start of a hole the live path will not revisit, or null.</param>
/// <param name="HoleTo">End of that hole, or null.</param>
/// <param name="ClampFired">The catch-up clamp raised the cutoff.</param>
public sealed record QueryStoreCutoff(DateTime? Cutoff, DateTime? HoleFrom, DateTime? HoleTo, bool ClampFired);

/// <summary>
/// Which server a database's Query Store read goes to, and from where it starts (#6254, #6279). Pure: no
/// connections, no clock, no store. The host builds a <see cref="QueryStoreRoutingInput"/> per database from
/// its reads and acts on the <see cref="QueryStoreRouteDecision"/>.
///
/// <para><b>Why a look-back.</b> A readable replica shows rows late and out of order: a row first visible at
/// time v has <c>last_execution_time</c> t &gt;= v - F - R (F flush interval, R redo lag), while every stored row
/// came from an earlier read, so the stored watermark W is below v. A cutoff of W - (F + 5 min) therefore
/// never skips a row while R stays under 5 minutes. A capturing secondary (#6279) delivers its rows to the
/// primary 3 to 18.5 minutes late at F = 900 with their own old times: a measured bound plus margin
/// (F + 10 min), not a proof.</para>
///
/// <para><b>Which reads use it.</b> (a) every replica read; (b) every primary read of a database in an
/// availability group on a server whose <c>query_store_replica</c> is set, every fallback reason included;
/// (c) every primary read of a database that has a capturing secondary. All other reads keep today's cutoff
/// exactly. Every read that uses it goes through <see cref="QueryStoreShipGuard"/>.</para>
/// </summary>
public static class QueryStoreReplicaRouting
{
    /// <summary>A database that flushes slower than this reads the primary (the look-back would be too wide to pay off).</summary>
    public const int MaxReplicaFlushSeconds = 900;

    /// <summary>The default Query Store flush interval, assumed when a database's own cannot be read.</summary>
    public const int DefaultFlushSeconds = 900;

    /// <summary>The redo lag past which the replica is too stale to read.</summary>
    public static readonly TimeSpan MaxRedoLag = TimeSpan.FromMinutes(5);

    /// <summary>L = F + this, for every database without a capturing secondary.</summary>
    public static readonly TimeSpan LookBackMargin = TimeSpan.FromMinutes(5);

    /// <summary>L = F + this, for a database that has a capturing secondary (rows up to 18.5 minutes old at F = 900).</summary>
    public static readonly TimeSpan CapturingLookBackMargin = TimeSpan.FromMinutes(10);

    /// <summary>The wire value of a reason, as <c>read_source</c> and the logs show it.</summary>
    public static string ReasonText(QueryStoreRouteReason reason) => reason switch
    {
        QueryStoreRouteReason.NotMirror => "not_mirror",
        QueryStoreRouteReason.FlushTooSlow => "flush_too_slow",
        QueryStoreRouteReason.RedoBehind => "redo_behind",
        QueryStoreRouteReason.NoRedoState => "no_redo_state",
        QueryStoreRouteReason.WrongDatabase => "wrong_database",
        QueryStoreRouteReason.VersionMismatch => "version_mismatch",
        QueryStoreRouteReason.ReplicaIsPrimary => "replica_is_primary",
        QueryStoreRouteReason.ReplicaUnavailable => "replica_unavailable",
        _ => "",
    };

    /// <summary>L for a flush interval: F + 5 minutes, or F + 10 when a capturing secondary feeds the database.</summary>
    public static TimeSpan LookBackFor(int flushIntervalSeconds, bool hasCapturingSecondary) =>
        TimeSpan.FromSeconds(Math.Max(0, flushIntervalSeconds))
        + (hasCapturingSecondary ? CapturingLookBackMargin : LookBackMargin);

    /// <summary>Decides one database's source, reason and look-back.</summary>
    public static QueryStoreRouteDecision Decide(QueryStoreRoutingInput input)
    {
        ArgumentNullException.ThrowIfNull(input);

        var capturing = input.HasCapturingSecondary;
        var inAg = input.PrimaryRedo is not null;
        var primaryFlush = input.Primary?.FlushIntervalSeconds ?? DefaultFlushSeconds;

        /* Primary reads. The look-back applies to (b) a database in an availability group on a server with the
           setting on, whatever the reason it is not read from the replica, and (c) a database with a capturing
           secondary. Anything else keeps today's cutoff exactly. */
        QueryStoreRouteDecision Primary(QueryStoreRouteReason reason, bool agFallback)
        {
            var lookBack = (agFallback || capturing)
                ? LookBackFor(primaryFlush, capturing)
                : TimeSpan.Zero;
            return new QueryStoreRouteDecision
            {
                Database = input.Database,
                Source = QueryStoreReadSource.Primary,
                Reason = reason,
                FlushIntervalSeconds = primaryFlush,
                LookBack = lookBack,
                HasCapturingSecondary = capturing,
                IsAgDatabase = inAg,
            };
        }

        if (input.IsAzureSqlDb)
        {
            return Primary(QueryStoreRouteReason.None, agFallback: false);
        }

        if (!input.ReplicaConfigured)
        {
            return Primary(QueryStoreRouteReason.None, agFallback: false);
        }

        if (!inAg)
        {
            /* Not in an availability group: nothing to route to, and nothing for read_source to report. */
            return Primary(QueryStoreRouteReason.None, agFallback: false);
        }

        if (input.ReplicaUnavailable)
        {
            return Primary(QueryStoreRouteReason.ReplicaUnavailable, agFallback: true);
        }

        var replica = input.Replica;

        if (input.ReplicaRedo?.IsPrimaryReplica == true)
        {
            return Primary(QueryStoreRouteReason.ReplicaIsPrimary, agFallback: true);
        }

        /* A capturing secondary is never read as a replica: the named host itself captures (actual_state 4), or
           another secondary of the group does (HasCapturingSecondary), whose rows reach the replica
           later still. A readable non-capturing secondary reads state 1, or 0 on SQL Server 2025 (measured). */
        if (replica is null
            || capturing
            || replica.ActualState is not (0 or 1)
            || (replica.ReadonlyReason & 8) != 8)
        {
            return Primary(QueryStoreRouteReason.NotMirror, agFallback: true);
        }

        /* The flush that bounds how stale the secondary's rows are is the PRIMARY's: a secondary reports its own,
           unreplicated option row (measured on SQL Server 2022 CU26: the primary at 60 s shows 900 s and a 60-minute
           interval length on its non-capturing secondary). So the larger of the two decides both the 900 s gate and L. */
        if (replica.FlushIntervalSeconds is not int replicaReportedFlush)
        {
            return Primary(QueryStoreRouteReason.FlushTooSlow, agFallback: true);
        }

        var replicaFlush = Math.Max(replicaReportedFlush, input.Primary?.FlushIntervalSeconds ?? 0);
        if (replicaFlush > MaxReplicaFlushSeconds)
        {
            return Primary(QueryStoreRouteReason.FlushTooSlow, agFallback: true);
        }

        if (input.ReplicaRedo is null
            || input.PrimaryRedo!.LastCommitTime is not DateTime primaryCommit
            || input.ReplicaRedo.LastCommitTime is not DateTime replicaCommit)
        {
            return Primary(QueryStoreRouteReason.NoRedoState, agFallback: true);
        }

        if (input.PrimaryRedo.GroupDatabaseId != input.ReplicaRedo.GroupDatabaseId)
        {
            return Primary(QueryStoreRouteReason.WrongDatabase, agFallback: true);
        }

        if (input.PrimaryMajorVersion != input.ReplicaMajorVersion
            || input.PrimaryMajorVersion == 0)
        {
            return Primary(QueryStoreRouteReason.VersionMismatch, agFallback: true);
        }

        if (primaryCommit - replicaCommit > MaxRedoLag)
        {
            return Primary(QueryStoreRouteReason.RedoBehind, agFallback: true);
        }

        return new QueryStoreRouteDecision
        {
            Database = input.Database,
            Source = QueryStoreReadSource.Replica,
            Reason = QueryStoreRouteReason.None,
            FlushIntervalSeconds = replicaFlush,
            LookBack = LookBackFor(replicaFlush, hasCapturingSecondary: false),
            HasCapturingSecondary = false,
            IsAgDatabase = true,
        };
    }

    /// <summary>
    /// The decision after ONE database's replica read failed mid-run: the same database reads the primary in the
    /// same run, <see cref="QueryStoreRouteReason.ReplicaUnavailable"/>, with the primary's own flush interval for
    /// F. The failure counts toward neither the #2111 shrink nor the failure count.
    /// </summary>
    public static QueryStoreRouteDecision FallbackAfterReplicaReadFailure(QueryStoreRoutingInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        return Decide(input with { ReplicaUnavailable = true });
    }

    /// <summary>
    /// The cutoff for one read. <paramref name="rawWatermark"/> is the stored per-database watermark BEFORE any
    /// clamp. The catch-up clamp (<see cref="WatermarkPolicy.MaxCatchup"/>) is decided on the raw watermark, as
    /// it is today; a read that uses the look-back and is not clamped starts at W - L, and a clamped one starts at
    /// the clamp floor with the hole starting at W - L, so the rows the look-back would have re-read are inside
    /// the hole rather than lost.
    /// </summary>
    /// <param name="rawWatermark">The stored <c>MAX(last_execution_time)</c> for the database, or null.</param>
    /// <param name="now">The collection time.</param>
    /// <param name="lookBack">The decision's <see cref="QueryStoreRouteDecision.LookBack"/>; zero keeps today's cutoff.</param>
    /// <param name="previousReadWasCut">
    /// The previous read of this database was cut by the row cap or byte budget
    /// (<see cref="QueryStoreShipGuard.WasCut"/>): this read starts at W with no look-back, so the same full
    /// window cannot repeat. The hole (W - L, W) was recorded when the cut happened (<see cref="CutHole"/>).
    /// </param>
    public static QueryStoreCutoff ComputeCutoff(DateTime? rawWatermark, DateTime now, TimeSpan lookBack, bool previousReadWasCut = false)
    {
        var clamped = WatermarkPolicy.ClampCatchup(rawWatermark, now);
        var clampFired = rawWatermark.HasValue && clamped != rawWatermark;

        var useLookBack = lookBack > TimeSpan.Zero && !previousReadWasCut;
        if (!useLookBack || rawWatermark is null)
        {
            return new QueryStoreCutoff(
                clamped,
                clampFired ? rawWatermark : null,
                clampFired ? clamped : null,
                clampFired);
        }

        if (clampFired)
        {
            return new QueryStoreCutoff(clamped, rawWatermark.Value - lookBack, clamped, true);
        }

        return new QueryStoreCutoff(rawWatermark.Value - lookBack, null, null, false);
    }

    /// <summary>
    /// The hole a read cut by the row cap or byte budget leaves: the next read starts at W (the watermark the cut
    /// read's rows produced), so rows that appear late between W - L and W are not re-read by the live path and
    /// ride a hole for backfill instead. Backfill reads the primary.
    /// </summary>
    public static (DateTime From, DateTime To) CutHole(DateTime watermark, TimeSpan lookBack) =>
        (watermark - lookBack, watermark);

    /// <summary>
    /// The <c>read_source</c> text for one run, for example <c>replica 12, primary 2 (redo_behind: Sales)</c>,
    /// at most three database names. Null when the server has no replica set, and databases outside an
    /// availability group are left out. Null also when nothing remains to report.
    /// </summary>
    public static string? Summarize(IEnumerable<QueryStoreRouteDecision> decisions, bool replicaConfigured)
    {
        ArgumentNullException.ThrowIfNull(decisions);
        if (!replicaConfigured)
        {
            return null;
        }

        var replica = 0;
        var primary = 0;
        var reasons = new List<(QueryStoreRouteReason Reason, string Name)>();
        foreach (var d in decisions)
        {
            if (!d.IsAgDatabase)
            {
                continue;
            }

            if (d.Source == QueryStoreReadSource.Replica)
            {
                replica++;
            }
            else
            {
                primary++;
                if (d.Reason != QueryStoreRouteReason.None)
                {
                    reasons.Add((d.Reason, d.Database));
                }
            }
        }

        if (replica == 0 && primary == 0)
        {
            return null;
        }

        var text = string.Create(CultureInfo.InvariantCulture, $"replica {replica}, primary {primary}");
        if (reasons.Count == 0)
        {
            return text;
        }

        var shown = reasons.Take(3).Select(r => $"{ReasonText(r.Reason)}: {r.Name}");
        return text + " (" + string.Join(", ", shown) + (reasons.Count > 3 ? ", ..." : "") + ")";
    }
}
