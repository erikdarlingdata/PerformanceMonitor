/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using System.Threading;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The per-database time budget on the live Query Store runtime-stats read (#5813), shared by both products and both of
/// each product's fan-out shapes (the enumerated per-database driver and the Azure SQL Database per-database loop).
///
/// <para><b>What it bounds.</b> ONE statement: the runtime-stats read for one database, from the open to the end of the
/// drain. The watermark refresh before it and the plan and text fetches after it are outside it; those keep
/// <see cref="QueryStoreCollector.PerDatabaseWallClockBudget"/> and the collector's 500 s command timeout, which #2776
/// sized for plan fetches that legitimately run past a minute.</para>
///
/// <para><b>Why it exists.</b> Before this the read was bounded only by the 500 s command timeout and the 10-minute item
/// budget, so one database whose Query Store was slow to answer held a 5-minute collector for up to 144 s in the field.
/// That slowness is spiky, not constant: the slowest field database read in about 100 ms at the median and 12.6 s at
/// p95, with its long reads clustered in two windows of the day. A row cap would not help a read that is fast at the
/// median; a time budget does, and the next pass usually lands outside the spike.</para>
///
/// <para><b>What a timeout costs.</b> Nothing collected. The read ships no rows, so the database's watermark (the #5684
/// per-batch side table and the runner's cache) does not move, and the next pass reads from the same watermark: no
/// interval is lost and none is written twice. The other databases in the pass are read and written as usual. The run's
/// collection_log row keeps the existing vocabulary: the #2623 partial-failure note names the database and this
/// message when some databases succeeded, and a pass in which every database failed is an ERROR as #3754 ruled.</para>
/// </summary>
public static class QueryStoreReadBudget
{
    /// <summary>
    /// 30 seconds against the 5-minute default cadence: a tenth of the interval, so even a run that times out on
    /// several databases stays well inside its slot. It sits above every healthy read measured in the field (12.6 s at
    /// the slowest database's p95) and far below the spikes it stops (60 s and more, up to 144 s). A healthy database
    /// that ever needs more than 30 s keeps its watermark and finishes next pass; if it keeps timing out, the #2111
    /// adaptive shrink narrows its window and hands the skipped range to the backfill worker, as it does for any
    /// repeated per-database failure.
    /// </summary>
    public static readonly TimeSpan Budget = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Appended to the database name in <c>collection_log.slowest_item</c> when the slowest item is a read that timed
    /// out, so <c>get_collection_health</c> can name the database and say it timed out without a new column. The ms
    /// value beside it is the budget, the time the read was allowed.
    /// </summary>
    public const string TimedOutMarker = " (timed out)";

    /// <summary>
    /// Starts the read budget for one database: a linked source, so host shutdown still cancels the read, that also
    /// cancels after <paramref name="budget"/>. Null for every collector but query_store, so their reads stay as they were.
    /// </summary>
    public static CancellationTokenSource? Start(string collectorName, TimeSpan budget, CancellationToken outer) =>
        string.Equals(collectorName, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
            ? EnumeratedCollectorDriver.StartItemBudget(budget, outer)
            : null;

    /// <summary>Did THIS read's budget fire, as opposed to the host shutting down? Shutdown wins the ambiguous case, as
    /// in <see cref="EnumeratedCollectorDriver.ItemBudgetExpired"/>.</summary>
    public static bool Expired(CancellationTokenSource? readBudget, CancellationToken outer) =>
        EnumeratedCollectorDriver.ItemBudgetExpired(readBudget, outer);

    /// <summary>The <c>slowest_item</c> value for a database whose read timed out.</summary>
    public static string SlowestItemName(string database) => database + TimedOutMarker;

    /// <summary>
    /// Counts a timed-out read in the run's fan-out cost, with the budget as its milliseconds and the marked name, so
    /// the timed-out database is the run's slowest item unless a completed one cost more. Returns false, and observes
    /// nothing, for any other exception. The item's SQL time is already in the run's blended total (both loops add it
    /// in their finally), so counting it here keeps the item count on the same denominator.
    /// </summary>
    public static bool ObserveTimeout(Exception exception, FanoutCostAccumulator fanout)
    {
        ArgumentNullException.ThrowIfNull(fanout);
        if (exception is not QueryStoreReadTimeoutException timeout)
        {
            return false;
        }

        fanout.Observe(SlowestItemName(timeout.Database), (long)timeout.Budget.TotalMilliseconds);
        return true;
    }
}

/// <summary>
/// One database's Query Store runtime-stats read ran past <see cref="QueryStoreReadBudget.Budget"/> (#5813). A
/// <see cref="TimeoutException"/> because that is what it is; the per-database skip arms in both products catch it as an
/// ordinary per-item failure, and the message says what happens next.
/// </summary>
public sealed class QueryStoreReadTimeoutException : TimeoutException
{
    /// <summary>The message: <c>{0}</c> = the database, <c>{1}</c> = the budget as <see cref="EnumeratedCollectorDriver.DescribeBudget"/> renders it.</summary>
    public const string MessageFormat =
        "Query Store read for database [{0}] timed out at its {1} budget; the database keeps its watermark and the next pass reads it from there.";

    /// <summary><see cref="MessageFormat"/> parsed once (CA1863); the const stays the greppable text.</summary>
    private static readonly CompositeFormat s_message = CompositeFormat.Parse(MessageFormat);

    /// <summary>Creates the timeout for <paramref name="database"/>, keeping whatever the provider raised as the inner exception.</summary>
    public QueryStoreReadTimeoutException(string database, TimeSpan budget, Exception? innerException)
        : base(string.Format(CultureInfo.InvariantCulture, s_message, database, EnumeratedCollectorDriver.DescribeBudget(budget)), innerException)
    {
        Database = database;
        Budget = budget;
    }

    /// <summary>The database whose read timed out.</summary>
    public string Database { get; }

    /// <summary>The budget it ran out of.</summary>
    public TimeSpan Budget { get; }
}

/// <summary>
/// Throttles the timed-out warning to once per server, database and calendar UTC day (#5813), the #4428 pattern
/// <c>CollectorDeltaCalculator.NoteWaitStatsClear</c> uses (a per-key <see cref="DateOnly"/> of the last warning). A
/// database that times out on every pass is named once a day, again at the day boundary, and not on every pass; each
/// timeout still lands in the run's collection_log row. In memory: a restart simply warns once more.
/// </summary>
public sealed class QueryStoreReadTimeoutLog
{
    private readonly ConcurrentDictionary<(int ServerId, string Database), DateOnly> _lastWarned = new();

    /// <summary>True the first time a (server, database) pair times out on a UTC day, false for the rest of that day.</summary>
    public bool ShouldWarn(int serverId, string database, DateTime nowUtc)
    {
        var today = DateOnly.FromDateTime(nowUtc);
        var key = (serverId, database);
        if (_lastWarned.TryGetValue(key, out var last) && last == today)
        {
            return false;
        }

        _lastWarned[key] = today;
        return true;
    }
}
