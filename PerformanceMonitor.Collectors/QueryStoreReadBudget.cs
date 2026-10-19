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
/// drain. The watermark refresh before it and the plan and text fetches after it are outside it. Darling's plan-XML fetch
/// has its own per-database budget, <see cref="PlanFetchBudget"/>, and so does its statement-text fetch,
/// <see cref="TextFetchBudget"/> (#6102).</para>
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
    /// The per-database, per-pass budget on Darling's plan-XML fetch that follows the read (#5813). In the field the read
    /// budget alone did not bound the slow runs: 30 runs of 30 s or more in a week, 97 s on average, spent 91 s of it in
    /// the plan fetch (about 136 plans a run, up to 2 s a plan on the server) and 1 to 3 s in the read, and the worst runs
    /// reached 231 s and 266 s. 30 s for the same reason as <see cref="Budget"/>: a tenth of the 5-minute cadence, so
    /// read plus fetch stay inside a fifth of it per database.
    ///
    /// <para><b>Cuts are the safety net, not the mechanism.</b> Each chunk asks for <see cref="PlanFetchWidth"/> plans,
    /// predicted from the database's last measured time per plan, so a slow database asks for fewer plans and fits
    /// instead of being cut. When a chunk is cut anyway, the plans already received are written and every id not
    /// received stays in the database's plan-fetch backlog, which the next pass fetches first. Plans are not the
    /// watermark's subject, so the watermark is unaffected either way.</para>
    ///
    /// <para><b>A cut usually receives nothing.</b> The fetch statement decompresses every requested plan into a temp
    /// table before it ships the first row, so a cut inside the statement is normally empty. The runner counts a cut
    /// pass that received no plan as a fetch failure (the #2776 backoff) and records the time per plan as at least twice
    /// the cut chunk's measured share (<see cref="NextMsPerPlan"/>), so the next pass asks for at most half as many and
    /// the same width is never cut empty twice.</para>
    /// </summary>
    public static readonly TimeSpan PlanFetchBudget = TimeSpan.FromSeconds(30);

    /// <summary>
    /// The share of the time left a predicted plan-fetch chunk aims to fill (#5813). Not all of it: a width sized to end
    /// exactly at the budget is cut whenever the next pass runs a little slower than the last, and a cut chunk usually
    /// receives nothing (see <see cref="PlanFetchBudget"/>), so a database whose time per plan only wobbles would be cut
    /// empty every other pass. A quarter of headroom absorbs that wobble; a larger swing is what the cut is for.
    /// </summary>
    public const double PlanFetchHeadroom = 0.75;

    /// <summary>
    /// How many plans the next plan-fetch chunk asks for (#5813):
    /// <c>clamp(floor(timeLeft x PlanFetchHeadroom / msPerPlan), floor, maxWidth)</c>, where <paramref name="msPerPlan"/>
    /// is the database's last measured time per plan. With no measurement yet the width is <paramref name="maxWidth"/>
    /// (the 400 ids per statement the fetch has always used). The floor is 1 for a pass's first chunk, so every database
    /// asks for at least one plan each pass and a slow one still progresses, and 0 for a later chunk, which means "stop:
    /// the time left does not fit one more plan, leave the rest to the next pass". No time left is always 0. Worked
    /// figures at a 30 s budget: 2 s a plan asks for 11, 56 ms or less asks for 400.
    /// </summary>
    public static int PlanFetchWidth(TimeSpan timeLeft, double? msPerPlan, int maxWidth, bool firstChunk) =>
        FetchWidth(timeLeft, msPerPlan, maxWidth, firstChunk, PlanFetchHeadroom);

    private static int FetchWidth(TimeSpan timeLeft, double? msPerId, int maxWidth, bool firstChunk, double headroom)
    {
        if (timeLeft <= TimeSpan.Zero || maxWidth <= 0)
        {
            return 0;
        }

        if (msPerId is not double perId || perId <= 0 || double.IsNaN(perId) || double.IsInfinity(perId))
        {
            return maxWidth;
        }

        var predicted = Math.Floor(timeLeft.TotalMilliseconds * headroom / perId);
        var floor = firstChunk ? 1 : 0;
        if (predicted <= floor)
        {
            return floor;
        }

        return predicted >= maxWidth ? maxWidth : (int)predicted;
    }

    /// <summary>
    /// The time per plan a plan-fetch pass leaves for the next one (#5813). A pass that received plans measured it: its
    /// target milliseconds over the plans received (a cut chunk's wasted time included, which can only make the next
    /// width narrower). A pass cut before it received anything measured only a lower bound, so it records twice the cut
    /// chunk's share, <c>2 x targetMs / cutWidth</c>: the next width is then at most half the cut one, the halving that
    /// ends a cut-empty loop. Any other pass (nothing received, nothing cut) keeps the previous figure.
    /// </summary>
    public static double? NextMsPerPlan(double? previous, long targetMs, int plansReceived, bool cut, int cutWidth) =>
        NextMsPerId(previous, targetMs, plansReceived, cut, cutWidth);

    private static double? NextMsPerId(double? previous, long targetMs, int received, bool cut, int cutWidth)
    {
        if (received > 0)
        {
            return Math.Max(targetMs, 1L) / (double)received;
        }

        if (cut && cutWidth > 0)
        {
            return 2.0 * Math.Max(targetMs, 1L) / cutWidth;
        }

        return previous;
    }

    /// <summary>
    /// The per-database, per-pass budget on Darling's statement-text fetch (#6102), the twin of
    /// <see cref="PlanFetchBudget"/> and a separate budget: text and plans are fetched by two statements with their own
    /// measured cost per id. In the field (#6102, Azure SQL Database targets, 3.10.0) the text fetch ran 20 to 27 s of
    /// target time for 320 to 580 ids on a pass, about 45 ms an id, with nothing bounding it but the byte budget and the
    /// 500 s command timeout. 30 s for the reason <see cref="PlanFetchBudget"/> gives, a tenth of the 5-minute cadence, so
    /// read plus plan fetch plus text fetch stay inside three tenths of it per database; the measured field runs sit
    /// under it and keep their full width, and what it bounds is the pass that grows past them.
    ///
    /// <para>The mechanism is the plan side's: each chunk asks for <see cref="TextFetchWidth"/> ids, predicted from the
    /// database's last measured time per id; a chunk cut anyway keeps the texts it received, and every id not received
    /// stays in the text backlog that the next pass fetches first. The text statement also materialises every requested
    /// text into a temp table before it ships a row, so a cut usually receives nothing; the runner then counts the pass as
    /// a fetch failure (the #2776 backoff) and records a time per id (<see cref="NextMsPerText"/>) that halves the next
    /// width.</para>
    /// </summary>
    public static readonly TimeSpan TextFetchBudget = TimeSpan.FromSeconds(30);

    /// <summary>The share of the time left a predicted text-fetch chunk aims to fill (#6102): the same quarter of
    /// headroom as <see cref="PlanFetchHeadroom"/>, for the same reason (a width sized to end exactly at the budget is cut
    /// whenever the next pass runs a little slower, and a cut usually receives nothing).</summary>
    public const double TextFetchHeadroom = 0.75;

    /// <summary>
    /// How many ids the next text-fetch chunk asks for (#6102):
    /// <c>clamp(floor(timeLeft x TextFetchHeadroom / msPerText), floor, maxWidth)</c>, with <paramref name="msPerText"/>
    /// the database's last measured time per text id. No measurement yet is <paramref name="maxWidth"/> (the 1000 ids per
    /// statement the fetch has always used). The floor is 1 for a pass's first chunk and 0 for a later one ("stop, leave
    /// the rest to the next pass"); no time left is always 0. Worked figures at a 30 s budget: 45 ms an id asks for 500,
    /// 22 ms or less asks for 1000.
    /// </summary>
    public static int TextFetchWidth(TimeSpan timeLeft, double? msPerText, int maxWidth, bool firstChunk) =>
        FetchWidth(timeLeft, msPerText, maxWidth, firstChunk, TextFetchHeadroom);

    /// <summary>
    /// The time per text id a text-fetch pass leaves for the next one (#6102), the rules of <see cref="NextMsPerPlan"/>:
    /// target milliseconds over the ids received; twice the cut chunk's share when a cut received nothing; otherwise the
    /// previous figure.
    /// </summary>
    public static double? NextMsPerText(double? previous, long targetMs, int textsReceived, bool cut, int cutWidth) =>
        NextMsPerId(previous, targetMs, textsReceived, cut, cutWidth);

    /// <summary>
    /// The words <c>get_collection_health</c> appends to a timed-out database's name (#5813). Rendered, never stored:
    /// <c>collection_log.slowest_item</c> is an identity column (the Query Store clutter reader groups by it as the
    /// database name and checks it against the server's current databases), so it carries the plain database name and
    /// the timeout is read off the value beside it. See <see cref="RenderSlowestItem"/>.
    /// </summary>
    public const string TimedOutMarker = " (timed out)";

    /// <summary>
    /// The <c>slowest</c> name <c>get_collection_health</c> shows (#5813 review): the stored name, plus
    /// <see cref="TimedOutMarker"/> when the item is a query_store database whose cost is exactly <see cref="Budget"/>,
    /// the milliseconds <see cref="ObserveTimeout"/> records for a read the budget ended. Exactly, not at least: a
    /// database that completed observes its read, plan and text fetches and its store write together, which can pass
    /// 30 s with no timeout in it. A completed database whose total lands on 30,000 ms to the millisecond reads as
    /// timed out; that is the price of no new column.
    /// </summary>
    public static string? RenderSlowestItem(string? collectorName, string? slowestItem, int? slowestItemMs) =>
        slowestItem is not null
        && slowestItemMs == (int)Budget.TotalMilliseconds
        && string.Equals(collectorName, QueryStoreCollector.Instance.Name, StringComparison.Ordinal)
            ? slowestItem + TimedOutMarker
            : slowestItem;

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

    /// <summary>
    /// Counts a timed-out read in the run's fan-out cost, with the budget as its milliseconds and the plain database
    /// name, so the timed-out database is the run's slowest item unless a completed one cost more. Returns false, and
    /// observes nothing, for any other exception. The item's SQL time is already in the run's blended total: the
    /// enumerated driver adds every item's slice in its finally, and both products' Azure per-database loops add a
    /// failed database's slice in their catch arms (#5813 review), so counting it here keeps the item count on the same
    /// denominator.
    /// </summary>
    public static bool ObserveTimeout(Exception exception, FanoutCostAccumulator fanout)
    {
        ArgumentNullException.ThrowIfNull(fanout);
        if (exception is not QueryStoreReadTimeoutException timeout)
        {
            return false;
        }

        fanout.Observe(timeout.Database, (long)timeout.Budget.TotalMilliseconds);
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
