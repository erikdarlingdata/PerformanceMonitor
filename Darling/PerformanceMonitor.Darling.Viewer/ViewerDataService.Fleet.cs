/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Media;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Fleet-wide NOC roll-up — the signature capability Darling's single central store enables that
/// NEITHER the Dashboard nor Lite can do. Each of those monitors one server per PerformanceMonitor
/// database, so they physically cannot rank or total servers against each other in SQL; Darling keys
/// every server's rows by <c>server_id</c> in ONE Postgres store, so a single cross-server GROUP-BY /
/// aggregate rolls the whole fleet up at once.
///
/// <para>This roll-up is deliberately split across two mechanisms so it can REUSE the per-server surface
/// rather than re-derive it:</para>
/// <list type="bullet">
/// <item><b>Fleet totals</b> (<see cref="GetFleetTotalsAsync"/> / <see cref="FleetTotalsSql"/>) are a
/// genuine single cross-server aggregate over the collector views — total blocking events and total
/// deadlocks in the window. Blocking honors the SAME per-server XE→DMV fallback the Overview cards use
/// (<c>ServerSummaryBlockingSql</c>): it counts per <c>server_id</c> from each source, takes the XE count
/// when a server has any XE row this window else its DMV count, then SUMs across the fleet — so the fleet
/// total reconciles with the sum of the per-server card counts. These are pure COUNT metrics with no C#
/// banding, so they belong in SQL.</item>
/// <item><b>Health-band counts, servers-with-collection-failures, and the worst-N ranking</b>
/// (<see cref="FleetRollup.Build"/>) are a pure C# reduction over the <see cref="ServerSummaryItem"/>
/// list the Overview already loads. They REUSE #1426's card banding verbatim: a server's fleet band comes
/// from <see cref="ServerSummaryItem.OverallMetricSeverity"/> + its freshness status (which themselves
/// reuse every per-metric band, including <see cref="CollectorHealthRow.HealthStatus"/> via the card's
/// FailedCollectorCount). Reproducing that six-metric composite in SQL would be inventing a parallel
/// banding, so the classification stays where the banding lives — in C#, over the already-read cards.</item>
/// </list>
///
/// <para>The totals read is windowed (parameterized, naive-UTC per the store convention). The Overview tab
/// has no per-tab toolbar; the caller passes the SAME one-hour window the per-server cards use, so the
/// fleet totals and the cards describe the same span.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// The cross-server fleet totals over a window — the read only Darling's central store can serve.
    /// <c>total_blocking_events</c> respects the Overview card's XE-preferred / DMV-fallback rule PER
    /// server before summing: each source is counted per <c>server_id</c> (a GROUP BY), a FULL OUTER JOIN
    /// lines the two sources up per server, and each server contributes its XE count when it has any XE row
    /// this window else its DMV count (Lite's <c>COALESCE(NULLIF(xe,0), dmv)</c>, applied per server) — so
    /// an AWS RDS server with only DMV snapshots still counts and a server with XE reports is never
    /// double-counted. <c>total_deadlocks</c> is a plain cross-server COUNT. $1 window start, $2 window end
    /// (both naive UTC).
    /// </summary>
    public const string FleetTotalsSql = @"
SELECT
    (
        SELECT COALESCE(SUM(CASE WHEN per_server.xe_count > 0 THEN per_server.xe_count ELSE per_server.dmv_count END), 0)
        FROM
        (
            SELECT
                COALESCE(xe.server_id, dmv.server_id) AS server_id,
                COALESCE(xe.cnt, 0) AS xe_count,
                COALESCE(dmv.cnt, 0) AS dmv_count
            FROM
            (
                SELECT server_id, COUNT(*) AS cnt
                FROM v_blocked_process_reports
                WHERE event_time >= $1
                AND   event_time <= $2
                GROUP BY server_id
            ) AS xe
            FULL OUTER JOIN
            (
                SELECT server_id, COUNT(*) AS cnt
                FROM v_dmv_blocking_snapshots
                WHERE event_time >= $1
                AND   event_time <= $2
                GROUP BY server_id
            ) AS dmv ON xe.server_id = dmv.server_id
        ) AS per_server
    ) AS total_blocking_events,
    (
        SELECT COUNT(*)
        FROM v_deadlocks
        WHERE deadlock_time >= $1
        AND   deadlock_time <= $2
    ) AS total_deadlocks";

    /// <summary>
    /// Reads the fleet's cross-server blocking + deadlock totals over the window. The single query that
    /// neither the Dashboard nor Lite can run (each sees one server's database). Window bounds are sent
    /// Kind=Unspecified (the naive-UTC store convention).
    /// </summary>
    public async Task<FleetTotals> GetFleetTotalsAsync(DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(FleetTotalsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (await reader.ReadAsync(cancellationToken))
        {
            return new FleetTotals
            {
                TotalBlockingEvents = reader.IsDBNull(0) ? 0 : Convert.ToInt64(reader.GetValue(0)),
                TotalDeadlocks = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
            };
        }

        return new FleetTotals();
    }
}

/// <summary>The cross-server fleet totals over a window — pure SQL aggregates keyed by <c>server_id</c>.</summary>
public sealed class FleetTotals
{
    /// <summary>Blocking events across the whole fleet this window (per-server XE→DMV fallback, then summed).</summary>
    public long TotalBlockingEvents { get; set; }

    /// <summary>Deadlocks across the whole fleet this window.</summary>
    public long TotalDeadlocks { get; set; }
}

/// <summary>
/// One entry in the fleet's worst-first "Needs attention" ranking — a problem server with its band, a
/// short human reason (built from the card's own metric displays), and its composite score. Clicking the
/// entry opens that server's tab (the caller maps <see cref="ServerId"/> back to the sidebar server and
/// calls MainWindow's OpenServerTab). Every display is a pure format of the underlying
/// <see cref="ServerSummaryItem"/>; the brushes reuse the card's severity palette.
/// </summary>
public sealed class FleetRankedServer
{
    private static readonly SolidColorBrush s_criticalBrush = MakeBrush("#E57373");
    private static readonly SolidColorBrush s_warningBrush = MakeBrush("#FFD54F");
    private static readonly SolidColorBrush s_healthyBrush = MakeBrush("#81C784");
    private static readonly SolidColorBrush s_offlineBrush = MakeBrush("#888888");

    public int ServerId { get; init; }
    public string DisplayName { get; init; } = "";
    public FleetHealthBand Band { get; init; }

    /// <summary>The composite worst-first ordering score (band rank + severity magnitude + incident tiebreak).</summary>
    public long Score { get; init; }

    /// <summary>A short "why it needs attention" line, e.g. "CPU 97%, Blocking 6" or "Offline — no recent collection".</summary>
    public string Reason { get; init; } = "";

    public string BandLabel => ServerHealthClassifier.BandLabel(Band);

    /// <summary>The band's dot / label brush — the card's severity palette (offline = the Unknown grey).</summary>
    public SolidColorBrush BandBrush => Band switch
    {
        FleetHealthBand.Critical => s_criticalBrush,
        FleetHealthBand.Warning => s_warningBrush,
        FleetHealthBand.Offline => s_offlineBrush,
        _ => s_healthyBrush,
    };

    private static SolidColorBrush MakeBrush(string hex)
    {
        var brush = new SolidColorBrush((Color)ColorConverter.ConvertFromString(hex));
        brush.Freeze();
        return brush;
    }
}

/// <summary>
/// The fleet-wide roll-up view-model rendered above the Overview's per-server cards: the fleet health
/// summary (total servers + counts by band), the fleet totals (cross-server blocking / deadlock sums +
/// servers with collection failures), and the worst-first "Needs attention" ranking. Built by
/// <see cref="Build"/> from the same <see cref="ServerSummaryItem"/> list the Overview loads plus the
/// <see cref="FleetTotals"/> SQL read — REUSING #1426's card banding for every band decision.
/// </summary>
public sealed class FleetRollup
{
    /// <summary>
    /// The default depth of the worst-first ranking (the "Needs attention" list caps at this).
    ///
    /// <para>Deliberately small, and deliberately unchanged by #2424 even though a 57-server fleet can leave
    /// 52 servers behind it. The ranking renders inside the fleet roll-up panel, which is docked to the top
    /// of the Overview and does NOT scroll — only the card grid beneath it does. A list that grew with the
    /// fleet would therefore push the cards it is pointing at off the screen, and it would stop being a
    /// triage shortlist ("look at these first") and become a second, worse copy of the grid without the
    /// metrics. The overflow is answered by giving it somewhere to go instead — the needs-attention card
    /// filter, over the grid that scrolls and carries the six metric rows.</para>
    /// </summary>
    public const int DefaultWorstCount = 5;

    /// <summary>An empty fleet (no servers registered) — every count zero, no ranking.</summary>
    public static readonly FleetRollup Empty = new();

    public int TotalServers { get; init; }
    public int HealthyCount { get; init; }
    public int WarningCount { get; init; }
    public int CriticalCount { get; init; }
    public int OfflineCount { get; init; }

    /// <summary>
    /// Registered servers whose per-server summary read did NOT complete this cycle (#2753 review finding)
    /// — <c>TotalServers</c> minus however many summaries actually loaded. Deliberately tracked separately
    /// from <see cref="ServersWithCollectionFailures"/>, which only reflects a LOADED summary's own FAILING
    /// collectors: a server that failed to load this cycle has no summary to ask, so it cannot be banded
    /// Healthy/Warning/Critical/Offline at all — it is neither known-healthy nor known-problem, and must
    /// not be silently counted as the former just because it is absent from the "problem" list.
    /// </summary>
    public int UnknownCount { get; init; }

    /// <summary>Servers whose 7-day collection banding flags at least one FAILING collector (card reuse).</summary>
    public int ServersWithCollectionFailures { get; init; }

    /// <summary>Fleet-wide blocking events this window (from the cross-server SQL total).</summary>
    public long TotalBlockingEvents { get; init; }

    /// <summary>Fleet-wide deadlocks this window (from the cross-server SQL total).</summary>
    public long TotalDeadlocks { get; init; }

    /// <summary>
    /// How much of the fleet <see cref="TotalDeadlocks"/> actually read a deadlock source for, with the
    /// causes named (#3029) — the shared <see cref="FleetDeadlockCoverage"/>, so this panel and the
    /// service's <c>get_fleet_overview</c> report the same denominator against the same total rather than
    /// two surfaces each having an opinion.
    ///
    /// <para><see cref="FleetDeadlockCoverage.ServersTotal"/> is <see cref="TotalServers"/>, the REGISTERED
    /// fleet — the same population the SQL total counts over, which is every server in the store and not
    /// just the ones whose card loaded this cycle. So its four causes need not add up to it: the shortfall
    /// is <see cref="UnknownCount"/>, the servers this cycle has no summary to classify, and that gap has
    /// its own line already (<see cref="UnknownStatusText"/>) rather than being attributed to a cause it
    /// was not measured to have.</para>
    /// </summary>
    public FleetDeadlockCoverage DeadlockCoverage { get; init; } = new();

    /// <summary>The worst-first problem servers (band != Healthy), capped at the requested depth.</summary>
    public IReadOnlyList<FleetRankedServer> WorstServers { get; init; } = Array.Empty<FleetRankedServer>();

    /// <summary>
    /// Problem servers beyond the capped ranking. Surfaced as the "+N more need attention" affordance,
    /// which is a LINK into the needs-attention card filter rather than a dead count (#2424): the ranking
    /// deliberately stays short (see <see cref="DefaultWorstCount"/>), so this number is the whole reason
    /// the filter exists.
    /// </summary>
    public int AdditionalProblemCount { get; init; }

    /// <summary>Any server needs attention (band != Healthy) — drives the ranking list vs the all-clear line.
    /// Deliberately NOT widened to include <see cref="UnknownCount"/>: the ranking is a list of specific
    /// servers to click into, and an unknown-status server cannot be ranked or clicked into, only counted.
    /// See <see cref="IsAllClear"/> for the property that gates the all-clear text.</summary>
    public bool HasProblems => WorstServers.Count > 0;

    /// <summary>
    /// True only when EVERY registered server is accounted for AND known-healthy — no ranked problems and
    /// no server whose status this cycle is simply unknown. #2753 review: before this, the all-clear text
    /// could read "All N servers healthy" while some of those N had no summary this cycle at all, which is
    /// an affirmative false claim, not an absent one — worse than the original undercount bug.
    /// </summary>
    public bool IsAllClear => !HasProblems && UnknownCount == 0;

    /// <summary>"+N more need attention" when the ranking overflows its cap, else empty.</summary>
    public string AdditionalProblemText =>
        AdditionalProblemCount > 0 ? $"+{AdditionalProblemCount} more need attention" : "";

    /// <summary>"N server(s) didn't report this cycle" when some registered server has no summary this
    /// cycle, else empty — the only place that gap is stated in words rather than silently absorbed into
    /// either the healthy or the problem counts.</summary>
    public string UnknownStatusText => UnknownCount switch
    {
        0 => "",
        1 => "1 server didn't report this cycle",
        _ => $"{UnknownCount} servers didn't report this cycle",
    };

    /// <summary>The all-clear affirmation shown when nothing needs attention. Content assumes the caller
    /// only renders this when <see cref="IsAllClear"/> is true — see that property's remarks.</summary>
    public string AllHealthyText => TotalServers == 1
        ? "All 1 server healthy"
        : $"All {TotalServers} servers healthy";

    /// <summary>"Monitoring N servers" subtitle for the fleet header.</summary>
    public string MonitoringText => TotalServers == 1
        ? "Monitoring 1 server"
        : $"Monitoring {TotalServers} servers";

    /// <summary>"N server(s)" for the collection-failures total line.</summary>
    public string CollectionFailuresText => ServersWithCollectionFailures == 1
        ? "1 server"
        : $"{ServersWithCollectionFailures} servers";

    /* ─────────────────────── the deadlock total's coverage (#3029) ─────────────────────── */

    /// <summary>
    /// What <see cref="TotalDeadlocks"/> is assembled from, and so why it needs a denominator at all. The
    /// leading sentence of <see cref="DeadlockCoverageTooltip"/>.
    /// </summary>
    public const string DeadlockSourceNote =
        "Deadlocks come from the SQL Server extended-event capture and nothing else, so a server this "
        + "total does not cover contributes nothing to it whatever that server's deadlocks do.";

    /// <summary>
    /// The sentence that keeps the two figures from being read as one measurement — the desktop wording of
    /// the same disclaimer the service's <see cref="FleetDeadlockCoverage.WindowNote"/> carries for an API
    /// reader.
    ///
    /// <para><b>The two windows genuinely diverge and that is the point of saying so.</b>
    /// <see cref="TotalDeadlocks"/> is counted over the last hour, the same window the cards use; coverage
    /// is banded over the FIXED trailing seven days of collection health, because whether a reader works is
    /// a durable fact and the banding thresholds are themselves defined in DAYS — an hour-wide health
    /// window could not produce a band at all. A note claiming both were read in the same window would be
    /// this issue's own defect one level up: a surface asserting a scope it did not measure.</para>
    /// </summary>
    public const string DeadlockCoverageWindowNote =
        "Coverage bands each server's deadlock reader over the fixed trailing seven days of collection "
        + "health - whether the reader works at all, which is a durable fact - while the deadlock total "
        + "counts only the last hour. The two windows differ deliberately, and this coverage figure "
        + "therefore makes no claim about what was read in the last hour.";

    /// <summary>What to do about the PostgreSQL arm, appended after its count. Names no tab, because a
    /// PostgreSQL target's deadlock grid is reached through that server's own tab rather than from here.
    ///
    /// <para>Every cause here is a VERB-FREE noun phrase, so one form reads correctly after both "1 server:"
    /// and "4 servers:" — an "N are ..." shape needs a second string the moment N is one, and the surface
    /// that forgets it prints "1 are PostgreSQL targets".</para></summary>
    public const string DeadlockPostgresCause =
        "PostgreSQL targets, whose deadlocks this total cannot count at all - collected separately, and "
        + "shown on that target's own server tab.";

    /// <summary>What to do about the silent arm, appended after its count.</summary>
    public const string DeadlockCollectorSilentCause =
        "no current deadlock collection - the collector has stopped being invoked, or has never run; the "
        + "server's Collection Health tab shows which.";

    /// <summary>What to do about the denied arm, appended after its count.</summary>
    public const string DeadlockCollectorDeniedCause =
        "every deadlock-collector attempt refused for permissions - needs a grant.";

    /// <summary>
    /// The arm the service's fleet reader has no equivalent of: a registered server whose per-server
    /// summary read did not complete this cycle (<see cref="UnknownCount"/>). It is uncovered for the same
    /// reason a null band is — nothing was read FOR IT, so counting it as read is how a coverage figure
    /// becomes another number nobody can trust — but it is deliberately NOT folded into
    /// <see cref="DeadlockCollectorSilentCause"/>: that one sends the reader to a collector, and this one is
    /// the viewer's own read having failed, which is a different thing to go and look at.
    /// </summary>
    public const string DeadlockUnreportedCause =
        "no summary this cycle, so whether the deadlock collection works could not be classified either "
        + "way.";

    /// <summary>Whether there is a fleet for the coverage figure to qualify. False only on an empty fleet,
    /// where the whole roll-up panel is collapsed anyway — never as a way of hiding a complete reading.</summary>
    public bool HasDeadlockCoverage => DeadlockCoverage.ServersTotal > 0;

    /// <summary>
    /// True when <see cref="TotalDeadlocks"/> did not read every registered server — the flag the coverage
    /// line's colour tracks. It tracks COVERAGE rather than the deadlock count's own severity: "read all 12
    /// servers" beside a large count is good news about a bad number and must not be painted as part of the
    /// alarm.
    /// </summary>
    public bool DeadlockCoverageIsPartial => DeadlockCoverage.ServersRead < DeadlockCoverage.ServersTotal;

    /// <summary>
    /// The coverage beside the deadlock total, in the "Label: value" shape the rest of that row already
    /// uses. Rendered whenever there is a fleet, INCLUDING at full coverage.
    ///
    /// <para><b>Always, rather than only when short.</b> A line that appeared only on partial coverage would
    /// make its ABSENCE the load-bearing signal, which a reader has to already know the rule to read, and it
    /// would leave "zero, whole fleet measured" looking identical to "zero, from a build that reports no
    /// coverage at all" — the same defect one step out. Present unconditionally, it says which one this is.
    /// The web fleet page's tile made the same call for the same reason.</para>
    ///
    /// <para>It carries its own subject rather than reading as a bare "read 0 of 12 servers": the row it
    /// sits in is a <see cref="System.Windows.Controls.WrapPanel"/> of independently-labelled phrases, so an
    /// unlabelled one could wrap onto the next line under a different figure and read as qualifying that.</para>
    /// </summary>
    public string DeadlockCoverageText
    {
        get
        {
            var total = DeadlockCoverage.ServersTotal;
            var noun = total == 1 ? "server" : "servers";

            return DeadlockCoverage.ServersRead >= total
                ? $"Deadlock coverage: read all {total} {noun}"
                : $"Deadlock coverage: read {DeadlockCoverage.ServersRead} of {total} {noun}";
        }
    }

    /// <summary>
    /// The hover detail behind <see cref="DeadlockCoverageText"/>: what the total is built from, only the
    /// causes that actually apply, and the two windows named against the figures they belong to.
    ///
    /// <para>DERIVED rather than assigned, for the reason
    /// <see cref="ServerSummaryItem.DeadlockSource"/> is: a settable string is one that can be omitted, or
    /// can drift from the counts it describes. Computed, the numbers and the sentence cannot disagree.</para>
    /// </summary>
    public string DeadlockCoverageTooltip
    {
        get
        {
            var note = new StringBuilder(DeadlockSourceNote)
                .Append(' ')
                .Append(DeadlockCoverageWindowNote);

            Cause(note, DeadlockCoverage.PostgresServers, DeadlockPostgresCause);
            Cause(note, DeadlockCoverage.ServersCollectorSilent, DeadlockCollectorSilentCause);
            Cause(note, DeadlockCoverage.ServersCollectorDenied, DeadlockCollectorDeniedCause);

            /* The viewer-only arm, last because it is about this cycle's read rather than about a target. */
            Cause(note, UnknownCount, DeadlockUnreportedCause);

            return note.ToString();
        }
    }

    /// <summary>
    /// One cause, appended only when it applies, as "N server(s): what it is" — so a reader is not handed
    /// four actions when one is called for.
    ///
    /// <para>The ONE place a count becomes words here, which is what keeps the four arms from disagreeing
    /// about the noun. Nothing in the cause strings inflects, so this needs no second form for N = 1.</para>
    /// </summary>
    private static void Cause(StringBuilder note, int count, string cause)
    {
        if (count <= 0)
        {
            return;
        }

        note.Append(' ').Append(count).Append(count == 1 ? " server: " : " servers: ").Append(cause);
    }

    /// <summary>
    /// Rolls the per-server Overview cards up into the fleet view-model. The band counts,
    /// servers-with-failures, and worst-first ranking reduce the <paramref name="summaries"/> using #1426's
    /// card banding (no new bands); the blocking / deadlock totals come from the cross-server
    /// <paramref name="totals"/> SQL read. <paramref name="worstCount"/> caps the "Needs attention" list.
    ///
    /// <para><paramref name="totalServerCount"/> is the REGISTERED fleet size — the same count the sidebar
    /// shows — and is what <see cref="TotalServers"/> reports. It defaults to <paramref name="summaries"/>'
    /// own count for callers (tests, mainly) that only ever pass a fully-loaded set, but the real caller
    /// MUST pass the registered count explicitly: <paramref name="summaries"/> is only the servers whose
    /// per-server summary read succeeded THIS cycle (#2753 — a transient per-server read failure silently
    /// drops that server from <paramref name="summaries"/>, so deriving the fleet total from its count made
    /// the Overview's "Total Servers" wobble cycle to cycle while the sidebar, sourced from the registry,
    /// never moved).</para>
    /// </summary>
    public static FleetRollup Build(IReadOnlyList<ServerSummaryItem> summaries, FleetTotals totals, int worstCount = DefaultWorstCount, int? totalServerCount = null)
    {
        var registeredTotal = totalServerCount ?? summaries.Count;
        var unknown = Math.Max(0, registeredTotal - summaries.Count);

        /* Reduced ONCE, above the early return, so the two exits cannot disagree about whether the deadlock
           total carries a denominator — a coverage object left at its default on one path would report every
           server uncovered on an empty fleet and nothing at all on a populated one. */
        var deadlockCoverage = ReduceDeadlockCoverage(summaries, registeredTotal);

        if (summaries.Count == 0)
        {
            return new FleetRollup
            {
                TotalServers = registeredTotal,
                UnknownCount = unknown,
                TotalBlockingEvents = totals.TotalBlockingEvents,
                TotalDeadlocks = totals.TotalDeadlocks,
                DeadlockCoverage = deadlockCoverage,
            };
        }

        var healthy = 0;
        var warning = 0;
        var critical = 0;
        var offline = 0;
        var failures = 0;

        foreach (var s in summaries)
        {
            switch (ClassifyBand(s))
            {
                case FleetHealthBand.Offline: offline++; break;
                case FleetHealthBand.Critical: critical++; break;
                case FleetHealthBand.Warning: warning++; break;
                default: healthy++; break;
            }

            /* Servers with collection failures — the card's own FAILING count (which reuses
               CollectorHealthRow.HealthStatus), not a re-derived SQL band. */
            if (s.FailedCollectorCount > 0)
            {
                failures++;
            }
        }

        /* Worst-first ranking: problem servers only (band != Healthy), highest composite score first,
           name as the stable tiebreak. The composite score keeps offline > critical > warning and, within
           a band, ranks by how many metrics are bad — see FleetHealthScore. */
        var problems = summaries
            .Where(s => ClassifyBand(s) != FleetHealthBand.Healthy)
            .OrderByDescending(FleetHealthScore)
            .ThenBy(s => s.DisplayName, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var worst = problems
            .Take(worstCount)
            .Select(s => new FleetRankedServer
            {
                ServerId = s.ServerId,
                DisplayName = s.DisplayName,
                Band = ClassifyBand(s),
                Score = FleetHealthScore(s),
                Reason = BuildReason(s),
            })
            .ToList();

        return new FleetRollup
        {
            TotalServers = registeredTotal,
            UnknownCount = unknown,
            HealthyCount = healthy,
            WarningCount = warning,
            CriticalCount = critical,
            OfflineCount = offline,
            ServersWithCollectionFailures = failures,
            TotalBlockingEvents = totals.TotalBlockingEvents,
            TotalDeadlocks = totals.TotalDeadlocks,
            DeadlockCoverage = deadlockCoverage,
            WorstServers = worst,
            AdditionalProblemCount = Math.Max(0, problems.Count - worst.Count),
        };
    }

    /// <summary>
    /// How much of the fleet <see cref="TotalDeadlocks"/> read a deadlock source for, reduced from the SAME
    /// cards every other count here comes from, so the coverage and the total it qualifies reconcile by
    /// construction rather than by two reads agreeing (#3029).
    ///
    /// <para><paramref name="registeredTotal"/> is the denominator, not <paramref name="summaries"/>' own
    /// count. The SQL total is a cross-server COUNT over the whole store, so the population it covers is the
    /// registered fleet; a denominator that shrank to whatever loaded this cycle would report a smaller
    /// fleet than exists, which is a new wrong number in place of the old one rather than a fix.</para>
    ///
    /// <para><b>Only <see cref="FleetDeadlockSource.Read"/> counts as read</b> — every other arm, INCLUDING
    /// an enum value a later build adds and this switch has never heard of, lands in the silent bucket. A
    /// new source kind that inflated the read count would restore exactly the defect this exists to fix,
    /// where one that lands in an uncovered bucket merely attributes a real gap imprecisely.</para>
    ///
    /// <para>The four causes therefore need not sum to <paramref name="registeredTotal"/>: a registered
    /// server with no summary this cycle is classified by none of them, and that shortfall is
    /// <see cref="UnknownCount"/> — stated in its own words by <see cref="UnknownStatusText"/> and by
    /// <see cref="DeadlockUnreportedCause"/>, rather than attributed to a cause it was not measured to
    /// have.</para>
    /// </summary>
    public static FleetDeadlockCoverage ReduceDeadlockCoverage(IReadOnlyList<ServerSummaryItem> summaries, int registeredTotal)
    {
        ArgumentNullException.ThrowIfNull(summaries);

        var read = 0;
        var postgres = 0;
        var silent = 0;
        var denied = 0;

        foreach (var s in summaries)
        {
            switch (s.DeadlockSource)
            {
                case FleetDeadlockSource.Read: read++; break;
                case FleetDeadlockSource.PostgresTarget: postgres++; break;
                case FleetDeadlockSource.CollectorDenied: denied++; break;
                default: silent++; break;
            }
        }

        return new FleetDeadlockCoverage
        {
            ServersRead = read,
            ServersTotal = registeredTotal,
            PostgresServers = postgres,
            ServersCollectorSilent = silent,
            ServersCollectorDenied = denied,
        };
    }

    /// <summary>
    /// Collapses a card's health to one fleet band — REUSING #1426's banding, mirroring
    /// <see cref="ServerSummaryItem.CardBorderBrush"/>: offline collection → Offline; else the card's
    /// worst metric band (<see cref="ServerSummaryItem.OverallMetricSeverity"/>) maps Critical → Critical,
    /// Warning → Warning, and a stale collection (<see cref="ServerSummaryItem.HasCollectorErrors"/>) is
    /// Warning too; otherwise Healthy. No new thresholds are introduced here.
    /// </summary>
    public static FleetHealthBand ClassifyBand(ServerSummaryItem s) =>
        ServerHealthClassifier.ClassifyBand(
            s.IsOnline,
            /* Via the card's discriminant, not the raw flag. The shared classifier honours an awaiting marker
               whatever IsOnline says, so an online card carrying a stray marker banded Warning while the card
               said "Online" and had nothing to report — a third reading of the same pair. See ServerCollectionStatus. */
            s.CardStatus == ServerCollectionStatus.AwaitingFirstCollection,
            s.HasCollectorErrors,
            s.OverallMetricSeverity);

    /// <summary>
    /// The worst-first ordering score — the SHARED <see cref="ServerHealthClassifier.FleetHealthScore"/> over
    /// the card's raw metrics: band rank dominates (Offline &gt; Critical &gt; Warning &gt; Healthy) in steps
    /// of 1000; within a band, how many card metrics are Critical / Warning, then the blocking + deadlock
    /// incident count as a tiebreak.
    /// </summary>
    public static long FleetHealthScore(ServerSummaryItem s) =>
        ServerHealthClassifier.FleetHealthScore(ClassifyBand(s), s.ToHealthMetrics());

    /// <summary>
    /// A short human reason for the ranking, built from the card's OWN metric displays so it never drifts
    /// from what the card shows. Offline states say so; otherwise it names the metrics that are Warning or
    /// Critical (CPU / Blocking / Deadlocks / Memory / Threads / Collectors), plus a stale-collection note.
    /// </summary>
    public static string BuildReason(ServerSummaryItem s)
    {
        /* Keyed on the card's own status discriminant rather than on the flags behind it. Reading
           AwaitingFirstCollection independently of IsOnline is what let an online card claim it was awaiting
           its first collection — see ServerCollectionStatus. */
        if (s.CardStatus == ServerCollectionStatus.Offline)
        {
            return "Offline — no recent collection";
        }

        if (s.CardStatus == ServerCollectionStatus.AwaitingFirstCollection)
        {
            /* The word itself, not a copy of it — this line held the fourth spelling of the phrase, in the
               fourth file, which is exactly the shape the #2473 pin now forbids. */
            return s.CardStatus.Word();
        }

        var parts = new List<string>();

        if (s.CpuSeverity >= HealthSeverity.Warning)
        {
            parts.Add($"CPU {s.CpuDisplay}");
        }
        if (s.ThreadsSeverity >= HealthSeverity.Warning)
        {
            parts.Add($"Threads {s.ThreadsDisplay}");
        }
        if (s.MemorySeverity >= HealthSeverity.Warning && s.HasMemoryPressure)
        {
            parts.Add($"Memory: {s.MemoryDetail}");
        }
        if (s.BlockingSeverity >= HealthSeverity.Warning && s.BlockingCount > 0)
        {
            parts.Add($"Blocking {s.BlockingCount}");
        }
        if (s.DeadlockSeverity >= HealthSeverity.Warning && s.DeadlockCount > 0)
        {
            parts.Add($"Deadlocks {s.DeadlockCount}");
        }
        if (s.CollectorSeverity >= HealthSeverity.Warning)
        {
            parts.Add($"{s.FailedCollectorCount} collector{(s.FailedCollectorCount == 1 ? "" : "s")} failing");
        }
        if (s.HasCollectorErrors)
        {
            parts.Add("collection stale");
        }

        return parts.Count > 0 ? string.Join(", ", parts) : UnspecifiedReason;
    }

    /// <summary>
    /// What <see cref="BuildReason"/> answers when it can name nothing — a card banded away from Healthy by a
    /// severity whose display the reason does not cover. It reads fine in the ranking, where every row is a
    /// problem server, and reads as an unexplained demand on a card, so the tooltip degrades to the band label
    /// rather than repeating it. Named so the two sides cannot drift apart on the spelling.
    /// </summary>
    public const string UnspecifiedReason = "Needs attention";

    /// <summary>The line every card tooltip ends on. The sidebar alert badge's tooltip has the same shape —
    /// the breakdown, then how to act on it — so the Overview card is not the surface that explains itself
    /// least; this one names the gesture the card actually supports.</summary>
    private const string CardTooltipAction = "Double-click the card to open this server's tab";

    /// <summary>
    /// The Overview card's status tooltip (#2422): the band the card's border is painted from, WHY it is in
    /// that band, and what to do next. Reported as "the card says Warning and will not say why" — the reader
    /// had to scan six metric rows hunting for the amber one, once per card, on a 57-server fleet.
    ///
    /// <para>It is <see cref="BuildReason"/>'s output verbatim, the same sentence the Needs Attention ranking
    /// already shows for that exact server. Re-deriving it here instead would forfeit the one property
    /// BuildReason exists for: it is built from the card's OWN metric displays, so it cannot disagree with the
    /// six rows the reader is looking at while they are looking at them.</para>
    /// </summary>
    public static string BuildStatusTooltip(ServerSummaryItem s)
    {
        ArgumentNullException.ThrowIfNull(s);

        return Headline(s) + "\n" + CardTooltipAction;
    }

    /// <summary>
    /// The tooltip's first line: what this card's state IS, in the words the card's own status line uses,
    /// followed by the reason when there is one to give.
    ///
    /// <para>It switches on <see cref="ServerSummaryItem.CardStatus"/> — the SAME discriminant
    /// <see cref="ServerSummaryItem.StatusDisplay"/> renders — rather than re-reading the flags underneath it.
    /// A tooltip that hangs off a word and then contradicts it is the defect this change exists to remove, and
    /// two independent readings of the same two flags is precisely how it comes back.</para>
    /// </summary>
    private static string Headline(ServerSummaryItem s)
    {
        var band = ClassifyBand(s);

        return s.CardStatus switch
        {
            /* Offline and never-reached come back from BuildReason as whole sentences that already name the
               state — the same sentence the status word shows — so a band label in front would only say
               "Offline" twice. */
            ServerCollectionStatus.Offline or ServerCollectionStatus.AwaitingFirstCollection => BuildReason(s),

            /* "Unknown" is the one status word with no band behind it: ClassifyBand goes straight to the
               metrics and, on a clean card, answers Healthy. The word wins, and the metrics are appended when
               they have something to add — not knowing whether a server is reporting is no reason to withhold
               the CPU number that WAS collected. */
            ServerCollectionStatus.Unknown => WithReason(UnknownStatus, "; ", s),

            /* Online and stale: the band is the headline. A healthy card gets an all-clear rather than
               BuildReason's "Needs attention" fallback, which is written for a ranking that only ever holds
               problem servers and on a grid showing EVERY server would say the opposite of the truth. */
            _ => band == FleetHealthBand.Healthy
                ? "Healthy — every metric on this card is inside its threshold"
                : WithReason(ServerHealthClassifier.BandLabel(band), " — ", s),
        };
    }

    /// <summary>
    /// A headline plus what the card can actually name — or the headline alone when it can name nothing.
    ///
    /// <para>Every arm that appends a reason goes through here, because a card CAN sit outside Healthy with
    /// nothing to say: a Blocking band raised by a long max-wait while the reason's own gate wants a non-zero
    /// event count, for one. Appending unguarded produces "Warning — Needs attention", which tells the reader
    /// exactly what they already knew and is how the ranking-only fallback reaches a card at all. Two arms had
    /// their own copy of the append and only one of them was guarded, which is the same lesson as
    /// <see cref="ServerCollectionStatus"/> one level down.</para>
    /// </summary>
    private static string WithReason(string headline, string separator, ServerSummaryItem s)
    {
        var reason = BuildReason(s);

        return reason == UnspecifiedReason ? headline : headline + separator + reason;
    }

    /// <summary>The words behind StatusDisplay's "Unknown" — a card whose freshness was never classified.</summary>
    private const string UnknownStatus = "Unknown — no collection status for this server";

    /// <summary>
    /// The problem servers among the Overview's cards — band != Healthy — kept in the order the caller handed
    /// them in, so the grid's chosen sort survives the filter. This is the SAME predicate <see cref="Build"/>
    /// uses to decide who is in the ranking and who counts toward <see cref="AdditionalProblemCount"/>, which
    /// is what makes "+52 more need attention" land on exactly 52 cards rather than on a second opinion.
    /// </summary>
    public static List<ServerSummaryItem> NeedsAttention(IEnumerable<ServerSummaryItem> summaries)
    {
        ArgumentNullException.ThrowIfNull(summaries);

        return summaries.Where(s => ClassifyBand(s) != FleetHealthBand.Healthy).ToList();
    }

    /// <summary>
    /// The count shown beside the filter toggle while it is on. A filtered grid that looks like an unfiltered
    /// one is a worse defect than the one the filter fixes, so the active state carries its own arithmetic —
    /// including the all-clear case, which is otherwise an empty grid with nothing saying why.
    /// </summary>
    public static string AttentionFilterCountText(int shown, int total)
    {
        if (shown > 0)
        {
            return $"showing {shown} of {total}";
        }

        /* Nothing left to show. On a populated fleet that is the honest all-clear; with no cards at all it
           must not report that zero servers are healthy, which is the sort of line that gets screenshotted. */
        return total switch
        {
            <= 0 => "no servers to filter",
            1 => "the 1 server monitored is healthy",
            _ => $"all {total} servers are healthy",
        };
    }
}
