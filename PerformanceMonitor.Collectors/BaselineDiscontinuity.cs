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
using System.Text.Json.Serialization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// One baseline discontinuity rendered from the store (#3653 A5, the rendered-marker clause): the instant a
/// carrier collector observed that a monitored server's identity epoch had moved and forgot the affected
/// delta baselines, so the series on either side of <see cref="At"/> were measured against different
/// instances or different counters and a step across it is not a change in the workload.
/// </summary>
/// <param name="At">The carrier run's <c>collection_log.collection_time</c> — naive UTC in the store's frame, never converted here; each surface converts for display.</param>
/// <param name="Reason">One word from <see cref="BaselineDiscontinuities.Reasons"/> saying what moved.</param>
/// <param name="Detail">The account: which facts moved and to what where the store still holds them, which collector runs marked it.</param>
public sealed record BaselineDiscontinuity(DateTime At, string Reason, string Detail);

/// <summary>
/// The wire shape of one entry in a trend payload's <c>discontinuities[]</c> — the same three keys on both SKUs,
/// spelled once here so <c>get_query_duration_trend</c> on Lite and on Darling cannot name them differently.
/// <c>at</c> is the store's naive-UTC instant in round-trip form with no zone suffix, exactly as the points'
/// <c>time</c> beside it prints, so the two read as one frame.
/// </summary>
public sealed record BaselineDiscontinuityPayload(
    [property: JsonPropertyName("at")] string At,
    [property: JsonPropertyName("reason")] string Reason,
    [property: JsonPropertyName("detail")] string Detail);

/// <summary>
/// The read side of identity-epoch detection (#3653 A5): how a trend surface finds the discontinuities inside
/// its window and what it says about each. <see cref="ServerEpoch"/> is the write side — a carrier collector
/// compares the pair of facts a target reports about itself against the pair persisted under its own name,
/// and on a change forgets the affected baselines, measures <c>identity_epoch_changes=1</c> (or the statements
/// / postmaster sibling) onto its run's <c>collection_log</c> note through the #3161 seam, and persists the new
/// pair with the pair it replaced beside it. Until this file the store carried all of that and no surface
/// rendered any of it: a chart whose baselines were forgotten at 03:12 showed a flat minute and a step, and
/// nothing on the chart, the MCP payload or the web page said the step was the instrument re-baselining rather
/// than the workload changing.
///
/// <para><b>The marker home is the <c>collection_log</c> row, and only the count survives per event.</b> Each
/// carrier run that observed an epoch wrote <c>label=1</c> onto its row; that row's <c>collection_time</c> is
/// WHEN, its <c>collector_name</c> is WHO, and the label is WHICH epoch (instance identity, statements, or
/// postmaster). WHAT moved — the old and new pair — is not on the row. <c>collector_state</c> holds one pair
/// per carrier (<see cref="ServerEpoch.IdentityStateKey"/> beside <see cref="ServerEpoch.IdentityPreviousStateKey"/>),
/// and it describes the LATEST change only: the next epoch overwrites it. So the reason word for an identity
/// event is derived from the pair when the pair is that event's — the carrier's most recent identity marker in
/// the window, with the pair's <c>updated_at</c> within <see cref="PairMatchTolerance"/> of the row — and is the
/// honest generic <see cref="IdentityReason"/> otherwise, with the detail saying why the specific word is not
/// recoverable. Making every event's reason specific needs the pair persisted PER EVENT, which is a schema rung
/// and is not written here; the read renders what the store holds.</para>
///
/// <para><b>The reason vocabulary.</b> An instance-identity epoch splits by which half of the
/// <see cref="ServerEpoch.Stamp"/> moved under the write side's own "unknown is not different" rule: the start
/// time alone → <see cref="RestartReason"/> (the same instance, restarted); the name alone →
/// <see cref="RenameReason"/>; both → <see cref="FailoverReason"/> (the connection now reaches a DIFFERENT
/// instance — an AG listener or a Multi-AZ endpoint that moved, or a registration re-pointed at another host —
/// which is the case #3694 built the pair to catch, since a different instance has both a different name and a
/// different start time). The statements epoch is <see cref="StatsResetReason"/> and the postmaster epoch is
/// <see cref="PostmasterRestartReason"/>; each names its one fact, so no split is needed.</para>
///
/// <para><b>Two carriers, one event, one marker.</b> Both SQL Server carriers (<c>wait_stats</c> first in the
/// order, <c>cpu_utilization</c> tenth) compare against their OWN persisted pair, so on the pass that first sees
/// an epoch both write a marker row, seconds to minutes apart, and #3705's memo makes only the first forget.
/// Rendering both would show one restart twice, so <see cref="Compose"/> folds same-epoch observations into
/// one discontinuity by the rule that a carrier observes one event once: walking a family's markers in time
/// order, an observation joins the open cluster when its collector has not yet contributed to that cluster, and
/// opens a new one when it has. No time tolerance decides this — the gap between the two carriers is however
/// long the eight collectors between them took, which on a heavy instance is minutes — and the rule stays
/// right when a carrier that was disabled comes back and observes an old change late (it joins the cluster
/// the other carrier opened, because that IS the same event). The rendered instant is the FIRST observation,
/// which is where the forget happened; the detail names every collector run that marked it. What the rule
/// cannot fold is a carrier that observed, failed at its store write (no pair persisted), and observed the same
/// change again on its next run: two rows from one collector, two markers, each a true observation.</para>
///
/// <para><b>One SQL text, both stores.</b> <see cref="MarkerRowsSql"/> and <see cref="IdentityPairSql"/> are
/// written once and executed unchanged by Npgsql against Darling's PostgreSQL store (the
/// <c>collect,config,public</c> search path resolves the bare table names, as every other collection_log read
/// relies on) and by DuckDB.NET against Lite's — both take positional <c>$n</c> parameters, both spell
/// <c>LIKE</c>, <c>IN</c> and <c>timestamp</c> comparison the same way, and the two tables have the same
/// columns on both stores (V1 / V44 and Lite's Schema.cs twins). The <c>LIKE</c> arms are a PREFILTER over the
/// rows that carry a note at all, not the truth: <c>_</c> is a single-character wildcard, so the pattern admits
/// a label it does not name, and <see cref="CollectorMeasurementNote.TryReadCount"/> — the grammar's own
/// token-exact inverse — decides in C#. The pair read is skipped when no identity marker was found.</para>
/// </summary>
public static class BaselineDiscontinuities
{
    /// <summary>The trailing key every trend payload carries on both SKUs — an array, empty when the window holds no discontinuity.</summary>
    public const string PayloadKey = "discontinuities";

    /// <summary>The instance restarted: <c>sqlserver_start_time</c> moved and <c>@@SERVERNAME</c> did not.</summary>
    public const string RestartReason = "restart";

    /// <summary>The instance was renamed: <c>@@SERVERNAME</c> moved and the start time did not.</summary>
    public const string RenameReason = "rename";

    /// <summary>The connection reaches a different instance: both the name and the start time moved — a listener or endpoint that failed over, or a registration re-pointed at another host.</summary>
    public const string FailoverReason = "failover";

    /// <summary>The instance identity moved, and the store no longer holds this event's old/new pair, so which half moved is not recoverable (see the class remarks).</summary>
    public const string IdentityReason = "identity";

    /// <summary><c>pg_stat_statements_info.stats_reset</c> moved: the statements family's counters were reset.</summary>
    public const string StatsResetReason = "stats_reset";

    /// <summary><c>pg_postmaster_start_time()</c> moved: PostgreSQL restarted, and Aurora's wait counters with it.</summary>
    public const string PostmasterRestartReason = "postmaster_restart";

    /// <summary>Every word <see cref="BaselineDiscontinuity.Reason"/> can carry, so a census can pin the vocabulary closed.</summary>
    public static readonly IReadOnlyList<string> Reasons = new[]
    {
        RestartReason, RenameReason, FailoverReason, IdentityReason, StatsResetReason, PostmasterRestartReason,
    };

    /// <summary>
    /// The one sentence every trend tool's description gains on both SKUs, naming the key and what a reader
    /// should do with it. Spelled once so the twenty descriptions cannot drift, and pinned by the census.
    /// </summary>
    public const string DescriptionSentence =
        " discontinuities[] lists every baseline discontinuity inside the window as { at, reason, detail } — an instant where the target's identity epoch moved (restart, failover, rename, stats_reset, postmaster_restart) and every delta baseline for the affected family was forgotten, so the points on either side of it were measured against different instances or different counters and a step across it is the instrument re-baselining, not the workload changing. Empty when the window holds none.";

    /// <summary>
    /// How far a carrier's persisted pair may sit from the marker row it belongs to and still be read as that
    /// event's pair. The pair is saved by the runner and the row by the host, seconds apart on Darling (state
    /// first, then the row at write time) and on Lite the other way round by the run's duration (the row carries
    /// the run's START time, the state is saved after the run). Ten minutes is far past either and far short of
    /// the next pass at which a further epoch could overwrite the pair; a pair further away than this describes
    /// some other event and the marker takes the generic reason.
    /// </summary>
    public static readonly TimeSpan PairMatchTolerance = TimeSpan.FromMinutes(10);

    /// <summary>
    /// The marker rows for one server inside a window — every <c>collection_log</c> row whose note carries one
    /// of the three epoch labels. <c>$1</c> server_id, <c>$2</c>/<c>$3</c> the window (naive UTC, both sides
    /// inclusive, like every trend read beside it). Oldest first, then by collector, so <see cref="Compose"/>'s
    /// walk is deterministic. Built from the label constants by concatenation so a renamed label reds the read
    /// at compile time rather than leaving it matching a token nothing writes — the
    /// <c>EnumeratedCollectorDriver.AbandonedByNotePredicateSql</c> idiom.
    /// </summary>
    public const string MarkerRowsSql =
        "SELECT collection_time, collector_name, error_message\n"
        + "FROM collection_log\n"
        + "WHERE server_id = $1\n"
        + "AND   collection_time >= $2\n"
        + "AND   collection_time <= $3\n"
        + "AND   error_message IS NOT NULL\n"
        + "AND   (error_message LIKE '%" + ServerEpoch.IdentityChangesMeasurement + "=%'\n"
        + "   OR  error_message LIKE '%" + ServerEpoch.StatementsChangesMeasurement + "=%'\n"
        + "   OR  error_message LIKE '%" + ServerEpoch.PostmasterChangesMeasurement + "=%')\n"
        + "ORDER BY collection_time, collector_name";

    /// <summary>
    /// The instance-identity pairs every carrier on one server has persisted — current beside replaced, with the
    /// instant each was written. <c>$1</c> server_id. No collector filter: a carrier is whichever collector
    /// declared the keys, and naming them here would be a second roster to keep in step with
    /// <c>CollectorStateContractTests</c>.
    /// </summary>
    public const string IdentityPairSql =
        "SELECT collector_name, state_key, state_value, updated_at\n"
        + "FROM collector_state\n"
        + "WHERE server_id = $1\n"
        + "AND   state_key IN ('" + ServerEpoch.IdentityStateKey + "', '" + ServerEpoch.IdentityPreviousStateKey + "')";

    /// <summary>One <see cref="MarkerRowsSql"/> row as the store returned it.</summary>
    public readonly record struct MarkerRow(DateTime CollectionTime, string CollectorName, string? Note);

    /// <summary>One <see cref="IdentityPairSql"/> row as the store returned it.</summary>
    public readonly record struct StateRow(string CollectorName, string StateKey, string StateValue, DateTime UpdatedAt);

    /// <summary>
    /// The rendered sentence, one spelling for the two desktop viewers and the web page:
    /// <c>baseline discontinuity at 2026-09-20 03:12 (restart)</c>. <paramref name="displayAt"/> is the instant
    /// ALREADY converted to the surface's display clock — this method does not know which clock a surface shows
    /// and must not guess.
    /// </summary>
    public static string Sentence(DateTime displayAt, string reason)
        => "baseline discontinuity at " + displayAt.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture) + " (" + reason + ")";

    /// <summary>The payload projection — <see cref="BaselineDiscontinuityPayload"/> per item, oldest first, the store's frame with no zone suffix.</summary>
    public static IReadOnlyList<BaselineDiscontinuityPayload> ToPayload(IReadOnlyList<BaselineDiscontinuity> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        var payload = new BaselineDiscontinuityPayload[items.Count];
        for (var i = 0; i < items.Count; i++)
        {
            payload[i] = new BaselineDiscontinuityPayload(
                DateTime.SpecifyKind(items[i].At, DateTimeKind.Unspecified).ToString("o", CultureInfo.InvariantCulture),
                items[i].Reason,
                items[i].Detail);
        }

        return payload;
    }

    /// <summary>
    /// Which half of the identity moved between the pair the epoch replaced and the pair it persisted — the
    /// class remarks' split, under <see cref="ServerEpoch.IsNewEpoch"/>'s own rule that a component unknown on
    /// either side is no evidence. <see cref="IdentityReason"/> only for a pair that agrees on every known
    /// component, which a persisted pair cannot (it was written because <see cref="ServerEpoch.IsNewEpoch"/> was
    /// true) unless the store's two rows are from different events.
    /// </summary>
    public static string DeriveIdentityReason(ServerEpoch.Stamp replaced, ServerEpoch.Stamp current)
    {
        var startMoved = replaced.StartTime.HasValue && current.StartTime.HasValue
            && replaced.StartTime.Value != current.StartTime.Value;
        var nameMoved = replaced.Name is not null && current.Name is not null
            && !string.Equals(replaced.Name, current.Name, StringComparison.OrdinalIgnoreCase);

        if (startMoved && nameMoved)
        {
            return FailoverReason;
        }

        if (nameMoved)
        {
            return RenameReason;
        }

        return startMoved ? RestartReason : IdentityReason;
    }

    /// <summary>
    /// The discontinuities a window holds, from the marker rows and the persisted pairs the two SQL texts
    /// returned — the whole read-side rule in one place so the three store readers (Darling's MCP and viewer
    /// over Npgsql, Lite over DuckDB) execute SQL and nothing else. Oldest first. Empty for no rows.
    /// </summary>
    public static IReadOnlyList<BaselineDiscontinuity> Compose(IReadOnlyList<MarkerRow> rows, IReadOnlyList<StateRow> state)
    {
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(state);

        if (rows.Count == 0)
        {
            return Array.Empty<BaselineDiscontinuity>();
        }

        var observations = new List<Observation>();
        foreach (var row in rows)
        {
            if (string.IsNullOrEmpty(row.CollectorName))
            {
                continue;
            }

            foreach (var label in Labels)
            {
                if (CollectorMeasurementNote.TryReadCount(row.Note, label, out var count) && count > 0)
                {
                    observations.Add(new Observation(row.CollectionTime, row.CollectorName, label));
                }
            }
        }

        if (observations.Count == 0)
        {
            return Array.Empty<BaselineDiscontinuity>();
        }

        /* The identity reason from the pair, for each carrier's LATEST identity marker only (the class
           remarks): the pair describes the most recent change under that carrier's name, and an older marker
           of the same carrier is a change the pair no longer describes. */
        var latestIdentityByCollector = observations
            .Where(o => o.Label == ServerEpoch.IdentityChangesMeasurement)
            .GroupBy(o => o.Collector, StringComparer.Ordinal)
            .ToDictionary(g => g.Key, g => g.Max(o => o.At), StringComparer.Ordinal);

        var resolved = new List<Resolved>(observations.Count);
        foreach (var o in observations)
        {
            resolved.Add(Resolve(o, latestIdentityByCollector, state));
        }

        var result = new List<BaselineDiscontinuity>();
        foreach (var family in resolved.GroupBy(r => r.Label, StringComparer.Ordinal))
        {
            result.AddRange(Cluster(family.OrderBy(r => r.At).ThenBy(r => r.Collector, StringComparer.Ordinal).ToList()));
        }

        result.Sort((a, b) => a.At.CompareTo(b.At));
        return result;
    }

    private static readonly string[] Labels =
    {
        ServerEpoch.IdentityChangesMeasurement,
        ServerEpoch.StatementsChangesMeasurement,
        ServerEpoch.PostmasterChangesMeasurement,
    };

    private readonly record struct Observation(DateTime At, string Collector, string Label);

    private readonly record struct Resolved(DateTime At, string Collector, string Label, string Reason, string Detail, bool Specific);

    private static Resolved Resolve(Observation o, Dictionary<string, DateTime> latestIdentityByCollector, IReadOnlyList<StateRow> state)
    {
        if (o.Label == ServerEpoch.StatementsChangesMeasurement)
        {
            return new Resolved(o.At, o.Collector, o.Label, StatsResetReason,
                "pg_stat_statements_info.stats_reset moved — the statements family's counters were reset (pg_stat_statements_reset(), a crash the extension's saved statistics did not survive, or an endpoint now reaching an instance with its own history), and its delta baselines were forgotten and re-established on the next pass",
                Specific: true);
        }

        if (o.Label == ServerEpoch.PostmasterChangesMeasurement)
        {
            return new Resolved(o.At, o.Collector, o.Label, PostmasterRestartReason,
                "pg_postmaster_start_time() moved — PostgreSQL restarted (or the endpoint now reaches a different instance), so the in-memory wait counters started from zero and the wait family's delta baselines were forgotten and re-established on the next pass",
                Specific: true);
        }

        /* Identity: the pair, if it is this event's. */
        if (latestIdentityByCollector.TryGetValue(o.Collector, out var latest) && latest == o.At
            && TryReadPair(state, o.Collector, o.At, out var replaced, out var current))
        {
            var reason = DeriveIdentityReason(replaced, current);
            return new Resolved(o.At, o.Collector, o.Label, reason,
                "instance identity changed — start time " + Describe(replaced.StartTime) + " → " + Describe(current.StartTime)
                + ", server name " + Describe(replaced.Name) + " → " + Describe(current.Name)
                + "; every delta baseline for this server was forgotten and re-established on the next pass",
                Specific: reason != IdentityReason);
        }

        return new Resolved(o.At, o.Collector, o.Label, IdentityReason,
            "instance identity changed (sqlserver_start_time or @@SERVERNAME moved) and every delta baseline for this server was forgotten and re-established on the next pass; the old and new pair for THIS event is not stored — collector_state holds only the latest change's pair — so whether this was a restart, a rename or a failover is not recoverable from the store",
            Specific: false);
    }

    private static bool TryReadPair(IReadOnlyList<StateRow> state, string collector, DateTime at, out ServerEpoch.Stamp replaced, out ServerEpoch.Stamp current)
    {
        replaced = default;
        current = default;
        StateRow? previousRow = null;
        StateRow? currentRow = null;
        foreach (var row in state)
        {
            if (!string.Equals(row.CollectorName, collector, StringComparison.Ordinal))
            {
                continue;
            }

            if (row.StateKey == ServerEpoch.IdentityPreviousStateKey)
            {
                previousRow = row;
            }
            else if (row.StateKey == ServerEpoch.IdentityStateKey)
            {
                currentRow = row;
            }
        }

        if (previousRow is not StateRow previous || currentRow is not StateRow now)
        {
            return false;
        }

        /* The replaced pair is written only on an epoch, so ITS updated_at dates the event; the current pair is
           also rewritten when a component merely becomes known, so its instant proves nothing. */
        if ((previous.UpdatedAt - at).Duration() > PairMatchTolerance)
        {
            return false;
        }

        return ServerEpoch.TryParse(previous.StateValue, out replaced) && !replaced.IsEmpty
            && ServerEpoch.TryParse(now.StateValue, out current) && !current.IsEmpty;
    }

    /* The class remarks' clustering rule: one family's observations, oldest first; a collector observes one
       event once, so a repeat of a collector already in the open cluster is the next event. */
    private static IEnumerable<BaselineDiscontinuity> Cluster(List<Resolved> family)
    {
        var open = new List<Resolved>();
        foreach (var r in family)
        {
            if (open.Count > 0 && open.Any(m => string.Equals(m.Collector, r.Collector, StringComparison.Ordinal)))
            {
                yield return Fold(open);
                open = new List<Resolved>();
            }

            open.Add(r);
        }

        if (open.Count > 0)
        {
            yield return Fold(open);
        }
    }

    private static BaselineDiscontinuity Fold(List<Resolved> members)
    {
        /* The most specific account any member could give, else the first member's generic one. Two members
           with different specific words would be two carriers disagreeing about one transition, which the
           write side's merge rule does not produce; the first in time order is taken. */
        var leadIndex = members.FindIndex(m => m.Specific);
        var lead = leadIndex >= 0 ? members[leadIndex] : members[0];

        var observedBy = string.Join(", ", members.Select(m => m.Collector));
        var detail = lead.Detail + "; observed by " + observedBy
            + (members.Count > 1
                ? " (" + members.Count.ToString(CultureInfo.InvariantCulture) + " collector runs marked one epoch; the instant shown is the first)"
                : string.Empty);

        return new BaselineDiscontinuity(members[0].At, lead.Reason, detail);
    }

    private static string Describe(DateTime? value)
        => value.HasValue ? value.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) : "unknown";

    private static string Describe(string? value)
        => string.IsNullOrEmpty(value) ? "unknown" : value;
}
