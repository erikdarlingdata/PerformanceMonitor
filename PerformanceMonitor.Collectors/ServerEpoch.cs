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
using System.Runtime.CompilerServices;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Identity-epoch detection for a monitored server's cumulative counters (#3653 A5, the item #3540 A5
/// deferred): the pure comparison, the persisted form, and the one call a collector definition makes
/// when it has the current identity in hand.
///
/// <para><b>The lie this ends.</b> Every delta family subtracts this pass's counter from a baseline
/// cached under the server's <c>server_id</c>, and nothing checked that the two readings came from the
/// same instance. A restarted instance, an AG listener or RDS Multi-AZ endpoint that now lands on the
/// other replica, a registration re-pointed at a different host under the same id, a
/// <c>pg_stat_statements_reset()</c> — each leaves the baseline describing counters that no longer exist.
/// The calculator's reset branch catches the readings that FELL (a restart's near-zero counters) and
/// reports them (0, 0), unknowable; it cannot catch readings that ROSE, and a failover to a hotter
/// replica raises every counter at once — <c>new instance's total minus old instance's baseline</c>,
/// stored as one interval's work, the positive storm the item names. The series-age rescue (#2235) and
/// the restart seed (#3540 A4) both trust the pass window for the same reason and are wrong in the same
/// way when the identity moves under them.</para>
///
/// <para><b>The shape.</b> An epoch is a pair of facts the target reports about itself that change
/// exactly when its counters restart or belong to a different instance — for SQL Server
/// <c>sys.dm_os_sys_info.sqlserver_start_time</c> and <c>@@SERVERNAME</c>, for the PostgreSQL statements
/// family <c>pg_stat_statements_info.stats_reset</c>, for Aurora's wait family
/// <c>pg_postmaster_start_time()</c> (one component each; the second stays null). A carrier
/// collector reads the pair on a round trip it already makes, hands it to <see cref="ObserveInstance"/>,
/// <see cref="ObserveStatements"/> or <see cref="ObservePostmaster"/>, and the comparison runs against the LAST PERSISTED pair from the
/// host's <c>collector_state</c> (<see cref="CollectorContext.State"/>), not an in-memory one: a host that
/// restarts seeds its baselines from its own store (#3614), so the target that restarted while the host
/// was down must be caught on the host's first pass, and only a persisted prior can do that. On a change
/// the carrier forgets the affected baselines THROUGH ITS OWN DELTA HANDLE before it subtracts anything —
/// so the carrier's own family is honest on the very pass that sees the epoch — records the change as a
/// count on its run's <c>collection_log</c> note (the #3161 measurement seam; the marker the read layer
/// can later render, in a table both stores already have), persists the new pair and the pair it
/// replaced, and hands the host one sentence to log.</para>
///
/// <para><b>Unknown is not different.</b> <c>sqlserver_start_time</c> is null on a login without
/// VIEW SERVER STATE and absent on the Azure SQL DB path; <c>stats_reset</c> is absent before
/// pg_stat_statements 1.9. A null on either side of a component is no evidence of anything, so only a
/// value → DIFFERENT value transition on some component is an epoch, and a null observation never
/// overwrites a known one (<see cref="Merge"/> keeps the last known component), so a later known value
/// is still compared against the last KNOWN value rather than against the gap.</para>
///
/// <para><b>Where the SQL Server carriers sit, and why there are two.</b> Both hosts run a server's due
/// collectors one after another in <c>CollectorScheduleDefaults.All</c>'s declared order (Darling's
/// <c>RunDueCollectorsAsync</c> iterates the keys; Lite's <c>ScheduleManager.GetDefaultSchedules()</c> is
/// pinned equal to it), and <c>wait_stats</c> is first on both. So <c>wait_stats</c> carries the pair as a
/// second result set on the batch it already runs (#3653 A5's carrier-order residue, #3694's stated
/// follow-up): it observes at the end of its read, before its own <c>WritePayload</c> subtracts, so on the
/// pass that first sees the epoch EVERY SQL Server delta family — its own included — re-baselines against
/// nothing instead of subtracting once from the dead instance. Until then the carrier was
/// <c>cpu_utilization</c>, tenth in the order, and the five families before it fabricated one interval each
/// on every restart, failover and re-point. <c>cpu_utilization</c> KEEPS carrying the pair: an operator can
/// disable <c>wait_stats</c> on either host (a Darling <c>config_collector_schedules</c> override, Lite's
/// schedule editor), and a carrier that can be switched off must not be the only one. Neither carrier reads
/// the pair on Azure SQL DB — #3694's ruling, unchanged: the CPU batch there never touched the DMV, and the
/// wait-stats batch keeps the same engine gate so the two carriers observe under one rule.</para>
///
/// <para><b>Two carriers, one forget.</b> Each carrier compares against the pair persisted under ITS OWN
/// collector name (both hosts load <c>collector_state</c> by <c>(server_id, collector_name)</c>), so on the
/// epoch pass both see the change. Two <c>ClearServer</c> calls would be worse than one: the second, from
/// <c>cpu_utilization</c>, would forget the baselines the five families between the carriers had just
/// re-established on the new instance and cost each of them one more (0, 0) pass — the very pass the
/// first carrier bought back. So <see cref="ObserveInstance"/> remembers, per calculator and per server,
/// the identity it last forgot TO, and a second observer that finds the calculator already on the current
/// identity forgets nothing and queues no sentence; it still measures its own marker (the observation
/// happened — this run's persisted pair did move) and catches its persisted pair up, so the next pass is
/// quiet under both names. The memo is keyed on the calculator instance, not process-wide, because the
/// calculator IS the thing whose baselines belong to an identity: a fresh calculator (a test, a host that
/// re-created its runtime) knows nothing and forgets on first sight, which is the conservative direction.
/// It is not persisted: on a host restart both carriers' persisted priors describe the same old instance,
/// the first to run forgets the #3614-seeded baselines once, and the second finds the memo.</para>
///
/// <para><b>What this does not do.</b> A carrier that is disabled observes nothing; with <c>wait_stats</c>
/// off, the CPU carrier's order (after five delta families) is the residue #3694 described, on that
/// operator's servers only — as it is on a Lite install whose persisted schedule file was hand-reordered
/// (Lite keeps a loaded file's order; the census pins the shipped default). No SQL Server carrier reads a
/// pass where neither collector is due (both are per-minute by default). No epoch is rendered — the marker
/// is the carrier run's <c>collection_log</c> note and the pair in <c>collector_state</c>.</para>
/// </summary>
public static class ServerEpoch
{
    /// <summary>
    /// One observed identity: the instance's start time and its name, either unknown. For the statements
    /// epoch only <see cref="StartTime"/> is used (it carries <c>stats_reset</c>) and <see cref="Name"/> is
    /// null. The start time is compared as the target reported it — a local <c>datetime2</c> from SQL
    /// Server, a UTC <c>timestamptz</c> from PostgreSQL — never converted, because the same source always
    /// reports the same clock and a conversion is one more thing that could move between two reads.
    /// </summary>
    public readonly record struct Stamp(DateTime? StartTime, string? Name)
    {
        /// <summary>Neither component known — nothing to persist and nothing to compare.</summary>
        public bool IsEmpty => !StartTime.HasValue && Name is null;
    }

    /* The collector_state keys the carriers write, under their own collector_name. Whole keys, not
       *KeyPrefix constants: one row per server, never per database, so the prune census that governs
       per-database prefixes does not apply and no prune is owed. The store keeps them for the life of the
       server_id, as it does default_trace_events' trace-file path. */

    /// <summary>The persisted instance identity (<see cref="Serialize"/> form) for a SQL Server target.</summary>
    public const string IdentityStateKey = "identity_epoch";

    /// <summary>The instance identity the latest epoch change REPLACED — old beside new, so the store carries both sides of the most recent discontinuity without a rung.</summary>
    public const string IdentityPreviousStateKey = "identity_epoch_previous";

    /// <summary>The persisted <c>pg_stat_statements_info.stats_reset</c> for a PostgreSQL target's statements family.</summary>
    public const string StatementsStateKey = "statements_epoch";

    /// <summary>The <c>stats_reset</c> the latest statements epoch change replaced.</summary>
    public const string StatementsPreviousStateKey = "statements_epoch_previous";

    /// <summary>The persisted <c>pg_postmaster_start_time()</c> for an Aurora PostgreSQL target's wait family (<c>pg_wait_stats</c>).</summary>
    public const string PostmasterStateKey = "postmaster_epoch";

    /// <summary>The postmaster start time the latest wait-family epoch change replaced.</summary>
    public const string PostmasterPreviousStateKey = "postmaster_epoch_previous";

    /* The measurement labels — counts, per CollectorMeasurement's grammar. Rendered as `identity_epoch_changes=1`
       onto the carrier run's collection_log.error_message by both hosts (#3161): that row, with its
       collection_time, IS the discontinuity marker in the store. A count of one, because a run observes one
       identity; the label is what a reader keys on. */

    /// <summary>Count of instance identity changes this run observed (0 or 1).</summary>
    public const string IdentityChangesMeasurement = "identity_epoch_changes";

    /// <summary>Count of statements-epoch changes this run observed (0 or 1).</summary>
    public const string StatementsChangesMeasurement = "statements_epoch_changes";

    /// <summary>Count of postmaster-epoch (Aurora wait family) changes this run observed (0 or 1).</summary>
    public const string PostmasterChangesMeasurement = "postmaster_epoch_changes";

    private const char Separator = '|';

    /* The identity each calculator's baselines were last forgotten TO, per server (the "two carriers, one
       forget" paragraph above). Keyed on the calculator instance through a ConditionalWeakTable rather than
       held in a static map: the memo describes THAT calculator's cache, so it must live and die with it — a
       test's fresh calculator or a host that rebuilt its runtime starts with no memo and forgets on first
       sight, and nothing here pins a calculator in memory. One entry per server_id; a ConcurrentDictionary
       because Darling runs its server bodies concurrently against one calculator. A Stamp, not the serialized
       text, so the check is IsNewEpoch's own "unknown is not different" rule: a second observer whose
       reading agrees on every component both sides know has nothing to forget. */
    private static readonly ConditionalWeakTable<ICollectorDeltaCalculator, ConcurrentDictionary<int, Stamp>> s_forgottenTo = new();

    /// <summary>
    /// True when <paramref name="current"/> is a different instance's identity than <paramref name="prior"/>:
    /// some component is KNOWN on both sides and differs. Unknown on either side of a component is no
    /// evidence, so a null → value, value → null or null → null transition is never an epoch on its own;
    /// nor is a null prior (the first observation, or a store with no row yet). Names compare
    /// case-insensitively because <c>@@SERVERNAME</c> is a SQL Server identifier.
    /// </summary>
    public static bool IsNewEpoch(Stamp? prior, Stamp current)
    {
        if (!prior.HasValue)
        {
            return false;
        }

        var p = prior.Value;

        var startTimeMoved = p.StartTime.HasValue && current.StartTime.HasValue
            && p.StartTime.Value != current.StartTime.Value;
        var nameMoved = p.Name is not null && current.Name is not null
            && !string.Equals(p.Name, current.Name, StringComparison.OrdinalIgnoreCase);

        return startTimeMoved || nameMoved;
    }

    /// <summary>
    /// The identity to persist after an observation: each component is the current one when known,
    /// otherwise the prior one. This is what keeps a permission-poor or Azure pass — which observes
    /// nothing — from erasing a known start time, so that the next KNOWN value is compared against the
    /// last known one and not against the hole.
    /// </summary>
    public static Stamp Merge(Stamp? prior, Stamp current)
        => new(current.StartTime ?? prior?.StartTime, current.Name ?? prior?.Name);

    /// <summary>
    /// The stored form: <c>start-time|name</c>, the start time in round-trip (<c>O</c>) form so its ticks and
    /// its Kind survive the text, either component empty when unknown. Culture-invariant on both sides.
    /// </summary>
    public static string Serialize(Stamp stamp)
    {
        var start = stamp.StartTime.HasValue
            ? stamp.StartTime.Value.ToString("O", CultureInfo.InvariantCulture)
            : string.Empty;

        return start + Separator + (stamp.Name ?? string.Empty);
    }

    /// <summary>
    /// Reads a <see cref="Serialize"/>d identity back. False on a null, empty or malformed value — the
    /// caller treats that as "no prior", which is the conservative direction: an unreadable prior can
    /// never manufacture an epoch, it can only make one observation late.
    /// </summary>
    public static bool TryParse(string? text, out Stamp stamp)
    {
        stamp = default;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var separator = text.IndexOf(Separator, StringComparison.Ordinal);
        if (separator < 0)
        {
            return false;
        }

        var startText = text[..separator];
        var name = separator + 1 < text.Length ? text[(separator + 1)..] : null;

        DateTime? startTime = null;
        if (startText.Length > 0)
        {
            if (!DateTime.TryParseExact(startText, "O", CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind, out var parsed))
            {
                return false;
            }

            startTime = parsed;
        }

        stamp = new Stamp(startTime, name);
        return true;
    }

    /// <summary>
    /// The instance-identity observation for a SQL Server target — the whole server's baselines ride on it,
    /// because every cumulative DMV on the instance restarts with the instance and belongs to whichever
    /// instance the connection now reaches. Compares <paramref name="current"/> against the pair persisted
    /// under <see cref="IdentityStateKey"/> for the CALLING collector; on an epoch, forgets every baseline and
    /// pass window for <see cref="CollectorContext.ServerId"/> (<see cref="ICollectorDeltaCalculator.ClearServer"/>)
    /// with a one-line account for the host to log — unless the calculator was already forgotten to this same
    /// identity by the other carrier this pass, in which case nothing is forgotten and no sentence is queued
    /// (the "two carriers, one forget" paragraph) — measures <see cref="IdentityChangesMeasurement"/> onto
    /// the run's note, and persists the replaced pair under <see cref="IdentityPreviousStateKey"/>. Always
    /// persists the merged pair when it differs from what the store holds — and ONLY then, so an ordinary
    /// pass writes no state row at all. Returns true when the persisted pair moved (an epoch, whichever
    /// carrier did the forgetting).
    /// </summary>
    public static bool ObserveInstance(CollectorContext context, Stamp current)
    {
        ArgumentNullException.ThrowIfNull(context);

        var prior = Prior(context, IdentityStateKey);
        var changed = IsNewEpoch(prior, current);
        if (changed)
        {
            /* IsNewEpoch is false on a null prior, so this is the known pair the epoch replaces. */
            var replaced = prior.GetValueOrDefault();
            var forgottenTo = s_forgottenTo.GetOrCreateValue(context.Deltas);
            if (!forgottenTo.TryGetValue(context.ServerId, out var known) || IsNewEpoch(known, current))
            {
                context.Deltas.ClearServer(
                    context.ServerId,
                    "identity epoch changed — start time " + Describe(replaced.StartTime) + " → " + Describe(current.StartTime)
                    + ", server name " + Describe(replaced.Name) + " → " + Describe(current.Name)
                    + ": every delta baseline and pass window for this server was forgotten, so this pass re-baselines "
                    + "and the next reports the first interval measured on the instance the connection now reaches");
                /* What was observed, not the merge with the prior: the prior's components describe the OLD
                   instance, and a component the new one has not reported yet stays unknown here so a later
                   observer that does know it compares against nothing rather than against a guess. */
                forgottenTo[context.ServerId] = current;
            }
            else
            {
                /* Already on this identity; a component that only this observer knows joins the memo. */
                forgottenTo[context.ServerId] = Merge(known, current);
            }

            context.Measure(IdentityChangesMeasurement, 1);
            context.PendingState[IdentityPreviousStateKey] = Serialize(replaced);
        }

        Persist(context, IdentityStateKey, prior, current);
        return changed;
    }

    /// <summary>
    /// The statements-epoch observation for a PostgreSQL target: <paramref name="statsReset"/> is
    /// <c>pg_stat_statements_info.stats_reset</c> (null before pg_stat_statements 1.9 / PostgreSQL 14, when the
    /// view does not exist), which moves when <c>pg_stat_statements_reset()</c> is called, when the extension's
    /// saved statistics did not survive a crash, and when the endpoint now reaches an instance with its own
    /// statistics history. It says nothing about any other counter on the server, so on an epoch only
    /// <paramref name="groups"/> — the family's own delta groups — are forgotten
    /// (<see cref="ICollectorDeltaCalculator.ClearGroups"/>). Same persistence and marker discipline as
    /// <see cref="ObserveInstance"/>, under <see cref="StatementsStateKey"/>. Returns true on an epoch.
    /// </summary>
    public static bool ObserveStatements(CollectorContext context, DateTime? statsReset, params string[] groups)
    {
        ArgumentNullException.ThrowIfNull(context);

        var current = new Stamp(statsReset, null);
        var prior = Prior(context, StatementsStateKey);
        var changed = IsNewEpoch(prior, current);
        if (changed)
        {
            var replaced = prior.GetValueOrDefault();
            context.Deltas.ClearGroups(
                context.ServerId,
                "pg_stat_statements epoch changed — stats_reset " + Describe(replaced.StartTime) + " → " + Describe(current.StartTime)
                + ": the pg_statement_stats baselines and pass window were forgotten, so this pass re-baselines the "
                + "family and the next reports the first interval measured against the reset counters",
                groups);
            context.Measure(StatementsChangesMeasurement, 1);
            context.PendingState[StatementsPreviousStateKey] = Serialize(replaced);
        }

        Persist(context, StatementsStateKey, prior, current);
        return changed;
    }

    /// <summary>
    /// The postmaster-epoch observation for Aurora's wait family: <paramref name="postmasterStartTime"/> is
    /// <c>pg_postmaster_start_time()</c>, which moves on every PostgreSQL restart — clean or not — and when
    /// the endpoint now reaches a different instance. It is the honest epoch for counters that live in
    /// instance memory and nowhere else: <c>aurora_stat_system_waits()</c> starts from zero with the
    /// postmaster, and <c>pg_stat_statements_info.stats_reset</c> (the statements carrier's epoch) does NOT
    /// move on a clean restart because that extension's counters survive one, so the statements epoch can
    /// say nothing about the wait counters. Conversely a start-time epoch would be WRONG for the statements
    /// family (it would discard a knowable interval on every clean restart), which is why the two families
    /// carry different epochs and forget only their own groups. Verified on the #3694 rig (PG 18.4): a clean
    /// restart moved the start time and left <c>stats_reset</c> and the statement counters continuous. Same
    /// persistence and marker discipline as <see cref="ObserveStatements"/>, under
/// <see cref="PostmasterStateKey"/>; a NULL reading (a source that did not carry the column — the function
/// itself is readable by any login and never null) is unknown, never a change. Returns true on an epoch.
    /// </summary>
    public static bool ObservePostmaster(CollectorContext context, DateTime? postmasterStartTime, params string[] groups)
    {
        ArgumentNullException.ThrowIfNull(context);

        var current = new Stamp(postmasterStartTime, null);
        var prior = Prior(context, PostmasterStateKey);
        var changed = IsNewEpoch(prior, current);
        if (changed)
        {
            var replaced = prior.GetValueOrDefault();
            context.Deltas.ClearGroups(
                context.ServerId,
                "postmaster epoch changed — pg_postmaster_start_time " + Describe(replaced.StartTime) + " → " + Describe(current.StartTime)
                + ": the pg_wait_stats baselines and pass window were forgotten, so this pass re-baselines the "
                + "family and the next reports the first interval measured on the restarted instance",
                groups);
            context.Measure(PostmasterChangesMeasurement, 1);
            context.PendingState[PostmasterPreviousStateKey] = Serialize(replaced);
        }

        Persist(context, PostmasterStateKey, prior, current);
        return changed;
    }

    private static Stamp? Prior(CollectorContext context, string stateKey)
        => context.State.TryGetValue(stateKey, out var text) && TryParse(text, out var parsed) && !parsed.IsEmpty
            ? parsed
            : null;

    /* Change-only: the carrier runs every minute on every target, and a state upsert per run per server
       would be a store write the product did not make before — on Lite's single-writer DuckDB in
       particular. Writing only when the merged text differs from the stored text makes the steady state
       zero writes; a first sighting, a component becoming known, and an epoch are the three passes that
       write. */
    private static void Persist(CollectorContext context, string stateKey, Stamp? prior, Stamp current)
    {
        var merged = Merge(prior, current);
        if (merged.IsEmpty)
        {
            return;
        }

        var text = Serialize(merged);
        context.State.TryGetValue(stateKey, out var stored);
        if (!string.Equals(text, stored, StringComparison.Ordinal))
        {
            context.PendingState[stateKey] = text;
        }
    }

    private static string Describe(DateTime? value)
        => value.HasValue ? value.Value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture) : "unknown";

    private static string Describe(string? value)
        => string.IsNullOrEmpty(value) ? "unknown" : value;
}
