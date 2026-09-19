/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

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
/// family <c>pg_stat_statements_info.stats_reset</c> (one component; the second stays null). A carrier
/// collector reads the pair on a round trip it already makes, hands it to <see cref="ObserveInstance"/>
/// or <see cref="ObserveStatements"/>, and the comparison runs against the LAST PERSISTED pair from the
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
/// <para><b>What this does not do.</b> It observes where the carrier sits in the schedule: on SQL
/// Server today the carrier is <c>cpu_utilization</c>, which the schedule runs AFTER wait_stats,
/// latch_stats, spinlock_stats, query_stats and procedure_stats — those five subtract once against the
/// old baseline on the pass that sees the epoch, exactly as every family did before this change, and are
/// forgotten in the same pass so the next one re-baselines; the three families after it are honest on the
/// pass itself. Moving the carrier's two columns onto the first collector in the order makes all eight
/// honest and is a follow-up on that collector, not on this type. A single instance-level cadence read
/// (<c>SELECT sqlserver_start_time, @@SERVERNAME</c>) at the top of each pass would do the same at the
/// cost of one round trip per pass and is the alternative the maintainer can choose instead.</para>
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

    /* The measurement labels — counts, per CollectorMeasurement's grammar. Rendered as `identity_epoch_changes=1`
       onto the carrier run's collection_log.error_message by both hosts (#3161): that row, with its
       collection_time, IS the discontinuity marker in the store. A count of one, because a run observes one
       identity; the label is what a reader keys on. */

    /// <summary>Count of instance identity changes this run observed (0 or 1).</summary>
    public const string IdentityChangesMeasurement = "identity_epoch_changes";

    /// <summary>Count of statements-epoch changes this run observed (0 or 1).</summary>
    public const string StatementsChangesMeasurement = "statements_epoch_changes";

    private const char Separator = '|';

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
    /// under <see cref="IdentityStateKey"/>; on an epoch, forgets every baseline and pass window for
    /// <see cref="CollectorContext.ServerId"/> (<see cref="ICollectorDeltaCalculator.ClearServer"/>) with a
    /// one-line account for the host to log, measures <see cref="IdentityChangesMeasurement"/> onto the run's
    /// note, and persists the replaced pair under <see cref="IdentityPreviousStateKey"/>. Always persists the
    /// merged pair when it differs from what the store holds — and ONLY then, so an ordinary pass writes no
    /// state row at all. Returns true on an epoch.
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
            context.Deltas.ClearServer(
                context.ServerId,
                "identity epoch changed — start time " + Describe(replaced.StartTime) + " → " + Describe(current.StartTime)
                + ", server name " + Describe(replaced.Name) + " → " + Describe(current.Name)
                + ": every delta baseline and pass window for this server was forgotten, so this pass re-baselines "
                + "and the next reports the first interval measured on the instance the connection now reaches");
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
