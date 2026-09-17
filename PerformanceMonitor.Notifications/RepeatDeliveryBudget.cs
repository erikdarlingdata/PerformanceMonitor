/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// #3430: a per-METRIC ceiling on REPEAT deliveries, so one fault spread across N servers costs a bounded
/// number of channel posts instead of N of them. Composed alongside <see cref="IncidentCooldown"/> on each
/// channel path (<see cref="EmailSendCore"/>, <see cref="WebhookAlertService"/>), which owns the
/// per-fingerprint decision this one reads.
///
/// <para><b>The defect.</b> <see cref="IncidentCooldown"/> bounds posts per #1140 fingerprint, and
/// <see cref="AlertFingerprint"/> hashes the server name into every fingerprint — so the SAME fault on 15
/// servers is 15 fingerprints, each delivering once per window, and the aggregate is the fingerprint count
/// multiplied by the per-fingerprint allowance. Measured on a 43-server store: 43 fires produced 16 posts in
/// 15.9 minutes from 15 servers carrying one metric, 60.2 posts/hour, with the throttle working exactly as
/// specified (26 cooldown-throttled, 0 failed). The multiplier is fleet size, so the post rate for a given
/// fault GROWS with the monitored fleet.</para>
///
/// <para><b>What this bounds, and what it deliberately does not.</b> A delivery is FOLDED — nothing posts,
/// and the (server, fingerprint) is recorded on the metric's roster — only when all of:
/// <list type="number">
/// <item>the caller asked for aggregation, which is Summary mode (see <see cref="Evaluate"/>'s
/// <c>aggregateRepeats</c>);</item>
/// <item>every fingerprint the delivery would carry has been delivered on this channel before —
/// <see cref="IncidentCooldown.Decision.AnyFirstNotice"/> is false; and</item>
/// <item>a repeat for the same metric already posted on this channel inside the window.</item>
/// </list>
/// So the FIRST notice of any fingerprint always posts, in full, immediately, unthrottled by anything
/// happening on another server. That is #1154's rule and it is the line this type must not cross: #1154
/// found the cooldown keyed on (server, metric) and re-keyed it onto the fingerprint precisely because
/// distinct concurrent incidents were being silently dropped, and a per-metric key that could swallow a
/// never-announced incident would reintroduce it. The rate bounded here is the rate of RE-telling; the rate
/// of first tellings is the number of distinct incidents, which is irreducible.</para>
///
/// <para><b>Why the roster enumerates rather than counts.</b> A folded repeat has already been delivered in
/// full, so nothing is lost by not repeating it — but the operator would lose the current SCOPE of the
/// fault, which today's N cards do convey. The carrier post therefore names every folded
/// (server, fingerprint) in its roster item (<see cref="RosterHeading"/>), so N incidents arrive in one
/// message without becoming fewer incidents. It extends #3313's "Other Incidents / Still Open" footer from
/// co-resident staleness WITHIN one alert to the same condition ACROSS alerts, and it names each entry where
/// that footer only counted, because there the omitted detail was already on the same card and here it is
/// not.</para>
///
/// <para><b>No new setting and no persisted state, both deliberate.</b> The window is the existing
/// <c>delivery.cooldown_minutes</c> the caller already passes to <see cref="IncidentCooldown"/>: #3430's
/// argument is that an operator reading <c>delivery.cooldown_minutes: 15</c> concludes "at most four of
/// these an hour", so the fix is to make that reading true rather than to add a second number beside it. The
/// state is in-memory on the host-lifetime delivery singleton, exactly like <see cref="IncidentCooldown"/>'s
/// own map — a restart costs one extra post and one missing roster line, which is the failure class that
/// design already accepts, and it buys no store column and therefore no migration rung.</para>
///
/// <para><b>Fails toward posting.</b> Every uncertainty resolves to "post": a caller that states no delivery
/// mode is not aggregated, a channel with no history store cannot tell a first notice from a repeat so every
/// key reads as a first notice and nothing folds, and a carrier whose send delivered nothing releases its
/// reservation. The cost of failing that way is a post; the cost of failing the other way is an unannounced
/// incident.</para>
/// </summary>
public sealed class RepeatDeliveryBudget
{
    /// <summary>
    /// Heading of the roster item the carrier post carries. A detail item rather than prose so all four
    /// webhook channels, both email bodies and the in-app dialog render it from
    /// <see cref="AlertContext.Details"/> without any renderer knowing about it.
    /// </summary>
    public const string RosterHeading = "Other Servers Affected";

    /// <summary>
    /// Fact naming how many (server, fingerprint) pairs the roster covers. A bare count, not a sentence:
    /// the point of giving it its own fact is that a consumer reads the number without parsing
    /// <see cref="RosterListFactName"/>, and a fact whose value is prose asks it to parse anyway. The
    /// heading carries the words. A fact name is a consumer API (see <see cref="AlertIncidentRenderer"/>),
    /// so it is declared here rather than spelled inline.
    /// </summary>
    public const string RosterCountFactName = "Affected";

    /// <summary>Fact carrying the enumeration itself, one entry per folded (server, fingerprint).</summary>
    public const string RosterListFactName = "Also Affected";

    /// <summary>
    /// Fact stating how many entries the enumeration omitted. Emitted ONLY when
    /// <see cref="MaxEnumeratedEntries"/> was reached, so its absence means the list is complete rather than
    /// that nobody checked.
    /// </summary>
    public const string RosterOmittedFactName = "Not Listed";

    /// <summary>
    /// How many entries the roster spells out. Above this the list is truncated and
    /// <see cref="RosterOmittedFactName"/> states the remainder — the ONE place this mechanism compresses
    /// rather than enumerates. Set well above any fleet size this product is deployed at (the largest store
    /// it runs on monitors 43 servers), so reaching it means something other than a fleet-wide fault.
    /// </summary>
    public const int MaxEnumeratedEntries = 120;

    /// <summary>
    /// How many folded entries one metric's roster retains. A bound on memory, not a policy: an entry is a
    /// repeat that was already delivered in full, so dropping the quietest costs a roster line and never an
    /// announcement. Entries also expire on their own, two windows after they last folded.
    /// </summary>
    public const int MaxTrackedEntriesPerMetric = 600;

    /// <summary>
    /// How many characters of a fingerprint a roster line shows. Dedup keys from
    /// <see cref="AlertFingerprint"/> are 64-char SHA-256 hex, and 43 of those would be 2.7 KB of card for no
    /// added meaning; a prefix is enough to correlate a line with the incident's own card, and the full key
    /// is on that card and in the <c>config_alert_log</c> row either way. A key SHORTER than this (the #2716
    /// callers pass a human-readable name) is shown whole, never padded or elided.
    /// </summary>
    public const int FingerprintDisplayLength = 12;

    private readonly ConcurrentDictionary<string, MetricState> _metrics = new(StringComparer.Ordinal);

    /// <summary>
    /// Decides whether this delivery posts, and what roster the post carries. Does NOT clear the roster — the
    /// caller calls <see cref="Commit"/> after a successful send (or <see cref="Release"/> after one that
    /// delivered nothing), so a send that fails leaves the folded entries for the next carrier to name.
    /// </summary>
    /// <param name="metricName">
    /// The immutable metric name, which is the aggregation axis. Not the display name a custom rule carries:
    /// the same rule must aggregate with itself across servers, and the display name is a title concern.
    /// </param>
    /// <param name="serverLabel">The server as an operator reads it, for the roster line.</param>
    /// <param name="cooldown">
    /// The per-fingerprint decision from <see cref="IncidentCooldown.EvaluateAsync"/>, already known to be
    /// <see cref="IncidentCooldown.Decision.ShouldSend"/>. Read for three things: its evaluation instant (so
    /// the two decisions cannot disagree about "now"), its
    /// <see cref="IncidentCooldown.Decision.AnyFirstNotice"/>, and its
    /// <see cref="IncidentCooldown.Decision.DeliverableDedupKeys"/> — the fingerprints this delivery would
    /// have carried, which are exactly the ones a fold owes the roster.
    /// </param>
    /// <param name="window">The same <c>delivery.cooldown_minutes</c> window the cooldown was evaluated on.</param>
    /// <param name="aggregateRepeats">
    /// Whether this delivery's mode permits folding. False for <see cref="AlertNotificationMode.PerEvent"/>
    /// and for any caller that did not resolve a mode. Per-event exists so downstream automation gets one
    /// message per distinct incident and can count recurrences on the #1140 fingerprint (#1140/#1141); a
    /// folded recurrence never reaches that consumer, so the mode's own contract is the boundary. A false
    /// here also declines the roster: a message that is not part of this aggregation must not carry its
    /// bookkeeping.
    /// </param>
    /// <param name="incidents">
    /// The alert's incidents, read ONLY for each fingerprint's <see cref="AlertIncident.OccurrenceCount"/> so
    /// a roster line can state how much is happening on that server. Nothing is retained: an entry holds two
    /// strings and three numbers, never an <see cref="AlertIncident"/>, whose attachment XML would then live
    /// as long as the roster.
    /// </param>
    public Decision Evaluate(
        string metricName,
        string serverLabel,
        IncidentCooldown.Decision cooldown,
        TimeSpan window,
        bool aggregateRepeats,
        IReadOnlyList<AlertIncident>? incidents = null)
    {
        if (cooldown is null)
        {
            throw new ArgumentNullException(nameof(cooldown));
        }

        if (!aggregateRepeats)
        {
            return Decision.Unaggregated;
        }

        var metric = metricName ?? string.Empty;
        var state = _metrics.GetOrAdd(metric, _ => new MetricState());

        /* No I/O inside the lock, and the lock is per metric rather than global: the engine evaluates
           DIFFERENT servers concurrently, so "am I this window's carrier" is a read-modify-write two servers
           reach at the same instant. Without it both would read an unstamped carrier time, both would post,
           and both would render the same roster. */
        lock (state)
        {
            state.Prune(cooldown.EvaluatedAtUtc, window);

            var repeat = !cooldown.AnyFirstNotice;

            if (repeat &&
                state.LastCarrierUtc is DateTime last &&
                cooldown.EvaluatedAtUtc - last < window)
            {
                return Decision.Folded(
                    metric,
                    state.Fold(serverLabel, cooldown.DeliverableDedupKeys, incidents, cooldown.EvaluatedAtUtc));
            }

            /* Reserve BEFORE the send so a concurrent sibling folds rather than posting a second card, and
               stamp it with the evaluation instant so a Release that never runs expires on its own at the end
               of the window instead of muting the metric's repeats forever. Neither caller releases from a
               finally — each decides on its send outcome and both sit inside an outer catch that swallows —
               so an exception escaping the send path skips Release, and the expiry is what bounds that.
               Two barriers, because the one that matters is the one that fails. */
            if (repeat)
            {
                state.LastCarrierUtc = cooldown.EvaluatedAtUtc;
            }

            var carried = state.SnapshotKeys();
            return Decision.Carrier(
                metric, cooldown.EvaluatedAtUtc, repeat, carried, state.BuildRoster(carried));
        }
    }

    /// <summary>
    /// Clears the roster entries a successful carrier post named. Entries folded WHILE that send was in
    /// flight are untouched, so they are named by the next carrier rather than lost to this one's commit.
    /// </summary>
    public void Commit(Decision decision)
    {
        if (decision is null)
        {
            throw new ArgumentNullException(nameof(decision));
        }

        if (decision.CarriedEntryKeys.Count == 0 || !_metrics.TryGetValue(decision.MetricName, out var state))
        {
            return;
        }

        lock (state)
        {
            state.Remove(decision.CarriedEntryKeys);
        }
    }

    /// <summary>
    /// Gives back a carrier reservation whose send delivered nothing, so the next repeat can post instead of
    /// waiting out a window that carried no card. Only clears the reservation this decision made — a newer
    /// one has already superseded it and is not this caller's to drop. The roster is left alone: a send that
    /// delivered nothing named nothing, so nothing has been reported.
    /// </summary>
    public void Release(Decision decision)
    {
        if (decision is null)
        {
            throw new ArgumentNullException(nameof(decision));
        }

        if (!decision.ReservedWindow || !_metrics.TryGetValue(decision.MetricName, out var state))
        {
            return;
        }

        lock (state)
        {
            if (state.LastCarrierUtc == decision.EvaluatedAtUtc)
            {
                state.LastCarrierUtc = null;
            }
        }
    }

    /// <summary>
    /// What <see cref="Evaluate"/> decided, as one value so a caller cannot take the send decision and drop
    /// the roster that belongs with it.
    /// </summary>
    public sealed class Decision
    {
        /// <summary>The answer for a caller that did not ask for aggregation: post, carry nothing, and hold
        /// no reservation to commit or release.</summary>
        internal static readonly Decision Unaggregated =
            new(true, null, 0, string.Empty, default, false, Array.Empty<string>());

        private Decision(
            bool shouldSend,
            AlertDetailItem? roster,
            int rosterEntryCount,
            string metricName,
            DateTime evaluatedAtUtc,
            bool reservedWindow,
            IReadOnlyList<string> carriedEntryKeys)
        {
            ShouldSend = shouldSend;
            Roster = roster;
            RosterEntryCount = rosterEntryCount;
            MetricName = metricName;
            EvaluatedAtUtc = evaluatedAtUtc;
            ReservedWindow = reservedWindow;
            CarriedEntryKeys = carriedEntryKeys;
        }

        /// <summary>Whether this delivery posts. False means folded: recorded on the metric's roster and
        /// named by the next carrier, never dropped.</summary>
        public bool ShouldSend { get; }

        /// <summary>
        /// The roster item to render alongside the alert, or null when nothing is owed. Appended to the
        /// delivery-scoped COPY of the context by <see cref="IncidentDeliveryFilter.ForDelivery"/>, never to
        /// the caller's own instance — the history row and every surface reading it stay unchanged, the same
        /// split #3313 drew.
        /// </summary>
        public AlertDetailItem? Roster { get; }

        /// <summary>How many folded (server, fingerprint) pairs this decision accounts for — what
        /// <see cref="Roster"/> covers on a carrier, and the metric's live roster size on a fold — so a
        /// caller can log the aggregate without re-reading the item's fields.</summary>
        public int RosterEntryCount { get; }

        /// <summary>The metric this decision is about — the aggregation axis, carried so
        /// <see cref="Commit"/> and <see cref="Release"/> need no second argument that could disagree.</summary>
        internal string MetricName { get; }

        /// <summary>The instant <see cref="Evaluate"/> ran, which is the cooldown's own evaluation instant.
        /// Identifies this decision's reservation so <see cref="Release"/> cannot drop a newer one.</summary>
        internal DateTime EvaluatedAtUtc { get; }

        /// <summary>Whether this decision consumed the metric's window. False on a first notice, which posts
        /// outside the budget entirely.</summary>
        internal bool ReservedWindow { get; }

        /// <summary>The roster entry keys this post names, cleared by <see cref="Commit"/>.</summary>
        internal IReadOnlyList<string> CarriedEntryKeys { get; }

        internal static Decision Folded(string metricName, int rosterEntryCount) =>
            new(false, null, rosterEntryCount, metricName, default, false, Array.Empty<string>());

        internal static Decision Carrier(
            string metricName,
            DateTime evaluatedAtUtc,
            bool reservedWindow,
            IReadOnlyList<string> carriedEntryKeys,
            AlertDetailItem? roster) =>
            new(true, roster, carriedEntryKeys.Count, metricName, evaluatedAtUtc, reservedWindow, carriedEntryKeys);
    }

    /* One metric's aggregate state: when a repeat last posted, and every (server, fingerprint) folded since
       the last carrier named them. Guarded by locking the instance itself (see Evaluate). */
    private sealed class MetricState
    {
        private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);

        public DateTime? LastCarrierUtc { get; set; }

        public int Count => _entries.Count;

        /// <summary>Records a folded delivery's fingerprints and returns the metric's live roster size.</summary>
        public int Fold(
            string serverLabel,
            IReadOnlyList<string>? dedupKeys,
            IReadOnlyList<AlertIncident>? incidents,
            DateTime nowUtc)
        {
            /* A metric-level alert (CPU, memory, tempdb, low disk, a job with no parseable incident) has no
               fingerprint at all, so its roster line is the server alone. That is the same fan-out — one CPU
               threshold breached on 15 servers is 15 metric-level keys — so it folds on the same axis rather
               than being exempted for lacking a fingerprint it never had. */
            if (dedupKeys is not { Count: > 0 })
            {
                Record(serverLabel, string.Empty, occurrences: 0, nowUtc);
                return _entries.Count;
            }

            foreach (var dedupKey in dedupKeys)
            {
                Record(serverLabel, dedupKey, OccurrencesFor(incidents, dedupKey), nowUtc);
            }

            return _entries.Count;
        }

        public IReadOnlyList<string> SnapshotKeys()
        {
            if (_entries.Count == 0)
            {
                return Array.Empty<string>();
            }

            var keys = new List<string>(_entries.Count);
            foreach (var key in _entries.Keys)
            {
                keys.Add(key);
            }

            return keys;
        }

        public AlertDetailItem? BuildRoster(IReadOnlyList<string> keys)
        {
            if (keys.Count == 0)
            {
                return null;
            }

            var list = new StringBuilder();
            var enumerated = 0;
            foreach (var key in keys)
            {
                if (enumerated == MaxEnumeratedEntries)
                {
                    break;
                }

                if (!_entries.TryGetValue(key, out var entry))
                {
                    continue;
                }

                if (enumerated > 0)
                {
                    list.Append("; ");
                }

                list.Append(entry.Describe());
                enumerated++;
            }

            var item = new AlertDetailItem { Heading = RosterHeading };
            item.Fields.Add((RosterCountFactName, keys.Count.ToString(CultureInfo.InvariantCulture)));
            item.Fields.Add((RosterListFactName, list.ToString()));
            if (keys.Count > enumerated)
            {
                item.Fields.Add((RosterOmittedFactName,
                    (keys.Count - enumerated).ToString(CultureInfo.InvariantCulture)));
            }

            return item;
        }

        public void Remove(IReadOnlyList<string> keys)
        {
            foreach (var key in keys)
            {
                _entries.Remove(key);
            }
        }

        /// <summary>
        /// Drops entries untouched for longer than 2x the window, the same bound and the same reasoning as
        /// <c>IncidentCooldown.Evict</c>: past 1x the fingerprint is already re-fire-eligible, so it re-folds
        /// itself if the condition is still live, and doubling only adds clock-skew margin. A stale entry is a
        /// repeat that was delivered in full and has since gone quiet, so expiring it costs a roster line and
        /// never an announcement.
        /// </summary>
        public void Prune(DateTime nowUtc, TimeSpan window)
        {
            var pruneBefore = nowUtc - TimeSpan.FromTicks(window.Ticks * 2);
            List<string>? stale = null;
            foreach (var pair in _entries)
            {
                if (pair.Value.LastUtc < pruneBefore)
                {
                    (stale ??= new List<string>()).Add(pair.Key);
                }
            }

            if (stale is not null)
            {
                Remove(stale);
            }
        }

        private void Record(string serverLabel, string dedupKey, int occurrences, DateTime nowUtc)
        {
            var key = serverLabel + " " + dedupKey;
            if (_entries.TryGetValue(key, out var existing))
            {
                existing.Folds++;
                existing.LastUtc = nowUtc;
                if (occurrences > existing.Occurrences)
                {
                    existing.Occurrences = occurrences;
                }

                return;
            }

            /* At the cap, drop the entry quiet longest rather than refusing the new one: the roster is most
               useful describing what is happening now, and the quietest entry is the one most likely to have
               stopped. */
            if (_entries.Count >= MaxTrackedEntriesPerMetric)
            {
                EvictQuietest();
            }

            _entries[key] = new Entry
            {
                ServerLabel = serverLabel,
                DedupKey = dedupKey,
                Occurrences = occurrences,
                Folds = 1,
                LastUtc = nowUtc
            };
        }

        private void EvictQuietest()
        {
            string? quietestKey = null;
            var quietest = DateTime.MaxValue;
            foreach (var pair in _entries)
            {
                if (pair.Value.LastUtc < quietest)
                {
                    quietest = pair.Value.LastUtc;
                    quietestKey = pair.Key;
                }
            }

            if (quietestKey is not null)
            {
                _entries.Remove(quietestKey);
            }
        }

        private static int OccurrencesFor(IReadOnlyList<AlertIncident>? incidents, string dedupKey)
        {
            if (incidents is null)
            {
                return 0;
            }

            foreach (var incident in incidents)
            {
                if (string.Equals(incident.DedupKey, dedupKey, StringComparison.Ordinal))
                {
                    return incident.OccurrenceCount;
                }
            }

            return 0;
        }
    }

    /* One folded (server, fingerprint): what it takes to write a roster line, and nothing that would keep an
       alert's attachment XML alive for the life of the roster. */
    private sealed class Entry
    {
        public string ServerLabel { get; set; } = string.Empty;

        public string DedupKey { get; set; } = string.Empty;

        public int Occurrences { get; set; }

        public int Folds { get; set; }

        public DateTime LastUtc { get; set; }

        /// <summary>
        /// One roster line: <c>SRV-A (1a2b3c4d5e6f, 4 occurrence(s))</c>. The server first because that is
        /// what an operator acts on; the fingerprint truncated because it is a correlator here and not the
        /// record (see <see cref="FingerprintDisplayLength"/>); the occurrence gauge only when the incident
        /// carried one, so a zero never reads as "measured none".
        /// </summary>
        public string Describe()
        {
            var parts = new List<string>(2);

            if (DedupKey.Length > 0)
            {
                parts.Add(DedupKey.Length > FingerprintDisplayLength
                    ? DedupKey.Substring(0, FingerprintDisplayLength)
                    : DedupKey);
            }

            if (Occurrences > 0)
            {
                parts.Add(string.Format(CultureInfo.InvariantCulture, "{0} occurrence(s)", Occurrences));
            }

            return parts.Count > 0
                ? ServerLabel + " (" + string.Join(", ", parts) + ")"
                : ServerLabel;
        }
    }
}
