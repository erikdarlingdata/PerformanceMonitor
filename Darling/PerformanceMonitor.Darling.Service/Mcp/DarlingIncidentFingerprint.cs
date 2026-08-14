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
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Notifications;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Recomputes the #1140 alert fingerprint (the operator-visible <b>Dedup Key</b>) for STORED incident rows, so
/// the incident readers can jump straight from "this alert" to "this incident" (#2159).
///
/// <para><b>What this is for.</b> The fingerprint already travels end to end — it is a fact on the alert and the
/// reporter sets it on their Azure DevOps tickets — but no reader accepted it, so triaging from an alert meant
/// pulling a server+time window and eyeballing rows for the one whose objects matched. That is slow and it is
/// easy to analyze the WRONG deadlock.</para>
///
/// <para><b>Why it recomputes instead of reading a column.</b> Nothing persists the key on the incident row, and
/// adding it would be a migration that back-fills nothing — historical rows would stay unsearchable, which is
/// exactly the history an operator triages. The key is a pure function of stored data, so deriving it on read
/// covers all retained history the moment this ships.</para>
///
/// <para><b>It calls the SAME groupers the alert path calls, and that is the whole design.</b> The fingerprint is
/// a SHA-256 over normalized identity members; any divergence in how those members are derived produces a
/// different hash and the filter silently matches NOTHING — the worst failure mode available here, because an
/// empty result is indistinguishable from "that incident is outside the window". So this does not reimplement
/// the derivation: it builds the same <see cref="DeadlockIncidentGrouper.DeadlockEvent"/> /
/// <see cref="BlockingIncidentGrouper.BlockedEvent"/> the alert builders build and hands them to the same
/// grouper. Re-deriving keys from stored rows is not novel either — the analysis drill-down
/// (<c>AnalysisNotificationService.BuildIncidents</c>) already does it from its own stored rows.</para>
///
/// <para><b>THE SCOPE TRAP, and the reason this class takes a "fingerprint name" rather than a server name.</b>
/// <see cref="AlertFingerprint"/> hashes the server name INTO the key, and the alert path passes
/// <c>runtime.Config.DisplayName</c> — that is <c>Name</c> if set, else <c>Host</c>. The MCP resolver returns the
/// STORAGE name (<c>servers.server_name</c>, i.e. <c>host[:database][:RO]</c>), which is a different string
/// whenever a server carries a custom display name, or whenever the registration names a database or read-only
/// intent. Fingerprinting with the storage name would therefore work on plain hosts and silently return nothing
/// on exactly the servers most likely to be carefully named. Callers must pass
/// <see cref="DarlingServerResolver.FingerprintNameOf"/>'s value, which reproduces the alert path's choice.</para>
/// </summary>
internal static class DarlingIncidentFingerprint
{
    /// <summary>
    /// Normalizes a user-pasted key for comparison. The key is lowercase SHA-256 hex, but it arrives by copy
    /// and paste out of a ticket or an alert card, so it can carry surrounding whitespace or have been
    /// upper-cased by whatever rendered it.
    /// </summary>
    public static string NormalizeKey(string? dedupKey) =>
        (dedupKey ?? string.Empty).Trim().ToLowerInvariant();

    /// <summary>
    /// True when <paramref name="dedupKey"/> is absent, meaning "no fingerprint filter requested". Keeps the
    /// filter's absence a single decision rather than a repeated null-or-empty test per tool.
    /// </summary>
    public static bool NoFilter(string? dedupKey) => NormalizeKey(dedupKey).Length == 0;

    /// <summary>
    /// The dedup key for every deadlock row, in input order, using the same extraction and grouper the alert
    /// path uses. A row whose graph yields no usable object has no incident and so gets <c>null</c> — it can
    /// never match a filter, which is correct rather than a gap: an incident with no identity members is one
    /// the alerting layer never emitted a key for either.
    /// </summary>
    public static List<string?> DeadlockKeys(string fingerprintName, IEnumerable<string?> graphXmls)
    {
        var keys = new List<string?>();
        foreach (var xml in graphXmls ?? Enumerable.Empty<string?>())
        {
            var objects = string.IsNullOrEmpty(xml)
                ? Array.Empty<string>()
                : (IReadOnlyList<string>)DeadlockObjectExtractor.FromGraphXml(xml);

            /* Grouped ONE ROW AT A TIME on purpose. A deadlock's key hashes only its own object set
               (AlertFingerprint.ForObjects over e.Objects), so per-row grouping yields the identical key while
               keeping this a straight positional map from row to key — which is what the callers need in order
               to filter their own row list without re-deriving the association. */
            var group = DeadlockIncidentGrouper.Group(
                fingerprintName,
                new[] { new DeadlockIncidentGrouper.DeadlockEvent(objects) });

            keys.Add(group.Count > 0 ? group[0].Incident.DedupKey : null);
        }

        return keys;
    }

    /// <summary>
    /// The dedup key for every blocked-process row, in input order.
    ///
    /// <para>Unlike the deadlock case this CANNOT be done row at a time, and the difference is load-bearing. A
    /// blocking key comes from the identity bucket's REPRESENTATIVE — the first event with that identity — and
    /// falls back from the contentious object to a normalized query-pair key when no object resolved. So the
    /// whole set is grouped once, exactly as the alert path groups it, and each row is then matched back to its
    /// group by that same identity. Grouping per row would silently make every row its own representative.</para>
    ///
    /// <para>The grouper also normalizes the contentious-object label internally (#1876), which is another
    /// reason to route through it rather than hash the stored column: the stored value can be a raw lock
    /// resource that the label normalizer resolves, and the alert's key is over the NORMALIZED form.</para>
    /// </summary>
    public static List<string?> BlockingKeys(
        string fingerprintName,
        IReadOnlyList<BlockingIncidentGrouper.BlockedEvent> events)
    {
        if (events is not { Count: > 0 })
        {
            return new List<string?>();
        }

        var groups = BlockingIncidentGrouper.Group(fingerprintName, events);

        /* Match rows back to groups on the group's own representative fields. The grouper does not return the
           per-event association, and reproducing IdentityKey here would be exactly the duplicated-derivation
           this class exists to avoid — so rows are matched on the tuple the group exposes, which is what the
           identity is built from. */
        var keys = new List<string?>(events.Count);
        foreach (var e in events)
        {
            var normalizedObject = ContentiousObjectLabel.Normalize(e.ContentiousObject, e.Database);
            var match = groups.FirstOrDefault(g =>
                string.Equals(g.ContentiousObject ?? string.Empty, normalizedObject ?? string.Empty, StringComparison.Ordinal)
                && string.Equals(g.Database ?? string.Empty, e.Database ?? string.Empty, StringComparison.Ordinal)
                && string.Equals(g.BlockedQuery ?? string.Empty, e.BlockedQuery ?? string.Empty, StringComparison.Ordinal)
                && string.Equals(g.BlockingQuery ?? string.Empty, e.BlockingQuery ?? string.Empty, StringComparison.Ordinal));

            keys.Add(match?.Incident.DedupKey);
        }

        return keys;
    }

    /// <summary>
    /// The message a fingerprint filter returns when it matched nothing. Deliberately NOT the bare "empty"
    /// status the unfiltered readers use.
    ///
    /// <para>An empty result here has three quite different causes and the operator cannot tell them apart from
    /// silence: the incident is outside the window (the common one, and the fixable one), the key belongs to a
    /// DIFFERENT server than the one resolved, or the server has been renamed since the alert fired — which
    /// re-keys every fingerprint it has ever produced, because the display name is hashed into the key. Saying
    /// how many rows were examined separates "nothing to match against" from "matched against plenty and found
    /// none", which is the first thing worth knowing.</para>
    /// </summary>
    public static string NoMatchMessage(string kind, string dedupKey, string fingerprintName, int examined) =>
        $"No {kind} in the specified time range matches dedup_key {NormalizeKey(dedupKey)}. " +
        $"Examined {examined} {kind} for server '{fingerprintName}'. " +
        "The fingerprint is scoped to the server's DISPLAY name and to the incident's involved objects, so check " +
        "that hours_back reaches back to when the alert fired, that the key came from this server, and that the " +
        "server has not been renamed since — a rename changes the key for every incident on it. Re-run without " +
        "dedup_key to see what the window does contain.";
}
