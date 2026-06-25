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
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Groups raw blocked-process rows into distinct incidents (#1140 / #1141). Today's alert lists the
/// same blocking chain several times (one row per sample, differing only by wait time) and caps the
/// list at a few rows while the count says more — gotqn's report. This collapses rows that share an
/// incident identity into one group carrying the true occurrence count and wait range, and attaches
/// the stable dedup fingerprint. Both apps' live "Blocking Detected" builders call this, so the
/// grouping + fingerprint are identical across Lite and Dashboard.
/// <para>
/// Incident identity (volatile fields excluded): the resolved contentious object when available
/// (Dashboard's <c>contentious_object</c> / Lite's event-resolved object — stable across index
/// rebuilds), else database + literal-stripped blocked/blocking query pair, matching plan §4.2.
/// </para>
/// </summary>
public static class BlockingIncidentGrouper
{
    /// <summary>One blocked-process sample projected from each app's row model into a shared shape.</summary>
    public readonly record struct BlockedEvent(
        string? Database,
        string? ContentiousObject,
        string? BlockedQuery,
        string? BlockingQuery,
        long WaitTimeMs,
        string? LockMode = null);

    /// <summary>One distinct blocking incident: a representative chain, its true occurrence count and
    /// wait range, and the dedup <see cref="AlertIncident"/>.</summary>
    public sealed record BlockingGroup(
        string? Database,
        string? ContentiousObject,
        string? BlockedQuery,
        string? BlockingQuery,
        int OccurrenceCount,
        long MinWaitMs,
        long MaxWaitMs,
        AlertIncident Incident);

    private static readonly Regex s_stringLiteral = new("'(?:[^']|'')*'", RegexOptions.Compiled);
    private static readonly Regex s_numericLiteral = new(@"\b\d+(\.\d+)?\b", RegexOptions.Compiled);
    private static readonly Regex s_whitespace = new(@"\s+", RegexOptions.Compiled);

    /// <summary>
    /// Collapses the events into one group per distinct incident, preserving input order of first
    /// appearance, with the highest occurrence count first. Empty input returns an empty list.
    /// </summary>
    public static List<BlockingGroup> Group(string serverName, IEnumerable<BlockedEvent> events)
    {
        var order = new List<string>();
        var buckets = new Dictionary<string, List<BlockedEvent>>(StringComparer.Ordinal);

        foreach (var e in events ?? Enumerable.Empty<BlockedEvent>())
        {
            var identity = IdentityKey(e);
            if (!buckets.TryGetValue(identity, out var list))
            {
                list = new List<BlockedEvent>();
                buckets[identity] = list;
                order.Add(identity);
            }
            list.Add(e);
        }

        var groups = new List<BlockingGroup>(order.Count);
        foreach (var identity in order)
        {
            var rows = buckets[identity];
            var representative = rows[0];
            var minWait = rows.Min(r => r.WaitTimeMs);
            var maxWait = rows.Max(r => r.WaitTimeMs);
            var waitRange = FormatWaitRange(minWait, maxWait);

            var hasObject = !string.IsNullOrWhiteSpace(representative.ContentiousObject);
            var incident = hasObject
                ? AlertFingerprint.ForObjects(
                    serverName, AlertFingerprint.Blocking,
                    new[] { representative.ContentiousObject! }, rows.Count, waitRange)
                : AlertFingerprint.ForKey(
                    serverName, AlertFingerprint.Blocking,
                    QueryPairKey(representative),
                    DatabaseDisplay(representative.Database), rows.Count, waitRange);

            // ForObjects/ForKey only return null on empty identity, which IdentityKey already excludes,
            // so incident is non-null here; guard defensively rather than assert.
            if (incident is null)
                continue;

            // #1141: carry the chain's forensic detail on the incident so per-event cards keep it
            // (Summary already shows it via the builder's items; this travels for the per-event split).
            var enriched = incident with { DetailFields = BlockingDetail(representative) };

            groups.Add(new BlockingGroup(
                representative.Database, representative.ContentiousObject,
                representative.BlockedQuery, representative.BlockingQuery,
                rows.Count, minWait, maxWait, enriched));
        }

        groups.Sort((a, b) => b.OccurrenceCount.CompareTo(a.OccurrenceCount));
        return groups;
    }

    private static string IdentityKey(BlockedEvent e) =>
        !string.IsNullOrWhiteSpace(e.ContentiousObject)
            ? "obj|" + Norm(e.Database) + "|" + Norm(e.ContentiousObject)
            : "qp|" + QueryPairKey(e);

    private static string QueryPairKey(BlockedEvent e) =>
        Norm(e.Database) + "|" + NormalizeQuery(e.BlockedQuery) + "|" + NormalizeQuery(e.BlockingQuery);

    private static string[] DatabaseDisplay(string? database) =>
        string.IsNullOrWhiteSpace(database) ? Array.Empty<string>() : new[] { database! };

    private static string Norm(string? s) =>
        string.IsNullOrWhiteSpace(s) ? string.Empty : s_whitespace.Replace(s.Trim(), " ").ToLowerInvariant();

    // Strip string + numeric literals so chains differing only by parameter values group together
    // ("WHERE id = 5" and "WHERE id = 9" -> the same key), then collapse whitespace + lower.
    private static string NormalizeQuery(string? query)
    {
        if (string.IsNullOrWhiteSpace(query))
            return string.Empty;
        var s = s_stringLiteral.Replace(query, "'?'");
        s = s_numericLiteral.Replace(s, "?");
        return s_whitespace.Replace(s.Trim(), " ").ToLowerInvariant();
    }

    private static string FormatWaitRange(long minMs, long maxMs)
    {
        string Sec(long ms) => (ms / 1000.0).ToString("F1", CultureInfo.InvariantCulture) + "s";
        return minMs == maxMs ? Sec(maxMs) : Sec(minMs) + "-" + Sec(maxMs);
    }

    // Forensic detail for a blocking incident's per-event card (#1141): the representative chain's
    // database, contentious object, the blocked/blocking query pair (truncated), and lock mode.
    private static List<AlertIncidentField> BlockingDetail(BlockedEvent e)
    {
        var f = new List<AlertIncidentField>();
        if (!string.IsNullOrWhiteSpace(e.Database)) f.Add(new AlertIncidentField("Database", e.Database!));
        if (!string.IsNullOrWhiteSpace(e.ContentiousObject)) f.Add(new AlertIncidentField("Contentious Object", e.ContentiousObject!));
        if (!string.IsNullOrWhiteSpace(e.BlockedQuery)) f.Add(new AlertIncidentField("Blocked Query", Truncate(e.BlockedQuery!)));
        if (!string.IsNullOrWhiteSpace(e.BlockingQuery)) f.Add(new AlertIncidentField("Blocking Query", Truncate(e.BlockingQuery!)));
        if (!string.IsNullOrWhiteSpace(e.LockMode)) f.Add(new AlertIncidentField("Lock Mode", e.LockMode!));
        return f;
    }

    private static string Truncate(string s) => s.Length <= 300 ? s : s.Substring(0, 300) + "…";
}

/// <summary>
/// Groups deadlock events by the sorted set of fully-qualified objects involved (#1140). One
/// deadlock spanning multiple databases/objects is a single incident listing them all; recurrences
/// over the same object set collapse to one fingerprint with the occurrence count. Both apps feed
/// the per-event object lists (Dashboard from <c>DeadlockItem.ObjectNames</c>, Lite via
/// <see cref="DeadlockObjectExtractor"/>) so the fingerprint is identical across them.
/// </summary>
public static class DeadlockIncidentGrouper
{
    /// <summary>One deadlock event projected to the distinct fully-qualified objects it involved, plus
    /// optional forensic detail (Victim SQL / Processes) carried onto the incident for per-event cards.</summary>
    public readonly record struct DeadlockEvent(
        IReadOnlyList<string> Objects,
        IReadOnlyList<AlertIncidentField>? DetailFields = null);

    /// <summary>One distinct deadlock incident: the involved object set, occurrence count, and fingerprint.</summary>
    public sealed record DeadlockGroup(IReadOnlyList<string> Objects, int OccurrenceCount, AlertIncident Incident);

    /// <summary>
    /// Collapses events sharing an involved-object set into one group (highest occurrence count first).
    /// Events with no parseable objects are skipped (no fingerprintable identity); the builder still
    /// displays them. Empty input returns an empty list.
    /// </summary>
    public static List<DeadlockGroup> Group(string serverName, IEnumerable<DeadlockEvent> events)
    {
        var order = new List<string>();
        var counts = new Dictionary<string, int>(StringComparer.Ordinal);
        var incidents = new Dictionary<string, AlertIncident>(StringComparer.Ordinal);
        var details = new Dictionary<string, IReadOnlyList<AlertIncidentField>?>(StringComparer.Ordinal);

        foreach (var e in events ?? Enumerable.Empty<DeadlockEvent>())
        {
            // Probe identity first so we group by the SAME normalized set the fingerprint hashes.
            var probe = AlertFingerprint.ForObjects(serverName, AlertFingerprint.Deadlock, e.Objects ?? Array.Empty<string>());
            if (probe is null)
                continue; // no parseable objects -> not fingerprintable

            var key = probe.DedupKey;
            if (!counts.TryGetValue(key, out var current))
            {
                order.Add(key);
                incidents[key] = probe;
                details[key] = e.DetailFields; // first event of the group is the representative
                current = 0;
            }
            counts[key] = current + 1;
        }

        var groups = new List<DeadlockGroup>(order.Count);
        foreach (var key in order)
        {
            var count = counts[key];
            var baseIncident = incidents[key];
            // Re-stamp the occurrence count + carry the representative's forensic detail (#1141).
            var incident = baseIncident with { OccurrenceCount = count, DetailFields = details[key] };
            groups.Add(new DeadlockGroup(incident.InvolvedObjects, count, incident));
        }

        groups.Sort((a, b) => b.OccurrenceCount.CompareTo(a.OccurrenceCount));
        return groups;
    }
}
