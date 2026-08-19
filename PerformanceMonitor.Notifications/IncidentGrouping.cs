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
/// Brings a stored <c>contentious_object</c> label to the <c>blocked_process_report</c> collector's
/// conventions before it becomes an incident identity (#1876).
///
/// <para>
/// Two collectors write that column and they never agreed. <c>blocked_process_report</c> writes a plain
/// <c>schema.object</c>, or an <c>Unresolved: key lock, database: Foo</c> sentinel when the lookup failed.
/// <c>dmv_blocking_snapshots</c> writes a <c>QUOTENAME</c>'d <c>[schema].[object]</c>, and for a KEY, PAGE
/// or RID lock — which it does not resolve at all — the RAW wait resource. Both feed one list into
/// <see cref="BlockingIncidentGrouper"/>, which hashes the label into the incident's dedup key, so the
/// same contended table blocking the same way alerted twice depending on which collector saw it.
/// </para>
///
/// <para>
/// The raw wait resource is the worse half. <c>KEY: 5:72057594041991168 (8194443284a0)</c> carries a hobt
/// id and a per-VALUE lock hash, so it is not merely a second identity for the object — it is a NEW
/// identity for almost every row, which turns one recurring unresolvable lock into an unbounded stream of
/// one-occurrence incidents. Collapsing it to the sentinel gives those rows the same
/// (lock type, database) grouping the report side has always had.
/// </para>
///
/// <para>
/// Deliberately a total, idempotent function on the column rather than a DMV-only transform, because one
/// read surface — the <c>top_blocking_chains</c> drill-down — <c>UNION</c>s the two sources and cannot
/// tell them apart by then. A label already in report form has no bracket to strip and cannot match the
/// wait-resource shape (that pattern requires an ALL-CAPS leading token, which <c>Unresolved: </c> is
/// not, and a plain <c>schema.object</c> has no <c>": "</c> at all), so it is returned untouched. Only
/// DMV-shaped values change, which is what keeps the fingerprint churn on the fallback path.
/// </para>
/// </summary>
public static class ContentiousObjectLabel
{
    /// <summary>The report collector's sentinel prefix, spelled once for both the producer's format and this.</summary>
    public const string UnresolvedPrefix = "Unresolved: ";

    /// <summary>What the report collector's <c>ISNULL(DB_NAME(...), N'unknown')</c> writes for a database it cannot name.</summary>
    public const string UnknownDatabase = "unknown";

    /// <summary>
    /// A wait resource's leading type token: ALL CAPS, then <c>": "</c>. Matches <c>KEY: </c>,
    /// <c>PAGE: </c>, <c>ALLOCATION_UNIT: </c>; deliberately does NOT match <c>Unresolved: </c>, whose
    /// token is mixed case, nor a plain <c>schema.object</c>, which has no colon.
    /// </summary>
    private static readonly Regex s_waitResource =
        new(@"^[A-Z][A-Z0-9_]*: ", RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// The lock-type order the collector's own classifier uses — an embedded type in a compound resource
    /// wins over the leading token, and the four are tested in this exact sequence.
    /// </summary>
    private static readonly string[] s_lockTypes = { "KEY", "OBJECT", "RID", "PAGE" };

    /// <summary>
    /// Returns <paramref name="value"/> in report-collector form. <paramref name="databaseName"/> names the
    /// database in a sentinel this has to synthesize; it is unused for a value that needs no rewrite.
    /// </summary>
    public static string Normalize(string? value, string? databaseName)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return value ?? string.Empty;
        }

        var trimmed = value.Trim();

        if (trimmed[0] == '[')
        {
            /* A QUOTENAME'd path is the DMV side's RESOLVED form and the report side's is unquoted, so
               the only difference between them is the brackets. Unquoting rather than string-replacing
               keeps an identifier that legitimately contains ']' (doubled by QUOTENAME) intact; anything
               that does not parse cleanly is left alone rather than mangled. */
            return TryUnquotePath(trimmed, out var plain) ? plain : value;
        }

        if (!s_waitResource.IsMatch(trimmed))
        {
            return value;
        }

        var lockType = LockTypeOf(trimmed);
        var database = string.IsNullOrWhiteSpace(databaseName) ? UnknownDatabase : databaseName.Trim();

        return lockType.Length == 0
            ? UnresolvedPrefix + "database: " + database
            : UnresolvedPrefix + lockType.ToLowerInvariant() + " lock, database: " + database;
    }

    /// <summary>
    /// The collector's classifier, in C#: a type embedded anywhere in a compound resource wins, otherwise
    /// the leading token up to the first colon, upper-cased and capped at the same 32 characters.
    /// </summary>
    private static string LockTypeOf(string waitResource)
    {
        foreach (var candidate in s_lockTypes)
        {
            if (waitResource.Contains(candidate + ": ", StringComparison.Ordinal))
            {
                return candidate;
            }
        }

        var colon = waitResource.IndexOf(':', StringComparison.Ordinal);
        var token = colon >= 0 ? waitResource[..colon] : waitResource;
        token = token.ToUpperInvariant();
        return token.Length <= 32 ? token : token[..32];
    }

    /// <summary>Reverses <c>QUOTENAME(a) + '.' + QUOTENAME(b)</c>, including its doubled <c>]]</c> escape.</summary>
    private static bool TryUnquotePath(string value, out string plain)
    {
        plain = string.Empty;
        var parts = new List<string>(2);
        var i = 0;

        while (i < value.Length)
        {
            if (value[i] != '[')
            {
                return false;
            }

            i++;
            var part = new System.Text.StringBuilder();
            var closed = false;
            while (i < value.Length)
            {
                if (value[i] == ']')
                {
                    if (i + 1 < value.Length && value[i + 1] == ']')
                    {
                        part.Append(']');
                        i += 2;
                        continue;
                    }

                    i++;
                    closed = true;
                    break;
                }

                part.Append(value[i]);
                i++;
            }

            if (!closed)
            {
                return false;
            }

            parts.Add(part.ToString());

            if (i == value.Length)
            {
                break;
            }

            if (value[i] != '.')
            {
                return false;
            }

            i++;
            if (i == value.Length)
            {
                return false;
            }
        }

        if (parts.Count == 0)
        {
            return false;
        }

        plain = string.Join('.', parts);
        return true;
    }
}

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

        foreach (var raw in events ?? Enumerable.Empty<BlockedEvent>())
        {
            /* #1876: normalize the label HERE, at the one point every blocking fingerprint passes
               through, rather than where the two collectors' rows are merged. Both matter:
               - Coverage. Two independent producers group blocking (the live alert builders and the
                 analysis drill-down, whose SQL UNIONs the two collectors), and only one of them goes
                 through the merge. Normalizing at the identity is the only placement a third producer
                 cannot arrive without.
               - Not destroying evidence. dmv_blocking_snapshots stores NO wait_resource column, so for
                 a lock it could not resolve, contentious_object IS the raw resource — the hobt id and
                 lock hash included. Rewriting it at the merge would erase that from every grid and MCP
                 payload the merge also feeds. Storage and display keep what the collector wrote; only
                 the INCIDENT IDENTITY is normalized. */
            var e = raw with
            {
                ContentiousObject = ContentiousObjectLabel.Normalize(raw.ContentiousObject, raw.Database),
            };

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
            /* #2361: the group already knows its database, so the incident carries it rather than leaving a
               consumer to string-search Details[] for it. Attached at the `with` so both the ForObjects and
               ForKey branches above get it from one place. UnknownDatabase is ContentiousObjectLabel's sentinel for
               "the row had none" and becomes null here -- a literal "unknown" in a Database field reads as a
               database actually called that. */
            var enriched = incident with
            {
                DetailFields = BlockingDetail(representative),
                Database = IncidentDatabaseHelpers.NormalizeDatabase(representative.Database),
            };

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

internal static class IncidentDatabaseHelpers
{
    /// <summary>
    /// The grouper's <c>UnknownDatabase</c> sentinel means "the row carried none" (#2361). It is fine as a
    /// display string and wrong as a data member: a consumer routing on <c>Database</c> would file tickets
    /// against a database literally named "unknown".
    /// </summary>
    internal static string? NormalizeDatabase(string? database) =>
        string.IsNullOrWhiteSpace(database)
        || string.Equals(database, ContentiousObjectLabel.UnknownDatabase, StringComparison.OrdinalIgnoreCase)
            ? null
            : database;

    /// <summary>
    /// Pulls the #2109 <c>Database</c> fact off an incident's detail fields (#2361) — the value a consumer was
    /// otherwise string-searching the payload for. Label match is case-insensitive; the first wins, because a
    /// deadlock graph spanning several databases already ships them as one CSV rather than repeated fields.
    /// </summary>
    internal static string? DatabaseFromFields(IReadOnlyList<AlertIncidentField>? fields)
    {
        if (fields is null)
        {
            return null;
        }

        foreach (var field in fields)
        {
            if (string.Equals(field.Label, "Database", StringComparison.OrdinalIgnoreCase))
            {
                return NormalizeDatabase(field.Value);
            }
        }

        return null;
    }
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
            /* #2361: deadlocks carry their database as a #2109 discrete fact on the representative's detail
               fields (a CSV when the graph spans several). Reading it here is the same lookup a consumer was
               doing by hand downstream -- done once, in the projection, where it is exact. */
            var incident = baseIncident with
            {
                OccurrenceCount = count,
                DetailFields = details[key],
                Database = IncidentDatabaseHelpers.DatabaseFromFields(details[key]),
            };
            groups.Add(new DeadlockGroup(incident.InvolvedObjects, count, incident));
        }

        groups.Sort((a, b) => b.OccurrenceCount.CompareTo(a.OccurrenceCount));
        return groups;
    }
}
