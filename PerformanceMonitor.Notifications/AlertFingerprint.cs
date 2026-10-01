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
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// Computes the stable, machine-readable dedup fingerprint and the human-readable involved-objects
/// list that ride on an <see cref="AlertIncident"/> (#1140). Downstream automation (e.g. a Logic App
/// creating Azure DevOps tickets) matches on the fingerprint to collapse recurrences of the same
/// incident instead of opening a new ticket each time.
///
/// <para>
/// The fingerprint hashes only STABLE identity members — the involved objects (or a natural key like
/// a query_hash / job name / mount point), scoped by server and incident type. Volatile per-sample
/// values (wait time, durations, SPIDs, counts) are deliberately excluded so the same incident hashes
/// identically across collection cycles. Reuses the SHA-256 idiom from
/// <c>PerformanceMonitor.Analysis.InferenceEngine</c>.
/// </para>
/// </summary>
public static class AlertFingerprint
{
    /// <summary>Incident-type tags hashed into the key so the same objects under different incident
    /// kinds (a deadlock vs a blocking chain on the same table) never collide.</summary>
    public const string Deadlock = "deadlock";
    public const string Blocking = "blocking";
    public const string Query = "query";
    public const string Job = "job";
    public const string Disk = "disk";
    public const string Database = "database";

    private static readonly Regex s_whitespace = new(@"\s+", RegexOptions.Compiled);

    /// <summary>
    /// Builds an incident from the set of fully-qualified objects it involves. The fingerprint is a
    /// hash over (serverName, incidentType, sorted-distinct-normalized objects); the returned
    /// <see cref="AlertIncident.InvolvedObjects"/> preserves the original casing (deduped, ordered)
    /// for display. Returns <c>null</c> when no usable object remains (caller emits no incident).
    /// </summary>
    public static AlertIncident? ForObjects(
        string serverName,
        string incidentType,
        IEnumerable<string> objects,
        int occurrenceCount = 1,
        string? waitRange = null)
    {
        // Dedup case-insensitively by normalized form, keep the first original casing for display,
        // order by normalized form so both the hash input and the display list are stable.
        var byNormalized = new SortedDictionary<string, string>(StringComparer.Ordinal);
        if (objects is not null)
        {
            foreach (var o in objects)
            {
                var normalized = Normalize(o);
                if (normalized.Length == 0)
                    continue;
                if (!byNormalized.ContainsKey(normalized))
                    byNormalized[normalized] = (o ?? string.Empty).Trim();
            }
        }

        if (byNormalized.Count == 0)
            return null;

        var key = Hash(BuildInput(serverName, incidentType, byNormalized.Keys));
        return new AlertIncident(key, byNormalized.Values.ToList(), occurrenceCount, waitRange);
    }

    /// <summary>
    /// Builds an incident from a single natural key (a query_hash, job name, mount point, or a
    /// "database:object_id" fallback) rather than an object set. The fingerprint hashes
    /// (serverName, incidentType, normalized naturalKey); <paramref name="displayObjects"/> are
    /// shown to the user but do NOT affect the key. Returns <c>null</c> when the key is blank.
    /// </summary>
    public static AlertIncident? ForKey(
        string serverName,
        string incidentType,
        string naturalKey,
        IReadOnlyList<string>? displayObjects = null,
        int occurrenceCount = 1,
        string? waitRange = null,
        /* #2361: the incident's database scope. Optional because most fingerprint kinds are not
           database-scoped -- a disk or a job is not -- and a caller that has no database says so by omission
           rather than by passing an empty string that would read as "no database" downstream. */
        string? database = null)
    {
        var normalized = Normalize(naturalKey);
        if (normalized.Length == 0)
            return null;

        var key = Hash(BuildInput(serverName, incidentType, new[] { normalized }));
        return new AlertIncident(
            key, displayObjects ?? Array.Empty<string>(), occurrenceCount, waitRange,
            Database: string.IsNullOrWhiteSpace(database) ? null : database);
    }

    /// <summary>
    /// The server string to hash into a dedup key. It is <paramref name="serverName"/> unchanged EXCEPT when
    /// another registration carries the same display name (<paramref name="nameIsShared"/>) and the server has
    /// a store id: then it is <c>name#id</c>.
    ///
    /// <para><b>Why only a shared name.</b> The display name is not unique: two databases registered with blank
    /// names on one Azure SQL Database logical server both display as the host, and two registrations can be
    /// typed with the same name. The same incident then hashed to the same key and a pager or webhook merged
    /// two real incidents. A server whose name is unique is already distinct by name, and changing its key
    /// would re-deliver every live incident on it and open a fresh PagerDuty incident, so its key stays
    /// byte-identical, whether the name is blank-on-host, host-equal or typed.</para>
    ///
    /// <para><b>Stability.</b> The id is the deterministic store id (a hash of the canonical storage
    /// identity), so a registration's key is the same across restarts. It changes when the server joins or
    /// leaves a same-named group, which is exactly when its name becomes, or stops being, ambiguous.</para>
    /// </summary>
    public static string ServerIdentity(string serverName, int? serverId, bool nameIsShared) =>
        nameIsShared && serverId.HasValue
            ? serverName + "#" + serverId.Value.ToString(System.Globalization.CultureInfo.InvariantCulture)
            : serverName;

    /// <summary>
    /// The display names that more than one registration carries, compared ordinal (a name differing only by
    /// case is a different name to every consumer of the key). The one helper both sides call over the same
    /// population, so the alert path and the MCP <c>dedup_key</c> filter cannot disagree on which servers
    /// get a suffix.
    /// </summary>
    public static IReadOnlySet<string> SharedDisplayNames(IEnumerable<string> displayNames)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var shared = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in displayNames)
        {
            if (name is not null && !seen.Add(name))
            {
                shared.Add(name);
            }
        }

        return shared;
    }

    /// <summary>SHA-256 of <paramref name="input"/> as lowercase hex (64 chars). Public for tests.</summary>
    public static string Hash(string input)
    {
        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(input ?? string.Empty));
        return Convert.ToHexString(bytes).ToLowerInvariant();
    }

    // serverName | incidentType | identity members. serverName/incidentType are normalized here;
    // members arrive already normalized. The server scope makes the same object name on two
    // instances two distinct incidents; the type tag separates deadlock from blocking.
    private static string BuildInput(string serverName, string incidentType, IEnumerable<string> normalizedMembers)
    {
        var sb = new StringBuilder();
        sb.Append(Normalize(serverName)).Append('|').Append(Normalize(incidentType));
        foreach (var member in normalizedMembers)
            sb.Append('|').Append(member);
        return sb.ToString();
    }

    // Trim, collapse internal whitespace runs to a single space, lower-invariant. Stable identity
    // at the cost of merging names that differ only by case (accepted; see plan SS9 Q3).
    private static string Normalize(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return string.Empty;
        return s_whitespace.Replace(value.Trim(), " ").ToLowerInvariant();
    }
}
