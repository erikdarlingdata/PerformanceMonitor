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
        string? waitRange = null)
    {
        var normalized = Normalize(naturalKey);
        if (normalized.Length == 0)
            return null;

        var key = Hash(BuildInput(serverName, incidentType, new[] { normalized }));
        return new AlertIncident(key, displayObjects ?? Array.Empty<string>(), occurrenceCount, waitRange);
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
