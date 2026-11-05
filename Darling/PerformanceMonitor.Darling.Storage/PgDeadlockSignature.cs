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
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #7082: the pattern key of one PostgreSQL deadlock report, for the 30-day signature store
/// (<c>config.investigation_signature_days</c>) and the alert's page-or-hold decision.
///
/// <para><b>Why not <c>deadlock_hash</c>.</b> That hash covers the timestamp and the pids, so it marks one report, not
/// one pattern: the same two statements deadlocking twice carry two hashes.</para>
///
/// <para><b>The key is the statement pair.</b> Every participant's statement from the report's graph
/// (<see cref="PgDeadlockLogParser.ParseStatements"/>; the collector has already replaced the literals), whitespace
/// collapsed, then sorted so two sessions swapping roles give the same key. When the graph holds no statement the key falls
/// back to the lock shape: participant count, lock modes and resources. The key starts with <c>pg:</c> so it can never
/// equal a SQL Server <c>dl1-</c> signature in the same table.</para>
/// </summary>
public static class PgDeadlockSignature
{
    /// <summary>The prefix of every PostgreSQL key.</summary>
    public const string Prefix = "pg:";

    private static readonly Regex s_whitespace = new(@"\s+", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>The key, and the sorted statements it was made from (empty when the key is the lock-shape fallback).</summary>
    public readonly record struct Key(string Value, IReadOnlyList<string> Statements);

    /// <summary>
    /// The key of one report. Never null: a report with neither statements nor shape still gets the key of an empty shape, so
    /// it counts as one pattern rather than vanishing.
    /// </summary>
    public static Key For(string? graphText, int participantCount, string? lockModes, string? resources)
    {
        var statements = new List<string>();
        if (!string.IsNullOrWhiteSpace(graphText))
        {
            try
            {
                statements = PgDeadlockLogParser.ParseStatements(graphText)
                    .Values
                    .Select(v => s_whitespace.Replace(v, " ").Trim())
                    .Where(v => v.Length > 0)
                    .OrderBy(v => v, StringComparer.Ordinal)
                    .ToList();
            }
            catch (Exception ex) when (ex is ArgumentException or RegexMatchTimeoutException)
            {
                statements = new List<string>();
            }
        }

        var text = statements.Count > 0
            ? "s:" + string.Join("\n", statements)
            : string.Create(CultureInfo.InvariantCulture, $"x:{participantCount}|{lockModes}|{resources}");
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes(text));
        return new Key(
            Prefix + Convert.ToHexString(hash, 0, 8).ToLowerInvariant(),
            statements);
    }
}
