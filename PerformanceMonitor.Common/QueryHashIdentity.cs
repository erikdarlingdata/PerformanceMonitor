/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Common;

/// <summary>
/// #5711: what a stored <c>query_hash</c> is worth as a query identity on the long-query trace.
///
/// <para>The trace's <c>sqlserver.query_hash</c> action is a statement-level value, and the trace's events are
/// batch-level: measured on SQL Server 2016, 2017 and 2019 with the trace's own event and action list, EVERY
/// <c>sql_batch_completed</c> carried <c>0</c> (normal and aborted alike), and so did every <c>attention</c>. So
/// <c>0</c> here means "the engine did not say", never "query number zero". A reader that groups, counts, joins or
/// labels by the raw value puts every such row into one bucket, and on this trace that is every row.</para>
///
/// <para>The rule: <see cref="IsUnknown"/> values (NULL, empty, <c>0</c>, <c>0x0000000000000000</c>) are unknown;
/// <see cref="Identity"/> keys a row by its real hash, else by <see cref="StatementFingerprint"/> over the stored
/// (already scrubbed) statement text, else by nothing (NULL), so a row with neither stays out of every group rather
/// than joining one.</para>
///
/// <para>Two copies live in <c>PerformanceMonitor.Analysis.SameStatementPileupDetector</c> (<c>IsUnknownHash</c> and the
/// text fingerprint behind <c>StatementIdentity</c>): Analysis cannot reference this project, because that would change
/// the committed <c>packages.lock.json</c> of the deprecated Dashboard projects as well. <c>QueryHashIdentityParityTests</c>
/// runs both over one corpus; change one definition and that test fails until the other follows.</para>
/// </summary>
public static class QueryHashIdentity
{
    /// <summary>Prefix on a text fingerprint, so a fingerprint can never read as, or collide with, a hash.</summary>
    public const string FingerprintPrefix = "text:";

    /// <summary>True for NULL, empty, and any all-zero value (<c>0</c>, <c>0x0000000000000000</c>): no usable hash.</summary>
    public static bool IsUnknown(string? hash)
    {
        if (string.IsNullOrWhiteSpace(hash))
        {
            return true;
        }

        var span = hash.AsSpan().Trim();
        if (span.Length > 2 && span[0] == '0' && (span[1] == 'x' || span[1] == 'X'))
        {
            span = span[2..];
        }

        foreach (var c in span)
        {
            if (c != '0')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>The trimmed hash, or NULL when <see cref="IsUnknown"/>.</summary>
    public static string? KnownHash(string? hash) => IsUnknown(hash) ? null : hash!.Trim();

    /// <summary>
    /// A stable 16-hex-character fingerprint of the statement text (lower-cased, whitespace runs collapsed), prefixed
    /// with <see cref="FingerprintPrefix"/>. NULL for no text, and for the withheld-statement marker: that marker is
    /// stored for ANY statement a collector withheld, so a fingerprint over it would put every withheld statement in one
    /// group, the same collapse this class exists to prevent.
    /// </summary>
    public static string? StatementFingerprint(string? statementText)
    {
        if (string.IsNullOrWhiteSpace(statementText)
            || string.Equals(statementText.Trim(), SensitiveStatements.PlaceholderText, StringComparison.Ordinal))
        {
            return null;
        }

        var sb = new StringBuilder(statementText.Length);
        var lastWasSpace = false;
        foreach (var c in statementText.Trim())
        {
            if (char.IsWhiteSpace(c))
            {
                if (!lastWasSpace)
                {
                    sb.Append(' ');
                    lastWasSpace = true;
                }
            }
            else
            {
                sb.Append(char.ToLowerInvariant(c));
                lastWasSpace = false;
            }
        }

        var bytes = SHA256.HashData(Encoding.UTF8.GetBytes(sb.ToString()));
        return FingerprintPrefix + Convert.ToHexString(bytes).ToLowerInvariant()[..16];
    }

    /// <summary>The row's identity: its real hash (lower-cased), else the statement fingerprint, else NULL.</summary>
    public static string? Identity(string? hash, string? statementText) =>
        KnownHash(hash)?.ToLowerInvariant() ?? StatementFingerprint(statementText);
}
