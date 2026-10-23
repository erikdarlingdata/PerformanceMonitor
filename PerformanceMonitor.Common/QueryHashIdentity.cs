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
/// <para>#5758: this is the ONE definition. <c>PerformanceMonitor.Analysis.SameStatementPileupDetector.StatementIdentity</c>
/// calls <see cref="Identity"/> (Analysis references this project), and <c>QueryHashIdentityTests</c> pins the rule's
/// cases, so there is no second copy to drift (#3684).</para>
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

    /// <summary>
    /// #5980: the one text a query-tag key uses for a hash. The DMV and Query Store collectors store <c>0x</c> plus 16
    /// hex digits; the long-query trace stores the raw XE <c>uint64</c> as decimal digits. A query seen by both must
    /// be one key, so a decimal value becomes <c>0x</c> plus 16 upper-case hex digits, a <c>0x</c> value is returned
    /// trimmed with its digits upper-cased, and an <see cref="IsUnknown"/> value (or text that is neither) yields NULL.
    /// </summary>
    public static string? CanonicalKey(string? hash)
    {
        var known = KnownHash(hash);
        if (known is null)
        {
            return null;
        }

        if (known.Length > 2 && known[0] == '0' && (known[1] == 'x' || known[1] == 'X'))
        {
            /* The resolver compares hashes ignoring case, so the key's digits are folded to one case here (#6020). */
            return "0x" + known[2..].ToUpperInvariant();
        }

        return ulong.TryParse(known, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var value)
            ? "0x" + value.ToString("X16", System.Globalization.CultureInfo.InvariantCulture)
            : known;
    }

    /// <summary>
    /// #6455: whether text can be the key of a NEW query-tag assignment: <c>0x</c> and 1 to 16 hex digits, or up to 20 decimal
    /// digits that fit an unsigned 64-bit number (the shapes the DMV, Query Store and long-query trace collectors store), and not
    /// the did-not-say hash (<see cref="IsUnknown"/>: zero in either spelling), which labels no query. Both products' assign tool
    /// asks this one question, so a key is refused the same way on each.
    /// </summary>
    public static bool IsAssignableHash(string? hash)
    {
        var known = KnownHash(hash);
        if (known is null)
        {
            return false;
        }

        if (known.Length > 2 && known[0] == '0' && (known[1] == 'x' || known[1] == 'X'))
        {
            var digits = known.AsSpan(2);
            if (digits.Length > 16)
            {
                return false;
            }

            foreach (var c in digits)
            {
                if (!Uri.IsHexDigit(c))
                {
                    return false;
                }
            }

            return true;
        }

        return ulong.TryParse(known, System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out _);
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
