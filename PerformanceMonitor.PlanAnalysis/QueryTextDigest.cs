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

namespace PerformanceMonitor.PlanAnalysis;

/// <summary>
/// The SHA-256 digest of a statement's full text (#5639). The Darling viewer sends the digest of the text it
/// showed in the consent prompt with an actual-plan request, and the service hashes the text it resolved from its
/// store and compares the two, so the statement that runs is the statement the operator confirmed. A digest is
/// not SQL, so the identifier-only command contract holds. Both ends call this one function so they cannot
/// disagree on the encoding.
/// </summary>
public static class QueryTextDigest
{
    /// <summary>The upper-case hex SHA-256 of the UTF-8 bytes of the FULL text (never a truncated display copy).</summary>
    public static string Compute(string text)
    {
        ArgumentNullException.ThrowIfNull(text);
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
    }

    /// <summary>True when <paramref name="expectedHex"/> is the digest of <paramref name="text"/> (hex case is ignored).</summary>
    public static bool Matches(string? expectedHex, string? text)
    {
        if (string.IsNullOrWhiteSpace(expectedHex) || text is null)
        {
            return false;
        }

        return string.Equals(expectedHex.Trim(), Compute(text), StringComparison.OrdinalIgnoreCase);
    }
}
