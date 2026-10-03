/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The install id (#4961): the eight characters that tell one install's Extended Events sessions from
/// another's, in the name of every session an install makes on a server it monitors. Lite keeps its id in a file
/// at its data root and Darling keeps its in the store; both make and check it here, so the format has one
/// definition.
///
/// <para><b>Format.</b> Exactly eight lowercase hex digits, checked on every load, and a value that fails the
/// check is replaced rather than repaired. Lowercase only, because on a case-sensitive server collation an id read
/// back in another case would name a different session than the one it made.</para>
///
/// <para>Nothing here touches the operating system beyond the random number generator, so it runs the same on
/// Linux, where Darling does.</para>
/// </summary>
public static class InstallId
{
    /// <summary>The number of characters in an id.</summary>
    public const int Length = 8;

    /// <summary>
    /// True when <paramref name="id"/> is exactly <see cref="Length"/> lowercase hex digits and nothing else:
    /// not null, no padding or line ending (a pattern anchored with <c>$</c> would let a trailing newline
    /// through), and no digit or letter from another script (<c>char.IsDigit</c> would).
    /// </summary>
    public static bool IsValid(string? id)
    {
        if (id is null || id.Length != Length)
        {
            return false;
        }

        foreach (var c in id)
        {
            if (!(c is >= '0' and <= '9' || c is >= 'a' and <= 'f'))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// A new random id, from the cryptographic generator rather than <see cref="Random"/>: two installs that
    /// start in the same moment must not share a seed and so must not share an id.
    /// </summary>
    public static string NewId() => Convert.ToHexStringLower(RandomNumberGenerator.GetBytes(Length / 2));
}
