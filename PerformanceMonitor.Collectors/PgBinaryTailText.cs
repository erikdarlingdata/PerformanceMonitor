/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Turns the binary route's bytes back into text leniently (#4046 part 1c): an invalid byte a failed
/// login planted becomes U+FFFD instead of the 22021 the text route throws on it.
/// </summary>
public static class PgBinaryTailText
{
    /// <summary>
    /// Decodes a whole <c>bytea</c> tail (the log_events route, which returns the body whole rather than
    /// filtering it server-side). <see cref="Encoding.UTF8"/>'s static instance already uses
    /// <see cref="DecoderReplacementFallback"/> — it does not throw on an invalid byte, it substitutes
    /// U+FFFD — so no custom decoder is needed here.
    /// </summary>
    public static string DecodeWhole(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        return Encoding.UTF8.GetString(bytes);
    }

    /// <summary>
    /// Reverses <c>encode(bytea, 'escape')</c> on text a server-side regex already matched over
    /// (<see cref="PgServerLogTail.TailCteBinarySql"/>'s deadlock/plan-capture route), then decodes the
    /// recovered bytes the same lenient way as <see cref="DecodeWhole"/>.
    ///
    /// <para><b>Verified on the rig (PostgreSQL 18.6), not assumed:</b> <c>escape</c> leaves every
    /// printable-ASCII byte (32-126) literal, INCLUDING control bytes like tab (0x09) and newline (0x0A) —
    /// they are not octal-escaped — doubles a literal backslash (0x5C to two 0x5C bytes), and octal-escapes
    /// (<c>\NNN</c>, exactly three digits) NUL and every byte at or above 0x80. That is why the existing
    /// deadlock/plan-capture patterns, which match literal <c>\n</c>/<c>\t</c> bytes, run UNCHANGED over
    /// this escaped text: those bytes never become a backslash sequence. Only backslash and <c>\NNN</c>
    /// need reversing here, which is what this method does — every other character escape() emits is a
    /// single printable ASCII code point, so casting it straight to <see cref="byte"/> is exact.</para>
    /// </summary>
    public static string UnescapeAndDecode(string escaped)
    {
        ArgumentNullException.ThrowIfNull(escaped);

        var bytes = new byte[escaped.Length];
        var length = 0;

        for (var i = 0; i < escaped.Length; i++)
        {
            var c = escaped[i];

            if (c == '\\' && i + 1 < escaped.Length)
            {
                if (escaped[i + 1] == '\\')
                {
                    bytes[length++] = (byte)'\\';
                    i++;
                    continue;
                }

                if (i + 3 < escaped.Length
                    && IsOctalDigit(escaped[i + 1]) && IsOctalDigit(escaped[i + 2]) && IsOctalDigit(escaped[i + 3]))
                {
                    var value = (escaped[i + 1] - '0') * 64 + (escaped[i + 2] - '0') * 8 + (escaped[i + 3] - '0');
                    bytes[length++] = (byte)value;
                    i += 3;
                    continue;
                }
            }

            /* Every character escape() emits outside the two forms above is a literal single-byte octet
               escape() left untouched — NOT only printable ASCII (32-126): verified live that bytes
               0x01-0x1F and 0x7F (control bytes, including a literal tab and newline) pass through exactly
               as printable ASCII does. Only NUL (0x00) and bytes >= 0x80 are octal-escaped; see the type
               header. One UTF-16 char, one byte, by construction either way. */
            bytes[length++] = (byte)c;
        }

        return Encoding.UTF8.GetString(bytes, 0, length);
    }

    private static bool IsOctalDigit(char c) => c is >= '0' and <= '7';
}
