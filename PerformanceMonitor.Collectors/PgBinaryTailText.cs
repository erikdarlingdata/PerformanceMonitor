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
    /// filtering it server-side), using <paramref name="encoding"/> — the connected database's own
    /// <c>server_encoding</c>, mapped by <see cref="PgServerEncoding"/> (#4062). Every <see cref="Encoding"/>
    /// this route is called with, including <see cref="Encoding.UTF8"/>, already substitutes U+FFFD for an
    /// invalid byte rather than throwing, so no custom decoder is needed here.
    /// </summary>
    public static string DecodeWhole(byte[] bytes, Encoding encoding)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        ArgumentNullException.ThrowIfNull(encoding);
        return WithoutNul(SplitAndDecode(bytes, bytes.Length, encoding));
    }

    /// <summary>
    /// Reverses <c>encode(bytea, 'escape')</c> on text a server-side regex already matched over
    /// (<see cref="PgServerLogTail.TailCteBinarySql"/>'s deadlock/plan-capture route), then decodes the
    /// recovered bytes the same lenient way as <see cref="DecodeWhole"/>.
    ///
    /// <para><b>Verified on the rig (PostgreSQL 18.6), not assumed:</b> <c>escape</c> leaves every byte
    /// from 0x01 to 0x7F literal except the backslash, control bytes like tab (0x09) and newline (0x0A)
    /// included — they are not octal-escaped — doubles a literal backslash (0x5C to two 0x5C bytes), and
    /// octal-escapes (<c>\NNN</c>, exactly three digits) NUL and every byte at or above 0x80. That is why the
    /// existing deadlock/plan-capture patterns, which match literal <c>\n</c>/<c>\t</c> bytes, run UNCHANGED
    /// over this escaped text: those bytes never become a backslash sequence. Only backslash and
    /// <c>\NNN</c> need reversing here, which is what this method does — every other character escape()
    /// emits is a single ASCII code point, so casting it straight to <see cref="byte"/> is exact.</para>
    /// </summary>
    public static string UnescapeAndDecode(string escaped, Encoding encoding)
    {
        ArgumentNullException.ThrowIfNull(escaped);
        ArgumentNullException.ThrowIfNull(encoding);

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

        return WithoutNul(SplitAndDecode(bytes, length, encoding));
    }

    /// <summary>
    /// Some DBCS decoders (EUC-JP/936/EUC-KR, code pages 51932/936/51949) pair an unmapped lead byte with
    /// whatever byte follows it, even an ASCII byte like 0x0A or 0x22, so a planted lead byte right before a
    /// real newline or quote eats the delimiter and can merge the next real line — or, for a quote, invert
    /// csvlog quote parity for everything before it (#4062 review, Medium; #4053 review M3) — into the
    /// attacker's. Splitting on the raw bytes 0x0A and 0x22 before decoding, then decoding each segment and
    /// re-emitting each delimiter byte literally, restores the property .NET's own UTF-8 decoder has: an
    /// ASCII control or punctuation byte is never consumed as a trail byte. This is safe for every encoding
    /// this route is called with: neither byte is ever a valid trail byte in EUC-JP, EUC-KR or GBK (their
    /// trail bytes are all 0xA1 or higher, and GBK's 0x40 floor is still above both), and single-byte
    /// encodings treat each as one character regardless. Valid text decodes to exactly the same string
    /// either way.
    /// </summary>
    private static string SplitAndDecode(byte[] bytes, int length, Encoding encoding)
    {
        var sb = new StringBuilder();
        var start = 0;

        for (var i = 0; i < length; i++)
        {
            var b = bytes[i];

            if (b != (byte)'\n' && b != (byte)'"')
            {
                continue;
            }

            sb.Append(encoding.GetString(bytes, start, i - start));
            sb.Append((char)b);
            start = i + 1;
        }

        sb.Append(encoding.GetString(bytes, start, length - start));

        return sb.ToString();
    }

    /// <summary>
    /// A NUL byte decodes to U+0000, which PostgreSQL refuses in a text or jsonb column: the store's INSERT
    /// would throw 22021 every cycle the byte sat in the window — the same blindness this route exists to
    /// end, moved to the store and blamed on it. The text route never passed one through (pg_read_file
    /// throws 22021 on it first), so NUL is the one character this route must not hand on; it becomes
    /// U+FFFD like any other byte that is not valid text.
    /// </summary>
    private static string WithoutNul(string text) => text.Contains('\0') ? text.Replace('\0', '\uFFFD') : text;

    private static bool IsOctalDigit(char c) => c is >= '0' and <= '7';
}
