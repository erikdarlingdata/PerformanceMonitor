/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// An explicit map of PostgreSQL SERVER encoding name (<c>current_setting('server_encoding')</c>) to a
/// .NET encoding, for the <c>pg_read_binary_file</c> route (#4062): the binary route must decode the
/// bytes it reads in the connected database's own encoding, not hard-coded UTF-8, or a database whose
/// encoding is not UTF8 gets its own non-ASCII text turned into U+FFFD.
///
/// <para><b>Only exact charsets are mapped.</b> Each entry here is PostgreSQL's own encoding, byte for
/// byte, as a .NET code page — not an approximation. An encoding this type does not map (EUC_TW,
/// EUC_JIS_2004, LATIN6, LATIN8, LATIN10, MULE_INTERNAL, and anything that fails to resolve on this
/// runtime) stays on the existing text route, gated exactly as before <see cref="TryGet"/> existed.
/// There is deliberately no UTF-8 fallback for an unmapped encoding: that would turn the database's own
/// text into U+FFFD just as surely as the bug this type fixes, only silently.</para>
///
/// <para><b>Replacement, never a throw.</b> Every <see cref="Encoding"/> this type hands out is built with
/// <see cref="EncoderFallback.ReplacementFallback"/> and <see cref="DecoderFallback.ReplacementFallback"/>
/// (<see cref="Encoding.GetEncoding(int, EncoderFallback, DecoderFallback)"/>), so a byte the encoding
/// cannot decode becomes U+FFFD instead of throwing — the same lenient behavior
/// <see cref="PgBinaryTailText"/> already relies on for <see cref="Encoding.UTF8"/>.</para>
/// </summary>
public static class PgServerEncoding
{
    private static readonly Dictionary<string, int> s_codePages = new(StringComparer.Ordinal)
    {
        ["LATIN1"] = 28591,
        ["LATIN2"] = 28592,
        ["LATIN3"] = 28593,
        ["LATIN4"] = 28594,
        ["LATIN5"] = 28599,
        ["LATIN7"] = 28603,
        ["LATIN9"] = 28605,
        ["ISO_8859_5"] = 28595,
        ["ISO_8859_6"] = 28596,
        ["ISO_8859_7"] = 28597,
        ["ISO_8859_8"] = 28598,
        ["WIN866"] = 866,
        ["WIN874"] = 874,
        ["WIN1250"] = 1250,
        ["WIN1251"] = 1251,
        ["WIN1252"] = 1252,
        ["WIN1253"] = 1253,
        ["WIN1254"] = 1254,
        ["WIN1255"] = 1255,
        ["WIN1256"] = 1256,
        ["WIN1257"] = 1257,
        ["WIN1258"] = 1258,
        ["KOI8R"] = 20866,
        ["KOI8U"] = 21866,
        ["EUC_JP"] = 51932,
        ["EUC_CN"] = 936,
        ["EUC_KR"] = 51949,
    };

    private static readonly Dictionary<string, Encoding> s_encodings = new(StringComparer.Ordinal);

    /// <summary>
    /// Builds every mapped encoding once, up front, so a later <see cref="TryGet"/> is a dictionary lookup
    /// and any code page this .NET runtime cannot resolve is dropped here rather than discovered per call.
    /// Registers <see cref="CodePagesEncodingProvider.Instance"/> first — most of the code pages above (every
    /// one except UTF8's 65001, which this type does not map) are only available through it.
    /// </summary>
    static PgServerEncoding()
    {
        Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

        s_encodings["UTF8"] = new UTF8Encoding(false, false);
        s_encodings["SQL_ASCII"] = new UTF8Encoding(false, false);

        foreach (var (name, codePage) in s_codePages)
        {
            try
            {
                s_encodings[name] = Encoding.GetEncoding(
                    codePage, EncoderFallback.ReplacementFallback, DecoderFallback.ReplacementFallback);
            }
            catch (NotSupportedException)
            {
                /* This runtime cannot resolve the code page: leave the name unmapped, same as an
                   encoding this type never listed. TryGet reports false and the caller stays on the
                   text route. */
            }
            catch (ArgumentException)
            {
                /* Same as above, for a code page number a target platform's ICU/NLS tables reject. */
            }
        }
    }

    /// <summary>
    /// Looks up the .NET <see cref="Encoding"/> for PostgreSQL's SERVER encoding name
    /// <paramref name="serverEncoding"/> (as <c>current_setting('server_encoding')</c> returns it —
    /// upper-case, e.g. <c>"WIN1252"</c>). True and <paramref name="encoding"/> set when this type maps
    /// the name and this runtime resolved it; false for anything else, including a null or unrecognized
    /// name — the caller's cue to stay on the existing text route.
    /// </summary>
    public static bool TryGet(string? serverEncoding, out Encoding encoding)
    {
        if (serverEncoding is not null && s_encodings.TryGetValue(serverEncoding, out var found))
        {
            encoding = found;
            return true;
        }

        encoding = Encoding.UTF8;
        return false;
    }
}
