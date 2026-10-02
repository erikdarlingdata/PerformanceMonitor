/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgServerEncoding"/> (#4062): the exact map from PostgreSQL SERVER encoding name to .NET
/// code page the binary log route decodes with.
/// </summary>
public sealed class PgServerEncodingTests
{
    [Theory]
    [InlineData("UTF8")]
    [InlineData("SQL_ASCII")]
    [InlineData("LATIN1")]
    [InlineData("LATIN2")]
    [InlineData("LATIN3")]
    [InlineData("LATIN4")]
    [InlineData("LATIN5")]
    [InlineData("LATIN7")]
    [InlineData("LATIN9")]
    [InlineData("ISO_8859_5")]
    [InlineData("ISO_8859_6")]
    [InlineData("ISO_8859_7")]
    [InlineData("ISO_8859_8")]
    [InlineData("WIN866")]
    [InlineData("WIN874")]
    [InlineData("WIN1250")]
    [InlineData("WIN1251")]
    [InlineData("WIN1252")]
    [InlineData("WIN1253")]
    [InlineData("WIN1254")]
    [InlineData("WIN1255")]
    [InlineData("WIN1256")]
    [InlineData("WIN1257")]
    [InlineData("WIN1258")]
    [InlineData("KOI8R")]
    [InlineData("KOI8U")]
    [InlineData("EUC_JP")]
    [InlineData("EUC_CN")]
    [InlineData("EUC_KR")]
    public void EveryMappedNameResolves(string serverEncoding)
    {
        Assert.True(PgServerEncoding.TryGet(serverEncoding, out var encoding));
        Assert.NotNull(encoding);
    }

    [Theory]
    [InlineData("EUC_TW")]
    [InlineData("EUC_JIS_2004")]
    [InlineData("LATIN6")]
    [InlineData("LATIN8")]
    [InlineData("LATIN10")]
    [InlineData("MULE_INTERNAL")]
    [InlineData("not-a-real-encoding")]
    [InlineData(null)]
    public void AnUnmappedNameReturnsFalse(string? serverEncoding)
    {
        Assert.False(PgServerEncoding.TryGet(serverEncoding, out _));
    }

    /// <summary>
    /// WIN1252 byte 0x81 never throws — checked here rather than assumed. .NET's Windows-1252 code page maps
    /// every byte to SOME Unicode code point (0x81 to U+0081, a C1 control byte reserved rather than assigned
    /// by the published standard), so the <see cref="DecoderReplacementFallback"/> fallback is never actually
    /// invoked for this byte — there is no byte value in a single-byte .NET code page that lacks a mapping.
    /// The no-throw guarantee is what matters for #4062 (a captured log line must never fail the whole read
    /// over one byte); which exact code point an unassigned byte round-trips to does not.
    /// </summary>
    [Fact]
    public void Win1252UnassignedByte_DecodesWithoutThrowing()
    {
        Assert.True(PgServerEncoding.TryGet("WIN1252", out var win1252));

        var bytes = new byte[] { 0x81 };
        var decoded = win1252.GetString(bytes);

        Assert.Equal(1, decoded.Length);
    }

    /// <summary>LATIN1 0xE9 is e-acute.</summary>
    [Fact]
    public void Latin1ByteE9_DecodesToEAcute()
    {
        Assert.True(PgServerEncoding.TryGet("LATIN1", out var latin1));

        var bytes = new byte[] { 0xE9 };
        var decoded = latin1.GetString(bytes);

        Assert.Equal("\u00e9", decoded);
    }

    /// <summary>An EUC_JP two-byte character round-trips through encode/decode.</summary>
    [Fact]
    public void EucJpTwoByteCharacter_RoundTrips()
    {
        Assert.True(PgServerEncoding.TryGet("EUC_JP", out var eucJp));

        const string text = "\u3042"; // Hiragana A
        var bytes = eucJp.GetBytes(text);
        var decoded = eucJp.GetString(bytes);

        Assert.Equal(text, decoded);
    }
}
