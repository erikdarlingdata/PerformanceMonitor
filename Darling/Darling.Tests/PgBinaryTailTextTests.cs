/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgBinaryTailText"/> turns the binary route's bytes back into text leniently (#4046 part 1c).
/// <see cref="DecodeWhole"/>-suffixed tests cover the whole-body route (<c>pg_log_events</c>);
/// <see cref="UnescapeAndDecode"/>-suffixed ones cover the <c>encode(bytea, 'escape')</c> route
/// (<c>pg_deadlocks</c>, <c>pg_plan_capture</c>).
/// </summary>
public sealed class PgBinaryTailTextTests
{
    [Fact]
    public void UnescapeAndDecode_DoubledBackslash_BecomesOneBackslash()
    {
        Assert.Equal("\\", PgBinaryTailText.UnescapeAndDecode("\\\\", Encoding.UTF8));
    }

    /// <summary>
    /// The one ambiguity that matters: a doubled backslash followed by plain digits must NOT be misread as
    /// the doubled slash swallowing part of an octal escape. <c>\\101</c> (backslash, backslash, '1', '0',
    /// '1') is a literal backslash byte followed by the literal text "101" — NOT octal 101 (decimal 65,
    /// 'A'). Getting this wrong would corrupt every literal backslash immediately followed by digits, which
    /// a SQL statement or a Windows path in a captured query routinely is.
    /// </summary>
    [Fact]
    public void UnescapeAndDecode_DoubledBackslashFollowedByDigits_IsNotMisreadAsOctal()
    {
        Assert.Equal("\\101", PgBinaryTailText.UnescapeAndDecode("\\\\101", Encoding.UTF8));
        Assert.NotEqual("A", PgBinaryTailText.UnescapeAndDecode("\\\\101", Encoding.UTF8));
    }

    [Fact]
    public void UnescapeAndDecode_OctalEscapedTwoByteSequence_DecodesToEAcute()
    {
        /* \303\251 is the octal escape() form of the UTF-8 bytes 0xC3 0xA9 - e-acute (U+00E9). */
        Assert.Equal("\u00e9", PgBinaryTailText.UnescapeAndDecode("\\303\\251", Encoding.UTF8));
    }

    [Fact]
    public void UnescapeAndDecode_OctalEscapedInvalidByte_BecomesReplacementCharacter()
    {
        /* \377 is octal 0xFF, never a valid UTF-8 lead or continuation byte on its own. */
        Assert.Equal("\uFFFD", PgBinaryTailText.UnescapeAndDecode("\\377", Encoding.UTF8));
    }

    /// <summary>
    /// A NUL must not come out as U+0000: PostgreSQL refuses it in a text or jsonb column, so the store's
    /// INSERT would throw 22021 every cycle — the byte route's blindness moved to the store.
    /// </summary>
    [Fact]
    public void UnescapeAndDecode_OctalEscapedNul_BecomesReplacementCharacter()
    {
        Assert.Equal("a\uFFFDb", PgBinaryTailText.UnescapeAndDecode("a\\000b", Encoding.UTF8));
    }

    [Fact]
    public void DecodeWhole_NulByte_BecomesReplacementCharacter()
    {
        var bytes = new byte[] { (byte)'a', 0x00, (byte)'b' };

        Assert.Equal("a\uFFFDb", PgBinaryTailText.DecodeWhole(bytes, Encoding.UTF8));
    }

    /// <summary>
    /// #4046: escape() leaves control bytes 0x01-0x1F and 0x7F literal, not octal-escaped - verified live on
    /// the rig (see the type's own remarks). A tab and a newline are the two that appear constantly in a
    /// captured deadlock report's DETAIL block.
    /// </summary>
    [Fact]
    public void UnescapeAndDecode_LiteralTabAndNewline_PassThroughUnchanged()
    {
        Assert.Equal("a\tb\nc", PgBinaryTailText.UnescapeAndDecode("a\tb\nc", Encoding.UTF8));
    }

    /// <summary>
    /// A realistic captured line mixing every form at once: a literal tab, an octal-escaped 2-byte sequence,
    /// and a doubled backslash immediately followed by digits.
    /// </summary>
    [Fact]
    public void UnescapeAndDecode_RealisticMixedLine_HandlesEveryFormTogether()
    {
        var escaped = "Process 1549:\tBEGIN; SELECT '\\303\\251', 'C:\\\\101';\n";
        var expected = "Process 1549:\tBEGIN; SELECT '\u00e9', 'C:\\101';\n";

        Assert.Equal(expected, PgBinaryTailText.UnescapeAndDecode(escaped, Encoding.UTF8));
    }

    [Fact]
    public void DecodeWhole_InvalidByte_BecomesReplacementCharacterWithSurroundingTextIntact()
    {
        var bytes = new byte[] { (byte)'a', (byte)'b', (byte)'c', 0xFF, (byte)'d', (byte)'e', (byte)'f' };

        Assert.Equal("abc\uFFFDdef", PgBinaryTailText.DecodeWhole(bytes, Encoding.UTF8));
    }

    [Fact]
    public void DecodeWhole_ValidMultiByteSequences_RoundTrip()
    {
        /* e-acute (2-byte), euro sign (3-byte), grinning face emoji (4-byte). */
        const string text = "caf\u00e9 costs \u20ac5 \U0001F600";
        var bytes = Encoding.UTF8.GetBytes(text);

        Assert.Equal(text, PgBinaryTailText.DecodeWhole(bytes, Encoding.UTF8));
    }

    /// <summary>#4062: decoding in the database's own server_encoding, not hard-coded UTF-8. WIN1252 byte
    /// 0x81 (unassigned by the published standard, but mapped by .NET's code page to U+0081 — see
    /// <c>PgServerEncodingTests.Win1252UnassignedByte_DecodesWithoutThrowing</c>) never throws, so the byte
    /// route never loses the whole read over it, whatever exact character it becomes.</summary>
    [Fact]
    public void DecodeWhole_Win1252UnassignedByte_DecodesWithoutThrowing()
    {
        Assert.True(PgServerEncoding.TryGet("WIN1252", out var win1252));
        var bytes = new byte[] { (byte)'a', 0x81, (byte)'b' };

        Assert.Equal(3, PgBinaryTailText.DecodeWhole(bytes, win1252).Length);
    }

    /// <summary>#4062: a WIN1252 byte that IS assigned (0xE9 = e-acute) decodes to that character, not
    /// U+FFFD — the whole reason the database's own encoding must be used instead of UTF-8.</summary>
    [Fact]
    public void DecodeWhole_Win1252AssignedByte_DecodesCorrectly()
    {
        Assert.True(PgServerEncoding.TryGet("WIN1252", out var win1252));
        var bytes = new byte[] { (byte)'a', 0xE9, (byte)'b' };

        Assert.Equal("a\u00e9b", PgBinaryTailText.DecodeWhole(bytes, win1252));
    }

    /// <summary>#4062: an EUC_JP two-byte character round-trips through the whole-body route.</summary>
    [Fact]
    public void DecodeWhole_EucJpTwoByteCharacter_RoundTrips()
    {
        Assert.True(PgServerEncoding.TryGet("EUC_JP", out var eucJp));
        const string text = "\u3042"; // Hiragana A
        var bytes = eucJp.GetBytes(text);

        Assert.Equal(text, PgBinaryTailText.DecodeWhole(bytes, eucJp));
    }

    /// <summary>#4062: the escape()-reversal route decoded with WIN1252 instead of UTF-8 — an octal-escaped
    /// 0xE9 byte (assigned in WIN1252) must decode to e-acute, not the two-byte UTF-8 sequence's answer.</summary>
    [Fact]
    public void UnescapeAndDecode_Win1252OctalEscapedByte_DecodesCorrectly()
    {
        Assert.True(PgServerEncoding.TryGet("WIN1252", out var win1252));

        Assert.Equal("a\u00e9b", PgBinaryTailText.UnescapeAndDecode("a\\351b", win1252));
    }

    /// <summary>#4062: the escape()-reversal route decoded with EUC_JP — an octal-escaped two-byte EUC_JP
    /// character round-trips.</summary>
    [Fact]
    public void UnescapeAndDecode_EucJpOctalEscapedTwoByteSequence_RoundTrips()
    {
        Assert.True(PgServerEncoding.TryGet("EUC_JP", out var eucJp));
        const string text = "\u3042"; // Hiragana A
        var eucBytes = eucJp.GetBytes(text);
        var escaped = string.Concat(eucBytes.Select(b => $"\\{Convert.ToString(b, 8).PadLeft(3, '0')}"));

        Assert.Equal(text, PgBinaryTailText.UnescapeAndDecode(escaped, eucJp));
    }

    /// <summary>
    /// Security review round 1, Medium: a planted DBCS lead byte (0xA4 is unmapped on its own in EUC_JP,
    /// EUC_CN and EUC_KR) must not eat the newline that follows it. Decoding the whole tail in one call
    /// would pair 0xA4 with the 0x0A byte after it and lose the line break; splitting on 0x0A first and
    /// decoding each segment keeps the newline and the next line's text intact.
    /// </summary>
    [Theory]
    [InlineData("EUC_JP")]
    [InlineData("EUC_CN")]
    [InlineData("EUC_KR")]
    public void DecodeWhole_PlantedEucLeadByteBeforeNewline_DoesNotSwallowTheLineBreak(string serverEncoding)
    {
        Assert.True(PgServerEncoding.TryGet(serverEncoding, out var encoding));
        var bytes = new byte[] { 0xA4, (byte)'\n', (byte)'x' };

        var decoded = PgBinaryTailText.DecodeWhole(bytes, encoding);

        Assert.Contains('\n', decoded);
        var lines = decoded.Split('\n');
        Assert.Equal(2, lines.Length);
        Assert.Equal("x", lines[1]);
    }

    /// <summary>Same planted-byte-before-newline case, through the escape()-reversal route: the byte
    /// arrives as an octal escape (<c>\244</c> is octal for 0xA4), and the newline arrives as a literal
    /// byte (escape() never octal-escapes 0x0A).</summary>
    [Theory]
    [InlineData("EUC_JP")]
    [InlineData("EUC_CN")]
    [InlineData("EUC_KR")]
    public void UnescapeAndDecode_PlantedEucLeadByteBeforeNewline_DoesNotSwallowTheLineBreak(string serverEncoding)
    {
        Assert.True(PgServerEncoding.TryGet(serverEncoding, out var encoding));

        var decoded = PgBinaryTailText.UnescapeAndDecode("\\244\nx", encoding);

        Assert.Contains('\n', decoded);
        var lines = decoded.Split('\n');
        Assert.Equal(2, lines.Length);
        Assert.Equal("x", lines[1]);
    }
}
