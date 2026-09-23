/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

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
        Assert.Equal("\\", PgBinaryTailText.UnescapeAndDecode("\\\\"));
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
        Assert.Equal("\\101", PgBinaryTailText.UnescapeAndDecode("\\\\101"));
        Assert.NotEqual("A", PgBinaryTailText.UnescapeAndDecode("\\\\101"));
    }

    [Fact]
    public void UnescapeAndDecode_OctalEscapedTwoByteSequence_DecodesToEAcute()
    {
        /* \303\251 is the octal escape() form of the UTF-8 bytes 0xC3 0xA9 - e-acute (U+00E9). */
        Assert.Equal("\u00e9", PgBinaryTailText.UnescapeAndDecode("\\303\\251"));
    }

    [Fact]
    public void UnescapeAndDecode_OctalEscapedInvalidByte_BecomesReplacementCharacter()
    {
        /* \377 is octal 0xFF, never a valid UTF-8 lead or continuation byte on its own. */
        Assert.Equal("\uFFFD", PgBinaryTailText.UnescapeAndDecode("\\377"));
    }

    /// <summary>
    /// A NUL must not come out as U+0000: PostgreSQL refuses it in a text or jsonb column, so the store's
    /// INSERT would throw 22021 every cycle — the byte route's blindness moved to the store.
    /// </summary>
    [Fact]
    public void UnescapeAndDecode_OctalEscapedNul_BecomesReplacementCharacter()
    {
        Assert.Equal("a�b", PgBinaryTailText.UnescapeAndDecode("a\\000b"));
    }

    [Fact]
    public void DecodeWhole_NulByte_BecomesReplacementCharacter()
    {
        var bytes = new byte[] { (byte)'a', 0x00, (byte)'b' };

        Assert.Equal("a�b", PgBinaryTailText.DecodeWhole(bytes));
    }

    /// <summary>
    /// #4046: escape() leaves control bytes 0x01-0x1F and 0x7F literal, not octal-escaped - verified live on
    /// the rig (see the type's own remarks). A tab and a newline are the two that appear constantly in a
    /// captured deadlock report's DETAIL block.
    /// </summary>
    [Fact]
    public void UnescapeAndDecode_LiteralTabAndNewline_PassThroughUnchanged()
    {
        Assert.Equal("a\tb\nc", PgBinaryTailText.UnescapeAndDecode("a\tb\nc"));
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

        Assert.Equal(expected, PgBinaryTailText.UnescapeAndDecode(escaped));
    }

    [Fact]
    public void DecodeWhole_InvalidByte_BecomesReplacementCharacterWithSurroundingTextIntact()
    {
        var bytes = new byte[] { (byte)'a', (byte)'b', (byte)'c', 0xFF, (byte)'d', (byte)'e', (byte)'f' };

        Assert.Equal("abc\uFFFDdef", PgBinaryTailText.DecodeWhole(bytes));
    }

    [Fact]
    public void DecodeWhole_ValidMultiByteSequences_RoundTrip()
    {
        /* e-acute (2-byte), euro sign (3-byte), grinning face emoji (4-byte). */
        const string text = "caf\u00e9 costs \u20ac5 \U0001F600";
        var bytes = Encoding.UTF8.GetBytes(text);

        Assert.Equal(text, PgBinaryTailText.DecodeWhole(bytes));
    }
}
