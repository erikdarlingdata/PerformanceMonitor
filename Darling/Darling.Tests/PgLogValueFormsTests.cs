/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The row-value forms beyond the first set: a COPY value that runs onto following lines, a foreign-key value that holds
/// the form's own closing text, the messages and JSON details that carry a value under any SQLSTATE (or none), and the
/// wording of the forms with no letter in their head in every major version from 13 on.
/// </summary>
public sealed class PgLogValueFormsTests
{
    private const string W = PgLogTextRedactor.WithheldValues;

    private static string Fill(string format, params string[] args)
    {
        var next = 0;
        return Regex.Replace(format, @"%(?:(?<position>[0-9])\$)?(?:llu|d|s)", m =>
            args[(m.Groups["position"].Success ? m.Groups["position"].Value[0] - '0' : ++next) - 1]);
    }

    /// <summary>A CSV value holding a newline runs onto the next physical line of the CONTEXT: the first line ends in a
    /// quote and reads as a whole frame, and what follows it that is not a frame is the rest of the value.</summary>
    [Fact]
    public void ACopyValueRunningOntoFollowingLines_IsWithheldFromWhereItContinues()
    {
        var column = PgLogTextRedactor.RedactContext("COPY t, line 1, column note: \"He said \"hi\"\nSSN 123-45-6789\"");
        Assert.Equal("COPY t, line 1, column note: \"?\"\n" + W, column);
        Assert.Equal(column, PgLogTextRedactor.RedactContext(column));

        var line = PgLogTextRedactor.RedactContext("COPY t, line 1: \"a,\"\"b\"\"\nSSN 123-45-6789,x\"");
        Assert.DoesNotContain("SSN", line, StringComparison.Ordinal);
        Assert.StartsWith("COPY t, line 1: \"?\"\n", line, StringComparison.Ordinal);

        Assert.Equal(
            "COPY t, line 1, column a: \"?\"\n\tPL/pgSQL function f() line 3 at SQL statement",
            PgLogTextRedactor.RedactContext("COPY t, line 1, column a: \"x\"\n\tPL/pgSQL function f() line 3 at SQL statement"));
        Assert.Equal("COPY t, line 1\nsome other line", PgLogTextRedactor.RedactContext("COPY t, line 1\nsome other line"));
    }

    [Fact]
    public void AForeignKeyValueHoldingTheFormsClosingText_LeavesNothingInTheTableName()
    {
        Assert.Equal(
            "Key (c)=(?) is not present in table \"orders\".",
            PgLogTextRedactor.RedactDetail("Key (c)=(x) is not present in table \"SECRET) is not present in table \"orders\"."));
        Assert.Equal(
            "Key (c)=(?) is still referenced from table \"orders\".",
            PgLogTextRedactor.RedactDetail("Key (c)=(x) is still referenced from table \"SECRET) is still referenced from table \"orders\"."));
        Assert.Equal(
            "value \"?\" is out of range for type integer",
            PgLogTextRedactor.RedactMessage("value \"x\" is out of range for type integer\" is out of range for type integer"));
    }

    /// <summary>Messages that carry a value are masked by their text, with the SQLSTATE a <c>%e</c> prefix gives, the
    /// verbose one ahead of the message, or none (PostgreSQL's and RDS's default stderr prefix).</summary>
    [Theory]
    [InlineData("malformed array literal: \"{Kept-S1\"", "malformed array literal: \"?\"", "22P02")]
    [InlineData("malformed record literal: \"(Kept-S1\"", "malformed record literal: \"?\"", "22P02")]
    [InlineData("malformed range literal: \"[Kept-S1\"", "malformed range literal: \"?\"", "22P02")]
    [InlineData("malformed multirange literal: \"{Kept-S1\"", "malformed multirange literal: \"?\"", "22P02")]
    [InlineData("invalid input value for enum mood: \"Kept-S1\"", "invalid input value for enum mood: \"?\"", "22P02")]
    [InlineData("value \"Kept-S1\" is out of range for type integer", "value \"?\" is out of range for type integer", "22003")]
    [InlineData("\"Kept-S1\" is out of range for type double precision", "\"?\" is out of range for type double precision", "22003")]
    [InlineData("date/time field value out of range: \"Kept-S1\"", "date/time field value out of range: \"?\"", "22008")]
    [InlineData("interval field value out of range: \"Kept-S1\"", "interval field value out of range: \"?\"", "22008")]
    [InlineData("time zone displacement out of range: \"Kept-S1\"", "time zone displacement out of range: \"?\"", "22009")]
    [InlineData("date out of range: \"Kept-S1\"", "date out of range: \"?\"", "22008")]
    [InlineData("timestamp out of range: \"Kept-S1\"", "timestamp out of range: \"?\"", "22008")]
    public void AMessageThatCarriesAValue_IsMaskedWithAndWithoutASqlState(string message, string expected, string sqlState)
    {
        Assert.Equal(expected, PgLogTextRedactor.RedactMessage(message));
        Assert.Equal(expected, PgLogTextRedactor.RedactMessage(message, sqlState: sqlState));
        Assert.Equal(sqlState + ": " + expected, PgLogTextRedactor.RedactMessage(sqlState + ": " + message));
        Assert.Equal(expected + " at character 8", PgLogTextRedactor.RedactMessage(message + " at character 8"));
        Assert.Equal(expected, PgLogTextRedactor.RedactMessage(expected));
    }

    [Theory]
    [InlineData("Token \"Kept-S1\" is invalid.", "Token \"?\" is invalid.")]
    [InlineData("Expected end of input, but found \"Kept-S1\".", "Expected end of input, but found \"?\".")]
    [InlineData("Expected JSON value, but found \"Kept-S1\".", "Expected JSON value, but found \"?\".")]
    [InlineData("Expected \",\" or \"}\", but found \"Kept-S1\".", "Expected \",\" or \"}\", but found \"?\".")]
    [InlineData("Escape sequence \"\\Kept-S1\" is invalid.", "Escape sequence \"\\?\" is invalid.")]
    public void AJsonParserDetailThatQuotesAToken_IsMaskedWithAndWithoutASqlState(string detail, string expected)
    {
        Assert.Equal(expected, PgLogTextRedactor.RedactDetail(detail));
        Assert.Equal(expected, PgLogTextRedactor.RedactDetail(detail, sqlState: "22P02"));
        Assert.Equal(expected, PgLogTextRedactor.RedactDetail(expected));
    }

    /// <summary>The forms whose head has no letter (a catalogue that writes a slot first) are read in the wording of every
    /// major version from 13 on; text holding such a form's literal in a wording this build does not list is withheld.</summary>
    [Theory]
    [InlineData("13,14", "%sのCOPY、行 %s: \"%s\"", 'c')]
    [InlineData("15,16,17,18", "%sのCOPY、行 %llu: \"%s\"", 'c')]
    [InlineData("13,14", "%sのCOPY、行 %s、列 %s: \"%s\"", 'k')]
    [InlineData("15,16,17,18", "%sのCOPY、行 %llu、列 %s: \"%s\"", 'k')]
    [InlineData("13,14", "%s 복사, %s번째 줄: \"%s\"", 'c')]
    [InlineData("15", "%s 복사, %llu번째 줄: \"%s\"", 'c')]
    [InlineData("13,14", "%s 복사, %s번째 줄, %s 열: \"%s\"", 'k')]
    [InlineData("15", "%s 복사, %llu번째 줄, %s 열: \"%s\"", 'k')]
    [InlineData("14", "%s型の入力構文が不正です: \"%s\"", 'm')]
    [InlineData("13,15,16,17,18", "\"%s\"型の入力構文が不正です: \"%s\"", 'm')]
    [InlineData("13,14,15,16,17,18", "%s 자료형 대한 잘못된 입력: \"%s\"", 'm')]
    [InlineData("13,16,17,18", "%s tipi için geçersiz giriş sözdizimi: \"%s\"", 'm')]
    [InlineData("13,14,15,16,17,18", "(%s)=(%s) 키가 \"%s\" 테이블에 없습니다.", 'd')]
    [InlineData("13,16,17,18", "\"%3$s\" tablosunda (%1$s)=(%2$s) anahtarı mevcut değildir.", 'd')]
    public void AFormWithNoLetterInItsHead_IsReadInEveryMajorVersionsWording(string versions, string format, char kind)
    {
        Assert.NotEmpty(versions);
        var (filled, expected) = kind switch
        {
            'c' => (Fill(format, "t", "3", "Kept-S1"), Fill(format, "t", "3", "?")),
            'k' => (Fill(format, "t", "3", "ssn", "Kept-S1"), Fill(format, "t", "3", "ssn", "?")),
            'm' => (Fill(format, "integer", "Kept-S1"), Fill(format, "integer", "?")),
            _ => (Fill(format, "id", "Kept-S1", "orders"), Fill(format, "id", "?", "orders")),
        };
        Func<string, string?> redact = kind switch
        {
            'c' or 'k' => c => PgLogTextRedactor.RedactContext(c),
            'm' => m => PgLogTextRedactor.RedactMessage(m),
            _ => d => PgLogTextRedactor.RedactDetail(d),
        };
        Assert.Equal(expected, redact(filled));
        Assert.Equal(expected, redact(expected));

        var literal = Regex.Split(format, @"%(?:[0-9]\$)?(?:llu|d|s)").OrderByDescending(p => p.Length).First();
        Assert.DoesNotContain("Kept-S1", redact("prefix " + literal + "Kept-S1 extra") ?? string.Empty, StringComparison.Ordinal);
    }
}
