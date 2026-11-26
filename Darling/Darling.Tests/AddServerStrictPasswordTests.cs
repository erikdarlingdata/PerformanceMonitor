/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9753: the single-server flag form of <c>--add-server</c> reads a piped or redirected password with the strict reader
/// <c>--set-login-password</c> uses (#9689), with the same refusals. Each test drives the flag form's own reader and builder
/// (<see cref="DarlingCliCommands.BuildAddServerJsonFromStdin"/>) with stdin as bytes and the input marked redirected, so no
/// test depends on whether the test process has a terminal.
/// </summary>
public sealed class AddServerStrictPasswordTests
{
    private static readonly DarlingCliCommands.AddServerFlags SqlLogin =
        new(Host: "sql01", Auth: "sql", Username: "darling_monitor");

    private static (string? Json, string? Problem) Build(byte[] stdin) =>
        DarlingCliCommands.BuildAddServerJsonFromStdin(SqlLogin, new MemoryStream(stdin), TextWriter.Null, inputRedirected: true);

    private static (string? Json, string? Problem) Build(string stdin) => Build(Encoding.UTF8.GetBytes(stdin));

    private static string SavedPassword(string? json)
    {
        Assert.NotNull(json);
        using var document = JsonDocument.Parse(json);
        return document.RootElement[0].GetProperty("password").GetString()!;
    }

    [Theory]
    [InlineData("pw\n", "pw")]
    [InlineData("pw\r\n", "pw")]
    [InlineData("pw", "pw")]
    [InlineData("  pw  \n", "  pw  ")]
    [InlineData("pw\n\n\n", "pw")]
    public void APipedPassword_IsSavedAsSent_WithOneTrailingNewlineStripped(string stdin, string expected)
    {
        var (json, problem) = Build(stdin);

        Assert.Null(problem);
        Assert.Equal(expected, SavedPassword(json));
    }

    [Fact]
    public void ANonAsciiPassword_IsSavedAsTheUtf8CharactersSent_NotAsAnOemCodePageReadsThem()
    {
        const string Typed = "pässwörd☃\U0001F600é";

        var (json, problem) = Build(Typed + "\n");

        Assert.Null(problem);
        Assert.Equal(Typed, SavedPassword(json));
    }

    [Fact]
    public void ABinaryOfTheSameNonAsciiPassword_WithoutANewline_IsSavedTheSame()
    {
        const string Typed = "naïve-пароль-密码";

        var (json, problem) = Build(Encoding.UTF8.GetBytes(Typed));

        Assert.Null(problem);
        Assert.Equal(Typed, SavedPassword(json));
    }

    [Fact]
    public void AByteOrderMark_IsRefused_NotSavedAsTheFirstCharacterOfThePassword()
    {
        var (json, problem) = Build([0xEF, 0xBB, 0xBF, (byte)'p', (byte)'w', (byte)'\n']);

        Assert.Null(json);
        Assert.Equal(DarlingCliCommands.LoginPasswordHasByteOrderMarkText, problem);
    }

    [Fact]
    public void ANulByte_IsRefused_AsUtf16FromAWindowsPipeProduces()
    {
        var (json, problem) = Build(Encoding.Unicode.GetBytes("pw\r\n"));

        Assert.Null(json);
        Assert.Equal(DarlingCliCommands.LoginPasswordHasNulText, problem);
    }

    [Theory]
    [InlineData("secret\rmore")]
    [InlineData("secret\r")]
    public void ALoneCarriageReturn_IsRefused_NotTakenAsTheEndOfThePassword(string stdin)
    {
        var (json, problem) = Build(stdin);

        Assert.Null(json);
        Assert.Equal(DarlingCliCommands.LoginPasswordHasStrayReturnText, problem);
    }

    [Theory]
    [InlineData("first-secret\nsecond-secret\n")]
    [InlineData("first-secret\r\nsecond-secret\r\n")]
    [InlineData("first-secret\n\nsecond-secret")]
    public void ASecondNonEmptyLine_IsRefused_NotDroppedInSilence_AndNoLineIsRepeated(string stdin)
    {
        var (json, problem) = Build(stdin);

        Assert.Null(json);
        Assert.Equal(DarlingCliCommands.LoginPasswordHasManyLinesText, problem);
        Assert.DoesNotContain("secret", problem, StringComparison.Ordinal);
    }

    [Fact]
    public void BytesThatAreNotUtf8_AreRefused_NotSavedAsReplacementCharacters()
    {
        var (json, problem) = Build([(byte)'p', 0xFF, 0xFE, (byte)'w']);

        Assert.Null(json);
        Assert.Equal(DarlingMcpServerAdminTools.PasswordCharactersText + " Nothing was changed.", problem);
    }

    [Fact]
    public void AStreamLongerThanAPasswordAndItsLineEnding_IsRefusedAsTooLong_AndIsNotReadToItsEnd()
    {
        var longest = new string('p', PasswordSeal.MaxPlaintextBytes);
        Assert.Equal(longest, SavedPassword(Build(longest + "\r\n").Json));

        var (json, problem) = Build(longest + "\r\n\n");
        Assert.Null(json);
        Assert.Equal(DarlingMcpServerAdminTools.PasswordTooLongText + " Nothing was changed.", problem);

        var endless = new EndlessStream();
        (json, problem) = DarlingCliCommands.BuildAddServerJsonFromStdin(SqlLogin, endless, TextWriter.Null, inputRedirected: true);
        Assert.Null(json);
        Assert.Equal(DarlingMcpServerAdminTools.PasswordTooLongText + " Nothing was changed.", problem);
        Assert.True(endless.BytesRead <= PasswordSeal.MaxPlaintextBytes + 3);
    }

    [Theory]
    [InlineData("")]
    [InlineData("\n")]
    [InlineData("\r\n\r\n")]
    public void EmptyStdin_GivesNoPassword_AndNoServerIsAdded(string stdin)
    {
        var (json, problem) = Build(stdin);

        Assert.Null(json);
        Assert.Equal("No password was entered, so no server was added.", problem);
    }

    [Fact]
    public void ALoginWithNoPassword_NeverReadsStdin()
    {
        var windows = new DarlingCliCommands.AddServerFlags(Host: "sql01", Auth: "windows");

        var (json, problem) = DarlingCliCommands.BuildAddServerJsonFromStdin(windows, new ThrowingStream(), TextWriter.Null, inputRedirected: true);

        Assert.Null(problem);
        Assert.NotNull(json);
    }

    [Fact]
    public void TheReadPasswordSeam_StillStandsInForTheReader()
    {
        var (json, problem) = DarlingCliCommands.BuildAddServerJsonFromStdin(
            SqlLogin, new ThrowingStream(), TextWriter.Null, inputRedirected: true, readPassword: _ => "from-the-seam");

        Assert.Null(problem);
        Assert.Equal("from-the-seam", SavedPassword(json));
    }

    /// <summary>A stream that never ends and counts what was read from it.</summary>
    private sealed class EndlessStream : Stream
    {
        public int BytesRead { get; private set; }

        public override int Read(byte[] buffer, int offset, int count)
        {
            Array.Fill(buffer, (byte)'p', offset, count);
            BytesRead += count;
            return count;
        }

        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => BytesRead; set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }

    /// <summary>A stream that fails the test when it is read.</summary>
    private sealed class ThrowingStream : Stream
    {
        public override int Read(byte[] buffer, int offset, int count) => throw new InvalidOperationException("stdin must not be read");
        public override bool CanRead => true;
        public override bool CanSeek => false;
        public override bool CanWrite => false;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }
        public override void Flush() { }
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        public override void Write(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    }
}
