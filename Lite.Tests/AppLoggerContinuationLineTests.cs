/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9768: the line <see cref="AppLogger"/> writes is cleaned with the rule Darling's file log has used since #4281 and #4286
/// (<see cref="LogLineText.Clean"/>), so a message holding a line feed writes its continuation lines indented rather than in
/// column 0, and a reader finds each entry's first line by the timestamp in column 0. Every test reads what the logger buffered
/// (<see cref="AppLogger.DrainBufferedLines"/>) or wrote (<see cref="AppLogger.FlushTo"/>), the two places a reader sees.
/// </summary>
[Collection("app-logger-statics")]
public sealed class AppLoggerContinuationLineTests : IDisposable
{
    private const string Indent = LogLineText.ContinuationIndent;

    private static readonly Regex s_entryStart = new(@"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3} \[", RegexOptions.Compiled);

    private readonly string _logDir =
        Path.Combine(Path.GetTempPath(), "lite-applog-continuation-tests", Guid.NewGuid().ToString("N"));

    public AppLoggerContinuationLineTests() => Directory.CreateDirectory(_logDir);

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_logDir))
            {
                Directory.Delete(_logDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    /// <summary>Logs <paramref name="message"/> at Info under a fresh tag and returns the one buffered entry that carries the tag.
    /// The tag keeps another test's lines in the process-wide buffer from being mistaken for this one's.</summary>
    private static string LogOne(string tag, string message)
    {
        AppLogger.DrainBufferedLines();
        AppLogger.Info("Test", message);
        return Assert.Single(AppLogger.DrainBufferedLines(), l => l.Contains(tag, StringComparison.Ordinal));
    }

    private static string[] LinesOf(string entry) => entry.Split('\n');

    [Fact]
    public void ATwoLineMessage_IsOneEntry_WhoseSecondLineStartsWithTheIndent()
    {
        var tag = Guid.NewGuid().ToString("N");

        var lines = LinesOf(LogOne(tag, $"first {tag}\nsecond"));

        Assert.Equal(2, lines.Length);
        Assert.Matches(s_entryStart, lines[0]);
        Assert.EndsWith($"first {tag}", lines[0], StringComparison.Ordinal);
        Assert.Equal($"{Indent}second", lines[1]);
    }

    [Fact]
    public void ACrLf_IsOneLineFeed_AndLeavesNoBlankLine()
    {
        var tag = Guid.NewGuid().ToString("N");

        var entry = LogOne(tag, $"first {tag}\r\nsecond\r\nthird");

        Assert.DoesNotContain('\r', entry);
        var lines = LinesOf(entry);
        Assert.Equal(new[] { $"{Indent}second", $"{Indent}third" }, lines.Skip(1));
        Assert.All(lines, l => Assert.False(string.IsNullOrWhiteSpace(l), "a blank line is a CRLF counted as two line feeds"));
    }

    [Fact]
    public void ALoneCarriageReturn_AndTheUnicodeLineSeparators_AreWrittenAsDots()
    {
        var tag = Guid.NewGuid().ToString("N");

        var entry = LogOne(tag, $"{tag} a\rb\u2028c\u2029d\u0007e");

        /* None of them starts a line, so the entry stays one physical line. */
        Assert.Single(LinesOf(entry));
        Assert.EndsWith($"{tag} a.b.c.d.e", entry, StringComparison.Ordinal);
    }

    [Fact]
    public void ATab_IsKept()
    {
        var tag = Guid.NewGuid().ToString("N");

        var entry = LogOne(tag, $"{tag} a\tb");

        Assert.EndsWith($"{tag} a\tb", entry, StringComparison.Ordinal);
    }

    [Fact]
    public void AMessageThatRepeatsAnEntryHeader_CannotStartAForgedEntry()
    {
        var tag = Guid.NewGuid().ToString("N");

        var entry = LogOne(tag, $"{tag} bad value\n2026-01-01 00:00:00.000 [ERROR] [Forged] not a real entry");

        var lines = LinesOf(entry);
        Assert.Equal(2, lines.Length);
        Assert.Matches(s_entryStart, lines[0]);
        Assert.DoesNotMatch(s_entryStart, lines[1]);
        Assert.StartsWith(Indent, lines[1], StringComparison.Ordinal);
    }

    [Fact]
    public void AnExceptionsStackAndInnerLines_AreIndentedToo()
    {
        var tag = Guid.NewGuid().ToString("N");
        var thrown = Throw(tag);

        AppLogger.DrainBufferedLines();
        AppLogger.Error("Test", $"it failed {tag}", thrown);
        /* The two messages carry the tag, the two stacks carry this class's frames. */
        var entries = AppLogger.DrainBufferedLines()
            .Where(l => l.Contains(tag, StringComparison.Ordinal) || (l.Contains("Stack: ", StringComparison.Ordinal) && l.Contains(nameof(AppLoggerContinuationLineTests), StringComparison.Ordinal)))
            .ToList();

        /* The message line, its Stack:, then the inner exception's two lines. A stack trace holds a line feed between frames on
           every platform, and the exception messages hold one on purpose. */
        Assert.Equal(4, entries.Count);
        Assert.All(entries, e => Assert.True(LinesOf(e).Length > 1, "an entry that should span lines is one line"));
        AssertEveryLineIsAnEntryStartOrIndented(entries);
    }

    [Fact]
    public void ALineLoggedThroughTheAdapter_IsCleanedTheSameWay()
    {
        var tag = Guid.NewGuid().ToString("N");

        AppLogger.DrainBufferedLines();
        new AppLoggerAdapter<AppLoggerContinuationLineTests>().Log(
            LogLevel.Warning, new EventId(1), tag, null, (state, _) => $"{state}\r\nsecond\u2028third");
        var entry = Assert.Single(AppLogger.DrainBufferedLines(), l => l.Contains(tag, StringComparison.Ordinal));

        var lines = LinesOf(entry);
        Assert.Equal(2, lines.Length);
        Assert.Equal($"{Indent}second.third", lines[1]);
    }

    [Fact]
    public void TheFileThatIsWritten_HasOnlyEntryStartsAndIndentedContinuations()
    {
        var tag = Guid.NewGuid().ToString("N");

        AppLogger.DrainBufferedLines();
        AppLogger.Warn("Test", $"{tag} one\ntwo\r\nthree\rfour");
        AppLogger.Error("Test", $"{tag} failed", Throw(tag));
        AppLogger.FlushTo(_logDir);

        var text = File.ReadAllText(Path.Combine(_logDir, $"lite_{DateTime.Now:yyyyMMdd}.log"));
        Assert.Contains($"{tag} one", text, StringComparison.Ordinal);
        var lines = text.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None).Where(l => l.Length > 0).ToList();
        Assert.True(lines.Count > 4, "the multi-line entries were not written as several lines");
        AssertEveryLineIsAnEntryStartOrIndented(lines);
        Assert.Contains($"{Indent}two", lines);
        Assert.Contains($"{Indent}three.four", lines);
    }

    private static void AssertEveryLineIsAnEntryStartOrIndented(IEnumerable<string> entriesOrLines)
    {
        foreach (var line in entriesOrLines.SelectMany(e => e.Split('\n')).Where(l => l.Length > 0))
        {
            Assert.True(
                s_entryStart.IsMatch(line) || line.StartsWith(Indent, StringComparison.Ordinal),
                $"a line starts in column 0 with no timestamp: '{(line.Length > 60 ? line[..60] : line)}'");
        }
    }

    private static Exception Throw(string tag)
    {
        try
        {
            Outer(tag);
        }
        catch (Exception ex)
        {
            return ex;
        }

        throw new InvalidOperationException("unreachable");
    }

    private static void Outer(string tag)
    {
        try
        {
            Inner(tag);
        }
        catch (Exception inner)
        {
            throw new InvalidOperationException($"outer {tag}\nsecond server error", inner);
        }
    }

    private static void Inner(string tag) => throw new ArgumentException($"inner {tag}\nsecond line");
}
