/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#5097: the service-log section, read from a temp directory.</summary>
public sealed class DiagnosticsBundleServiceLogTests
{
    private static string Stamp(DateTime t) => t.ToString("yyyy-MM-dd HH:mm:ss.fff", System.Globalization.CultureInfo.InvariantCulture);

    [Fact]
    public void Parse_KeepsWarnErrorCrit_AttachesContinuations_AndDropsTheRest()
    {
        var t = new DateTime(2026, 1, 2, 3, 4, 5);
        var text = string.Join("\n",
            $"{Stamp(t)} [INFO ] [Cat.A] fine",
            $"{Stamp(t)} [WARN ] [Cat.B] slow thing",
            $"{Stamp(t)} [ERROR] [Cat.C] boom",
            "    at frame one",
            "    at frame two",
            $"{Stamp(t)} [CRIT ] [Cat.D] dead",
            $"{Stamp(t)} [DEBUG] [Cat.E] noise");
        var entries = DiagnosticsBundleServiceLog.Parse(text, partial: false, t.AddHours(-1));
        Assert.Equal(new[] { "WARN", "ERROR", "CRIT" }, entries.Select(e => e.Level).ToArray());
        Assert.Contains("at frame two", entries[1].Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parse_DropsThePartialFirstLine_OfATailRead()
    {
        var t = new DateTime(2026, 1, 2, 3, 4, 5);
        var text = $"{Stamp(t)} [ERROR] [Cat] cut in half\n{Stamp(t)} [ERROR] [Cat] whole";
        Assert.Single(DiagnosticsBundleServiceLog.Parse(text, partial: true, t.AddHours(-1)));
        Assert.Equal(2, DiagnosticsBundleServiceLog.Parse(text, partial: false, t.AddHours(-1)).Count);
    }

    [Fact]
    public void Parse_CapsAnEntryAtTwoThousandCharacters()
    {
        var t = new DateTime(2026, 1, 2, 3, 4, 5);
        var entries = DiagnosticsBundleServiceLog.Parse($"{Stamp(t)} [ERROR] [Cat] {new string('x', 9_000)}", false, t.AddHours(-1));
        Assert.Equal(DiagnosticsBundleServiceLog.MaxEntryChars, entries[0].Message.Length);
    }

    /// <summary>
    /// #5320: an entry is judged whole and then cut. A value sitting before the cut whose naming statement sits past it
    /// (here past the alias-pass keep length too, and past a continuation line the old read stopped appending) is
    /// withheld; an entry with no such statement is only cut.
    /// </summary>
    [Theory]
    [InlineData(DiagnosticsBundleServiceLog.MaxEntryChars)]
    [InlineData(DiagnosticsBundleServiceLog.AliasInputChars)]
    public void Parse_JudgesTheWholeEntryBeforeTheCut_SoATriggerPastTheCutWithholdsAValueBeforeIt(int keep)
    {
        var t = new DateTime(2026, 1, 2, 3, 4, 5);
        var padding = string.Concat(Enumerable.Repeat("word ", DiagnosticsBundleServiceLog.AliasInputChars / 5 + 100));
        var first = $"login attempt with N'S3cret-canary-ssf' failed {padding}";
        var log = string.Join("\n",
            $"{Stamp(t)} [ERROR] [Cat] {first}",
            "    " + padding,
            "    " + StatementScrubCanary.CanaryStatement,
            $"{Stamp(t)} [ERROR] [Cat] {first}",
            $"{Stamp(t)} [ERROR] [Cat] an ordinary failure {padding}");

        var entries = DiagnosticsBundleServiceLog.Parse(log, false, t.AddHours(-1), keep);

        Assert.Equal(3, entries.Count);
        Assert.Equal(PerformanceMonitor.Common.SensitiveStatements.PlaceholderText, entries[0].Message);
        Assert.DoesNotContain("S3cret-canary-ssf", entries[0].Message, StringComparison.Ordinal);
        Assert.Contains("S3cret-canary-ssf", entries[1].Message, StringComparison.Ordinal);
        Assert.StartsWith("an ordinary failure", entries[2].Message, StringComparison.Ordinal);
        Assert.True(entries[2].Message.Length <= keep);
    }

    [Fact]
    public void Read_ReadsAFileAnotherHandleHoldsOpenForWrite_AndSkipsFilesOutsideTheWindow()
    {
        var dir = Directory.CreateTempSubdirectory("darling-bundle-log-");
        try
        {
            var now = DateTime.Now;
            var today = Path.Combine(dir.FullName, $"darling-service_{now:yyyyMMdd}.log");
            var old = Path.Combine(dir.FullName, $"darling-service_{now.AddDays(-9):yyyyMMdd}.log");
            File.WriteAllText(old, $"{Stamp(now.AddDays(-9))} [ERROR] [Cat] ancient\n");
            using (var writer = new FileStream(today, FileMode.Create, FileAccess.Write, FileShare.ReadWrite))
            {
                var bytes = Encoding.UTF8.GetBytes($"{Stamp(now)} [ERROR] [Cat] current problem\n");
                writer.Write(bytes);
                writer.Flush();
                var result = DiagnosticsBundleServiceLog.Read(dir.FullName, "--log-dir", now.AddHours(-24));
                Assert.Equal("ok", result["status"]!.GetValue<string>());
                Assert.Single((JsonArray)result["entries"]!);
                Assert.Single((JsonArray)result["files"]!);
            }
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }

    [Fact]
    public void Read_MissingDirectory_SaysNotFoundWithAReason_AndDoesNotThrow()
    {
        var result = DiagnosticsBundleServiceLog.Read(Path.Combine(Path.GetTempPath(), "darling-no-such-" + Guid.NewGuid().ToString("N")), "default", DateTime.Now.AddHours(-1));
        Assert.Equal("not_found", result["status"]!.GetValue<string>());
        Assert.False(string.IsNullOrWhiteSpace(result["reason"]!.GetValue<string>()));
        Assert.Contains("--log-dir", result["hint"]!.GetValue<string>(), StringComparison.Ordinal);
    }

    [Fact]
    public void Read_DirectoryWithNoFileInTheWindow_SaysSo()
    {
        var dir = Directory.CreateTempSubdirectory("darling-bundle-log-empty-");
        try
        {
            Assert.Equal("no_files_in_window", DiagnosticsBundleServiceLog.Read(dir.FullName, "default", DateTime.Now.AddHours(-1))["status"]!.GetValue<string>());
        }
        finally
        {
            dir.Delete(recursive: true);
        }
    }
}
