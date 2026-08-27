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
using PerformanceMonitor.Common;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2425. One trailing comma in a hand-edited settings.json used to reset all eighty-eight Lite settings
/// at once, say nothing about it anywhere, and then leave the file exposed to the next whole-document
/// rewrite. Two properties are pinned here, and the second is the one that turns an annoyance into data
/// loss.
///
/// <para><b>Absent is not unreadable.</b> The old loaders had a single bare <c>catch</c>, so a first run
/// with no file and a corrupt file took the same silent path to defaults. That is why the silence looked
/// reasonable in the code and was wrong in practice — half the traffic through it really was fine. The
/// split has to hold in BOTH directions: a missing file must stay completely silent, because a first-run
/// warning is pure noise, and a present-but-broken file must never be.</para>
///
/// <para><b>Nothing overwrites what it could not read.</b> Every Save in Lite rewrites the whole document,
/// including saves nobody thinks of as saves, so a writer that starts from a fresh object after a failed
/// parse destroys the only record of the user's real configuration. The copy has to exist BEFORE the write,
/// and the original has to survive making it.</para>
/// </summary>
public sealed class SettingsFileGuardTests
{
    /// <summary>A settings.json a user would recognize: real keys, one syntax error.</summary>
    private const string TrailingComma = @"{
  ""alerts_enabled"": true,
  ""alert_cpu_threshold"": 91,
}";

    private static string NewTempDir(string tag)
    {
        var dir = Path.Combine(Path.GetTempPath(), $"pmlite_{tag}_{Guid.NewGuid():N}");
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static string WriteSettings(string dir, string content)
    {
        var path = Path.Combine(dir, "settings.json");
        File.WriteAllText(path, content);
        return path;
    }

    /// <summary>
    /// The legitimate first run. No file, no problem, nothing to say — and specifically no Problem string,
    /// because anything non-null there becomes a log line and a dialog for a user who has done nothing
    /// wrong.
    /// </summary>
    [Fact]
    public void Read_IsSilentlyAbsent_WhenThereIsNoFile()
    {
        var dir = NewTempDir("absent");
        try
        {
            var read = SettingsFileGuard.Read(Path.Combine(dir, "settings.json"));

            Assert.Equal(SettingsFileState.Absent, read.State);
            Assert.Null(read.Problem);
            Assert.Null(read.Root);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    [Fact]
    public void Read_IsReadable_ForAnOrdinarySettingsFile()
    {
        var dir = NewTempDir("ok");
        try
        {
            var path = WriteSettings(dir, @"{""alerts_enabled"":true,""alert_cpu_threshold"":91}");

            var read = SettingsFileGuard.Read(path);

            Assert.Equal(SettingsFileState.Readable, read.State);
            Assert.Null(read.Problem);
            Assert.NotNull(read.Root);
            Assert.True(read.Root!["alerts_enabled"]!.GetValue<bool>());
            Assert.NotNull(read.Text);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// The headline case, and the reason the diagnostic carries a position rather than the word "failed":
    /// "settings.json is broken" sends someone looking through a file they already believe is correct,
    /// while "line 4" is a minute's work. The System.Text.Json " Path: $ | LineNumber: ..." tail is cut
    /// because the same facts are already in the sentence, in the form a person reads them.
    /// </summary>
    [Fact]
    public void Read_ReportsTheLineAndPosition_ForATrailingComma()
    {
        var dir = NewTempDir("comma");
        try
        {
            var path = WriteSettings(dir, TrailingComma);

            var read = SettingsFileGuard.Read(path);

            Assert.Equal(SettingsFileState.Unreadable, read.State);
            Assert.NotNull(read.Problem);
            Assert.Contains("line 4", read.Problem!, StringComparison.Ordinal);
            Assert.Contains("position", read.Problem!, StringComparison.Ordinal);
            Assert.DoesNotContain(" Path: ", read.Problem!, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// A file holding the JSON literal null parses fine and is still unusable. It gets its own case because
    /// it is the shape that slipped past the writers' old <c>JsonNode.Parse(json) ?? new JsonObject()</c>
    /// read: no exception, no warning, and the very next Save replaced the document with a fresh one
    /// holding a single key.
    /// </summary>
    [Fact]
    public void Read_IsUnreadable_ForARootThatIsNotAnObject()
    {
        var dir = NewTempDir("notobject");
        try
        {
            Assert.Equal(SettingsFileState.Unreadable, SettingsFileGuard.Read(WriteSettings(dir, "null")).State);

            var array = SettingsFileGuard.Read(WriteSettings(dir, "[1,2,3]"));
            Assert.Equal(SettingsFileState.Unreadable, array.State);
            Assert.Contains("array", array.Problem!, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// An empty file has nothing to preserve but plenty to explain — settings that were there yesterday are
    /// gone today, and a write interrupted by a full disk or a crash is the likeliest reason. Reported, not
    /// quietly folded into "absent".
    /// </summary>
    [Fact]
    public void Read_IsUnreadable_ForAnEmptyFile()
    {
        var dir = NewTempDir("empty");
        try
        {
            var read = SettingsFileGuard.Read(WriteSettings(dir, "   \n"));

            Assert.Equal(SettingsFileState.Unreadable, read.State);
            Assert.NotNull(read.Problem);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// The path that has always worked keeps working: a readable file is MERGED into, so keys the writer
    /// never mentions — hand-edited ones with no UI, most of all — survive the save.
    /// </summary>
    [Fact]
    public void RootForWrite_MergesIntoTheExistingDocument_WhenReadable()
    {
        var dir = NewTempDir("merge");
        try
        {
            var path = WriteSettings(dir, @"{""check_for_updates_on_startup"":false,""alert_cpu_threshold"":91}");

            var forWrite = SettingsFileGuard.RootForWrite(path, DateTime.Now);

            Assert.Null(forWrite.Problem);
            Assert.Null(forWrite.QuarantinedTo);
            Assert.False(forWrite.Root["check_for_updates_on_startup"]!.GetValue<bool>());
            Assert.Equal(91, forWrite.Root["alert_cpu_threshold"]!.GetValue<int>());
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// The first run again, from the writer's side: no file means a fresh document and still no diagnostic.
    /// A quarantine copy here would be litter, and a warning would be a lie.
    /// </summary>
    [Fact]
    public void RootForWrite_StartsFreshAndSilent_WhenTheFileIsAbsent()
    {
        var dir = NewTempDir("firstwrite");
        try
        {
            var forWrite = SettingsFileGuard.RootForWrite(Path.Combine(dir, "settings.json"), DateTime.Now);

            Assert.Null(forWrite.Problem);
            Assert.Null(forWrite.QuarantinedTo);
            Assert.Empty(forWrite.Root);
            Assert.Empty(Directory.GetFiles(dir));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// The data-loss case. An unreadable file is copied aside BEFORE the caller is handed a document to
    /// write, the copy holds the original bytes verbatim, and the original is still where it was — a move
    /// would leave a hole if the write that follows also failed.
    /// </summary>
    [Fact]
    public void RootForWrite_CopiesTheUnreadableFileAside_BeforeHandingBackAFreshDocument()
    {
        var dir = NewTempDir("quarantine");
        try
        {
            var path = WriteSettings(dir, TrailingComma);

            var forWrite = SettingsFileGuard.RootForWrite(path, new DateTime(2026, 8, 21, 14, 5, 2, DateTimeKind.Local));

            Assert.NotNull(forWrite.Problem);
            Assert.NotNull(forWrite.QuarantinedTo);
            Assert.Equal(path + ".unreadable-20260821-140502", forWrite.QuarantinedTo);
            Assert.Equal(TrailingComma, File.ReadAllText(forWrite.QuarantinedTo!));

            /* Copied, not moved: the caller's write can still fail. */
            Assert.Equal(TrailingComma, File.ReadAllText(path));

            /* And what the caller writes must not carry anything from the file nobody could read. */
            Assert.Empty(forWrite.Root);
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }

    /// <summary>
    /// Two unreadable files quarantined inside the same second must not collide, because the collision
    /// would destroy exactly the bytes the first copy was made to keep. Same timestamp, twice, deliberately.
    /// </summary>
    [Fact]
    public void Quarantine_DoesNotOverwriteAnEarlierCopyFromTheSameSecond()
    {
        var dir = NewTempDir("collide");
        try
        {
            var path = WriteSettings(dir, TrailingComma);
            var stamp = new DateTime(2026, 8, 21, 14, 5, 2, DateTimeKind.Local);

            var first = SettingsFileGuard.Quarantine(path, stamp);
            File.WriteAllText(path, "{ still not json");
            var second = SettingsFileGuard.Quarantine(path, stamp);

            Assert.NotNull(first);
            Assert.NotNull(second);
            Assert.NotEqual(first, second);
            Assert.Equal(TrailingComma, File.ReadAllText(first!));
            Assert.Equal("{ still not json", File.ReadAllText(second!));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }
}

/// <summary>
/// The category guard behind #2425 and #2433, rather than the instance. The defect was never "WriteSetting
/// is wrong" — it was that separate methods each rolled their own read of settings.json in front of their
/// own whole-document rewrite, so the safety of a Save depended on which method you happened to be in, and
/// so did whether anyone could find out afterwards that it had failed.
///
/// <para>#2425 answered the first half by making every one of those reads the quarantining one. #2433
/// answered the second by removing the reads and the writes: the Settings window opens the document once,
/// hands it to all ten of its writers, and writes it once. So the invariant this guard pins is now
/// stronger and simpler than counting reads against writes per file — settings.json has exactly ONE
/// writer in the whole of Lite, and that writer takes the quarantining read.</para>
///
/// <para>Source-parsing because the invariant is a wiring one. A behavioral test can prove the guard works
/// and still not notice a caller that never asks it.</para>
/// </summary>
public sealed class SettingsWriterQuarantineWiringTests
{
    [Fact]
    public void SettingsJson_HasExactlyOneWriterInAllOfLite()
    {
        var writers = new List<string>();
        var described = new List<string>();
        var total = 0;

        foreach (var file in LiteSourceFiles())
        {
            var rewrites = Regex.Matches(WithoutComments(File.ReadAllText(file)), @"File\.WriteAllText\(\s*settingsPath").Count;
            if (rewrites == 0)
            {
                continue;
            }

            total += rewrites;
            writers.Add(file);
            described.Add($"{Path.GetFileName(file)} ({rewrites})");
        }

        Assert.True(total > 0,
            "No settings.json rewrite found anywhere in Lite — this guard's anchor moved and it is testing nothing.");
        Assert.True(total == 1,
            $"{total} whole-document rewrite(s) of settings.json across {writers.Count} file(s): " +
            string.Join(", ", described) + ". A second writer brings back both defects at once — a rewrite " +
            "that reads the file itself can replace an unparseable settings.json without copying it aside " +
            "(#2425), and a save split across several writes has no single honest answer to report (#2433).");
        Assert.EndsWith("App.xaml.cs", writers[0], StringComparison.Ordinal);
    }

    [Fact]
    public void TheOneWriter_TakesTheQuarantiningRead()
    {
        var source = File.ReadAllText(FindRepoFile(Path.Combine("Lite", "App.xaml.cs")));

        Assert.Matches(new Regex(@"[=(,]\s*SettingsRootForWrite\(\)"), source);
    }

    /// <summary>
    /// The exact shape that made a non-object root a silent total overwrite: Parse returns null for the JSON
    /// literal null, the null-coalesce reads that as "no file", and the save replaces the document. Banned
    /// across all of Lite so it cannot be reintroduced by copy-paste from a sibling writer.
    /// </summary>
    [Fact]
    public void NoWriter_FallsBackToAFreshDocumentOnAFailedParse()
    {
        var banned = new Regex(@"JsonNode\.Parse\([^;]*\)\s*\?\?\s*new JsonObject\(\)");

        foreach (var file in LiteSourceFiles())
        {
            Assert.DoesNotMatch(banned, WithoutComments(File.ReadAllText(file)));
        }
    }

    /* Both scans run on code with the comments removed, and SettingsFileGuard is why: the one place that
       explains WHY `JsonNode.Parse(json) ?? new JsonObject()` is banned has to quote it to explain it, and
       a scanner that cannot tell a comment from code reads the explanation as the offence. Same trap as
       #2418's key extractor, one file over. */
    private static string WithoutComments(string source)
    {
        var withoutBlocks = Regex.Replace(source, @"/\*.*?\*/", "", RegexOptions.Singleline);
        return Regex.Replace(withoutBlocks, @"//[^\r\n]*", "");
    }

    private static IEnumerable<string> LiteSourceFiles() =>
        Directory.EnumerateFiles(FindRepoDirectory("Lite"), "*.cs", SearchOption.AllDirectories)
            .Where(f => !f.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                     && !f.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .OrderBy(f => f, StringComparer.Ordinal);

    private static string FindRepoFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new FileNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }

    private static string FindRepoDirectory(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 8 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (Directory.Exists(candidate) && File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")))
            {
                return candidate;
            }
            dir = Path.GetDirectoryName(dir);
        }

        throw new DirectoryNotFoundException($"Could not locate {relativePath} walking up from {AppContext.BaseDirectory}");
    }
}
