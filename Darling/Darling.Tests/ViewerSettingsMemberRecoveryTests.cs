/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2456. #2444 gave Lite a reader that checks a value's <c>ValueKind</c> before it calls a getter, so one
/// badly-shaped setting costs its own setting and the startup dialog names every key it could not read. The
/// viewer had the same complaint one level up and a different mechanism: it round-trips a WHOLE object, so
/// the deserialize fails before any property exists — it could name the file and the parse position and
/// nothing else, and one bad member cost every setting in the file.
///
/// <para><b>Per-property reads were the wrong answer here, and the reason is not the typing.</b> Lite's
/// settings.json is built key by key on BOTH sides — #2441 made every writer mutate one
/// <c>JsonObject</c> — so a per-key read is symmetric with a per-key write. These files are a whole-object
/// round trip on both sides. Converting only the READ half to hand-written properties would break that
/// symmetry, and a property added to <see cref="ViewerAppSettings"/> afterwards would still be serialized
/// on save and silently never read back. That is a worse defect than the one being fixed and an invisible
/// one. So the fix names the member the deserializer stopped on, drops it, and runs the SAME deserialize
/// again — which recovers the whole set rather than the first one, and keeps the round trip intact.</para>
///
/// <para><b>The dangerous half is what it refuses to recover</b>, and
/// <see cref="ReadObject_LeavesAnArrayRootedRegistryAllOrNothing"/> is the control for it. The registry of
/// monitored servers is a root JSON array; "drop the element that would not deserialize" there means
/// silently deleting a server the operator added, which is the data loss #2434 exists to prevent wearing a
/// repair's clothes. Only a top-level member of a root OBJECT is ever dropped.</para>
/// </summary>
public sealed class ViewerSettingsMemberRecoveryTests : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), $"viewer-settings-members-{Guid.NewGuid():N}");

    public ViewerSettingsMemberRecoveryTests() => Directory.CreateDirectory(_directory);

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private string SettingsPath => Path.Combine(_directory, "viewer-settings.json");

    private string RegistryPath => Path.Combine(_directory, "viewer-servers.json");

    private string[] QuarantineCopies(string ofFile) =>
        Directory.GetFiles(_directory)
            .Where(f => f.StartsWith(ofFile + SettingsFileGuard.QuarantineInfix, StringComparison.Ordinal))
            .ToArray();

    // ── What the reader can now say ───────────────────────────────────────────────────────────────

    /// <summary>
    /// The defect, and the whole of the issue's title. One member holds a string where an int belongs. On
    /// dev that costs every setting in the file and the message can only say "line 3, position 30". Here it
    /// names <c>AlertCpuThreshold</c>, and the eight settings around it — including the ones AFTER the bad
    /// one, which is where the old ordering-dependent loss was worst — keep the values the file gave them.
    /// </summary>
    [Fact]
    public void ReadObject_NamesTheSettingItCouldNotRead_AndKeepsEveryOtherOne()
    {
        File.WriteAllText(SettingsPath, @"{
  ""AlertsEnabled"": false,
  ""AlertCpuThreshold"": ""ninety"",
  ""SmtpServer"": ""mail.example.com"",
  ""AlertCooldownMinutes"": 22
}");

        var read = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Equal(SettingsFileState.Unreadable, read.State);
        Assert.NotNull(read.Value);
        Assert.NotNull(read.UnreadableMembers);
        Assert.Equal(new[] { "AlertCpuThreshold" }, read.UnreadableMembers!.Select(m => m.Member));

        /* Both sides of the bad member survive. The setting before it, and the two after it — which is the
           half the single try/catch could never have delivered, because it threw and stopped reading. */
        Assert.False(read.Value!.AlertsEnabled);
        Assert.Equal("mail.example.com", read.Value.SmtpServer);
        Assert.Equal(22, read.Value.AlertCooldownMinutes);

        /* And the one it could not read is at its own default, not at some neighbour's value. */
        Assert.Equal(80, read.Value.AlertCpuThreshold);
    }

    /// <summary>
    /// The whole set, not the first one. <c>JsonException.Path</c> names ONE member — measured: a file with
    /// two bad members reports only whichever the reader met first — so "which settings were lost" needs
    /// the retry, not just the path. Someone who hand-edited one line has one mistake; someone who pasted a
    /// block has several, and stopping at the first means fixing, restarting, and finding the next.
    /// </summary>
    [Fact]
    public void ReadObject_NamesEverySettingItCouldNotRead_NotJustTheFirst()
    {
        File.WriteAllText(SettingsPath, @"{
  ""McpEnabled"": ""yes"",
  ""AlertCpuThreshold"": ""ninety"",
  ""SmtpPort"": [ 25 ],
  ""AlertCooldownMinutes"": 22
}");

        var read = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Equal(
            new[] { "McpEnabled", "AlertCpuThreshold", "SmtpPort" },
            read.UnreadableMembers!.Select(m => m.Member));
        Assert.Equal(22, read.Value!.AlertCooldownMinutes);

        /* The summary is the line the log and the dialog render, so it names them rather than counting
           them — a count is what sends someone off to proofread an eighty-key file. */
        Assert.Contains("McpEnabled", read.Problem!, StringComparison.Ordinal);
        Assert.Contains("AlertCpuThreshold", read.Problem!, StringComparison.Ordinal);
        Assert.Contains("SmtpPort", read.Problem!, StringComparison.Ordinal);
    }

    /// <summary>
    /// A member's problem carries no line or position, and that omission is a correctness fix rather than a
    /// trim. Each retry runs over the document with the previous member removed, and removing it
    /// re-serializes — so from the second member onward the exception's position describes the RE-SERIALIZED
    /// text, not the file the user is about to open. Measured on the first draft of this change: three bad
    /// members on lines 2, 3 and 4 reported "line 2, position 22", then "line 1, position 30" and "line 1,
    /// position 14". A position that confidently names the wrong line is worse than none, and the member's
    /// NAME is a better locator anyway.
    ///
    /// <para>The document fault keeps its position, in the same test, because there the parse position is
    /// the only locator there is — and because a rule applied to both would have quietly taken it away.</para>
    /// </summary>
    [Fact]
    public void AMemberProblemCarriesNoParsePosition_ThoughTheDocumentFaultStillDoes()
    {
        File.WriteAllText(SettingsPath, @"{
  ""McpEnabled"": ""yes"",
  ""AlertCpuThreshold"": ""ninety"",
  ""SmtpPort"": [ 25 ]
}");

        var members = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Equal(3, members.UnreadableMembers!.Count);
        foreach (var problem in members.UnreadableMembers!)
        {
            Assert.DoesNotContain("line ", problem.Problem, StringComparison.Ordinal);
            Assert.DoesNotContain("position ", problem.Problem, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("line ", members.Problem!, StringComparison.Ordinal);

        File.WriteAllText(SettingsPath, @"{
  ""AlertsEnabled"": true,
  ""AlertCpuThreshold"": 91,
}");

        var document = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Contains("line 4, position 1", document.Problem!, StringComparison.Ordinal);
    }

    /// <summary>
    /// A bad ELEMENT inside a list-valued setting costs that setting and nothing else. The exception's path
    /// is <c>$.AlertExcludedDatabases[1]</c>, so the reader takes the top-level member off the front of it
    /// and drops the whole list — coarse on purpose: editing an operator's list down to the elements that
    /// happened to parse would be a repair nobody asked for.
    /// </summary>
    [Fact]
    public void ReadObject_DropsTheWholeListWhenOneElementIsWrong()
    {
        File.WriteAllText(SettingsPath, @"{
  ""AlertExcludedDatabases"": [ ""tempdb"", 7 ],
  ""AlertCooldownMinutes"": 22
}");

        var read = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Equal(new[] { "AlertExcludedDatabases" }, read.UnreadableMembers!.Select(m => m.Member));
        Assert.Empty(read.Value!.AlertExcludedDatabases);
        Assert.Equal(22, read.Value.AlertCooldownMinutes);
    }

    /// <summary>
    /// A DOCUMENT fault is unchanged, and has to be: there is no member to name, nothing was read, and
    /// every setting really is at its default. The message says where the parse stopped, which is what it
    /// could always say and is the right answer here. Reporting a member list for this case would be the
    /// overstatement the issue warns about.
    /// </summary>
    [Fact]
    public void ReadObject_LeavesADocumentFaultAllOrNothing()
    {
        File.WriteAllText(SettingsPath, @"{
  ""AlertsEnabled"": true,
  ""AlertCpuThreshold"": 91,
}");

        var read = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Equal(SettingsFileState.Unreadable, read.State);
        Assert.Null(read.Value);
        Assert.True(read.UnreadableMembers is null or { Count: 0 });
        Assert.Contains("line 4", read.Problem!, StringComparison.Ordinal);
    }

    /// <summary>
    /// THE control, and the mistake that was one line away. The viewer's server registry is a root JSON
    /// ARRAY, and a bad entry's path is <c>$[1]</c> — recovering it would mean deleting a monitored server
    /// from the operator's registry and calling it a repair. That is the exact class of loss #2434 was
    /// filed about. The registry stays all-or-nothing, and the count is asserted so a future widening of
    /// the recovery cannot silently start editing it.
    /// </summary>
    [Fact]
    public void ReadObject_LeavesAnArrayRootedRegistryAllOrNothing()
    {
        File.WriteAllText(RegistryPath, @"[
  { ""ServerName"": ""SQL2022"", ""DisplayName"": ""Prod"" },
  { ""ServerName"": 7 }
]");

        var read = SettingsFileGuard.ReadObject<List<ViewerServerEntry>>(RegistryPath);

        Assert.Equal(SettingsFileState.Unreadable, read.State);
        Assert.Null(read.Value);
        Assert.True(read.UnreadableMembers is null or { Count: 0 });
    }

    /// <summary>The control that keeps every assertion above meaningful: a healthy file is Readable, reports
    /// nothing, and is not touched. The first thing a recovery that fires too eagerly breaks.</summary>
    [Fact]
    public void ReadObject_SaysNothingAboutAHealthyFile()
    {
        File.WriteAllText(SettingsPath, @"{ ""AlertCpuThreshold"": 91, ""AlertsEnabled"": false }");

        var read = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Equal(SettingsFileState.Readable, read.State);
        Assert.True(read.UnreadableMembers is null or { Count: 0 });
        Assert.Null(read.Problem);
        Assert.Equal(91, read.Value!.AlertCpuThreshold);
    }

    /// <summary>
    /// The invariant, and the combination review found that broke it: a non-empty
    /// <c>UnreadableMembers</c> always comes with a non-null <c>Value</c>. One direction only — an ordinary
    /// readable file has a value and no members, which <see cref="ReadObject_SaysNothingAboutAHealthyFile"/>
    /// covers.
    ///
    /// <para>The reader can drop one member successfully and THEN meet a fault it cannot attribute — a path
    /// like <c>$['weird.name']</c>, which System.Text.Json bracket-quotes for a property name that is not a
    /// bare identifier. Before the fix that returned a null value carrying a member list, and the viewer's
    /// dialog routes on the list: it would have said "these settings could not be read… everything else in
    /// their file loaded normally" about a file where NOTHING loaded and every setting reverted. An
    /// overstatement in exactly the place this issue exists to remove one.</para>
    ///
    /// <para>The fixture uses a local type because no viewer setting has a name like that today, which is
    /// what made this latent rather than live. That is the reason to pin it rather than to leave it: the
    /// first property that needs a converter throwing <c>ArgumentException</c> — an enum, a DateTime — makes
    /// it reachable with no warning, and the failure is a confident lie rather than a crash.</para>
    /// </summary>
    [Fact]
    public void APartialRecoveryThatCannotFinish_ReportsNoMembersAtAll()
    {
        File.WriteAllText(SettingsPath, @"{
  ""Fine"": ""not a number"",
  ""weird.name"": ""not a number either""
}");

        var read = SettingsFileGuard.ReadObject<AwkwardlyNamed>(SettingsPath);

        Assert.Equal(SettingsFileState.Unreadable, read.State);
        Assert.Null(read.Value);
        Assert.True(read.UnreadableMembers is null or { Count: 0 },
            "A read that recovered nothing must not name the members it managed to drop on the way.");

        /* And the message is the FIRST failure's, whose line and position are against the file the user has
           rather than against a re-serialized copy of it. */
        Assert.Contains("line 2", read.Problem!, StringComparison.Ordinal);
    }

    /// <summary>
    /// A hand-named member recovers like any other. Review raised the narrow identifier match as a diacritic
    /// edge case; measuring System.Text.Json showed it is wider and far more ordinary than that — STJ writes
    /// the DOT form for every name needing no escaping, so <c>$.alert-cpu-threshold</c>,
    /// <c>$.dollar$sign</c> and <c>$.サーバー</c> all arrive that way, and only a name like
    /// <c>$['with space']</c> gets bracket-quoted.
    ///
    /// <para>A hyphenated key is an ordinary thing to find in a settings file. The narrow form captured
    /// <c>alert</c> out of it, found no such member, and fell back to whole-file <c>Unreadable</c> — safe,
    /// and silently short of the per-member recovery, for exactly the settings someone had to name by hand.
    /// Pinned with the two names that actually differ between the forms rather than with the diacritic,
    /// because those are the ones a settings file is likely to contain.</para>
    /// </summary>
    [Fact]
    public void AHandNamedMemberRecoversLikeAnyOther()
    {
        File.WriteAllText(SettingsPath, @"{
  ""alert-cpu-threshold"": ""ninety"",
  ""dollar$sign"": ""also not a number"",
  ""Fine"": 22
}");

        var read = SettingsFileGuard.ReadObject<HandNamed>(SettingsPath);

        Assert.Equal(
            new[] { "alert-cpu-threshold", "dollar$sign" },
            read.UnreadableMembers!.Select(m => m.Member));
        Assert.Equal(22, read.Value!.Fine);
    }

    /// <summary>And the boundary that must NOT move with it: a bracket-quoted path is still refused, because
    /// the reader has no business editing a document it cannot name a member in unambiguously.</summary>
    [Fact]
    public void ABracketQuotedPathIsStillRefused()
    {
        File.WriteAllText(SettingsPath, @"{ ""with space"": ""not a number"" }");

        var read = SettingsFileGuard.ReadObject<HandNamed>(SettingsPath);

        Assert.Equal(SettingsFileState.Unreadable, read.State);
        Assert.Null(read.Value);
        Assert.True(read.UnreadableMembers is null or { Count: 0 });
    }

    /// <summary>Members System.Text.Json renders in its dot form despite not being C# identifiers, plus one
    /// it bracket-quotes. Local to the test: these exist to cover the path shapes, not to model a setting.</summary>
    private sealed class HandNamed
    {
        [JsonPropertyName("alert-cpu-threshold")] public int Hyphenated { get; set; } = 80;

        [JsonPropertyName("dollar$sign")] public int Dollar { get; set; } = 1;

        [JsonPropertyName("with space")] public int Spaced { get; set; } = 2;

        public int Fine { get; set; } = 3;
    }

    /// <summary>A type with a member System.Text.Json reports as <c>$['weird.name']</c> — a path the reader
    /// deliberately will not edit a document on. Local to this test because it exists to reach a branch, not
    /// to model anything the viewer stores.</summary>
    private sealed class AwkwardlyNamed
    {
        [JsonPropertyName("weird.name")]
        public int Weird { get; set; } = 7;

        public int Fine { get; set; } = 3;
    }

    /// <summary>
    /// The recovery borrows the caller's leniency, so the two reads inside it cannot judge the same file by
    /// different standards — the "one judge, not two" property the whole design rests on.
    ///
    /// <para>Review found the seam. The retry deserializes with the caller's
    /// <see cref="JsonSerializerOptions"/>, but dropping a member re-parses the text, and a bare
    /// <c>JsonNode.Parse</c> runs with trailing commas and comments DISALLOWED however lenient the caller
    /// is. So for a caller that permits them, the deserialize would reach the bad member and the re-parse
    /// would throw on the very same text — and the recovery would quietly bail to whole-file Unreadable,
    /// giving up the per-member repair on exactly the files it was written for. Silent, because it falls
    /// back to the old behaviour rather than crashing.</para>
    ///
    /// <para>Not hypothetical in this repo: <c>ViewerSettings</c> reads darling.json with
    /// <c>AllowTrailingCommas</c> and <c>ReadCommentHandling.Skip</c> because that file is JSONC, and seven
    /// call sites here set those options. None of the three viewer stores does today, which is what made it
    /// dormant — and <see cref="SettingsFileGuard.ReadObject{T}"/> is a general-purpose Common API, so the
    /// next caller is as likely to be lenient as strict.</para>
    /// </summary>
    [Fact]
    public void TheRecoveryBorrowsTheCallersLeniency_SoBothOfItsReadsAgree()
    {
        var lenient = new JsonSerializerOptions
        {
            AllowTrailingCommas = true,
            ReadCommentHandling = JsonCommentHandling.Skip,
        };

        /* A file the LENIENT deserialize is perfectly happy with except for one member's value: a comment
           and a trailing comma, both of which a strict re-parse rejects outright. */
        File.WriteAllText(SettingsPath, @"{
  // the operator left a note here
  ""AlertCpuThreshold"": ""ninety"",
  ""AlertCooldownMinutes"": 22,
}");

        var read = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath, lenient);

        Assert.Equal(new[] { "AlertCpuThreshold" }, read.UnreadableMembers!.Select(m => m.Member));
        Assert.NotNull(read.Value);
        Assert.Equal(22, read.Value!.AlertCooldownMinutes);
    }

    /// <summary>And the control: the SAME file under STRICT options is a document fault, not a member one.
    /// Without this the test above would pass on a reader that had simply been made permanently lenient,
    /// which would be a different defect — every strict caller silently accepting JSONC.</summary>
    [Fact]
    public void TheSameFileUnderStrictOptionsIsStillADocumentFault()
    {
        File.WriteAllText(SettingsPath, @"{
  // the operator left a note here
  ""AlertCpuThreshold"": ""ninety"",
  ""AlertCooldownMinutes"": 22,
}");

        var read = SettingsFileGuard.ReadObject<ViewerAppSettings>(SettingsPath);

        Assert.Equal(SettingsFileState.Unreadable, read.State);
        Assert.Null(read.Value);
        Assert.True(read.UnreadableMembers is null or { Count: 0 });
    }

    // ── What the STORES do with it ────────────────────────────────────────────────────────────────

    /// <summary>
    /// The store surfaces the names, which is what the startup dialog reads. And the settings that DID load
    /// are the file's, not defaults — the load is no longer all-or-nothing even though the state still says
    /// the file could not be read, which it could not, as written.
    /// </summary>
    [Fact]
    public void Load_SurfacesTheNamedSettings_AndKeepsTheRestOfTheFile()
    {
        File.WriteAllText(SettingsPath, @"{
  ""AlertCpuThreshold"": ""ninety"",
  ""ConnectionTimeoutSeconds"": 42
}");

        var store = new ViewerAppSettingsStore(SettingsPath);
        var settings = store.Load();

        Assert.Equal(SettingsFileState.Unreadable, store.LastLoadState);
        Assert.Equal(new[] { "AlertCpuThreshold" }, store.LastLoadUnreadableMembers.Select(m => m.Member));
        Assert.Equal(42, settings.ConnectionTimeoutSeconds);
        Assert.Equal(80, settings.AlertCpuThreshold);
    }

    /// <summary>
    /// The safety-critical one, and the reason the state stays <c>Unreadable</c> for a partially-recovered
    /// file rather than becoming some third thing. The dropped member's original text exists nowhere but
    /// that file, and the very next save replaces the file with the recovered object. If the permit had
    /// been relaxed to "we read most of it, go ahead", the one setting the user actually got wrong would be
    /// the one destroyed, with no copy — a strictly worse outcome than dev's, where nothing was recovered
    /// and everything was copied aside.
    /// </summary>
    [Fact]
    public void Save_StillCopiesAPartiallyRecoveredFileAside()
    {
        const string Original = @"{
  ""AlertCpuThreshold"": ""ninety"",
  ""ConnectionTimeoutSeconds"": 42
}";
        File.WriteAllText(SettingsPath, Original);

        var store = new ViewerAppSettingsStore(SettingsPath);
        var settings = store.Load();

        Assert.True(store.Save(settings));

        var copies = QuarantineCopies(SettingsPath);
        Assert.Single(copies);
        Assert.Equal(Original, File.ReadAllText(copies[0]));

        /* And the rewritten file is the recovered settings, so the good half of the user's configuration
           survived the round trip rather than being reset along with the bad member. */
        var reopened = new ViewerAppSettingsStore(SettingsPath).Load();
        Assert.Equal(42, reopened.ConnectionTimeoutSeconds);
    }

    /// <summary>
    /// The category rather than the instance, in the shape #2439 established when writing exactly this kind
    /// of guard turned up a THIRD store nobody had looked at. All three answer the same three questions;
    /// a fourth store written without the member list fails here instead of shipping.
    /// </summary>
    [Fact]
    public void EveryViewerSettingsStore_CanNameTheSettingsItLost()
    {
        foreach (var store in new[]
                 {
                     typeof(ViewerAppSettingsStore),
                     typeof(ViewerPreferencesStore),
                     typeof(ViewerServerStore),
                 })
        {
            Assert.NotNull(store.GetProperty("LastLoadState"));
            Assert.NotNull(store.GetProperty("LastLoadProblem"));
            Assert.NotNull(store.GetProperty("LastLoadUnreadableMembers"));
        }
    }

    // ── The dialog, which is what the issue is actually about ─────────────────────────────────────

    private static string CodeBehind => ReadRepoFile(Path.Combine(
        "Darling", "PerformanceMonitor.Darling.Viewer", "MainWindow.xaml.cs"));

    /// <summary>
    /// The two facts get two sentences. A file that could not be read at all costs every setting in it; a
    /// file read after dropping named members costs only those. One list covering both would have to put an
    /// overstatement in front of whichever case it was not describing — and the issue is explicit that an
    /// overstated capability is worse than an admitted gap. Source-scanned because the routing is a
    /// <c>MessageBox</c> string, which no assertion about an object can reach and which compiles perfectly
    /// clean when it is wrong.
    /// </summary>
    [Fact]
    public void TheStartupDialog_SaysWhichSettingsWereLost_WithoutClaimingTheWholeFile()
    {
        var source = CodeBehind;

        Assert.Contains("_unreadableSettingsFiles", source, StringComparison.Ordinal);
        Assert.Contains("_unreadableSettingsValues", source, StringComparison.Ordinal);

        /* The all-or-nothing sentence and the named-settings sentence both exist, and each is written once. */
        Assert.Equal(1, Occurrences(source, "so the settings they hold are at "));
        Assert.Equal(1, Occurrences(source, "These individual settings could not be read"));
        Assert.Contains("Everything else in their file loaded normally", source, StringComparison.Ordinal);

        /* And the routing: a file with named members goes on the values list and nowhere else, so the two
           sentences can never both describe the same file. */
        Assert.Contains("if (members.Count > 0)", source, StringComparison.Ordinal);
    }

    private static int Occurrences(string haystack, string needle)
    {
        var count = 0;
        for (var i = haystack.IndexOf(needle, StringComparison.Ordinal); i >= 0;
             i = haystack.IndexOf(needle, i + needle.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string ReadRepoFile(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (File.Exists(candidate))
            {
                return File.ReadAllText(candidate);
            }
        }

        throw new FileNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
