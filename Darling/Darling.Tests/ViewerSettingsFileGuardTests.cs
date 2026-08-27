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
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2434. The viewer's two per-user JSON stores had #2425's defect and a worse version of its consequence.
///
/// <para><b>The failure was unreportable, not merely unreported.</b> Both loaders ended in a bare
/// <c>catch</c> whose only output was <c>Debug.WriteLine</c>, which carries
/// <c>[Conditional("DEBUG")]</c> and is removed by the compiler from a Release build. In the viewer anyone
/// actually runs there was no log line, no dialog and no trace — the settings simply became defaults.</para>
///
/// <para><b>And the next click destroyed the file.</b> Neither Save merged; each serialized its whole
/// in-memory object over the file. Lite's pre-#2425 writers at least parsed first and so ABORTED against a
/// corrupt file, which left it intact by accident. The viewer had no such accident: load-then-save from the
/// time-display dropdown or the Overview sort selector replaced every setting in an unreadable file with a
/// default, on one click nobody would call a save.</para>
///
/// <para>So the pins below come in pairs — what the store now SAYS about each of the three states, and what
/// it leaves on disk. The second is the one that turns an annoyance into data loss.</para>
/// </summary>
public sealed class ViewerSettingsFileGuardTests : IDisposable
{
    /// <summary>A viewer-settings.json a user would recognize: real keys, one trailing comma.</summary>
    private const string TrailingComma = @"{
  ""AlertsEnabled"": true,
  ""AlertCpuThreshold"": 91,
}";

    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), $"viewer-settings-guard-{Guid.NewGuid():N}");

    public ViewerSettingsFileGuardTests() => Directory.CreateDirectory(_directory);

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            Directory.Delete(_directory, recursive: true);
        }
    }

    private string SettingsPath => Path.Combine(_directory, "viewer-settings.json");

    private string PreferencesPath => Path.Combine(_directory, "viewer-preferences.json");

    private string RegistryPath => Path.Combine(_directory, "viewer-servers.json");

    private string[] QuarantineCopies(string ofFile) =>
        Directory.GetFiles(_directory)
            .Where(f => f.StartsWith(ofFile + SettingsFileGuard.QuarantineInfix, StringComparison.Ordinal))
            .ToArray();

    /// <summary>
    /// The legitimate first run, and the reason the three states are not two: no file, no problem, nothing
    /// to say. Anything reported here becomes a log line and a dialog for a user who has done nothing
    /// wrong, which is how a diagnostic earns being ignored.
    /// </summary>
    [Fact]
    public void Load_IsSilentlyAbsent_WhenThereIsNoFile()
    {
        var store = new ViewerAppSettingsStore(SettingsPath);

        var settings = store.Load();

        Assert.Equal(SettingsFileState.Absent, store.LastLoadState);
        Assert.Null(store.LastLoadProblem);
        Assert.Equal(80, settings.AlertCpuThreshold);
    }

    [Fact]
    public void Load_IsReadable_ForAnOrdinaryFile()
    {
        File.WriteAllText(SettingsPath, @"{""AlertCpuThreshold"":91}");
        var store = new ViewerAppSettingsStore(SettingsPath);

        var settings = store.Load();

        Assert.Equal(SettingsFileState.Readable, store.LastLoadState);
        Assert.Null(store.LastLoadProblem);
        Assert.Equal(91, settings.AlertCpuThreshold);
    }

    /// <summary>
    /// The headline case. Defaults are still returned — the viewer must not refuse to open over a bad
    /// settings file — but the store now says the file was there and could not be read, and says WHERE it
    /// broke. "line 4" is a minute's work with an editor; "using defaults" sends someone hunting through a
    /// file they already believe is correct, which is what the old Debug trace would have said if it
    /// survived compilation at all.
    /// </summary>
    [Fact]
    public void Load_ReportsUnreadable_AndWhere_ForACorruptFile()
    {
        File.WriteAllText(SettingsPath, TrailingComma);
        var store = new ViewerAppSettingsStore(SettingsPath);

        var settings = store.Load();

        Assert.Equal(SettingsFileState.Unreadable, store.LastLoadState);
        Assert.NotNull(store.LastLoadProblem);
        Assert.Contains("line 4", store.LastLoadProblem!, StringComparison.Ordinal);
        Assert.Equal(80, settings.AlertCpuThreshold);
    }

    /// <summary>
    /// A file that is a perfectly good JSON object and still cannot be used: one key holding a value of the
    /// wrong shape. It gets its own case because only the TYPED deserialize can see it, which is why that
    /// deserialize is inside the guard rather than in front of it — a classify step that stops at "is this
    /// a JSON object" calls this file readable and hands the exception straight back to the bare catch it
    /// was meant to replace.
    /// </summary>
    [Fact]
    public void Load_ReportsUnreadable_WhenAValueHasTheWrongShape()
    {
        File.WriteAllText(PreferencesPath, @"{""DefaultTimeRangeIndex"":""not a number""}");
        var store = new ViewerPreferencesStore(PreferencesPath);

        var preferences = store.Load();

        Assert.Equal(SettingsFileState.Unreadable, store.LastLoadState);
        Assert.NotNull(store.LastLoadProblem);
        Assert.Equal(3, preferences.DefaultTimeRangeIndex);
    }

    /// <summary>
    /// The data-loss pin, and the one this issue exists for. On dev, one click on the time-display dropdown
    /// loads an unreadable viewer-settings.json as defaults and then serializes those defaults straight over
    /// it — every setting in the file gone, nothing kept, nothing said. The copy has to be made BEFORE the
    /// write, it has to hold the original bytes verbatim, and the save itself still has to happen.
    /// </summary>
    [Fact]
    public void Save_CopiesAnUnreadableFileAside_BeforeReplacingIt()
    {
        File.WriteAllText(SettingsPath, TrailingComma);
        var store = new ViewerAppSettingsStore(SettingsPath);

        var settings = store.Load();
        settings.TimeDisplayMode = "UTC";
        var saved = store.Save(settings);

        Assert.True(saved);

        var copies = QuarantineCopies(SettingsPath);
        Assert.Single(copies);
        Assert.Equal(TrailingComma, File.ReadAllText(copies[0]));

        /* And the save it was protecting still happened. */
        Assert.Contains("UTC", File.ReadAllText(SettingsPath), StringComparison.Ordinal);
    }

    /// <summary>
    /// The same pin on the preferences store, because that is the file the SIDEBAR rewrites: collapsing a
    /// tag group is a whole-file replace of viewer-preferences.json and nobody thinks of it as a save. One
    /// store fixed and the other not would leave the defect exactly where it is hardest to notice.
    /// </summary>
    [Fact]
    public void Save_CopiesAnUnreadablePreferencesFileAside_BeforeReplacingIt()
    {
        File.WriteAllText(PreferencesPath, "{ not json at all");
        var store = new ViewerPreferencesStore(PreferencesPath);

        var preferences = store.Load();
        preferences.CollapsedFleetGroups.Add("Favorites");

        Assert.True(store.Save(preferences));

        var copies = QuarantineCopies(PreferencesPath);
        Assert.Single(copies);
        Assert.Equal("{ not json at all", File.ReadAllText(copies[0]));
    }

    /// <summary>
    /// The third store, and the one where losing the file costs the operator the most: the registry of
    /// monitored servers. Its shape used to make this worse rather than better — it began EMPTY on an
    /// unreadable file, so the very first favourite toggle wrote an empty list over the whole registry.
    /// </summary>
    [Fact]
    public void Save_CopiesAnUnreadableServerRegistryAside_BeforeReplacingIt()
    {
        const string Corrupt = @"[{""ServerName"":""SQL2022"",}]";
        File.WriteAllText(RegistryPath, Corrupt);
        var store = new ViewerServerStore(RegistryPath, new NoSecrets());

        Assert.Equal(SettingsFileState.Unreadable, store.LastLoadState);
        Assert.NotNull(store.LastLoadProblem);

        store.AddServer(new ViewerServerEntry { ServerName = "SQL2019", DisplayName = "Dev" }, null, null);

        var copies = QuarantineCopies(RegistryPath);
        Assert.Single(copies);
        Assert.Equal(Corrupt, File.ReadAllText(copies[0]));
    }

    /// <summary>
    /// And the control that makes the one above meaningful, because it is the mistake that was one line
    /// away: the registry's root is a JSON ARRAY, which the merge path's "the root must be an object" rule
    /// calls unreadable. Borrowing that rule for the typed read would have quarantined a copy of every
    /// healthy registry on every favourite toggle — data loss traded for litter, and litter is how a real
    /// quarantine copy gets ignored.
    /// </summary>
    [Fact]
    public void Save_LeavesNoCopyBehind_ForAHealthyRegistry_ThoughItsRootIsAnArray()
    {
        var store = new ViewerServerStore(RegistryPath, new NoSecrets());
        store.AddServer(new ViewerServerEntry { ServerName = "SQL2022", DisplayName = "Prod" }, null, null);

        var reopened = new ViewerServerStore(RegistryPath, new NoSecrets());
        Assert.Equal(SettingsFileState.Readable, reopened.LastLoadState);

        reopened.AddServer(new ViewerServerEntry { ServerName = "SQL2019", DisplayName = "Dev" }, null, null);

        Assert.Empty(QuarantineCopies(RegistryPath));
        Assert.Equal(2, reopened.GetAllServers().Count);
    }

    /// <summary>
    /// An empty registry file and a missing one are both "no servers", and only one of them is worth
    /// saying anything about. A first run must stay silent or every new install opens on a warning.
    /// </summary>
    [Fact]
    public void Load_IsSilentlyAbsent_ForARegistryThatWasNeverWritten()
    {
        var store = new ViewerServerStore(RegistryPath, new NoSecrets());

        Assert.Equal(SettingsFileState.Absent, store.LastLoadState);
        Assert.Null(store.LastLoadProblem);
        Assert.Empty(store.GetAllServers());
    }

    /// <summary>
    /// The registry answers whether its write landed, which is what lets the sidebar's pin status line and
    /// the import dialog stop claiming things that did not happen. It starts true so "nothing has failed"
    /// is the position before any write, not a claim about one nobody made.
    /// </summary>
    [Fact]
    public void TheRegistry_ReportsWhetherItsWriteLanded()
    {
        var store = new ViewerServerStore(RegistryPath, new NoSecrets());

        Assert.True(store.LastSaveSucceeded);

        store.AddServer(new ViewerServerEntry { ServerName = "SQL2022", DisplayName = "Prod" }, null, null);

        Assert.True(store.LastSaveSucceeded);
        Assert.True(File.Exists(RegistryPath));
    }

    /// <summary>The registry never touches Windows Credential Manager from a test.</summary>
    private sealed class NoSecrets : IViewerServerSecretStore
    {
        private readonly Dictionary<string, (string Username, string Password)> _map = new();

        public void Save(string id, string username, string password) => _map[id] = (username, password);

        public (string Username, string Password)? Find(string id) =>
            _map.TryGetValue(id, out var v) ? v : null;

        public void Delete(string id) => _map.Remove(id);
    }

    /// <summary>
    /// The control, and it matters as much as the pin above: a readable file is replaced with no copy made.
    /// A quarantine on every save would fill the operator's %APPDATA% with junk and teach them to ignore
    /// the one copy that ever means anything.
    /// </summary>
    [Fact]
    public void Save_LeavesNoCopyBehind_WhenTheFileWasReadable()
    {
        File.WriteAllText(SettingsPath, @"{""AlertCpuThreshold"":91}");
        var store = new ViewerAppSettingsStore(SettingsPath);

        Assert.True(store.Save(store.Load()));

        Assert.Empty(QuarantineCopies(SettingsPath));
    }

    /// <summary>The first run from the writer's side: a file that was never there leaves no copy either.</summary>
    [Fact]
    public void Save_LeavesNoCopyBehind_OnAFirstRun()
    {
        var store = new ViewerAppSettingsStore(SettingsPath);

        Assert.True(store.Save(new ViewerAppSettings()));

        Assert.Empty(QuarantineCopies(SettingsPath));
        Assert.True(File.Exists(SettingsPath));
    }

    /// <summary>
    /// Save answers whether it wrote. It is a bool rather than an exception because the two handlers that
    /// call it persist on an ordinary click and neither wraps it — but the answer only stops a UI claiming
    /// a save that did not happen if it EXISTS, and on dev it does not: Save returns void there.
    /// </summary>
    [Fact]
    public void Save_ReportsSuccess_OnAnOrdinaryWrite()
    {
        bool saved = new ViewerAppSettingsStore(SettingsPath).Save(new ViewerAppSettings());

        Assert.True(saved);
    }
}

/// <summary>
/// The category behind #2434 rather than the instance. The defect was never "ViewerAppSettingsStore.Save is
/// wrong" — it was that each store rolled its own read and its own whole-file replace, so whether a save
/// could destroy your configuration depended on which of them you were in. A third store written the same
/// way tomorrow would be just as silent, and nothing but this would notice.
///
/// <para>Source-parsing because the invariant is a wiring one. A behavioral test can prove the guard works
/// and still not notice a store that never asks it.</para>
/// </summary>
public sealed class ViewerSettingsStoreWiringTests
{
    public static TheoryData<string> StoreFiles() => new()
    {
        "ViewerAppSettings.cs",
        "ViewerPreferences.cs",
        "ViewerServerStore.cs"
    };

    /// <summary>
    /// Keyed on <c>_filePath</c> — the store's OWN file — rather than on <c>File.*</c> in general, because
    /// ViewerServerStore legitimately reads a file the user picked in ImportServersFromFile, and a rule
    /// that could not tell those apart would either miss the defect or forbid a feature.
    ///
    /// <para>The lookbehind is not decoration: without it <c>ViewerSettingsFile.Save(_filePath, ...)</c>
    /// contains the very text the rule bans, so the guard fails on the fix it exists to require.</para>
    /// </summary>
    [Theory]
    [MemberData(nameof(StoreFiles))]
    public void EveryViewerSettingsStore_ReadsAndWritesItsOwnFileThroughTheGuardedHelper(string fileName)
    {
        var source = File.ReadAllText(FindViewerFile(fileName));

        Assert.Contains("ViewerSettingsFile.Load<", source, StringComparison.Ordinal);
        Assert.Contains("ViewerSettingsFile.Save(", source, StringComparison.Ordinal);

        Assert.DoesNotMatch(new Regex(@"(?<!\w)File\.\w+\(\s*_filePath"), source);
    }

    /// <summary>
    /// The exact shape that made the loss unreportable: a diagnostic the compiler deletes from the build
    /// the operator runs. Banned outright in the stores so it cannot come back by copy-paste from a sibling.
    /// </summary>
    [Theory]
    [MemberData(nameof(StoreFiles))]
    public void NoViewerSettingsStore_ReportsThroughDebugWriteLine(string fileName)
    {
        var source = File.ReadAllText(FindViewerFile(fileName));

        Assert.DoesNotContain("Debug.WriteLine", source, StringComparison.Ordinal);
    }

    private static string FindViewerFile(string fileName)
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            var candidate = Path.Combine(
                directory.FullName, "Darling", "PerformanceMonitor.Darling.Viewer", fileName);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            directory = directory.Parent;
        }

        throw new FileNotFoundException($"Could not locate {fileName} walking up from {AppContext.BaseDirectory}");
    }
}
