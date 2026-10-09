/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Darling.Tests;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #4961 (test 2): Import Settings copies a previous install's settings into this one, and the previous install's
/// <c>install-id.json</c> must never be among them. Two installs that share an id would share Extended Events
/// session names, and each would drop the other's sessions: the very defect the id exists to end. The list of what
/// is copied lives in <see cref="SettingsImport"/>, so it can be read here; and the import handler is pinned to
/// copy through it, so a second, private list cannot grow back inside the window.
/// </summary>
public sealed class SettingsImportTests : IDisposable
{
    private static readonly string[] ConfigFileNames = ["settings.json", "collection_schedule.json", "ignored_wait_types.json"];

    private readonly string _root =
        Path.Combine(Path.GetTempPath(), "lite-import-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_root))
            {
                Directory.Delete(_root, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    /// <summary>A previous install's data root (with its <c>config</c> folder) or a new install's.</summary>
    private (string Root, string Config) NewInstall(string name)
    {
        var root = Path.Combine(_root, name);
        var config = Path.Combine(root, "config");
        Directory.CreateDirectory(config);
        return (root, config);
    }

    [Fact]
    public void TheLists_NameTheSettingsFiles_AndNeverTheInstallIdFile()
    {
        Assert.Equal(ConfigFileNames, SettingsImport.ConfigFiles);
        Assert.Equal(new[] { "alert_state.json" }, SettingsImport.RootFiles);

        var everyName = SettingsImport.ConfigFiles.Concat(SettingsImport.RootFiles);
        Assert.DoesNotContain(InstallIdStore.FileName, everyName, StringComparer.OrdinalIgnoreCase);
    }

    [Fact]
    public void CopyMissing_CopiesTheSettingsFiles_AndNeverTheInstallIdFile()
    {
        var old = NewInstall("old");
        var current = NewInstall("current");
        foreach (var name in ConfigFileNames)
        {
            File.WriteAllText(Path.Combine(old.Config, name), "old " + name);
        }

        File.WriteAllText(Path.Combine(old.Root, "alert_state.json"), "old alert state");

        /* The previous install's id, in both places a copy could pick it up from. */
        File.WriteAllText(Path.Combine(old.Root, InstallIdStore.FileName), "{\"id\":\"0123abcd\"}");
        File.WriteAllText(Path.Combine(old.Config, InstallIdStore.FileName), "{\"id\":\"0123abcd\"}");

        var copied = SettingsImport.CopyMissing(old.Config, old.Root, current.Config, current.Root);

        Assert.Equal(ConfigFileNames.Length + 1, copied);
        foreach (var name in ConfigFileNames)
        {
            Assert.Equal("old " + name, File.ReadAllText(Path.Combine(current.Config, name)));
        }

        Assert.Equal("old alert state", File.ReadAllText(Path.Combine(current.Root, "alert_state.json")));
        Assert.False(File.Exists(Path.Combine(current.Root, InstallIdStore.FileName)),
            "the previous install's id file was copied into the data root");
        Assert.False(File.Exists(Path.Combine(current.Config, InstallIdStore.FileName)),
            "the previous install's id file was copied into the config folder");
    }

    /// <summary>The import has always kept what this install already has; moving the list must not change that.</summary>
    [Fact]
    public void CopyMissing_KeepsAFileThisInstallAlreadyHas_AndDoesNotCountIt()
    {
        var old = NewInstall("old");
        var current = NewInstall("current");
        File.WriteAllText(Path.Combine(old.Config, "settings.json"), "old settings");
        File.WriteAllText(Path.Combine(old.Config, "ignored_wait_types.json"), "old waits");
        File.WriteAllText(Path.Combine(current.Config, "settings.json"), "current settings");
        File.WriteAllText(Path.Combine(old.Root, "alert_state.json"), "old alert state");
        File.WriteAllText(Path.Combine(current.Root, "alert_state.json"), "current alert state");

        var copied = SettingsImport.CopyMissing(old.Config, old.Root, current.Config, current.Root);

        Assert.Equal(1, copied);
        Assert.Equal("current settings", File.ReadAllText(Path.Combine(current.Config, "settings.json")));
        Assert.Equal("old waits", File.ReadAllText(Path.Combine(current.Config, "ignored_wait_types.json")));
        Assert.Equal("current alert state", File.ReadAllText(Path.Combine(current.Root, "alert_state.json")));
    }

    /// <summary>
    /// The handler copies through <see cref="SettingsImport"/> and no longer carries a copy loop and list of its
    /// own, which is where an id file could be added back without the tests above seeing it.
    /// </summary>
    [Fact]
    public void TheImportHandler_CopiesThroughSettingsImport_NotThroughAListOfItsOwn()
    {
        var source = CSharpSourceWalker.StripCommentsAndStrings(
            File.ReadAllText(Path.Combine(RepoRoot(), "Lite", "MainWindow.xaml.cs")));
        var at = source.IndexOf("private void ImportSettingsButton_Click(", StringComparison.Ordinal);
        Assert.True(at >= 0, "MainWindow has the Import Settings handler");
        var body = CSharpSourceWalker.BraceBalanced(source, source.IndexOf('{', at));

        Assert.Contains("SettingsImport.CopyMissing(", body, StringComparison.Ordinal);
        Assert.DoesNotContain("File.Copy(", body, StringComparison.Ordinal);
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "") =>
        Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
