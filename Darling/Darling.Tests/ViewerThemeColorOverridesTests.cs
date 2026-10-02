/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3577 (arm B) as it lands in the Darling viewer. The loader, the key list, the rewriter and the
/// regeneration are shared <c>PerformanceMonitor.Ui</c> code and are pinned once, in
/// <c>Lite.Tests/ThemeColorOverrideTests</c>, against the theme files of BOTH apps. What is the viewer's own
/// is where its file lives and where it does not: <c>theme-overrides.json</c> sits beside
/// <c>viewer-settings.json</c> under the viewer's per-user directory, and the preference is LOCAL to the
/// machine — it never goes to the store or the control plane, because a color is a preference of the
/// person at this screen and not a fact about the fleet. A viewer seat on another machine keeps its own.
/// </summary>
public sealed class ViewerThemeColorOverridesTests
{
    private static readonly string ViewerDir = Path.Combine("Darling", "PerformanceMonitor.Darling.Viewer");

    [Fact]
    public void TheOverridesFile_SitsBesideViewerSettings_UnderTheViewersOwnDirectory()
    {
        var app = File.ReadAllText(Path.Combine(RepoRoot(), ViewerDir, "App.xaml.cs"));

        Assert.Contains("Path.GetDirectoryName(ViewerAppSettingsStore.DefaultFilePath())", app, StringComparison.Ordinal);
        Assert.Contains("ThemeColorOverrides.FileName", app, StringComparison.Ordinal);
        Assert.Equal("theme-overrides.json", ThemeColorOverrides.FileName);
    }

    /// <summary>
    /// The preference is not a settings-model property, not a store column and not a control-plane knob.
    /// <c>ViewerAppSettings</c> keeps <c>ColorTheme</c> (which theme) and nothing about the colors inside
    /// it; the service and storage projects never see the word.
    /// </summary>
    [Fact]
    public void TheColorOverrides_AreNotPersistedInTheSettingsModel_TheStore_OrTheControlPlane()
    {
        var root = RepoRoot();

        var settingsModel = File.ReadAllText(Path.Combine(root, ViewerDir, "ViewerAppSettings.cs"));
        Assert.DoesNotContain("ThemeColorOverride", settingsModel, StringComparison.Ordinal);
        Assert.DoesNotContain("theme-overrides", settingsModel, StringComparison.Ordinal);

        foreach (var project in new[] { "PerformanceMonitor.Darling.Storage", "PerformanceMonitor.Darling.Service" })
        {
            var directory = Path.Combine(root, "Darling", project);
            foreach (var file in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories))
            {
                if (file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
                {
                    continue;
                }

                var text = File.ReadAllText(file);
                Assert.False(text.Contains("ThemeColorOverride", StringComparison.Ordinal) || text.Contains("theme-overrides", StringComparison.Ordinal),
                    $"{Path.GetRelativePath(root, file)} mentions the viewer's color overrides; they are viewer-local and must stay out of the store and the service.");
            }
        }
    }

    /// <summary>
    /// The viewer's Settings window hosts the same shared panel Lite does, beneath its theme combo, and the
    /// panel is the ONLY colors surface — no viewer-only copy of the rows that could drift from Lite's.
    /// </summary>
    [Fact]
    public void TheViewerSettingsWindow_HostsTheSharedPanel_AndNoPrivateCopy()
    {
        var xaml = File.ReadAllText(Path.Combine(RepoRoot(), ViewerDir, "SettingsWindow.xaml"));

        Assert.Single(Regex.Matches(xaml, "<ui:ThemeColorsPanel"));
        Assert.True(xaml.IndexOf("x:Name=\"ColorThemeCombo\"", StringComparison.Ordinal) < xaml.IndexOf("<ui:ThemeColorsPanel", StringComparison.Ordinal));
        Assert.DoesNotContain("AccentForegroundColor", xaml, StringComparison.Ordinal); // no hand-rolled rows
    }

    /// <summary>The viewer's own three theme files are the ones the shared regeneration reads — embedded under the loader's names.</summary>
    [Fact]
    public void TheViewerProject_EmbedsItsThemeText()
    {
        var csproj = File.ReadAllText(Path.Combine(RepoRoot(), ViewerDir, "PerformanceMonitor.Darling.Viewer.csproj"));

        Assert.Contains("<EmbeddedResource Include=\"Themes\\*.xaml\" LogicalName=\"Themes/%(Filename).xaml\" />", csproj, StringComparison.Ordinal);
        foreach (var theme in ThemeColorOverrides.ThemeNames)
        {
            Assert.True(File.Exists(Path.Combine(RepoRoot(), ViewerDir, "Themes", theme + "Theme.xaml")),
                $"{ThemeManager.ThemeTextResourceName(theme)} names a file the viewer does not ship.");
        }
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
