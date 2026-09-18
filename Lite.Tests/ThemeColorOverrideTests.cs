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
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Windows;
using System.Windows.Media;
using PerformanceMonitor.Ui;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3577 (arm B): user-maintainable theme colors with a reset to default, in Lite and the Darling viewer.
///
/// <para>What is pinned here and why. The CLOSED KEY LIST — twelve of the eighteen palette colors — is the
/// scope decision the whole feature rests on, so the twelve are spelled out and the six that are withheld
/// are proven to exist in every theme file (an exclusion is a choice only if the thing excluded is
/// there). The LOADER's contract is that a bad file can never take the app down at startup: every wrong
/// shape is warned about and skipped, at the smallest grain that makes sense, and a Dark override never
/// touches Light. RESET removes exactly one theme's block. And the REGENERATION — the part that makes an
/// override reach the screen — is exercised against the real theme files: the dependency map from color
/// to brush is DERIVED from the XAML (not hand-listed, because a hand list rots the day someone adds a
/// brush), and every brush that map says depends on an overridden color must carry the override after the
/// dictionary is rebuilt. The test files are the six shipped themes, both apps, so a brush added to one
/// theme tomorrow is covered the day it lands.</para>
///
/// <para>The regeneration tests parse WPF XAML and so run on an STA thread, the same shape as
/// <c>MainWindowAccessKeyTests</c>. Nothing here shows a window.</para>
/// </summary>
public sealed class ThemeColorOverrideTests
{
    private static readonly string[][] ThemeFiles =
    {
        new[] { "Lite", "Themes", "DarkTheme.xaml" },
        new[] { "Lite", "Themes", "LightTheme.xaml" },
        new[] { "Lite", "Themes", "CoolBreezeTheme.xaml" },
        new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "Themes", "DarkTheme.xaml" },
        new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "Themes", "LightTheme.xaml" },
        new[] { "Darling", "PerformanceMonitor.Darling.Viewer", "Themes", "CoolBreezeTheme.xaml" },
    };

    public static IEnumerable<object[]> EveryThemeFile() => ThemeFiles.Select(f => new object[] { f });

    /* ================================================================================================
       The closed list
       ================================================================================================ */

    /// <summary>
    /// The twelve, in the order the Settings window shows them. Changing this list is a product decision
    /// (the class remarks on <see cref="ThemeColorOverrides"/> say why it stops at the palette level), so
    /// the test spells every name rather than counting.
    /// </summary>
    [Fact]
    public void TheExposedKeys_AreExactlyTheseTwelve()
    {
        Assert.Equal(
            new[]
            {
                "BackgroundColor", "BackgroundLightColor", "BackgroundDarkColor",
                "ForegroundColor", "ForegroundDimColor",
                "AccentColor", "AccentForegroundColor",
                "AlternatingRowColor",
                "SuccessColor", "WarningColor", "ErrorColor", "InfoColor",
            },
            ThemeColorOverrides.ExposedKeys);
    }

    [Fact]
    public void TheWithheldKeys_AreExactlyTheseSix()
    {
        Assert.Equal(
            new[]
            {
                "AccentHoverColor", "AccentPressedColor", "BackgroundLighterColor",
                "ForegroundMutedColor", "BorderColor", "BorderLightColor",
            },
            ThemeColorOverrides.WithheldKeys);
    }

    /// <summary>
    /// Exposed plus withheld is the whole palette of every theme file in both apps — eighteen
    /// <c>&lt;Color&gt;</c> declarations, no more, no less. So the six are withheld by choice, not because
    /// they are missing, and a nineteenth color added to a theme has to be filed on one side or the other.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryThemeFile))]
    public void EveryThemeFile_DeclaresExactlyTheExposedAndWithheldColors(string[] relativePath)
    {
        var declared = ThemeXamlRewriter.DeclaredColors(ReadRepoFile(relativePath)).Keys
            .OrderBy(k => k, StringComparer.Ordinal).ToArray();

        var expected = ThemeColorOverrides.ExposedKeys.Concat(ThemeColorOverrides.WithheldKeys)
            .OrderBy(k => k, StringComparer.Ordinal).ToArray();

        Assert.Equal(expected, declared);
        Assert.Equal(18, declared.Length);
    }

    [Fact]
    public void ExposedAndWithheld_DoNotOverlap()
    {
        Assert.Empty(ThemeColorOverrides.ExposedKeys.Intersect(ThemeColorOverrides.WithheldKeys));
    }

    /// <summary>Every row measures against another row: a partner outside the twelve would be a readout of nothing.</summary>
    [Fact]
    public void EverySlot_MeasuresAgainstAnotherExposedKey()
    {
        foreach (var slot in ThemeColorOverrides.Slots)
        {
            Assert.True(ThemeColorOverrides.IsExposed(slot.ContrastAgainst),
                $"{slot.Key} measures against {slot.ContrastAgainst}, which is not an exposed key.");
            Assert.NotEqual(slot.Key, slot.ContrastAgainst);
        }
    }

    /// <summary>The pairs the brief named, spelled out so a re-grouping cannot silently re-pair them.</summary>
    [Theory]
    [InlineData("BackgroundColor", "ForegroundColor")]
    [InlineData("BackgroundLightColor", "ForegroundColor")]
    [InlineData("AccentColor", "AccentForegroundColor")]
    [InlineData("AccentForegroundColor", "AccentColor")]
    [InlineData("AlternatingRowColor", "ForegroundColor")]
    [InlineData("SuccessColor", "BackgroundColor")]
    [InlineData("WarningColor", "BackgroundColor")]
    [InlineData("ErrorColor", "BackgroundColor")]
    [InlineData("InfoColor", "BackgroundColor")]
    public void TheContrastPartners_AreTheSurfacesEachColorRendersOn(string key, string partner)
    {
        Assert.Equal(partner, ThemeColorOverrides.SlotFor(key)!.ContrastAgainst);
    }

    /* ================================================================================================
       Hex parsing
       ================================================================================================ */

    [Theory]
    [InlineData("#3A7BD5", 255, 0x3A, 0x7B, 0xD5)]
    [InlineData("3A7BD5", 255, 0x3A, 0x7B, 0xD5)]
    [InlineData("#3a7bd5", 255, 0x3A, 0x7B, 0xD5)]
    [InlineData("  #3A7BD5  ", 255, 0x3A, 0x7B, 0xD5)]
    [InlineData("#803A7BD5", 0x80, 0x3A, 0x7B, 0xD5)]
    public void TryParseHex_AcceptsRrggbbAndAarrggbb(string text, int a, int r, int g, int b)
    {
        Assert.True(ThemeColorOverrides.TryParseHex(text, out var color));
        Assert.Equal(Color.FromArgb((byte)a, (byte)r, (byte)g, (byte)b), color);
    }

    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData(null)]
    [InlineData("#FFF")]
    [InlineData("#GGGGGG")]
    [InlineData("red")]
    [InlineData("#3A7BD")]
    [InlineData("#3A7BD5FF00")]
    [InlineData("rgb(1,2,3)")]
    public void TryParseHex_RejectsEverythingElse(string? text)
    {
        Assert.False(ThemeColorOverrides.TryParseHex(text, out _));
    }

    [Fact]
    public void ToHex_WritesUpperCaseRrggbb_AndAlphaOnlyWhenTranslucent()
    {
        Assert.Equal("#3A7BD5", ThemeColorOverrides.ToHex(Color.FromRgb(0x3A, 0x7B, 0xD5)));
        Assert.Equal("#803A7BD5", ThemeColorOverrides.ToHex(Color.FromArgb(0x80, 0x3A, 0x7B, 0xD5)));
    }

    /* ================================================================================================
       Loader: parse / ignore semantics
       ================================================================================================ */

    [Fact]
    public void FromJson_KeepsGoodEntries_AndWarnsAboutEachBadOneWithoutThrowing()
    {
        var warnings = new List<string>();
        var root = (JsonObject)JsonNode.Parse("""
            {
              "Dark": {
                "AccentColor": "#3A7BD5",
                "AccentHoverColor": "#111111",
                "BorderColor": "#222222",
                "NoSuchKey": "#333333",
                "ErrorColor": "not a color",
                "InfoColor": 12345,
                "WarningColor": null,
                "SuccessColor": "#22C55E"
              },
              "Solarized": { "AccentColor": "#000000" },
              "Light": "#FFFFFF"
            }
            """)!;

        var set = ThemeColorOverrides.FromJson(root, "theme-overrides.json", warnings.Add);

        var dark = set.For("Dark");
        Assert.Equal(2, dark.Count);
        Assert.Equal(Color.FromRgb(0x3A, 0x7B, 0xD5), dark["AccentColor"]);
        Assert.Equal(Color.FromRgb(0x22, 0xC5, 0x5E), dark["SuccessColor"]);

        Assert.Empty(set.For("Light"));
        Assert.Empty(set.For("CoolBreeze"));

        /* One warning per skipped thing: the two withheld keys, the unknown key, the three bad values, the
           unknown theme, the Light block that is a string. Eight. */
        Assert.Equal(8, warnings.Count);
        Assert.Contains(warnings, w => w.Contains("AccentHoverColor", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("BorderColor", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("NoSuchKey", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("Dark.ErrorColor", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("Dark.InfoColor", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("Dark.WarningColor", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("Solarized", StringComparison.Ordinal));
        Assert.Contains(warnings, w => w.Contains("'Light' block", StringComparison.Ordinal));
    }

    /// <summary>The whole point of keying the file by theme: a Dark override is a Dark override.</summary>
    [Fact]
    public void Overrides_AreIsolatedPerTheme()
    {
        var root = (JsonObject)JsonNode.Parse("""{ "Dark": { "AccentColor": "#111111" }, "Light": { "AccentColor": "#EEEEEE" } }""")!;
        var set = ThemeColorOverrides.FromJson(root, "f", null);

        Assert.Equal(Color.FromRgb(0x11, 0x11, 0x11), set.For("Dark")["AccentColor"]);
        Assert.Equal(Color.FromRgb(0xEE, 0xEE, 0xEE), set.For("Light")["AccentColor"]);
        Assert.Empty(set.For("CoolBreeze"));
    }

    [Fact]
    public void Load_MissingFile_IsEmptyAndSilent()
    {
        var warnings = new List<string>();
        var set = ThemeColorOverrides.Load(Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString("N"), "theme-overrides.json"), warnings.Add);

        Assert.False(set.Any);
        Assert.Null(set.SourceText);
        Assert.Empty(warnings);
    }

    [Theory]
    [InlineData("{ \"Dark\": { \"AccentColor\": \"#123456\", } ")]   // trailing comma, unterminated
    [InlineData("[ \"Dark\" ]")]                                    // root is an array
    [InlineData("null")]
    [InlineData("")]
    [InlineData("not json at all")]
    public void Load_MalformedFile_IsEmptyAndWarnsOnce_NeverThrows(string contents)
    {
        var path = TempFile(contents);
        try
        {
            var warnings = new List<string>();
            var set = ThemeColorOverrides.Load(path, warnings.Add);

            Assert.False(set.Any);
            Assert.Single(warnings);
            Assert.Contains("could not be read", warnings[0], StringComparison.Ordinal);
            Assert.Equal(contents, File.ReadAllText(path)); // the loader never touches the file
        }
        finally
        {
            File.Delete(path);
        }
    }

    /* ================================================================================================
       The watcher's reload: a read that fails is a log line, never a throw
       ================================================================================================ */

    /// <summary>
    /// The one path the first cut of this PR left narrow (review on #3606): the watcher's re-read of the
    /// file caught only <see cref="IOException"/>, and it runs from a <c>DispatcherTimer.Tick</c> on the UI
    /// thread with no caller to catch for it — so an <see cref="UnauthorizedAccessException"/> from an ACL
    /// change, an AV scan or a sync client's lock would have been an app crash over a file watcher, against
    /// the class header's own promise. The read is now behind <see cref="ThemeManager.OverridesFileReader"/>
    /// so the failure can be made to happen here: no exception escapes, one warning names the exception,
    /// and the overrides that were applied stay applied.
    /// </summary>
    [Fact]
    public void ReloadFromDisk_WhenTheReadThrowsUnauthorizedAccess_LogsAWarningAndKeepsTheLoadedOverrides()
    {
        var path = TempFile("""{ "Dark": { "AccentColor": "#3A7BD5" } }""");
        using var scope = new ThemeManagerScope(path);
        ThemeManager.LoadOverrides();
        var loaded = ThemeManager.Overrides;
        Assert.Equal(Color.FromRgb(0x3A, 0x7B, 0xD5), loaded.For("Dark")["AccentColor"]);

        ThemeManager.OverridesFileReader = _ => throw new UnauthorizedAccessException("Access to the path is denied.");

        var outcome = ThemeManager.ReloadFromDiskIfChanged();

        Assert.Equal(ThemeManager.ReloadOutcome.Failed, outcome);
        Assert.Single(scope.Warnings);
        Assert.Contains("UnauthorizedAccessException", scope.Warnings[0], StringComparison.Ordinal);
        Assert.Contains("stays as it was", scope.Warnings[0], StringComparison.Ordinal);
        Assert.Same(loaded, ThemeManager.Overrides);
    }

    /// <summary>
    /// An <see cref="IOException"/> is the editor still writing: retried without a warning, up to the cap;
    /// past the cap it is reported like any other failure and the theme is left alone. Whatever the
    /// exception type, the outcome is a return value, never a throw.
    /// </summary>
    [Fact]
    public void ReloadFromDisk_RetriesAnIoExceptionUpToTheCap_ThenWarnsAndStops()
    {
        var path = TempFile("{}");
        using var scope = new ThemeManagerScope(path);
        ThemeManager.LoadOverrides();
        var loaded = ThemeManager.Overrides;

        ThemeManager.OverridesFileReader = _ => throw new IOException("The process cannot access the file because it is being used by another process.");

        for (var attempt = 0; attempt < ThemeManager.MaxReloadRetries; attempt++)
        {
            Assert.Equal(ThemeManager.ReloadOutcome.Retrying, ThemeManager.ReloadFromDiskIfChanged());
        }

        Assert.Empty(scope.Warnings);

        Assert.Equal(ThemeManager.ReloadOutcome.Failed, ThemeManager.ReloadFromDiskIfChanged());
        Assert.Single(scope.Warnings);
        Assert.Contains("IOException", scope.Warnings[0], StringComparison.Ordinal);
        Assert.Same(loaded, ThemeManager.Overrides);

        /* The cap is per change, not per session: a later change starts a fresh count. */
        Assert.Equal(ThemeManager.ReloadOutcome.Retrying, ThemeManager.ReloadFromDiskIfChanged());
    }

    /// <summary>The app's own save reloads and so refreshes the fingerprint; a write that reproduces it is not re-applied.</summary>
    [Fact]
    public void ReloadFromDisk_WhenTheTextIsWhatIsAlreadyLoaded_DoesNothing()
    {
        var path = TempFile("""{ "Dark": { "AccentColor": "#3A7BD5" } }""");
        using var scope = new ThemeManagerScope(path);
        ThemeManager.LoadOverrides();
        var loaded = ThemeManager.Overrides;

        Assert.Equal(ThemeManager.ReloadOutcome.Unchanged, ThemeManager.ReloadFromDiskIfChanged());
        Assert.Same(loaded, ThemeManager.Overrides);
        Assert.Empty(scope.Warnings);
    }

    [Fact]
    public void ReloadFromDisk_WithNoFileConfigured_DoesNothing()
    {
        using var scope = new ThemeManagerScope(null);

        Assert.Equal(ThemeManager.ReloadOutcome.NothingToDo, ThemeManager.ReloadFromDiskIfChanged());
        Assert.Empty(scope.Warnings);
    }

    /// <summary>
    /// <see cref="ThemeManager"/> is process-wide state. Each reload test points it at its own temp file,
    /// captures its warnings, and puts every static back the way it found it — including the reader — so
    /// the regeneration tests in this class (which run after or before, in either order) see the defaults.
    /// </summary>
    private sealed class ThemeManagerScope : IDisposable
    {
        private readonly string? _previousPath = ThemeManager.OverridesFilePath;
        private readonly Action<string>? _previousWarn = ThemeManager.LogWarning;
        private readonly Action<string>? _previousInfo = ThemeManager.LogInfo;
        private readonly Func<string, string> _previousReader = ThemeManager.OverridesFileReader;
        private readonly string? _path;

        public List<string> Warnings { get; } = new();

        public ThemeManagerScope(string? path)
        {
            _path = path;
            ThemeManager.OverridesFilePath = path;
            ThemeManager.LogWarning = Warnings.Add;
            ThemeManager.LogInfo = null;
        }

        public void Dispose()
        {
            ThemeManager.OverridesFileReader = _previousReader;
            ThemeManager.OverridesFilePath = _previousPath;
            ThemeManager.LogWarning = _previousWarn;
            ThemeManager.LogInfo = _previousInfo;
            ThemeManager.LoadOverrides(); // back to whatever the restored path says (nothing, in the suite)

            if (_path is not null && File.Exists(_path))
            {
                File.Delete(_path);
            }
        }
    }

    /* ================================================================================================
       Save / Reset
       ================================================================================================ */

    [Fact]
    public void SaveTheme_ReplacesOnlyThatThemesBlock_AndLeavesSiblingsVerbatim()
    {
        var path = TempFile("""
            {
              "Light": { "AccentColor": "#EEEEEE" },
              "Solarized": { "kept": "as-is" }
            }
            """);
        try
        {
            var written = ThemeColorOverrides.SaveTheme(path, "Dark",
                new Dictionary<string, Color> { ["AccentColor"] = Color.FromRgb(0x3A, 0x7B, 0xD5), ["ErrorColor"] = Color.FromRgb(0xE5, 0x73, 0x73) },
                null);

            Assert.NotNull(written);
            Assert.Equal(written, File.ReadAllText(path));

            var root = (JsonObject)JsonNode.Parse(written!)!;
            Assert.Equal("#3A7BD5", root["Dark"]!["AccentColor"]!.GetValue<string>());
            Assert.Equal("#E57373", root["Dark"]!["ErrorColor"]!.GetValue<string>());
            Assert.Equal("#EEEEEE", root["Light"]!["AccentColor"]!.GetValue<string>());
            Assert.Equal("as-is", root["Solarized"]!["kept"]!.GetValue<string>()); // skipped on load, preserved on save
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void SaveTheme_WritesKeysInSlotOrder_SoTheFileReadsLikeTheSettingsWindow()
    {
        var path = TempFile("{}");
        try
        {
            var colors = new Dictionary<string, Color>
            {
                ["InfoColor"] = Colors.Blue,
                ["BackgroundColor"] = Colors.Black,
                ["AccentColor"] = Colors.Red,
            };
            var written = ThemeColorOverrides.SaveTheme(path, "Dark", colors, null)!;
            var keys = ((JsonObject)JsonNode.Parse(written)!["Dark"]!).Select(kv => kv.Key).ToArray();

            Assert.Equal(new[] { "BackgroundColor", "AccentColor", "InfoColor" }, keys);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>Reset to default: the current theme's block goes, the siblings stay, the file stays.</summary>
    [Fact]
    public void SaveTheme_WithNoColors_RemovesExactlyThatThemesBlock()
    {
        var path = TempFile("""{ "Dark": { "AccentColor": "#111111" }, "Light": { "AccentColor": "#EEEEEE" } }""");
        try
        {
            var written = ThemeColorOverrides.SaveTheme(path, "Dark", new Dictionary<string, Color>(), null);

            Assert.NotNull(written);
            var root = (JsonObject)JsonNode.Parse(written!)!;
            Assert.False(root.ContainsKey("Dark"));
            Assert.Equal("#EEEEEE", root["Light"]!["AccentColor"]!.GetValue<string>());
            Assert.True(File.Exists(path));
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Fact]
    public void SaveTheme_OverAnUnreadableFile_CopiesItAsideFirst_AndSaysSo()
    {
        var path = TempFile("{ this is not json");
        var directory = Path.GetDirectoryName(path)!;
        try
        {
            var warnings = new List<string>();
            var written = ThemeColorOverrides.SaveTheme(path, "Dark",
                new Dictionary<string, Color> { ["AccentColor"] = Colors.Red }, warnings.Add);

            Assert.NotNull(written);
            Assert.Single(warnings);
            Assert.Contains("copied to", warnings[0], StringComparison.Ordinal);

            var quarantined = Directory.GetFiles(directory, Path.GetFileName(path) + ".unreadable-*");
            Assert.Single(quarantined);
            Assert.Equal("{ this is not json", File.ReadAllText(quarantined[0]));

            foreach (var q in quarantined)
            {
                File.Delete(q);
            }
        }
        finally
        {
            File.Delete(path);
        }
    }

    /* ================================================================================================
       The rewriter
       ================================================================================================ */

    [Fact]
    public void Rewrite_ReplacesOnlyTheNamedDeclarations_AndLeavesTheRestByteForByte()
    {
        const string xaml = """
            <ResourceDictionary xmlns="http://schemas.microsoft.com/winfx/2006/xaml/presentation" xmlns:x="http://schemas.microsoft.com/winfx/2006/xaml">
                <Color x:Key="AccentColor">#2eaef1</Color>
                <Color x:Key="AccentHoverColor">#5bc4f5</Color>
                <Color x:Key="BackgroundColor">#181b1f</Color>
                <SolidColorBrush x:Key="AccentBrush" Color="{StaticResource AccentColor}"/>
            </ResourceDictionary>
            """;

        var warnings = new List<string>();
        var rewritten = ThemeXamlRewriter.Rewrite(xaml,
            new Dictionary<string, Color> { ["AccentColor"] = Color.FromRgb(0x3A, 0x7B, 0xD5) }, warnings.Add);

        Assert.Empty(warnings);
        Assert.Contains("<Color x:Key=\"AccentColor\">#3A7BD5</Color>", rewritten, StringComparison.Ordinal);
        Assert.Contains("<Color x:Key=\"AccentHoverColor\">#5bc4f5</Color>", rewritten, StringComparison.Ordinal);
        Assert.Contains("<Color x:Key=\"BackgroundColor\">#181b1f</Color>", rewritten, StringComparison.Ordinal);
        Assert.Equal(xaml.Replace("#2eaef1", "#3A7BD5", StringComparison.Ordinal), rewritten);
    }

    [Fact]
    public void Rewrite_WarnsAndSkips_AKeyTheThemeDoesNotDeclare()
    {
        const string xaml = """<ResourceDictionary><Color x:Key="AccentColor">#2eaef1</Color></ResourceDictionary>""";
        var warnings = new List<string>();

        var rewritten = ThemeXamlRewriter.Rewrite(xaml, new Dictionary<string, Color> { ["InfoColor"] = Colors.Red }, warnings.Add);

        Assert.Equal(xaml, rewritten);
        Assert.Single(warnings);
        Assert.Contains("InfoColor", warnings[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// The rewriter's regex must see every one of the eighteen declarations in every file, or an override
    /// for a color it cannot see would be skipped with a warning nobody reads. Also pins that the themes
    /// declare colors in the element form the regex expects and never twice.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryThemeFile))]
    public void TheColorDeclarationRegex_MatchesEveryDeclarationInEveryTheme_ExactlyOnce(string[] relativePath)
    {
        var xaml = ReadRepoFile(relativePath);
        var matches = ThemeXamlRewriter.ColorDeclaration.Matches(xaml);

        Assert.Equal(18, matches.Count);
        Assert.Equal(18, matches.Select(m => m.Groups["key"].Value).Distinct(StringComparer.Ordinal).Count());
        Assert.Equal(18, Regex.Matches(xaml, "<Color ").Count);

        foreach (Match m in matches)
        {
            Assert.True(ThemeColorOverrides.TryParseHex(m.Groups["value"].Value, out _),
                $"{m.Groups["key"].Value} declares '{m.Groups["value"].Value}', which the override parser would not accept.");
        }
    }

    /* ================================================================================================
       Regeneration against the real theme files
       ================================================================================================ */

    /// <summary>
    /// The regeneration, end to end, on every shipped theme: all twelve exposed colors overridden to
    /// distinct sentinels, the dictionary rebuilt the way <see cref="ThemeManager.Apply"/> rebuilds it, and
    /// then every brush whose XAML says <c>Color="{StaticResource X}"</c> for an overridden X must carry
    /// the sentinel — and every brush derived from a WITHHELD color must not have moved. The brush→color
    /// map is read out of the file under test, so a brush added to a theme tomorrow is checked tomorrow.
    /// This is the pin on the reason the loader regenerates the text instead of merging an override
    /// dictionary: the theme's own brushes follow.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryThemeFile))]
    public void RegeneratingATheme_MovesEveryBrushTheXamlDerivesFromAnOverriddenColor(string[] relativePath)
    {
        var xaml = ReadRepoFile(relativePath);
        var dependencies = ThemeXamlRewriter.BrushDependencies(xaml);
        Assert.NotEmpty(dependencies);

        var stockColors = ThemeXamlRewriter.DeclaredColors(xaml)
            .ToDictionary(kv => kv.Key, kv => { Assert.True(ThemeColorOverrides.TryParseHex(kv.Value, out var c)); return c; }, StringComparer.Ordinal);

        /* Distinct sentinels — #010101, #020202, … — so a brush that picked up the WRONG override is
           caught, not just one that picked up none. None of them equals any stock value. */
        var overrides = ThemeColorOverrides.ExposedKeys
            .Select((key, i) => (key, color: Color.FromRgb((byte)(i + 1), (byte)(i + 1), (byte)(i + 1))))
            .ToDictionary(t => t.key, t => t.color, StringComparer.Ordinal);
        Assert.All(overrides.Values, sentinel => Assert.DoesNotContain(sentinel, stockColors.Values));

        var (dictionary, warnings) = OnStaThread(() =>
        {
            var captured = new List<string>();
            var previous = ThemeManager.LogWarning;
            ThemeManager.LogWarning = captured.Add;
            try
            {
                return (ThemeManager.TryParseRegenerated("test", xaml, overrides), captured);
            }
            finally
            {
                ThemeManager.LogWarning = previous;
            }
        });

        Assert.Empty(warnings);
        Assert.NotNull(dictionary);

        var checkedBrushes = 0;
        foreach (var (brushKeyText, colorKey) in dependencies)
        {
            var brush = Assert.IsType<SolidColorBrush>(dictionary![ResourceKey(brushKeyText)]);
            var expected = overrides.TryGetValue(colorKey, out var sentinel) ? sentinel : stockColors[colorKey];

            Assert.True(expected == brush.Color,
                $"{brushKeyText} is derived from {colorKey} and should be {ThemeColorOverrides.ToHex(expected)} after regeneration, " +
                $"but is {ThemeColorOverrides.ToHex(brush.Color)}.");
            checkedBrushes++;
        }

        /* Every exposed color is depended on by at least one brush in every theme, or the override would
           have nothing visible to move — which would make the row in Settings a lie. */
        foreach (var key in ThemeColorOverrides.ExposedKeys)
        {
            Assert.Contains(key, dependencies.Values);
        }

        Assert.True(checkedBrushes >= 20, $"Only {checkedBrushes} brushes derive from palette colors in {string.Join('/', relativePath)}; expected the full derived set.");

        /* And the Color entries themselves, for the app XAML that reads a Color directly. */
        foreach (var (key, sentinel) in overrides)
        {
            Assert.Equal(sentinel, Assert.IsType<Color>(dictionary![key]));
        }
    }

    /// <summary>
    /// A theme WITHOUT overrides is never regenerated — the compiled dictionary is the load path — and a
    /// regeneration with an empty override set must at least reproduce the stock palette, so the two paths
    /// agree on what the theme is.
    /// </summary>
    [Fact]
    public void RegeneratingWithNoOverrides_ReproducesTheStockPalette()
    {
        var xaml = ReadRepoFile(ThemeFiles[0]);
        var stock = ThemeXamlRewriter.DeclaredColors(xaml);

        var dictionary = OnStaThread(() => ThemeManager.TryParseRegenerated("Dark", xaml, new Dictionary<string, Color>()));

        Assert.NotNull(dictionary);
        foreach (var (key, hex) in stock)
        {
            Assert.True(ThemeColorOverrides.TryParseHex(hex, out var expected));
            Assert.Equal(expected, Assert.IsType<Color>(dictionary![key]));
        }
    }

    [Fact]
    public void RegeneratingUnparseableText_ReturnsNullAndWarns_NeverThrows()
    {
        var warnings = new List<string>();
        var dictionary = OnStaThread(() =>
        {
            var previous = ThemeManager.LogWarning;
            ThemeManager.LogWarning = warnings.Add;
            try
            {
                return ThemeManager.TryParseRegenerated("Dark", "<ResourceDictionary><Color x:Key=\"AccentColor\">oops", new Dictionary<string, Color>());
            }
            finally
            {
                ThemeManager.LogWarning = previous;
            }
        });

        Assert.Null(dictionary);
        Assert.Single(warnings);
        Assert.Contains("stock theme is used", warnings[0], StringComparison.Ordinal);
    }

    /* ================================================================================================
       Contrast
       ================================================================================================ */

    /// <summary>
    /// The numbers PR #3589's tables published, now computed by the class the Settings readout uses. The
    /// two arms of #3577 have to agree on what a pair measures.
    /// </summary>
    [Theory]
    [InlineData("#FFFFFF", "#000000", 21.00)]
    [InlineData("#777777", "#777777", 1.00)]
    [InlineData("#1A2A3A", "#1E6FA8", 2.71)]   // Cool Breeze selected tab, as reported
    [InlineData("#FFFFFF", "#1E6FA8", 5.39)]   // Cool Breeze selected tab, as fixed
    [InlineData("#FFFFFF", "#267BB8", 4.56)]   // Cool Breeze hover ink on the nudged AccentHoverColor
    [InlineData("#E4E6EB", "#2eaef1", 1.99)]   // Dark selected tab, as it was
    [InlineData("#111217", "#2eaef1", 7.52)]   // Dark selected tab, as fixed
    [InlineData("#111217", "#5bc4f5", 9.49)]   // Dark hover ink
    [InlineData("#E4E6EB", "#181b1f", 13.84)]  // Dark page text on the page
    public void Ratio_MatchesTheArmAFixtures(string a, string b, double expected)
    {
        Assert.True(ThemeColorOverrides.TryParseHex(a, out var ca));
        Assert.True(ThemeColorOverrides.TryParseHex(b, out var cb));

        Assert.Equal(expected, Math.Round(WcagContrast.Ratio(ca, cb), 2));
        Assert.Equal(expected, Math.Round(WcagContrast.Ratio(cb, ca), 2)); // order-independent
    }

    [Theory]
    [InlineData(21.0, ContrastBand.Pass)]
    [InlineData(4.5, ContrastBand.Pass)]
    [InlineData(4.49, ContrastBand.Marginal)]
    [InlineData(3.0, ContrastBand.Marginal)]
    [InlineData(2.99, ContrastBand.Fail)]
    [InlineData(1.0, ContrastBand.Fail)]
    public void Band_SplitsAtThreeAndFourPointFive(double ratio, ContrastBand expected)
    {
        Assert.Equal(expected, WcagContrast.Band(ratio));
    }

    [Fact]
    public void Format_PrintsTwoDecimalsAndTheRatioSuffix()
    {
        Assert.Equal("5.39:1", WcagContrast.Format(5.3923));
        Assert.Equal("21.00:1", WcagContrast.Format(21));
    }

    /// <summary>
    /// The stock backgrounds, text tones, accent pair and alternating row read green on every shipped theme
    /// — so a fresh install's Colors section shows those eight rows green, and any amber there is the
    /// operator's own. The four STATUS rows are deliberately not in this pin: on the two light themes the
    /// stock <c>WarningColor</c> (2.09 / 2.47:1) and <c>InfoColor</c> (4.25 / 2.32:1) sit below the text
    /// threshold against their page, and Cool Breeze's Success and Error are marginal (4.04 / 4.43). That
    /// is a fact about those palettes this readout now makes visible (arm A's sweep measured Dark's markers
    /// and Cool Breeze's accent surfaces, not the light themes' status colors on the page), reported with
    /// this PR rather than repainted here — changing a palette value is a theme-design call, and it has to
    /// land in the Dashboard's copies too or <c>ThemeParityTests</c> fails.
    /// </summary>
    [Theory]
    [MemberData(nameof(EveryThemeFile))]
    public void TheStockSurfaceTextAndAccentPairs_PassAaInEveryTheme(string[] relativePath)
    {
        var colors = StockColors(relativePath);

        var failing = ThemeColorOverrides.Slots
            .Where(s => s.Group != "Status")
            .Select(s => (s.Key, s.ContrastAgainst, Ratio: WcagContrast.Ratio(colors[s.Key], colors[s.ContrastAgainst])))
            .Where(t => WcagContrast.Band(t.Ratio) != ContrastBand.Pass)
            .Select(t => $"{t.Key} vs {t.ContrastAgainst} = {WcagContrast.Format(t.Ratio)}")
            .ToList();

        Assert.True(failing.Count == 0, $"{string.Join('/', relativePath)}: {string.Join("; ", failing)}");
    }

    /// <summary>Dark — the default theme — reads green on all twelve rows out of the box, status colors included.</summary>
    [Theory]
    [InlineData("Lite", "Themes", "DarkTheme.xaml")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Viewer", "Themes", "DarkTheme.xaml")]
    public void EveryStockPair_PassesAaInDark(params string[] relativePath)
    {
        var colors = StockColors(relativePath);

        foreach (var slot in ThemeColorOverrides.Slots)
        {
            var ratio = WcagContrast.Ratio(colors[slot.Key], colors[slot.ContrastAgainst]);
            Assert.True(WcagContrast.Band(ratio) == ContrastBand.Pass,
                $"{slot.Key} vs {slot.ContrastAgainst} = {WcagContrast.Format(ratio)}");
        }
    }

    private static Dictionary<string, Color> StockColors(string[] relativePath) =>
        ThemeXamlRewriter.DeclaredColors(ReadRepoFile(relativePath))
            .ToDictionary(kv => kv.Key, kv => { Assert.True(ThemeColorOverrides.TryParseHex(kv.Value, out var c)); return c; }, StringComparer.Ordinal);

    /* ================================================================================================
       The picker's math
       ================================================================================================ */

    [Theory]
    [InlineData(0xFF, 0x00, 0x00, 0, 1, 1)]
    [InlineData(0x00, 0xFF, 0x00, 120, 1, 1)]
    [InlineData(0x00, 0x00, 0xFF, 240, 1, 1)]
    [InlineData(0xFF, 0xFF, 0xFF, 0, 0, 1)]
    [InlineData(0x00, 0x00, 0x00, 0, 0, 0)]
    public void Hsv_DecomposesThePrimaries(int r, int g, int b, double hue, double saturation, double value)
    {
        var hsv = HsvColor.FromColor(Color.FromRgb((byte)r, (byte)g, (byte)b));

        Assert.Equal(hue, Math.Round(hsv.Hue, 6));
        Assert.Equal(saturation, Math.Round(hsv.Saturation, 6));
        Assert.Equal(value, Math.Round(hsv.Value, 6));
    }

    [Theory]
    [InlineData("#2eaef1")]
    [InlineData("#1E6FA8")]
    [InlineData("#E4E6EB")]
    [InlineData("#111217")]
    [InlineData("#FFD54F")]
    [InlineData("#81C784")]
    public void Hsv_RoundTripsThePaletteColors(string hex)
    {
        Assert.True(ThemeColorOverrides.TryParseHex(hex, out var color));

        var back = HsvColor.FromColor(color).ToColor();

        Assert.Equal(color, back);
    }

    /* ================================================================================================
       Wiring pins: both apps embed the text, set the path before the first Apply, host the panel
       ================================================================================================ */

    [Theory]
    [InlineData("Lite", "PerformanceMonitorLite.csproj")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Viewer", "PerformanceMonitor.Darling.Viewer.csproj")]
    public void EachApp_EmbedsItsThemeTextUnderTheNameTheLoaderAsksFor(params string[] relativePath)
    {
        var csproj = ReadRepoFile(relativePath);

        Assert.Contains("<EmbeddedResource Include=\"Themes\\*.xaml\" LogicalName=\"Themes/%(Filename).xaml\" />", csproj, StringComparison.Ordinal);

        /* The loader's name for a theme and MSBuild's expansion of the LogicalName must be the same string. */
        Assert.Equal("Themes/DarkTheme.xaml", ThemeManager.ThemeTextResourceName("Dark"));
        Assert.Equal("Themes/LightTheme.xaml", ThemeManager.ThemeTextResourceName("Light"));
        Assert.Equal("Themes/CoolBreezeTheme.xaml", ThemeManager.ThemeTextResourceName("CoolBreeze"));
    }

    /// <summary>
    /// The path and the log hooks must be set BEFORE the first Apply, or the first paint is the stock
    /// palette and the second is the operator's — and the watcher must be started, or an edit made through
    /// "Open file" waits for a restart.
    /// </summary>
    [Theory]
    [InlineData("Lite", "App.xaml.cs")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Viewer", "App.xaml.cs")]
    public void EachApp_ConfiguresTheOverridesFileBeforeItsFirstApply_AndWatchesIt(params string[] relativePath)
    {
        var source = ReadRepoFile(relativePath);

        var pathSet = source.IndexOf("ThemeManager.OverridesFilePath =", StringComparison.Ordinal);
        var firstApply = source.IndexOf("ThemeManager.Apply(", StringComparison.Ordinal);

        Assert.True(pathSet >= 0, "OverridesFilePath is never set.");
        Assert.True(firstApply >= 0, "ThemeManager.Apply is never called.");
        Assert.True(pathSet < firstApply, "OverridesFilePath must be set before the first ThemeManager.Apply.");
        Assert.Contains("ThemeColorOverrides.FileName", source, StringComparison.Ordinal);
        Assert.Contains("ThemeManager.LogWarning =", source, StringComparison.Ordinal);
        Assert.Contains("ThemeManager.WatchOverridesFile();", source, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("Lite", "Windows", "SettingsWindow.xaml")]
    [InlineData("Darling", "PerformanceMonitor.Darling.Viewer", "SettingsWindow.xaml")]
    public void EachSettingsWindow_HostsTheSharedColorsPanel_BesideTheThemeCombo(params string[] relativePath)
    {
        var xaml = ReadRepoFile(relativePath);

        var combo = xaml.IndexOf("x:Name=\"ColorThemeCombo\"", StringComparison.Ordinal);
        var panel = xaml.IndexOf("<ui:ThemeColorsPanel", StringComparison.Ordinal);

        Assert.True(combo >= 0);
        Assert.True(panel > combo, "The Colors panel should sit beneath the theme combo it edits.");
        Assert.Contains("xmlns:ui=\"clr-namespace:PerformanceMonitor.Ui;assembly=PerformanceMonitor.Ui\"", xaml, StringComparison.Ordinal);
    }

    /// <summary>
    /// The panel shares its host window's access-key scope, and both Settings windows have spent the
    /// natural letters — so its buttons carry no mnemonics at all. An underscore in a Button's Content
    /// here would register a key in a scope this test cannot see, so the pin is on the panel's XAML.
    /// </summary>
    [Fact]
    public void TheColorsPanel_CarriesNoAccessKeys()
    {
        var xaml = ReadRepoFile(new[] { "PerformanceMonitor.Ui", "ThemeColorsPanel.xaml" });

        foreach (Match m in Regex.Matches(xaml, @"<(?:Button|CheckBox|Expander|Label|ToggleButton)\b[^>]*\b(?:Content|Header)=""(?<text>[^""]*)"""))
        {
            Assert.DoesNotContain("_", m.Groups["text"].Value, StringComparison.Ordinal);
        }

        Assert.Contains("Content=\"Apply colors\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Content=\"Reset to default\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Content=\"Open file\"", xaml, StringComparison.Ordinal);
    }

    /* ================================================================================================
       Helpers
       ================================================================================================ */

    /// <summary>
    /// The dictionary key for a brush's <c>x:Key</c> text: a plain name, or the <see cref="SystemColors"/>
    /// resource key the grid-selection brushes are keyed by.
    /// </summary>
    private static object ResourceKey(string keyText)
    {
        var m = Regex.Match(keyText, @"^\{x:Static\s+SystemColors\.(?<name>\w+)\}$");
        if (!m.Success)
        {
            return keyText;
        }

        var property = typeof(SystemColors).GetProperty(m.Groups["name"].Value);
        Assert.True(property is not null, $"SystemColors has no property {m.Groups["name"].Value}.");
        return property!.GetValue(null)!;
    }

    private static string ReadRepoFile(string[] relativePath) =>
        File.ReadAllText(Path.Combine(new[] { RepoRoot() }.Concat(relativePath).ToArray()));

    private static string TempFile(string contents)
    {
        var directory = Path.Combine(Path.GetTempPath(), "pm-theme-overrides-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        var path = Path.Combine(directory, "theme-overrides.json");
        File.WriteAllText(path, contents);
        return path;
    }

    private static string RepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? throw new InvalidOperationException("PerformanceMonitor.sln not found above the test output directory.");
    }

    /// <summary>WPF objects require STA; same shape as <c>MainWindowAccessKeyTests</c>.</summary>
    private static T OnStaThread<T>(Func<T> body)
    {
        T result = default!;
        Exception? error = null;
        var thread = new Thread(() =>
        {
            try { result = body(); }
            catch (Exception ex) { error = ex; }
        });
        thread.SetApartmentState(ApartmentState.STA);
        thread.Start();
        thread.Join();

        if (error is not null)
        {
            throw error;
        }

        return result;
    }
}
