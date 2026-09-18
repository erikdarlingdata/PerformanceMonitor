/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Windows;
using System.Windows.Markup;
using System.Windows.Media;
using System.Windows.Threading;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Manages the application color theme (Dark / Light / CoolBreeze) by swapping the merged resource
    /// dictionary at runtime — and, since #3577 (arm B), applies the operator's per-theme color overrides
    /// from <c>theme-overrides.json</c> on the way in.
    ///
    /// <para><b>How an override reaches the screen.</b> A theme with no overrides loads exactly as it always
    /// has: the compiled dictionary at <c>pack://application:,,,/Themes/{Theme}Theme.xaml</c>. A theme WITH
    /// overrides is regenerated instead: the same file's XAML text (each app embeds its three theme files a
    /// second time as plain text for this) has its overridden <c>&lt;Color&gt;</c> declarations rewritten
    /// by <see cref="ThemeXamlRewriter"/> and is parsed with <see cref="XamlReader"/>, so every brush and
    /// every style setter inside the dictionary resolves against the new value. The class remarks on
    /// <see cref="ThemeXamlRewriter"/> explain why it has to be the text and not a merged override
    /// dictionary or an in-place brush edit. Either way the result is ONE dictionary in
    /// <c>Application.Current.Resources.MergedDictionaries</c>, and every <c>{DynamicResource}</c> in the
    /// app repaints — the same live swap the theme combo has always done, with no restart.</para>
    ///
    /// <para><b>Nothing here can take the app down.</b> The overrides file is read before the first window
    /// exists. A missing or unreadable file, an unknown key, a bad value, a theme text that fails to parse
    /// — each is reported through <see cref="LogWarning"/> and the theme falls back to the compiled
    /// dictionary for that Apply. An operator who breaks the file gets the stock theme and a log line, not
    /// a crash at startup.</para>
    ///
    /// <para><b>Opt-in per app.</b> An app that never sets <see cref="OverridesFilePath"/> (the deprecated
    /// Dashboard, the tests) gets the pre-#3577 behaviour exactly: compiled dictionary, no file, no
    /// watcher.</para>
    /// </summary>
    public static class ThemeManager
    {
        /// <summary>The currently active theme name ("Dark", "Light" or "CoolBreeze").</summary>
        public static string CurrentTheme { get; private set; } = "Dark";

        /// <summary>Returns true when the active theme uses a light background (Light or CoolBreeze).</summary>
        public static bool HasLightBackground => CurrentTheme != "Dark";

        /// <summary>Fired after the theme dictionary has been swapped — on every Apply, including a re-apply
        /// with new color overrides, so a listener that caches theme-derived values recomputes them.</summary>
        public static event Action<string>? ThemeChanged;

        /* ---------------------------------------------------------------------------------------------
           #3577 arm B: per-user color overrides.
           --------------------------------------------------------------------------------------------- */

        /// <summary>
        /// The per-user overrides file (<c>theme-overrides.json</c>) — set by each app at startup, BEFORE
        /// its first <see cref="Apply"/>, to a path under its own settings directory (Lite: beside
        /// settings.json; the Darling viewer: beside viewer-settings.json). Null leaves the feature off.
        /// </summary>
        public static string? OverridesFilePath { get; set; }

        /// <summary>Where the loader reports a problem with the file or a theme text. Null discards.</summary>
        public static Action<string>? LogWarning { get; set; }

        /// <summary>Where the loader notes what it did (a regeneration and its cost, a reload from disk). Null discards.</summary>
        public static Action<string>? LogInfo { get; set; }

        /// <summary>
        /// The source of a theme's XAML TEXT, by theme name — the input to a regeneration. Defaults to the
        /// embedded copy in the running app's assembly (<c>Themes/{Theme}Theme.xaml</c>, embedded by each
        /// app's project file). Replaceable so the loader can be exercised against a file on disk or a
        /// string, which is what the tests do; there is no other seam into the regeneration.
        ///
        /// <para>Swapping the provider empties <see cref="StockPalette"/>'s cache. The cache is keyed by theme
        /// name and the palette it holds was read through whichever provider was current at the time, so a
        /// test that installed its own text and then asked for the stock palette would otherwise get the
        /// previous provider's answer back, silently (review note on #3606). In the app the provider is set
        /// once and the clear never runs.</para>
        /// </summary>
        public static Func<string, string?> ThemeXamlTextProvider
        {
            get => s_themeXamlTextProvider;
            set
            {
                s_themeXamlTextProvider = value;
                s_stockPalettes.Clear();
            }
        }

        private static Func<string, string?> s_themeXamlTextProvider = DefaultThemeXamlText;

        /// <summary>The overrides currently loaded from <see cref="OverridesFilePath"/> — what the last Apply used.</summary>
        public static ThemeColorOverrideSet Overrides { get; private set; } = ThemeColorOverrideSet.Empty;

        /// <summary>
        /// How the watcher re-reads the file after an outside edit. <see cref="File.ReadAllText(string)"/> in
        /// the app; replaceable so a test can make the read fail the way a real disk does
        /// (<see cref="UnauthorizedAccessException"/>, <see cref="IOException"/>) and prove the failure is a
        /// log line and not a crash. This is the only seam into the reload path.
        /// </summary>
        public static Func<string, string> OverridesFileReader { get; set; } = File.ReadAllText;

        /// <summary>
        /// How many consecutive <see cref="IOException"/>s the watcher treats as "the editor is still
        /// writing" before it gives up on this change and says so. Each retry is another debounce interval
        /// away; a file still locked after two seconds is a different problem from a save in progress.
        /// </summary>
        public const int MaxReloadRetries = 5;

        private static bool s_overridesLoaded;
        private static FileSystemWatcher? s_watcher;
        private static DispatcherTimer? s_reloadDebounce;
        private static int s_reloadRetries;
        private static readonly Dictionary<string, IReadOnlyDictionary<string, Color>> s_stockPalettes =
            new(StringComparer.Ordinal);

        /// <summary>
        /// Applies the specified theme by replacing the top-level merged resource dictionary.
        /// All DynamicResource bindings in XAML update automatically.
        /// </summary>
        public static void Apply(string theme)
        {
            CurrentTheme = theme;

            var dictionary = BuildDictionary(theme);

            var dictionaries = Application.Current.Resources.MergedDictionaries;
            dictionaries.Clear();
            dictionaries.Add(dictionary);

            ThemeChanged?.Invoke(theme);
        }

        /// <summary>
        /// Re-reads <see cref="OverridesFilePath"/> and re-applies the current theme. What the Settings
        /// window calls after it writes the file, and what the watcher calls after someone else does.
        /// </summary>
        public static void ReloadOverridesAndApply()
        {
            LoadOverrides();
            Apply(CurrentTheme);
        }

        /// <summary>
        /// Writes <paramref name="colors"/> as <paramref name="theme"/>'s block of the overrides file and
        /// re-applies. An empty set removes the block (Reset to default). Returns false when nothing reached
        /// disk — already reported through <see cref="LogWarning"/> — in which case nothing was re-applied
        /// either, so the screen keeps saying what the file says.
        /// </summary>
        public static bool SaveOverridesAndApply(string theme, IReadOnlyDictionary<string, Color> colors)
        {
            if (OverridesFilePath is null)
            {
                LogWarning?.Invoke("No overrides file is configured for this app, so the colors were not saved.");
                return false;
            }

            var written = ThemeColorOverrides.SaveTheme(OverridesFilePath, theme, colors, LogWarning);
            if (written is null)
            {
                return false;
            }

            ReloadOverridesAndApply();
            return true;
        }

        /// <summary>
        /// Makes sure the overrides file exists so "Open file" has something to open, writing a skeleton
        /// with an empty block for <paramref name="theme"/> when it does not. Returns the path, or null when
        /// the feature is off or the file could not be created.
        /// </summary>
        public static string? EnsureOverridesFileExists(string theme)
        {
            if (OverridesFilePath is null)
            {
                return null;
            }

            if (File.Exists(OverridesFilePath))
            {
                return OverridesFilePath;
            }

            try
            {
                var directory = Path.GetDirectoryName(OverridesFilePath);
                if (!string.IsNullOrEmpty(directory))
                {
                    Directory.CreateDirectory(directory);
                }

                /* An empty block for the current theme rather than a bare {} — it shows the shape (theme
                   name, then color keys) to someone opening the file cold, and an empty block applies
                   nothing. The README lists the keys; JSON has no comments to list them here. */
                File.WriteAllText(OverridesFilePath, "{\n  \"" + theme + "\": {\n  }\n}\n");
                LoadOverrides();
                return OverridesFilePath;
            }
            catch (Exception ex)
            {
                LogWarning?.Invoke($"'{OverridesFilePath}' could not be created ({ex.GetType().Name}: {ex.Message}).");
                return null;
            }
        }

        /// <summary>
        /// The theme's stock palette — the twelve exposed colors as the theme file declares them, before any
        /// override. What the Settings rows show as the default and what Reset returns to. Read from the
        /// embedded text; falls back to the compiled dictionary when the text is unavailable.
        /// </summary>
        public static IReadOnlyDictionary<string, Color> StockPalette(string theme)
        {
            var name = NormalizeThemeName(theme);
            if (s_stockPalettes.TryGetValue(name, out var cached))
            {
                return cached;
            }

            var palette = new Dictionary<string, Color>(StringComparer.Ordinal);
            var text = TryGetThemeXamlText(name);
            if (text is not null)
            {
                foreach (var (key, hex) in ThemeXamlRewriter.DeclaredColors(text))
                {
                    if (ThemeColorOverrides.IsExposed(key) && ThemeColorOverrides.TryParseHex(hex, out var color))
                    {
                        palette[key] = color;
                    }
                }
            }

            if (palette.Count < ThemeColorOverrides.ExposedKeys.Count)
            {
                try
                {
                    var compiled = new ResourceDictionary { Source = ThemeUri(name) };
                    foreach (var key in ThemeColorOverrides.ExposedKeys)
                    {
                        if (!palette.ContainsKey(key) && compiled[key] is Color color)
                        {
                            palette[key] = color;
                        }
                    }
                }
                catch (Exception ex)
                {
                    LogWarning?.Invoke($"The {name} theme's stock palette could not be read ({ex.GetType().Name}: {ex.Message}).");
                }
            }

            s_stockPalettes[name] = palette;
            return palette;
        }

        /// <summary>
        /// Watches <see cref="OverridesFilePath"/> and re-applies the current theme when someone edits the
        /// file outside the app — the "Open file" button hands the operator an editor, and what they save
        /// there should show up without a restart. Debounced, because an editor's save is several
        /// filesystem events; and a write that produces the text already loaded (our own save, an edit that
        /// changed nothing) is recognised by content and ignored, so the app's own writes never re-apply
        /// twice. Call once, from the UI thread, after the first Apply. A no-op when the feature is off.
        /// </summary>
        public static void WatchOverridesFile()
        {
            if (OverridesFilePath is null || s_watcher is not null)
            {
                return;
            }

            try
            {
                var directory = Path.GetDirectoryName(OverridesFilePath);
                if (string.IsNullOrEmpty(directory))
                {
                    return;
                }

                Directory.CreateDirectory(directory);

                var dispatcher = Application.Current?.Dispatcher ?? Dispatcher.CurrentDispatcher;
                var watcher = new FileSystemWatcher(directory, Path.GetFileName(OverridesFilePath))
                {
                    NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.Size | NotifyFilters.FileName | NotifyFilters.CreationTime,
                };

                FileSystemEventHandler onChange = (_, _) => dispatcher.BeginInvoke(ScheduleReloadFromDisk);
                watcher.Changed += onChange;
                watcher.Created += onChange;
                watcher.Deleted += onChange;
                watcher.Renamed += (_, _) => dispatcher.BeginInvoke(ScheduleReloadFromDisk);
                watcher.EnableRaisingEvents = true;
                s_watcher = watcher;
            }
            catch (Exception ex)
            {
                /* A watcher is a convenience. Without it the file still applies on the next Apply or
                   restart, so a failure to start one is a log line and nothing else. */
                LogWarning?.Invoke($"'{OverridesFilePath}' will not be watched for outside edits ({ex.GetType().Name}: {ex.Message}); " +
                                   "changes made in an editor apply on the next Apply or restart.");
            }
        }

        /// <summary>
        /// The dictionary for <paramref name="theme"/> with the current overrides applied: the compiled
        /// dictionary when there are none, or when regeneration is not possible; the regenerated one
        /// otherwise. Public so a caller can build a themed dictionary without touching
        /// <c>Application.Current</c> — the seam the loader tests use.
        /// </summary>
        public static ResourceDictionary BuildDictionary(string theme)
        {
            EnsureOverridesLoaded();

            var name = NormalizeThemeName(theme);
            var overrides = Overrides.For(name);
            if (overrides.Count == 0)
            {
                return new ResourceDictionary { Source = ThemeUri(name) };
            }

            var regenerated = TryBuildRegenerated(name, overrides);
            return regenerated ?? new ResourceDictionary { Source = ThemeUri(name) };
        }

        /// <summary>
        /// The regeneration: theme text → rewritten text → parsed dictionary. Null, with the reason
        /// reported, when any step cannot complete — the caller falls back to the compiled dictionary.
        /// Separate from <see cref="BuildDictionary"/> so a test can drive it with its own text and
        /// overrides and inspect the result.
        /// </summary>
        public static ResourceDictionary? TryBuildRegenerated(string theme, IReadOnlyDictionary<string, Color> overrides)
        {
            ArgumentNullException.ThrowIfNull(overrides);

            var text = TryGetThemeXamlText(theme);
            if (text is null)
            {
                LogWarning?.Invoke($"The {theme} theme's XAML text is not embedded in this app, so its {overrides.Count} color " +
                                   "override(s) cannot be applied; the stock theme is used.");
                return null;
            }

            return TryParseRegenerated(theme, text, overrides);
        }

        /// <summary>
        /// The last two steps of the regeneration over caller-supplied text (the tests hand it the theme
        /// files from the source tree). Never throws.
        /// </summary>
        public static ResourceDictionary? TryParseRegenerated(string theme, string themeXaml, IReadOnlyDictionary<string, Color> overrides)
        {
            ArgumentNullException.ThrowIfNull(themeXaml);
            ArgumentNullException.ThrowIfNull(overrides);

            var stopwatch = Stopwatch.StartNew();
            var rewritten = ThemeXamlRewriter.Rewrite(themeXaml, overrides, LogWarning);

            try
            {
                if (XamlReader.Parse(rewritten) is ResourceDictionary dictionary)
                {
                    LogInfo?.Invoke($"{theme} theme regenerated with {overrides.Count} color override(s) in {stopwatch.ElapsedMilliseconds} ms.");
                    return dictionary;
                }

                LogWarning?.Invoke($"The {theme} theme's XAML text did not parse to a ResourceDictionary, so its color overrides " +
                                   "cannot be applied; the stock theme is used.");
                return null;
            }
            catch (Exception ex)
            {
                /* XamlParseException carries a line and position; the message includes them. Everything
                   else (a type that failed to load, a bad value that got past the rewriter) is worth the
                   same sentence: the overrides did not apply, the stock theme did, and here is why. */
                LogWarning?.Invoke($"The {theme} theme could not be regenerated with its color overrides ({ex.GetType().Name}: " +
                                   $"{ex.Message}); the stock theme is used.");
                return null;
            }
        }

        /// <summary>The compiled dictionary's pack URI for a theme — the pre-#3577 load path.</summary>
        public static Uri ThemeUri(string theme) => theme switch
        {
            "Light"      => new Uri("pack://application:,,,/Themes/LightTheme.xaml"),
            "CoolBreeze" => new Uri("pack://application:,,,/Themes/CoolBreezeTheme.xaml"),
            _            => new Uri("pack://application:,,,/Themes/DarkTheme.xaml")
        };

        /// <summary>
        /// The manifest resource name each app embeds its theme text under
        /// (<c>&lt;EmbeddedResource Include="Themes\*.xaml" LogicalName="Themes/%(Filename).xaml"/&gt;</c>).
        /// </summary>
        public static string ThemeTextResourceName(string theme) => "Themes/" + NormalizeThemeName(theme) + "Theme.xaml";

        /// <summary>The three names the file and the combos use; anything else is Dark, as <see cref="ThemeUri"/> has always treated it.</summary>
        private static string NormalizeThemeName(string theme) =>
            theme is "Light" or "CoolBreeze" ? theme : "Dark";

        private static void EnsureOverridesLoaded()
        {
            if (!s_overridesLoaded)
            {
                LoadOverrides();
            }
        }

        /// <summary>Re-reads the file into <see cref="Overrides"/> without applying. Internal for the reload tests.</summary>
        internal static void LoadOverrides()
        {
            s_overridesLoaded = true;
            Overrides = OverridesFilePath is null
                ? ThemeColorOverrideSet.Empty
                : ThemeColorOverrides.Load(OverridesFilePath, LogWarning);
        }

        private static string? TryGetThemeXamlText(string theme)
        {
            try
            {
                return ThemeXamlTextProvider(theme);
            }
            catch (Exception ex)
            {
                LogWarning?.Invoke($"The {theme} theme's XAML text could not be read ({ex.GetType().Name}: {ex.Message}).");
                return null;
            }
        }

        private static string? DefaultThemeXamlText(string theme)
        {
            var assembly = Application.Current?.GetType().Assembly ?? Assembly.GetEntryAssembly();
            using var stream = assembly?.GetManifestResourceStream(ThemeTextResourceName(theme));
            if (stream is null)
            {
                return null;
            }

            using var reader = new StreamReader(stream);
            return reader.ReadToEnd();
        }

        private static void ScheduleReloadFromDisk()
        {
            if (s_reloadDebounce is null)
            {
                s_reloadDebounce = new DispatcherTimer { Interval = TimeSpan.FromMilliseconds(400) };
                s_reloadDebounce.Tick += (_, _) =>
                {
                    s_reloadDebounce.Stop();

                    /* The last line of defence. A Tick handler runs on the dispatcher with no caller of ours
                       above it, so anything that escapes here is an unhandled exception on the UI thread —
                       an app crash over a file watcher, which the class header promises cannot happen.
                       ReloadFromDiskIfChanged catches its own read failures; this catch is for whatever
                       nobody anticipated in the rest of the path (the fingerprint compare, the re-apply). */
                    try
                    {
                        ReloadFromDiskIfChanged();
                    }
                    catch (Exception ex)
                    {
                        LogWarning?.Invoke($"'{OverridesFilePath}' changed on disk but the {CurrentTheme} theme could not be re-applied " +
                                           $"({ex.GetType().Name}: {ex.Message}); the theme stays as it was.");
                    }
                };
            }

            s_reloadDebounce.Stop();
            s_reloadDebounce.Start();
        }

        /// <summary>What one pass of the watcher's reload did. Returned so a test can see it; the app ignores it.</summary>
        internal enum ReloadOutcome
        {
            /// <summary>No file is configured.</summary>
            NothingToDo,

            /// <summary>The read failed with an <see cref="IOException"/> and another attempt is scheduled.</summary>
            Retrying,

            /// <summary>The read failed and will not be retried; a warning was logged and the theme is unchanged.</summary>
            Failed,

            /// <summary>The file's text is what is already loaded (our own write, or an edit that changed nothing).</summary>
            Unchanged,

            /// <summary>The file changed; the overrides were reloaded and the theme re-applied.</summary>
            Reapplied,
        }

        /// <summary>
        /// One pass of the watcher's reload: read the file, compare it to what is loaded, re-apply if it
        /// differs. Never throws for a read that fails — the watcher is a convenience, the file still applies
        /// on the next Apply or restart, and this runs on the dispatcher's timer with no caller to catch for
        /// it. Internal so a test can drive it with a failing <see cref="OverridesFileReader"/> and no
        /// watcher, no dispatcher and no window.
        /// </summary>
        internal static ReloadOutcome ReloadFromDiskIfChanged()
        {
            if (OverridesFilePath is null)
            {
                return ReloadOutcome.NothingToDo;
            }

            string? text = null;
            if (File.Exists(OverridesFilePath))
            {
                try
                {
                    text = OverridesFileReader(OverridesFilePath);
                }
                catch (IOException) when (s_reloadRetries < MaxReloadRetries)
                {
                    /* Mid-write: the editor still holds the file. Try again shortly rather than reporting a
                       file that is about to be fine — but not forever; a file held open for seconds is a
                       different problem, and the next real change will retry anyway. */
                    s_reloadRetries++;
                    ScheduleReloadFromDisk();
                    return ReloadOutcome.Retrying;
                }
                catch (Exception ex)
                {
                    /* Retries exhausted, or a failure that is not a mid-write at all: an ACL that changed
                       under us, an AV scan or a sync client holding the file in a way that surfaces as
                       UnauthorizedAccessException rather than IOException. File.Exists said yes and the read
                       said no, and the read is the one that knows. The watcher is a convenience — the file
                       still applies on the next Apply or restart — so this is a log line and the theme stays
                       exactly as it was. Every other read this class makes catches the same way; the
                       narrower catch this used to be was the one path that could take the app down. */
                    s_reloadRetries = 0;
                    LogWarning?.Invoke($"'{OverridesFilePath}' changed on disk but could not be re-read ({ex.GetType().Name}: {ex.Message}); " +
                                       $"the {CurrentTheme} theme stays as it was. It applies on the next Apply or restart.");
                    return ReloadOutcome.Failed;
                }
            }

            s_reloadRetries = 0;
            if (string.Equals(text, Overrides.SourceText, StringComparison.Ordinal))
            {
                return ReloadOutcome.Unchanged; // our own write, or an edit that changed nothing
            }

            LogInfo?.Invoke($"'{OverridesFilePath}' changed on disk; re-applying the {CurrentTheme} theme.");
            ReloadOverridesAndApply();
            return ReloadOutcome.Reapplied;
        }
    }
}
