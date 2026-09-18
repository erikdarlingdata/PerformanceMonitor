/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Windows.Media;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// One user-adjustable palette color (#3577): the theme dictionary key it overrides, the group the Settings
/// window files it under, the words the row shows, and the color it is measured against.
/// </summary>
/// <param name="Key">The <c>&lt;Color x:Key="…"&gt;</c> in every theme dictionary, spelled exactly.</param>
/// <param name="Group">The Settings-window group header: Backgrounds, Text, Accent, Rows or Status.</param>
/// <param name="Label">The short row label.</param>
/// <param name="Meaning">One line on what the color paints, for the row's tooltip and the README.</param>
/// <param name="ContrastAgainst">The key this color is measured against in the readout — the surface it
/// most often renders on (a background's text, a text tone's page, an accent's ink, a status color's
/// page). Every slot has one; the readout is the point of the row.</param>
public sealed record ThemeColorSlot(string Key, string Group, string Label, string Meaning, string ContrastAgainst);

/// <summary>
/// The per-user color overrides for the three themes — the closed list of what may be overridden, the
/// on-disk file that holds the overrides, and the parse rules for both (#3577, arm B).
///
/// <para><b>Why a closed list.</b> Each theme dictionary declares 18 <c>Color</c>s and derives 51–59
/// <c>SolidColorBrush</c>es and every control style from them. Twelve of the eighteen are exposed here:
/// the three page surfaces, the two text tones, the accent and its ink, the alternating-row tint, and the
/// four status colors. That is the PALETTE level, and it is deliberately where the scope stops. The other
/// six — <c>AccentHoverColor</c>, <c>AccentPressedColor</c>, <c>BackgroundLighterColor</c>,
/// <c>ForegroundMutedColor</c>, <c>BorderColor</c>, <c>BorderLightColor</c> — are secondary tones each
/// theme derives from a primary one (a hover is the accent lightened a step, a border is the background
/// lifted a step, muted text is dim text quieted a step), and exposing them invites the operator to pull a
/// derived tone away from the tone it derives from, which is how a hover state ends up darker than the
/// rest state. They follow their primaries as the theme author tuned them.</para>
///
/// <para>The list is also the answer to the request that will come next: "can I change just the selected
/// tab?" That is the STYLE layer — a setter inside one control template — and it is not on offer. A palette
/// key is a color with a name that every surface in the app agrees on; a style-layer knob is a per-control
/// exception that the next theme change has to remember. Arm (A) of this issue (PR #3589) fixed the selected
/// tab by giving the accent an INK key that every accent surface shares, not by special-casing the tab, and
/// the same discipline holds here: the operator adjusts the accent and its ink, and every accent surface
/// follows.</para>
///
/// <para><b>Per theme, not global.</b> The base palettes differ — Dark's accent is a sky blue on
/// near-black, Cool Breeze's a steel blue on white — so one override set cannot suit all three. The file is
/// keyed by theme name first, then by color key, and a Dark override never touches Light.</para>
///
/// <para><b>The file never takes the app down.</b> It is read at startup before the first window exists.
/// A missing file is a first run and is silent. A malformed one, an unknown theme name, an unknown color
/// key or an unparseable value is reported through <see cref="ThemeManager.LogWarning"/> and skipped, and
/// everything else in the file still applies — the same absent/unreadable discipline
/// <see cref="SettingsFileGuard"/> gives settings.json (#2425), reused here rather than re-invented.</para>
/// </summary>
public static class ThemeColorOverrides
{
    /// <summary>The file name under each app's per-user settings directory.</summary>
    public const string FileName = "theme-overrides.json";

    /// <summary>The theme names the file may be keyed by — the same three <see cref="ThemeManager.Apply"/> accepts.</summary>
    public static readonly IReadOnlyList<string> ThemeNames = new[] { "Dark", "Light", "CoolBreeze" };

    /// <summary>
    /// The twelve palette colors an operator may override, in the order the Settings window lists them,
    /// with the group, the words and the contrast partner for each. This is THE list: the loader, the
    /// Settings window, the README and the tests all read it from here.
    /// </summary>
    public static readonly IReadOnlyList<ThemeColorSlot> Slots = new[]
    {
        new ThemeColorSlot("BackgroundColor", "Backgrounds", "Page",
            "The window and panel background most text sits on.", "ForegroundColor"),
        new ThemeColorSlot("BackgroundLightColor", "Backgrounds", "Raised panel",
            "Cards, settings sections, buttons at rest, headers — one step lighter than the page.", "ForegroundColor"),
        new ThemeColorSlot("BackgroundDarkColor", "Backgrounds", "Sidebar / well",
            "The sidebar, grid rows and other recessed areas — one step darker than the page.", "ForegroundColor"),

        new ThemeColorSlot("ForegroundColor", "Text", "Text",
            "Primary text: values, headers, labels.", "BackgroundColor"),
        new ThemeColorSlot("ForegroundDimColor", "Text", "Secondary text",
            "Captions and secondary labels, a step quieter than primary text.", "BackgroundColor"),

        new ThemeColorSlot("AccentColor", "Accent", "Accent",
            "The selected tab, the selected grid row, highlighted combo items, accent buttons, links.", "AccentForegroundColor"),
        new ThemeColorSlot("AccentForegroundColor", "Accent", "Text on accent",
            "The ink for text and glyphs that sit on an accent fill (added by #3589 so it could be measured).", "AccentColor"),

        new ThemeColorSlot("AlternatingRowColor", "Rows", "Alternating row",
            "Every other grid row.", "ForegroundColor"),

        new ThemeColorSlot("SuccessColor", "Status", "Success",
            "Healthy / OK markers and the Done row mark.", "BackgroundColor"),
        new ThemeColorSlot("WarningColor", "Status", "Warning",
            "Warning markers and the To Do row mark.", "BackgroundColor"),
        new ThemeColorSlot("ErrorColor", "Status", "Error",
            "Error / critical markers and the Do Not Do row mark.", "BackgroundColor"),
        new ThemeColorSlot("InfoColor", "Status", "Info",
            "Informational markers.", "BackgroundColor"),
    };

    /// <summary>The twelve keys, in <see cref="Slots"/> order. The closed set the loader honors.</summary>
    public static readonly IReadOnlyList<string> ExposedKeys = Slots.Select(s => s.Key).ToArray();

    /// <summary>
    /// The six palette colors that exist in every theme and are deliberately NOT exposed — derived tones
    /// that follow a primary (see the class remarks). Named here so a test can prove the exclusion is a
    /// choice: all eighteen exist in every theme file, twelve are offered, these six are withheld.
    /// </summary>
    public static readonly IReadOnlyList<string> WithheldKeys = new[]
    {
        "AccentHoverColor",
        "AccentPressedColor",
        "BackgroundLighterColor",
        "ForegroundMutedColor",
        "BorderColor",
        "BorderLightColor",
    };

    private static readonly HashSet<string> s_exposed = new(ExposedKeys, StringComparer.Ordinal);
    private static readonly HashSet<string> s_themes = new(ThemeNames, StringComparer.Ordinal);

    private static readonly JsonSerializerOptions s_writeOptions = new() { WriteIndented = true };

    /// <summary>The slot for a key, or null when the key is not one of the twelve.</summary>
    public static ThemeColorSlot? SlotFor(string key) => Slots.FirstOrDefault(s => s.Key == key);

    /// <summary>True when <paramref name="key"/> is one of the twelve exposed keys.</summary>
    public static bool IsExposed(string key) => s_exposed.Contains(key);

    /// <summary>True when <paramref name="theme"/> is one of the three theme names.</summary>
    public static bool IsKnownTheme(string theme) => s_themes.Contains(theme);

    /// <summary>
    /// Parses the hex forms the file accepts: <c>#RRGGBB</c> and <c>#AARRGGBB</c>, case-insensitive, the
    /// hash optional. Nothing else — no named colors, no <c>#RGB</c> shorthand — because the file is
    /// written back in <c>#RRGGBB</c> and a value that round-trips into a different spelling is a value the
    /// operator will not recognise as theirs.
    /// </summary>
    public static bool TryParseHex(string? text, out Color color)
    {
        color = default;
        if (string.IsNullOrWhiteSpace(text))
        {
            return false;
        }

        var hex = text.Trim();
        if (hex.StartsWith('#'))
        {
            hex = hex[1..];
        }

        if (hex.Length != 6 && hex.Length != 8)
        {
            return false;
        }

        if (!uint.TryParse(hex, NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var value))
        {
            return false;
        }

        if (hex.Length == 6)
        {
            color = Color.FromRgb((byte)(value >> 16), (byte)(value >> 8), (byte)value);
        }
        else
        {
            color = Color.FromArgb((byte)(value >> 24), (byte)(value >> 16), (byte)(value >> 8), (byte)value);
        }

        return true;
    }

    /// <summary>
    /// <c>#RRGGBB</c>, upper-case; <c>#AARRGGBB</c> only when the alpha is not fully opaque. This is the
    /// spelling the file is written in and the spelling the Settings rows display.
    /// </summary>
    public static string ToHex(Color color)
    {
        return color.A == 255
            ? string.Format(CultureInfo.InvariantCulture, "#{0:X2}{1:X2}{2:X2}", color.R, color.G, color.B)
            : string.Format(CultureInfo.InvariantCulture, "#{0:X2}{1:X2}{2:X2}{3:X2}", color.A, color.R, color.G, color.B);
    }

    /// <summary>
    /// Reads the overrides file into a per-theme map. Never throws.
    ///
    /// <para>An absent file returns an empty set in silence. Anything else that is wrong is said once
    /// through <paramref name="warn"/> and skipped at the smallest grain that makes sense: a malformed
    /// document costs the whole file, an unknown theme block costs that block, an unknown key or a bad value
    /// costs that one entry. The operator gets every override the file managed to say.</para>
    /// </summary>
    /// <param name="path">The file to read.</param>
    /// <param name="warn">Where problems are reported; null discards them (tests).</param>
    public static ThemeColorOverrideSet Load(string path, Action<string>? warn)
    {
        var read = SettingsFileGuard.Read(path);
        if (read.State == SettingsFileState.Absent)
        {
            return ThemeColorOverrideSet.Empty;
        }

        if (read.State == SettingsFileState.Unreadable || read.Root is null)
        {
            warn?.Invoke($"'{path}' could not be read ({read.Problem}), so no color overrides are applied " +
                         "this session. The file has not been changed; fix it, or Reset to default in Settings " +
                         "to replace it (the unreadable original is copied aside first).");
            return new ThemeColorOverrideSet(
                new Dictionary<string, Dictionary<string, Color>>(StringComparer.Ordinal), read.Text);
        }

        return FromJson(read.Root, path, warn, read.Text);
    }

    /// <summary>The parse behind <see cref="Load"/>, over an already-parsed document (tests feed it text).</summary>
    /// <param name="root">The document.</param>
    /// <param name="path">The file it came from, for the warnings.</param>
    /// <param name="warn">Where problems are reported; null discards them.</param>
    /// <param name="sourceText">The raw text the document was parsed from, carried on the set so the file
    /// watcher can tell a real change from a write that reproduced what is already loaded.</param>
    public static ThemeColorOverrideSet FromJson(JsonObject root, string path, Action<string>? warn, string? sourceText = null)
    {
        ArgumentNullException.ThrowIfNull(root);

        var themes = new Dictionary<string, Dictionary<string, Color>>(StringComparer.Ordinal);

        foreach (var (themeName, themeNode) in root)
        {
            if (!IsKnownTheme(themeName))
            {
                warn?.Invoke($"'{path}': '{themeName}' is not a theme (the themes are {string.Join(", ", ThemeNames)}); " +
                             "that block is ignored.");
                continue;
            }

            if (themeNode is not JsonObject themeBlock)
            {
                warn?.Invoke($"'{path}': the '{themeName}' block is not a JSON object of color keys; it is ignored.");
                continue;
            }

            var colors = new Dictionary<string, Color>(StringComparer.Ordinal);
            foreach (var (key, valueNode) in themeBlock)
            {
                if (!IsExposed(key))
                {
                    warn?.Invoke($"'{path}': '{themeName}.{key}' is not an adjustable color (the adjustable ones are " +
                                 $"{string.Join(", ", ExposedKeys)}); it is ignored.");
                    continue;
                }

                var text = valueNode is JsonValue v && v.TryGetValue<string>(out var s) ? s : null;
                if (!TryParseHex(text, out var color))
                {
                    warn?.Invoke($"'{path}': '{themeName}.{key}' is '{valueNode?.ToJsonString() ?? "null"}', which is not a " +
                                 "#RRGGBB or #AARRGGBB color; it is ignored.");
                    continue;
                }

                colors[key] = color;
            }

            if (colors.Count > 0)
            {
                themes[themeName] = colors;
            }
        }

        return new ThemeColorOverrideSet(themes, sourceText);
    }

    /// <summary>
    /// Writes <paramref name="colors"/> as the <paramref name="theme"/> block of the file, replacing that
    /// block and leaving every other block exactly as it was. An empty <paramref name="colors"/> REMOVES the
    /// block — that is Reset to default. Returns the text written, or null when nothing reached disk (the
    /// failure has already been reported through <paramref name="warn"/>).
    ///
    /// <para>Read-modify-write over the document rather than a serialize of the in-memory set, so a block
    /// the loader skipped with a warning (an unknown theme name, say) survives a save it had nothing to do
    /// with. An unreadable file is copied aside before it is replaced, through
    /// <see cref="SettingsFileGuard.RootForWrite"/> — the same rule settings.json follows (#2425).</para>
    /// </summary>
    public static string? SaveTheme(string path, string theme, IReadOnlyDictionary<string, Color> colors, Action<string>? warn)
    {
        ArgumentNullException.ThrowIfNull(colors);

        var write = SettingsFileGuard.RootForWrite(path, DateTime.Now);
        if (write.Problem is not null)
        {
            if (write.QuarantinedTo is null)
            {
                /* The guard's contract: unreadable AND uncopyable means do not write. The file is the only
                   record of whatever the operator put there, and replacing it now would be the last event in
                   its life. Leaving it alone beats replacing it when the alternative is permanent. */
                warn?.Invoke($"'{path}' could not be read ({write.Problem}) and no copy of it could be made, so it has " +
                             "been left untouched and the colors were not saved. Fix the file, or move it aside by " +
                             "hand, and try again.");
                return null;
            }

            warn?.Invoke($"'{path}' could not be read ({write.Problem}), so this save replaces it. The original was " +
                         $"copied to '{Path.GetFileName(write.QuarantinedTo)}' first.");
        }

        var root = write.Root;
        if (colors.Count == 0)
        {
            root.Remove(theme);
        }
        else
        {
            var block = new JsonObject();
            foreach (var key in ExposedKeys)
            {
                if (colors.TryGetValue(key, out var color))
                {
                    block[key] = ToHex(color);
                }
            }

            root[theme] = block;
        }

        var text = root.ToJsonString(s_writeOptions);
        try
        {
            var directory = Path.GetDirectoryName(path);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            File.WriteAllText(path, text);
            return text;
        }
        catch (Exception ex)
        {
            warn?.Invoke($"'{path}' could not be written ({ex.GetType().Name}: {ex.Message}), so the colors were not saved.");
            return null;
        }
    }
}

/// <summary>
/// The parsed contents of theme-overrides.json: theme name → color key → color. Immutable; the loader hands
/// one out and <see cref="ThemeManager"/> holds the current one.
/// </summary>
public sealed class ThemeColorOverrideSet
{
    private static readonly IReadOnlyDictionary<string, Color> s_none =
        new Dictionary<string, Color>(StringComparer.Ordinal);

    private readonly IReadOnlyDictionary<string, Dictionary<string, Color>> _themes;

    /// <summary>No overrides for any theme — an absent file.</summary>
    public static readonly ThemeColorOverrideSet Empty =
        new(new Dictionary<string, Dictionary<string, Color>>(StringComparer.Ordinal), null);

    internal ThemeColorOverrideSet(IReadOnlyDictionary<string, Dictionary<string, Color>> themes, string? sourceText)
    {
        _themes = themes;
        SourceText = sourceText;
    }

    /// <summary>
    /// The raw file text this set was read from; null for an absent file. The file watcher compares the
    /// text on disk against this, so the app's own save (which reloads and so refreshes it) and an edit
    /// that changed nothing are both recognised as "already applied" rather than re-applied.
    /// </summary>
    public string? SourceText { get; }

    /// <summary>The overrides for one theme; empty when the theme has none.</summary>
    public IReadOnlyDictionary<string, Color> For(string theme) =>
        _themes.TryGetValue(theme, out var colors) ? colors : s_none;

    /// <summary>True when at least one theme carries at least one override.</summary>
    public bool Any => _themes.Count > 0;
}
