/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Windows.Media;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Turns a theme dictionary's XAML text plus a set of color overrides into the XAML text of the SAME
/// dictionary with those <c>&lt;Color&gt;</c> declarations rewritten — the generated dictionary
/// <see cref="ThemeManager.Apply"/> parses when a theme carries overrides (#3577, arm B).
///
/// <para><b>Why the whole dictionary is regenerated rather than a small override dictionary merged over
/// it.</b> The obvious design — load the compiled theme, then merge a second dictionary that redefines
/// <c>AccentColor</c> and <c>AccentBrush</c> — reaches only the consumers that look the key up at runtime:
/// the ~700 <c>{DynamicResource}</c> references in the app's own XAML and the code-behind
/// <c>FindResource</c> calls. It does NOT reach the theme dictionary's own interior, and that interior is
/// where the app's look actually lives: every one of its 51–59 brushes is declared
/// <c>Color="{StaticResource XColor}"</c>, and every one of its control styles and templates — Window,
/// TextBlock, Button, TabItem, DataGrid, ComboBox, ScrollBar, Menu — sets its brushes with
/// <c>{StaticResource XBrush}</c> (184–200 such references per file against two <c>DynamicResource</c>).
/// WPF resolves a <c>StaticResource</c> inside a compiled dictionary at load, to the OBJECT: the Window
/// style's Background setter holds the very <c>SolidColorBrush</c> instance <c>BackgroundBrush</c> resolved
/// to, and a later dictionary that redefines the key changes what the key means, not what the setter
/// holds. Merging an override dictionary would have recolored the DynamicResource surfaces and left every
/// styled control in the stock palette — a two-tone app.</para>
///
/// <para>Mutating the loaded brushes in place does not work either: values realised from a dictionary that
/// belongs to <c>Application.Resources</c> are sealed on the way out (<c>ResourceDictionary.SealValue</c>
/// freezes any <c>Freezable</c> once the dictionary has an application owner), and a brush referenced from
/// a style setter is frozen again when the style seals. Setting <c>Color</c> on either throws.</para>
///
/// <para>So the only place a color override can be applied and reach everything is BEFORE the dictionary
/// is parsed — in the text. Each app embeds its three theme files a second time as plain text (the
/// compiled BAML stays the fast path for a theme with no overrides), this class rewrites the twelve
/// <c>&lt;Color x:Key="…"&gt;</c> declarations that carry overrides, and <see cref="ThemeManager"/> parses
/// the result with <c>XamlReader.Parse</c>. Every <c>{StaticResource XColor}</c> in the file — the brushes,
/// the two <c>ColorAnimation To=</c> targets, anything added later — resolves against the rewritten value,
/// because it is resolved inside the dictionary being built. There is no color→brush map to maintain, and
/// nothing for one to fall out of step with; <see cref="BrushDependencies"/> derives the map from the text
/// only so the tests can prove the regenerated dictionary honours every dependency the XAML declares.</para>
///
/// <para>The cost is one runtime XAML parse of a ~1,400-line dictionary per Apply on a theme with
/// overrides — a fraction of a second, paid at startup and on the Apply button, never per frame.</para>
/// </summary>
public static class ThemeXamlRewriter
{
    /// <summary>
    /// A palette color declaration: <c>&lt;Color x:Key="AccentColor"&gt;#2eaef1&lt;/Color&gt;</c>. Tolerates
    /// the whitespace the files actually vary in; requires the element form (the themes never use the
    /// attribute-shorthand form for colors, and a test pins that all eighteen match this).
    /// </summary>
    public static readonly Regex ColorDeclaration = new(
        @"<Color\s+x:Key=""(?<key>[A-Za-z_][A-Za-z0-9_]*)""\s*>\s*(?<value>[^<]*?)\s*</Color>",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// A brush declared from a palette color: <c>&lt;SolidColorBrush x:Key="AccentBrush"
    /// Color="{StaticResource AccentColor}" …/&gt;</c>. The key may be a plain name or an
    /// <c>{x:Static SystemColors.…Key}</c> (the grid-selection brushes), so the key group is whatever sits
    /// inside the quotes.
    /// </summary>
    public static readonly Regex BrushFromColor = new(
        @"<SolidColorBrush\s+x:Key=""(?<brush>[^""]+)""[^>]*?Color=""\{StaticResource\s+(?<color>[A-Za-z_][A-Za-z0-9_]*)\s*\}""",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    /// <summary>
    /// The theme's <c>&lt;Color&gt;</c> declarations as written: key → the hex text between the tags.
    /// The stock palette, read from the same text the regeneration rewrites.
    /// </summary>
    public static Dictionary<string, string> DeclaredColors(string xaml)
    {
        ArgumentNullException.ThrowIfNull(xaml);

        var colors = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match m in ColorDeclaration.Matches(xaml))
        {
            colors[m.Groups["key"].Value] = m.Groups["value"].Value;
        }

        return colors;
    }

    /// <summary>
    /// Every brush the theme derives from a palette color: brush key → color key. Derived from the XAML,
    /// never hand-listed, so the tests that use it cannot rot ahead of the files.
    /// </summary>
    public static Dictionary<string, string> BrushDependencies(string xaml)
    {
        ArgumentNullException.ThrowIfNull(xaml);

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match m in BrushFromColor.Matches(xaml))
        {
            map[m.Groups["brush"].Value] = m.Groups["color"].Value;
        }

        return map;
    }

    /// <summary>
    /// The theme text with each overridden <c>&lt;Color&gt;</c> declaration's value replaced. Keys that are
    /// not in the text are reported through <paramref name="warn"/> and skipped (it cannot happen for the
    /// twelve exposed keys, which a test pins in all six files, but the rewriter should not be the thing
    /// that silently does nothing when it does). Only the first declaration of a key is rewritten — the
    /// files declare each once, and a second declaration would be a StaticResource-hygiene defect the
    /// theme tests already refuse.
    /// </summary>
    public static string Rewrite(string xaml, IReadOnlyDictionary<string, Color> overrides, Action<string>? warn)
    {
        ArgumentNullException.ThrowIfNull(xaml);
        ArgumentNullException.ThrowIfNull(overrides);

        var text = xaml;
        foreach (var (key, color) in overrides)
        {
            var declaration = new Regex(
                @"(<Color\s+x:Key=""" + Regex.Escape(key) + @"""\s*>)\s*[^<]*?\s*(</Color>)",
                RegexOptions.CultureInvariant);

            var hex = ThemeColorOverrides.ToHex(color);
            if (!declaration.IsMatch(text))
            {
                warn?.Invoke($"The theme declares no <Color x:Key=\"{key}\">, so the override {hex} for it has nowhere to land and is skipped.");
                continue;
            }

            text = declaration.Replace(text, m => m.Groups[1].Value + hex + m.Groups[2].Value, count: 1);
        }

        return text;
    }
}
