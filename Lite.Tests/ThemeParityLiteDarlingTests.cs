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
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Xml.Linq;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Cross-app theme-parity guard for the two ACTIVE apps: Lite and the Darling viewer. Their WPF
/// brush/color dictionaries are NOT shared — each ships its own <c>Themes/{Dark,Light,CoolBreeze}Theme.xaml</c>,
/// hand-synced with no enforcement. An existing <c>ThemeParityTests</c> guards Dashboard↔Lite, but it lives
/// in <c>Dashboard.Tests</c>, which no CI workflow builds or runs — pure decoration — and it never looks at
/// Darling. Nothing at all pins Lite↔Darling, so a color tweak to one silently drifts the go-forward pair
/// apart. This test closes that hole and lives in Lite.Tests, which BOTH build.yml and nightly.yml run.
///
/// For each theme it parses the <c>x:Key -&gt; color</c> map from both apps' same-theme file and asserts
/// that every key present in BOTH (the intersection) resolves to the same color. Keys that exist in only
/// one app are allowed and ignored — Darling legitimately adds 8 viewer-only brushes (CardBorder + the
/// Viewer* set), and either app may add app-specific resources without tripping the guard.
///
/// RATCHET: <see cref="KnownColorDivergences"/> is the escape hatch for an intentional, documented color
/// divergence. It is EMPTY today because the shared palette is byte-for-byte identical across the two apps;
/// if a deliberate divergence is ever introduced, grandfather it there (with a comment) to keep the build
/// green while the drift stays visible — same shrinking-to-do discipline as the coverage pin.
///
/// Text-parses the XAML (no WPF apartment needed) and locates both projects via
/// <see cref="CallerFilePathAttribute"/>.
/// </summary>
public sealed class ThemeParityLiteDarlingTests
{
    private static readonly XNamespace X = "http://schemas.microsoft.com/winfx/2006/xaml";

    /// <summary>
    /// Intentional, documented color divergences to ignore, keyed as <c>"{Theme}:{Key}"</c> (e.g.
    /// <c>"Dark:AccentBrush"</c>). Empty — Lite and the Darling viewer share an identical palette today.
    /// Add an entry ONLY for a deliberate divergence, with a comment explaining why the two apps differ.
    /// </summary>
    private static readonly HashSet<string> KnownColorDivergences = new(StringComparer.Ordinal)
    {
        // (empty) -- the Lite and Darling shared palette is identical.
    };

    [Theory]
    [InlineData("Dark")]
    [InlineData("Light")]
    [InlineData("CoolBreeze")]
    public void SharedThemeKeys_HaveIdenticalColors_AcrossLiteAndDarling(string theme)
    {
        var liteFile = Path.Combine(LiteThemesDir(), $"{theme}Theme.xaml");
        var darlingFile = Path.Combine(DarlingThemesDir(), $"{theme}Theme.xaml");
        Assert.True(File.Exists(liteFile), $"Lite theme file not found: {liteFile}");
        Assert.True(File.Exists(darlingFile), $"Darling viewer theme file not found: {darlingFile}");

        var lite = LoadColorMap(liteFile);
        var darling = LoadColorMap(darlingFile);

        var shared = lite.Keys
            .Intersect(darling.Keys)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToList();

        // Guard against a parser regression (e.g. a XAML namespace change) silently turning this into a
        // vacuous pass: the two apps share dozens of palette keys today, so an empty intersection is a bug.
        Assert.True(shared.Count > 0,
            $"No shared color/brush keys parsed for the {theme} theme — the parser or the theme files changed shape.");

        var mismatches = shared
            .Where(k => !KnownColorDivergences.Contains($"{theme}:{k}"))
            .Where(k => !string.Equals(lite[k], darling[k], StringComparison.Ordinal))
            .Select(k => $"  {k}: Lite={lite[k]}  Darling={darling[k]}")
            .ToList();

        Assert.True(mismatches.Count == 0,
            $"{theme} theme: {mismatches.Count} shared color/brush key(s) differ between " +
            $"Lite\\Themes\\{theme}Theme.xaml and the Darling viewer's Themes\\{theme}Theme.xaml. Re-sync " +
            "the two palettes (or, if the divergence is intentional, add \"" + theme + ":<Key>\" to " +
            "KnownColorDivergences with a comment):\n" + string.Join("\n", mismatches));
    }

    /// <summary>
    /// Parses a theme ResourceDictionary into a map of x:Key -> normalized #AARRGGBB color. Handles the two
    /// resource forms used in these files: standalone &lt;Color x:Key="..."&gt;#hex&lt;/Color&gt; and
    /// &lt;SolidColorBrush x:Key="..." Color="#hex | {StaticResource SomeColor}"/&gt;, resolving a brush's
    /// {StaticResource} reference back to the underlying &lt;Color&gt; hex. Entries whose color cannot be
    /// resolved to a concrete value are omitted. First definition wins.
    /// </summary>
    private static Dictionary<string, string> LoadColorMap(string xamlPath)
    {
        var doc = XDocument.Load(xamlPath);

        // Pass 1: literal <Color> resources.
        var colors = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var el in doc.Descendants().Where(e => e.Name.LocalName == "Color"))
        {
            var key = (string?)el.Attribute(X + "Key");
            if (key is null || colors.ContainsKey(key))
            {
                continue;
            }
            colors[key] = el.Value.Trim();
        }

        // Pass 2: <SolidColorBrush> resources (Color is an unprefixed attribute, so no namespace).
        var brushes = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var el in doc.Descendants().Where(e => e.Name.LocalName == "SolidColorBrush"))
        {
            var key = (string?)el.Attribute(X + "Key");
            var color = (string?)el.Attribute("Color");
            if (key is null || color is null || brushes.ContainsKey(key))
            {
                continue;
            }
            brushes[key] = color.Trim();
        }

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var pair in colors)
        {
            var norm = NormalizeColor(pair.Value);
            if (norm is not null)
            {
                map[pair.Key] = norm;
            }
        }
        foreach (var pair in brushes)
        {
            var resolved = ResolveColor(pair.Value, colors);
            var norm = resolved is null ? null : NormalizeColor(resolved);
            if (norm is not null)
            {
                map[pair.Key] = norm;
            }
        }
        return map;
    }

    private static readonly Regex StaticResourceRef =
        new(@"^\{StaticResource\s+([^}]+)\}$", RegexOptions.Compiled);

    /// <summary>
    /// Resolves a brush Color value to a literal color token, following one or more {StaticResource X} hops
    /// into the &lt;Color&gt; table. Returns null when the chain dead-ends at a key that is not a &lt;Color&gt;.
    /// </summary>
    private static string? ResolveColor(string value, Dictionary<string, string> colors, int depth = 0)
    {
        if (depth > 10)
        {
            return null;
        }

        var trimmed = value.Trim();
        var match = StaticResourceRef.Match(trimmed);
        if (!match.Success)
        {
            return trimmed;
        }

        var refKey = match.Groups[1].Value.Trim();
        return colors.TryGetValue(refKey, out var inner)
            ? ResolveColor(inner, colors, depth + 1)
            : null;
    }

    /// <summary>
    /// Normalizes a color token to canonical upper-case #AARRGGBB so equivalent spellings (#RGB, #ARGB,
    /// #RRGGBB, #AARRGGBB — WPF treats a missing alpha as fully opaque) compare equal. Non-hex named colors
    /// (e.g. White, Transparent) are upper-cased as-is so they still compare. Returns null for empty input.
    /// </summary>
    private static string? NormalizeColor(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        var c = raw.Trim();
        var match = Regex.Match(c, "^#([0-9A-Fa-f]+)$");
        if (!match.Success)
        {
            return c.ToUpperInvariant();
        }

        var h = match.Groups[1].Value.ToUpperInvariant();
        h = h.Length switch
        {
            3 => "FF" + string.Concat(h.Select(ch => $"{ch}{ch}")), // #RGB -> #FFRRGGBB
            4 => string.Concat(h.Select(ch => $"{ch}{ch}")),        // #ARGB -> #AARRGGBB
            6 => "FF" + h,                                          // #RRGGBB -> #FFRRGGBB
            _ => h,
        };

        // Anything that did not land on a clean 8-digit ARGB (e.g. a malformed 5/7-digit token) falls back
        // to the upper-cased original so it still compares deterministically rather than being dropped.
        return h.Length == 8 ? "#" + h : c.ToUpperInvariant();
    }

    /// <summary>Lite's Themes directory, resolved from this test file's compile-time path (Lite.Tests is a
    /// sibling of the Lite project).</summary>
    private static string LiteThemesDir([CallerFilePath] string thisFile = "")
    {
        var testDir = Path.GetDirectoryName(thisFile)!;
        return Path.GetFullPath(Path.Combine(testDir, "..", "Lite", "Themes"));
    }

    /// <summary>The Darling viewer's Themes directory, resolved from this test file's compile-time path
    /// (Lite.Tests and Darling both sit at the repo root).</summary>
    private static string DarlingThemesDir([CallerFilePath] string thisFile = "")
    {
        var testDir = Path.GetDirectoryName(thisFile)!;
        return Path.GetFullPath(Path.Combine(testDir, "..", "Darling", "PerformanceMonitor.Darling.Viewer", "Themes"));
    }
}
