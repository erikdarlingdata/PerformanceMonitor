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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3045 / #3031: on every page that builds one, a rollup tile's NUMBER is programmatically tied to the text
/// that says what it counts, so a consumer reaching the number's node on its own gets the figure and its
/// meaning together. Reading order alone leaves that relationship inferable but not determinable.
///
/// <para><b>Why the population is derived rather than listed.</b> #3038 wired the fleet rollup and left the
/// AG rollup — the only other page with the same tile — reading proximity-only. A check that asserted the
/// fleet tiles carry a description would have stayed green through all of it: a positive-only check cannot
/// find a site nobody remembered, which is the whole reason that page survived #3031. So the modules under
/// test are found by scanning the shipped <c>wwwroot/js</c> for rollup figures, and every figure found must
/// carry the relationship. A page that grows a rollup tomorrow is swept in without this file being edited,
/// and there is a floor under every enumeration — an empty match is the one way a check like this reads as
/// clean while proving nothing.</para>
///
/// <para><b>Why the label rides in <c>aria-describedby</c>.</b> <c>.num</c> is a plain <c>div</c>. ARIA
/// prohibits NAMING <c>role=generic</c>, so a name set there via <c>aria-label</c> / <c>aria-labelledby</c>
/// is invalid and may be dropped, while a description is a supported property on it. That reasoning is
/// page-independent, so it is pinned here rather than re-argued per page.</para>
///
/// <para>These are source pins. This repository carries no JavaScript test runner (see
/// <see cref="FleetPageAttentionFilterTests"/>), so a relationship built in a browser module is out of reach
/// of any assertion about a C# object; the modules are located by walking up from this file's compile-time
/// path, the same pattern. Behaviour under a DOM shim, and the mutations each assertion below reds on, are in
/// the PR.</para>
/// </summary>
public sealed class RollupTileDescriptionTests
{
    /// <summary>An <c>el("div", { … class: "num" … })</c> construction: a rollup tile's figure. Scoped to a
    /// <c>div</c> so the compose grid's numeric <c>td.num</c> cell — a table cell whose meaning comes from its
    /// column header, not from a sibling label — is not swept in.</summary>
    private static readonly Regex NumberFigure = new(
        @"el\(\s*""div""\s*,\s*\{[^{}]*\bclass:\s*""num""[^{}]*\}", RegexOptions.Compiled);

    /// <summary>A rollup tile container: the figure's parent, and the thing a second construction path would
    /// have to build.</summary>
    private static readonly Regex TileContainer = new(
        @"el\(\s*""div""\s*,\s*\{[^{}]*\bclass:\s*""tile", RegexOptions.Compiled);

    /// <summary>A CALL to the shared id derivation. The lookbehind keeps its own definition out.</summary>
    private static readonly Regex IdDerivationCall = new(
        @"(?<!function\s)\brollupTextId\(", RegexOptions.Compiled);

    /// <summary>The derivation's definition, wherever it lives.</summary>
    private static readonly Regex IdDerivationDefinition = new(
        @"function\s+rollupTextId\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// Every rollup figure carries a description, on every module that builds one. The set of modules is
    /// derived from the source, so this covers a page nobody thought to add here.
    /// </summary>
    [Fact]
    public void EveryRollupFigure_CarriesADescription()
    {
        var modules = WebModules();
        var pages = new List<string>();
        var undescribed = new List<string>();
        var figures = 0;

        foreach (var (path, text) in modules.OrderBy(m => m.Key, StringComparer.Ordinal))
        {
            var found = NumberFigure.Matches(text);
            if (found.Count == 0)
            {
                continue;
            }

            pages.Add(path);
            foreach (var figure in found.Cast<Match>())
            {
                figures++;
                if (!figure.Value.Contains(@"""aria-describedby"":", StringComparison.Ordinal))
                {
                    undescribed.Add($"{path}: {Collapse(figure.Value)}");
                }
            }
        }

        /* The floor. Two pages build rollup tiles today and each builds its figure in one shared closure, so
           a sweep that comes back with fewer than two figures found nothing rather than found them wired. */
        Assert.True(figures >= 2, $"expected at least 2 rollup figures under wwwroot/js, found {figures}");
        Assert.Contains("pages/fleet.js", pages);
        Assert.Contains("pages/ag.js", pages);

        Assert.Empty(undescribed);
    }

    /// <summary>
    /// One construction path per page, so no tile can be built beside the wired one. Both rollups build every
    /// tile from a single closure — AG's three, fleet's seven — so each page constructs exactly one tile
    /// container and exactly one figure. A hand-rolled second tile is how a fourth AG tile arrives outside
    /// the helper, and it reds here whether or not whoever wrote it remembered a description.
    /// </summary>
    [Fact]
    public void EveryRollupPage_BuildsEveryTileFromOneClosure()
    {
        var modules = WebModules();
        var tilePages = new List<string>();

        foreach (var (path, text) in modules.OrderBy(m => m.Key, StringComparer.Ordinal))
        {
            var tiles = TileContainer.Matches(text).Count;
            var figures = NumberFigure.Matches(text).Count;
            if (tiles == 0 && figures == 0)
            {
                continue;
            }

            /* Neither half may exist without the other: a tile with no figure, or a figure built outside a
               tile, means the two enumerations above have stopped describing the same construct and the
               sweep in the sibling test is measuring something else. */
            Assert.True(tiles == 1 && figures == 1,
                $"{path}: expected exactly one rollup tile construction and one figure, found {tiles} and {figures}");
            tilePages.Add(path);
        }

        Assert.True(tilePages.Count >= 2, $"expected at least 2 rollup pages, found {tilePages.Count}");
        Assert.Contains("pages/fleet.js", tilePages);
        Assert.Contains("pages/ag.js", tilePages);
    }

    /// <summary>
    /// The AG page's tiles, enumerated from source: three of them, and the number's description names the id
    /// its own label carries. Three is asserted as floor AND ceiling, so a regex that stops matching cannot
    /// read as three wired tiles.
    /// </summary>
    [Fact]
    public void TheAgNumbers_DescribeThemselvesWithTheirLabelsId()
    {
        var ag = WebModules()["pages/ag.js"];

        var calls = Regex.Matches(ag, @"(?<![\w.])tile\(").Count;
        Assert.Equal(3, calls);
        foreach (var label in new[] { "Groups", "Reporting servers", "Views" })
        {
            Assert.Matches(@"(?<![\w.])tile\([^)]*""" + Regex.Escape(label) + @"""", ag);
        }

        /* Both halves of the reference, so it resolves rather than pointing at a plausible-looking string:
           the figure describes itself with the id the label element is given, in the same closure. */
        Assert.Contains(
            @"el(""div"", { class: ""num"", text: fmtInt(num), ""aria-describedby"": lblId })",
            ag, StringComparison.Ordinal);
        Assert.Contains(@"el(""div"", { class: ""lbl"", id: lblId, text: lbl })", ag, StringComparison.Ordinal);

        /* The scope decision, pinned. The fleet tile takes a sub-line and a title because that rollup
           qualifies one of its numbers with coverage and hovers a long note; these three counts qualify
           nothing and hover over nothing, so a second id would point at an element that does not exist.
           `subId` must not arrive here by resemblance to the model it was copied from. */
        Assert.DoesNotContain("subId", ag, StringComparison.Ordinal);
        Assert.DoesNotContain(@"""sub""", ag, StringComparison.Ordinal);
    }

    /// <summary>
    /// The id derivation exists once, in <c>util.js</c>, and every page that wires a figure imports it from
    /// there. A local copy would be written without the de-duplication rule below — correct on a page whose
    /// labels all differ, silently wrong the day a tile repeats one — so the shape is pinned, not just the
    /// behaviour.
    /// </summary>
    [Fact]
    public void TheIdDerivation_ExistsExactlyOnce_AndEveryFigurePageImportsIt()
    {
        var modules = WebModules();

        var definitions = modules
            .Where(m => IdDerivationDefinition.IsMatch(m.Value))
            .Select(m => m.Key)
            .OrderBy(k => k, StringComparer.Ordinal)
            .ToArray();
        Assert.Equal(new[] { "util.js" }, definitions);
        Assert.Contains("export function rollupTextId(lbl, part, used) {", modules["util.js"], StringComparison.Ordinal);

        var importers = 0;
        foreach (var (path, text) in modules.Where(m => !string.Equals(m.Key, "util.js", StringComparison.Ordinal)))
        {
            if (!NumberFigure.IsMatch(text))
            {
                continue;
            }

            Assert.Matches(@"import \{[^}]*\brollupTextId\b[^}]*\} from ""\.\./util\.js"";", text);
            importers++;
        }

        Assert.True(importers >= 2, $"expected at least 2 pages importing the shared derivation, found {importers}");
    }

    /// <summary>
    /// The shared derivation de-duplicates within one render. Two tiles sharing a label would emit a
    /// duplicate id, and <c>aria-describedby</c> resolves a duplicate to the FIRST match — a silent
    /// mis-association rather than a visible break. AG's three labels all differ, so nothing on that page
    /// exercises the collision loop today: it is inert there and load-bearing the day a tile repeats a label,
    /// which is precisely why it must not be a rule that each page keeps its own copy of.
    /// </summary>
    [Fact]
    public void TheSharedDerivation_DeDuplicatesWithinARender()
    {
        var util = WebModules()["util.js"];

        Assert.Contains(@"for (let n = 2; used.has(id); n++) id = base + ""-"" + n;", util, StringComparison.Ordinal);
        Assert.Contains("used.add(id);", util, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every call site hands the derivation a set, and the page creates that set per render. The collision
    /// loop above cannot fire on a call that drops the argument — it would throw on <c>used.has</c> — so the
    /// argument is not optional and is pinned at the call, not just at the definition.
    /// </summary>
    [Fact]
    public void EveryIdDerivationCall_IsHandedTheRendersUsedIdSet()
    {
        var modules = WebModules();
        var calls = 0;

        foreach (var (path, text) in modules.OrderBy(m => m.Key, StringComparer.Ordinal))
        {
            var sites = IdDerivationCall.Matches(text).Cast<Match>().ToArray();
            if (sites.Length == 0)
            {
                continue;
            }

            foreach (var call in sites)
            {
                var args = CallArguments(text, call.Index + call.Length - 1);
                Assert.True(args.Count == 3,
                    $"{path}: rollupTextId takes (label, part, usedIds); this call passes {args.Count}: {Collapse(call.Value + string.Join(", ", args) + ")")}");
                calls++;
            }

            /* Per RENDER, not per module: a set hoisted to module scope would accumulate across the page's
               60s re-render and start suffixing ids that never collided. */
            Assert.Contains("const usedIds = new Set();", text, StringComparison.Ordinal);
        }

        Assert.True(calls >= 3, $"expected at least 3 id derivations across wwwroot/js, found {calls}");
    }

    /// <summary>
    /// The shipped browser modules, keyed by their path under <c>wwwroot/js</c>, newlines normalised so a
    /// multi-line anchor holds whether the checkout gave them CRLF (.gitattributes says it does) or LF.
    /// </summary>
    private static IReadOnlyDictionary<string, string> WebModules()
    {
        var dir = LocateRepoDirectory(Path.Combine(
            "Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        var modules = Directory.EnumerateFiles(dir, "*.js", SearchOption.AllDirectories)
            .ToDictionary(
                f => Path.GetRelativePath(dir, f).Replace('\\', '/'),
                f => File.ReadAllText(f).Replace("\r\n", "\n", StringComparison.Ordinal),
                StringComparer.Ordinal);

        /* A floor under the enumeration itself. A moved wwwroot or a broken glob has to fail here rather than
           leave every sweep above passing over an empty set. */
        Assert.True(modules.Count >= 10, $"expected the shipped module set under {dir}, found {modules.Count}");
        Assert.Contains("util.js", modules.Keys);
        Assert.Contains("pages/fleet.js", modules.Keys);
        Assert.Contains("pages/ag.js", modules.Keys);

        return modules;
    }

    /// <summary>The top-level arguments of the call whose opening paren sits at <paramref name="open"/>.</summary>
    private static IReadOnlyList<string> CallArguments(string text, int open)
    {
        var args = new List<string>();
        var depth = 0;
        var start = open + 1;

        for (var i = open; i < text.Length; i++)
        {
            var c = text[i];
            if (c is '(' or '[' or '{')
            {
                depth++;
            }
            else if (c is ')' or ']' or '}')
            {
                depth--;
                if (depth == 0)
                {
                    args.Add(text[start..i].Trim());
                    return args;
                }
            }
            else if (c == ',' && depth == 1)
            {
                args.Add(text[start..i].Trim());
                start = i + 1;
            }
        }

        throw new InvalidOperationException($"unbalanced call argument list at offset {open}");
    }

    /// <summary>One line, for an assertion message that names the offending construction.</summary>
    private static string Collapse(string s) => Regex.Replace(s, @"\s+", " ").Trim();

    private static string LocateRepoDirectory(string relative, [CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, relative);
            if (Directory.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new DirectoryNotFoundException($"Could not locate {relative} walking up from {thisFile}");
    }
}
