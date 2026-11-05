/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// A grid cell that opens a row's explanation (<c>details.disclosure.cell-detail</c>, #6154) sat about 4 px lower than the plain cells
/// beside it, because <c>.panel .disclosure { margin-top: 0.6rem }</c> (written for a disclosure under a panel) also matched it (#7482).
/// The first two tests resolve the shipped stylesheet's cascade for the element as the page builds it. The third runs the shipped table
/// script under Node (<c>web-cell-detail-harness.mjs</c>): opening a cell's detail scrolls it into view, because in a right-hand column
/// of a grid that scrolls sideways the opened text ran past the grid's edge. Skipped when Node is not installed.
/// </summary>
public sealed class WebCellDetailLayoutBehaviourTests
{
    // One element of the page, root first: tag and classes. The page is section.panel > div.table-wrap > table.data > tbody > tr > td > details.
    private sealed record El(string Tag, params string[] Classes);

    private static readonly El[] Cell =
    {
        new("main"), new("section", "panel"), new("div", "table-wrap"), new("table", "data"), new("tbody"), new("tr"), new("td"),
        new("details", "disclosure", "cell-detail"),
    };

    private static readonly El[] UnderAPanel =
    {
        new("main"), new("section", "panel"), new("details", "disclosure"),
    };

    private sealed record Rule(string[] Selector, (int Ids, int Classes, int Tags) Specificity, int Order, Dictionary<string, string> Declarations);

    private static readonly Regex Compound = new(@"^([a-zA-Z][a-zA-Z0-9]*)?((?:\.[A-Za-z0-9_-]+)*)$", RegexOptions.Compiled);

    /// <summary>
    /// The stylesheet's rules made of tags, classes, descendant and child combinators. A selector with an id, attribute, pseudo-class,
    /// pseudo-element or sibling combinator is left out: none of them can decide the margin of the elements these tests build.
    /// At-rules (media queries) are left out too.
    /// </summary>
    private static List<Rule> ParseRules(string css)
    {
        css = Regex.Replace(css, @"/\*.*?\*/", "", RegexOptions.Singleline);
        var rules = new List<Rule>();
        var i = 0;
        while (i < css.Length)
        {
            var open = css.IndexOf('{', i);
            if (open < 0) break;
            var depth = 1;
            var j = open + 1;
            while (j < css.Length && depth > 0)
            {
                if (css[j] == '{') depth++;
                else if (css[j] == '}') depth--;
                j++;
            }

            var prelude = css[i..open].Trim();
            var block = css[(open + 1)..(j - 1)];
            i = j;
            if (prelude.StartsWith('@')) continue;

            var decls = new Dictionary<string, string>();
            foreach (var d in block.Split(';'))
            {
                var colon = d.IndexOf(':');
                if (colon > 0) decls[d[..colon].Trim().ToLowerInvariant()] = d[(colon + 1)..].Trim();
            }

            foreach (var sel in prelude.Split(','))
            {
                var tokens = sel.Replace(">", " > ", StringComparison.Ordinal).Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
                if (tokens.Length == 0 || tokens.Any(t => t != ">" && !Compound.IsMatch(t))) continue;
                var classes = tokens.Where(t => t != ">").Sum(t => t.Count(c => c == '.'));
                var tags = tokens.Where(t => t != ">").Count(t => char.IsLetter(t[0]));
                rules.Add(new Rule(tokens, (0, classes, tags), rules.Count, decls));
            }
        }

        return rules;
    }

    private static bool Matches(string compound, El e)
    {
        var m = Compound.Match(compound);
        if (m.Groups[1].Success && !string.Equals(m.Groups[1].Value, e.Tag, StringComparison.OrdinalIgnoreCase)) return false;
        return m.Groups[2].Value.Split('.', StringSplitOptions.RemoveEmptyEntries).All(c => e.Classes.Contains(c));
    }

    private static bool Matches(string[] sel, int ti, El[] chain, int ci)
    {
        if (!Matches(sel[ti], chain[ci])) return false;
        if (ti == 0) return true;
        if (sel[ti - 1] == ">") return ci > 0 && Matches(sel, ti - 2, chain, ci - 1);
        for (var k = ci - 1; k >= 0; k--)
        {
            if (Matches(sel, ti - 1, chain, k)) return true;
        }

        return false;
    }

    /// <summary>The margin-top the cascade gives the last element of the chain: the matching rule with the highest specificity, the later one on a tie.</summary>
    private static string MarginTop(El[] chain)
    {
        var rules = ParseRules(Wwwroot("css", "app.css"));
        string? value = null;
        (int, int, int, int) best = (-1, -1, -1, -1);
        foreach (var r in rules)
        {
            if (!Matches(r.Selector, r.Selector.Length - 1, chain, chain.Length - 1)) continue;
            string? v = null;
            if (r.Declarations.TryGetValue("margin-top", out var mt)) v = mt;
            else if (r.Declarations.TryGetValue("margin", out var m)) v = m.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0];
            if (v is null) continue;
            var key = (r.Specificity.Ids, r.Specificity.Classes, r.Specificity.Tags, r.Order);
            if (key.CompareTo(best) <= 0) continue;
            best = key;
            value = v;
        }

        return value ?? "0";
    }

    private static string Wwwroot(params string[] parts) =>
        ReadRepoFile(new[] { "Darling", "PerformanceMonitor.Darling.Service", "wwwroot" }.Concat(parts).ToArray());

    private static bool IsZero(string v) => double.TryParse(new string(v.TakeWhile(c => char.IsDigit(c) || c == '.').ToArray()), NumberStyles.Float, CultureInfo.InvariantCulture, out var n) && n == 0;

    [Fact]
    public void ADisclosureInAGridCell_HasNoTopMargin_SoItsTextLinesUpWithTheCellsBesideIt()
    {
        var margin = MarginTop(Cell);
        Assert.True(IsZero(margin), "a cell's disclosure resolved margin-top " + margin + " (the cell centres its content, so a margin moves the text down)");
    }

    [Fact]
    public void ADisclosureUnderAPanel_KeepsItsGapAboveIt()
    {
        var margin = MarginTop(UnderAPanel);
        Assert.False(IsZero(margin), "a disclosure placed under a panel lost its gap above it: " + margin);
    }

    private static JsonElement Run()
    {
        var psi = new ProcessStartInfo("node") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        psi.ArgumentList.Add(PathTo("Darling", "Darling.Tests", "web-cell-detail-harness.mjs"));
        psi.ArgumentList.Add(PathTo("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js"));

        Process proc;
        try
        {
            proc = Process.Start(psi)!;
        }
        catch (Win32Exception)
        {
            Assert.Skip("Node is not installed, so the shipped table script cannot be run.");
            return default;
        }

        using (proc)
        {
            var error = proc.StandardError.ReadToEndAsync();
            var output = proc.StandardOutput.ReadToEnd().Trim();
            if (!proc.WaitForExit(20000))
            {
                proc.Kill(entireProcessTree: true);
                Assert.Fail("the cell detail harness did not finish in 20 s");
            }

            Assert.True(proc.ExitCode == 0, "the cell detail harness failed: " + error.Result);
            using var doc = JsonDocument.Parse(output.Split('\n')[0]);
            return doc.RootElement.Clone();
        }
    }

    [Fact]
    public void OpeningACellDetail_ScrollsItIntoView_AndClosingItDoesNot()
    {
        var r = Run();
        Assert.Equal(1, r.GetProperty("detailBoxes").GetInt32());
        Assert.True(r.GetProperty("isCellDetail").GetBoolean());
        Assert.Equal(0, r.GetProperty("scrollsWhenClosed").GetInt32());
        Assert.Equal(1, r.GetProperty("scrollsWhenOpened").GetInt32());
        var args = r.GetProperty("scrollArgs");
        Assert.Equal("nearest", args.GetProperty("inline").GetString());
        Assert.Equal("nearest", args.GetProperty("block").GetString());
    }
}
