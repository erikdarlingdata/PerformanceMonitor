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
using System.Xml.Linq;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A <c>Grid.Row</c> or <c>Grid.Column</c> past the end of the grid's own definitions (#3049).
///
/// <para><b>Nothing else catches it.</b> The XAML compiles, BAML is produced, the build is green and the
/// window opens. WPF clamps an out-of-range index to the last available line rather than throwing, so the
/// control lands in a cell that already has a tenant and the two draw on top of each other — a note and
/// the grid it describes sharing one row, which looks like a rendering glitch and never like an index
/// arithmetic mistake. #3049 was <c>Grid.Row="10"</c> in a ten-row grid, and it survived review because
/// the row count is a list of twelve near-identical lines fifty lines away from the assignment.</para>
///
/// <para><b>Why the assertion is the RANGE and not the collision.</b> Two controls in one cell is the
/// symptom, but it is also a deliberate and pervasive idiom here: an empty-state <c>TextBlock</c> over the
/// grid it replaces, a <c>LoadingOverlay</c> over its chart, a comparison grid over the grid it swaps in
/// for. Thirty-one of those sit in the shipped XAML, all visibility-toggled and all correct, so a
/// collision rule would be an exemption table with thirty-one entries and no teeth. An index past the
/// definitions has no legitimate use at all — the clamp is never what the author meant — so this rule
/// carries no exemptions, and it should not acquire any.</para>
///
/// <para>Every <c>.xaml</c> in the tree is scanned, including <c>deprecated/</c>, because those projects
/// are still in <c>PerformanceMonitor.sln</c> and their markup clamps identically. Only DIRECT children of
/// a <c>Grid</c> are checked: an attached <c>Grid.Row</c> on something nested inside a <c>StackPanel</c> is
/// inert, and reading it as an assignment against the enclosing grid would invent offenders.</para>
/// </summary>
public sealed class XamlGridRowRangeTests
{
    [Fact]
    public void NoLayoutGrid_AssignsARowOrColumnItNeverDeclared()
    {
        var root = FindRepoRoot();
        Assert.True(root is not null,
            "Could not locate the repository root (walked up from the test binary looking for "
            + "PerformanceMonitor.sln). This test scans the source tree, so it cannot run without it — fix "
            + "the walk-up rather than skipping, or the rule stops being enforced without anyone noticing.");

        var files = Directory.EnumerateFiles(root!, "*.xaml", SearchOption.AllDirectories)
            .Where(f => !IsBuildOutput(root!, f))
            .OrderBy(f => f, StringComparer.Ordinal)
            .ToList();

        /* Floors, because every assertion below is over what the walk found: a glob that matched nothing,
           or a parse that silently produced an empty tree, would pass this test by having no grid to
           disagree with — which is the same shape of failure the rule itself is about. */
        Assert.True(files.Count >= 100, $"Only {files.Count} XAML files found; the tree walk is not finding the markup.");

        var offenders = new List<string>();
        var gridsInspected = 0;
        var assignmentsChecked = 0;

        foreach (var file in files)
        {
            XDocument document;
            try
            {
                document = XDocument.Load(file);
            }
            catch (Exception ex)
            {
                /* Not skipped: markup this scan cannot read is markup the rule is not enforced on, and a
                   file that stopped being well-formed XML is worth knowing about on its own. */
                offenders.Add($"{Path.GetRelativePath(root!, file)}: could not be parsed as XML — {ex.Message}");
                continue;
            }

            var (found, grids, assignments) = Offenders(document, Path.GetRelativePath(root!, file));
            offenders.AddRange(found);
            gridsInspected += grids;
            assignmentsChecked += assignments;
        }

        Assert.True(gridsInspected >= 300, $"Only {gridsInspected} grids declared rows or columns; the element walk is not descending the tree.");
        Assert.True(assignmentsChecked >= 1500, $"Only {assignmentsChecked} Grid.Row/Grid.Column assignments were checked; the attribute read is not finding them.");

        Assert.True(offenders.Count == 0,
            "A control is assigned a Grid.Row or Grid.Column at or past the end of its grid's own "
            + "definitions. WPF CLAMPS that to the last line instead of throwing, so the control shares a "
            + "cell with whatever is already there and the build stays green — the defect is only ever "
            + "visible as two things drawn on top of each other. Declare the missing line, or renumber the "
            + "assignment:\n  "
            + string.Join("\n  ", offenders));
    }

    /// <summary>
    /// The scan fires. Without this the rule above passes on any tree where the walk returns nothing, and
    /// its own floors are the only thing standing between a green run and an unenforced rule. Each case
    /// goes through the SAME <see cref="Offenders"/> the real scan uses, so a change that stopped it
    /// reporting would fail here rather than quietly passing there.
    /// </summary>
    [Theory]
    /* One row declared, row 1 assigned — the shape of #3049, off by exactly one. */
    [InlineData("<RowDefinition/>", "Grid.Row=\"1\"", "Grid.Row=1")]
    /* Ten rows, row 10 assigned — #3049 itself. */
    [InlineData("<RowDefinition/><RowDefinition/><RowDefinition/><RowDefinition/><RowDefinition/>"
                + "<RowDefinition/><RowDefinition/><RowDefinition/><RowDefinition/><RowDefinition/>",
                "Grid.Row=\"10\"", "Grid.Row=10")]
    /* A row well past the end, which clamps the same way and is no more visible. */
    [InlineData("<RowDefinition/><RowDefinition/>", "Grid.Row=\"9\"", "Grid.Row=9")]
    /* No RowDefinitions at all is ONE implicit row, so any row above zero is out of range. */
    [InlineData("", "Grid.Row=\"1\"", "Grid.Row=1")]
    /* Row in range but the SPAN reaches past the end — clamped identically, and just as silent. */
    [InlineData("<RowDefinition/><RowDefinition/>", "Grid.Row=\"1\" Grid.RowSpan=\"2\"", "Grid.RowSpan")]
    public void TheScan_ReportsAnAssignmentPastTheDefinitions(string rows, string assignment, string expected)
    {
        var (found, _, _) = Offenders(Synthetic(rows, assignment), "synthetic.xaml");

        Assert.Single(found);
        Assert.Contains(expected, found[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// The negative control: the shapes that are correct, and the ones that only LOOK wrong. Without it a
    /// scan that reported every grid would satisfy the theory above and fail the tree for no reason.
    /// </summary>
    [Theory]
    /* Last declared row, which is the boundary the off-by-one sits just past. */
    [InlineData("<RowDefinition/><RowDefinition/>", "Grid.Row=\"1\"")]
    /* No RowDefinitions and no Grid.Row: one implicit row, occupied implicitly. */
    [InlineData("", "")]
    /* Row zero of a single implicit row. */
    [InlineData("", "Grid.Row=\"0\"")]
    /* A span that ends exactly on the last line. */
    [InlineData("<RowDefinition/><RowDefinition/>", "Grid.Row=\"0\" Grid.RowSpan=\"2\"")]
    public void TheScan_AcceptsAGridThatDeclaresEveryLineItUses(string rows, string assignment)
        => Assert.Empty(Offenders(Synthetic(rows, assignment), "synthetic.xaml").Offenders);

    /// <summary>
    /// An attached <c>Grid.Row</c> on something nested inside another panel is INERT — the grid is not its
    /// layout parent — so reading it against the enclosing grid's definitions would invent an offender out
    /// of correct markup. That is why only direct children are checked, and this is the case that says so.
    /// </summary>
    [Fact]
    public void TheScan_IgnoresAnAttachedRowOnAGrandchild()
    {
        var xaml = XDocument.Parse(
            "<Grid xmlns=\"http://schemas.microsoft.com/winfx/2006/xaml/presentation\">"
            + "<Grid.RowDefinitions><RowDefinition/></Grid.RowDefinitions>"
            + "<StackPanel><TextBlock Grid.Row=\"7\"/></StackPanel>"
            + "</Grid>");

        Assert.Empty(Offenders(xaml, "synthetic.xaml").Offenders);
    }

    private static XDocument Synthetic(string rows, string assignment)
    {
        var definitions = rows.Length == 0
            ? string.Empty
            : $"<Grid.RowDefinitions>{rows}</Grid.RowDefinitions>";

        return XDocument.Parse(
            "<Grid xmlns=\"http://schemas.microsoft.com/winfx/2006/xaml/presentation\">"
            + definitions
            + $"<TextBlock {assignment}/>"
            + "</Grid>");
    }

    /// <summary>
    /// Every direct child of every <c>Grid</c> whose row or column reaches past that grid's definitions,
    /// with the grid and assignment counts so the caller can prove the walk did something.
    ///
    /// <para>A grid with no <c>&lt;Grid.RowDefinitions&gt;</c> has ONE row, not none: that is WPF's
    /// default and a <c>Grid.Row="1"</c> against it clamps exactly like the declared case. The span is
    /// folded into the same arithmetic because <c>Grid.Row="9" Grid.RowSpan="3"</c> in a ten-row grid is
    /// the same silent clamp arriving by a different attribute.</para>
    /// </summary>
    private static (List<string> Offenders, int Grids, int Assignments) Offenders(XDocument document, string file)
    {
        var offenders = new List<string>();
        var grids = 0;
        var assignments = 0;

        foreach (var grid in document.Descendants().Where(e => e.Name.LocalName == "Grid"))
        {
            grids++;

            var rows = Declared(grid, "Grid.RowDefinitions");
            var columns = Declared(grid, "Grid.ColumnDefinitions");

            foreach (var child in grid.Elements())
            {
                /* <Grid.RowDefinitions> and friends are property elements, not content. */
                if (child.Name.LocalName.Contains('.', StringComparison.Ordinal))
                {
                    continue;
                }

                foreach (var (indexName, spanName, declared) in new[]
                         {
                             ("Grid.Row", "Grid.RowSpan", rows),
                             ("Grid.Column", "Grid.ColumnSpan", columns),
                         })
                {
                    var raw = child.Attribute(indexName)?.Value;
                    if (raw is null)
                    {
                        continue;
                    }

                    assignments++;

                    /* A binding or a non-numeric value is not something this can reason about, and
                       swallowing it would be a hole in the rule rather than a tolerance. */
                    if (!int.TryParse(raw, out var index))
                    {
                        offenders.Add($"{file}: {Describe(child)} has {indexName}=\"{raw}\", which is not a number");
                        continue;
                    }

                    if (index >= declared)
                    {
                        offenders.Add($"{file}: {Describe(child)} has {indexName}={index} in a grid that declares {declared}");
                        continue;
                    }

                    var rawSpan = child.Attribute(spanName)?.Value;
                    if (rawSpan is null)
                    {
                        continue;
                    }

                    if (!int.TryParse(rawSpan, out var span))
                    {
                        offenders.Add($"{file}: {Describe(child)} has {spanName}=\"{rawSpan}\", which is not a number");
                    }
                    else if (index + span > declared)
                    {
                        offenders.Add(
                            $"{file}: {Describe(child)} has {indexName}={index} with {spanName}={span}, "
                            + $"reaching past the {declared} the grid declares");
                    }
                }
            }
        }

        return (offenders, grids, assignments);
    }

    /// <summary>The count a grid declares on one axis, where absent means WPF's implicit single line.</summary>
    private static int Declared(XElement grid, string property)
    {
        var definitions = grid.Elements().FirstOrDefault(e => e.Name.LocalName == property);

        return definitions is null ? 1 : Math.Max(definitions.Elements().Count(), 1);
    }

    private static string Describe(XElement element)
    {
        var name = element.Attribute(XName.Get("Name", "http://schemas.microsoft.com/winfx/2006/xaml"))?.Value
                   ?? element.Attribute("Name")?.Value;

        return name is null ? element.Name.LocalName : $"{element.Name.LocalName} {name}";
    }

    /// <summary>
    /// Build output, by path SEGMENT rather than substring: a directory legitimately named <c>Binaries</c>
    /// or a file under <c>.../obj-store/...</c> must not be skipped, and the separator-padded substring
    /// form misses the case where the segment ends the path.
    /// </summary>
    private static bool IsBuildOutput(string root, string file) =>
        Path.GetRelativePath(root, file)
            .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
            .Any(segment => string.Equals(segment, "bin", StringComparison.OrdinalIgnoreCase)
                            || string.Equals(segment, "obj", StringComparison.OrdinalIgnoreCase));

    /// <summary>Same walk-up idiom as <c>DocCommentHygieneTests.FindRepoRoot</c>.</summary>
    private static string? FindRepoRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);
        for (var i = 0; i < 10 && directory is not null; i++)
        {
            if (File.Exists(Path.Combine(directory.FullName, "PerformanceMonitor.sln")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        return null;
    }
}
