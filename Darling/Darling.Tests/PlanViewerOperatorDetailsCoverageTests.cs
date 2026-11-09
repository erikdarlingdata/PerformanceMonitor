/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Windows.Controls;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8456: the Properties panel shows an Operator Details section exactly when the selected operator has a row for it to print. No
/// hand-kept gate decides it: the section opens for every operator and is removed when no row went into it, so an index scan whose only
/// detail is a FORCESEEK hint, or an update whose only detail is Halloween protection, shows that row in the panel and in "Copy all
/// properties", while an operator with no detail, or with only a flag that prints inside another row, gets no section anywhere. The
/// control is built on the shared STA thread and its private builders are reached by reflection, as
/// <see cref="PlanViewerEstimatedExecutionsTests"/> does; nothing reads source text (#5833).
/// </summary>
public sealed class PlanViewerOperatorDetailsCoverageTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>Each row Operator Details prints from a property that no other row shares, with the label and value it shows when it is the operator's only detail.</summary>
    public static TheoryData<string, string> SingleDetailRows() => new()
    {
        { "Primary Node Id", "3" },
        { "Row Count", "True" },
        { "ForceSeek Columns", "2" },
        { "Partition Id", "[PtnId1000]" },
        { "Star Join Root", "True" },
        { "Star Join Type", "Fetch" },
        { "Probe Column", "[Opt_Bitmap1005]" },
        { "In-Row", "True" },
        { "Compute Sequence", "True" },
        { "Rollup Highest Level", "2" },
        { "Rollup Levels", "0, 1" },
        { "TVF Parameters", "@p1" },
        { "Original Action Col", "[Act1004]" },
        { "WITH TIES Columns", "[t].[c]" },
        { "UDX Name", "FOR XML" },
        { "Group Executed", "True" },
        { "Remote Data Access", "True" },
        { "Halloween Protection", "True" },
        { "Stats Collection Id", "7" },
    };

    /// <summary>Operators that print no Operator Details row: one with no detail at all, and one whose only flag prints inside the Top row, which it has no Top expression to print.</summary>
    public static TheoryData<string> OperatorsWithoutARow() => new() { "nothing", "WITH TIES without a Top expression" };

    private static PlanNode BareScan() => new() { NodeId = 1, PhysicalOp = "Index Scan", LogicalOp = "Index Scan" };

    /* An operator whose only Operator Details row is the one named: every other property the section reads is at its default. */
    private static PlanNode OperatorWithOnly(string row)
    {
        var node = BareScan();
        switch (row)
        {
            case "Primary Node Id": node.PrimaryNodeId = 3; break;
            case "Row Count": node.RowCount = true; break;
            case "ForceSeek Columns": node.ForceSeekColumnCount = 2; break;
            case "Partition Id": node.PartitionId = "[PtnId1000]"; break;
            case "Star Join Root": node.IsStarJoin = true; break;
            case "Star Join Type": node.StarJoinOperationType = "Fetch"; break;
            case "Probe Column": node.ProbeColumn = "[Opt_Bitmap1005]"; break;
            case "In-Row": node.InRow = true; break;
            case "Compute Sequence": node.ComputeSequence = true; break;
            case "Rollup Highest Level": node.RollupHighestLevel = 2; break;
            case "Rollup Levels": node.RollupLevels.AddRange(new[] { 0, 1 }); break;
            case "TVF Parameters": node.TvfParameters = "@p1"; break;
            case "Original Action Col": node.OriginalActionColumn = "[Act1004]"; break;
            case "WITH TIES Columns": node.TieColumns = "[t].[c]"; break;
            case "UDX Name": node.UdxName = "FOR XML"; break;
            case "Group Executed": node.GroupExecuted = true; break;
            case "Remote Data Access": node.RemoteDataAccess = true; break;
            case "Halloween Protection": node.OptimizedHalloweenProtectionUsed = true; break;
            case "Stats Collection Id": node.StatsCollectionId = 7; break;
            case "nothing": break;
            case "WITH TIES without a Top expression": node.WithTies = true; break;
            default: throw new ArgumentOutOfRangeException(nameof(row), row, "no operator is defined for this row");
        }

        return node;
    }

    [Theory]
    [MemberData(nameof(SingleDetailRows))]
    public void ThePropertiesPanel_OpensOperatorDetails_ForAnOperatorWhoseOnlyDetailIsThatRow(string label, string value)
    {
        var (sections, rows) = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            ShowProperties(control, OperatorWithOnly(label));
            return (SectionTitles(control), PropertyRows(control));
        });

        Assert.Contains("Operator Details", sections);
        Assert.True(rows.TryGetValue(label, out var shown), $"the Properties panel has no '{label}' row; its rows are: {string.Join(", ", rows.Keys)}");
        Assert.Equal(value, shown);
    }

    [Theory]
    [MemberData(nameof(SingleDetailRows))]
    public void CopyAllProperties_CarriesTheRow_ForAnOperatorWhoseOnlyDetailIsThatRow(string label, string value)
    {
        var text = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            ShowProperties(control, OperatorWithOnly(label));
            return CopyAllText(control);
        });

        Assert.Contains("Operator Details", text, StringComparison.Ordinal);
        Assert.Contains(label, text, StringComparison.Ordinal);
        Assert.Contains(value, text, StringComparison.Ordinal);
    }

    [Theory]
    [MemberData(nameof(OperatorsWithoutARow))]
    public void ThePropertiesPanel_AndCopyAll_HaveNoOperatorDetails_ForAnOperatorThatPrintsNoRow(string operatorWith)
    {
        var (sections, text) = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            ShowProperties(control, OperatorWithOnly(operatorWith));
            return (SectionTitles(control), CopyAllText(control));
        });

        Assert.DoesNotContain("Operator Details", sections);
        Assert.DoesNotContain("Operator Details", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every public settable property of <see cref="PlanNode"/> with a plain value, set alone by reflection to a non-default value, so a
    /// row added to the section later is swept in without a list here: the section is never shown or copied empty, and the panel and
    /// "Copy all properties" agree on whether it is there.
    /// </summary>
    [Fact]
    public void ThePropertiesPanel_NeverShowsOrCopiesAnEmptyOperatorDetailsSection_WhateverOnePropertyAnOperatorHas()
    {
        var properties = typeof(PlanNode).GetProperties(BindingFlags.Public | BindingFlags.Instance)
            .Where(p => p.CanWrite && p.GetIndexParameters().Length == 0 && SampleValue(p.PropertyType) is not null)
            .ToList();
        Assert.NotEmpty(properties);

        var problems = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            var found = new List<string>();
            foreach (var property in properties)
            {
                var node = BareScan();
                property.SetValue(node, SampleValue(property.PropertyType));
                ShowProperties(control, node);

                var section = PropertiesContent(control).Children.OfType<Expander>().FirstOrDefault(e => (e.Header as TextBlock)?.Text == "Operator Details");
                var rowCount = section?.Content is Panel body ? body.Children.OfType<Grid>().Count() : 0;
                var copied = CopyAllText(control).Contains("Operator Details", StringComparison.Ordinal);
                if (section is not null && rowCount == 0)
                {
                    found.Add($"{property.Name}: the panel shows an Operator Details section with no row");
                }

                if (copied != (section is not null))
                {
                    found.Add($"{property.Name}: copy-all {(copied ? "names" : "omits")} Operator Details while the panel {(section is not null ? "shows" : "has")} {(section is not null ? "it" : "none")}");
                }
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>A non-default value for a plain property type, or null for a type the sweep leaves alone.</summary>
    private static object? SampleValue(Type type)
    {
        if (type == typeof(bool)) return true;
        if (type == typeof(string)) return "x";
        if (type == typeof(int)) return 1;
        if (type == typeof(long)) return 1L;
        if (type == typeof(List<string>)) return new List<string> { "x" };
        if (type == typeof(List<int>)) return new List<int> { 1 };
        return null;
    }

    private static void ShowProperties(PlanViewerControl control, PlanNode node)
    {
        var show = typeof(PlanViewerControl).GetMethod("ShowPropertiesPanel", Instance);
        Assert.True(show is not null, "PlanViewerControl.ShowPropertiesPanel no longer exists under that name - this test's reflection anchor moved.");
        show!.Invoke(control, new object[] { node });
    }

    /// <summary>The "Copy all properties" text, rendered from the section models the panel keeps.</summary>
    private static string CopyAllText(PlanViewerControl control)
    {
        var build = typeof(PlanViewerControl).GetMethod("BuildPropertiesText", Instance);
        Assert.True(build is not null, "PlanViewerControl.BuildPropertiesText no longer exists under that name - this test's reflection anchor moved.");
        return (string)build!.Invoke(control, Array.Empty<object>())!;
    }

    private static Panel PropertiesContent(PlanViewerControl control)
    {
        var content = control.FindName("PropertiesContent") as Panel;
        Assert.True(content is not null, "the plan viewer has no PropertiesContent panel - the Properties panel's host moved");
        return content!;
    }

    /// <summary>The Properties panel's section titles, in panel order: each section is an Expander whose header is a TextBlock.</summary>
    private static List<string> SectionTitles(PlanViewerControl control) =>
        PropertiesContent(control).Children.OfType<Expander>().Select(e => (e.Header as TextBlock)?.Text ?? "").ToList();

    /// <summary>The Properties panel's rows by label: each is a Grid of a label TextBlock and a value TextBox inside a section's Expander.</summary>
    private static Dictionary<string, string> PropertyRows(PlanViewerControl control)
    {
        var rows = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var grid in PropertiesContent(control).Children.OfType<Expander>().Select(e => e.Content).OfType<Panel>().SelectMany(p => p.Children.OfType<Grid>()))
        {
            var label = grid.Children.OfType<TextBlock>().FirstOrDefault()?.Text;
            var value = grid.Children.OfType<TextBox>().FirstOrDefault()?.Text;
            if (label is not null && value is not null)
            {
                rows[label] = value;
            }
        }

        return rows;
    }
}
