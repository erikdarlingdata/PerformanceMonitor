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
/// #8456: the Properties panel opens an Operator Details section whenever the selected operator has a row for it to print. The
/// section's gate covers every row the section can print, so an index scan whose only detail is a FORCESEEK hint, or an update whose
/// only detail is Halloween protection, shows that row in the panel and in "Copy all properties"; an operator with no detail at all
/// still gets no section. The control is built on the shared STA thread and its private builders are reached by reflection, as
/// <see cref="PlanViewerEstimatedExecutionsTests"/> does; nothing reads source text (#5833).
/// </summary>
public sealed class PlanViewerOperatorDetailsCoverageTests
{
    private const BindingFlags Instance = BindingFlags.Instance | BindingFlags.NonPublic;

    /// <summary>Each row Operator Details can print that no other property feeds, with the label and value it shows when it is the operator's only detail.</summary>
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

    /* An operator whose only Operator Details row is the one named: every other property the section reads is at its default. */
    private static PlanNode OperatorWithOnly(string row)
    {
        var node = new PlanNode { NodeId = 1, PhysicalOp = "Index Scan", LogicalOp = "Index Scan" };
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
            var build = typeof(PlanViewerControl).GetMethod("BuildPropertiesText", Instance);
            Assert.True(build is not null, "PlanViewerControl.BuildPropertiesText no longer exists under that name - this test's reflection anchor moved.");
            return (string)build!.Invoke(control, Array.Empty<object>())!;
        });

        Assert.Contains("Operator Details", text, StringComparison.Ordinal);
        Assert.Contains(label, text, StringComparison.Ordinal);
        Assert.Contains(value, text, StringComparison.Ordinal);
    }

    [Fact]
    public void ThePropertiesPanel_OpensNoOperatorDetails_ForAnOperatorWithNoDetail()
    {
        var sections = StaTestThread.Run(() =>
        {
            var control = new PlanViewerControl();
            ShowProperties(control, new PlanNode { NodeId = 1, PhysicalOp = "Index Scan", LogicalOp = "Index Scan" });
            return SectionTitles(control);
        });

        Assert.DoesNotContain("Operator Details", sections);
    }

    private static void ShowProperties(PlanViewerControl control, PlanNode node)
    {
        var show = typeof(PlanViewerControl).GetMethod("ShowPropertiesPanel", Instance);
        Assert.True(show is not null, "PlanViewerControl.ShowPropertiesPanel no longer exists under that name - this test's reflection anchor moved.");
        show!.Invoke(control, new object[] { node });
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
