/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4574: <see cref="PropertyRows"/> pulls the properties panel's row model, filter matching and
/// "Copy all properties" text out of <c>PlanViewerControl.Properties.cs</c> (WPF, can't run in a
/// unit test on macOS) so this suite can pin them directly. New type, so every assertion here is
/// a compile-only RED against dev: <c>PropertyRows</c> doesn't exist on dev.
/// </summary>
public sealed class Viewer4574Tests
{
    private static PropertySection Section(string title, params PropertyRow[] rows)
    {
        var section = new PropertySection { Title = title };
        section.Rows.AddRange(rows);
        return section;
    }

    [Fact]
    public void SectionTitleMatches_EmptyFilter_AlwaysTrue()
    {
        Assert.True(PropertyRows.SectionTitleMatches("General", ""));
    }

    [Fact]
    public void SectionTitleMatches_IsCaseInsensitive()
    {
        Assert.True(PropertyRows.SectionTitleMatches("General", "GENERAL"));
        Assert.True(PropertyRows.SectionTitleMatches("General", "general"));
    }

    [Fact]
    public void SectionTitleMatches_NoMatch_False()
    {
        Assert.False(PropertyRows.SectionTitleMatches("General", "zzz"));
    }

    [Fact]
    public void RowMatches_SectionTitleMatch_KeepsEveryRowRegardlessOfOwnText()
    {
        var row = new PropertyRow { Label = "Node ID", Value = "3", SearchText = "Node ID 3" };

        // Filter text "general" matches the section title, not this row's own text.
        Assert.True(PropertyRows.RowMatches(row, "general", sectionTitleMatches: true));
    }

    [Fact]
    public void RowMatches_OwnTextMatch_CaseInsensitiveOnLabelAndValue()
    {
        var labelRow = new PropertyRow { Label = "Physical Operation", Value = "Hash Match", SearchText = "Physical Operation Hash Match" };
        var valueRow = new PropertyRow { Label = "Ordered", Value = "False", SearchText = "Ordered False" };

        Assert.True(PropertyRows.RowMatches(labelRow, "PHYSICAL", sectionTitleMatches: false));
        Assert.True(PropertyRows.RowMatches(valueRow, "false", sectionTitleMatches: false));
    }

    [Fact]
    public void RowMatches_NoMatch_False()
    {
        var row = new PropertyRow { Label = "Ordered", Value = "False", SearchText = "Ordered False" };

        Assert.False(PropertyRows.RowMatches(row, "zzz", sectionTitleMatches: false));
    }

    [Fact]
    public void SectionVisible_NoVisibleRows_False()
    {
        Assert.False(PropertyRows.SectionVisible(0));
    }

    [Fact]
    public void SectionVisible_AtLeastOneVisibleRow_True()
    {
        Assert.True(PropertyRows.SectionVisible(1));
    }

    [Fact]
    public void ClampWidth_BelowMin_ClampsUp()
    {
        Assert.Equal(PropertyRows.MinPropertiesWidth, PropertyRows.ClampWidth(50));
    }

    [Fact]
    public void ClampWidth_AboveMax_ClampsDown()
    {
        Assert.Equal(PropertyRows.MaxPropertiesWidth, PropertyRows.ClampWidth(5000));
    }

    [Fact]
    public void ClampWidth_WithinBounds_Unchanged()
    {
        Assert.Equal(400, PropertyRows.ClampWidth(400));
    }

    [Fact]
    public void CopyValue_OrdinaryRow_ReturnsValue()
    {
        var row = new PropertyRow { Label = "Node ID", Value = "3" };

        Assert.Equal("3", row.CopyValue);
    }

    [Fact]
    public void CopyLabelAndValue_OrdinaryRow_FormatsLabelColonValue()
    {
        var row = new PropertyRow { Label = "Node ID", Value = "3" };

        Assert.Equal("Node ID: 3", row.CopyLabelAndValue);
    }

    [Fact]
    public void CopyValue_BlockRow_ReturnsBlockTextNotValue()
    {
        var row = new PropertyRow { Label = "Thread 1", Value = "ignored", BlockText = "Thread 1: 500 ms" };

        Assert.Equal("Thread 1: 500 ms", row.CopyValue);
        Assert.Equal("Thread 1: 500 ms", row.CopyLabelAndValue);
    }

    [Fact]
    public void BuildPropertiesText_OrdinaryRow_FormatsAsIndentedLabelColonValue()
    {
        var sections = new List<PropertySection>
        {
            Section("General",
                new PropertyRow { Label = "Physical Operation", Value = "Hash Match" })
        };

        var text = PropertyRows.BuildPropertiesText("Hash Match", "Node ID: 3", sections);

        Assert.Equal(
            "Hash Match\nNode ID: 3\n\nGeneral\n  Physical Operation: Hash Match",
            text.Replace("\r\n", "\n"));
    }

    [Fact]
    public void BuildPropertiesText_CodeRow_PutsValueOnItsOwnLineVerbatim()
    {
        var sections = new List<PropertySection>
        {
            Section("Object",
                new PropertyRow { Label = "Full Name", Value = "[Db].[dbo].[Table]", IsCode = true })
        };

        var text = PropertyRows.BuildPropertiesText("Clustered Index Scan", "", sections);

        Assert.Equal(
            "Clustered Index Scan\n\nObject\n  Full Name:\n[Db].[dbo].[Table]",
            text.Replace("\r\n", "\n"));
    }

    [Fact]
    public void BuildPropertiesText_BlockRow_EmitsBlockTextVerbatim()
    {
        var sections = new List<PropertySection>
        {
            Section("Actual CPU",
                new PropertyRow { BlockText = "CPU\n    Thread 0: 10 ms\n    Thread 1: 20 ms" })
        };

        var text = PropertyRows.BuildPropertiesText("Hash Match", "", sections);

        Assert.Equal(
            "Hash Match\n\nActual CPU\nCPU\n    Thread 0: 10 ms\n    Thread 1: 20 ms",
            text.Replace("\r\n", "\n"));
    }

    [Fact]
    public void BuildPropertiesText_SectionWithNoRows_IsSkipped()
    {
        var sections = new List<PropertySection>
        {
            Section("Empty"),
            Section("General", new PropertyRow { Label = "Node ID", Value = "3" })
        };

        var text = PropertyRows.BuildPropertiesText("Hash Match", "", sections);

        Assert.DoesNotContain("Empty", text);
        Assert.Contains("General", text);
    }
}
