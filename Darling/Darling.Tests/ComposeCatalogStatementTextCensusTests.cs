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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4348, L8: the Custom Views catalog is a closed list, and what it can chart or label is a number or a short name.
/// Statement text and plan or event XML never reach a Custom View (a measure column, a dimension column or an annotation
/// label), so <c>/api/compose/run</c>, <c>/api/catalog</c> and the two Custom View tools carry none and need no filter of
/// their own. This census pins that: a catalog column named like a statement or XML column fails the build until the
/// read behind it is judged.
/// </summary>
public sealed class ComposeCatalogStatementTextCensusTests
{
    private static readonly Regex StatementColumn = new(
        @"^(query_text|[a-z0-9_]*sql_text|statement_text|batch_text|text_data|inputbuf|input_buffer|command|definition"
        + @"|event_xml|[a-z0-9_]*_xml|query_plan[a-z0-9_]*|[a-z0-9_]*plan_xml|deadlock_graph[a-z0-9_]*|[a-z0-9_]*_text)$",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static IEnumerable<(string Where, string Column)> CatalogColumns()
    {
        foreach (var m in MeasureCatalog.Measures)
        {
            foreach (var column in new[] { m.Column, m.DeltaColumn, m.WeightedValueColumn, m.WeightColumn })
            {
                if (!string.IsNullOrEmpty(column))
                {
                    yield return ("measure " + m.Key, column);
                }
            }
        }

        foreach (var d in MeasureCatalog.Dimensions)
        {
            yield return ("dimension " + d.SourceTable + "." + d.Name, d.Column);
            if (!string.IsNullOrEmpty(d.FallbackColumn))
            {
                yield return ("dimension " + d.SourceTable + "." + d.Name + " fallback", d.FallbackColumn);
            }
        }

        foreach (var a in MeasureCatalog.AnnotationSources)
        {
            yield return ("annotation " + a.Key + " label", a.LabelColumn);
            yield return ("annotation " + a.Key + " time", a.TimeColumn);
        }
    }

    [Fact]
    public void NoMeasureDimensionOrAnnotationColumnIsAStatementTextOrXmlColumn()
    {
        var columns = CatalogColumns().ToList();
        Assert.True(columns.Count > 100, "the census read only " + columns.Count + " catalog columns; the walk is broken");

        var hits = columns.Where(c => StatementColumn.IsMatch(c.Column)).Select(c => c.Where + " -> " + c.Column).ToList();
        Assert.True(hits.Count == 0,
            "A Custom View column is a statement or XML column, so its answer needs the statement filter: " + string.Join("; ", hits));
    }

    [Theory]
    [InlineData("query_text")]
    [InlineData("statement_text")]
    [InlineData("batch_text")]
    [InlineData("text_data")]
    [InlineData("inputbuf")]
    [InlineData("event_xml")]
    [InlineData("query_plan_xml")]
    [InlineData("query_plan")]
    [InlineData("blocked_process_xml")]
    [InlineData("deadlock_graph_xml")]
    [InlineData("last_sql_text")]
    public void ThePatternRecognizesTheColumnsItGuards(string column)
    {
        Assert.Matches(StatementColumn, column);
    }

    [Theory]
    [InlineData("object_name")]
    [InlineData("query_hash")]
    [InlineData("database_name")]
    [InlineData("wait_type")]
    [InlineData("event_name")]
    [InlineData("contentious_object")]
    public void ThePatternLeavesAnOrdinaryLabelColumnAlone(string column)
    {
        Assert.DoesNotMatch(StatementColumn, column);
    }
}
