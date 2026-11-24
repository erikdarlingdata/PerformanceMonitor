/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Windows.Controls;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8839: the Add Alert Rule dialog opened with a blank Aggregate and Unit (the first metric's choices were never filled),
/// and a metric with no aggregate (a ratio) left that box blank, looking like a box to fill. Both dropdowns now hold a real
/// choice, or say the metric does not use them.
/// </summary>
public sealed class ViewerAlertRuleDialogChoiceTests
{
    private static AlertRuleEditDialog NewDialog() => new(Array.Empty<string>(), Array.Empty<DarlingTag>());

    [Fact]
    public void ANewRule_ShowsARealAggregateAndUnit_ForTheFirstMetric()
    {
        StaTestThread.Run(() =>
        {
            var dialog = NewDialog();
            var aggregate = (ComboBox)dialog.FindName("AggregateCombo");
            var unit = (ComboBox)dialog.FindName("UnitCombo");
            var first = AlertRuleEditorModel.Measures[0];

            Assert.Equal(AlertRuleEditorModel.DefaultAggregateFor(first), aggregate.SelectedItem);
            Assert.True(aggregate.IsEnabled);
            Assert.True(AlertRuleEditorModel.IsChoice(unit.SelectedItem as string));
            Assert.True(unit.IsEnabled);
            dialog.Close();
        });
    }

    [Fact]
    public void AMetricWithNoAggregate_SaysSo_InsteadOfShowingABlankDropdown()
    {
        StaTestThread.Run(() =>
        {
            var dialog = NewDialog();
            var metric = (ComboBox)dialog.FindName("MeasureCombo");
            var aggregate = (ComboBox)dialog.FindName("AggregateCombo");
            var ratio = AlertRuleEditorModel.Measures.ToList().FindIndex(m => AlertRuleEditorModel.AggregatesFor(m).Count == 0);
            Assert.True(ratio >= 0);

            metric.SelectedIndex = ratio;

            Assert.Equal(AlertRuleEditorModel.NotUsedText, aggregate.SelectedItem);
            Assert.False(aggregate.IsEnabled);

            // Back to a metric that has aggregates: the box is a real choice again.
            metric.SelectedIndex = 0;
            Assert.True(AlertRuleEditorModel.IsChoice(aggregate.SelectedItem as string));
            Assert.True(aggregate.IsEnabled);
            dialog.Close();
        });
    }

    [Fact]
    public void TheNotUsedLine_IsNeverACandidateValue_AndEveryMetricHasAtLeastOneLine()
    {
        Assert.False(AlertRuleEditorModel.IsChoice(AlertRuleEditorModel.NotUsedText));
        Assert.False(AlertRuleEditorModel.IsChoice(null));
        foreach (var measure in AlertRuleEditorModel.Measures)
        {
            Assert.NotEmpty(AlertRuleEditorModel.AggregateChoicesFor(measure));
            Assert.NotEmpty(AlertRuleEditorModel.UnitChoicesFor(measure, null));
        }
    }
}
