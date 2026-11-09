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
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8447: a statement's warnings are its statement-level warnings plus every operator's warnings, at
/// every severity, and the three places the plan viewer counts them agree: the root operator's
/// "N warnings" badge, the root operator's tooltip, and the Statements grid's Critical and Warnings
/// columns, which sum to the badge. A statement whose findings are all statement-level, such as a
/// memory grant warning, still shows them on the root.
///
/// <para>Rendered on the shared STA thread with the Viewer's theme merged into the control's own
/// resources, so the application-scope theme is untouched.</para>
/// </summary>
public class PlanViewerStatementWarningCountTests
{
    private const BindingFlags Instance = BindingFlags.NonPublic | BindingFlags.Instance;

    private const string MemoryGrant = "Memory Grant Warning";
    private const string CompileTime = "High Compile Time";
    private const string Hint = "Plan Hint";
    private const string Residual = "Residual Predicate";

    [Fact]
    public void RootBadge_CountsStatementLevelWarnings_WhenNoOperatorHasOne()
    {
        StaTestThread.Run(() =>
        {
            var control = ThemedControl();
            try
            {
                Render(control, StatementLevelOnly());

                Assert.Equal("3 warnings", BadgeText(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void RootBadge_CountsStatementAndOperatorWarnings_AtEverySeverity()
    {
        StaTestThread.Run(() =>
        {
            var control = ThemedControl();
            try
            {
                // Three statement-level warnings and one Info warning on a nested operator.
                Render(control, MixedSeverities());

                Assert.Equal("4 warnings", BadgeText(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void RootTooltip_ListsStatementLevelWarnings_WhenNoOperatorHasOne()
    {
        StaTestThread.Run(() =>
        {
            var control = ThemedControl();
            try
            {
                Render(control, StatementLevelOnly());

                var tooltip = Assert.IsType<ToolTip>(RootBorder(control).ToolTip);
                var lines = Texts(tooltip.Content);
                Assert.Contains(lines, t => t.Contains(MemoryGrant, StringComparison.Ordinal));
                Assert.Contains(lines, t => t.Contains(CompileTime, StringComparison.Ordinal));
                Assert.Contains(lines, t => t.Contains(Hint, StringComparison.Ordinal));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    [Fact]
    public void StatementsGrid_CriticalAndWarningsColumns_SumToTheRootBadge()
    {
        StaTestThread.Run(() =>
        {
            var control = ThemedControl();
            try
            {
                var mixed = MixedSeverities();
                Populate(control, new List<PlanStatement> { StatementLevelOnly(), mixed });

                var rows = Assert.IsAssignableFrom<IEnumerable<StatementRow>>(control.StatementsGrid.ItemsSource).ToList();
                Assert.Equal(2, rows.Count);

                // One Critical; the Warnings column holds everything that is not critical, the Info one included.
                Assert.Equal(1, rows[1].Critical);
                Assert.Equal(3, rows[1].Warnings);

                Render(control, mixed);
                Assert.Equal($"{rows[1].Critical + rows[1].Warnings} warnings", BadgeText(control));
            }
            finally
            {
                control.Cleanup();
            }
        });
    }

    /* ── statements ── */

    /// <summary>Three statement-level warnings at the three severities, and no operator warning anywhere.</summary>
    private static PlanStatement StatementLevelOnly() => new()
    {
        StatementText = "SELECT c.* FROM dbo.Comments AS c",
        RootNode = Node(0, "Clustered Index Scan"),
        PlanWarnings = new List<PlanWarning>
        {
            Warn(MemoryGrant, PlanWarningSeverity.Critical),
            Warn(CompileTime, PlanWarningSeverity.Warning),
            Warn(Hint, PlanWarningSeverity.Info),
        },
    };

    /// <summary>The same three statement-level warnings, plus one Info warning on a nested operator.</summary>
    private static PlanStatement MixedSeverities()
    {
        var leaf = Node(2, "Index Seek");
        leaf.Warnings.Add(Warn(Residual, PlanWarningSeverity.Info));

        return new PlanStatement
        {
            StatementText = "SELECT c.* FROM dbo.Comments AS c JOIN dbo.Users AS u ON u.Id = c.UserId",
            RootNode = Node(0, "Nested Loops", Node(1, "Clustered Index Scan"), leaf),
            PlanWarnings = new List<PlanWarning>
            {
                Warn(MemoryGrant, PlanWarningSeverity.Critical),
                Warn(CompileTime, PlanWarningSeverity.Warning),
                Warn(Hint, PlanWarningSeverity.Warning),
            },
        };
    }

    private static PlanWarning Warn(string type, PlanWarningSeverity severity) =>
        new() { WarningType = type, Message = type + " on this statement", Severity = severity };

    private static PlanNode Node(int id, string op, params PlanNode[] children) =>
        new() { NodeId = id, PhysicalOp = op, LogicalOp = op, Children = children.ToList() };

    /* ── the control ── */

    private static PlanViewerControl ThemedControl()
    {
        var control = new PlanViewerControl();
        control.Resources.MergedDictionaries.Add(new ResourceDictionary
        {
            Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
        });
        return control;
    }

    private static void Render(PlanViewerControl control, PlanStatement statement) =>
        Invoke(control, "RenderStatement", statement);

    private static void Populate(PlanViewerControl control, List<PlanStatement> statements) =>
        Invoke(control, "PopulateStatementsGrid", statements);

    private static void Invoke(PlanViewerControl control, string method, object argument)
    {
        var target = typeof(PlanViewerControl).GetMethod(method, Instance);
        Assert.True(target is not null, $"PlanViewerControl.{method} no longer exists under that name - this test's reflection anchor moved.");
        target!.Invoke(control, new[] { argument });
    }

    /// <summary>The root operator's card: the leftmost node on the canvas.</summary>
    private static PlanNodeBorder RootBorder(PlanViewerControl control)
    {
        var borders = control.PlanCanvas.Children.OfType<PlanNodeBorder>().ToList();
        Assert.NotEmpty(borders);
        return borders.OrderBy(Canvas.GetLeft).First();
    }

    /// <summary>The root badge's text ("N warnings"), or null when the root carries no badge.</summary>
    private static string? BadgeText(PlanViewerControl control) =>
        Texts(RootBorder(control))
            .SingleOrDefault(t => t.EndsWith(" warning", StringComparison.Ordinal) || t.EndsWith(" warnings", StringComparison.Ordinal));

    /// <summary>Every TextBlock's text under an element that may not be in a visual tree yet.</summary>
    private static List<string> Texts(object? root)
    {
        var texts = new List<string>();
        Walk(root);
        return texts;

        void Walk(object? element)
        {
            switch (element)
            {
                case TextBlock t:
                    texts.Add(t.Text);
                    break;
                case Panel p:
                    foreach (var child in p.Children) Walk(child);
                    break;
                case Decorator d:
                    Walk(d.Child);
                    break;
                case ContentControl c:
                    Walk(c.Content);
                    break;
            }
        }
    }
}
