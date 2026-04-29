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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;

namespace PerformanceMonitorLite.Controls;

public partial class PlanViewerControl : UserControl
{
    private ParsedPlan? _currentPlan;
    private PlanStatement? _currentStatement;
    private double _zoomLevel = 1.0;
    private const double ZoomStep = 0.15;
    private const double MinZoom = 0.1;
    private const double MaxZoom = 3.0;
    private string _label = "";

    // Node selection
    private Border? _selectedNodeBorder;
    private Brush? _selectedNodeOriginalBorder;
    private Thickness _selectedNodeOriginalThickness;
    private PlanNode? _selectedNode;

    // Brushes — accent/neutral tones that suit every theme
    private static readonly SolidColorBrush SelectionBrush = new(Color.FromRgb(0x4F, 0xA3, 0xFF));
    private static readonly SolidColorBrush EdgeBrush = new(Color.FromRgb(0x6B, 0x72, 0x80));
    private static readonly SolidColorBrush OrangeBrush = new(Color.FromRgb(0xFF, 0xB3, 0x47));

    // Theme-aware brushes resolved at call time from Application.Resources
    private SolidColorBrush TooltipBgBrush =>
        (TryFindResource("PlanTooltipBgBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x1A, 0x1D, 0x23));
    private SolidColorBrush TooltipBorderBrush =>
        (TryFindResource("PlanTooltipBorderBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x3A, 0x3D, 0x45));
    private SolidColorBrush TooltipFgBrush =>
        (TryFindResource("PlanPanelTextBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0xE4, 0xE6, 0xEB));
    private SolidColorBrush MutedBrush =>
        (TryFindResource("PlanPanelMutedBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0xE4, 0xE6, 0xEB));
    private SolidColorBrush SectionHeaderBrush =>
        (TryFindResource("PlanSectionHeaderBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x4F, 0xA3, 0xFF));
    private SolidColorBrush PropSeparatorBrush =>
        (TryFindResource("PlanPropSeparatorBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x2A, 0x2D, 0x35));

    // Current property section for collapsible groups
    private StackPanel? _currentPropertySection;

    // Canvas panning
    private bool _isPanning;
    private Point _panStart;
    private double _panStartOffsetX;
    private double _panStartOffsetY;

    public PlanViewerControl()
    {
        InitializeComponent();
        Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
        Unloaded += (_, _) => Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
    }

    private void OnThemeChanged(string _)
    {
        if (_currentStatement == null) return;

        var nodeToRestore = _selectedNode;
        RenderStatement(_currentStatement);

        if (nodeToRestore == null) return;

        // Find the re-created border for the previously selected node and reopen properties
        foreach (var child in PlanCanvas.Children)
        {
            if (child is Border b && b.Tag == nodeToRestore)
            {
                SelectNode(b, nodeToRestore);
                break;
            }
        }
    }

    public void LoadPlan(string planXml, string label, string? queryText = null)
    {
        _label = label;

        if (!string.IsNullOrEmpty(queryText))
        {
            QueryTextBox.Text = queryText;
            QueryTextExpander.Visibility = Visibility.Visible;
        }
        else
        {
            QueryTextExpander.Visibility = Visibility.Collapsed;
        }
        _currentPlan = ShowPlanParser.Parse(planXml);
        PlanAnalyzer.Analyze(_currentPlan);

        var allStatements = _currentPlan.Batches
            .SelectMany(b => b.Statements)
            .Where(s => s.RootNode != null)
            .ToList();

        if (allStatements.Count == 0)
        {
            EmptyState.Visibility = Visibility.Visible;
            PlanScrollViewer.Visibility = Visibility.Collapsed;
            return;
        }

        EmptyState.Visibility = Visibility.Collapsed;
        PlanScrollViewer.Visibility = Visibility.Visible;

        // Populate statement selector
        if (allStatements.Count > 1)
        {
            StatementSelector.Items.Clear();
            for (int i = 0; i < allStatements.Count; i++)
            {
                var s = allStatements[i];
                var text = s.StatementText.Length > 80
                    ? s.StatementText[..80] + "..."
                    : s.StatementText;
                if (string.IsNullOrWhiteSpace(text))
                    text = $"Statement {i + 1}";
                StatementSelector.Items.Add(new ComboBoxItem
                {
                    Content = $"[{s.StatementSubTreeCost:F4}] {text}",
                    Tag = i
                });
            }
            StatementSelector.SelectedIndex = 0;
            StatementLabel.Visibility = Visibility.Visible;
            StatementSelector.Visibility = Visibility.Visible;
            CostText.Visibility = Visibility.Visible;
        }
        else
        {
            StatementLabel.Visibility = Visibility.Collapsed;
            StatementSelector.Visibility = Visibility.Collapsed;
            CostText.Visibility = Visibility.Collapsed;
            RenderStatement(allStatements[0]);
        }
    }

    public void Clear()
    {
        PlanCanvas.Children.Clear();
        _currentPlan = null;
        _currentStatement = null;
        _selectedNodeBorder = null;
        EmptyState.Visibility = Visibility.Visible;
        PlanScrollViewer.Visibility = Visibility.Collapsed;
        InsightsPanel.Visibility = Visibility.Collapsed;
        StatementLabel.Visibility = Visibility.Collapsed;
        StatementSelector.Visibility = Visibility.Collapsed;
        CostText.Text = "";
        CostText.Visibility = Visibility.Collapsed;
        ClosePropertiesPanel();
    }

    private static void CollectWarnings(PlanNode node, List<PlanWarning> warnings)
    {
        warnings.AddRange(node.Warnings);
        foreach (var child in node.Children)
            CollectWarnings(child, warnings);
    }

    private void SavePlan_Click(object sender, RoutedEventArgs e)
    {
        if (_currentPlan == null || string.IsNullOrEmpty(_currentPlan.RawXml)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "SQL Plan Files (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
            DefaultExt = ".sqlplan",
            FileName = $"plan_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
        };

        if (dialog.ShowDialog() == true)
        {
            File.WriteAllText(dialog.FileName, _currentPlan.RawXml);
        }
    }

    private void StatementSelector_Changed(object sender, SelectionChangedEventArgs e)
    {
        if (StatementSelector.SelectedItem is ComboBoxItem item && item.Tag is int index)
        {
            var allStatements = _currentPlan?.Batches
                .SelectMany(b => b.Statements)
                .Where(s => s.RootNode != null)
                .ToList();

            if (allStatements != null && index >= 0 && index < allStatements.Count)
                RenderStatement(allStatements[index]);
        }
    }
}
