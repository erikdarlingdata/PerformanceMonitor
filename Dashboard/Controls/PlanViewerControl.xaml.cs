using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard.Controls;

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

        // Populate statement grid for multi-statement plans
        if (allStatements.Count > 1)
        {
            PopulateStatementsGrid(allStatements);
            ShowStatementsPanel();
            CostText.Visibility = Visibility.Visible;
            // Auto-select first statement to render it
            if (StatementsGrid.Items.Count > 0)
                StatementsGrid.SelectedIndex = 0;
        }
        else
        {
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
        CloseStatementsPanel();
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

    #region Save & Statement Selection

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

    private void PopulateStatementsGrid(List<PlanStatement> statements)
    {
        StatementsHeader.Text = $"Statements ({statements.Count})";

        var hasActualTimes = statements.Any(s => s.QueryTimeStats != null &&
            (s.QueryTimeStats.CpuTimeMs > 0 || s.QueryTimeStats.ElapsedTimeMs > 0));
        var hasUdf = statements.Any(s => s.QueryUdfElapsedTimeMs > 0);

        // Build columns
        StatementsGrid.Columns.Clear();

        StatementsGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "#",
            Binding = new System.Windows.Data.Binding("Index"),
            Width = new DataGridLength(40),
            IsReadOnly = true
        });

        StatementsGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Query",
            Binding = new System.Windows.Data.Binding("QueryText"),
            Width = new DataGridLength(1, DataGridLengthUnitType.Star),
            IsReadOnly = true
        });

        if (hasActualTimes)
        {
            StatementsGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "CPU",
                Binding = new System.Windows.Data.Binding("CpuDisplay"),
                Width = new DataGridLength(70),
                IsReadOnly = true,
                SortMemberPath = "CpuMs"
            });
            StatementsGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Elapsed",
                Binding = new System.Windows.Data.Binding("ElapsedDisplay"),
                Width = new DataGridLength(70),
                IsReadOnly = true,
                SortMemberPath = "ElapsedMs"
            });
        }

        if (hasUdf)
        {
            StatementsGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "UDF",
                Binding = new System.Windows.Data.Binding("UdfDisplay"),
                Width = new DataGridLength(70),
                IsReadOnly = true,
                SortMemberPath = "UdfMs"
            });
        }

        if (!hasActualTimes)
        {
            StatementsGrid.Columns.Add(new DataGridTextColumn
            {
                Header = "Est. Cost",
                Binding = new System.Windows.Data.Binding("CostDisplay"),
                Width = new DataGridLength(80),
                IsReadOnly = true,
                SortMemberPath = "EstCost"
            });
        }

        StatementsGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Critical",
            Binding = new System.Windows.Data.Binding("Critical"),
            Width = new DataGridLength(60),
            IsReadOnly = true
        });

        StatementsGrid.Columns.Add(new DataGridTextColumn
        {
            Header = "Warnings",
            Binding = new System.Windows.Data.Binding("Warnings"),
            Width = new DataGridLength(70),
            IsReadOnly = true
        });

        // Build rows
        var rows = new List<StatementRow>();
        for (int i = 0; i < statements.Count; i++)
        {
            var stmt = statements[i];
            var allWarnings = stmt.PlanWarnings.ToList();
            if (stmt.RootNode != null)
                CollectWarnings(stmt.RootNode, allWarnings);

            var text = stmt.StatementText;
            if (string.IsNullOrWhiteSpace(text))
                text = $"Statement {i + 1}";
            if (text.Length > 120)
                text = text[..120] + "...";

            rows.Add(new StatementRow
            {
                Index = i + 1,
                QueryText = text,
                CpuMs = stmt.QueryTimeStats?.CpuTimeMs ?? 0,
                ElapsedMs = stmt.QueryTimeStats?.ElapsedTimeMs ?? 0,
                UdfMs = stmt.QueryUdfElapsedTimeMs,
                EstCost = stmt.StatementSubTreeCost,
                Critical = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Critical),
                Warnings = allWarnings.Count(w => w.Severity == PlanWarningSeverity.Warning),
                Statement = stmt
            });
        }

        StatementsGrid.ItemsSource = rows;
    }

    private void StatementsGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        if (StatementsGrid.SelectedItem is StatementRow row)
            RenderStatement(row.Statement);
    }

    private void CopyStatementText_Click(object sender, RoutedEventArgs e)
    {
        if (StatementsGrid.SelectedItem is StatementRow row)
        {
            var text = row.Statement.StatementText;
            if (!string.IsNullOrEmpty(text))
                Clipboard.SetText(text);
        }
    }

    private void ToggleStatements_Click(object sender, RoutedEventArgs e)
    {
        if (StatementsPanel.Visibility == Visibility.Visible)
            CloseStatementsPanel();
        else
            ShowStatementsPanel();
    }

    private void CloseStatements_Click(object sender, RoutedEventArgs e)
    {
        CloseStatementsPanel();
    }

    private void ShowStatementsPanel()
    {
        StatementsColumn.Width = new GridLength(450);
        StatementsSplitterColumn.Width = new GridLength(5);
        StatementsSplitter.Visibility = Visibility.Visible;
        StatementsPanel.Visibility = Visibility.Visible;
        StatementsButton.Visibility = Visibility.Visible;
        StatementsButtonSeparator.Visibility = Visibility.Visible;
    }

    private void CloseStatementsPanel()
    {
        StatementsPanel.Visibility = Visibility.Collapsed;
        StatementsSplitter.Visibility = Visibility.Collapsed;
        StatementsColumn.Width = new GridLength(0);
        StatementsSplitterColumn.Width = new GridLength(0);
    }

    #endregion
}

/// <summary>Data model for the statement DataGrid rows.</summary>
public class StatementRow
{
    public int Index { get; set; }
    public string QueryText { get; set; } = "";
    public long CpuMs { get; set; }
    public long ElapsedMs { get; set; }
    public long UdfMs { get; set; }
    public double EstCost { get; set; }
    public int Critical { get; set; }
    public int Warnings { get; set; }
    public PlanStatement Statement { get; set; } = null!;

    // Display helpers — grid binds to these, sorting uses the raw properties via SortMemberPath
    public string CpuDisplay => FormatDuration(CpuMs);
    public string ElapsedDisplay => FormatDuration(ElapsedMs);
    public string UdfDisplay => UdfMs > 0 ? FormatDuration(UdfMs) : "";
    public string CostDisplay => EstCost > 0 ? $"{EstCost:F2}" : "";

    private static string FormatDuration(long ms)
    {
        if (ms < 1000) return $"{ms}ms";
        if (ms < 60_000) return $"{ms / 1000.0:F1}s";
        return $"{ms / 60_000}m {(ms % 60_000) / 1000}s";
    }
}
