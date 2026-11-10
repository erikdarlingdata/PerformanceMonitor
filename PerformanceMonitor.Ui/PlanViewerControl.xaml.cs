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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using Microsoft.Win32;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Ui;

public partial class PlanViewerControl : UserControl
{
    /// <summary>
    /// #4535: the plan analyzer's per-rule config, set by the host (the Darling Viewer from its
    /// darling.json "analyzer" section, Lite from settings.json's "analyzer" key) before
    /// <see cref="LoadPlan"/> runs. Null (the default — no host has set it, or the section is
    /// omitted) behaves as <see cref="AnalyzerConfig.Default"/>: no rule disabled, no severity
    /// overridden, exactly today's behavior.
    /// </summary>
    public AnalyzerConfig? AnalyzerConfig { get; set; }

    private ParsedPlan? _currentPlan;
    private bool _planWithheld;
    private PlanStatement? _currentStatement;
    private int _allStatementsCount;
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

    // Edge accuracy-ratio colors, matching erikdarlingdata/PerformanceStudio's PlanViewerControl exactly.
    private static readonly SolidColorBrush EdgeLightOrangeBrush = new(Color.FromRgb(0xFF, 0xB7, 0x4D));
    private static readonly SolidColorBrush EdgeFluoOrangeBrush = new(Color.FromRgb(0xFF, 0x8C, 0x00));
    private static readonly SolidColorBrush EdgeFluoRedBrush = new(Color.FromRgb(0xFF, 0x17, 0x44));
    private static readonly SolidColorBrush EdgeBlueBrush = new(Color.FromRgb(0x42, 0x8B, 0xCA));
    private static readonly SolidColorBrush EdgeLightBlueBrush = new(Color.FromRgb(0x64, 0xB5, 0xF6));
    private static readonly SolidColorBrush EdgeFluoBlueBrush = new(Color.FromRgb(0x00, 0xE5, 0xFF));

    private bool _insightsFitPending;
    private bool _insightsSetByUser;
    private bool _applyingInsightsFit;

    // The width the Statements panel asks for: 450 px as it opens, then the width its splitter was dragged to. The width it is
    // given can be narrower (FitSidePanels).
    private double _statementsWidth = PlanPanelLayout.DefaultStatementsWidth;
    private bool _fittingSidePanels;

    /// <summary>
    /// Opens or collapses the insights panel by the viewer's height (<see cref="PlanInsightsLayout.StartsExpanded"/>) as a plan shows it: in a
    /// small window the plan graph keeps most of the height and the panel starts collapsed. A panel the person has opened or closed
    /// themselves stays as they left it. Before the first layout pass the fit waits for the first real height.
    /// </summary>
    private void FitInsightsToHeight()
    {
        if (_insightsSetByUser)
            return;
        if (ActualHeight > 0)
            ApplyInsightsFit(ActualHeight);
        else
            _insightsFitPending = true;
    }

    private void ApplyInsightsFit(double height)
    {
        _insightsFitPending = false;
        _applyingInsightsFit = true;
        InsightsExpander.IsExpanded = PlanInsightsLayout.StartsExpanded(height);
        _applyingInsightsFit = false;
    }

    /// <summary>
    /// How far a child operator's actual row count may diverge from its estimate before the edge feeding
    /// it is colored; matches PerformanceStudio's <c>AccuracyRatioDivergenceLimit</c> setting (default 10,
    /// floored at <see cref="PlanEdgeColour.MinDivergenceLimit"/> when applied). Hosts set this from their
    /// own settings store before rendering.
    /// </summary>
    public double AccuracyRatioDivergenceLimit { get; set; } = PlanEdgeColour.DefaultDivergenceLimit;

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

    // Plan Insights per-card accent brushes (theme-token backed; see InsightCardStyle for the
    // quiet/non-quiet state these feed).
    private SolidColorBrush IndexAccentBrush =>
        (TryFindResource("InsightIndexBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0xFF, 0xB3, 0x47));
    private SolidColorBrush WaitsAccentBrush =>
        (TryFindResource("InsightWaitsBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x4F, 0xA3, 0xFF));
    private SolidColorBrush ParamsAccentBrush =>
        (TryFindResource("InsightParamsBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x7B, 0xCF, 0x7B));
    private SolidColorBrush ServerAccentBrush =>
        (TryFindResource("InsightServerBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x9B, 0x9B, 0xFF));

    // Parameters card value brushes: theme tokens shared with the rest of the viewer's alert colours.
    private SolidColorBrush WarningBrush =>
        (TryFindResource("WarningBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0xFF, 0xD5, 0x4F));
    private SolidColorBrush ErrorBrush =>
        (TryFindResource("ErrorBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0xE5, 0x73, 0x73));
    private SolidColorBrush AccentBrush =>
        (TryFindResource("AccentBrush") as SolidColorBrush) ?? new SolidColorBrush(Color.FromRgb(0x2E, 0xAE, 0xF1));

    /// <summary>
    /// #4629: the "critical" tier of plan-viewer text that used to be the fixed <c>Brushes.OrangeRed</c>
    /// (cost &gt;= 50%, elapsed/CPU &gt;= 1s, row estimate off by 10x+) — 3.44:1 on the Light theme's
    /// white node background, under WCAG AA's 4.5:1 floor for text (and 4.46:1 on Dark's node
    /// background, just under it too). <c>CriticalTextBrush</c> is a theme resource so Light and
    /// Dark each get their own AA-passing shade while keeping the same red-orange, more-severe-than-
    /// <see cref="WarningBrush"/> meaning.
    /// </summary>
    private SolidColorBrush CriticalOrangeBrush =>
        // #4632: the deprecated Dashboard's themes carry no CriticalTextBrush, so fall back to the pre-#4629 OrangeRed (3.44:1) there, not #FF7043 (~2.7:1 on white, worse).
        (TryFindResource("CriticalTextBrush") as SolidColorBrush) ?? Brushes.OrangeRed;

    /// <summary>
    /// Flips one Plan Insights card between its normal and its quiet state. A card with nothing to
    /// report drops its header to the muted foreground and dims its accent edge, so an empty panel
    /// reads as empty instead of shouting in the panel's accent colour. Ported from
    /// PerformanceStudio's SetInsightQuiet (erikdarlingdata/PerformanceStudio@87bad14); the
    /// quiet/non-quiet values come from PerformanceMonitor.PlanAnalysis.InsightCardStyle.
    /// </summary>
    /// <param name="header">The card's header TextBlock.</param>
    /// <param name="accentBrush">The card's own accent brush (its normal, non-empty header colour).</param>
    /// <param name="accentEdge">The card's 3px accent-edge Border.</param>
    /// <param name="isEmpty">Whether the card currently has nothing to report.</param>
    private void SetInsightQuiet(TextBlock header, Brush accentBrush, Border accentEdge, bool isEmpty)
    {
        header.Foreground = InsightCardStyle.HeaderUsesMutedForeground(isEmpty) ? MutedBrush : accentBrush;
        accentEdge.Opacity = InsightCardStyle.AccentOpacity(isEmpty);
    }

    // Current property section for collapsible groups
    private StackPanel? _currentPropertySection;

    // Properties panel row model, filter and remembered width (#4574). The width is static so a
    // width the user drags out survives closing the panel and switching plan tabs.
    private const double DefaultPropertiesWidth = PerformanceMonitor.PlanAnalysis.PropertyRows.DefaultPropertiesWidth;
    private const double MinPropertiesWidth = PerformanceMonitor.PlanAnalysis.PropertyRows.MinPropertiesWidth;
    private const double MaxPropertiesWidth = PerformanceMonitor.PlanAnalysis.PropertyRows.MaxPropertiesWidth;
    private static double _propertiesPanelWidth = DefaultPropertiesWidth;
    private readonly List<PropertyPanelSection> _propertySections = new();
    private PropertyPanelSection? _currentSection;

    // Canvas panning
    private bool _isPanning;
    private Point _panStart;
    private double _panStartOffsetX;
    private double _panStartOffsetY;

    public PlanViewerControl()
    {
        InitializeComponent();
        /* Subscribe for the life of the control. Do NOT unsubscribe on Unloaded — a TabControl
           fires Unloaded when you switch tabs, which would permanently detach this handler.
           Hosts call Cleanup() when the plan tab/window is actually closed. */
        ThemeManager.ThemeChanged += OnThemeChanged;

        /* A plan shown before the first layout pass fits its insights panel on the first real height. A toggle of the expander that is
           not this fit is the person's choice, and the fit then leaves the panel alone. */
        SizeChanged += (_, e) =>
        {
            if (_insightsFitPending && e.NewSize.Height > 0)
                ApplyInsightsFit(e.NewSize.Height);
        };
        InsightsExpander.Expanded += (_, _) => { if (!_applyingInsightsFit) _insightsSetByUser = true; };
        InsightsExpander.Collapsed += (_, _) => { if (!_applyingInsightsFit) _insightsSetByUser = true; };

        /* The insights strip scrolls sideways, so it measures its grid with no width limit, and a star column measured that way sizes
           to its content: the Wait Stats card would take its longest row and its rows would never ellipsize. The grid's width is set
           instead (BoundInsightsGridToStrip), after each layout pass, since the content-sized columns change with the statement and
           the server metadata, and only when the bound has moved, so the pass that applies it is the last. */
        InsightsGrid.LayoutUpdated += (_, _) => BoundInsightsGridToStrip();

        // The splitter writes the dragged size straight onto the column, so that is where the
        // remembered width (#4574) comes from - no drag tracking of our own.
        var widthDescriptor = System.ComponentModel.DependencyPropertyDescriptor.FromProperty(
            System.Windows.Controls.ColumnDefinition.WidthProperty, typeof(ColumnDefinition));
        widthDescriptor?.AddValueChanged(PropertiesColumn, (_, _) =>
        {
            // A width FitSidePanels gives the panel in a narrow viewer is not one the person dragged, so it is not remembered.
            if (_fittingSidePanels) return;
            if (PropertiesPanel.Visibility != Visibility.Visible) return;
            var width = PropertiesColumn.Width;
            if (width.IsAbsolute && width.Value > 0)
                _propertiesPanelWidth = width.Value;
        });

        /* The two side panels share the viewer's width with the canvas, so their widths are fitted again whenever that width
           changes; a drag of the Statements splitter is the width that panel asks for from then on. */
        PlanBody.SizeChanged += (_, e) => { if (e.WidthChanged) FitSidePanels(); };
        StatementsSplitter.DragCompleted += (_, _) =>
        {
            if (StatementsPanel.Visibility == Visibility.Visible && StatementsColumn.ActualWidth > 0)
                _statementsWidth = StatementsColumn.ActualWidth;
        };

        UpdateToolbarState();
    }

    /// <summary>
    /// The zoom, Fit and Minimap buttons act on the operators on the canvas, and Save writes the plan behind them, so they are
    /// enabled only while a statement is drawn: not before a plan loads, and not after text that does not parse or a plan with no
    /// statement to draw. Save is also enabled for a withheld plan, where its click is what says why that plan is not saved.
    /// </summary>
    private void UpdateToolbarState()
    {
        var drawn = _currentStatement?.RootNode != null;

        /* A button that is disabled while it holds the keyboard focus drops the focus out of the viewer, and the viewer's
           own Ctrl+V with it: a plan pasted after a click on Fit would stop the next paste from reaching the viewer. The
           viewer takes the focus first, so it stays inside. */
        if (!drawn && (ZoomInButton.IsKeyboardFocused || ZoomOutButton.IsKeyboardFocused || ZoomFitButton.IsKeyboardFocused
            || MinimapToggleButton.IsKeyboardFocused || SavePlanButton.IsKeyboardFocused))
        {
            Focus();
        }

        ZoomInButton.IsEnabled = drawn;
        ZoomOutButton.IsEnabled = drawn;
        ZoomFitButton.IsEnabled = drawn;
        MinimapToggleButton.IsEnabled = drawn;
        SavePlanButton.IsEnabled = drawn || _planWithheld;
    }

    /// <summary>
    /// Gives the open side panels the widths <see cref="PlanPanelLayout.Fit"/> works out for the viewer's width, and the canvas
    /// column the minimum that goes with them, so neither a panel opening nor a splitter drag leaves the canvas less than that.
    /// Before the first layout pass there is no width to fit to; the pass that gives one fits the panels.
    /// </summary>
    private void FitSidePanels()
    {
        var viewerWidth = PlanBody.ActualWidth;
        if (viewerWidth <= 0)
            return;

        var statementsOpen = StatementsPanel.Visibility == Visibility.Visible;
        var propertiesOpen = PropertiesPanel.Visibility == Visibility.Visible;
        var fit = PlanPanelLayout.Fit(
            viewerWidth,
            statementsOpen ? _statementsWidth : null,
            propertiesOpen ? PerformanceMonitor.PlanAnalysis.PropertyRows.ClampWidth(_propertiesPanelWidth) : null);

        _fittingSidePanels = true;
        try
        {
            if (statementsOpen)
                StatementsColumn.Width = new GridLength(fit.Statements);
            if (propertiesOpen)
                PropertiesColumn.Width = new GridLength(fit.Properties);
            CanvasColumn.MinWidth = fit.CanvasMinimum;
        }
        finally
        {
            _fittingSidePanels = false;
        }
    }

    /// <summary>
    /// Bounds the insights grid's width: the strip's width, so in a wide window the Wait Stats column fills what the four
    /// content-sized columns leave and its rows ellipsize as that width allows; or, when the strip is narrower than that, the four
    /// content-sized columns (each capped at 360 px) plus the Wait Stats column's minimum, so the grid keeps its minimums and the
    /// strip scrolls to the rest. Ported from PerformanceStudio (erikdarlingdata/PerformanceStudio#688).
    /// </summary>
    private void BoundInsightsGridToStrip()
    {
        var stripWidth = InsightsStrip.ViewportWidth;
        if (stripWidth <= 0 || double.IsInfinity(stripWidth))
            return;

        var columns = InsightsGrid.ColumnDefinitions;
        var contentColumns = 0.0;
        for (var i = 0; i < columns.Count - 1; i++)
            contentColumns += columns[i].ActualWidth;
        var width = Math.Max(stripWidth, contentColumns + columns[columns.Count - 1].MinWidth);
        /* An exact comparison: the bound's inputs do not depend on the grid's own width, so the pass that applies it is the last,
           and a tolerance would leave the grid a fraction of a pixel wider than the strip at a fractional display scale, which
           shows a scroll bar. A Width that is not yet set (NaN) compares unequal and is set. */
        if (InsightsGrid.Width != width)
            InsightsGrid.Width = width;
    }

    /// <summary>Unsubscribes from theme changes. Hosts must call this when the plan tab/window is closed.</summary>
    public void Cleanup()
    {
        ThemeManager.ThemeChanged -= OnThemeChanged;
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

    /// <summary>
    /// #4530: the server's edition/MAXDOP for rule 38, set by the caller from a store read (this app has no
    /// live connection to the monitored server at plan-view time — see <see cref="LoadPlan"/>). <c>null</c>
    /// when the caller has no metadata; the analyzer then falls back to rule 38's Info branch. Also feeds
    /// the Server Context card (#4597): setting this after a statement is already showing refreshes that
    /// card in place, matching PerformanceStudio's <c>Metadata</c> setter
    /// (erikdarlingdata/PerformanceStudio@85492a1).
    /// </summary>
    private PerformanceMonitor.PlanAnalysis.ServerMetadata? _serverMetadata;
    public PerformanceMonitor.PlanAnalysis.ServerMetadata? ServerMetadata
    {
        get => _serverMetadata;
        set
        {
            _serverMetadata = value;
            if (_currentStatement != null)
                ShowServerContext();
        }
    }

    /// <summary>
    /// The full query text the host passed <see cref="LoadPlan"/>, held for "Copy Query Text"'s
    /// truncated-single-statement fallback (#4582, PerformanceStudio's <c>_queryText</c>): the same
    /// text shown in <see cref="QueryTextExpander"/>, not re-derived from it, so the fallback still
    /// works even if that panel's own text is edited or hidden later.
    /// </summary>
    public string? CapturedQueryText { get; private set; }

    public async System.Threading.Tasks.Task LoadPlan(string planXml, string label, string? queryText = null)
    {
        _label = label;
        CapturedQueryText = queryText;

        if (!string.IsNullOrEmpty(queryText))
        {
            QueryTextBox.Text = queryText;
            QueryTextExpander.Visibility = Visibility.Visible;
        }
        else
        {
            QueryTextExpander.Visibility = Visibility.Collapsed;
        }

        /* #5320: a plan the statement filter withheld whole is the marker, not a plan. Say so, rather than letting
           the parser fail on it and show "The plan XML could not be read". Save refuses it with the same sentence. */
        var withheld = WithheldPlanGuard.WithheldSentence(planXml);
        if (withheld is not null)
        {
            Clear();
            CapturedQueryText = queryText;
            _planWithheld = true;
            UpdateToolbarState();
            EmptyStateTitle.Text = withheld;
            EmptyStateDetail.Visibility = Visibility.Collapsed;
            return;
        }

        _planWithheld = false;

        /* Parse + analyze off the UI thread — a multi-MB showplan is two heavy passes that would
           otherwise freeze the window for seconds. Only the render below touches the UI. A refused
           or exception-terminated parse sets ParsedPlan.ParseError instead of throwing; see below. */
        var analyzerConfig = AnalyzerConfig;
        var serverMetadata = ServerMetadata;
        var parsedPlan = await System.Threading.Tasks.Task.Run(() =>
        {
            var plan = ShowPlanParser.Parse(planXml);
            PlanAnalysisPipeline.Run(plan, analyzerConfig, serverMetadata, System.Threading.CancellationToken.None);
            return plan;
        });

        /* A paste (#8446) loads into the control that shows the previous plan, so what that plan put on screen goes
           before this one's paths decide what to show: its statement list, its selected operator and the properties
           panel showing it, its canvas, insights and minimap. The same Clear the withheld path uses, after the parse,
           so the previous plan stays on screen while a large plan is read. The multi-statement path below reopens the
           statement list for the plan it lists. */
        Clear();
        CapturedQueryText = queryText;
        _currentPlan = parsedPlan;

        // #4551: a refused or exception-terminated parse still returns whatever parsed before the
        // failure. Surface the reason in the empty-state slot instead of showing the plain "No
        // Plan Loaded" text, which would look like the caller never asked for a plan at all.
        var parseErrorMessage = PlanDisplayText.ParseErrorMessage(_currentPlan);
        if (parseErrorMessage != null)
        {
            EmptyStateTitle.Text = parseErrorMessage;
            EmptyStateDetail.Visibility = Visibility.Collapsed;
            EmptyState.Visibility = Visibility.Visible;
            PlanScrollViewer.Visibility = Visibility.Collapsed;
            return;
        }

        // #4514: includes statements nested inside a stored procedure or UDF body, so the
        // viewer's statement list shows the statements the analyzer actually found findings on.
        // #4582: this count - not Batches.Sum - is also what "Copy Query Text" uses to decide
        // whether the plan is single-statement, matching the grid.
        var allStatements = PlanStatements.EnumerateAll(_currentPlan)
            .Where(s => s.RootNode != null)
            .ToList();
        _allStatementsCount = allStatements.Count;

        if (allStatements.Count == 0)
        {
            EmptyStateTitle.Text = "No Plan Loaded";
            EmptyStateDetail.Visibility = Visibility.Visible;
            EmptyState.Visibility = Visibility.Visible;
            PlanScrollViewer.Visibility = Visibility.Collapsed;
            return;
        }

        EmptyStateTitle.Text = "No Plan Loaded";
        EmptyStateDetail.Visibility = Visibility.Visible;
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
        _planWithheld = false;
        _currentStatement = null;
        _allStatementsCount = 0;
        CapturedQueryText = null;
        _selectedNodeBorder = null;
        EmptyStateTitle.Text = "No Plan Loaded";
        EmptyStateDetail.Visibility = Visibility.Visible;
        EmptyState.Visibility = Visibility.Visible;
        PlanScrollViewer.Visibility = Visibility.Collapsed;
        InsightsPanel.Visibility = Visibility.Collapsed;
        CloseStatementsPanel();
        /* The list behind the closed panel, its count and the toolbar button that reopens it belong to the plan that is
           going (#8446). CloseStatementsPanel keeps them, because the toggle hides the panel for a plan that stays on
           screen; here the next plan populates and shows its own, or has none. */
        StatementsGrid.ItemsSource = null;
        StatementsHeader.Text = "Statements";
        StatementsButton.Visibility = Visibility.Collapsed;
        StatementsButtonSeparator.Visibility = Visibility.Collapsed;
        CostText.Text = "";
        CostText.Visibility = Visibility.Collapsed;
        ClosePropertiesPanel();
        CloseMinimapPanel();
        /* The zoom belongs to the plan that is going: the next plan is drawn at 100%, as the first one is, whatever the
           previous plan was zoomed or fitted to. */
        SetZoom(1.0);
        UpdateToolbarState();
    }

    #region Save & Statement Selection

    private void SavePlan_Click(object sender, RoutedEventArgs e)
    {
        if (_planWithheld)
        {
            WithheldPlanGuard.RefuseSave(SensitiveStatements.PlaceholderText);
            return;
        }

        if (WithheldPlanGuard.RefuseSave(_currentPlan?.RawXml)) return;

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
            // The set the root badge counts: statement-level and operator warnings, at every severity.
            var allWarnings = PlanWarningDisplay.StatementWarnings(stmt);

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
                // Every warning that is not critical, Info included, so Critical + Warnings is the root badge's count.
                Warnings = allWarnings.Count(w => w.Severity != PlanWarningSeverity.Critical),
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
            var text = PlanDisplayText.CopyQueryText(row.Statement, _allStatementsCount, CapturedQueryText);
            if (!string.IsNullOrEmpty(text))
                ClipboardText.TrySetText(text);
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
        _statementsWidth = PlanPanelLayout.DefaultStatementsWidth;
        StatementsColumn.Width = new GridLength(_statementsWidth);
        StatementsSplitterColumn.Width = new GridLength(PlanPanelLayout.SplitterWidth);
        StatementsSplitter.Visibility = Visibility.Visible;
        StatementsPanel.Visibility = Visibility.Visible;
        StatementsButton.Visibility = Visibility.Visible;
        StatementsButtonSeparator.Visibility = Visibility.Visible;
        FitSidePanels();
    }

    private void CloseStatementsPanel()
    {
        StatementsPanel.Visibility = Visibility.Collapsed;
        StatementsSplitter.Visibility = Visibility.Collapsed;
        StatementsColumn.Width = new GridLength(0);
        StatementsSplitterColumn.Width = new GridLength(0);
        FitSidePanels();
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
    public string CpuDisplay => MetricFormatter.FormatDuration(CpuMs);
    public string ElapsedDisplay => MetricFormatter.FormatDuration(ElapsedMs);
    public string UdfDisplay => UdfMs > 0 ? MetricFormatter.FormatDuration(UdfMs) : "";
    public string CostDisplay => EstCost > 0 ? $"{EstCost:F2}" : "";
}
