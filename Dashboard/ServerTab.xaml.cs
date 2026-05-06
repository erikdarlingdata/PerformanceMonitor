using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        private readonly DatabaseService _databaseService;
        private readonly ServerConnection _serverConnection;
        private readonly ICredentialService _credentialService;

        /// <summary>
        /// This server's UTC offset in minutes, used to restore the global
        /// ServerTimeHelper when this tab becomes active.
        /// </summary>
        public int UtcOffsetMinutes { get; }

        public DatabaseService DatabaseService => _databaseService;
        private static string GetLoadingMessage() => LoadingMessages.GetRandom();


        private readonly UserPreferencesService _preferencesService;
        private DispatcherTimer? _autoRefreshTimer;
        private CancellationTokenSource? _autoRefreshCts;
        private bool _isRefreshing;
        private DateTime _refreshStartedUtc;
        private bool _suppressPickerUpdates;
        private readonly HashSet<string> _initializedTabs = new();

        // Legend panel references for edge-based legends (ScottPlot issue #4717 workaround)
        private Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();

        // Stored event handler delegates for cleanup
        private Action<string, string, string?>? _viewPlanHandler;
        private Action<string>? _actualPlanStartedHandler;
        private Action? _actualPlanFinishedHandler;
        private Action<DateTime, DateTime>? _drillDownTimeRangeHandler;
        private Action? _subTabChangedHandler;
        private Analysis.SqlServerBaselineProvider? _baselineProvider;

        public ServerTab(ServerConnection serverConnection, int utcOffsetMinutes = 0)
        {
            InitializeComponent();

            // Apply theme immediately to every WpfPlot field in this control.
            // Child UserControls (MemoryContent, ResourceMetricsContent, etc.) handle their own charts;
            // this loop covers the charts declared directly in ServerTab.xaml (ResourceOverview*, Blocking*, etc.).
            foreach (var field in GetType().GetFields(
                System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance))
            {
                if (field.GetValue(this) is ScottPlot.WPF.WpfPlot chart)
                    Helpers.TabHelpers.ApplyThemeToChart(chart);
            }

            _resourceOverviewCpuHover = new Helpers.ChartHoverHelper(ResourceOverviewCpuChart, "%");
            _resourceOverviewMemoryHover = new Helpers.ChartHoverHelper(ResourceOverviewMemoryChart, "MB");
            _resourceOverviewIoHover = new Helpers.ChartHoverHelper(ResourceOverviewIoChart, "ms");
            _resourceOverviewWaitHover = new Helpers.ChartHoverHelper(ResourceOverviewWaitChart, "ms/sec");
            _lockWaitStatsHover = new Helpers.ChartHoverHelper(LockWaitStatsChart, "ms/sec");
            _blockingEventsHover = new Helpers.ChartHoverHelper(BlockingStatsBlockingEventsChart, "events");
            _blockingDurationHover = new Helpers.ChartHoverHelper(BlockingStatsDurationChart, "ms");
            _deadlocksHover = new Helpers.ChartHoverHelper(BlockingStatsDeadlocksChart, "events");
            _deadlockWaitTimeHover = new Helpers.ChartHoverHelper(BlockingStatsDeadlockWaitTimeChart, "ms");
            _collectorDurationHover = new Helpers.ChartHoverHelper(CollectorDurationChart, "ms");
            _currentWaitsDurationHover = new Helpers.ChartHoverHelper(CurrentWaitsDurationChart, "ms");
            _currentWaitsBlockedHover = new Helpers.ChartHoverHelper(CurrentWaitsBlockedChart, "sessions");

            _serverConnection = serverConnection;
            UtcOffsetMinutes = utcOffsetMinutes;
            _credentialService = new CredentialService();
            _databaseService = new DatabaseService(serverConnection.GetConnectionString(_credentialService));
            _preferencesService = new UserPreferencesService();

            InitializeDefaultTimeRanges();
            SetupChartContextMenus();
            SetupSubTabContextMenus();

            BlockingSlicer.RangeChanged += OnBlockingSlicerChanged;
            DeadlockSlicer.RangeChanged += OnDeadlockSlicerChanged;

            Loaded += ServerTab_Loaded;
            Unloaded += ServerTab_Unloaded;
            KeyDown += ServerTab_KeyDown;
            Helpers.ThemeManager.ThemeChanged += OnThemeChanged;
            Focusable = true;

            // Initialize Overview sub-tab UserControls
            DailySummaryTab.Initialize(_databaseService);
            CriticalIssuesTab.Initialize(_databaseService);
            CriticalIssuesTab.InvestigateRequested += OnInvestigateCriticalIssue;
            DefaultTraceTab.Initialize(_databaseService);
            CurrentConfigTab.Initialize(_databaseService);
            ConfigChangesTab.Initialize(_databaseService);
            MemoryTab.Initialize(_databaseService);
            MemoryTab.ChartDrillDownRequested += OnChildChartDrillDown;
            PerformanceTab.Initialize(_databaseService, s => StatusText.Text = s);
            _viewPlanHandler = (planXml, label, queryText) =>
            {
                OpenPlanTab(planXml, label, queryText);
                PlanViewerTabItem.IsSelected = true;
            };
            _actualPlanStartedHandler = (label) => ShowPlanLoading(label);
            _actualPlanFinishedHandler = () => HidePlanLoading();
            _drillDownTimeRangeHandler = (from, to) => SetDrillDownGlobalRange(from, to);
            _subTabChangedHandler = () => UpdateCompareDropdownState();
            PerformanceTab.ViewPlanRequested += _viewPlanHandler;
            PerformanceTab.ActualPlanStarted += _actualPlanStartedHandler;
            PerformanceTab.ActualPlanFinished += _actualPlanFinishedHandler;
            PerformanceTab.DrillDownTimeRangeRequested += _drillDownTimeRangeHandler;
            PerformanceTab.SubTabChanged += _subTabChangedHandler;
            SystemEventsContent.Initialize(_databaseService);
            _baselineProvider = new Analysis.SqlServerBaselineProvider(_databaseService.ConnectionString);
            ResourceMetricsContent.Initialize(_databaseService, _baselineProvider);
            ResourceMetricsContent.ChartDrillDownRequested += OnChildChartDrillDown;

            // Set default time range on UserControls based on user preferences
            var prefs = _preferencesService.GetPreferences();
            CriticalIssuesTab.SetTimeRange(prefs.DefaultHoursBack);

            // Sync time display mode picker with current setting
            var modeTag = ServerTimeHelper.CurrentDisplayMode.ToString();
            for (int i = 0; i < TimeDisplayModeBox.Items.Count; i++)
            {
                if (TimeDisplayModeBox.Items[i] is ComboBoxItem item && item.Tag?.ToString() == modeTag)
                {
                    TimeDisplayModeBox.SelectedIndex = i;
                    break;
                }
            }
        }

        private void SetupAutoRefresh()
        {
            var prefs = _preferencesService.GetPreferences();

            if (prefs.AutoRefreshEnabled)
            {
                StartAutoRefreshLoop(prefs.AutoRefreshIntervalSeconds);
                AutoRefreshToggle.IsChecked = true;
                AutoRefreshToggle.Content = $"Auto-Refresh: {prefs.AutoRefreshIntervalSeconds}s";
            }
            else
            {
                AutoRefreshToggle.IsChecked = false;
                AutoRefreshToggle.Content = "Auto-Refresh: Off";
            }
        }

        /// <summary>
        /// Async loop that replaces DispatcherTimer for auto-refresh. Task.Delay is not
        /// subject to Dispatcher priority starvation under heavy UI load (chart rendering,
        /// data binding) that can indefinitely defer Background-priority DispatcherTimer ticks.
        /// </summary>
        private async void StartAutoRefreshLoop(int intervalSeconds)
        {
            if (_autoRefreshCts != null && !_autoRefreshCts.IsCancellationRequested)
                return;

            _autoRefreshCts?.Cancel();
            var cts = new CancellationTokenSource();
            _autoRefreshCts = cts;

            try
            {
                while (!cts.Token.IsCancellationRequested)
                {
                    await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), cts.Token);
                    if (cts.Token.IsCancellationRequested) break;
                    if (_isRefreshing) continue;

                    _isRefreshing = true;
                    _refreshStartedUtc = DateTime.UtcNow;
                    try
                    {
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        await RefreshVisibleTabAsync();
                        StatusText.Text = "Ready";
                        FooterText.Text = $"Last refresh: {DateTime.Now:yyyy-MM-dd HH:mm:ss} | Server: {_serverConnection.DisplayName}";
                        Logger.Info($"Auto-refresh completed in {sw.ElapsedMilliseconds}ms for {_serverConnection.DisplayName}");
                    }
                    catch (OperationCanceledException) when (!cts.Token.IsCancellationRequested)
                    {
                        Logger.Error($"Auto-refresh query cancelled for {_serverConnection.DisplayName}, continuing loop");
                    }
                    catch (Exception ex) when (ex is not OperationCanceledException)
                    {
                        Logger.Error($"Auto-refresh error: {ex.Message}", ex);
                        StatusText.Text = "Auto-refresh error";
                    }
                    finally
                    {
                        _isRefreshing = false;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Logger.Info($"Auto-refresh loop stopped for {_serverConnection.DisplayName}");
            }
        }

        private void ServerTab_Unloaded(object sender, RoutedEventArgs e)
        {
            // WPF fires Unloaded on tab switch, not just destruction.
            // Don't tear down state here — the auto-refresh loop and chart
            // state must survive tab switches. Cleanup happens when the tab
            // is actually removed from the TabControl (via CleanupOnClose).
        }

        /// <summary>
        /// Full cleanup — call when the server tab is permanently removed, not on tab switch.
        /// </summary>
        public void CleanupOnClose()
        {
            _autoRefreshCts?.Cancel();
            _autoRefreshTimer?.Stop();
            _autoRefreshTimer = null;
            _initializedTabs.Clear();

            Helpers.ThemeManager.ThemeChanged -= OnThemeChanged;
            Loaded -= ServerTab_Loaded;
            Unloaded -= ServerTab_Unloaded;
            KeyDown -= ServerTab_KeyDown;

            BlockingSlicer.RangeChanged -= OnBlockingSlicerChanged;
            DeadlockSlicer.RangeChanged -= OnDeadlockSlicerChanged;

            CriticalIssuesTab.InvestigateRequested -= OnInvestigateCriticalIssue;
            MemoryTab.ChartDrillDownRequested -= OnChildChartDrillDown;
            ResourceMetricsContent.ChartDrillDownRequested -= OnChildChartDrillDown;

            if (_viewPlanHandler != null) PerformanceTab.ViewPlanRequested -= _viewPlanHandler;
            if (_actualPlanStartedHandler != null) PerformanceTab.ActualPlanStarted -= _actualPlanStartedHandler;
            if (_actualPlanFinishedHandler != null) PerformanceTab.ActualPlanFinished -= _actualPlanFinishedHandler;
            if (_drillDownTimeRangeHandler != null) PerformanceTab.DrillDownTimeRangeRequested -= _drillDownTimeRangeHandler;
            if (_subTabChangedHandler != null) PerformanceTab.SubTabChanged -= _subTabChangedHandler;

            DisposeChartHelpers();

            _collectionHealthUnfilteredData = null;
            _blockingEventsUnfilteredData = null;
            _deadlocksUnfilteredData = null;
            _collectionHealthFilters.Clear();
            _blockingEventsFilters.Clear();
            _deadlocksFilters.Clear();
            _legendPanels.Clear();

            _baselineProvider?.ClearCache();
        }

        public void DisposeChartHelpers()
        {
            _resourceOverviewCpuHover?.Dispose();
            _resourceOverviewMemoryHover?.Dispose();
            _resourceOverviewIoHover?.Dispose();
            _resourceOverviewWaitHover?.Dispose();
            _lockWaitStatsHover?.Dispose();
            _blockingEventsHover?.Dispose();
            _blockingDurationHover?.Dispose();
            _deadlocksHover?.Dispose();
            _deadlockWaitTimeHover?.Dispose();
            _collectorDurationHover?.Dispose();
            _currentWaitsDurationHover?.Dispose();
            _currentWaitsBlockedHover?.Dispose();

            MemoryTab.DisposeChartHelpers();
            ResourceMetricsContent.DisposeChartHelpers();
            PerformanceTab.DisposeChartHelpers();
            SystemEventsContent.DisposeChartHelpers();
        }

        public void RefreshAutoRefreshSettings()
        {
            // Stop existing loop and timer
            _autoRefreshCts?.Cancel();
            _autoRefreshCts = null;
            _autoRefreshTimer?.Stop();
            _autoRefreshTimer = null;

            // Reload settings and restart if enabled
            var prefs = _preferencesService.GetPreferences();

            if (prefs.AutoRefreshEnabled)
            {
                StartAutoRefreshLoop(prefs.AutoRefreshIntervalSeconds);
                AutoRefreshToggle.IsChecked = true;
                AutoRefreshToggle.Content = $"Auto-Refresh: {prefs.AutoRefreshIntervalSeconds}s";
            }
            else
            {
                AutoRefreshToggle.IsChecked = false;
                AutoRefreshToggle.Content = "Auto-Refresh: Off";
            }
        }

        private void AutoRefreshToggle_Click(object sender, RoutedEventArgs e)
        {
            var prefs = _preferencesService.GetPreferences();

            if (AutoRefreshToggle.IsChecked == true)
            {
                // Turn on auto-refresh
                prefs.AutoRefreshEnabled = true;
                _preferencesService.SavePreferences(prefs);

                StartAutoRefreshLoop(prefs.AutoRefreshIntervalSeconds);
                AutoRefreshToggle.Content = $"Auto-Refresh: {prefs.AutoRefreshIntervalSeconds}s";
            }
            else
            {
                // Turn off auto-refresh
                prefs.AutoRefreshEnabled = false;
                _preferencesService.SavePreferences(prefs);

                _autoRefreshCts?.Cancel();
                AutoRefreshToggle.Content = "Auto-Refresh: Off";
            }
        }

        private async void ServerTab_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
        {
            try
            {
                if (e.Key == System.Windows.Input.Key.F5)
                {
                    e.Handled = true;
                    bool fullRefresh = System.Windows.Input.Keyboard.Modifiers.HasFlag(System.Windows.Input.ModifierKeys.Control);
                    await LoadDataAsync(fullRefresh);
                }
                else if (e.Key == System.Windows.Input.Key.V &&
                         System.Windows.Input.Keyboard.Modifiers == System.Windows.Input.ModifierKeys.Control &&
                         e.OriginalSource is not System.Windows.Controls.TextBox &&
                         PlanViewerTabItem.IsSelected)
                {
                    var xml = System.Windows.Clipboard.GetText();
                    if (!string.IsNullOrWhiteSpace(xml))
                    {
                        e.Handled = true;
                        OpenPlanTab(xml, "Pasted Plan");
                        PlanViewerTabItem.IsSelected = true;
                    }
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error in ServerTab_KeyDown: {ex.Message}", ex);
                StatusText.Text = "Error refreshing data";
            }
        }

        private async void ServerTab_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                // Apply minimum column widths based on header text
                Helpers.TabHelpers.AutoSizeColumnMinWidths(HealthDataGrid);
                Helpers.TabHelpers.AutoSizeColumnMinWidths(BlockingEventsDataGrid);
                Helpers.TabHelpers.AutoSizeColumnMinWidths(DeadlocksDataGrid);

                // Freeze identifier columns
                Helpers.TabHelpers.FreezeColumns(HealthDataGrid, 1);
                Helpers.TabHelpers.FreezeColumns(BlockingEventsDataGrid, 1);
                Helpers.TabHelpers.FreezeColumns(DeadlocksDataGrid, 1);

                LoadUserPreferences();

                // Sync time range button visual with saved preference
                HighlightTimeButton(_globalHoursBack);
                GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();

                // Apply saved time range to all UserControls before initial load
                PerformanceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                MemoryTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                ResourceMetricsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                SystemEventsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                CriticalIssuesTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                DefaultTraceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);

                await LoadDataAsync(fullRefresh: false);
                SetupAutoRefresh();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading ServerTab: {ex.Message}", ex);
                StatusText.Text = "Error loading data";
            }
        }

        private async void RefreshButton_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                bool fullRefresh = System.Windows.Input.Keyboard.Modifiers.HasFlag(System.Windows.Input.ModifierKeys.Control);
                await LoadDataAsync(fullRefresh);
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing data: {ex.Message}", ex);
                StatusText.Text = "Error refreshing data";
            }
        }

        /// <summary>
        /// Handles the main TabControl's SelectionChanged event to refresh the newly
        /// visible tab with current data. Guards against bubbling from nested TabControls.
        /// </summary>
        private async void DataTabControl_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            // Only handle events from the main DataTabControl, not from nested sub-tab controls
            if (e.Source != DataTabControl) return;

            UpdateCompareDropdownState();

            // Don't refresh during initial load or if already refreshing
            if (!IsLoaded) return;
            if (_isRefreshing && (DateTime.UtcNow - _refreshStartedUtc).TotalMinutes < 2) return;

            _isRefreshing = true;
            _refreshStartedUtc = DateTime.UtcNow;
            try
            {
                await RefreshVisibleTabAsync();
                StatusText.Text = "Ready";
                FooterText.Text = $"Last refresh: {DateTime.Now:yyyy-MM-dd HH:mm:ss} | Server: {_serverConnection.DisplayName}";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing on tab switch: {ex.Message}", ex);
                StatusText.Text = "Error refreshing data";
            }
            finally
            {
                _isRefreshing = false;
            }
        }

        private void HealthDataGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            if (!Helpers.TabHelpers.IsDoubleClickOnRow((DependencyObject)e.OriginalSource)) return;
            if (HealthDataGrid.SelectedItem is CollectionHealthItem item)
            {
                var logWindow = new CollectionLogWindow(item.CollectorName, _databaseService);
                logWindow.Owner = Window.GetWindow(this);
                logWindow.ShowDialog();
            }
        }

        private void EditSchedules_Click(object sender, RoutedEventArgs e)
        {
            var scheduleWindow = new CollectorScheduleWindow(_databaseService, _serverConnection.DisplayName);
            scheduleWindow.Owner = Window.GetWindow(this);
            scheduleWindow.ShowDialog();
        }

        #region Global Compare Dropdown

        private async void CompareToCombo_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (!IsLoaded || _isRefreshing) return;

            var comparisonRange = GetComparisonRange();

            try
            {
                // Feed comparison to Resource Metrics (Server Trends overlay)
                await ResourceMetricsContent.SetComparisonRangeAsync(comparisonRange);

                // Feed comparison to Query Performance grids
                PerformanceTab.SetComparisonRange(comparisonRange);
                await PerformanceTab.RefreshComparisonAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Comparison refresh failed: {ex.Message}", ex);
            }
        }

        private (DateTime From, DateTime To)? GetComparisonRange()
        {
            if (CompareToCombo == null || CompareToCombo.SelectedIndex <= 0) return null;

            var currentEnd = _globalToDate ?? DateTime.UtcNow;
            var currentStart = _globalFromDate ?? currentEnd.AddHours(-_globalHoursBack);

            return CompareToCombo.SelectedIndex switch
            {
                1 => (currentStart.AddDays(-1), currentEnd.AddDays(-1)),   // Yesterday
                2 => (currentStart.AddDays(-7), currentEnd.AddDays(-7)),   // Last week
                3 => (currentStart.AddDays(-7), currentEnd.AddDays(-7)),   // Same day last week
                _ => null
            };
        }

        private bool IsComparisonSupportedOnCurrentTab()
        {
            return DataTabControl.SelectedIndex switch
            {
                1 => PerformanceTab.SubTabControl.SelectedIndex is 3 or 4 or 5, // Query Stats / Proc Stats / Query Store
                3 => true, // Resource Metrics — Server Trends overlay
                _ => false
            };
        }

        private void UpdateCompareDropdownState()
        {
            var supported = IsComparisonSupportedOnCurrentTab();

            if (supported)
            {
                CompareToCombo.IsEnabled = true;
                CompareToCombo.Opacity = 1.0;
                CompareToCombo.ToolTip = "Compare current period against a baseline";
            }
            else
            {
                CompareToCombo.SelectedIndex = 0;
                CompareToCombo.IsEnabled = false;
                CompareToCombo.Opacity = 0.5;
                CompareToCombo.ToolTip = "Comparison is not available for this tab";
            }
        }

        #endregion
    }
}
