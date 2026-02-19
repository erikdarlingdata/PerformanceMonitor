using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Windows.Data;
using System.Text;
using System.Collections.Generic;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Controls.Primitives;
using System.Windows.Media;
using System.Windows.Threading;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Services;
using PerformanceMonitorDashboard.Helpers;
using PerformanceMonitorDashboard.Controls;
using ScottPlot.WPF;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        private readonly DatabaseService _databaseService;
        private readonly ServerConnection _serverConnection;
        private readonly ICredentialService _credentialService;
        private ServerHealthStatus? _lastKnownStatus;

        /// <summary>
        /// This server's UTC offset in minutes, used to restore the global
        /// ServerTimeHelper when this tab becomes active.
        /// </summary>
        public int UtcOffsetMinutes { get; }

        public DatabaseService DatabaseService => _databaseService;
        private static string GetLoadingMessage() => LoadingMessages.GetRandom();


        private readonly UserPreferencesService _preferencesService;
        private DispatcherTimer? _autoRefreshTimer;
        private bool _isRefreshing;

        // Filter state dictionaries for each DataGrid
        private Dictionary<string, ColumnFilterState> _serverConfigChangesFilters = new();
        private List<ServerConfigChangeItem>? _serverConfigChangesUnfilteredData;

        private Dictionary<string, ColumnFilterState> _dbConfigChangesFilters = new();
        private List<DatabaseConfigChangeItem>? _dbConfigChangesUnfilteredData;

        private Dictionary<string, ColumnFilterState> _traceFlagChangesFilters = new();
        private List<TraceFlagChangeItem>? _traceFlagChangesUnfilteredData;

        private Dictionary<string, ColumnFilterState> _collectionHealthFilters = new();
        private List<CollectionHealthItem>? _collectionHealthUnfilteredData;

        private Dictionary<string, ColumnFilterState> _blockingEventsFilters = new();
        private List<BlockingEventItem>? _blockingEventsUnfilteredData;

        private Dictionary<string, ColumnFilterState> _deadlocksFilters = new();
        private List<DeadlockItem>? _deadlocksUnfilteredData;

        // Shared filter popup
        private Popup? _filterPopup;
        private ColumnFilterPopup? _filterPopupContent;
        private string _currentFilterDataGrid = string.Empty;
        private Button? _currentFilterButton;

        // Legend panel references for edge-based legends (ScottPlot issue #4717 workaround)
        private Dictionary<ScottPlot.WPF.WpfPlot, ScottPlot.IPanel?> _legendPanels = new();

        // Chart hover tooltips
        private Helpers.ChartHoverHelper? _resourceOverviewCpuHover;
        private Helpers.ChartHoverHelper? _resourceOverviewMemoryHover;
        private Helpers.ChartHoverHelper? _resourceOverviewIoHover;
        private Helpers.ChartHoverHelper? _resourceOverviewWaitHover;
        private Helpers.ChartHoverHelper? _lockWaitStatsHover;
        private Helpers.ChartHoverHelper? _blockingEventsHover;
        private Helpers.ChartHoverHelper? _blockingDurationHover;
        private Helpers.ChartHoverHelper? _deadlocksHover;
        private Helpers.ChartHoverHelper? _deadlockWaitTimeHover;

        public ServerTab(ServerConnection serverConnection, int utcOffsetMinutes = 0)
        {
            InitializeComponent();

            _resourceOverviewCpuHover = new Helpers.ChartHoverHelper(ResourceOverviewCpuChart, "%");
            _resourceOverviewMemoryHover = new Helpers.ChartHoverHelper(ResourceOverviewMemoryChart, "MB");
            _resourceOverviewIoHover = new Helpers.ChartHoverHelper(ResourceOverviewIoChart, "ms");
            _resourceOverviewWaitHover = new Helpers.ChartHoverHelper(ResourceOverviewWaitChart, "ms/sec");
            _lockWaitStatsHover = new Helpers.ChartHoverHelper(LockWaitStatsChart, "ms/sec");
            _blockingEventsHover = new Helpers.ChartHoverHelper(BlockingStatsBlockingEventsChart, "events");
            _blockingDurationHover = new Helpers.ChartHoverHelper(BlockingStatsDurationChart, "ms");
            _deadlocksHover = new Helpers.ChartHoverHelper(BlockingStatsDeadlocksChart, "events");
            _deadlockWaitTimeHover = new Helpers.ChartHoverHelper(BlockingStatsDeadlockWaitTimeChart, "ms");

            _serverConnection = serverConnection;
            UtcOffsetMinutes = utcOffsetMinutes;
            _credentialService = new CredentialService();
            _databaseService = new DatabaseService(serverConnection.GetConnectionString(_credentialService));
            _preferencesService = new UserPreferencesService();

            InitializeDefaultTimeRanges();
            SetupChartContextMenus();
            SetupAutoRefresh();
            SetupSubTabContextMenus();

            Loaded += ServerTab_Loaded;
            Unloaded += ServerTab_Unloaded;
            KeyDown += ServerTab_KeyDown;
            Focusable = true;

            // Initialize Overview sub-tab UserControls
            DailySummaryTab.Initialize(_databaseService);
            CriticalIssuesTab.Initialize(_databaseService);
            MemoryTab.Initialize(_databaseService);
            PerformanceTab.Initialize(_databaseService, s => StatusText.Text = s);
            SystemEventsContent.Initialize(_databaseService);
            ResourceMetricsContent.Initialize(_databaseService);

            // Set default time range on UserControls based on user preferences
            var prefs = _preferencesService.GetPreferences();
            CriticalIssuesTab.SetTimeRange(prefs.DefaultHoursBack);
        }

        private void InitializeDefaultTimeRanges()
        {
            var prefs = _preferencesService.GetPreferences();
            int defaultHours = prefs.DefaultHoursBack;

            // Initialize query logging settings
            Helpers.QueryLogger.SetEnabled(prefs.LogSlowQueries);
            Helpers.QueryLogger.SetThreshold(prefs.SlowQueryThresholdSeconds);

            // Initialize global time range to user's preferred default
            _globalHoursBack = defaultHours;

            // Initialize time picker ComboBoxes
            InitializeTimeComboBoxes();

            // Initialize all hours-back fields to the user's preferred default
            _collectionHealthHoursBack = defaultHours;
            _blockingHoursBack = defaultHours;
            _deadlocksHoursBack = defaultHours;
            _blockingStatsHoursBack = defaultHours;
            // Performance tab state variables now managed by QueryPerformanceContent UserControl
            // Memory state variables now managed by MemoryContent UserControl
            _serverConfigChangesHoursBack = defaultHours;
            _dbConfigChangesHoursBack = defaultHours;
            _traceFlagChangesHoursBack = defaultHours;
            // _sessionStatsHoursBack and _queryPerfTrendsHoursBack now managed by QueryPerformanceContent UserControl
            // _criticalIssuesHoursBack now managed by CriticalIssuesTab UserControl
            // System Health/HealthParser state now managed by SystemEventsContent UserControl
            SystemEventsContent.SetTimeRange(defaultHours);
            // Resource Metrics state now managed by ResourceMetricsContent UserControl
            ResourceMetricsContent.SetTimeRange(defaultHours);
        }

        private void InitializeTimeComboBoxes()
        {
            // Populate hour ComboBoxes (12-hour format with AM/PM)
            var hours = new List<string>();
            for (int h = 0; h < 24; h++)
            {
                var dt = DateTime.Today.AddHours(h);
                hours.Add(dt.ToString("HH:00")); // "00:00", "01:00", ..., "23:00"
            }

            GlobalFromHour.ItemsSource = hours;
            GlobalToHour.ItemsSource = hours;
            GlobalFromHour.SelectedIndex = 0;  // Default to 12 AM
            GlobalToHour.SelectedIndex = 23;   // Default to 11 PM

            // Populate minute ComboBoxes (15-minute intervals)
            var minutes = new List<string> { ":00", ":15", ":30", ":45" };
            GlobalFromMinute.ItemsSource = minutes;
            GlobalToMinute.ItemsSource = minutes;
            GlobalFromMinute.SelectedIndex = 0; // Default to :00
            GlobalToMinute.SelectedIndex = 3;   // Default to :45 (so 11:45 PM is end)
        }

        private DateTime? GetDateTimeFromPickers(DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
        {
            if (!datePicker.SelectedDate.HasValue) return null;

            var date = datePicker.SelectedDate.Value.Date;
            int hour = hourCombo.SelectedIndex >= 0 ? hourCombo.SelectedIndex : 0;
            int minute = minuteCombo.SelectedIndex >= 0 ? minuteCombo.SelectedIndex * 15 : 0;

            return date.AddHours(hour).AddMinutes(minute);
        }

        private void SetPickersFromDateTime(DateTime serverTime, DatePicker datePicker, ComboBox hourCombo, ComboBox minuteCombo)
        {
            // Convert server time to local time for display in UI pickers
            var localTime = Helpers.ServerTimeHelper.ToLocalTime(serverTime);
            datePicker.SelectedDate = localTime.Date;
            hourCombo.SelectedIndex = localTime.Hour;
            minuteCombo.SelectedIndex = localTime.Minute / 15; // Round down to nearest 15-min interval
        }

        private void SetupAutoRefresh()
        {
            var prefs = _preferencesService.GetPreferences();

            if (prefs.AutoRefreshEnabled)
            {
                _autoRefreshTimer = new DispatcherTimer
                {
                    Interval = TimeSpan.FromSeconds(prefs.AutoRefreshIntervalSeconds)
                };
                _autoRefreshTimer.Tick += async (s, e) =>
                {
                    if (_isRefreshing) return;
                    _isRefreshing = true;

                    try
                    {
                        await LoadDataAsync();
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Error in auto-refresh: {ex.Message}", ex);
                        StatusText.Text = "Auto-refresh error";
                    }
                    finally
                    {
                        _isRefreshing = false;
                    }
                };
                _autoRefreshTimer.Start();
                AutoRefreshToggle.IsChecked = true;
                AutoRefreshToggle.Content = $"Auto-Refresh: {prefs.AutoRefreshIntervalSeconds}s";
                AutoRefreshToggle.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#1A472A")); // Dark green when active
            }
            else
            {
                AutoRefreshToggle.IsChecked = false;
                AutoRefreshToggle.Content = "Auto-Refresh: Off";
                AutoRefreshToggle.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#2a2d35")); // Gray when inactive
            }
        }

        private void ServerTab_Unloaded(object sender, RoutedEventArgs e)
        {
            // Stop the timer when the tab is closed
            _autoRefreshTimer?.Stop();
            _autoRefreshTimer = null;

            // Unsubscribe event handlers to prevent memory leaks
            Loaded -= ServerTab_Loaded;
            Unloaded -= ServerTab_Unloaded;
            KeyDown -= ServerTab_KeyDown;
        }

        public void RefreshAutoRefreshSettings()
        {
            // Stop existing timer
            _autoRefreshTimer?.Stop();
            _autoRefreshTimer = null;

            // Reload settings and restart if enabled
            var prefs = _preferencesService.GetPreferences();

            if (prefs.AutoRefreshEnabled)
            {
                _autoRefreshTimer = new DispatcherTimer
                {
                    Interval = TimeSpan.FromSeconds(prefs.AutoRefreshIntervalSeconds)
                };
                _autoRefreshTimer.Tick += async (s, e) =>
                {
                    if (_isRefreshing) return;
                    _isRefreshing = true;

                    try
                    {
                        await LoadDataAsync();
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Error in auto-refresh: {ex.Message}", ex);
                        StatusText.Text = "Auto-refresh error";
                    }
                    finally
                    {
                        _isRefreshing = false;
                    }
                };
                _autoRefreshTimer.Start();
                AutoRefreshToggle.IsChecked = true;
                AutoRefreshToggle.Content = $"Auto-Refresh: {prefs.AutoRefreshIntervalSeconds}s";
            }
            else
            {
                AutoRefreshToggle.IsChecked = false;
                AutoRefreshToggle.Content = "Auto-Refresh: Off";
            }
        }

        private void ApplyDarkModeToChart(ScottPlot.WPF.WpfPlot chart)
        {
            TabHelpers.ApplyDarkModeToChart(chart);
        }

        private void AutoRefreshToggle_Click(object sender, RoutedEventArgs e)
        {
            var prefs = _preferencesService.GetPreferences();

            if (AutoRefreshToggle.IsChecked == true)
            {
                // Turn on auto-refresh
                prefs.AutoRefreshEnabled = true;
                _preferencesService.SavePreferences(prefs);

                _autoRefreshTimer = new DispatcherTimer
                {
                    Interval = TimeSpan.FromSeconds(prefs.AutoRefreshIntervalSeconds)
                };
                _autoRefreshTimer.Tick += async (s, args) =>
                {
                    if (_isRefreshing) return;
                    _isRefreshing = true;

                    try
                    {
                        await LoadDataAsync();
                    }
                    catch (Exception ex)
                    {
                        Logger.Error($"Error in auto-refresh: {ex.Message}", ex);
                        StatusText.Text = "Auto-refresh error";
                    }
                    finally
                    {
                        _isRefreshing = false;
                    }
                };
                _autoRefreshTimer.Start();
                AutoRefreshToggle.Content = $"Auto-Refresh: {prefs.AutoRefreshIntervalSeconds}s";
                AutoRefreshToggle.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#1A472A")); // Dark green when active
            }
            else
            {
                // Turn off auto-refresh
                prefs.AutoRefreshEnabled = false;
                _preferencesService.SavePreferences(prefs);

                _autoRefreshTimer?.Stop();
                _autoRefreshTimer = null;
                AutoRefreshToggle.Content = "Auto-Refresh: Off";
                AutoRefreshToggle.Background = new SolidColorBrush((Color)ColorConverter.ConvertFromString("#2a2d35")); // Gray when inactive
            }
        }

        private async void ServerTab_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
        {
            try
            {
                if (e.Key == System.Windows.Input.Key.F5)
                {
                    e.Handled = true;
                    await LoadDataAsync();
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error in ServerTab_KeyDown: {ex.Message}", ex);
                StatusText.Text = "Error refreshing data";
            }
        }

        private void SetupChartContextMenus()
        {
            // Resource Overview charts
            SetupChartSaveMenu(ResourceOverviewCpuChart, "CPU_Utilization", "collect.cpu_utilization_stats");
            SetupChartSaveMenu(ResourceOverviewMemoryChart, "Memory_Utilization", "collect.memory_stats");
            SetupChartSaveMenu(ResourceOverviewIoChart, "IO_Latency", "collect.file_io_stats");
            SetupChartSaveMenu(ResourceOverviewWaitChart, "Wait_Stats", "collect.wait_stats");

            // Blocking Stats charts
            SetupChartSaveMenu(LockWaitStatsChart, "Lock_Wait_Stats", "collect.wait_stats");
            SetupChartSaveMenu(BlockingStatsBlockingEventsChart, "Blocking_Events", "collect.blocking_deadlock_stats");
            SetupChartSaveMenu(BlockingStatsDurationChart, "Blocking_Duration", "collect.blocking_deadlock_stats");
            SetupChartSaveMenu(BlockingStatsDeadlocksChart, "Deadlocks", "collect.blocking_deadlock_stats");
            SetupChartSaveMenu(BlockingStatsDeadlockWaitTimeChart, "Deadlock_Wait_Time", "collect.blocking_deadlock_stats");

            // Query Performance Trends charts now handled by QueryPerformanceContent UserControl

            // Server Utilization Trends charts now handled by ResourceMetricsContent UserControl

            // System Health charts now handled by SystemEventsContent UserControl
            // Memory Analysis charts now handled by MemoryContent UserControl
        }

        private void SetupChartSaveMenu(WpfPlot chart, string chartName, string? dataSource = null)
        {
            // Create native WPF context menu (simpler - no zoom items since double-click handles reset)
            var contextMenu = new ContextMenu();

            var copyItem = new MenuItem { Header = "Copy Image", Icon = new TextBlock { Text = "📋" } };
            copyItem.Click += (s, e) =>
            {
                var tempFile = Path.Combine(Path.GetTempPath(), $"chart_copy_{Guid.NewGuid()}.png");
                try
                {
                    chart.Plot.SavePng(tempFile, (int)chart.ActualWidth, (int)chart.ActualHeight);
                    var bitmap = new System.Windows.Media.Imaging.BitmapImage();
                    bitmap.BeginInit();
                    bitmap.CacheOption = System.Windows.Media.Imaging.BitmapCacheOption.OnLoad;
                    bitmap.UriSource = new Uri(tempFile);
                    bitmap.EndInit();
                    bitmap.Freeze();
                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(new System.Windows.DataObject(System.Windows.DataFormats.Bitmap, bitmap), false);
                }
                finally
                {
                    if (File.Exists(tempFile)) File.Delete(tempFile);
                }
            };
            contextMenu.Items.Add(copyItem);

            var saveItem = new MenuItem { Header = "Save Image As...", Icon = new TextBlock { Text = "💾" } };
            saveItem.Click += (s, e) =>
            {
                var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture);
                var defaultFileName = $"{chartName}_{timestamp}.png";
                var saveDialog = new SaveFileDialog
                {
                    Filter = "PNG Image|*.png|JPEG Image|*.jpg|BMP Image|*.bmp",
                    FileName = defaultFileName,
                    DefaultExt = ".png"
                };
                if (saveDialog.ShowDialog() == true)
                {
                    chart.Plot.SavePng(saveDialog.FileName, (int)chart.ActualWidth, (int)chart.ActualHeight);
                }
            };
            contextMenu.Items.Add(saveItem);

            var openWindowItem = new MenuItem { Header = "Open in New Window", Icon = new TextBlock { Text = "🗗" } };
            openWindowItem.Click += (s, e) =>
            {
                var newWindow = new Window
                {
                    Title = chartName.Replace("_", " ", StringComparison.Ordinal),
                    Width = 800,
                    Height = 600
                };
                var tempFile = Path.Combine(Path.GetTempPath(), $"chart_temp_{Guid.NewGuid()}.png");
                try
                {
                    chart.Plot.SavePng(tempFile, 800, 600);
                    var image = new System.Windows.Controls.Image();
                    var bitmap = new System.Windows.Media.Imaging.BitmapImage();
                    bitmap.BeginInit();
                    bitmap.CacheOption = System.Windows.Media.Imaging.BitmapCacheOption.OnLoad;
                    bitmap.UriSource = new Uri(tempFile);
                    bitmap.EndInit();
                    bitmap.Freeze();
                    image.Source = bitmap;
                    newWindow.Content = image;
                }
                finally
                {
                    if (File.Exists(tempFile)) File.Delete(tempFile);
                }
                newWindow.Show();
            };
            contextMenu.Items.Add(openWindowItem);

            contextMenu.Items.Add(new Separator());

            var autoscaleItem = new MenuItem { Header = "Revert (or double-click)", Icon = new TextBlock { Text = "↩" } };
            autoscaleItem.Click += (s, e) =>
            {
                chart.Plot.Axes.AutoScale();
                chart.Refresh();
            };
            contextMenu.Items.Add(autoscaleItem);

            contextMenu.Items.Add(new Separator());

            var exportCsvItem = new MenuItem { Header = "Export Data to CSV...", Icon = new TextBlock { Text = "📊" } };
            exportCsvItem.Click += (s, e) =>
            {
                var timestamp = DateTime.Now.ToString("yyyy-MM-dd_HH-mm-ss", CultureInfo.InvariantCulture);
                var defaultFileName = $"{chartName}_data_{timestamp}.csv";
                var saveDialog = new SaveFileDialog
                {
                    Filter = "CSV Files|*.csv|All Files|*.*",
                    FileName = defaultFileName,
                    DefaultExt = ".csv"
                };
                if (saveDialog.ShowDialog() == true)
                {
                    try
                    {
                        var sb = new StringBuilder();
                        sb.AppendLine("DateTime,Series,Value");

                        var plottables = chart.Plot.GetPlottables();
                        int seriesIndex = 1;
                        foreach (var plottable in plottables)
                        {
                            if (plottable is ScottPlot.Plottables.Scatter scatter)
                            {
                                var seriesName = scatter.LegendText ?? $"Series{seriesIndex}";
                                var points = scatter.Data.GetScatterPoints();

                                foreach (var point in points)
                                {
                                    var dateTime = DateTime.FromOADate(point.X);
                                    sb.AppendLine(CultureInfo.InvariantCulture, $"{dateTime:yyyy-MM-dd HH:mm:ss},{TabHelpers.EscapeCsvField(seriesName)},{point.Y}");
                                }
                                seriesIndex++;
                            }
                        }

                        File.WriteAllText(saveDialog.FileName, sb.ToString());
                        MessageBox.Show($"Data exported to:\n{saveDialog.FileName}", "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show($"Error exporting data:\n\n{ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                }
            };
            contextMenu.Items.Add(exportCsvItem);

            // Show Data Source (if provided)
            if (!string.IsNullOrEmpty(dataSource))
            {
                contextMenu.Items.Add(new Separator());

                var dataSourceItem = new MenuItem { Header = "Show Data Source", Icon = new TextBlock { Text = "ℹ" } };
                dataSourceItem.Click += (s, e) =>
                {
                    MessageBox.Show(
                        $"Data Source:\n\n{dataSource}",
                        "Chart Data Source",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information);
                };
                contextMenu.Items.Add(dataSourceItem);
            }

            // Disable ScottPlot's default right-click context menu handling
            chart.UserInputProcessor.UserActionResponses.RemoveAll(r =>
                r.GetType().Name.Contains("Context", StringComparison.Ordinal) ||
                r.GetType().Name.Contains("RightClick", StringComparison.Ordinal) ||
                r.GetType().Name.Contains("Menu", StringComparison.Ordinal));

            // Use PreviewMouseRightButtonDown to show context menu before ScottPlot handles it
            chart.PreviewMouseRightButtonDown += (s, e) =>
            {
                e.Handled = true; // Prevent ScottPlot from handling
                contextMenu.PlacementTarget = chart;
                contextMenu.Placement = System.Windows.Controls.Primitives.PlacementMode.MousePoint;
                contextMenu.IsOpen = true;
            };

            // Disable ALL of ScottPlot's default double-click behaviors
            chart.UserInputProcessor.UserActionResponses.RemoveAll(r =>
                r.GetType().Name.Contains("DoubleClick", StringComparison.Ordinal));

            // Use PreviewMouseDoubleClick to intercept before ScottPlot
            chart.PreviewMouseDoubleClick += (s, e) =>
            {
                e.Handled = true;
                _isAutoScaling = true;

                Dispatcher.BeginInvoke(new Action(async () =>
                {
                    try
                    {
                        if (_isZoomed)
                        {
                            await ResetToOriginalRange();
                        }
                        else
                        {
                            chart.Plot.Axes.AutoScale();
                            chart.Refresh();
                        }
                    }
                    finally
                    {
                        await Task.Delay(200);
                        _isAutoScaling = false;
                    }
                }), System.Windows.Threading.DispatcherPriority.Background);
            };

            // Add AxisLimitsChanged handler for auto-zoom-out detection
            chart.Plot.RenderManager.AxisLimitsChanged += (s, e) =>
            {
                if (_isApplyingZoom || _isAutoScaling) return; // Avoid recursion

                // Debounce - reset timer on each change
                _lastZoomedChart = chart;

                if (_chartZoomDebounceTimer == null)
                {
                    _chartZoomDebounceTimer = new DispatcherTimer
                    {
                        Interval = TimeSpan.FromMilliseconds(800)
                    };
                    _chartZoomDebounceTimer.Tick += ChartZoomDebounce_Tick;
                }

                _chartZoomDebounceTimer.Stop();
                _chartZoomDebounceTimer.Start();
            };
        }

        private async void ChartZoomDebounce_Tick(object? sender, EventArgs e)
        {
            _chartZoomDebounceTimer?.Stop();

            if (_lastZoomedChart == null || _isApplyingZoom) return;

            try
            {
                var limits = _lastZoomedChart.Plot.Axes.GetLimits();
                var fromDate = DateTime.FromOADate(limits.Left);
                var toDate = DateTime.FromOADate(limits.Right);

                // Validate dates
                if (fromDate >= toDate || fromDate.Year < 2000 || toDate.Year > 2100)
                    return;

                // Get current data range
                DateTime currentFrom, currentTo;
                if (_globalFromDate.HasValue && _globalToDate.HasValue)
                {
                    currentFrom = _globalFromDate.Value;
                    currentTo = _globalToDate.Value;
                }
                else
                {
                    currentTo = Helpers.ServerTimeHelper.ServerNow;
                    currentFrom = currentTo.AddHours(-_globalHoursBack);
                }

                // Check if zoomed significantly beyond current data range (more than 10% wider)
                var currentSpan = (currentTo - currentFrom).TotalMinutes;
                var newSpan = (toDate - fromDate).TotalMinutes;

                bool zoomedOut = fromDate < currentFrom.AddMinutes(-currentSpan * 0.1) ||
                                 toDate > currentTo.AddMinutes(currentSpan * 0.1);

                if (zoomedOut && newSpan > currentSpan * 1.1)
                {
                    // Auto-apply the zoomed-out range to fetch more data
                    _isApplyingZoom = true;
                    await ZoomToTimeRange(fromDate, toDate);
                    _isApplyingZoom = false;
                }
            }
            catch (Exception ex)
            {
                // Log but don't show error for zoom operations - they're non-critical
                Logger.Warning($"Chart zoom error: {ex.Message}");
            }
        }

        private async void ServerTab_Loaded(object sender, RoutedEventArgs e)
        {
            try
            {
                // Apply minimum column widths based on header text
                Helpers.TabHelpers.AutoSizeColumnMinWidths(ServerConfigChangesDataGrid);
                Helpers.TabHelpers.AutoSizeColumnMinWidths(DatabaseConfigChangesDataGrid);
                Helpers.TabHelpers.AutoSizeColumnMinWidths(TraceFlagChangesDataGrid);
                Helpers.TabHelpers.AutoSizeColumnMinWidths(HealthDataGrid);
                Helpers.TabHelpers.AutoSizeColumnMinWidths(BlockingEventsDataGrid);
                Helpers.TabHelpers.AutoSizeColumnMinWidths(DeadlocksDataGrid);

                // Freeze identifier columns
                Helpers.TabHelpers.FreezeColumns(ServerConfigChangesDataGrid, 1);
                Helpers.TabHelpers.FreezeColumns(DatabaseConfigChangesDataGrid, 2);
                Helpers.TabHelpers.FreezeColumns(TraceFlagChangesDataGrid, 1);
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
                
                await LoadDataAsync();
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
                await LoadDataAsync();
            }
            catch (Exception ex)
            {
                Logger.Error($"Error refreshing data: {ex.Message}", ex);
                StatusText.Text = "Error refreshing data";
            }
        }

        // ====================================================================
        // Global Time Range Controls
        // ====================================================================

        private int _globalHoursBack = 24;
        private DateTime? _globalFromDate = null;
        private DateTime? _globalToDate = null;

        // Original range tracking for zoom/reset functionality
        private int? _originalHoursBack = null;
        private DateTime? _originalFromDate = null;
        private DateTime? _originalToDate = null;
        private bool _isZoomed = false;

        // Debounce timer for auto-applying chart zoom
        private DispatcherTimer? _chartZoomDebounceTimer;
        private WpfPlot? _lastZoomedChart;
        private bool _isApplyingZoom = false;
        private bool _isAutoScaling = false;

        private async void GlobalTimeRange_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (sender is Button button && button.Tag is string hoursStr)
                {
                    _globalHoursBack = int.Parse(hoursStr, CultureInfo.InvariantCulture);
                    _globalFromDate = null;
                    _globalToDate = null;

                    // Clear any zoom state when user clicks a time button
                    ClearZoomStateWithoutRefresh();

                    // Update button visual states
                    HighlightTimeButton(_globalHoursBack);

                    // Clear custom date/time pickers
                    GlobalFromDate.SelectedDate = null;
                    GlobalToDate.SelectedDate = null;

                    // Update status indicator
                    GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();

                    // Apply to current tab and refresh it
                    await ApplyAndRefreshCurrentTabAsync();
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error changing time range: {ex.Message}", ex);
                StatusText.Text = "Error changing time range";
            }
        }

        private async void GlobalCustomDateTime_Changed(object sender, SelectionChangedEventArgs e)
        {
            await UpdateGlobalDateTimeRange();
        }

        private async void GlobalTimeCombo_Changed(object sender, SelectionChangedEventArgs e)
        {
            // Only update if both dates are selected (time change alone isn't meaningful without dates)
            if (GlobalFromDate.SelectedDate.HasValue && GlobalToDate.SelectedDate.HasValue)
            {
                await UpdateGlobalDateTimeRange();
            }
        }

        private async Task UpdateGlobalDateTimeRange()
        {
            try
            {
                var fromDateTime = GetDateTimeFromPickers(GlobalFromDate, GlobalFromHour, GlobalFromMinute);
                var toDateTime = GetDateTimeFromPickers(GlobalToDate, GlobalToHour, GlobalToMinute);

                if (fromDateTime.HasValue && toDateTime.HasValue)
                {
                    /* Convert local dates/times to server time - user picks in their timezone,
                       but database stores collection_time in server's timezone */
                    _globalFromDate = Helpers.ServerTimeHelper.ToServerTime(fromDateTime.Value);
                    _globalToDate = Helpers.ServerTimeHelper.ToServerTime(toDateTime.Value);

                    if (_globalFromDate > _globalToDate)
                    {
                        MessageBox.Show("Start date/time cannot be after end date/time.", "Invalid Date Range", MessageBoxButton.OK, MessageBoxImage.Warning);
                        GlobalFromDate.SelectedDate = null;
                        GlobalToDate.SelectedDate = null;
                        return;
                    }

                    // Clear any zoom state when user manually changes date pickers
                    ClearZoomStateWithoutRefresh();

                    // Clear button selection
                    ClearTimeButtonHighlights();

                    _globalHoursBack = 0;
                    GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();

                    // Apply to current tab and refresh it
                    await ApplyAndRefreshCurrentTabAsync();
                }
            }
            catch (Exception ex)
            {
                Logger.Error($"Error applying custom date range: {ex.Message}", ex);
                StatusText.Text = "Error applying date range";
            }
        }

        private void DatePicker_CalendarOpened(object sender, RoutedEventArgs e)
        {
            if (sender is DatePicker datePicker)
            {
                // Use BeginInvoke to ensure visual tree is ready
                Dispatcher.BeginInvoke(new Action(() =>
                {
                    // Get the Popup and Calendar from the DatePicker template
                    var popup = datePicker.Template.FindName("PART_Popup", datePicker) as System.Windows.Controls.Primitives.Popup;
                    if (popup?.Child is System.Windows.Controls.Calendar calendar)
                    {
                        TabHelpers.ApplyDarkThemeToCalendar(calendar);
                    }
                }));
            }
        }

        private async void ApplyToAllTabs_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                // Apply the global time range to all tab-specific fields (ServerTab's own fields)
                ApplyGlobalRangeToAllTabs();

                // Apply the global time range to all extracted UserControls
                PerformanceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                MemoryTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                ResourceMetricsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                SystemEventsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                CriticalIssuesTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);

                // Refresh all data
                StatusText.Text = GetLoadingMessage();
                await LoadDataAsync();
                StatusText.Text = "Time range applied to all tabs";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error applying time range to all tabs: {ex.Message}", ex);
                StatusText.Text = "Error applying time range";
            }
        }

        private string GetGlobalDateRangeText()
        {
            DateTime from, to;

            if (_globalFromDate.HasValue && _globalToDate.HasValue)
            {
                from = _globalFromDate.Value;
                to = _globalToDate.Value;
            }
            else
            {
                // Calculate actual range from hours back using server time
                to = Helpers.ServerTimeHelper.ServerNow;
                from = to.AddHours(-_globalHoursBack);
            }

            return FormatDateRange("Showing", from, to);
        }

        private string GetOriginalRangeText()
        {
            DateTime from, to;

            if (_originalFromDate.HasValue && _originalToDate.HasValue)
            {
                from = _originalFromDate.Value;
                to = _originalToDate.Value;
            }
            else if (_originalHoursBack.HasValue)
            {
                to = Helpers.ServerTimeHelper.ServerNow;
                from = to.AddHours(-_originalHoursBack.Value);
            }
            else
            {
                return "";
            }

            return FormatDateRange("Original", from, to);
        }

        private static string FormatDateRange(string prefix, DateTime from, DateTime to)
        {
            // Same day: "Feb 7, 2:15 PM – 3:15 PM"
            if (from.Date == to.Date)
            {
                return $"{prefix}: {from:MMM d, h:mm tt} – {to:h:mm tt}";
            }

            // Same year, different days: "Feb 6, 3:15 PM – Feb 7, 3:15 PM"
            if (from.Year == to.Year)
            {
                return $"{prefix}: {from:MMM d, h:mm tt} – {to:MMM d, h:mm tt}";
            }

            // Different years: "Dec 31, 2025, 11:00 PM – Jan 1, 2026, 11:00 PM"
            return $"{prefix}: {from:MMM d, yyyy, h:mm tt} – {to:MMM d, yyyy, h:mm tt}";
        }

        private void StoreOriginalRangeIfNeeded()
        {
            if (!_isZoomed)
            {
                // Store current range as original before zooming
                _originalHoursBack = _globalHoursBack;
                _originalFromDate = _globalFromDate;
                _originalToDate = _globalToDate;
            }
        }

        private async Task ZoomToTimeRange(DateTime from, DateTime to)
        {
            // Store original if this is the first zoom
            StoreOriginalRangeIfNeeded();

            // Update global range to the zoomed range
            _globalFromDate = from;
            _globalToDate = to;
            _isZoomed = true;

            // Update date/time pickers with full datetime
            SetPickersFromDateTime(from, GlobalFromDate, GlobalFromHour, GlobalFromMinute);
            SetPickersFromDateTime(to, GlobalToDate, GlobalToHour, GlobalToMinute);

            // Clear button highlighting since we're using custom range
            ClearTimeButtonHighlights();

            // Update indicators
            GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();
            var originalText = GetOriginalRangeText();
            if (!string.IsNullOrEmpty(originalText))
            {
                OriginalRangeIndicator.Text = "Original: " + originalText;
                OriginalRangeIndicator.Visibility = Visibility.Visible;
                RevertHintText.Visibility = Visibility.Visible;
            }
            else
            {
                OriginalRangeIndicator.Visibility = Visibility.Collapsed;
                RevertHintText.Visibility = Visibility.Collapsed;
            }

            // Refresh current tab
            await ApplyAndRefreshCurrentTabAsync();
        }

        private async Task ResetToOriginalRange()
        {
            if (!_isZoomed) return;

            // Restore original range
            if (_originalFromDate.HasValue && _originalToDate.HasValue)
            {
                _globalFromDate = _originalFromDate;
                _globalToDate = _originalToDate;
                SetPickersFromDateTime(_originalFromDate.Value, GlobalFromDate, GlobalFromHour, GlobalFromMinute);
                SetPickersFromDateTime(_originalToDate.Value, GlobalToDate, GlobalToHour, GlobalToMinute);
                ClearTimeButtonHighlights();
            }
            else if (_originalHoursBack.HasValue)
            {
                _globalHoursBack = _originalHoursBack.Value;
                _globalFromDate = null;
                _globalToDate = null;
                GlobalFromDate.SelectedDate = null;
                GlobalToDate.SelectedDate = null;
                HighlightTimeButton(_originalHoursBack.Value);
            }

            // Clear zoom state
            _isZoomed = false;
            _originalHoursBack = null;
            _originalFromDate = null;
            _originalToDate = null;

            // Update indicators
            GlobalDateRangeIndicator.Text = GetGlobalDateRangeText();
            OriginalRangeIndicator.Text = "";
            OriginalRangeIndicator.Visibility = Visibility.Collapsed;
            RevertHintText.Visibility = Visibility.Collapsed;

            // Refresh current tab
            await ApplyAndRefreshCurrentTabAsync();
        }

        private void ClearZoomStateWithoutRefresh()
        {
            _isZoomed = false;
            _originalHoursBack = null;
            _originalFromDate = null;
            _originalToDate = null;
            OriginalRangeIndicator.Text = "";
            OriginalRangeIndicator.Visibility = Visibility.Collapsed;
            RevertHintText.Visibility = Visibility.Collapsed;
        }

        private void ClearTimeButtonHighlights()
        {
            // Use dark theme background color (#404040)
            var defaultBrush = new SolidColorBrush(Color.FromRgb(0x40, 0x40, 0x40));
            GlobalLast1HourButton.FontWeight = FontWeights.Normal;
            GlobalLast1HourButton.Background = defaultBrush;
            GlobalLast4HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast4HoursButton.Background = defaultBrush;
            GlobalLast8HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast8HoursButton.Background = defaultBrush;
            GlobalLast12HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast12HoursButton.Background = defaultBrush;
            GlobalLast24HoursButton.FontWeight = FontWeights.Normal;
            GlobalLast24HoursButton.Background = defaultBrush;
            GlobalLast7DaysButton.FontWeight = FontWeights.Normal;
            GlobalLast7DaysButton.Background = defaultBrush;
            GlobalLast30DaysButton.FontWeight = FontWeights.Normal;
            GlobalLast30DaysButton.Background = defaultBrush;
        }

        private void HighlightTimeButton(int hours)
        {
            ClearTimeButtonHighlights();
            // Use accent color (#2eaef1) for selected button
            var highlightBrush = new SolidColorBrush(Color.FromRgb(0x2E, 0xAE, 0xF1));
            switch (hours)
            {
                case 1:
                    GlobalLast1HourButton.FontWeight = FontWeights.Bold;
                    GlobalLast1HourButton.Background = highlightBrush;
                    break;
                case 4:
                    GlobalLast4HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast4HoursButton.Background = highlightBrush;
                    break;
                case 8:
                    GlobalLast8HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast8HoursButton.Background = highlightBrush;
                    break;
                case 12:
                    GlobalLast12HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast12HoursButton.Background = highlightBrush;
                    break;
                case 24:
                    GlobalLast24HoursButton.FontWeight = FontWeights.Bold;
                    GlobalLast24HoursButton.Background = highlightBrush;
                    break;
                case 168:
                    GlobalLast7DaysButton.FontWeight = FontWeights.Bold;
                    GlobalLast7DaysButton.Background = highlightBrush;
                    break;
                case 720:
                    GlobalLast30DaysButton.FontWeight = FontWeights.Bold;
                    GlobalLast30DaysButton.Background = highlightBrush;
                    break;
            }
        }

        private void ApplyGlobalRangeToAllTabs()
        {
            // Apply global settings to all per-tab time range fields
            // Collection Health
            _collectionHealthHoursBack = _globalHoursBack;
            _collectionHealthFromDate = _globalFromDate;
            _collectionHealthToDate = _globalToDate;

            // Resource Overview (on Overview tab)
            _resourceOverviewHoursBack = _globalHoursBack;
            _resourceOverviewFromDate = _globalFromDate;
            _resourceOverviewToDate = _globalToDate;

            // Blocking
            _blockingHoursBack = _globalHoursBack;
            _blockingFromDate = _globalFromDate;
            _blockingToDate = _globalToDate;

            // Deadlocks
            _deadlocksHoursBack = _globalHoursBack;
            _deadlocksFromDate = _globalFromDate;
            _deadlocksToDate = _globalToDate;

            // Blocking Stats
            _blockingStatsHoursBack = _globalHoursBack;
            _blockingStatsFromDate = _globalFromDate;
            _blockingStatsToDate = _globalToDate;

        }

        /// <summary>
        /// Extracts the text from a TabItem's header, handling both simple string headers
        /// and complex headers (like StackPanel with TextBlock for tabs with badges).
        /// </summary>
        private static string GetTabHeaderText(TabItem tabItem)
        {
            if (tabItem.Header is string headerString)
                return headerString;

            if (tabItem.Header is StackPanel stackPanel)
            {
                var textBlock = stackPanel.Children.OfType<TextBlock>().FirstOrDefault();
                if (textBlock != null)
                    return textBlock.Text;
            }

            return tabItem.Header?.ToString() ?? "";
        }

        private async Task ApplyAndRefreshCurrentTabAsync()
        {
            if (_databaseService == null) return;

            // Get the current tab
            var selectedTab = DataTabControl.SelectedItem as TabItem;
            if (selectedTab == null) return;

            var tabHeader = GetTabHeaderText(selectedTab);
            StatusText.Text = GetLoadingMessage();

            try
            {
                switch (tabHeader)
                {
                    case "Overview":
                        // Overview tab has Collection Health, Daily Summary, Critical Issues, Resource Overview sub-tabs
                        _collectionHealthHoursBack = _globalHoursBack;
                        _collectionHealthFromDate = _globalFromDate;
                        _collectionHealthToDate = _globalToDate;
                        _resourceOverviewHoursBack = _globalHoursBack;
                        _resourceOverviewFromDate = _globalFromDate;
                        _resourceOverviewToDate = _globalToDate;
                        CriticalIssuesTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        CollectionHealth_Refresh_Click(null, new RoutedEventArgs());
                        await CriticalIssuesTab.RefreshDataAsync();
                        await RefreshResourceOverviewAsync();
                        break;

                    case "Locking":
                        // Locking tab has sub-tabs, refresh all of them
                        _blockingHoursBack = _globalHoursBack;
                        _blockingFromDate = _globalFromDate;
                        _blockingToDate = _globalToDate;
                        _deadlocksHoursBack = _globalHoursBack;
                        _deadlocksFromDate = _globalFromDate;
                        _deadlocksToDate = _globalToDate;
                        _blockingStatsHoursBack = _globalHoursBack;
                        _blockingStatsFromDate = _globalFromDate;
                        _blockingStatsToDate = _globalToDate;
                        Blocking_Refresh_Click(null, new RoutedEventArgs());
                        Deadlocks_Refresh_Click(null, new RoutedEventArgs());
                        BlockingStats_Refresh_Click(null, new RoutedEventArgs());
                        break;

                    case "Queries":
                        // Queries tab content is in QueryPerformanceContent UserControl
                        PerformanceTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        await PerformanceTab.RefreshAllDataAsync();
                        break;

                    case "Memory":
                        // Memory tab content is now in MemoryContent UserControl
                        MemoryTab.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        await MemoryTab.RefreshAllDataAsync();
                        break;

                    case "Resource Metrics":
                        // Resource Metrics tab content is now in ResourceMetricsContent UserControl
                        ResourceMetricsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        await ResourceMetricsContent.RefreshAllDataAsync();
                        break;

                    case "System Events":
                        // System Events tab - HealthParser data is handled by SystemEventsContent UserControl
                        SystemEventsContent.SetTimeRange(_globalHoursBack, _globalFromDate, _globalToDate);
                        await SystemEventsContent.RefreshAllDataAsync();
                        // Configuration sub-tabs (Server Config Changes, DB Config Changes, Trace Flags) are still in ServerTab
                        _serverConfigChangesHoursBack = _globalHoursBack;
                        _serverConfigChangesFromDate = _globalFromDate;
                        _serverConfigChangesToDate = _globalToDate;
                        _dbConfigChangesHoursBack = _globalHoursBack;
                        _dbConfigChangesFromDate = _globalFromDate;
                        _dbConfigChangesToDate = _globalToDate;
                        _traceFlagChangesHoursBack = _globalHoursBack;
                        _traceFlagChangesFromDate = _globalFromDate;
                        _traceFlagChangesToDate = _globalToDate;
                        ServerConfigChanges_Refresh_Click(null, new RoutedEventArgs());
                        DatabaseConfigChanges_Refresh_Click(null, new RoutedEventArgs());
                        TraceFlagChanges_Refresh_Click(null, new RoutedEventArgs());
                        break;

                    default:
                        // For tabs without time range filters, just note we can't filter
                        StatusText.Text = $"{tabHeader} doesn't use time range filters";
                        return;
                }

                StatusText.Text = $"{tabHeader} refreshed with new time range";
            }
            catch (Exception ex)
            {
                StatusText.Text = $"Error refreshing {tabHeader}: {ex.Message}";
                Logger.Error($"Error refreshing {tabHeader}", ex);
            }
        }

        private async Task LoadDataAsync()
        {
            using var _ = Helpers.MethodProfiler.StartTiming("ServerTab");
            try
            {
                StatusText.Text = GetLoadingMessage();
                RefreshButton.IsEnabled = false;

                bool connected = await _databaseService.TestConnectionAsync();
                if (!connected)
                {
                    StatusText.Text = $"Failed to connect to {_serverConnection.DisplayName}";
                    MessageBox.Show(
                        $"Could not connect to SQL Server: {_serverConnection.ServerName}\n\nCheck connection settings",
                        "Connection Error",
                        MessageBoxButton.OK,
                        MessageBoxImage.Error
                    );
                    return;
                }

                StatusText.Text = GetLoadingMessage();

                // Fetch all data in parallel — overview queries + all tab refreshes
                var healthTask = _databaseService.GetCollectionHealthAsync();
                var blockingEventsTask = _databaseService.GetBlockingEventsAsync();
                var deadlocksTask = _databaseService.GetDeadlocksAsync();
                var blockingStatsTask = _databaseService.GetBlockingDeadlockStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var lockWaitStatsTask = _databaseService.GetLockWaitStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);

                var performanceTask = PerformanceTab.RefreshAllDataAsync();
                var memoryTask = MemoryTab.RefreshAllDataAsync();
                var resourceOverviewTask = RefreshResourceOverviewAsync();
                var runningJobsTask = RefreshRunningJobsAsync();
                var resourceMetricsTask = ResourceMetricsContent.RefreshAllDataAsync();
                var dailySummaryTask = DailySummaryTab.RefreshDataAsync();
                var criticalIssuesTask = CriticalIssuesTab.RefreshDataAsync();
                var systemEventsTask = SystemEventsContent.RefreshAllDataAsync();

                // Wait for everything to complete before _isRefreshing resets
                await Task.WhenAll(
                    healthTask, blockingEventsTask, deadlocksTask, blockingStatsTask, lockWaitStatsTask,
                    performanceTask, memoryTask, resourceOverviewTask, runningJobsTask,
                    resourceMetricsTask, dailySummaryTask, criticalIssuesTask, systemEventsTask);

                // Populate grids with fetched data
                var healthData = await healthTask;
                HealthDataGrid.ItemsSource = healthData;
                UpdateDataGridFilterButtonStyles(HealthDataGrid, _collectionHealthFilters);
                HealthNoDataMessage.Visibility = healthData.Count == 0 ? Visibility.Visible : Visibility.Collapsed;

                try
                {
                    var blockingEvents = await blockingEventsTask;
                    BlockingEventsDataGrid.ItemsSource = blockingEvents;
                    UpdateDataGridFilterButtonStyles(BlockingEventsDataGrid, _blockingEventsFilters);
                    BlockingEventsNoDataMessage.Visibility = blockingEvents.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                }
                catch (Exception blockingEx)
                {
                    Logger.Warning($"Could not load blocking events: {blockingEx.Message}");
                }

                try
                {
                    var deadlocks = await deadlocksTask;
                    DeadlocksDataGrid.ItemsSource = deadlocks;
                    UpdateDataGridFilterButtonStyles(DeadlocksDataGrid, _deadlocksFilters);
                    DeadlocksNoDataMessage.Visibility = deadlocks.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                }
                catch (Exception deadlockEx)
                {
                    Logger.Warning($"Could not load deadlocks: {deadlockEx.Message}");
                }

                try
                {
                    var blockingStats = await blockingStatsTask;
                    var lockWaitStats = await lockWaitStatsTask;
                    LoadBlockingStatsCharts(blockingStats, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                    LoadLockWaitStatsChart(lockWaitStats, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                }
                catch (Exception blockingStatsEx)
                {
                    Logger.Warning($"Could not load blocking/deadlock stats: {blockingStatsEx.Message}");
                }

                int failing = healthData.Count(h => h.HealthStatus == "FAILING");
                int stale = healthData.Count(h => h.HealthStatus == "STALE");
                int healthy = healthData.Count(h => h.HealthStatus == "HEALTHY");

                StatusText.Text = "Ready";
                FooterText.Text = $"Last refresh: {DateTime.Now:yyyy-MM-dd HH:mm:ss} | Server: {_serverConnection.DisplayName}";
            }
            catch (Exception ex)
            {
                StatusText.Text = "Error loading data";
                MessageBox.Show(
                    $"Error loading data:\n\n{ex.Message}",
                    "Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Error
                );
            }
            finally
            {
                RefreshButton.IsEnabled = true;
            }
        }

        private void HealthDataGrid_MouseDoubleClick(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            if (HealthDataGrid.SelectedItem is CollectionHealthItem item)
            {
                var logWindow = new CollectionLogWindow(item.CollectorName, _databaseService);
                logWindow.Owner = Window.GetWindow(this);
                logWindow.ShowDialog();
            }
        }

        private void EditSchedules_Click(object sender, RoutedEventArgs e)
        {
            var scheduleWindow = new CollectorScheduleWindow(_databaseService);
            scheduleWindow.Owner = Window.GetWindow(this);
            scheduleWindow.ShowDialog();
        }

        private void DownloadQueryPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is ExpensiveQueryItem item)
            {
                if (string.IsNullOrWhiteSpace(item.QueryPlanXml))
                {
                    MessageBox.Show(
                        "No query plan available for this query.",
                        "No Query Plan",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                var defaultFileName = $"performancemonitor_expensivequery_{timestamp}.sqlplan";

                var saveFileDialog = new SaveFileDialog
                {
                    FileName = defaultFileName,
                    DefaultExt = ".sqlplan",
                    Filter = "SQL Server Query Plan (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                    Title = "Save Query Plan"
                };

                if (saveFileDialog.ShowDialog() == true)
                {
                    try
                    {
                        File.WriteAllText(saveFileDialog.FileName, item.QueryPlanXml);
                        MessageBox.Show(
                            $"Query plan saved successfully to:\n{saveFileDialog.FileName}",
                            "Success",
                            MessageBoxButton.OK,
                            MessageBoxImage.Information
                        );
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(
                            $"Failed to save query plan:\n\n{ex.Message}",
                            "Error Saving File",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error
                        );
                    }
                }
            }
        }

        private void DownloadBlockingXml_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is BlockingEventItem item)
            {
                if (string.IsNullOrWhiteSpace(item.BlockedProcessReportXml))
                {
                    MessageBox.Show(
                        "No blocked process report XML available for this event.",
                        "No XML",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var rowNumber = BlockingEventsDataGrid.Items.IndexOf(item) + 1;
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                var defaultFileName = $"blocked_process_report_{rowNumber}_{timestamp}.xml";

                var saveFileDialog = new SaveFileDialog
                {
                    FileName = defaultFileName,
                    DefaultExt = ".xml",
                    Filter = "XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                    Title = "Save Blocked Process Report"
                };

                if (saveFileDialog.ShowDialog() == true)
                {
                    try
                    {
                        File.WriteAllText(saveFileDialog.FileName, item.BlockedProcessReportXml);
                        MessageBox.Show(
                            $"Blocked process report saved successfully to:\n{saveFileDialog.FileName}",
                            "Success",
                            MessageBoxButton.OK,
                            MessageBoxImage.Information
                        );
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(
                            $"Error saving blocked process report:\n{ex.Message}",
                            "Error",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error
                        );
                    }
                }
            }
        }

        private void DownloadDeadlockGraph_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is DeadlockItem item)
            {
                if (string.IsNullOrWhiteSpace(item.DeadlockGraph))
                {
                    MessageBox.Show(
                        "No deadlock graph available for this event.",
                        "No Graph",
                        MessageBoxButton.OK,
                        MessageBoxImage.Information
                    );
                    return;
                }

                var rowNumber = DeadlocksDataGrid.Items.IndexOf(item) + 1;
                var timestamp = DateTime.Now.ToString("yyyyMMdd_HHmmss", CultureInfo.InvariantCulture);
                var defaultFileName = $"deadlock_graph_{rowNumber}_{timestamp}.xdl";

                var saveFileDialog = new SaveFileDialog
                {
                    FileName = defaultFileName,
                    DefaultExt = ".xdl",
                    Filter = "Deadlock Files (*.xdl)|*.xdl|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                    Title = "Save Deadlock Graph"
                };

                if (saveFileDialog.ShowDialog() == true)
                {
                    try
                    {
                        File.WriteAllText(saveFileDialog.FileName, item.DeadlockGraph);
                        MessageBox.Show(
                            $"Deadlock graph saved successfully to:\n{saveFileDialog.FileName}",
                            "Success",
                            MessageBoxButton.OK,
                            MessageBoxImage.Information
                        );
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show(
                            $"Error saving deadlock graph:\n{ex.Message}",
                            "Error",
                            MessageBoxButton.OK,
                            MessageBoxImage.Error
                        );
                    }
                }
            }
        }

        private void LoadUserPreferences()
        {
            var prefs = _preferencesService.GetPreferences();

            // Blocking - uses global time range now
            _blockingHoursBack = prefs.BlockingHoursBack;
            if (prefs.BlockingUseCustomDates && !string.IsNullOrEmpty(prefs.BlockingFromDate) && !string.IsNullOrEmpty(prefs.BlockingToDate))
            {
                _blockingFromDate = DateTime.Parse(prefs.BlockingFromDate, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
                _blockingToDate = DateTime.Parse(prefs.BlockingToDate, CultureInfo.InvariantCulture, DateTimeStyles.RoundtripKind);
            }
        }

        // Date range filtering state

        private string GetDateRangeDisplayText(int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            if (fromDate.HasValue && toDate.HasValue)
            {
                return $"Showing: Custom Range ({fromDate.Value:yyyy-MM-dd} to {toDate.Value:yyyy-MM-dd})";
            }

            return hoursBack switch
            {
                1 => "Showing: Last Hour",
                6 => "Showing: Last 6 Hours",
                24 => "Showing: Last 24 Hours",
                168 => "Showing: Last 7 Days",
                _ => $"Showing: Last {hoursBack} Hours"
            };
        }

        // Blocking date range filtering state
        private int _blockingHoursBack = 24;
        private DateTime? _blockingFromDate = null;
        private DateTime? _blockingToDate = null;

        // Deadlocks date range filtering state
        private int _deadlocksHoursBack = 24;
        private DateTime? _deadlocksFromDate = null;
        private DateTime? _deadlocksToDate = null;

        // ====================================================================
        // Deadlocks Date Range Filtering
        // ====================================================================

        private async void Deadlocks_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = GetLoadingMessage();
                var deadlocks = await _databaseService.GetDeadlocksAsync(_deadlocksHoursBack, _deadlocksFromDate, _deadlocksToDate);
                DeadlocksDataGrid.ItemsSource = deadlocks;
                DeadlocksNoDataMessage.Visibility = deadlocks.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = $"Loaded {deadlocks.Count} deadlocks";
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error refreshing deadlocks:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                StatusText.Text = "Error refreshing deadlocks";
            }
        }

        // ====================================================================
        // Blocking/Deadlock Stats Tab Handlers
        // ====================================================================

        private int _blockingStatsHoursBack = 24;
        private DateTime? _blockingStatsFromDate = null;
        private DateTime? _blockingStatsToDate = null;

        private async void BlockingStats_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = GetLoadingMessage();

                var blockingStatsTask = _databaseService.GetBlockingDeadlockStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                var lockWaitStatsTask = _databaseService.GetLockWaitStatsAsync(_blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                await Task.WhenAll(blockingStatsTask, lockWaitStatsTask);

                var data = await blockingStatsTask;
                var lockWaitStats = await lockWaitStatsTask;

                // Load charts with explicit time range for proper axis scaling
                LoadBlockingStatsCharts(data, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                LoadLockWaitStatsChart(lockWaitStats, _blockingStatsHoursBack, _blockingStatsFromDate, _blockingStatsToDate);
                StatusText.Text = $"Loaded {data.Count} blocking/deadlock stats records";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading blocking/deadlock stats: {ex.Message}");
                StatusText.Text = $"Error loading blocking/deadlock stats";
            }
        }

        /// <summary>
        /// Locks the vertical axis of a chart so mouse wheel zooming only affects the time (X) axis.
        /// </summary>
        private void LockChartVerticalAxis(WpfPlot chart)
        {
            TabHelpers.LockChartVerticalAxis(chart);
        }

        private void LoadBlockingStatsCharts(List<BlockingDeadlockStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            // Calculate the time range for X-axis limits (use server time, not local time)
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            var orderedData = data?.OrderBy(d => d.CollectionTime).ToList() ?? new List<BlockingDeadlockStatsItem>();

            // Get all unique time points for consistent X-axis across all charts
            // Blocking Events Chart (raw per-interval count, not delta)
            BlockingStatsBlockingEventsChart.Plot.Clear();
            _blockingEventsHover?.Clear();
            ApplyDarkModeToChart(BlockingStatsBlockingEventsChart);
            var (blockingXs, blockingYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.BlockingEventCount));
            if (blockingXs.Length > 0)
            {
                var scatter = BlockingStatsBlockingEventsChart.Plot.Add.Scatter(blockingXs, blockingYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[0];
                _blockingEventsHover?.Add(scatter, "Blocking Events");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsBlockingEventsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsBlockingEventsChart.Plot.Axes.DateTimeTicksBottom();
            BlockingStatsBlockingEventsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsBlockingEventsChart.Plot.YLabel("Count");
            LockChartVerticalAxis(BlockingStatsBlockingEventsChart);
            BlockingStatsBlockingEventsChart.Refresh();

            // Blocking Duration Chart (raw per-interval total, not delta)
            BlockingStatsDurationChart.Plot.Clear();
            _blockingDurationHover?.Clear();
            ApplyDarkModeToChart(BlockingStatsDurationChart);
            var (durationXs, durationYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.TotalBlockingDurationMs));
            if (durationXs.Length > 0)
            {
                var scatter = BlockingStatsDurationChart.Plot.Add.Scatter(durationXs, durationYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[2];
                _blockingDurationHover?.Add(scatter, "Blocking Duration");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsDurationChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsDurationChart.Plot.Axes.DateTimeTicksBottom();
            BlockingStatsDurationChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsDurationChart.Plot.YLabel("Duration (ms)");
            LockChartVerticalAxis(BlockingStatsDurationChart);
            BlockingStatsDurationChart.Refresh();

            // Deadlock Count Chart (raw per-interval count, not delta)
            BlockingStatsDeadlocksChart.Plot.Clear();
            _deadlocksHover?.Clear();
            ApplyDarkModeToChart(BlockingStatsDeadlocksChart);
            var (deadlockXs, deadlockYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.DeadlockCount));
            if (deadlockXs.Length > 0)
            {
                var scatter = BlockingStatsDeadlocksChart.Plot.Add.Scatter(deadlockXs, deadlockYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[3];
                _deadlocksHover?.Add(scatter, "Deadlocks");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsDeadlocksChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsDeadlocksChart.Plot.Axes.DateTimeTicksBottom();
            BlockingStatsDeadlocksChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsDeadlocksChart.Plot.YLabel("Count");
            LockChartVerticalAxis(BlockingStatsDeadlocksChart);
            BlockingStatsDeadlocksChart.Refresh();

            // Deadlock Wait Time Chart (raw per-interval total, not delta)
            BlockingStatsDeadlockWaitTimeChart.Plot.Clear();
            _deadlockWaitTimeHover?.Clear();
            ApplyDarkModeToChart(BlockingStatsDeadlockWaitTimeChart);
            var (deadlockWaitXs, deadlockWaitYs) = TabHelpers.FillTimeSeriesGaps(
                orderedData.Select(d => d.CollectionTime),
                orderedData.Select(d => (double)d.TotalDeadlockWaitTimeMs));
            if (deadlockWaitXs.Length > 0)
            {
                var scatter = BlockingStatsDeadlockWaitTimeChart.Plot.Add.Scatter(deadlockWaitXs, deadlockWaitYs);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[4];
                _deadlockWaitTimeHover?.Add(scatter, "Deadlock Wait Time");
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = BlockingStatsDeadlockWaitTimeChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }
            BlockingStatsDeadlockWaitTimeChart.Plot.Axes.DateTimeTicksBottom();
            BlockingStatsDeadlockWaitTimeChart.Plot.Axes.SetLimitsX(xMin, xMax);
            BlockingStatsDeadlockWaitTimeChart.Plot.YLabel("Duration (ms)");
            LockChartVerticalAxis(BlockingStatsDeadlockWaitTimeChart);
            BlockingStatsDeadlockWaitTimeChart.Refresh();
        }

        private void LoadLockWaitStatsChart(List<LockWaitStatsItem> data, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            // Calculate the time range for X-axis limits (use server time, not local time)
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(LockWaitStatsChart, out var existingPanel) && existingPanel != null)
            {
                LockWaitStatsChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[LockWaitStatsChart] = null;
            }
            LockWaitStatsChart.Plot.Clear();
            _lockWaitStatsHover?.Clear();
            ApplyDarkModeToChart(LockWaitStatsChart);

            // Get all unique time points across all wait types for gap filling
            // Group by wait type and plot each as a separate series
            var waitTypes = data.Select(d => d.WaitType).Distinct().OrderBy(w => w).ToList();
            var colors = TabHelpers.ChartColors;

            int colorIndex = 0;
            foreach (var waitType in waitTypes)
            {
                var waitTypeData = data.Where(d => d.WaitType == waitType).OrderBy(d => d.CollectionTime).ToList();
                if (waitTypeData.Count > 0)
                {
                    // Fill gaps with zeros so lines are continuous
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        waitTypeData.Select(d => d.CollectionTime),
                        waitTypeData.Select(d => (double)d.WaitTimeMsPerSecond));

                    var scatter = LockWaitStatsChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];
                    var lockLabel = waitType.Replace("LCK_M_", "").Replace("LCK_", "");
                    scatter.LegendText = lockLabel;
                    _lockWaitStatsHover?.Add(scatter, lockLabel);
                    colorIndex++;
                }
            }

            if (data.Count == 0)
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = LockWaitStatsChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            LockWaitStatsChart.Plot.Axes.DateTimeTicksBottom();
            LockWaitStatsChart.Plot.Axes.SetLimitsX(xMin, xMax);
            LockWaitStatsChart.Plot.YLabel("Wait Time (ms/sec)");
            _legendPanels[LockWaitStatsChart] = LockWaitStatsChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
            LockWaitStatsChart.Plot.Legend.FontSize = 12;
            LockChartVerticalAxis(LockWaitStatsChart);
            LockWaitStatsChart.Refresh();
        }

        // ====================================================================
        // Context Menu Event Handlers
        // ====================================================================

        private void CopyCell_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.CurrentCell.Item != null)
                {
                    var cellContent = GetCellContent(dataGrid, dataGrid.CurrentCell);
                    if (cellContent != null)
                    {
                        /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                        Clipboard.SetDataObject(cellContent, false);
                    }
                }
            }
        }

        private void CopyRow_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.SelectedItem != null)
                {
                    var rowText = GetRowAsText(dataGrid, dataGrid.SelectedItem);
                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(rowText, false);
                }
            }
        }

        private void CopyAllRows_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var sb = new StringBuilder();

                    // Add headers
                    var headers = new List<string>();
                    foreach (var column in dataGrid.Columns)
                    {
                        if (column is DataGridBoundColumn boundColumn)
                        {
                            headers.Add(GetColumnHeader(column));
                        }
                    }
                    sb.AppendLine(string.Join("	", headers));

                    // Add all rows
                    foreach (var item in dataGrid.Items)
                    {
                        sb.AppendLine(GetRowAsText(dataGrid, item));
                    }

                    /* Use SetDataObject with copy=false to avoid WPF's problematic Clipboard.Flush() */
                    Clipboard.SetDataObject(sb.ToString(), false);
                }
            }
        }

        private void ExportToCsv_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Parent is ContextMenu contextMenu)
            {
                var dataGrid = FindDataGridFromContextMenu(contextMenu);
                if (dataGrid != null && dataGrid.Items.Count > 0)
                {
                    var saveFileDialog = new SaveFileDialog
                    {
                        FileName = $"export_{DateTime.Now:yyyyMMdd_HHmmss}.csv",
                        DefaultExt = ".csv",
                        Filter = "CSV Files (*.csv)|*.csv|All Files (*.*)|*.*"
                    };

                    if (saveFileDialog.ShowDialog() == true)
                    {
                        try
                        {
                            var sb = new StringBuilder();

                            // Add headers
                            var headers = new List<string>();
                            foreach (var column in dataGrid.Columns)
                            {
                                if (column is DataGridBoundColumn)
                                {
                                    headers.Add(EscapeCsvField(GetColumnHeader(column)));
                                }
                            }
                            sb.AppendLine(string.Join(",", headers));

                            // Add all rows
                            foreach (var item in dataGrid.Items)
                            {
                                var values = GetRowValues(dataGrid, item);
                                sb.AppendLine(string.Join(",", values.Select(v => EscapeCsvField(v))));
                            }

                            File.WriteAllText(saveFileDialog.FileName, sb.ToString());
                            MessageBox.Show($"Data exported successfully to:\n{saveFileDialog.FileName}", "Export Complete", MessageBoxButton.OK, MessageBoxImage.Information);
                        }
                        catch (Exception ex)
                        {
                            MessageBox.Show($"Error exporting data:\n\n{ex.Message}", "Export Error", MessageBoxButton.OK, MessageBoxImage.Error);
                        }
                    }
                }
            }
        }

        private DataGrid? FindDataGridFromContextMenu(ContextMenu contextMenu)
        {
            if (contextMenu.PlacementTarget is DataGridRow row)
            {
                return FindParent<DataGrid>(row);
            }
            return null;
        }

        private T? FindParent<T>(DependencyObject child) where T : DependencyObject
        {
            return TabHelpers.FindParent<T>(child);
        }

        private string GetCellContent(DataGrid dataGrid, DataGridCellInfo cellInfo)
        {
            var column = cellInfo.Column as DataGridBoundColumn;
            if (column?.Binding is Binding binding && binding.Path != null)
            {
                var propertyName = binding.Path.Path;
                var property = cellInfo.Item.GetType().GetProperty(propertyName);
                if (property != null)
                {
                    var value = property.GetValue(cellInfo.Item);
                    return value?.ToString() ?? string.Empty;
                }
            }
            return string.Empty;
        }

        private string GetRowAsText(DataGrid dataGrid, object item)
        {
            var values = GetRowValues(dataGrid, item);
            return string.Join("	", values);
        }

        private List<string> GetRowValues(DataGrid dataGrid, object item)
        {
            var values = new List<string>();
            foreach (var column in dataGrid.Columns)
            {
                if (column is DataGridBoundColumn boundColumn && boundColumn.Binding is Binding binding)
                {
                    var propertyName = binding.Path.Path;
                    var property = item.GetType().GetProperty(propertyName);
                    if (property != null)
                    {
                        var value = property.GetValue(item);
                        values.Add(value?.ToString() ?? string.Empty);
                    }
                }
            }
            return values;
        }

        private string GetColumnHeader(DataGridColumn column)
        {
            return TabHelpers.GetColumnHeader(column);
        }

        private string EscapeCsvField(string field)
        {
            return TabHelpers.EscapeCsvField(field);
        }

        // ====================================================================
        // Blocking Refresh Handler
        // ====================================================================

        private async void Blocking_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Refreshing blocking events...";
                var blocking = await _databaseService.GetBlockingEventsAsync(_blockingHoursBack, _blockingFromDate, _blockingToDate);
                BlockingEventsDataGrid.ItemsSource = blocking;
                BlockingEventsNoDataMessage.Visibility = blocking.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = $"Loaded {blocking.Count} blocking events";
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error refreshing blocking events:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                StatusText.Text = "Error refreshing blocking events";
            }
        }

        // ====================================================================
        // Collection Health
        // ====================================================================

        private int _collectionHealthHoursBack = 24;
        private DateTime? _collectionHealthFromDate = null;
        private DateTime? _collectionHealthToDate = null;

        private async void CollectionHealth_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Refreshing collection health...";
                var healthData = await _databaseService.GetCollectionHealthAsync();
                HealthDataGrid.ItemsSource = healthData;
                HealthNoDataMessage.Visibility = healthData.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = "Ready";
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Error refreshing collection health:\n\n{ex.Message}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
                StatusText.Text = "Error";
            }
        }

        // ====================================================================
        // Configuration Changes (System Events Tab)
        // ====================================================================

        #region Configuration Changes

        // Server Configuration Changes
        private int _serverConfigChangesHoursBack = 24;
        private DateTime? _serverConfigChangesFromDate;
        private DateTime? _serverConfigChangesToDate;

        private async void ServerConfigChanges_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Loading server configuration changes...";
                var data = await _databaseService.GetServerConfigChangesAsync(_serverConfigChangesHoursBack, _serverConfigChangesFromDate, _serverConfigChangesToDate);
                ServerConfigChangesDataGrid.ItemsSource = data;
                UpdateDataGridFilterButtonStyles(ServerConfigChangesDataGrid, _serverConfigChangesFilters);
                ServerConfigChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = $"Loaded {data.Count} server configuration change records";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading server configuration changes: {ex.Message}");
                StatusText.Text = "Error loading server configuration changes";
            }
        }

        private void ServerConfigChangesBoolFilter_Changed(object sender, SelectionChangedEventArgs e)
        {
            ApplyServerConfigChangesPopupFilters();
        }

        // Database Configuration Changes
        private int _dbConfigChangesHoursBack = 24;
        private DateTime? _dbConfigChangesFromDate;
        private DateTime? _dbConfigChangesToDate;

        private async void DatabaseConfigChanges_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Loading database configuration changes...";
                var data = await _databaseService.GetDatabaseConfigChangesAsync(_dbConfigChangesHoursBack, _dbConfigChangesFromDate, _dbConfigChangesToDate);
                DatabaseConfigChangesDataGrid.ItemsSource = data;
                UpdateDataGridFilterButtonStyles(DatabaseConfigChangesDataGrid, _dbConfigChangesFilters);
                DatabaseConfigChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = $"Loaded {data.Count} database configuration change records";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading database configuration changes: {ex.Message}");
                StatusText.Text = "Error loading database configuration changes";
            }
        }

        // Trace Flag Changes
        private int _traceFlagChangesHoursBack = 24;
        private DateTime? _traceFlagChangesFromDate;
        private DateTime? _traceFlagChangesToDate;

        private async void TraceFlagChanges_Refresh_Click(object? sender, RoutedEventArgs e)
        {
            try
            {
                StatusText.Text = "Loading trace flag changes...";
                var data = await _databaseService.GetTraceFlagChangesAsync(_traceFlagChangesHoursBack, _traceFlagChangesFromDate, _traceFlagChangesToDate);
                TraceFlagChangesDataGrid.ItemsSource = data;
                UpdateDataGridFilterButtonStyles(TraceFlagChangesDataGrid, _traceFlagChangesFilters);
                TraceFlagChangesNoDataMessage.Visibility = data.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
                StatusText.Text = $"Loaded {data.Count} trace flag change records";
            }
            catch (Exception ex)
            {
                Logger.Error($"Error loading trace flag changes: {ex.Message}");
                StatusText.Text = "Error loading trace flag changes";
            }
        }

        private void TraceFlagChangesBoolFilter_Changed(object sender, SelectionChangedEventArgs e)
        {
            ApplyTraceFlagChangesPopupFilters();
        }

        #endregion

        // ====================================================================
        // Resource Overview Tab (on Overview tab)
        // ====================================================================

        #region Resource Overview

        private int _resourceOverviewHoursBack = 24;
        private DateTime? _resourceOverviewFromDate = null;
        private DateTime? _resourceOverviewToDate = null;

        private async Task RefreshResourceOverviewAsync()
        {
            if (_databaseService == null) return;

            try
            {
                // Load all four charts in parallel
                var cpuTask = _databaseService.GetCpuDataAsync(_resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
                var memoryTask = _databaseService.GetMemoryDataAsync(_resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
                var ioTask = _databaseService.GetFileIoDataAsync(_resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
                var waitTask = _databaseService.GetWaitStatsDataAsync(_resourceOverviewHoursBack, 5, _resourceOverviewFromDate, _resourceOverviewToDate);

                await Task.WhenAll(cpuTask, memoryTask, ioTask, waitTask);

                // Load CPU chart
                LoadResourceOverviewCpuChart(await cpuTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);

                // Load Memory chart
                LoadResourceOverviewMemoryChart(await memoryTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);

                // Load I/O chart
                LoadResourceOverviewIoChart(await ioTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);

                // Load Wait Stats chart
                LoadResourceOverviewWaitChart(await waitTask, _resourceOverviewHoursBack, _resourceOverviewFromDate, _resourceOverviewToDate);
            }
            catch (Exception ex)
            {
                Logger.Error("Error refreshing Resource Overview charts", ex);
            }
        }

        private async Task RefreshRunningJobsAsync()
        {
            if (_databaseService == null) return;

            try
            {
                var runningJobs = await _databaseService.GetRunningJobsAsync();
                RunningJobsDataGrid.ItemsSource = runningJobs;
                RunningJobsNoDataMessage.Visibility = runningJobs.Count == 0 ? Visibility.Visible : Visibility.Collapsed;
            }
            catch (Exception ex)
            {
                Logger.Warning($"Could not load running jobs: {ex.Message}");
                RunningJobsNoDataMessage.Visibility = Visibility.Visible;
            }
        }

        private void LoadResourceOverviewCpuChart(IEnumerable<CpuDataPoint> cpuData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewCpuChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewCpuChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewCpuChart] = null;
            }
            ResourceOverviewCpuChart.Plot.Clear();
            _resourceOverviewCpuHover?.Clear();
            ApplyDarkModeToChart(ResourceOverviewCpuChart);

            var dataList = cpuData?.OrderBy(d => d.SampleTime).ToList() ?? new List<CpuDataPoint>();

            // Build time series with boundary points for continuous lines
            var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.SampleTime),
                dataList.Select(d => (double)d.SqlServerCpu));

            if (xs.Length > 0)
            {
                var scatter = ResourceOverviewCpuChart.Plot.Add.Scatter(xs, ys);
                scatter.LineWidth = 2;
                scatter.MarkerSize = 5;
                scatter.Color = TabHelpers.ChartColors[0];
                scatter.LegendText = "SQL CPU %";
                _resourceOverviewCpuHover?.Add(scatter, "SQL CPU %");

                _legendPanels[ResourceOverviewCpuChart] = ResourceOverviewCpuChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewCpuChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewCpuChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewCpuChart.Plot.Axes.DateTimeTicksBottom();
            ResourceOverviewCpuChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewCpuChart.Plot.Axes.SetLimitsY(0, 100);
            ResourceOverviewCpuChart.Plot.YLabel("CPU %");
            LockChartVerticalAxis(ResourceOverviewCpuChart);
            ResourceOverviewCpuChart.Refresh();
        }

        private void LoadResourceOverviewMemoryChart(IEnumerable<MemoryDataPoint> memoryData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewMemoryChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewMemoryChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewMemoryChart] = null;
            }
            ResourceOverviewMemoryChart.Plot.Clear();
            _resourceOverviewMemoryHover?.Clear();
            ApplyDarkModeToChart(ResourceOverviewMemoryChart);

            var dataList = memoryData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<MemoryDataPoint>();
            // Buffer Pool series with gap filling
            var (bufferXs, bufferYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.BufferPoolMb));

            // Memory Grants series with gap filling
            var (grantsXs, grantsYs) = TabHelpers.FillTimeSeriesGaps(
                dataList.Select(d => d.CollectionTime),
                dataList.Select(d => (double)d.GrantedMemoryMb));

            if (bufferXs.Length > 0)
            {
                var bufferScatter = ResourceOverviewMemoryChart.Plot.Add.Scatter(bufferXs, bufferYs);
                bufferScatter.LineWidth = 2;
                bufferScatter.MarkerSize = 5;
                bufferScatter.Color = TabHelpers.ChartColors[4];
                bufferScatter.LegendText = "Buffer Pool";
                _resourceOverviewMemoryHover?.Add(bufferScatter, "Buffer Pool");

                var grantsScatter = ResourceOverviewMemoryChart.Plot.Add.Scatter(grantsXs, grantsYs);
                grantsScatter.LineWidth = 2;
                grantsScatter.MarkerSize = 5;
                grantsScatter.Color = TabHelpers.ChartColors[2];
                grantsScatter.LegendText = "Memory Grants";
                _resourceOverviewMemoryHover?.Add(grantsScatter, "Memory Grants");

                _legendPanels[ResourceOverviewMemoryChart] = ResourceOverviewMemoryChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewMemoryChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewMemoryChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewMemoryChart.Plot.Axes.DateTimeTicksBottom();
            ResourceOverviewMemoryChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewMemoryChart.Plot.YLabel("MB");
            LockChartVerticalAxis(ResourceOverviewMemoryChart);
            ResourceOverviewMemoryChart.Refresh();
        }

        private void LoadResourceOverviewIoChart(IEnumerable<FileIoDataPoint> ioData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewIoChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewIoChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewIoChart] = null;
            }
            ResourceOverviewIoChart.Plot.Clear();
            _resourceOverviewIoHover?.Clear();
            ApplyDarkModeToChart(ResourceOverviewIoChart);

            var dataList = ioData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<FileIoDataPoint>();
            int bucketMinutes = hoursBack <= 1 ? 1 : hoursBack <= 6 ? 5 : hoursBack <= 24 ? 15 : 60;

            var aggregated = dataList
                .GroupBy(d => new DateTime(
                    d.CollectionTime.Year, d.CollectionTime.Month, d.CollectionTime.Day,
                    d.CollectionTime.Hour, (d.CollectionTime.Minute / bucketMinutes) * bucketMinutes, 0))
                .Select(g => new
                {
                    BucketTime = g.Key,
                    AvgReadLatency = g.Average(x => (double)x.AvgReadLatencyMs),
                    AvgWriteLatency = g.Average(x => (double)x.AvgWriteLatencyMs)
                })
                .OrderBy(x => x.BucketTime)
                .ToList();

            // Read latency series with gap filling
            var (readXs, readYs) = TabHelpers.FillTimeSeriesGaps(
                aggregated.Select(d => d.BucketTime),
                aggregated.Select(d => d.AvgReadLatency));

            // Write latency series with gap filling
            var (writeXs, writeYs) = TabHelpers.FillTimeSeriesGaps(
                aggregated.Select(d => d.BucketTime),
                aggregated.Select(d => d.AvgWriteLatency));

            if (readXs.Length > 0)
            {
                var readScatter = ResourceOverviewIoChart.Plot.Add.Scatter(readXs, readYs);
                readScatter.LineWidth = 2;
                readScatter.MarkerSize = 5;
                readScatter.Color = TabHelpers.ChartColors[1];
                readScatter.LegendText = "Read ms";
                _resourceOverviewIoHover?.Add(readScatter, "Read ms");

                var writeScatter = ResourceOverviewIoChart.Plot.Add.Scatter(writeXs, writeYs);
                writeScatter.LineWidth = 2;
                writeScatter.MarkerSize = 5;
                writeScatter.Color = TabHelpers.ChartColors[2];
                writeScatter.LegendText = "Write ms";
                _resourceOverviewIoHover?.Add(writeScatter, "Write ms");

                _legendPanels[ResourceOverviewIoChart] = ResourceOverviewIoChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewIoChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewIoChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewIoChart.Plot.Axes.DateTimeTicksBottom();
            ResourceOverviewIoChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewIoChart.Plot.Axes.AutoScaleY();
            ResourceOverviewIoChart.Plot.YLabel("Latency (ms)");
            LockChartVerticalAxis(ResourceOverviewIoChart);
            ResourceOverviewIoChart.Refresh();
        }

        private void LoadResourceOverviewWaitChart(IEnumerable<WaitStatsDataPoint> waitData, int hoursBack, DateTime? fromDate, DateTime? toDate)
        {
            DateTime rangeEnd = toDate ?? Helpers.ServerTimeHelper.ServerNow;
            DateTime rangeStart = fromDate ?? rangeEnd.AddHours(-hoursBack);
            double xMin = rangeStart.ToOADate();
            double xMax = rangeEnd.ToOADate();

            if (_legendPanels.TryGetValue(ResourceOverviewWaitChart, out var existingPanel) && existingPanel != null)
            {
                ResourceOverviewWaitChart.Plot.Axes.Remove(existingPanel);
                _legendPanels[ResourceOverviewWaitChart] = null;
            }
            ResourceOverviewWaitChart.Plot.Clear();
            _resourceOverviewWaitHover?.Clear();
            ApplyDarkModeToChart(ResourceOverviewWaitChart);

            var dataList = waitData?.OrderBy(d => d.CollectionTime).ToList() ?? new List<WaitStatsDataPoint>();

            // Get all unique time points across all wait types for gap filling
            if (dataList.Count > 0)
            {
                var topWaitTypes = dataList
                    .GroupBy(d => d.WaitType)
                    .Select(g => new { WaitType = g.Key, TotalWait = g.Sum(x => x.WaitTimeMsPerSecond) })
                    .OrderByDescending(x => x.TotalWait)
                    .Take(5)
                    .Select(x => x.WaitType)
                    .ToList();

                var colors = TabHelpers.ChartColors;
                int colorIndex = 0;

                foreach (var waitType in topWaitTypes)
                {
                    var waitTypeData = dataList.Where(d => d.WaitType == waitType).ToList();
                    if (waitTypeData.Count < 2) continue;

                    // Fill gaps with zeros so lines are continuous
                    var (xs, ys) = TabHelpers.FillTimeSeriesGaps(
                        waitTypeData.Select(d => d.CollectionTime),
                        waitTypeData.Select(d => (double)d.WaitTimeMsPerSecond));

                    var scatter = ResourceOverviewWaitChart.Plot.Add.Scatter(xs, ys);
                    scatter.LineWidth = 2;
                    scatter.MarkerSize = 5;
                    scatter.Color = colors[colorIndex % colors.Length];
                    var waitLabel = waitType.Length > 15 ? waitType.Substring(0, 15) + "..." : waitType;
                    scatter.LegendText = waitLabel;
                    _resourceOverviewWaitHover?.Add(scatter, waitLabel);
                    colorIndex++;
                }

                _legendPanels[ResourceOverviewWaitChart] = ResourceOverviewWaitChart.Plot.ShowLegend(ScottPlot.Edge.Bottom);
                ResourceOverviewWaitChart.Plot.Legend.FontSize = 12;
            }
            else
            {
                double xCenter = xMin + (xMax - xMin) / 2;
                var noDataText = ResourceOverviewWaitChart.Plot.Add.Text("No data for selected time range", xCenter, 0.5);
                noDataText.LabelFontSize = 14;
                noDataText.LabelFontColor = ScottPlot.Colors.Gray;
                noDataText.LabelAlignment = ScottPlot.Alignment.MiddleCenter;
            }

            ResourceOverviewWaitChart.Plot.Axes.DateTimeTicksBottom();
            ResourceOverviewWaitChart.Plot.Axes.SetLimitsX(xMin, xMax);
            ResourceOverviewWaitChart.Plot.Axes.AutoScaleY();
            ResourceOverviewWaitChart.Plot.YLabel("Wait Time (ms/sec)");
            LockChartVerticalAxis(ResourceOverviewWaitChart);
            ResourceOverviewWaitChart.Refresh();
        }

        #endregion

        // ====================================================================
        // Column Filter Popup Infrastructure
        // ====================================================================

        #region Filter Popup Infrastructure

        private void ShowFilterPopup(Button button, string columnName, string dataGridName)
        {
            if (_filterPopup == null)
            {
                _filterPopupContent = new ColumnFilterPopup();
                _filterPopupContent.FilterApplied += FilterPopup_FilterApplied;
                _filterPopupContent.FilterCleared += FilterPopup_FilterCleared;

                _filterPopup = new Popup
                {
                    Child = _filterPopupContent,
                    StaysOpen = false,
                    Placement = PlacementMode.Bottom,
                    AllowsTransparency = true
                };
            }

            _currentFilterDataGrid = dataGridName;
            _currentFilterButton = button;

            // Get existing filter state
            ColumnFilterState? existingFilter = null;
            switch (dataGridName)
            {
                case "ServerConfigChanges":
                    _serverConfigChangesFilters.TryGetValue(columnName, out existingFilter);
                    break;
                case "DbConfigChanges":
                    _dbConfigChangesFilters.TryGetValue(columnName, out existingFilter);
                    break;
                case "TraceFlagChanges":
                    _traceFlagChangesFilters.TryGetValue(columnName, out existingFilter);
                    break;
                case "CollectionHealth":
                    _collectionHealthFilters.TryGetValue(columnName, out existingFilter);
                    break;
                case "BlockingEvents":
                    _blockingEventsFilters.TryGetValue(columnName, out existingFilter);
                    break;
                case "Deadlocks":
                    _deadlocksFilters.TryGetValue(columnName, out existingFilter);
                    break;
            }

            _filterPopupContent!.Initialize(columnName, existingFilter);
            _filterPopup.PlacementTarget = button;
            _filterPopup.IsOpen = true;
        }

        private void FilterPopup_FilterApplied(object? sender, FilterAppliedEventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;

            switch (_currentFilterDataGrid)
            {
                case "ServerConfigChanges":
                    UpdateFilterState(_serverConfigChangesFilters, e.FilterState);
                    ApplyServerConfigChangesPopupFilters();
                    UpdateDataGridFilterButtonStyles(ServerConfigChangesDataGrid, _serverConfigChangesFilters);
                    break;
                case "DbConfigChanges":
                    UpdateFilterState(_dbConfigChangesFilters, e.FilterState);
                    ApplyDbConfigChangesFilters();
                    UpdateDataGridFilterButtonStyles(DatabaseConfigChangesDataGrid, _dbConfigChangesFilters);
                    break;
                case "TraceFlagChanges":
                    UpdateFilterState(_traceFlagChangesFilters, e.FilterState);
                    ApplyTraceFlagChangesPopupFilters();
                    UpdateDataGridFilterButtonStyles(TraceFlagChangesDataGrid, _traceFlagChangesFilters);
                    break;
                case "CollectionHealth":
                    UpdateFilterState(_collectionHealthFilters, e.FilterState);
                    ApplyCollectionHealthFilters();
                    UpdateDataGridFilterButtonStyles(HealthDataGrid, _collectionHealthFilters);
                    break;
                case "BlockingEvents":
                    UpdateFilterState(_blockingEventsFilters, e.FilterState);
                    ApplyBlockingEventsFilters();
                    UpdateDataGridFilterButtonStyles(BlockingEventsDataGrid, _blockingEventsFilters);
                    break;
                case "Deadlocks":
                    UpdateFilterState(_deadlocksFilters, e.FilterState);
                    ApplyDeadlocksFilters();
                    UpdateDataGridFilterButtonStyles(DeadlocksDataGrid, _deadlocksFilters);
                    break;
            }
        }

        private void FilterPopup_FilterCleared(object? sender, EventArgs e)
        {
            if (_filterPopup != null)
                _filterPopup.IsOpen = false;
        }

        private void UpdateFilterState(Dictionary<string, ColumnFilterState> filters, ColumnFilterState filterState)
        {
            if (filterState.IsActive)
            {
                filters[filterState.ColumnName] = filterState;
            }
            else
            {
                filters.Remove(filterState.ColumnName);
            }
        }

        private void UpdateFilterButtonVisual(Button? button, ColumnFilterState filterState)
        {
            if (button == null) return;

            bool isActive = filterState.IsActive;

            // Create a TextBlock with the filter icon - gold when active, white when inactive
            var textBlock = new System.Windows.Controls.TextBlock
            {
                Text = "",
                FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                Foreground = isActive
                ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                    : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
            };
            button.Content = textBlock;

            // Update tooltip to show current filter
            button.ToolTip = isActive
                ? $"Filter: {filterState.DisplayText}\n(Click to modify)"
                : "Click to filter";
        }


        private void UpdateDataGridFilterButtonStyles(DataGrid dataGrid, Dictionary<string, ColumnFilterState> filters)
        {
            foreach (var column in dataGrid.Columns)
            {
                if (column.Header is StackPanel headerPanel)
                {
                    var filterButton = headerPanel.Children.OfType<Button>().FirstOrDefault();
                    if (filterButton != null && filterButton.Tag is string columnName)
                    {
                        bool hasActiveFilter = filters.TryGetValue(columnName, out var filter) && filter.IsActive;

                        // Create a TextBlock with the filter icon - gold when active, white when inactive
                        var textBlock = new System.Windows.Controls.TextBlock
                        {
                            Text = "",
                            FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                            Foreground = hasActiveFilter
                            ? new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xD7, 0x00)) // Gold
                                : new System.Windows.Media.SolidColorBrush(System.Windows.Media.Color.FromRgb(0xFF, 0xFF, 0xFF)) // White
                        };
                        filterButton.Content = textBlock;

                        filterButton.ToolTip = hasActiveFilter && filter != null
                            ? $"Filter: {filter.DisplayText}\n(Click to modify)"
                            : "Click to filter";
                    }
                }
            }
        }

        #endregion

        // ====================================================================
        // Server Config Changes Filter Handlers
        // ====================================================================

        #region Server Config Changes Filters

        private void ServerConfigChangesFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "ServerConfigChanges");
        }

        private void ApplyServerConfigChangesPopupFilters()
        {
            if (_serverConfigChangesUnfilteredData == null)
            {
                _serverConfigChangesUnfilteredData = ServerConfigChangesDataGrid.ItemsSource as List<ServerConfigChangeItem>;
                if (_serverConfigChangesUnfilteredData == null && ServerConfigChangesDataGrid.ItemsSource != null)
                {
                    _serverConfigChangesUnfilteredData = (ServerConfigChangesDataGrid.ItemsSource as IEnumerable<ServerConfigChangeItem>)?.ToList();
                }
            }

            if (_serverConfigChangesUnfilteredData == null) return;

            // Get boolean filter states
            var restartFilter = (ServerConfigChangesRestartFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();
            var dynamicFilter = (ServerConfigChangesDynamicFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();
            var advancedFilter = (ServerConfigChangesAdvancedFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();

            if (_serverConfigChangesFilters.Count == 0 && restartFilter == "All" && dynamicFilter == "All" && advancedFilter == "All")
            {
                ServerConfigChangesDataGrid.ItemsSource = _serverConfigChangesUnfilteredData;
                return;
            }

            var filteredData = _serverConfigChangesUnfilteredData.Where(item =>
            {
                // Apply popup filters
                foreach (var filter in _serverConfigChangesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }

                // Apply boolean filters
                if (restartFilter != "All" && restartFilter != null)
                {
                    bool expected = restartFilter == "True";
                    if (item.RequiresRestart != expected) return false;
                }
                if (dynamicFilter != "All" && dynamicFilter != null)
                {
                    bool expected = dynamicFilter == "True";
                    if (item.IsDynamic != expected) return false;
                }
                if (advancedFilter != "All" && advancedFilter != null)
                {
                    bool expected = advancedFilter == "True";
                    if (item.IsAdvanced != expected) return false;
                }

                return true;
            }).ToList();

            ServerConfigChangesDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================
        // Database Config Changes Filter Handlers
        // ====================================================================

        #region Database Config Changes Filters

        private void DbConfigChangesFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "DbConfigChanges");
        }

        private void ApplyDbConfigChangesFilters()
        {
            if (_dbConfigChangesUnfilteredData == null)
            {
                _dbConfigChangesUnfilteredData = DatabaseConfigChangesDataGrid.ItemsSource as List<DatabaseConfigChangeItem>;
                if (_dbConfigChangesUnfilteredData == null && DatabaseConfigChangesDataGrid.ItemsSource != null)
                {
                    _dbConfigChangesUnfilteredData = (DatabaseConfigChangesDataGrid.ItemsSource as IEnumerable<DatabaseConfigChangeItem>)?.ToList();
                }
            }

            if (_dbConfigChangesUnfilteredData == null) return;

            if (_dbConfigChangesFilters.Count == 0)
            {
                DatabaseConfigChangesDataGrid.ItemsSource = _dbConfigChangesUnfilteredData;
                return;
            }

            var filteredData = _dbConfigChangesUnfilteredData.Where(item =>
            {
                foreach (var filter in _dbConfigChangesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            DatabaseConfigChangesDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================
        // Trace Flag Changes Filter Handlers
        // ====================================================================

        #region Trace Flag Changes Filters

        private void TraceFlagChangesFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "TraceFlagChanges");
        }

        private void ApplyTraceFlagChangesPopupFilters()
        {
            if (_traceFlagChangesUnfilteredData == null)
            {
                _traceFlagChangesUnfilteredData = TraceFlagChangesDataGrid.ItemsSource as List<TraceFlagChangeItem>;
                if (_traceFlagChangesUnfilteredData == null && TraceFlagChangesDataGrid.ItemsSource != null)
                {
                    _traceFlagChangesUnfilteredData = (TraceFlagChangesDataGrid.ItemsSource as IEnumerable<TraceFlagChangeItem>)?.ToList();
                }
            }

            if (_traceFlagChangesUnfilteredData == null) return;

            // Get boolean filter states
            var globalFilter = (TraceFlagChangesGlobalFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();
            var sessionFilter = (TraceFlagChangesSessionFilterCombo?.SelectedItem as ComboBoxItem)?.Content?.ToString();

            if (_traceFlagChangesFilters.Count == 0 && globalFilter == "All" && sessionFilter == "All")
            {
                TraceFlagChangesDataGrid.ItemsSource = _traceFlagChangesUnfilteredData;
                return;
            }

            var filteredData = _traceFlagChangesUnfilteredData.Where(item =>
            {
                // Apply popup filters
                foreach (var filter in _traceFlagChangesFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }

                // Apply boolean filters
                if (globalFilter != "All" && globalFilter != null)
                {
                    bool expected = globalFilter == "True";
                    if (item.IsGlobal != expected) return false;
                }
                if (sessionFilter != "All" && sessionFilter != null)
                {
                    bool expected = sessionFilter == "True";
                    if (item.IsSession != expected) return false;
                }

                return true;
            }).ToList();

            TraceFlagChangesDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================
        // Collection Health Filter Handlers
        // ====================================================================

        #region Collection Health Filters

        private void CollectionHealthFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "CollectionHealth");
        }

        private void ApplyCollectionHealthFilters()
        {
            if (_collectionHealthUnfilteredData == null)
            {
                _collectionHealthUnfilteredData = HealthDataGrid.ItemsSource as List<CollectionHealthItem>;
                if (_collectionHealthUnfilteredData == null && HealthDataGrid.ItemsSource != null)
                {
                    _collectionHealthUnfilteredData = (HealthDataGrid.ItemsSource as IEnumerable<CollectionHealthItem>)?.ToList();
                }
            }

            if (_collectionHealthUnfilteredData == null) return;

            if (_collectionHealthFilters.Count == 0)
            {
                HealthDataGrid.ItemsSource = _collectionHealthUnfilteredData;
                return;
            }

            var filteredData = _collectionHealthUnfilteredData.Where(item =>
            {
                foreach (var filter in _collectionHealthFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            HealthDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================
        // Blocking Events Filter Handlers
        // ====================================================================

        #region Blocking Events Filters

        private void BlockingEventsFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "BlockingEvents");
        }

        private void ApplyBlockingEventsFilters()
        {
            if (_blockingEventsUnfilteredData == null)
            {
                _blockingEventsUnfilteredData = BlockingEventsDataGrid.ItemsSource as List<BlockingEventItem>;
                if (_blockingEventsUnfilteredData == null && BlockingEventsDataGrid.ItemsSource != null)
                {
                    _blockingEventsUnfilteredData = (BlockingEventsDataGrid.ItemsSource as IEnumerable<BlockingEventItem>)?.ToList();
                }
            }

            if (_blockingEventsUnfilteredData == null) return;

            if (_blockingEventsFilters.Count == 0)
            {
                BlockingEventsDataGrid.ItemsSource = _blockingEventsUnfilteredData;
                return;
            }

            var filteredData = _blockingEventsUnfilteredData.Where(item =>
            {
                foreach (var filter in _blockingEventsFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            BlockingEventsDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================
        // Deadlocks Filter Handlers
        // ====================================================================

        #region Deadlocks Filters

        private void DeadlocksFilter_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not Button button || button.Tag is not string columnName) return;
            ShowFilterPopup(button, columnName, "Deadlocks");
        }

        private void ApplyDeadlocksFilters()
        {
            if (_deadlocksUnfilteredData == null)
            {
                _deadlocksUnfilteredData = DeadlocksDataGrid.ItemsSource as List<DeadlockItem>;
                if (_deadlocksUnfilteredData == null && DeadlocksDataGrid.ItemsSource != null)
                {
                    _deadlocksUnfilteredData = (DeadlocksDataGrid.ItemsSource as IEnumerable<DeadlockItem>)?.ToList();
                }
            }

            if (_deadlocksUnfilteredData == null) return;

            if (_deadlocksFilters.Count == 0)
            {
                DeadlocksDataGrid.ItemsSource = _deadlocksUnfilteredData;
                return;
            }

            var filteredData = _deadlocksUnfilteredData.Where(item =>
            {
                foreach (var filter in _deadlocksFilters.Values)
                {
                    if (filter.IsActive && !DataGridFilterService.MatchesFilter(item, filter))
                    {
                        return false;
                    }
                }
                return true;
            }).ToList();

            DeadlocksDataGrid.ItemsSource = filteredData;
        }

        #endregion

        // ====================================================================

        #region Badge Updates

        /// <summary>
        /// Gets the server ID for this tab.
        /// </summary>
        public string ServerId => _serverConnection.Id;

        /// <summary>
        /// Updates the sub-tab badges based on server health status.
        /// </summary>
        public void UpdateBadges(ServerHealthStatus? status, AlertStateService alertService)
        {
            // Cache latest health status for acknowledge baseline snapshots
            if (status != null)
                _lastKnownStatus = status;

            if (status == null || status.IsOnline != true)
            {
                // Hide all badges when server is offline or no status
                LockingBadge.Visibility = Visibility.Collapsed;
                MemoryBadge.Visibility = Visibility.Collapsed;
                ResourceMetricsBadge.Visibility = Visibility.Collapsed;
                return;
            }

            // Locking badge: blocking or deadlocks
            var showLocking = alertService.ShouldShowBadge(_serverConnection.Id, "Locking", status);
            LockingBadge.Visibility = showLocking ? Visibility.Visible : Visibility.Collapsed;

            // Memory badge: memory pressure
            var showMemory = alertService.ShouldShowBadge(_serverConnection.Id, "Memory", status);
            MemoryBadge.Visibility = showMemory ? Visibility.Visible : Visibility.Collapsed;

            // Resource Metrics badge: high CPU
            var showResourceMetrics = alertService.ShouldShowBadge(_serverConnection.Id, "Resource Metrics", status);
            ResourceMetricsBadge.Visibility = showResourceMetrics ? Visibility.Visible : Visibility.Collapsed;
        }

        /// <summary>
        /// Sets up context menus for sub-tabs that have alert badges.
        /// </summary>
        private void SetupSubTabContextMenus()
        {
            // Add context menus to the tabs with badges
            var tabsWithBadges = new[]
            {
                (Tab: LockingTabItem, Badge: LockingBadge, Name: "Locking"),
                (Tab: MemoryTabItem, Badge: MemoryBadge, Name: "Memory"),
                (Tab: ResourceMetricsTabItem, Badge: ResourceMetricsBadge, Name: "Resource Metrics")
            };

            foreach (var (tab, badge, name) in tabsWithBadges)
            {
                var localBadge = badge; // Capture for closure
                var localName = name;

                var contextMenu = new ContextMenu();

                var acknowledgeItem = new MenuItem
                {
                    Header = "Acknowledge Alert",
                    Tag = name,
                    Icon = new TextBlock { Text = "✓", FontWeight = FontWeights.Bold }
                };
                acknowledgeItem.Click += AcknowledgeSubTabAlert_Click;

                var silenceItem = new MenuItem
                {
                    Header = "Silence This Tab",
                    Tag = name,
                    Icon = new TextBlock { Text = "🔇" }
                };
                silenceItem.Click += SilenceSubTab_Click;

                var unsilenceItem = new MenuItem
                {
                    Header = "Unsilence",
                    Tag = name,
                    Icon = new TextBlock { Text = "🔔" }
                };
                unsilenceItem.Click += UnsilenceSubTab_Click;

                contextMenu.Items.Add(acknowledgeItem);
                contextMenu.Items.Add(silenceItem);
                contextMenu.Items.Add(new Separator());
                contextMenu.Items.Add(unsilenceItem);

                // Update menu items based on silenced state and alert presence when opened
                contextMenu.Opened += (s, args) =>
                {
                    var alertService = GetAlertService();
                    if (alertService != null)
                    {
                        var isSilenced = alertService.IsSubTabSilenced(_serverConnection.Id, localName);
                        var hasAlert = localBadge.Visibility == Visibility.Visible;

                        // Acknowledge only enabled if there's a visible alert
                        acknowledgeItem.IsEnabled = hasAlert;
                        silenceItem.IsEnabled = !isSilenced;
                        unsilenceItem.IsEnabled = isSilenced;
                    }
                };

                // Attach context menu to the TabItem for reliable right-click
                tab.ContextMenu = contextMenu;
            }
        }

        private AlertStateService? GetAlertService()
        {
            var mainWindow = Window.GetWindow(this) as MainWindow;
            return mainWindow?.AlertStateService;
        }

        private void AcknowledgeSubTabAlert_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string tabName)
            {
                var alertService = GetAlertService();
                if (alertService != null)
                {
                    alertService.AcknowledgeAlert(_serverConnection.Id, tabName, _lastKnownStatus);

                    // Hide the badge immediately
                    var badge = tabName switch
                    {
                        "Locking" => LockingBadge,
                        "Memory" => MemoryBadge,
                        "Resource Metrics" => ResourceMetricsBadge,
                        _ => null
                    };
                    if (badge != null)
                    {
                        badge.Visibility = Visibility.Collapsed;
                    }
                }
            }
        }

        private void SilenceSubTab_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string tabName)
            {
                var alertService = GetAlertService();
                if (alertService != null)
                {
                    alertService.SilenceSubTab(_serverConnection.Id, tabName);

                    // Hide the badge immediately
                    var badge = tabName switch
                    {
                        "Locking" => LockingBadge,
                        "Memory" => MemoryBadge,
                        "Resource Metrics" => ResourceMetricsBadge,
                        _ => null
                    };
                    if (badge != null)
                    {
                        badge.Visibility = Visibility.Collapsed;
                    }
                }
            }
        }

        private void UnsilenceSubTab_Click(object sender, RoutedEventArgs e)
        {
            if (sender is MenuItem menuItem && menuItem.Tag is string tabName)
            {
                var alertService = GetAlertService();
                alertService?.UnsilenceSubTab(_serverConnection.Id, tabName);
            }
        }

        #endregion
    }
}
