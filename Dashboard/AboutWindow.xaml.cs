/*
 * Performance Monitor Dashboard
 * Copyright (c) 2026 Darling Data, LLC
 * Licensed under the MIT License - see LICENSE file for details
 */

using System.Diagnostics;
using System.Reflection;
using System.Windows;
using PerformanceMonitorDashboard.Services;

namespace PerformanceMonitorDashboard
{
    public partial class AboutWindow : Window
    {
        private const string GitHubUrl = "https://github.com/erikdarlingdata/PerformanceMonitor";
        private const string IssuesUrl = "https://github.com/erikdarlingdata/PerformanceMonitor/issues";
        private const string ReleasesUrl = "https://github.com/erikdarlingdata/PerformanceMonitor/releases";
        private const string DarlingDataUrl = "https://www.erikdarling.com";

        private string? _updateReleaseUrl;

        public AboutWindow()
        {
            InitializeComponent();
            LoadVersionInfo();
        }

        private void LoadVersionInfo()
        {
            var version = Assembly.GetExecutingAssembly().GetName().Version;
            VersionText.Text = $"Version {version?.Major}.{version?.Minor}.{version?.Build}";
        }

        private void GitHubLink_Click(object sender, RoutedEventArgs e)
        {
            OpenUrl(GitHubUrl);
        }

        private void ReportIssueLink_Click(object sender, RoutedEventArgs e)
        {
            OpenUrl(IssuesUrl);
        }

        private async void CheckUpdatesLink_Click(object sender, RoutedEventArgs e)
        {
            UpdateStatusText.Text = "Checking for updates...";
            UpdateStatusText.Visibility = Visibility.Visible;

            var result = await UpdateCheckService.CheckForUpdateAsync(bypassCache: true);

            if (result == null)
            {
                UpdateStatusText.Text = "Unable to check for updates. Please try again later.";
            }
            else if (result.IsUpdateAvailable)
            {
                _updateReleaseUrl = result.ReleaseUrl;
                UpdateStatusText.Text = $"Update available: {result.LatestVersion} (you have {result.CurrentVersion})";
                UpdateStatusText.Cursor = System.Windows.Input.Cursors.Hand;
                UpdateStatusText.MouseLeftButtonUp += UpdateStatusText_Click;
                UpdateStatusText.TextDecorations = System.Windows.TextDecorations.Underline;
                UpdateStatusText.Foreground = FindResource("AccentBrush") as System.Windows.Media.Brush
                    ?? System.Windows.Media.Brushes.DodgerBlue;
            }
            else
            {
                UpdateStatusText.Text = $"You're up to date ({result.CurrentVersion})";
            }
        }

        private void UpdateStatusText_Click(object sender, System.Windows.Input.MouseButtonEventArgs e)
        {
            if (!string.IsNullOrEmpty(_updateReleaseUrl))
                OpenUrl(_updateReleaseUrl);
        }

        private void DarlingDataLink_Click(object sender, RoutedEventArgs e)
        {
            OpenUrl(DarlingDataUrl);
        }

        private void CloseButton_Click(object sender, RoutedEventArgs e)
        {
            Close();
        }

        private static void OpenUrl(string url)
        {
            try
            {
                Process.Start(new ProcessStartInfo
                {
                    FileName = url,
                    UseShellExecute = true
                });
            }
            catch
            {
                MessageBox.Show($"Could not open URL: {url}", "Error", MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }
    }
}
