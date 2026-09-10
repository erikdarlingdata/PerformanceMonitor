/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using Microsoft.Data.SqlClient;
using Microsoft.Win32;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorDashboard
{
    public partial class ServerTab : UserControl
    {
        private void ShowPlanLoading(string label)
        {
            PlanLoadingLabel.Text = $"Executing: {label}";
            PlanEmptyState.Visibility = Visibility.Collapsed;
            PlanTabControl.Visibility = Visibility.Collapsed;
            PlanLoadingState.Visibility = Visibility.Visible;
            PlanViewerTabItem.IsSelected = true;
        }

        private void HidePlanLoading()
        {
            PlanLoadingState.Visibility = Visibility.Collapsed;
            if (PlanTabControl.Items.Count > 0)
                PlanTabControl.Visibility = Visibility.Visible;
            else
                PlanEmptyState.Visibility = Visibility.Visible;
        }

        private void CancelPlanButton_Click(object sender, RoutedEventArgs e)
        {
            PerformanceTab.CancelActualPlan();
        }

        // Returns a Task (not async void) so the Ctrl+V paste handler can await it inside its _pasteInProgress
        // guard -- the guard must span the off-thread parse in LoadPlan, not just the clipboard read (#2870).
        // Non-paste "View Plan" callers discard the Task (_ = OpenPlanTab(...)); CS4014 is an error in this project.
        private async Task OpenPlanTab(string planXml, string label, string? queryText = null)
        {
            HidePlanLoading();
            var viewer = new PlanViewerControl();
            try
            {
                /* LoadPlan parses+analyzes off the UI thread; it throws XmlException for malformed
                   plan XML, replacing the redundant up-front XDocument.Parse validation. */
                await viewer.LoadPlan(planXml, label, queryText);
            }
            catch (System.Xml.XmlException ex)
            {
                viewer.Cleanup();
                MessageBox.Show(
                    $"The plan XML is not valid:\n\n{ex.Message}",
                    "Invalid Plan XML",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
                return;
            }
            catch (Exception ex)
            {
                viewer.Cleanup();
                MessageBox.Show(
                    $"Failed to load the execution plan:\n\n{ex.Message}",
                    "Plan Load Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
                return;
            }

            var header = new StackPanel { Orientation = Orientation.Horizontal };
            header.Children.Add(new TextBlock
            {
                Text = label.Length > 30 ? label[..30] + "…" : label,
                VerticalAlignment = VerticalAlignment.Center,
                ToolTip = label
            });
            var closeBtn = new Button
            {
                Style = (Style)FindResource("TabCloseButton")
            };
            header.Children.Add(closeBtn);

            var tab = new TabItem { Header = header, Content = viewer };
            closeBtn.Tag = tab;
            closeBtn.Click += ClosePlanTab_Click;

            PlanTabControl.Items.Add(tab);
            PlanTabControl.SelectedItem = tab;
            PlanEmptyState.Visibility = Visibility.Collapsed;
            PlanTabControl.Visibility = Visibility.Visible;
        }

        private void ClosePlanTab_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button btn && btn.Tag is TabItem tab)
            {
                (tab.Content as PlanViewerControl)?.Cleanup();
                PlanTabControl.Items.Remove(tab);
                if (PlanTabControl.Items.Count == 0)
                {
                    PlanTabControl.Visibility = Visibility.Collapsed;
                    PlanEmptyState.Visibility = Visibility.Visible;
                }
            }
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

        private async void DownloadBlockingXml_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is BlockingEventItem item)
            {
                // The grid defers the (large) report XML for speed; fetch this one on demand.
                var reportXml = item.BlockedProcessReportXml;
                if (string.IsNullOrWhiteSpace(reportXml) && item.HasBlockedProcessReport && _databaseService != null)
                    reportXml = await _databaseService.GetBlockedProcessReportAsync(item.BlockingId, item.CollectionTime) ?? string.Empty;

                if (string.IsNullOrWhiteSpace(reportXml))
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
                        File.WriteAllText(saveFileDialog.FileName, reportXml);
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

        private async void DownloadDeadlockGraph_Click(object sender, RoutedEventArgs e)
        {
            if (sender is Button button && button.DataContext is DeadlockItem item)
            {
                // Graph is deferred out of the grid query; fetch it on demand.
                var graphXml = item.DeadlockGraph;
                if (string.IsNullOrWhiteSpace(graphXml) && _databaseService != null)
                    graphXml = await _databaseService.GetDeadlockGraphAsync(item.CollectionTime, item.DeadlockId) ?? string.Empty;
                if (string.IsNullOrWhiteSpace(graphXml))
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
                        File.WriteAllText(saveFileDialog.FileName, graphXml);
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

        // ── Blocked Process Report / Deadlock plan lookup ──

        /* SQL Server writes this 42-byte all-zero handle into executionStack frames
           for dynamic SQL / system contexts where no persistent sql_handle exists.
           Filter matches sp_HumanEventsBlockViewer's XPath exclusion. */
        private static readonly string ZeroSqlHandle = "0x" + new string('0', 84);

        private async void ViewBlockedSidePlan_Click(object sender, RoutedEventArgs e)
            => await ShowBlockedProcessPlanAsync(sender, blockingSide: false);

        private async void ViewBlockingSidePlan_Click(object sender, RoutedEventArgs e)
            => await ShowBlockedProcessPlanAsync(sender, blockingSide: true);

        private async Task ShowBlockedProcessPlanAsync(object sender, bool blockingSide)
        {
            if (sender is not MenuItem menuItem) return;
            if (menuItem.Parent is not ContextMenu cm) return;
            var grid = FindDataGridFromContextMenu(cm);
            if (grid?.SelectedItem is not BlockingEventItem row) return;

            var sideLabel = blockingSide ? "Blocking" : "Blocked";

            // The grid defers the (large) blocked process report XML; fetch this one on demand.
            var reportXml = row.BlockedProcessReportXml;
            if (string.IsNullOrWhiteSpace(reportXml) && row.HasBlockedProcessReport && _databaseService != null)
                reportXml = await _databaseService.GetBlockedProcessReportAsync(row.BlockingId, row.CollectionTime) ?? string.Empty;

            // A blocked process report can list several frames, each with its own sql_handle —
            // retrieve and open every plan we can still recover, not just the first.
            var frames = ExtractBlockedProcessFrames(reportXml, blockingSide);
            if (frames.Count == 0)
            {
                MessageBox.Show(
                    $"The {sideLabel.ToLowerInvariant()} process report has no resolvable sql_handle. " +
                    "This usually means the query ran as dynamic SQL or a system context — " +
                    "SQL Server records a zero handle in that case and the plan can't be recovered.",
                    "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            int opened = 0;
            try
            {
                var connStr = _serverConnection.GetConnectionString(_credentialService);
                for (int i = 0; i < frames.Count; i++)
                {
                    var f = frames[i];
                    var planXml = await FetchPlanBySqlHandleAsync(
                        connStr, row.DatabaseName, f.SqlHandle, f.StmtStart, f.StmtEnd);
                    if (string.IsNullOrEmpty(planXml)) continue;

                    var label = frames.Count > 1
                        ? $"Est Plan - {sideLabel} SPID {row.Spid} ({i + 1}/{frames.Count})"
                        : $"Est Plan - {sideLabel} SPID {row.Spid}";
                    _ = OpenPlanTab(planXml, label, row.QueryText);
                    opened++;
                }
            }
            catch { }

            if (opened > 0)
            {
                PlanViewerTabItem.IsSelected = true;
            }
            else
            {
                MessageBox.Show(
                    $"The plan for the {sideLabel.ToLowerInvariant()} query is no longer in the plan cache on {_serverConnection.DisplayName}. " +
                    "Blocked process reports only give us a sql_handle — if that plan has been evicted, we can't recover it.",
                    "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        private static IReadOnlyList<(string SqlHandle, int StmtStart, int StmtEnd)> ExtractBlockedProcessFrames(
            string bprXml, bool blockingSide)
        {
            var empty = Array.Empty<(string, int, int)>();
            if (string.IsNullOrWhiteSpace(bprXml)) return empty;
            try
            {
                var doc = System.Xml.Linq.XElement.Parse(bprXml);
                // The stored report's root is the full XE <event>; the process nodes live deeper, at
                // /event/data/value/blocked-process-report/{blocking|blocked}-process/process/executionStack.
                // Search by descendant rather than direct child so the frames are actually found.
                var processContainer = doc
                    .Descendants(blockingSide ? "blocking-process" : "blocked-process")
                    .FirstOrDefault();
                var stack = processContainer?.Descendants("executionStack").FirstOrDefault();
                if (stack == null) return empty;

                var frames = new List<(string, int, int)>();
                foreach (var frame in stack.Elements("frame"))
                {
                    var handle = frame.Attribute("sqlhandle")?.Value;
                    if (string.IsNullOrWhiteSpace(handle)) continue;
                    if (string.Equals(handle, ZeroSqlHandle, StringComparison.OrdinalIgnoreCase)) continue;

                    int stmtStart = 0;
                    int stmtEnd = -1;
                    _ = int.TryParse(frame.Attribute("stmtstart")?.Value, out stmtStart);
                    if (int.TryParse(frame.Attribute("stmtend")?.Value, out var se)) stmtEnd = se;

                    frames.Add((handle!, stmtStart, stmtEnd));
                }
                return frames;
            }
            catch
            {
                return empty;
            }
        }

        /* Deadlock graph XML puts sqlhandle/stmtstart/stmtend directly on the
           <process> node, with optional <executionStack><frame sqlhandle=...>
           children for the call stack. Match by SPID since Dashboard's row
           model doesn't carry the process graph id. */
        private async void ViewDeadlockProcessPlan_Click(object sender, RoutedEventArgs e)
        {
            if (sender is not MenuItem menuItem) return;
            if (menuItem.Parent is not ContextMenu cm) return;
            var grid = FindDataGridFromContextMenu(cm);
            if (grid?.SelectedItem is not DeadlockItem row) return;

            var sideLabel = string.IsNullOrWhiteSpace(row.DeadlockType) ? "Process" : row.DeadlockType;
            var label = $"Est Plan - {sideLabel} SPID {row.Spid}";

            // Graph is deferred out of the grid query; fetch it on demand.
            var graphXml = row.DeadlockGraph;
            if (string.IsNullOrWhiteSpace(graphXml) && _databaseService != null)
                graphXml = await _databaseService.GetDeadlockGraphAsync(row.CollectionTime, row.DeadlockId) ?? string.Empty;
            var frames = ExtractDeadlockProcessFrames(graphXml, row.Spid);
            if (frames.Count == 0)
            {
                MessageBox.Show(
                    "The process has no resolvable sql_handle in the deadlock graph. " +
                    "This usually means the query ran as dynamic SQL or a system context — " +
                    "SQL Server records a zero handle in that case and the plan can't be recovered.",
                    "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
                return;
            }

            string? planXml = null;
            try
            {
                var connStr = _serverConnection.GetConnectionString(_credentialService);
                foreach (var f in frames)
                {
                    planXml = await FetchPlanBySqlHandleAsync(
                        connStr, row.DatabaseName, f.SqlHandle, f.StmtStart, f.StmtEnd);
                    if (!string.IsNullOrEmpty(planXml)) break;
                }
            }
            catch { }

            if (!string.IsNullOrEmpty(planXml))
            {
                _ = OpenPlanTab(planXml, label, row.Query);
                PlanViewerTabItem.IsSelected = true;
            }
            else
            {
                MessageBox.Show(
                    $"The plan for this process is no longer in the plan cache on {_serverConnection.DisplayName}. " +
                    "Deadlock graphs only give us a sql_handle — if that plan has been evicted, we can't recover it.",
                    "No Plan Available", MessageBoxButton.OK, MessageBoxImage.Information);
            }
        }

        private static IReadOnlyList<(string SqlHandle, int StmtStart, int StmtEnd)> ExtractDeadlockProcessFrames(
            string graphXml, short? spid)
        {
            var empty = Array.Empty<(string, int, int)>();
            if (string.IsNullOrWhiteSpace(graphXml) || !spid.HasValue) return empty;
            try
            {
                var doc = System.Xml.Linq.XElement.Parse(graphXml);
                var spidStr = spid.Value.ToString(CultureInfo.InvariantCulture);
                var process = doc.Descendants("process")
                    .FirstOrDefault(p => string.Equals(p.Attribute("spid")?.Value, spidStr, StringComparison.Ordinal));
                if (process == null) return empty;

                var frames = new List<(string, int, int)>();

                var procHandle = process.Attribute("sqlhandle")?.Value;
                if (!string.IsNullOrWhiteSpace(procHandle) &&
                    !string.Equals(procHandle, ZeroSqlHandle, StringComparison.OrdinalIgnoreCase))
                {
                    int ps = 0, pe = -1;
                    _ = int.TryParse(process.Attribute("stmtstart")?.Value, out ps);
                    if (int.TryParse(process.Attribute("stmtend")?.Value, out var peParsed)) pe = peParsed;
                    frames.Add((procHandle!, ps, pe));
                }

                var stack = process.Element("executionStack");
                if (stack != null)
                {
                    foreach (var frame in stack.Elements("frame"))
                    {
                        var handle = frame.Attribute("sqlhandle")?.Value;
                        if (string.IsNullOrWhiteSpace(handle)) continue;
                        if (string.Equals(handle, ZeroSqlHandle, StringComparison.OrdinalIgnoreCase)) continue;

                        int fs = 0, fe = -1;
                        _ = int.TryParse(frame.Attribute("stmtstart")?.Value, out fs);
                        if (int.TryParse(frame.Attribute("stmtend")?.Value, out var feParsed)) fe = feParsed;
                        frames.Add((handle!, fs, fe));
                    }
                }

                return frames;
            }
            catch
            {
                return empty;
            }
        }

        private static async Task<string?> FetchPlanBySqlHandleAsync(
            string connectionString,
            string databaseName,
            string sqlHandleHex,
            int statementStartOffset,
            int statementEndOffset)
        {
            if (string.IsNullOrWhiteSpace(sqlHandleHex)) return null;
            var handleBytes = HexStringToBytes(sqlHandleHex);
            if (handleBytes == null || handleBytes.Length == 0) return null;

            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();

            /* Database context is only used to route the execution; sys.dm_exec_query_stats
               is server-scoped, so if the supplied name isn't valid we fall back to master. */
            var quotedDbName = QuoteDatabaseName(databaseName) ?? "[master]";

            var query = $@"
EXECUTE {quotedDbName}.sys.sp_executesql
    N'
SELECT TOP (1)
    query_plan_text = tqp.query_plan
FROM sys.dm_exec_query_stats AS qs
OUTER APPLY sys.dm_exec_text_query_plan(qs.plan_handle, qs.statement_start_offset, qs.statement_end_offset) AS tqp
WHERE qs.sql_handle = @h
AND   qs.statement_start_offset = @stmt_start
AND   qs.statement_end_offset = @stmt_end
AND   tqp.query_plan IS NOT NULL
ORDER BY
    qs.last_execution_time DESC
OPTION(RECOMPILE);',
    N'@h varbinary(64), @stmt_start int, @stmt_end int',
    @h, @stmt_start, @stmt_end;";

            using var command = new SqlCommand(query, connection) { CommandTimeout = 30 };
            command.Parameters.Add(new SqlParameter("@h", SqlDbType.VarBinary, 64) { Value = handleBytes });
            command.Parameters.Add(new SqlParameter("@stmt_start", SqlDbType.Int) { Value = statementStartOffset });
            command.Parameters.Add(new SqlParameter("@stmt_end", SqlDbType.Int) { Value = statementEndOffset });
            var result = await command.ExecuteScalarAsync();
            return result as string;
        }

        private static byte[]? HexStringToBytes(string hex)
        {
            var start = hex.StartsWith("0x", StringComparison.OrdinalIgnoreCase) ? 2 : 0;
            var len = hex.Length - start;
            if (len <= 0 || (len % 2) != 0) return null;
            var bytes = new byte[len / 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                if (!byte.TryParse(hex.AsSpan(start + i * 2, 2),
                                   NumberStyles.HexNumber,
                                   CultureInfo.InvariantCulture,
                                   out bytes[i]))
                {
                    return null;
                }
            }
            return bytes;
        }

        /* Only accept names that are syntactically plain identifiers so we can safely
           interpolate into the EXEC statement. Unknown / invalid names fall back to master. */
        private static string? QuoteDatabaseName(string? dbName)
        {
            if (string.IsNullOrWhiteSpace(dbName)) return null;
            foreach (var c in dbName)
            {
                if (!(char.IsLetterOrDigit(c) || c == '_' || c == '$' || c == '#' || c == '-' || c == ' '))
                    return null;
            }
            return "[" + dbName.Replace("]", "]]") + "]";
        }
    }
}
