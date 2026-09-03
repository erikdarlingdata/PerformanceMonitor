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
using System.Windows;
using System.Windows.Controls;
using System.Windows.Threading;
using PerformanceMonitor.Ui;

namespace PerformanceMonitorDashboard
{
    public partial class MainWindow : Window
    {
        #region Main Window Plan Viewer

        private const string PlanAddTabId = "__PLAN_ADD_TAB__";
        private TabControl? _mainPlanTabControl;
        private Grid? _planViewerContainer;

        /* Re-entrancy latch for the "+"-sentinel auto-add (see HandleAddTabSelected / #2825). */
        private bool _addTabInsertDeferred;

        private void OpenPlanViewer_Click(object sender, RoutedEventArgs e)
        {
            if (_planViewerTab != null && ServerTabControl.Items.Contains(_planViewerTab))
            {
                AddNewEmptyPlanSubTab();
                ServerTabControl.SelectedItem = _planViewerTab;
                Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(() => _planViewerContainer?.Focus()));
                return;
            }
            OpenPlanViewerTab();
        }

        private void OpenPlanViewerTab()
        {
            if (_planViewerTab != null && ServerTabControl.Items.Contains(_planViewerTab))
            {
                ServerTabControl.SelectedItem = _planViewerTab;
                return;
            }

            _mainPlanTabControl = new TabControl
            {
                Background = System.Windows.Media.Brushes.Transparent,
                BorderThickness = new Thickness(0)
            };

            // "+" tab at the end of the inner strip
            var addTabHeader = new TextBlock
            {
                Text = "+",
                FontSize = 14,
                FontWeight = FontWeights.Bold,
                VerticalAlignment = VerticalAlignment.Center,
                ToolTip = "Open a new plan sub-tab"
            };
            var addTab = new TabItem
            {
                Header = addTabHeader,
                Tag = PlanAddTabId,
                Content = new Grid() // no content needed
            };
            _mainPlanTabControl.Items.Add(addTab);

            _mainPlanTabControl.SelectionChanged += (_, _) =>
            {
                if (_mainPlanTabControl?.SelectedItem is TabItem { Tag: string t } && t == PlanAddTabId)
                    HandleAddTabSelected();
            };

            var container = new Grid();
            container.AllowDrop = true;
            container.Focusable = true;
            container.DragOver += MainWindowPlanViewer_DragOver;
            container.Drop += MainWindowPlanViewer_Drop;
            container.KeyDown += MainWindowPlanViewer_KeyDown;
            container.Children.Add(_mainPlanTabControl);
            _planViewerContainer = container;

            var header = new StackPanel { Orientation = Orientation.Horizontal };
            header.Children.Add(new TextBlock
            {
                Text = "Plan Viewer",
                VerticalAlignment = VerticalAlignment.Center,
                FontWeight = FontWeights.SemiBold
            });
            var closeBtn = new Button
            {
                Style = (Style)FindResource("TabCloseButton"),
                Tag = PlanViewerTabId
            };
            closeBtn.Click += CloseTab_Click;
            header.Children.Add(closeBtn);

            _planViewerTab = new TabItem
            {
                Header = header,
                Content = container,
                Tag = PlanViewerTabId
            };

            ServerTabControl.Items.Add(_planViewerTab);
            ServerTabControl.SelectedItem = _planViewerTab;

            // Open the first empty sub-tab immediately
            AddNewEmptyPlanSubTab();
            Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(() => _planViewerContainer?.Focus()));
        }

        /// <summary>
        /// The "+" sentinel became the selected tab -- add a fresh empty sub-tab and select it. Mirrors the shared
        /// <c>StandalonePlanViewerController</c>'s #2825 re-entrancy fix: when the Plan Viewer tab is realized for the
        /// first time, WPF re-asserts this selection for the auto-selected "+" tab from INSIDE the inner TabControl's
        /// container generator, and <see cref="AddNewEmptyPlanSubTab"/>'s <c>ItemCollection.Insert</c> mid-generation
        /// throws "Cannot call StartAt when content generation is in progress", which is unhandled and crashes the app.
        /// Only the "+" tab present means we are in that first-time realization, so defer the insert off the generation
        /// pass; the opener (<see cref="OpenPlanViewerTab"/>) adds a real sub-tab synchronously right after, so the
        /// deferred pass finds "+" deselected and no-ops -- no duplicate tab. A real user click (steady state, more than
        /// one item) stays synchronous exactly as before.
        /// </summary>
        private void HandleAddTabSelected()
        {
            if (_mainPlanTabControl == null) return;

            // Steady state: real sub-tab(s) already present => a genuine user click on "+". Safe to mutate now.
            if (_mainPlanTabControl.Items.Count > 1)
            {
                var newSub = AddNewEmptyPlanSubTab();
                _mainPlanTabControl.SelectedItem = newSub;
                return;
            }

            // Only the "+" sentinel exists => first-time realization, possibly mid-generation. Defer once.
            if (_addTabInsertDeferred) return;
            _addTabInsertDeferred = true;
            Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(() =>
            {
                _addTabInsertDeferred = false;
                if (_mainPlanTabControl != null &&
                    _mainPlanTabControl.SelectedItem is TabItem { Tag: string t } && t == PlanAddTabId)
                {
                    var newSub = AddNewEmptyPlanSubTab();
                    _mainPlanTabControl.SelectedItem = newSub;
                }
            }));
        }

        /// <summary>
        /// Adds a new empty "New Plan" sub-tab to the inner plan TabControl and selects it.
        /// Returns the newly created sub-tab.
        /// </summary>
        private TabItem AddNewEmptyPlanSubTab()
        {
            // --- Empty state layer ---
            var emptyState = new Grid();
            var dashedRect = new System.Windows.Shapes.Rectangle
            {
                Margin = new Thickness(24),
                Stroke = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                StrokeThickness = 1.5,
                StrokeDashArray = new System.Windows.Media.DoubleCollection { 6, 4 },
                RadiusX = 10, RadiusY = 10,
                Opacity = 0.25
            };
            var emptyStack = new StackPanel { HorizontalAlignment = HorizontalAlignment.Center, VerticalAlignment = VerticalAlignment.Center };
            emptyStack.Children.Add(new TextBlock
            {
                Text = "\uE896",
                FontFamily = new System.Windows.Media.FontFamily("Segoe MDL2 Assets"),
                FontSize = 52,
                HorizontalAlignment = HorizontalAlignment.Center,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                Opacity = 0.45,
                Margin = new Thickness(0, 0, 0, 12)
            });
            emptyStack.Children.Add(new TextBlock
            {
                Text = "New Plan",
                FontSize = 20,
                FontWeight = FontWeights.Light,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                HorizontalAlignment = HorizontalAlignment.Center
            });
            emptyStack.Children.Add(new TextBlock
            {
                Text = "Open or paste execution plan XML to render it",
                FontSize = 13,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 8, 0, 0)
            });
            var btnPanel = new StackPanel { Orientation = Orientation.Horizontal, HorizontalAlignment = HorizontalAlignment.Center, Margin = new Thickness(0, 20, 0, 0) };
            var openBtn = new Button { Content = "Open .sqlplan File", Height = 28, Padding = new Thickness(12, 0, 12, 0), ToolTip = "Open a .sqlplan or .xml file from disk" };
            var pasteBtn = new Button { Content = "Paste XML", Height = 28, Padding = new Thickness(12, 0, 12, 0), Margin = new Thickness(8, 0, 0, 0), ToolTip = "Paste execution plan XML to render it (or use Ctrl+V)" };
            btnPanel.Children.Add(openBtn);
            btnPanel.Children.Add(pasteBtn);
            emptyStack.Children.Add(btnPanel);
            emptyStack.Children.Add(new TextBlock
            {
                Text = "or drag & drop a .sqlplan file anywhere in this area",
                FontSize = 11,
                Foreground = (System.Windows.Media.Brush)FindResource("ForegroundMutedBrush"),
                HorizontalAlignment = HorizontalAlignment.Center,
                Margin = new Thickness(0, 12, 0, 0)
            });
            emptyState.Children.Add(dashedRect);
            emptyState.Children.Add(emptyStack);

            // --- Viewer layer (hidden until a plan is loaded) ---
            var viewer = new PlanViewerControl
            {
                Visibility = Visibility.Collapsed
            };

            // Sub-tab content grid: index 0 = emptyState, index 1 = viewer
            var subTabContent = new Grid();
            subTabContent.Children.Add(emptyState);
            subTabContent.Children.Add(viewer);

            // --- Sub-tab header: label + close button ---
            var initialLabel = GetUniqueSubTabLabel("New Plan");
            var labelBlock = new TextBlock
            {
                Text = initialLabel,
                VerticalAlignment = VerticalAlignment.Center,
                ToolTip = initialLabel
            };
            var subCloseBtn = new Button { Style = (Style)FindResource("TabCloseButton") };
            var subTabHeader = new StackPanel { Orientation = Orientation.Horizontal };
            subTabHeader.Children.Add(labelBlock);
            subTabHeader.Children.Add(subCloseBtn);

            var subTab = new TabItem { Header = subTabHeader, Content = subTabContent };

            subCloseBtn.Tag = subTab;
            subCloseBtn.Click += (_, _) =>
            {
                (subTabContent.Children[1] as PlanViewerControl)?.Cleanup();
                _mainPlanTabControl!.Items.Remove(subTab);
                // If only the "+" tab remains, re-open a fresh empty sub-tab
                if (_mainPlanTabControl.Items.Count == 1 &&
                    _mainPlanTabControl.Items[0] is TabItem { Tag: string t2 } && t2 == PlanAddTabId)
                {
                    AddNewEmptyPlanSubTab();
                }
            };

            // Wire per-sub-tab buttons with closures over this sub-tab
            openBtn.Click += (_, _) =>
            {
                var dialog = new Microsoft.Win32.OpenFileDialog
                {
                    Filter = "SQL Plan Files (*.sqlplan)|*.sqlplan|XML Files (*.xml)|*.xml|All Files (*.*)|*.*",
                    DefaultExt = ".sqlplan",
                    Multiselect = true
                };
                if (dialog.ShowDialog() != true) return;
                var isFirst = true;
                foreach (var fileName in dialog.FileNames)
                {
                    try
                    {
                        var xml = System.IO.File.ReadAllText(fileName);
                        var targetTab = isFirst ? subTab : AddNewEmptyPlanSubTab();
                        LoadPlanIntoSubTab(targetTab, xml, System.IO.Path.GetFileName(fileName));
                    }
                    catch (Exception ex)
                    {
                        MessageBox.Show($"Failed to open file:\n\n{ex.Message}", "Error",
                            MessageBoxButton.OK, MessageBoxImage.Error);
                    }
                    isFirst = false;
                }
            };

            pasteBtn.Click += (_, _) =>
            {
                var xml = Clipboard.GetText();
                if (!string.IsNullOrWhiteSpace(xml))
                {
                    LoadPlanIntoSubTab(subTab, xml, "Pasted Plan");
                    return;
                }
                MessageBox.Show("The clipboard does not contain any text.", "Paste Plan XML",
                    MessageBoxButton.OK, MessageBoxImage.Information);
            };

            // Insert before the "+" tab
            var addTabIndex = -1;
            for (var i = 0; i < _mainPlanTabControl!.Items.Count; i++)
            {
                if (_mainPlanTabControl.Items[i] is TabItem { Tag: string t3 } && t3 == PlanAddTabId)
                {
                    addTabIndex = i;
                    break;
                }
            }
            if (addTabIndex >= 0)
                _mainPlanTabControl.Items.Insert(addTabIndex, subTab);
            else
                _mainPlanTabControl.Items.Add(subTab);

            _mainPlanTabControl.SelectedItem = subTab;
            return subTab;
        }

        /// <summary>
        /// Loads plan XML into an existing sub-tab (replacing whatever was there before).
        /// Updates the sub-tab header label and shows the viewer layer.
        /// </summary>
        private async void LoadPlanIntoSubTab(TabItem subTab, string planXml, string label, string? queryText = null)
        {
            if (subTab.Content is not Grid subTabContent) return;
            if (subTabContent.Children.Count < 2) return;

            var emptyState = subTabContent.Children[0] as FrameworkElement;
            var viewer = subTabContent.Children[1] as PlanViewerControl;
            if (viewer == null) return;

            try
            {
                /* LoadPlan parses+analyzes off the UI thread; XmlException replaces the redundant
                   up-front XDocument.Parse validation. */
                await viewer.LoadPlan(planXml, label, queryText);
            }
            catch (System.Xml.XmlException ex)
            {
                MessageBox.Show(
                    $"The plan XML is not valid:\n\n{ex.Message}",
                    "Invalid Plan XML",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
                return;
            }
            catch (Exception ex)
            {
                MessageBox.Show(
                    $"Failed to load the execution plan:\n\n{ex.Message}",
                    "Plan Load Error",
                    MessageBoxButton.OK,
                    MessageBoxImage.Warning);
                return;
            }
            emptyState!.Visibility = Visibility.Collapsed;
            viewer.Visibility = Visibility.Visible;

            // Update header label (unique)
            var uniqueLabel = GetUniqueSubTabLabel(label);
            if (subTab.Header is StackPanel headerPanel &&
                headerPanel.Children[0] is TextBlock headerLabel)
            {
                headerLabel.Text = uniqueLabel.Length > 30 ? uniqueLabel[..30] + "\u2026" : uniqueLabel;
                headerLabel.ToolTip = uniqueLabel;
            }
        }

        /// <summary>
        /// Returns a label that is unique among current inner plan sub-tab headers.
        /// If <paramref name="baseLabel"/> is already taken, appends " (1)", " (2)", \u2026
        /// </summary>
        private string GetUniqueSubTabLabel(string baseLabel)
        {
            var existing = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            foreach (var item in _mainPlanTabControl!.Items)
            {
                if (item is TabItem { Tag: string t } && t == PlanAddTabId) continue;
                if (item is TabItem subTab &&
                    subTab.Header is StackPanel sp &&
                    sp.Children[0] is TextBlock tb)
                    existing.Add(tb.ToolTip as string ?? tb.Text);
            }
            if (!existing.Contains(baseLabel)) return baseLabel;
            var counter = 1;
            string candidate;
            do { candidate = $"{baseLabel} ({counter++})"; }
            while (existing.Contains(candidate));
            return candidate;
        }

        /// <summary>
        /// Returns the currently active real plan sub-tab (skips the "+" tab).
        /// </summary>
        private TabItem? GetActivePlanSubTab()
        {
            if (_mainPlanTabControl == null) return null;
            if (_mainPlanTabControl.SelectedItem is TabItem { Tag: string t } && t == PlanAddTabId)
                return null;
            return _mainPlanTabControl.SelectedItem as TabItem;
        }

        private void MainWindowPlanViewer_DragOver(object sender, DragEventArgs e)
        {
            if (e.Data.GetDataPresent(DataFormats.FileDrop))
            {
                var files = e.Data.GetData(DataFormats.FileDrop) as string[];
                if (files?.Any(IsPlanFile) == true)
                {
                    e.Effects = DragDropEffects.Copy;
                    e.Handled = true;
                    return;
                }
            }
            e.Effects = DragDropEffects.None;
            e.Handled = true;
        }

        private void MainWindowPlanViewer_Drop(object sender, DragEventArgs e)
        {
            if (!e.Data.GetDataPresent(DataFormats.FileDrop)) return;
            var planFiles = (e.Data.GetData(DataFormats.FileDrop) as string[])
                ?.Where(IsPlanFile).ToArray();
            if (planFiles == null || planFiles.Length == 0) return;
            LoadMainWindowPlanFromFileIntoActiveTab(planFiles[0]);
            for (var i = 1; i < planFiles.Length; i++)
            {
                var newTab = AddNewEmptyPlanSubTab();
                try
                {
                    var xml = System.IO.File.ReadAllText(planFiles[i]);
                    LoadPlanIntoSubTab(newTab, xml, System.IO.Path.GetFileName(planFiles[i]));
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Failed to open file:\n\n{ex.Message}", "Error",
                        MessageBoxButton.OK, MessageBoxImage.Error);
                }
            }
        }

        private void MainWindowPlanViewer_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
        {
            if (e.Key == System.Windows.Input.Key.V &&
                System.Windows.Input.Keyboard.Modifiers == System.Windows.Input.ModifierKeys.Control &&
                e.OriginalSource is not System.Windows.Controls.TextBox)
            {
                var xml = Clipboard.GetText();
                if (!string.IsNullOrWhiteSpace(xml))
                {
                    e.Handled = true;
                    LoadPlanIntoActivePlanSubTab(xml, "Pasted Plan");
                }
            }
        }

        private void LoadMainWindowPlanFromFileIntoActiveTab(string path)
        {
            try
            {
                var xml = System.IO.File.ReadAllText(path);
                LoadPlanIntoActivePlanSubTab(xml, System.IO.Path.GetFileName(path));
            }
            catch (Exception ex)
            {
                MessageBox.Show($"Failed to open file:\n\n{ex.Message}", "Error",
                    MessageBoxButton.OK, MessageBoxImage.Error);
            }
        }

        private void LoadPlanIntoActivePlanSubTab(string planXml, string label)
        {
            var activeSubTab = GetActivePlanSubTab();
            if (activeSubTab != null)
                LoadPlanIntoSubTab(activeSubTab, planXml, label);
        }

        private static bool IsPlanFile(string path)
        {
            var ext = System.IO.Path.GetExtension(path);
            return string.Equals(ext, ".sqlplan", StringComparison.OrdinalIgnoreCase)
                || string.Equals(ext, ".xml", StringComparison.OrdinalIgnoreCase);
        }

        #endregion
    }
}
