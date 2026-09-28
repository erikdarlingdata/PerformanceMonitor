/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using System.Windows;

namespace PerformanceMonitorLite.Windows;

/// <summary>
/// Standalone window that hosts a <see cref="PerformanceMonitor.Ui.PlanViewerControl"/> so any
/// drill-down / history window can open a collected or actual plan in-app. Shown owned + non-modal so it
/// stays interactive above a modal drill-down and closes with its host.
/// </summary>
public partial class PlanViewerWindow : Window
{
    public PlanViewerWindow()
    {
        InitializeComponent();
        // The shared PlanViewerControl needs an explicit Cleanup() to unsubscribe ThemeManager.
        Closed += (_, _) => Viewer.Cleanup();
    }

    /// <summary>
    /// Esc closes the window. This is the bubbling KeyDown rather than PreviewKeyDown on purpose: a
    /// child that wants Esc for itself (a filter popup clearing its value, say) marks the event handled
    /// and never reaches here, whereas a preview handler would take Esc away from it. There is no close
    /// button to hang <c>IsCancel</c> on — the plan control fills the window edge to edge.
    /// </summary>
    private void Window_KeyDown(object sender, System.Windows.Input.KeyEventArgs e)
    {
        if (e.Key == System.Windows.Input.Key.Escape)
        {
            e.Handled = true;
            Close();
        }
    }

    /// <summary>Loads a plan into this window's viewer. Throws <see cref="System.Xml.XmlException"/> for invalid XML.</summary>
    /// <param name="serverMetadata">
    /// #4530: the server's edition/MAXDOP for plan-analysis rule 38, set by the caller from a store read
    /// (this window has no live connection to the monitored server). Null keeps rule 38 on its Info branch,
    /// same as before #4530.
    /// </param>
    public async Task LoadPlanAsync(
        string planXml, string label, string? queryText, PerformanceMonitor.PlanAnalysis.ServerMetadata? serverMetadata = null)
    {
        if (!string.IsNullOrWhiteSpace(label))
            Title = label.Length > 80 ? label[..80] : label;
        Viewer.ServerMetadata = serverMetadata;
        await Viewer.LoadPlan(planXml, label, queryText);
    }

    /// <summary>
    /// Opens a new, owned, non-modal plan window and loads the plan. A new window per call lets the user
    /// compare plans side by side; owned so it stays usable above a modal host window.
    /// </summary>
    public static async Task ShowPlanAsync(
        Window owner, string planXml, string label, string? queryText,
        PerformanceMonitor.PlanAnalysis.ServerMetadata? serverMetadata = null)
    {
        var window = new PlanViewerWindow { Owner = owner };
        window.Show();
        try
        {
            await window.LoadPlanAsync(planXml, label, queryText, serverMetadata);
        }
        catch (System.Xml.XmlException ex)
        {
            window.Close();
            MessageBox.Show(owner, $"The plan XML is not valid:\n\n{ex.Message}", "Invalid Plan XML",
                MessageBoxButton.OK, MessageBoxImage.Warning);
        }
        catch (Exception ex)
        {
            window.Close();
            MessageBox.Show(owner, $"Failed to load the execution plan:\n\n{ex.Message}", "Plan Load Error",
                MessageBoxButton.OK, MessageBoxImage.Warning);
        }
    }
}
