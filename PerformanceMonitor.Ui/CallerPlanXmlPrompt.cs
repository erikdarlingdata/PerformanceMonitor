/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Windows;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Ui;

/// <summary>
/// The viewer side of <see cref="CallerPlanXml"/>: the open, drop and paste paths call it on the text a person
/// supplied, right after they read it and before anything is loaded. It shows the reason and tells the caller to
/// stop. Plans the collectors stored reach the viewer through <c>LoadPlan</c> directly and never come here.
/// </summary>
internal static class CallerPlanXmlPrompt
{
    /// <summary>True when the text was refused (the dialog has been shown and nothing may be loaded).</summary>
    internal static bool Refuse(string planXml)
    {
        var reason = CallerPlanXml.RefusalReason(planXml);
        if (reason == null) return false;

        MessageBox.Show(
            reason,
            "Plan Not Opened",
            MessageBoxButton.OK,
            MessageBoxImage.Warning);
        return true;
    }
}
