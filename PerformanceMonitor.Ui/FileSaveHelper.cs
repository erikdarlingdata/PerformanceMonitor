/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Windows;
using Microsoft.Win32;

namespace PerformanceMonitor.Ui;

/// <summary>
/// Shared "save this text to a user-chosen file" dialogs used by Lite's and Darling's plan-download and
/// deadlock / blocked-process XML-download buttons. (The deprecated Full Dashboard keeps its own inline
/// copies and is not developed further.)
/// </summary>
public static class FileSaveHelper
{
    /// <summary>
    /// Prompts for a <c>.sqlplan</c> path and writes <paramref name="planXml"/> there; the pre-filled name is
    /// <paramref name="defaultName"/> plus a <c>yyyyMMdd_HHmmss</c> timestamp. A plan the statement filter withheld whole
    /// is refused with <see cref="PerformanceMonitor.Common.SensitiveStatements.WithheldPlanSentence"/> and nothing is written.
    /// </summary>
    public static void SavePlanFile(string planXml, string defaultName)
    {
        /* #5320: the whole-plan marker is not a plan; say it is withheld instead of writing it into a .sqlplan. */
        if (WithheldPlanGuard.RefuseSave(planXml)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|All files (*.*)|*.*",
            DefaultExt = ".sqlplan",
            FileName = $"{defaultName}_{DateTime.Now:yyyyMMdd_HHmmss}.sqlplan"
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, planXml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save plan: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }

    /// <summary>
    /// Prompts for a <c>.xml</c> path and writes <paramref name="xml"/> there. <paramref name="suggestedFileName"/>
    /// is the full pre-filled file name; <paramref name="whatLabel"/> names the content in the error dialog;
    /// <paramref name="withheldSubject"/> names it in the withheld sentence (#5367: "deadlock graph", "blocked process
    /// report"; the plan viewers' own sentence says "plan").
    /// </summary>
    public static void SaveXmlToFile(string xml, string suggestedFileName, string whatLabel, string withheldSubject)
    {
        /* #5320: a deadlock graph or blocked process report the statement filter withheld whole is the marker, not XML.
           Say so and write no file, the same as the plan save sites do (shared by Lite and the Darling viewer). */
        if (WithheldPlanGuard.RefuseSave(xml, withheldSubject)) return;

        var dialog = new SaveFileDialog
        {
            Filter = "XML files (*.xml)|*.xml|All files (*.*)|*.*",
            DefaultExt = ".xml",
            FileName = suggestedFileName
        };

        if (dialog.ShowDialog() != true) return;

        try
        {
            File.WriteAllText(dialog.FileName, xml, Encoding.UTF8);
        }
        catch (Exception ex)
        {
            MessageBox.Show($"Failed to save {whatLabel}: {ex.Message}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        }
    }
}
