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
    /// <paramref name="defaultName"/> plus <paramref name="timestamp"/> (default: the local time as <c>yyyyMMdd_HHmmss</c>; a window that saves an older stored snapshot passes that snapshot's time), with any character a file name cannot hold
    /// (a collected object name can carry one) replaced by an underscore (#7228). A plan the statement filter withheld whole
    /// is refused with <see cref="PerformanceMonitor.Common.SensitiveStatements.WithheldPlanSentence"/> and nothing is written.
    /// Returns true only when the file was written: a refusal, a cancelled dialog and a failed write (which shows its own
    /// error) all return false, so a caller can show "Saved" on true alone.
    /// </summary>
    public static bool SavePlanFile(string planXml, string defaultName, string? timestamp = null)
    {
        /* #5320: the whole-plan marker is not a plan; say it is withheld instead of writing it into a .sqlplan. */
        if (WithheldPlanGuard.RefuseSave(planXml)) return false;

        var dialog = new SaveFileDialog
        {
            Filter = "SQL Plan files (*.sqlplan)|*.sqlplan|All files (*.*)|*.*",
            DefaultExt = ".sqlplan",
            FileName = $"{SanitizeFileName(defaultName)}_{timestamp ?? DateTime.Now.ToString("yyyyMMdd_HHmmss")}.sqlplan"
        };

        if (dialog.ShowDialog() != true) return false;

        if (TryWriteText(dialog.FileName, planXml, out var error)) return true;

        MessageBox.Show($"Failed to save plan: {error}", "Save Error", MessageBoxButton.OK, MessageBoxImage.Error);
        return false;
    }

    /// <summary>Replaces every <see cref="Path.GetInvalidFileNameChars"/> character in <paramref name="name"/> with an underscore.</summary>
    public static string SanitizeFileName(string name)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var chars = name.ToCharArray();
        for (var i = 0; i < chars.Length; i++)
        {
            if (Array.IndexOf(invalid, chars[i]) >= 0) chars[i] = '_';
        }

        return new string(chars);
    }

    /// <summary>Writes <paramref name="text"/> as UTF-8; returns false with the failure text instead of throwing.</summary>
    public static bool TryWriteText(string path, string text, out string? error)
    {
        try
        {
            File.WriteAllText(path, text, Encoding.UTF8);
            error = null;
            return true;
        }
        catch (Exception ex)
        {
            error = ex.Message;
            return false;
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
