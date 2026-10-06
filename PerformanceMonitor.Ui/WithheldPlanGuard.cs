/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Windows;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Ui;

/// <summary>
/// #5320 (the plan viewers half of #4348): a stored or live plan the statement filter withheld whole is the marker
/// (<see cref="SensitiveStatements.PlaceholderText"/>), not a plan. The plan viewer shows it as withheld
/// (<see cref="SensitiveStatements.WithheldPlanSentence"/>) rather than as a parse error, and every site that writes
/// a plan to a <c>.sqlplan</c> file asks <see cref="RefuseSave"/> first, so the marker is never saved as if it were
/// a plan. Shared by Lite and the Darling viewer (both reach it through this project).
/// </summary>
public static class WithheldPlanGuard
{
    /// <summary>The sentence to show for <paramref name="planXml"/> when it is the whole-plan marker; null for a
    /// real plan, an empty string and null (those keep their own messages).</summary>
    public static string? WithheldSentence(string? planXml) =>
        SensitiveStatements.IsWithheldPlan(planXml) ? SensitiveStatements.WithheldPlanSentence : null;

    /// <summary>
    /// The withheld sentence for something other than a plan (a deadlock graph, a blocked process report):
    /// the same words as <see cref="SensitiveStatements.WithheldPlanSentence"/> with <paramref name="subject"/> in
    /// place of "plan". For "plan" it returns that sentence itself.
    /// </summary>
    public static string WithheldSentenceFor(string subject) =>
        string.Equals(subject, "plan", StringComparison.Ordinal)
            ? SensitiveStatements.WithheldPlanSentence
            : $"This {subject} was withheld by the statement filter (#4348).";

    /// <summary>
    /// True when <paramref name="planXml"/> is the whole-plan marker; the sentence is then shown in a message box
    /// and the caller must not write the file. False for anything else, and nothing is shown. <paramref name="subject"/>
    /// names what is being saved in that sentence and in the box title (#5367: a withheld deadlock graph or blocked
    /// process report is not "a plan"); the plan save sites keep the default.
    /// </summary>
    public static bool RefuseSave(string? planXml, string subject = "plan")
    {
        if (!SensitiveStatements.IsWithheldPlan(planXml))
        {
            return false;
        }

        var title = char.ToUpperInvariant(subject[0]).ToString() + subject.Substring(1) + " Withheld";
        MessageBox.Show(WithheldSentenceFor(subject), title, MessageBoxButton.OK, MessageBoxImage.Information);
        return true;
    }
}
