/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// The text of the dialogs Lite shows for an error nobody handled (#7077). They carry the message and the log file
/// that holds the details. The stack trace and the exception type name go to the log only, because the person
/// reading the dialog cannot act on them and the log already has them.
/// </summary>
internal static class ErrorDialogText
{
    /// <summary>The dialog shown when the process is about to end.</summary>
    internal static string Fatal(Exception? ex, string logFilePath) =>
        "Performance Monitor Lite hit an unexpected error and must close." +
        "\n\n" + MessageOf(ex) +
        "\n\n" + LogSentence(logFilePath);

    /// <summary>The dialog shown when Lite catches the error and keeps running.</summary>
    internal static string Recoverable(Exception? ex, string logFilePath) =>
        "Performance Monitor Lite hit an unexpected error." +
        "\n\n" + MessageOf(ex) +
        "\n\nThe application will attempt to continue. " + LogSentence(logFilePath);

    private static string MessageOf(Exception? ex) =>
        ex == null || string.IsNullOrWhiteSpace(ex.Message) ? "Unknown error" : ex.Message;

    private static string LogSentence(string logFilePath) =>
        "The details are in the log: " + logFilePath + ". Please include that file when you report this.";
}
