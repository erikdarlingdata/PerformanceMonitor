/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Helpers;

/// <summary>
/// Composes the body of the "Connection Failed" dialog: the driver's own error, what stage of a
/// Microsoft Entra MFA broker handshake failed when that is what happened, and where the log with
/// the full exception chain lives.
///
/// <para><b>Why the log location is in the dialog.</b> The reporter of #3196 hit a connection
/// failure whose dialog text was the only record of it, went looking for a log, and reported back
/// that neither the data directory nor a logs directory under it existed. A dialog that shows an
/// error and does not say where the rest of it went leaves a reporter guessing at a path, and the
/// path is not guessable — it is a resolved absolute directory, not a name. Naming it costs one
/// line and is the difference between a follow-up that carries the exception chain and one that
/// carries a screenshot.</para>
///
/// <para>Pure, and takes the log directory rather than reading <c>App.DataDirectory</c>, so the
/// composition is testable without a WPF application object.</para>
/// </summary>
public static class ConnectionFailureMessage
{
    /// <summary>
    /// Builds the detail block that follows "Could not connect to {server}." — including its leading
    /// blank line — or an empty string when there is nothing to say.
    /// </summary>
    /// <param name="driverError">The exception message from the connection attempt, or null.</param>
    /// <param name="brokerFailure">
    /// The broker stage from <see cref="EntraBrokerFailure.Classify"/>.
    /// <see cref="EntraBrokerFailureKind.None"/> adds nothing.
    /// </param>
    /// <param name="logDirectory">
    /// The resolved directory holding the log files, or null/blank to omit the pointer. Omitted
    /// rather than guessed: a wrong path sends a reporter somewhere empty and reads as "there are no
    /// logs", which is the confusion this is here to end.
    /// </param>
    public static string Compose(
        string? driverError,
        EntraBrokerFailureKind brokerFailure,
        string? logDirectory)
    {
        var body = new StringBuilder();

        if (!string.IsNullOrWhiteSpace(driverError))
        {
            body.Append("\n\nError: ").Append(driverError);
        }

        /* The explanation goes AFTER the driver's text, not instead of it. The driver's message
           carries the error codes a report needs to be actionable; this only says which side of the
           handshake they came from. */
        var explanation = EntraBrokerFailure.Explain(brokerFailure);
        if (explanation is not null)
        {
            body.Append("\n\n").Append(explanation);
        }

        if (!string.IsNullOrWhiteSpace(logDirectory))
        {
            body.Append("\n\nThe full error, including any inner exceptions, is in the log at:\n")
                .Append(logDirectory);
        }

        return body.ToString();
    }
}
