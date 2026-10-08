/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One statement a read ran (#5097): its arrival ordinal, a label, a short stable hash of the text,
/// its duration, and the row count when Npgsql reported one.</summary>
internal readonly record struct StatementTiming(int Ordinal, string Label, string Hash, double DurationMs, long? Rows);

/// <summary>Turns a statement's text into its label and hash. Only the product's own SQL text is read; bound
/// parameter values are never part of it.</summary>
internal static class StatementLabel
{
    internal const int MaxLabelLength = 120;

    /// <summary>The first line that is not blank and not a <c>--</c> comment, trimmed and cut to
    /// <see cref="MaxLabelLength"/> characters.</summary>
    internal static string Label(string? sql)
    {
        if (string.IsNullOrEmpty(sql))
        {
            return string.Empty;
        }

        foreach (var raw in sql.Split('\n'))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith("--", StringComparison.Ordinal))
            {
                continue;
            }

            return line.Length > MaxLabelLength ? line[..MaxLabelLength] : line;
        }

        return string.Empty;
    }

    /// <summary>Eight hex characters of the SHA-256 of the whole text: stable across runs and processes.</summary>
    internal static string Hash(string? sql) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(sql ?? string.Empty)), 0, 4).ToLowerInvariant();
}

/// <summary>
/// The one process-wide Npgsql <see cref="ActivityListener"/> (#5097). It samples a command only while the
/// calling flow runs inside a <see cref="ReadScope"/> that asked for capture, so collectors and the worker
/// create no activity. Its callbacks never throw.
/// </summary>
internal static class ReadStatementCapture
{
    private static readonly object s_gate = new();
    private static ActivityListener? s_listener;

    /// <summary>Registers the listener once per process; later calls do nothing.</summary>
    internal static void Register()
    {
        lock (s_gate)
        {
            if (s_listener is not null)
            {
                return;
            }

            s_listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "Npgsql",
                Sample = Sample,
                ActivityStopped = Stopped,
            };
            ActivitySource.AddActivityListener(s_listener);
        }
    }

    /// <summary>The sampling decision, split out so a test can assert the outside-a-scope answer.</summary>
    internal static ActivitySamplingResult Sample(ref ActivityCreationOptions<ActivityContext> options)
    {
        try
        {
            return ReadScope.Current is { CaptureStatements: true }
                ? ActivitySamplingResult.AllDataAndRecorded
                : ActivitySamplingResult.None;
        }
        catch
        {
            return ActivitySamplingResult.None;
        }
    }

    private static void Stopped(Activity activity)
    {
        try
        {
            if (ReadScope.Current is not { CaptureStatements: true } scope)
            {
                return;
            }

            string? sql = null;
            long? rows = null;
            foreach (var tag in activity.TagObjects)
            {
                if (tag.Key == "db.query.text")
                {
                    sql = tag.Value as string;
                }
                else if (tag.Key == "db.response.returned_rows" && tag.Value is not null)
                {
                    rows = Convert.ToInt64(tag.Value, System.Globalization.CultureInfo.InvariantCulture);
                }
            }

            scope.AddStatement(sql, activity.Duration.TotalMilliseconds, rows);
        }
        catch
        {
            /* A capture fault must never reach the read. */
        }
    }
}
