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
using System.Text.RegularExpressions;

namespace PerformanceMonitor.Darling.Service;

/// <summary>One statement a read ran (#5097): its arrival ordinal, a label, a short stable hash of the text,
/// its duration, and the row count when Npgsql reported one.</summary>
internal readonly record struct StatementTiming(int Ordinal, string Label, string Hash, double DurationMs, long? Rows);

/// <summary>Turns a statement's text into its label and hash. Only the product's own SQL text is read; bound
/// parameter values are never part of it.</summary>
internal static class StatementLabel
{
    internal const int MaxLabelLength = 120;

    private static readonly Regex s_passwordKeyword = new(@"\sPASSWORD\b", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant, TimeSpan.FromSeconds(1));

    /// <summary>The statement's own first line: leading whitespace, <c>--</c> line comments and
    /// <c>/* */</c> block comments are skipped (a product query often opens with a comment that explains it),
    /// then the rest of that line is trimmed and cut to <see cref="MaxLabelLength"/> characters.</summary>
    internal static string Label(string? sql)
    {
        if (string.IsNullOrEmpty(sql))
        {
            return string.Empty;
        }

        var text = sql;
        var i = 0;
        while (i < text.Length)
        {
            if (char.IsWhiteSpace(text[i]))
            {
                i++;
            }
            else if (string.CompareOrdinal(text, i, "--", 0, 2) == 0)
            {
                var eol = text.IndexOf('\n', i);
                i = eol < 0 ? text.Length : eol + 1;
            }
            else if (string.CompareOrdinal(text, i, "/*", 0, 2) == 0)
            {
                var close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = close < 0 ? text.Length : close + 2;
            }
            else
            {
                break;
            }
        }

        if (i >= text.Length)
        {
            return string.Empty;
        }

        var end = text.IndexOf('\n', i);
        var line = (end < 0 ? text.AsSpan(i) : text.AsSpan(i, end - i)).Trim().ToString();

        /* #9641: a statement that sets a login's password (CREATE ROLE or ALTER ROLE ... PASSWORD 'SCRAM-SHA-256$...') carries the
           password's salt and stored key. The label stops before the keyword, so a slow-read line never shows them. */
        var password = s_passwordKeyword.Match(line);
        if (password.Success)
        {
            line = line[..password.Index].TrimEnd();
        }

        return line.Length > MaxLabelLength ? line[..MaxLabelLength] : line;
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

    /// <summary>The Npgsql display-name prefix of the activity a physical connection open emits (Npgsql 11
    /// <c>NpgsqlActivitySource.PhysicalConnectionOpen</c>: <c>"CONNECT " + database</c>).</summary>
    internal const string ConnectionOpenNamePrefix = "CONNECT ";

    /// <summary>What an Npgsql activity inside a capturing scope is (#9923).</summary>
    internal enum ActivityKindSeen
    {
        /// <summary>Neither a command nor a connection open: never counted as a statement.</summary>
        Other,

        /// <summary>A command or COPY: Npgsql sets <c>db.query.text</c> on every one of them.</summary>
        Statement,

        /// <summary>A physical connection open: no <c>db.query.text</c>, display name <c>CONNECT &lt;database&gt;</c>.</summary>
        ConnectionOpen,
    }

    /// <summary>
    /// Tells a statement from a physical connection open by what Npgsql puts on each (#9923). A statement carries the
    /// <c>db.query.text</c> tag, present even when its text is empty. A connection open has no such tag and is named
    /// <c>CONNECT &lt;database&gt;</c>. An activity that is neither is <see cref="ActivityKindSeen.Other"/>, so a missing text
    /// alone never makes a statement.
    /// </summary>
    internal static ActivityKindSeen Classify(Activity activity, out string? sql, out long? rows)
    {
        sql = null;
        rows = null;
        var hasText = false;
        foreach (var tag in activity.TagObjects)
        {
            if (tag.Key == "db.query.text")
            {
                hasText = true;
                sql = tag.Value as string;
            }
            else if (tag.Key == "db.response.returned_rows" && tag.Value is not null)
            {
                rows = Convert.ToInt64(tag.Value, System.Globalization.CultureInfo.InvariantCulture);
            }
        }

        if (hasText)
        {
            return ActivityKindSeen.Statement;
        }

        return activity.DisplayName.StartsWith(ConnectionOpenNamePrefix, StringComparison.Ordinal)
            ? ActivityKindSeen.ConnectionOpen
            : ActivityKindSeen.Other;
    }

    /// <summary>Hands one stopped activity to the scope: a statement to <see cref="ReadScope.AddStatement"/>, a connection
    /// open to <see cref="ReadScope.AddConnectionOpen"/>, anything else nowhere. Split out so a test can run it without a store.</summary>
    internal static void Observe(ReadScope scope, Activity activity)
    {
        switch (Classify(activity, out var sql, out var rows))
        {
            case ActivityKindSeen.Statement:
                scope.AddStatement(sql, activity.Duration.TotalMilliseconds, rows);
                break;
            case ActivityKindSeen.ConnectionOpen:
                scope.AddConnectionOpen(activity.Duration.TotalMilliseconds);
                break;
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

            Observe(scope, activity);
        }
        catch
        {
            /* A capture fault must never reach the read. */
        }
    }
}
