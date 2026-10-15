/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Notifications;

/// <summary>
/// What one alert resolution says on every channel (#5825): the server, the alert that cleared, when it fired and
/// cleared, how long it lasted, the last value its fire sent, the resolution's own message and the fire's triage link.
/// The email renders <see cref="Rows"/> as its data table; Teams, Slack, the generic webhook and the PagerDuty resolve
/// carry <see cref="DetailText"/> as their detail prose, beside the card's own Server fact.
/// </summary>
/// <param name="Title">The resolution's own name, such as "CPU Resolved": the headline.</param>
/// <param name="AlertName">The alert that cleared, as it fired (its firing metric name).</param>
/// <param name="ServerName">The server the alert was about.</param>
/// <param name="FiredAtUtc">The episode's first delivered fire.</param>
/// <param name="ClearedAtUtc">When the clear was handled.</param>
/// <param name="LastValue">The value the last delivered fire sent; blank when none is known.</param>
/// <param name="Message">The resolution's own line, such as "CPU has returned to normal".</param>
/// <param name="TriageUrl">The triage link the fire carried, or null.</param>
public sealed record AlertResolutionFacts(
    string Title, string AlertName, string ServerName, DateTime FiredAtUtc, DateTime ClearedAtUtc,
    string? LastValue, string? Message, string? TriageUrl)
{
    /// <summary>How long the alert lasted, never negative.</summary>
    public TimeSpan Lasted => ClearedAtUtc > FiredAtUtc ? ClearedAtUtc - FiredAtUtc : TimeSpan.Zero;

    /// <summary>The facts as label/value rows, in reading order. The last value is left out when none is known.</summary>
    public IReadOnlyList<(string Label, string Value)> Rows()
    {
        var rows = new List<(string Label, string Value)>
        {
            ("Server", ServerName),
            ("Alert", AlertName),
            ("Fired (UTC)", FiredAtUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)),
            ("Cleared (UTC)", ClearedAtUtc.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture)),
            ("Lasted", DurationText(Lasted)),
        };
        if (!string.IsNullOrWhiteSpace(LastValue))
        {
            rows.Add(("Last value", LastValue));
        }

        return rows;
    }

    /// <summary>The prose form, for a channel that shows text only (the PagerDuty resolve): the message, then when the
    /// alert fired and cleared, how long it lasted and the last value.</summary>
    public string DetailText()
    {
        var text = new System.Text.StringBuilder(Message ?? "");
        text.Append(CultureInfo.InvariantCulture, $"\nAlert: {AlertName}");
        text.Append(CultureInfo.InvariantCulture, $"\nFired: {FiredAtUtc:yyyy-MM-dd HH:mm:ss} UTC");
        text.Append(CultureInfo.InvariantCulture, $"\nCleared: {ClearedAtUtc:yyyy-MM-dd HH:mm:ss} UTC");
        text.Append(CultureInfo.InvariantCulture, $"\nLasted: {DurationText(Lasted)}");
        if (!string.IsNullOrWhiteSpace(LastValue))
        {
            text.Append(CultureInfo.InvariantCulture, $"\nLast value: {LastValue}");
        }

        return text.ToString().TrimStart('\n');
    }

    /// <summary>"1h 5m", "4m 0s" or "12s".</summary>
    public static string DurationText(TimeSpan lasted)
    {
        if (lasted < TimeSpan.Zero)
        {
            lasted = TimeSpan.Zero;
        }

        return lasted.TotalHours >= 1
            ? string.Create(CultureInfo.InvariantCulture, $"{(int)lasted.TotalHours}h {lasted.Minutes}m")
            : lasted.TotalMinutes >= 1
                ? string.Create(CultureInfo.InvariantCulture, $"{lasted.Minutes}m {lasted.Seconds}s")
                : string.Create(CultureInfo.InvariantCulture, $"{lasted.Seconds}s");
    }
}
