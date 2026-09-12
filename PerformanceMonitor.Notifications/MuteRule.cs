/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Notifications;

public class MuteRule
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public bool Enabled { get; set; } = true;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? ExpiresAtUtc { get; set; }
    public string? Reason { get; set; }

    public string? ServerName { get; set; }
    public string? MetricName { get; set; }
    public string? DatabasePattern { get; set; }
    public string? QueryTextPattern { get; set; }
    public string? WaitTypePattern { get; set; }
    public string? JobNamePattern { get; set; }

    public bool IsExpired => ExpiresAtUtc.HasValue && DateTime.UtcNow >= ExpiresAtUtc.Value;

    public MuteRule Clone() => new()
    {
        Id = Id,
        Enabled = Enabled,
        CreatedAtUtc = CreatedAtUtc,
        ExpiresAtUtc = ExpiresAtUtc,
        Reason = Reason,
        ServerName = ServerName,
        MetricName = MetricName,
        DatabasePattern = DatabasePattern,
        QueryTextPattern = QueryTextPattern,
        WaitTypePattern = WaitTypePattern,
        JobNamePattern = JobNamePattern
    };

    public string ExpiresDisplay => ExpiresAtUtc.HasValue
        ? (IsExpired ? "Expired" : ExpiresAtUtc.Value.ToLocalTime().ToString("g"))
        : "Never";

    /// <summary>
    /// The match dimensions this rule actually constrains, rendered one per entry. The SINGLE enumeration
    /// behind both <see cref="Summary"/> and <see cref="MatchesEveryAlert"/>, and it names the same fields
    /// <see cref="Matches"/> tests.
    ///
    /// <para>One list rather than two hand-kept copies: a seventh dimension added to <see cref="Matches"/>
    /// but missed by a copied "is this rule unconstrained" predicate would make a rule narrowed ONLY by
    /// that new dimension read as matching every alert. Sharing the list makes the two answers move
    /// together by construction.</para>
    /// </summary>
    private List<string> MatchDescriptions()
    {
        var parts = new List<string>();
        if (MetricName != null) parts.Add(MetricName);
        if (ServerName != null) parts.Add($"on {ServerName}");
        if (DatabasePattern != null) parts.Add($"db≈{DatabasePattern}");
        if (QueryTextPattern != null) parts.Add($"query≈{QueryTextPattern}");
        if (WaitTypePattern != null) parts.Add($"wait≈{WaitTypePattern}");
        if (JobNamePattern != null) parts.Add($"job≈{JobNamePattern}");
        return parts;
    }

    public string Summary
    {
        get
        {
            var parts = MatchDescriptions();
            return parts.Count > 0 ? string.Join(", ", parts) : "(matches all alerts)";
        }
    }

    /// <summary>
    /// True when the rule constrains NOTHING — no server, no metric, none of its patterns — so
    /// <see cref="Matches"/> accepts every alert on the store rather than one recurring alert. The blast
    /// radius, not the age: a blanket rule makes a whole fleet read quiet, which is why it is severity-
    /// bearing wherever a mute is reported.
    /// </summary>
    public bool MatchesEveryAlert => MatchDescriptions().Count == 0;

    public bool Matches(AlertMuteContext context)
    {
        if (!Enabled || IsExpired) return false;

        if (ServerName != null &&
            !string.Equals(ServerName, context.ServerName, StringComparison.OrdinalIgnoreCase))
            return false;

        if (MetricName != null &&
            !string.Equals(MetricName, context.MetricName, StringComparison.OrdinalIgnoreCase))
            return false;

        if (DatabasePattern != null &&
            (context.DatabaseName == null ||
             !context.DatabaseName.Contains(DatabasePattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (QueryTextPattern != null &&
            (context.QueryText == null ||
             !context.QueryText.Contains(QueryTextPattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (WaitTypePattern != null &&
            (context.WaitType == null ||
             !context.WaitType.Contains(WaitTypePattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        if (JobNamePattern != null &&
            (context.JobName == null ||
             !context.JobName.Contains(JobNamePattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        return true;
    }
}

public class AlertMuteContext
{
    public string ServerName { get; set; } = "";
    public string MetricName { get; set; } = "";
    public string? DatabaseName { get; set; }
    public string? QueryText { get; set; }
    public string? WaitType { get; set; }
    public string? JobName { get; set; }

    /// <summary>
    /// Extracts context fields (Database, Query, Wait Type, Job Name) from the
    /// structured detail_text stored with each alert. The format is label/value
    /// pairs indented with two spaces, e.g. "  Database: MyDB".
    /// Query values may span multiple lines and use variant labels
    /// (Blocked Query, Blocking Query, Victim SQL).
    /// </summary>
    /// <param name="metricName">The alert's metric name, when known. A custom alert rule
    /// (<c>"Custom:&lt;id&gt;"</c>) has no Database/Wait Type/Job/Query dimension, so its detail_text is
    /// NOT parsed for a mute-context pre-fill (#3309); omit or pass null for built-in alerts to parse as before.</param>
    public void PopulateFromDetailText(string? detailText, string? metricName = null)
    {
        if (string.IsNullOrEmpty(detailText)) return;

        /* #3309: a custom alert rule ("Custom:<id>") carries NO Database/Wait Type/Job/Query dimension. Its
           detail_text is the user-authored rule name/description, so DMV-label parsing it for a mute-context
           pre-fill is meaningless AND is the vector by which a crafted single-line name (e.g. "Database: master")
           could forge a label line. Skip it for custom alerts; their mute context is ServerName + MetricName. */
        if (metricName is not null && metricName.StartsWith("Custom:", StringComparison.Ordinal)) return;

        System.Text.StringBuilder? queryBuilder = null;
        var lines = detailText.Split('\n');

        foreach (var line in lines)
        {
            var trimmed = line.TrimStart();

            if (DatabaseName == null && trimmed.StartsWith("Database: ", StringComparison.Ordinal))
            {
                FlushQuery(ref queryBuilder);
                DatabaseName = trimmed.Substring("Database: ".Length).Trim();
            }
            else if (WaitType == null && trimmed.StartsWith("Wait Type: ", StringComparison.Ordinal))
            {
                FlushQuery(ref queryBuilder);
                WaitType = trimmed.Substring("Wait Type: ".Length).Trim();
            }
            else if (JobName == null && trimmed.StartsWith("Job Name: ", StringComparison.Ordinal))
            {
                FlushQuery(ref queryBuilder);
                JobName = trimmed.Substring("Job Name: ".Length).Trim();
            }
            else if (QueryText == null && queryBuilder == null && TryExtractQueryValue(trimmed, out var qv))
            {
                queryBuilder = new System.Text.StringBuilder(qv);
            }
            else if (queryBuilder != null)
            {
                // Continuation lines from multi-line query values don't start
                // with the two-space indent used by ContextToDetailText fields.
                if (string.IsNullOrWhiteSpace(trimmed) || line.StartsWith("  ", StringComparison.Ordinal))
                {
                    FlushQuery(ref queryBuilder);
                }
                else
                {
                    queryBuilder.Append(' ').Append(trimmed.Trim());
                }
            }
        }

        FlushQuery(ref queryBuilder);
    }

    private void FlushQuery(ref System.Text.StringBuilder? builder)
    {
        if (builder != null && QueryText == null)
            QueryText = builder.ToString();
        builder = null;
    }

    private static bool TryExtractQueryValue(string trimmed, out string value)
    {
        foreach (var prefix in new[] { "Query: ", "Blocked Query: ", "Blocking Query: ", "Victim SQL: " })
        {
            if (trimmed.StartsWith(prefix, StringComparison.Ordinal))
            {
                value = trimmed.Substring(prefix.Length).Trim();
                return true;
            }
        }
        value = "";
        return false;
    }
}
