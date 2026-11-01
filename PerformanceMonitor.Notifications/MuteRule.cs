/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitor.Notifications;

/// <summary>What a mute rule does to an alert it matches (#7083): <see cref="Mute"/> records the alert as muted and sends nothing,
/// <see cref="Digest"/> records it as a digest row and sends nothing, so it reaches the daily digest instead of a page. Stored as text.</summary>
public static class MuteRuleActions
{
    public const string Mute = "mute";
    public const string Digest = "digest";

    /// <summary>The stored word for <paramref name="action"/>: <see cref="Digest"/> when it says so (any case), else <see cref="Mute"/>.
    /// A value no build knows reads as a mute, the effect every rule had before the action existed.</summary>
    public static string Normalize(string? action) =>
        string.Equals(action?.Trim(), Digest, StringComparison.OrdinalIgnoreCase) ? Digest : Mute;

    /// <summary>Whether <paramref name="action"/> is one of the two words a write may carry (blank counts as the default, a mute).</summary>
    public static bool IsValid(string? action) =>
        string.IsNullOrWhiteSpace(action)
        || string.Equals(action.Trim(), Mute, StringComparison.OrdinalIgnoreCase)
        || string.Equals(action.Trim(), Digest, StringComparison.OrdinalIgnoreCase);

    public const string InvalidActionMessage = "action must be 'mute' or 'digest'.";
}

public class MuteRule
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public bool Enabled { get; set; } = true;
    public DateTime CreatedAtUtc { get; set; } = DateTime.UtcNow;
    public DateTime? ExpiresAtUtc { get; set; }

    /// <summary>The moment the rule begins to apply (#6231), or null for a rule that applies from the moment it exists, which is
    /// every rule stored before this field. A quiet window for planned work is a rule with a start in the future and an end: until
    /// the start passes, <see cref="MatchesAt"/> declines every alert, so the rule is stored and listed but mutes nothing. Lite's
    /// store does not persist it, so a Lite rule never carries one.</summary>
    public DateTime? StartsAtUtc { get; set; }

    /// <summary>What the rule does to a matching alert (#7083): <c>mute</c> (the default, and every rule stored before this field) or
    /// <c>digest</c>, which holds the alert back from paging and records it as a digest row. A digest rule is meant for a scheduled
    /// window (a nightly delete job), so it normally carries a recurring schedule. Darling only; Lite never sets it. See <see cref="MuteRuleActions"/>.</summary>
    public string Action { get; set; } = MuteRuleActions.Mute;

    /// <summary>Whether the rule sends a matching alert to the digest rather than muting it.</summary>
    public bool IsDigestAction => string.Equals(Action, MuteRuleActions.Digest, StringComparison.OrdinalIgnoreCase);

    /// <summary>The days of the week a recurring schedule (#7040) runs on, as a bit mask (bit n = <see cref="DayOfWeek"/> value n,
    /// Sunday = bit 0), or null for every day. Meaningful only with <see cref="RecurStartTime"/> and <see cref="RecurEndTime"/>.
    /// Darling only; Lite never sets any of the four recurrence fields, so a Lite rule is never recurring.</summary>
    public int? RecurDays { get; set; }

    /// <summary>The daily time the recurring window opens, in <see cref="RecurTimeZone"/>, or null for a rule with no schedule.
    /// Set together with <see cref="RecurEndTime"/>; a window that crosses midnight has the end earlier than the start.</summary>
    public TimeOnly? RecurStartTime { get; set; }

    /// <summary>The daily time the recurring window closes (exclusive); see <see cref="RecurStartTime"/>.</summary>
    public TimeOnly? RecurEndTime { get; set; }

    /// <summary>The IANA id of the zone the recurring times are read in. Set whenever the times are.</summary>
    public string? RecurTimeZone { get; set; }

    /// <summary>Whether the rule carries a recurring schedule.</summary>
    public bool IsRecurring => RecurStartTime.HasValue;

    /// <summary>Checks the rule's recurrence fields and normalises <see cref="RecurTimeZone"/> to an IANA id (the host's own zone
    /// when times are given with none). The one check every write path (MCP, web, Viewer) runs; see <see cref="MuteRecurrence.TryNormalize"/>.</summary>
    public bool TryNormalizeRecurrence(out string? error)
    {
        var zone = RecurTimeZone;
        if (!MuteRecurrence.TryNormalize(RecurDays, RecurStartTime, RecurEndTime, ref zone, out error)) return false;
        RecurTimeZone = zone;
        return true;
    }

    /// <summary>Whether the recurring schedule is inside a window at <paramref name="nowUtc"/>; always true for a rule with none.
    /// This is the schedule alone: the rule's start, expiry and enabled flag are separate (see <see cref="IsInEffectAt"/>).</summary>
    public bool IsInWindowAt(DateTime nowUtc) =>
        MuteRecurrence.IsInWindowAt(RecurDays, RecurStartTime, RecurEndTime, RecurTimeZone, nowUtc);

    /// <summary>The UTC start of the next recurring window after <paramref name="nowUtc"/> that falls inside the rule's span, or
    /// null for a rule with no schedule or no later window. A rule that has not started yet is searched from its
    /// <see cref="StartsAtUtc"/>: it reports the first window after it begins, or the start itself when a window is already open then.</summary>
    public DateTime? NextWindowStartUtc(DateTime nowUtc)
    {
        DateTime? next;
        if (StartsAtUtc.HasValue && StartsAtUtc.Value > nowUtc)
        {
            /* The rule mutes nothing before it starts. If a window is open at that moment (it opened before the start, or opens exactly then),
               the rule begins muting at the start; otherwise the first window after the start is the next one. */
            next = IsRecurring && IsInWindowAt(StartsAtUtc.Value)
                ? StartsAtUtc.Value
                : MuteRecurrence.NextWindowStartUtc(RecurDays, RecurStartTime, RecurEndTime, RecurTimeZone, StartsAtUtc.Value);
        }
        else
        {
            next = MuteRecurrence.NextWindowStartUtc(RecurDays, RecurStartTime, RecurEndTime, RecurTimeZone, nowUtc);
        }

        if (next is null) return null;
        if (ExpiresAtUtc.HasValue && next.Value >= ExpiresAtUtc.Value) return null;
        return next;
    }

    /// <summary>Whether both rules carry the same schedule: the same days, times and zone (the zone compared ignoring case), or neither has one.</summary>
    public bool HasSameSchedule(MuteRule other) =>
        RecurDays == other.RecurDays
        && RecurStartTime == other.RecurStartTime
        && RecurEndTime == other.RecurEndTime
        && string.Equals(RecurTimeZone, other.RecurTimeZone, StringComparison.OrdinalIgnoreCase);

    /// <summary>The schedule as one line (<c>Mon,Tue 22:00-02:00 America/New_York</c>), or null for a rule with none.</summary>
    public string? ScheduleText => MuteRecurrence.Describe(RecurDays, RecurStartTime, RecurEndTime, RecurTimeZone);

    /// <summary>Whether the rule applies at <paramref name="nowUtc"/>: enabled, past its start, before its expiry, and (for a
    /// recurring rule) inside a window. The single "is this rule active now" check, so every path that mutes uses the schedule.
    /// Match fields are separate (see <see cref="MatchesAt"/>).</summary>
    public bool IsInEffectAt(DateTime nowUtc) =>
        Enabled && !IsExpiredAt(nowUtc) && !IsUpcomingAt(nowUtc) && IsInWindowAt(nowUtc);

    public string? Reason { get; set; }

    public string? ServerName { get; set; }

    /// <summary>The store id of the ONE server this rule covers, or null for a rule scoped by
    /// <see cref="ServerName"/> alone. A display name is not an identity: a blank name falls back to the
    /// host, so two databases registered on one Azure SQL Database logical server share it, and a rule keyed
    /// on it covers both. A rule that carries an id is keyed on the id, and <see cref="ServerName"/> is then
    /// only the label the rule lists under. Null keeps the name-keyed match every rule had before this
    /// field existed, so a stored rule keeps exactly the effect it had. Lite never sets it.</summary>
    public int? ServerId { get; set; }

    /// <summary>The name of the ONE external alert source this rule covers (#5654), or null for a rule that is
    /// not limited to a source. A source is a service that raises alerts through Darling's channels with
    /// its own login; its name is the lower-case registry name, and it is compared ignoring case. A rule that
    /// carries one matches only an alert from that source, so it never mutes a built-in alert and never mutes
    /// another source. Null keeps the matching every rule had before this field existed: a rule without a source
    /// still mutes an external alert when its other dimensions (server, metric, patterns) match. Lite never sets it.</summary>
    public string? ExternalSource { get; set; }

    /// <summary>Whether <paramref name="name"/> is a well-formed external source name (#5654):
    /// <c>^[a-z][a-z0-9_]{1,30}$</c>, the shape the source registry's own check constraint holds every source name
    /// to. The one check the MCP create/update tools and the web mute routes run on a rule's source, so none of
    /// them can store a name the registry could never hold (a rule on such a name would mute nothing while
    /// reading as if it muted something). Upper case is refused, not folded: a stored name is lower case, and
    /// the refusal says so.</summary>
    public static bool IsValidExternalSource(string? name)
    {
        if (name is null || name.Length < 2 || name.Length > 31) return false;
        if (name[0] < 'a' || name[0] > 'z') return false;
        for (var i = 1; i < name.Length; i++)
        {
            var c = name[i];
            if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_')) return false;
        }
        return true;
    }

    /// <summary>The refusal text for a source name <see cref="IsValidExternalSource"/> rejects.</summary>
    public static string InvalidExternalSourceMessage(string? name) =>
        $"'external_source' is not a valid source name (got '{name}'). A source name is 2 to 31 characters, lower case: a letter, then letters, digits or underscores (e.g. 'nagios').";

    public string? MetricName { get; set; }
    public string? DatabasePattern { get; set; }
    public string? QueryTextPattern { get; set; }
    public string? WaitTypePattern { get; set; }
    public string? JobNamePattern { get; set; }

    /// <summary>Case-insensitive substring the alert's client application name must contain (#5640: mute
    /// SSMS cancels while still alerting on an application's own timeouts). A rule with this pattern never
    /// matches an alert that carries no client application, the same as every other text pattern.</summary>
    public string? ClientAppPattern { get; set; }

    /// <summary>Whether the rule's bound has passed as of <paramref name="nowUtc"/>. The clock-taking form,
    /// so a caller that already holds an injected clock judges expiry on the SAME instant it judges
    /// everything else — a rule read as unexpired by one clock and expired by another is a rule whose
    /// suppression decision depends on which line of the caller asked.</summary>
    public bool IsExpiredAt(DateTime nowUtc) => ExpiresAtUtc.HasValue && nowUtc >= ExpiresAtUtc.Value;

    public bool IsExpired => IsExpiredAt(DateTime.UtcNow);

    /// <summary>Whether the rule's start (#6231) is still ahead of <paramref name="nowUtc"/>: the rule exists but does not apply yet.</summary>
    public bool IsUpcomingAt(DateTime nowUtc) => StartsAtUtc.HasValue && nowUtc < StartsAtUtc.Value;

    /// <summary>The rule's state at <paramref name="nowUtc"/> as one word: <c>disabled</c> (paused by hand, whatever its times),
    /// <c>expired</c>, <c>upcoming</c> (its start is ahead) or <c>active</c>. The single place the word is decided, so the MCP read,
    /// the web list and the Viewer cannot disagree about which a rule is.</summary>
    public string StateAt(DateTime nowUtc) =>
        IsExpiredAt(nowUtc) ? "expired"
        : !Enabled ? "disabled"
        : IsUpcomingAt(nowUtc) ? "upcoming"
        : "active";

    public MuteRule Clone() => new()
    {
        Id = Id,
        Action = Action,
        Enabled = Enabled,
        CreatedAtUtc = CreatedAtUtc,
        ExpiresAtUtc = ExpiresAtUtc,
        StartsAtUtc = StartsAtUtc,
        RecurDays = RecurDays,
        RecurStartTime = RecurStartTime,
        RecurEndTime = RecurEndTime,
        RecurTimeZone = RecurTimeZone,
        Reason = Reason,
        ServerName = ServerName,
        ServerId = ServerId,
        ExternalSource = ExternalSource,
        MetricName = MetricName,
        DatabasePattern = DatabasePattern,
        QueryTextPattern = QueryTextPattern,
        WaitTypePattern = WaitTypePattern,
        JobNamePattern = JobNamePattern,
        ClientAppPattern = ClientAppPattern
    };

    /// <summary>The Action column (#7122): what the rule does with a matching alert, in the Viewer's own label. (The web list has no Action column: it marks a digest rule with "(to the daily digest)" on its scope cell and shows nothing for a mute rule.)</summary>
    public string ActionDisplay => IsDigestAction ? "Daily digest" : "Mute";

    /// <summary>The Starts column (#6231): "At once" for a rule with no start, the local start time otherwise, marked while it is still ahead.</summary>
    public string StartsDisplay => StartsAtUtc.HasValue
        ? StartsAtUtc.Value.ToLocalTime().ToString("g") + (IsUpcomingAt(DateTime.UtcNow) ? " (not started)" : "")
        : "At once";

    public string ExpiresDisplay => ExpiresAtUtc.HasValue
        ? (IsExpired ? "Expired" : ExpiresAtUtc.Value.ToLocalTime().ToString("g"))
        : "Never";

    /// <summary>
    /// The match dimensions this rule actually constrains, rendered one per entry. The SINGLE enumeration
    /// behind both <see cref="Summary"/> and <see cref="MatchesEveryAlert"/>, and it names the same fields
    /// <see cref="MatchesAt"/> tests (the server dimension is the id when the rule has one, else the name).
    ///
    /// <para>One list rather than two hand-kept copies: a seventh dimension added to <see cref="MatchesAt"/>
    /// but missed by a copied "is this rule unconstrained" predicate would make a rule narrowed ONLY by
    /// that new dimension read as matching every alert. Sharing the list makes the two answers move
    /// together by construction.</para>
    /// </summary>
    private List<string> MatchDescriptions()
    {
        var parts = new List<string>();
        if (MetricName != null) parts.Add(MetricName);
        if (ExternalSource != null) parts.Add($"from {ExternalSource}");
        if (ServerId.HasValue)
            parts.Add(ServerName != null ? $"on {ServerName} (#{ServerId.Value})" : $"on server #{ServerId.Value}");
        else if (ServerName != null)
            parts.Add($"on {ServerName}");
        if (DatabasePattern != null) parts.Add($"db≈{DatabasePattern}");
        if (QueryTextPattern != null) parts.Add($"query≈{QueryTextPattern}");
        if (WaitTypePattern != null) parts.Add($"wait≈{WaitTypePattern}");
        if (JobNamePattern != null) parts.Add($"job≈{JobNamePattern}");
        if (ClientAppPattern != null) parts.Add($"app≈{ClientAppPattern}");
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

    /// <summary>
    /// True when the rule's <see cref="MetricName"/> NAMES <paramref name="metricName"/> — the operator
    /// typed this metric into this rule, rather than the rule reaching it because the metric dimension was
    /// left unconstrained.
    ///
    /// <para>The distinction matters where a caller must tell a decision ABOUT an alert from a decision
    /// that merely covers it (#3348). It is a meaningful distinction here only because the metric dimension
    /// is EXACT: this is the same <see cref="StringComparison.OrdinalIgnoreCase"/> full-string equality
    /// <see cref="MatchesAt"/> applies, and <see cref="MatchesAt"/> routes its metric arm through this
    /// method so the two cannot drift into disagreeing about what "names" means. Unlike the four
    /// <c>*Pattern</c> dimensions there is no substring, glob or regex form of a metric constraint, so a
    /// rule either spells the metric out or does not constrain metrics at all — there is no third shape
    /// that could match one incidentally while looking deliberate.</para>
    /// </summary>
    public bool NamesMetric(string? metricName) =>
        MetricName != null
        && string.Equals(MetricName, metricName, StringComparison.OrdinalIgnoreCase);

    public bool Matches(AlertMuteContext context) => MatchesAt(context, DateTime.UtcNow);

    /// <summary>Whether this is a digest rule (#7083) that applies to <paramref name="context"/> at <paramref name="nowUtc"/>:
    /// <see cref="MatchesAt"/>'s test, for the rules <see cref="MatchesAt"/> declines because they send to the digest instead of muting.</summary>
    public bool MatchesDigestAt(AlertMuteContext context, DateTime nowUtc) => IsDigestAction && MatchesDimensions(context, nowUtc);

    /// <summary>The line a held fire's history row carries as its reason: which digest rule held it, and the window it was in.</summary>
    public string DigestHoldReason()
    {
        var name = string.IsNullOrWhiteSpace(Reason) ? Summary : Reason.Trim();
        var window = ScheduleText;
        return window is null
            ? $"Held for the daily digest by a digest rule ({name})"
            : $"Held for the daily digest by a digest rule ({name}) inside its window {window}";
    }

    /// <summary>
    /// <see cref="Matches"/> judged against a caller-supplied instant rather than the ambient clock, so a
    /// caller that already holds one decides the whole question on a single "now". Pure: no clock, no I/O.
    /// </summary>
    public bool MatchesAt(AlertMuteContext context, DateTime nowUtc) => !IsDigestAction && MatchesDimensions(context, nowUtc);

    /* The rule's own test, whatever its action: in effect now, and every dimension it constrains matches. A digest rule (#7083) is
       judged by MatchesDigestAt and never by MatchesAt, so the one check that mutes cannot be reached by a rule that does not mute. */
    private bool MatchesDimensions(AlertMuteContext context, DateTime nowUtc)
    {
        if (!IsInEffectAt(nowUtc)) return false;

        /* An id-keyed rule is decided on the id ALONE: the name is only the label the rule lists under, so a
           same-named sibling does not match and a renamed server does not escape. A context with no id never
           matches it. A rule without an id keeps the name test every stored rule was written against. */
        if (ServerId.HasValue)
        {
            if (context.ServerId != ServerId) return false;
        }
        else if (ServerName != null &&
            !string.Equals(ServerName, context.ServerName, StringComparison.OrdinalIgnoreCase))
            return false;

        /* A source rule is decided on the source ALONE for this dimension: a context with no source (every
           built-in alert, and Lite's) never matches it. A rule without a source skips the test, so it keeps
           muting an external alert on its other dimensions. */
        if (ExternalSource != null &&
            !string.Equals(ExternalSource, context.ExternalSource, StringComparison.OrdinalIgnoreCase))
            return false;

        if (MetricName != null && !NamesMetric(context.MetricName))
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

        if (ClientAppPattern != null &&
            (context.ClientAppName == null ||
             !context.ClientAppName.Contains(ClientAppPattern, StringComparison.OrdinalIgnoreCase)))
            return false;

        return true;
    }
}

public class AlertMuteContext
{
    public string ServerName { get; set; } = "";

    /// <summary>The store id of the server the alert is about, when the producer knows it. A rule with a
    /// <see cref="MuteRule.ServerId"/> matches only a context carrying that id; a null here never matches an
    /// id-keyed rule.</summary>
    public int? ServerId { get; set; }

    /// <summary>The external alert source the alert came from (#5654), or null for an alert that did not come
    /// from one. A rule with a <see cref="MuteRule.ExternalSource"/> matches only a context carrying that
    /// source; a null here never matches it. Lite never sets it.</summary>
    public string? ExternalSource { get; set; }

    public string MetricName { get; set; } = "";
    public string? DatabaseName { get; set; }
    public string? QueryText { get; set; }
    public string? WaitType { get; set; }
    public string? JobName { get; set; }

    /// <summary>The source name inside an external alert's metric name (<c>External:&lt;source&gt;:&lt;key&gt;</c>,
    /// #5654), or null for any other metric name. For a surface that holds the stored metric name and not the
    /// drain's own record of the source (the viewer's alert rows). A source name holds no colon, so the first
    /// colon after the prefix ends it.</summary>
    public static string? ExternalSourceOf(string? metricName)
    {
        if (metricName is null || !metricName.StartsWith(AlertFamily.ExternalMetricPrefix, StringComparison.Ordinal))
            return null;
        var rest = metricName.AsSpan(AlertFamily.ExternalMetricPrefix.Length);
        var colon = rest.IndexOf(':');
        return colon > 0 ? rest[..colon].ToString() : null;
    }
    /// <summary>The client application the alert is about (the attention events' client_app_name), or null
    /// when the alert has none. A rule with a <see cref="MuteRule.ClientAppPattern"/> never matches a null here (#5640).</summary>
    public string? ClientAppName { get; set; }

    /// <summary>The longest query-text pattern a mute dialog pre-fills from an alert.</summary>
    public const int SeedQueryTextMax = 200;

    /// <summary>The query-text pattern a mute dialog pre-fills for this alert, or null for none. #4348: a statement
    /// the collector withheld reads as <see cref="WithheldStatementMarker.Text"/>, and a rule on that text
    /// would "match" every withheld statement and nothing else, so no pattern is seeded from it. #5320: the text
    /// parsed out of a stored alert's detail may predate the statement filter (alert rows written before the
    /// filter were never judged), so the WHOLE text goes through <paramref name="judgeStatement"/> first and the
    /// cut to <see cref="SeedQueryTextMax"/> is taken from the judged text, never from the stored text (a cut
    /// first could leave half of a statement the whole of it names). A statement the judge withholds seeds
    /// nothing. This project cannot reference the project that holds the judge, so each dialog passes
    /// <c>SensitiveStatements.Text</c>.</summary>
    /// <param name="judgeStatement">The shared statement judge: returns the text unchanged when it is clean and
    /// the withheld marker (or null) when it is named. Required: there is no unjudged overload.</param>
    public string? SeedQueryTextPattern(Func<string?, string?> judgeStatement)
    {
        ArgumentNullException.ThrowIfNull(judgeStatement);
        if (string.IsNullOrEmpty(QueryText))
        {
            return null;
        }

        var judged = judgeStatement(QueryText);
        if (string.IsNullOrEmpty(judged) || WithheldStatementMarker.IsMarker(judged))
        {
            return null;
        }

        if (judged.Length <= SeedQueryTextMax)
        {
            return judged;
        }

        /* Do not cut between the halves of a surrogate pair. */
        var cut = char.IsHighSurrogate(judged[SeedQueryTextMax - 1]) ? SeedQueryTextMax - 1 : SeedQueryTextMax;
        return judged.Substring(0, cut);
    }

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

        /* #5654: an external alert's text is another service's. Its prose is one sanitized line, but a title that
           begins "Database: x" would still read as a label line, so no dimension is pre-filled for it either. */
        if (metricName is not null && metricName.StartsWith(AlertFamily.ExternalMetricPrefix, StringComparison.Ordinal)) return;

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
            else if (ClientAppName == null && trimmed.StartsWith("Client Application: ", StringComparison.Ordinal))
            {
                /* #5640: the Client Timeouts alert lists its client applications top first, and the FIRST line is the one a
                   mute dialog pre-fills (the same first-wins rule as Database). */
                FlushQuery(ref queryBuilder);
                ClientAppName = trimmed.Substring("Client Application: ".Length).Trim();
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
