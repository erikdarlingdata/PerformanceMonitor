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
using System.Linq;
using System.Text;

namespace PerformanceMonitor.Common;

/* #5913 (#5632 Phase B): what a person says about an investigation report. Darling only; Lite has no feedback table. */

/// <summary>
/// The rules on a person's feedback about a report (#5913), shared by the store, the desktop Viewer and the web routes so no
/// surface can write a record another refuses. They mirror the V191 table's CHECK constraints, so a bad value is refused
/// here in plain words instead of surfacing as a constraint violation.
/// </summary>
public static class InvestigationFeedbackRules
{
    /// <summary>What a person may say about the report: it was right, partly right or wrong.</summary>
    public static readonly IReadOnlyList<string> Verdicts = new[] { "right", "partly", "wrong" };

    /// <summary>The longest <c>missed_step</c> or <c>comment</c> (V191's CHECK).</summary>
    public const int MaxTextChars = 4000;

    /// <summary>The longest author (V191's CHECK).</summary>
    public const int MaxAuthorLength = 64;

    /// <summary>The report verdicts a person may correct the report to (the report's own domain).</summary>
    public static readonly IReadOnlyList<string> CorrectedVerdicts = new[] { "real", "false_positive", "needs_person" };

    /// <summary>
    /// True when <paramref name="author"/> is 1 to <see cref="MaxAuthorLength"/> characters from <c>[A-Za-z0-9._:-]</c>
    /// (V191's CHECK, the family's identity shape).
    /// </summary>
    public static bool IsValidAuthor(string? author) => InvestigationRules.IsValidAgentId(author);

    /// <summary>
    /// The author a person's sign-in name is stored as: every character outside <c>[A-Za-z0-9._:-]</c> becomes an underscore
    /// (so <c>DOMAIN\erik</c> is <c>DOMAIN_erik</c>, and an e-mail sign-in keeps its shape without the at sign), cut to
    /// <see cref="MaxAuthorLength"/>. Null when nothing is left to name (a blank name).
    /// </summary>
    public static string? AuthorFromIdentity(string? identity)
    {
        var trimmed = identity?.Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            return null;
        }

        var builder = new StringBuilder(Math.Min(trimmed.Length, MaxAuthorLength));
        foreach (var c in trimmed)
        {
            if (builder.Length >= MaxAuthorLength)
            {
                break;
            }

            builder.Append(c is (>= 'A' and <= 'Z') or (>= 'a' and <= 'z') or (>= '0' and <= '9') or '.' or '_' or ':' or '-' ? c : '_');
        }

        return builder.ToString();
    }

    /// <summary>The problems with <paramref name="feedback"/>, in plain words; empty when the record is acceptable.</summary>
    public static IReadOnlyList<string> Validate(InvestigationFeedbackSubmission feedback)
    {
        ArgumentNullException.ThrowIfNull(feedback);
        var problems = new List<string>();
        if (string.IsNullOrWhiteSpace(feedback.Verdict))
        {
            problems.Add("verdict: missing; say whether the report was right, partly or wrong.");
        }
        else if (!Verdicts.Contains(feedback.Verdict, StringComparer.Ordinal))
        {
            problems.Add("verdict: '" + Clip(feedback.Verdict) + "' is not right, partly or wrong.");
        }

        if (feedback.CorrectedVerdict is { } corrected && !CorrectedVerdicts.Contains(corrected, StringComparer.Ordinal))
        {
            problems.Add("corrected_verdict: '" + Clip(corrected) + "' is not real, false_positive or needs_person; leave it out to keep the report's verdict.");
        }

        if (feedback.MissedStep is { } missed && missed.Length > MaxTextChars)
        {
            problems.Add("missed_step: " + missed.Length.ToString(CultureInfo.InvariantCulture) + " characters is over the "
                + MaxTextChars.ToString(CultureInfo.InvariantCulture) + " allowed.");
        }

        if (feedback.Comment is { } comment && comment.Length > MaxTextChars)
        {
            problems.Add("comment: " + comment.Length.ToString(CultureInfo.InvariantCulture) + " characters is over the "
                + MaxTextChars.ToString(CultureInfo.InvariantCulture) + " allowed.");
        }

        if (!IsValidAuthor(feedback.Author))
        {
            problems.Add("author: 1 to 64 characters, each a letter, a digit, '.', '_', ':' or '-'.");
        }

        return problems;
    }

    /// <summary>The refusal sentence for <paramref name="problems"/>.</summary>
    public static string RefusalMessage(IReadOnlyList<string> problems) =>
        "The feedback was not recorded. " + string.Join(" ", problems);

    private static string Clip(string value) => value.Length <= 40 ? value : value[..40] + "...";
}

/// <summary>
/// One person's feedback on one report. <see cref="CorrectedVerdict"/> is what the report should have said when the person
/// disagrees (real, false_positive or needs_person); null keeps the report's verdict. <see cref="MissedStep"/> is the step
/// the agent should have taken. Blank text is stored as null.
/// </summary>
public sealed record InvestigationFeedbackSubmission(
    Guid RequestId,
    string Verdict,
    string? CorrectedVerdict,
    string? MissedStep,
    string? Comment,
    string Author);

/// <summary>What recording feedback answered.</summary>
public enum InvestigationFeedbackOutcome
{
    /// <summary>The author's first record for this request was stored.</summary>
    Recorded,

    /// <summary>The author had already given feedback on this request; this record replaced it.</summary>
    Replaced,

    /// <summary>A value failed the rules; nothing was written. <see cref="InvestigationFeedbackResult.Message"/> says which.</summary>
    Refused,

    /// <summary>No request has that id.</summary>
    NotFound,

    /// <summary>The request has no report to give feedback on.</summary>
    NoReport,
}

/// <summary>The result of recording feedback; <see cref="Message"/> is a plain sentence for every outcome but the two that wrote.</summary>
public sealed record InvestigationFeedbackResult(InvestigationFeedbackOutcome Outcome, string? Message);

/// <summary>One stored feedback record. <see cref="CreatedAtUtc"/> is when this record was written (a replacement restamps it).</summary>
public sealed record InvestigationFeedbackRow(
    Guid RequestId,
    string Verdict,
    string? CorrectedVerdict,
    string? MissedStep,
    string? Comment,
    string Author,
    DateTime CreatedAtUtc);

/// <summary>
/// One reported request with the feedback people gave it, for a feedback list. <see cref="ReportVerdict"/>,
/// <see cref="RootCause"/> and <see cref="Summary"/> are the agent's report.
/// </summary>
public sealed record InvestigationFeedbackListRow(
    Guid RequestId,
    string ServerName,
    string Family,
    string FirstMetric,
    DateTime OpenedAtUtc,
    DateTime ReportedAtUtc,
    string AgentId,
    string ReportVerdict,
    string RootCause,
    string? Summary,
    IReadOnlyList<InvestigationFeedbackRow> Feedback);

/// <summary>
/// How often one alert family's investigations were false positives (#5913). A request counts when it holds a report.
/// <see cref="AgentFalsePositives"/> are the reports that say false_positive; <see cref="FalsePositives"/> are the requests
/// whose <i>settled</i> verdict is false_positive: the corrected verdict of the newest feedback that gave one, else the
/// report's verdict. So feedback moves the count in both directions: a person correcting a real report to false_positive
/// raises it, and correcting a false_positive report to real lowers it. <see cref="WithFeedback"/> are the reported requests
/// somebody gave feedback on, and <see cref="MarkedWrong"/> those where the newest feedback says the report was wrong.
/// </summary>
public sealed record InvestigationFamilyCounts(
    string Family,
    int Reported,
    int WithFeedback,
    int MarkedWrong,
    int AgentFalsePositives,
    int FalsePositives);
