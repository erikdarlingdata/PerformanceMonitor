/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Common;

/// <summary>
/// What a frozen investigation evidence row says when the statement filter withheld a whole document it is read from (#9873):
/// a deadlock graph (<c>graph_withheld</c>), a blocked-process report (<c>report_withheld</c>) or a stored plan
/// (<c>good_plan_withheld</c>, <c>bad_plan_withheld</c>). Darling's capture and Lite's capture both write these sentences, so a
/// reader sees the same words from either product.
/// </summary>
internal static class InvestigationEvidenceWithheld
{
    /// <summary>What a row says when the statement filter withheld its whole deadlock graph.</summary>
    internal const string GraphReason =
        "The statement filter withheld the whole deadlock graph (it ran out of time reading it, or could not read it safely), so its parties and objects are not shown.";

    /// <summary>What a row says when the statement filter withheld the whole blocked-process report its head blocker is read from.</summary>
    internal const string ReportReason =
        "The statement filter withheld the whole blocked-process report (it ran out of time reading it, or could not read it safely), so the blocker's statement is not read from it.";

    /// <summary>What a row says when the statement filter withheld a whole stored plan its compiled values are read from.</summary>
    internal const string PlanReason =
        "The statement filter withheld the whole plan (it ran out of time reading it, or could not read it safely), so its compiled values are not shown.";
}
