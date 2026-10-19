/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Remediation;

public enum RemediationOutcome
{
    /// <summary>The action ran (or, on a dry run, every check passed and nothing was run).</summary>
    Done,

    /// <summary>A rule said no, before any write. <see cref="RemediationResult.Reason"/> says which.</summary>
    Refused,

    /// <summary>The server or the connection failed. <see cref="RemediationResult.Message"/> is the server's message, with the secret removed.</summary>
    Failed,
}

/// <summary>
/// One executor's answer (#5632, step 6). <see cref="Wrote"/> is true only when a write statement ran and the server accepted it; a dry run, a
/// refusal and a failure before the write all say false. The text of a secret is in no field: messages are scrubbed of it.
/// </summary>
public sealed record RemediationResult(
    RemediationOutcome Outcome, string Action, bool DryRun, bool Wrote, string? Reason, string Message)
{
    /// <summary>The exact statement a force, unforce or eviction runs (or would run, on a dry run). A force names the database quoted the way <c>QUOTENAME</c> quotes it; an eviction names the handle in its normal form. Null when the result is not about one.</summary>
    public string? Statement { get; init; }

    /// <summary>A kill's facts about the session it named, read in the batch that decided (#5632, step 12). Null for every other action, and when no such session was found.</summary>
    public KillSessionFacts? Session { get; init; }

    /// <summary>A PostgreSQL kill's facts about the session it named, read in the same sign-in that decided (#5632). Null for every other action, and when no such session was found.</summary>
    public PgKillSessionFacts? PgSession { get; init; }

    /// <summary>A real kill's rollback read (#5632, step 12): whether the session's request showed a rollback right after the kill. Null except after a kill.</summary>
    public KillRollback? Rollback { get; init; }

    public static class Reasons
    {
        public const string PermitMismatch = "permit_mismatch";
        public const string BadDatabase = "bad_database";
        public const string BadHandle = "bad_plan_handle";
        public const string BadSession = "bad_session_id";
        public const string PlanNotFound = "plan_not_found";
        public const string AlreadyForced = "plan_already_forced";
        public const string NotForced = "plan_not_forced";
        public const string NotCached = "plan_not_cached";
        public const string SessionGone = "session_gone";
        public const string SessionReused = "session_reused";
        public const string OwnSession = "own_session";
        public const string NotUserSession = "not_user_session";
        public const string KillPermissionDenied = "kill_permission_denied";
        public const string NotPermitted = "not_permitted";
        public const string KillNotBuiltOnPlatform = "kill_not_built_on_platform";
        public const string LoginMissing = "login_missing";
        public const string SecretSlotMissing = "secret_slot_missing";
        public const string StoredPassword = "stored_password_refused";
        public const string SecretUnreadable = "secret_unreadable";
        public const string SecretEmpty = "secret_empty";
        public const string SignInFailed = "sign_in_failed";
    }

    internal static RemediationResult Done(string action, bool dryRun, bool wrote, string message) =>
        new(RemediationOutcome.Done, action, dryRun, wrote, null, message);

    internal static RemediationResult Refused(string action, bool dryRun, string reason, string message) =>
        new(RemediationOutcome.Refused, action, dryRun, false, reason, message);

    internal static RemediationResult Failed(string action, bool dryRun, string message, string? reason = null) =>
        new(RemediationOutcome.Failed, action, dryRun, false, reason, message);
}
