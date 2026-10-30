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

    /// <summary>The write statement was sent and its reply never came back (a lost connection, a timeout, a cancel): the server may or may not
    /// have done it. Not a failure: the caller must not journal it as failed, and must say the change may have happened.</summary>
    Unknown,
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

    /// <summary>A module recompile's read of the object it named (#6569): its <c>sys.objects.type_desc</c> (for example <c>SQL_STORED_PROCEDURE</c>). Null for every other action, and when the object was not found.</summary>
    public string? ObjectType { get; init; }

    /// <summary>A module recompile's resolved object (#6569): the schema the server resolved the input to (<c>OBJECT_SCHEMA_NAME</c> of the resolved id), not the input text. Null for every other action, and when the name did not resolve.</summary>
    public string? ObjectSchema { get; init; }

    /// <summary>A module recompile's resolved object name (<c>OBJECT_NAME</c> of the resolved id), not the input text. Null for every other action, and when the name did not resolve.</summary>
    public string? ObjectName { get; init; }

    /// <summary>A plan guide create's count of cached batches, compiled in the guide's own database, whose text equals a SQL guide's batch (#6569). Null for every other action, for an OBJECT guide, and when the login cannot read the plan cache.</summary>
    public long? CachedMatches { get; init; }

    /// <summary>A plan guide drop's read of the guide it named (#6569, slice 2b): the whole definition as <c>sys.plan_guides</c> holds it, which the journal keeps so an Undo can re-create the guide. Null for every other result, and when the guide was not found.</summary>
    public PlanGuideDefinition? Guide { get; init; }

    /// <summary>Whether the guide a drop read was disabled (<c>sys.plan_guides.is_disabled</c>); the Undo disables the re-created guide again. Null when <see cref="Guide"/> is null.</summary>
    public bool? GuideDisabled { get; init; }

    /// <summary>The message <c>sys.fn_validate_plan_guide</c> returned for a plan guide the create dropped again (#6569). Null for every other result.</summary>
    public string? Validation { get; init; }

    /// <summary>A kill's facts about the session it named, read in the batch that decided (#5632, step 12). Null for every other action, and when no such session was found.</summary>
    public KillSessionFacts? Session { get; init; }

    /// <summary>A PostgreSQL kill's facts about the session it named, read in the same sign-in that decided (#5632). Null for every other action, and when no such session was found.</summary>
    public PgKillSessionFacts? PgSession { get; init; }

    /// <summary>A real kill's rollback read (#5632, step 12): whether the session's request showed a rollback right after the kill. Null except after a kill.</summary>
    public KillRollback? Rollback { get; init; }

    /// <summary>A PostgreSQL kill's read right after the signal (#5632): true when the same backend (pid and start second) was still there,
    /// false when it was gone, null when the read did not run or failed.</summary>
    public bool? PgStillThere { get; init; }

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
        public const string BadModuleName = "bad_module_name";
        public const string ModuleNotFound = "module_not_found";
        public const string NotAModule = "not_a_module";
        public const string RecompilePermissionDenied = "recompile_permission_denied";
        public const string BadGuideName = "bad_guide_name";
        public const string BadGuideType = "bad_guide_type";
        public const string BadGuideArgument = "bad_guide_argument";
        public const string StatementNotInModule = "statement_not_in_module";
        public const string GuideExists = "guide_exists";
        public const string GuideNotFound = "guide_not_found";
        public const string GuideChanged = "guide_changed";
        public const string GuideInvalid = "guide_invalid";
        public const string GuidePermissionDenied = "guide_permission_denied";
        public const string SessionGone = "session_gone";
        public const string SessionReused = "session_reused";
        public const string OwnSession = "own_session";
        public const string NotUserSession = "not_user_session";
        public const string KillPermissionDenied = "kill_permission_denied";
        public const string NotPermitted = "not_permitted";
        public const string DatabaseRequired = "database_required";
        public const string NotAzureSqlDatabase = "not_azure_sql_database";
        public const string WrongDatabase = "wrong_database";
        public const string NotPrimary = "not_primary";
        public const string ViewStatePermissionDenied = "view_state_permission_denied";
        public const string SessionInOtherDatabase = "session_in_other_database";
        public const string ToolLoginSession = "tool_login_session";
        public const string DatabaseUnavailable = "database_unavailable";
        public const string LoginMissing = "login_missing";
        public const string SecretSlotMissing = "secret_slot_missing";
        public const string StoredPassword = "stored_password_refused";
        public const string SecretUnreadable = "secret_unreadable";
        public const string SecretEmpty = "secret_empty";
        public const string SignInFailed = "sign_in_failed";
        public const string SignInNotStrongEnough = "sign_in_not_strong_enough";
        public const string SearchPathNotPinned = "search_path_not_pinned";
        public const string OutcomeUnknown = "outcome_unknown";
        public const string BackendIdHoldsServerStart = "backend_id_has_no_session_start";
    }

    internal static RemediationResult Done(string action, bool dryRun, bool wrote, string message) =>
        new(RemediationOutcome.Done, action, dryRun, wrote, null, message);

    internal static RemediationResult Refused(string action, bool dryRun, string reason, string message) =>
        new(RemediationOutcome.Refused, action, dryRun, false, reason, message);

    internal static RemediationResult Unknown(string action, string message) =>
        new(RemediationOutcome.Unknown, action, false, false, RemediationResult.Reasons.OutcomeUnknown, message);

    internal static RemediationResult Failed(string action, bool dryRun, string message, string? reason = null) =>
        new(RemediationOutcome.Failed, action, dryRun, false, reason, message);
}
