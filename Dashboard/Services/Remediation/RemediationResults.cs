/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>Outcome status for a single per-target apply/unapply attempt.</summary>
    public enum RemediationStatus
    {
        /// <summary>The mutation ran and succeeded.</summary>
        Success,

        /// <summary>
        /// No mutation needed or possible, but not an error: already forced, the
        /// plan/query is gone (stale), or Query Store is not READ_WRITE. Audited
        /// as <c>skipped</c>.
        /// </summary>
        Skipped,

        /// <summary>
        /// The gate refused the target before any mutation (e.g. the connected DB
        /// is not the intended target, or the audit table is absent on the
        /// monitoring server). Audited as <c>aborted</c>.
        /// </summary>
        Blocked,

        /// <summary>
        /// The monitoring login lacks ALTER on the target database. Fails closed
        /// with grant guidance; no mutation, no elevation prompt. Audited as
        /// <c>skipped</c>.
        /// </summary>
        PermissionDenied,

        /// <summary>The mutation was attempted and the server raised an error.</summary>
        Error
    }

    /// <summary>Advisory pre-execution disposition for the UI (display driver only).</summary>
    public enum RemediationDisposition
    {
        Ok,
        AlreadyForced,
        WarnFailing,
        BlockQueryStoreOff,
        BlockStale,
        BlockNoAlter,
        BlockWrongDatabase,
        BlockAuditTableAbsent,

        /// <summary>DB_CONFIG: the setting is already in the desired state — idempotent skip (no ALTER).</summary>
        AlreadyInDesiredState,

        /// <summary>DB_CONFIG: the target database was not found on the server (renamed/dropped) — blocked, no ALTER.</summary>
        BlockDatabaseNotFound,

        /// <summary>
        /// SERVER_CONFIG: the setting is advise-only (max/min server memory) — the executor refuses
        /// to apply a guessed value, so the card is copy-paste only and never runs sp_configure.
        /// </summary>
        AdviseOnly,

        Error
    }

    /// <summary>
    /// Read-only preflight reading for one target (display driver only — never the
    /// authoritative gate; <c>ApplyAsync</c> re-derives its own gate before any
    /// mutation).
    /// </summary>
    public sealed class TargetPreflight
    {
        public string Database { get; init; } = "";
        public long QueryId { get; init; }
        public long PlanId { get; init; }
        public string? ExecutingLogin { get; init; }
        public string? CurrentDatabase { get; init; }
        public bool HasAlter { get; init; }
        public string? QueryStoreState { get; init; }
        public bool PlanPresent { get; init; }
        public bool IsForcedPlan { get; init; }
        public long ForceFailureCount { get; init; }
        public RemediationDisposition Disposition { get; set; }
        public string? Message { get; set; }
    }

    /// <summary>Aggregate preflight result for an action (one entry per target).</summary>
    public sealed class PreflightResult
    {
        public IReadOnlyList<TargetPreflight> Targets { get; init; } = new List<TargetPreflight>();

        /// <summary>
        /// Whether <c>config.remediation_action_log</c> exists on the monitoring
        /// server. When false, every target is hard-blocked at apply time (no
        /// mutation) — the server is on pre-3.0.0 schema.
        /// </summary>
        public bool AuditTableExists { get; init; }
    }

    /// <summary>
    /// Outcome of a single executor force/unforce call. The executor runs the
    /// authoritative gate and the EXEC on ONE open connection; <see cref="GateSpid"/>
    /// and <see cref="ExecSpid"/> are the server SPID observed at the gate read and
    /// at the mutation respectively — equal SPIDs prove the gate and the mutation
    /// rode the same connection (R2-MOD-1).
    /// </summary>
    public sealed class ForcePlanOutcome
    {
        public string Database { get; init; } = "";
        public long QueryId { get; init; }
        public long PlanId { get; init; }
        public RemediationStatus Status { get; init; }

        /// <summary>True only when an EXEC actually ran and succeeded.</summary>
        public bool Forced { get; init; }
        public string? ExecutingLogin { get; init; }
        public string? Message { get; init; }
        public int? GateSpid { get; init; }
        public int? ExecSpid { get; init; }
    }

    /// <summary>
    /// Read-only display probe for one DB-config target (advisory only — never the
    /// authoritative gate; <see cref="IRemediationExecutor.SetDatabaseOptionAsync"/>
    /// re-derives its own gate on the mutating connection before any ALTER).
    /// </summary>
    public sealed class DbConfigPreflight
    {
        public string Database { get; init; } = "";
        public DbConfigSetting Setting { get; init; }

        /// <summary>True when the database exists on the server (parameterized sys.databases check).</summary>
        public bool DatabaseExists { get; init; }

        /// <summary>HAS_PERMS_BY_NAME(@db,'DATABASE','ALTER') wrapped ISNULL(...,0).</summary>
        public bool HasAlter { get; init; }

        /// <summary>True when the live sys.databases read shows the setting already in the desired state.</summary>
        public bool AlreadyInDesiredState { get; init; }

        public string? ExecutingLogin { get; init; }

        /// <summary>The current value read live (display/audit prior value).</summary>
        public string? CurrentValue { get; init; }

        public RemediationDisposition Disposition { get; set; }
        public string? Message { get; set; }
    }

    /// <summary>
    /// Outcome of a single DB-config <c>ALTER DATABASE SET</c> attempt. The gate
    /// (existence + permission + freshness) and the ALTER run on ONE open monitoring
    /// connection; <see cref="GateSpid"/>/<see cref="ExecSpid"/> prove they shared it
    /// (R2-MOD-1). <see cref="GeneratedSql"/> is the exact statement executed.
    /// </summary>
    public sealed class DbConfigOutcome
    {
        public string Database { get; init; } = "";
        public DbConfigSetting Setting { get; init; }
        public RemediationStatus Status { get; init; }

        /// <summary>True only when an ALTER actually ran and succeeded.</summary>
        public bool Applied { get; init; }
        public string? ExecutingLogin { get; init; }
        public string? Message { get; init; }

        /// <summary>The prior setting value, captured at the gate read (audit prior_value).</summary>
        public string? PriorValue { get; init; }

        /// <summary>The exact ALTER DATABASE statement executed (audited generated_sql).</summary>
        public string? GeneratedSql { get; init; }
        public int? GateSpid { get; init; }
        public int? ExecSpid { get; init; }
    }

    /// <summary>
    /// Read-only display probe for one percent-autogrowth file target (advisory only — never
    /// the authoritative gate; <see cref="IRemediationExecutor.SetFileGrowthAsync"/> re-derives
    /// its own gate on the mutating connection before any ALTER). Mirrors
    /// <see cref="DbConfigPreflight"/>.
    /// </summary>
    public sealed class FileGrowthPreflight
    {
        public string Database { get; init; } = "";
        public string LogicalFileName { get; init; } = "";

        /// <summary>The already-computed fixed-MB FILEGROWTH target (probed for, never recomputed).</summary>
        public int RecommendedGrowthMb { get; init; }

        /// <summary>True when the database exists on the server (parameterized sys.databases check).</summary>
        public bool DatabaseExists { get; init; }

        /// <summary>True when the logical file exists on that database (parameterized sys.master_files check).</summary>
        public bool FileExists { get; init; }

        /// <summary>HAS_PERMS_BY_NAME(@db,'DATABASE','ALTER') wrapped ISNULL(...,0).</summary>
        public bool HasAlter { get; init; }

        /// <summary>True when the live read shows the file already at the desired fixed-MB growth.</summary>
        public bool AlreadyInDesiredState { get; init; }

        public string? ExecutingLogin { get; init; }

        /// <summary>The current growth value read live (display/audit prior value), e.g. "10%" or "256 MB".</summary>
        public string? CurrentValue { get; init; }

        public RemediationDisposition Disposition { get; set; }
        public string? Message { get; set; }
    }

    /// <summary>
    /// Outcome of a single percent-autogrowth <c>ALTER DATABASE … MODIFY FILE</c> attempt. The
    /// gate (existence + file existence + permission + freshness) and the ALTER run on ONE open
    /// monitoring connection; <see cref="GateSpid"/>/<see cref="ExecSpid"/> prove they shared it
    /// (R2-MOD-1). <see cref="GeneratedSql"/> is the exact statement executed. Mirrors
    /// <see cref="DbConfigOutcome"/>.
    /// </summary>
    public sealed class FileGrowthOutcome
    {
        public string Database { get; init; } = "";
        public string LogicalFileName { get; init; } = "";
        public RemediationStatus Status { get; init; }

        /// <summary>True only when an ALTER actually ran and succeeded.</summary>
        public bool Applied { get; init; }
        public string? ExecutingLogin { get; init; }
        public string? Message { get; init; }

        /// <summary>The prior growth value (e.g. "10%"), captured at the gate read (audit prior_value).</summary>
        public string? PriorValue { get; init; }

        /// <summary>The exact ALTER DATABASE … MODIFY FILE statement executed (audited generated_sql).</summary>
        public string? GeneratedSql { get; init; }
        public int? GateSpid { get; init; }
        public int? ExecSpid { get; init; }
    }

    /// <summary>
    /// Read-only display probe for one server-config target (advisory only — never the
    /// authoritative gate; <see cref="IRemediationExecutor.SetServerConfigAsync"/> re-derives its
    /// own gate on the mutating connection before any sp_configure). Mirrors
    /// <see cref="DbConfigPreflight"/>. Memory settings are advise-only — <see cref="Executable"/>
    /// is false and the executor refuses them.
    /// </summary>
    public sealed class ServerConfigPreflight
    {
        public ServerConfigSetting Setting { get; init; }

        /// <summary>The recommended value WS3 would apply (MAXDOP/CTFP) or display (memory).</summary>
        public long RecommendedValue { get; init; }

        /// <summary>True for MAXDOP/CostThreshold; false for the advise-only memory settings.</summary>
        public bool Executable { get; init; }

        /// <summary>ISNULL(HAS_PERMS_BY_NAME(NULL,NULL,'ALTER SETTINGS'),0)=1 OR sysadmin/serveradmin.</summary>
        public bool HasPermission { get; init; }

        /// <summary>True when the live read shows the setting already at the recommended value (executable settings only).</summary>
        public bool AlreadyInDesiredState { get; init; }

        public string? ExecutingLogin { get; init; }

        /// <summary>The current value read live (display/audit prior value).</summary>
        public long CurrentValue { get; init; }

        public RemediationDisposition Disposition { get; set; }
        public string? Message { get; set; }
    }

    /// <summary>
    /// Outcome of a single server-config <c>sp_configure</c>+<c>RECONFIGURE</c> attempt. The gate
    /// (permission + live freshness) and the mutation run on ONE open connection;
    /// <see cref="GateSpid"/>/<see cref="ExecSpid"/> prove they shared it (R2-MOD-1).
    /// <see cref="GeneratedSql"/> is the exact batch executed. Mirrors <see cref="DbConfigOutcome"/>.
    /// </summary>
    public sealed class ServerConfigOutcome
    {
        public ServerConfigSetting Setting { get; init; }
        public RemediationStatus Status { get; init; }

        /// <summary>True only when an sp_configure actually ran and succeeded.</summary>
        public bool Applied { get; init; }
        public string? ExecutingLogin { get; init; }
        public string? Message { get; init; }

        /// <summary>The prior value, captured at the gate read (audit prior_value).</summary>
        public long? PriorValue { get; init; }

        /// <summary>The exact sp_configure + RECONFIGURE batch executed (audited generated_sql).</summary>
        public string? GeneratedSql { get; init; }
        public int? GateSpid { get; init; }
        public int? ExecSpid { get; init; }
    }

    /// <summary>
    /// One live-resolved cached plan for a query hash: the database it ran in and a short
    /// query-text snippet (M-2 per-handle blast-radius disclosure). The plan handle itself
    /// is NEVER surfaced here — it is bound as a typed <c>varbinary(64)</c> parameter
    /// inside the executor and passed straight to DBCC, never round-tripped as a string.
    /// </summary>
    public sealed class ClearPlanHandleContext
    {
        public string? Database { get; init; }
        public string? QueryTextSnippet { get; init; }

        /// <summary>True only when this handle's DBCC FREEPROCCACHE ran without error.</summary>
        public bool Cleared { get; init; }

        /// <summary>Per-handle error message when <see cref="Cleared"/> is false due to a server error.</summary>
        public string? Error { get; init; }
    }

    /// <summary>
    /// Outcome of a single executor clear-cached-plan call for one query hash. The gate
    /// (ALTER SERVER STATE permission), the live handle resolve (with the null/zero-length
    /// guard), and every <c>DBCC FREEPROCCACHE(@plan_handle)</c> run on ONE open monitoring
    /// connection; <see cref="GateSpid"/>/<see cref="ExecSpid"/> prove they shared it
    /// (R2-MOD-1, GateSpid == ExecSpid). <see cref="HandlesCleared"/> is the count of
    /// handles actually cleared; <see cref="Handles"/> carries the per-handle disclosure
    /// context (M-2). PermissionDenied is the DOMINANT runtime path on a least-privilege
    /// install (the default login lacks ALTER SERVER STATE — opt-in feature).
    /// </summary>
    public sealed class ClearPlanOutcome
    {
        public string QueryHash { get; init; } = "";
        public RemediationStatus Status { get; init; }

        /// <summary>True only when at least one DBCC FREEPROCCACHE actually ran and succeeded.</summary>
        public bool Cleared { get; init; }

        /// <summary>How many plan handles were cleared (0 on Skip/PermissionDenied/Block).</summary>
        public int HandlesCleared { get; init; }

        public string? ExecutingLogin { get; init; }
        public string? Message { get; init; }

        /// <summary>Per-handle context for the disclosure / audit (M-2). Empty on a Skip.</summary>
        public IReadOnlyList<ClearPlanHandleContext> Handles { get; init; } = new List<ClearPlanHandleContext>();

        /// <summary>The DBCC FREEPROCCACHE statements actually run (display/audit generated_sql).</summary>
        public string? GeneratedSql { get; init; }

        /// <summary>A short prior-state summary for the audit prior_value ("{N} plans, ~{ratio}x baseline").</summary>
        public string? PriorValue { get; init; }
        public int? GateSpid { get; init; }
        public int? ExecSpid { get; init; }
    }

    /// <summary>Per-target outcome of an apply/unapply, including the audit disposition.</summary>
    public sealed class TargetOutcome
    {
        public string Database { get; init; } = "";
        public long QueryId { get; init; }
        public long PlanId { get; init; }
        public RemediationStatus Status { get; init; }
        public string? Message { get; init; }
        public string? ExecutingLogin { get; init; }

        /// <summary>Whether the audit row was written for this attempt.</summary>
        public bool AuditWritten { get; init; }

        /// <summary>
        /// The force succeeded but the audit INSERT failed against a present table
        /// (O3). Surfaced as a visible "applied-but-unlogged" warning; never the
        /// un-upgraded-server default (that case is hard-blocked before mutation).
        /// </summary>
        public bool AppliedButUnlogged { get; init; }
    }

    /// <summary>Aggregate apply/unapply result (one entry per target).</summary>
    public sealed class ApplyResult
    {
        public IReadOnlyList<TargetOutcome> Outcomes { get; init; } = new List<TargetOutcome>();
    }

    /// <summary>
    /// One row to write to <c>config.remediation_action_log</c>. Built by the
    /// handler for every attempt (success / skip / error / abort) and persisted on
    /// the monitoring connection by the audit writer, which fills
    /// <see cref="TargetServer"/> from the connection's DataSource when null.
    /// </summary>
    public sealed class RemediationAuditRecord
    {
        public string? OperatorIdentity { get; init; }
        public string? ExecutingLogin { get; init; }
        public string? TargetServer { get; set; }
        public string TargetDatabase { get; init; } = "";
        public string FactKey { get; init; } = "";
        public long? QueryId { get; init; }                 // force-plan only; null for DB_CONFIG
        public long? PlanId { get; init; }                  // force-plan only; null for DB_CONFIG
        public string Action { get; init; } = "";          // "force" | "unforce" | "set_*"
        public string? PriorValue { get; init; }            // DB_CONFIG prior value ("ON" | "NONE" | ...); null for force-plan
        public string? GeneratedSql { get; init; }
        public string Result { get; init; } = "";          // "success" | "skipped" | "error" | "aborted"
        public string? ErrorMessage { get; init; }

        /// <summary>
        /// B3 Phase 3 (B-3 / M-3): true only when this row records a DESTRUCTIVE apply
        /// that passed the informed-consent (acknowledge-each-risk) gate (RcsiHandler).
        /// Always false for the always-safe DB-config rows and the force-plan rows.
        /// Persisted to the queryable <c>consent_acknowledged</c> bit so a destructive
        /// apply is distinguishable in the log from an always-safe one.
        /// </summary>
        public bool ConsentAcknowledged { get; init; }

        public string? SourceAlertRef { get; init; }
    }
}
