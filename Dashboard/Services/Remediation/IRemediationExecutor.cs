/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;

namespace PerformanceMonitorDashboard.Services.Remediation
{
    /// <summary>
    /// The narrow, purpose-built seam between a remediation handler and the
    /// privileged server access. There is deliberately NO general
    /// <c>Execute(sql)</c> method: every member is a single, typed operation.
    ///
    /// <para>
    /// The authoritative gate lives inside <see cref="ForcePlanAsync"/> /
    /// <see cref="UnforcePlanAsync"/>: each opens ONE retargeted connection, runs
    /// the correct-database assert, the ALTER permission check, and the freshness
    /// probe on that same open connection, and only then issues the EXEC — no
    /// re-open between gate and mutation (R2-MOD-1). A handler cannot hand these
    /// methods a forged "all-clear"; the gate is re-derived internally every call.
    /// <see cref="PreflightForcePlanAsync"/> is the DISPLAY driver only.
    /// </para>
    /// </summary>
    public interface IRemediationExecutor
    {
        /// <summary>
        /// Read-only display probe for one target (permission, Query Store state,
        /// freshness, executing login, connected database). Advisory only — does
        /// NOT authorise a mutation.
        /// </summary>
        Task<TargetPreflight> PreflightForcePlanAsync(string database, long queryId, long planId, CancellationToken ct);

        /// <summary>
        /// Whether <c>config.remediation_action_log</c> exists on the MONITORING
        /// connection (PerformanceMonitor DB). When false, apply is hard-blocked
        /// before any mutation (R2-MOD-2).
        /// </summary>
        Task<bool> AuditTableExistsAsync(CancellationToken ct);

        /// <summary>
        /// Whether a prior successful <c>force</c> for this target was recorded and
        /// not since unforced — used to restrict un-apply to actions B3 itself
        /// applied. Queried on the MONITORING connection.
        /// </summary>
        Task<bool> HasPriorForceAsync(string database, long queryId, long planId, CancellationToken ct);

        /// <summary>
        /// Self-gating force. Opens one retargeted connection, runs the gate
        /// (DB_NAME assert + HAS_PERMS_BY_NAME(ALTER) + freshness) and — only if it
        /// passes — the <c>sp_query_store_force_plan</c> EXEC, on that same open
        /// connection.
        /// </summary>
        Task<ForcePlanOutcome> ForcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct);

        /// <summary>Self-gating inverse of <see cref="ForcePlanAsync"/> (sp_query_store_unforce_plan).</summary>
        Task<ForcePlanOutcome> UnforcePlanAsync(string database, long queryId, long planId, RemediationIdentity identity, CancellationToken ct);

        /// <summary>
        /// Read-only display probe for one DB-config target (existence, ALTER
        /// permission, live current value / desired-state). Advisory only — does NOT
        /// authorise a mutation. Runs on the monitoring connection (no retarget).
        /// </summary>
        Task<DbConfigPreflight> PreflightDbConfigAsync(string database, DbConfigSetting setting, CancellationToken ct);

        /// <summary>
        /// Self-gating always-safe DB-config ALTER. R2-MOD-1: the gate (parameterized
        /// sys.databases existence + HAS_PERMS_BY_NAME(@db,'DATABASE','ALTER') +
        /// live freshness read) and the <c>ALTER DATABASE [db] SET &lt;literal&gt;</c>
        /// run on ONE open MONITORING connection (no InitialCatalog retarget, no
        /// re-open between gate and mutation). The database identifier is validated
        /// against sys.databases (parameterized), then the SAME validated string is
        /// bracketed by an inline QUOTENAME-equivalent and placed as the ONLY
        /// non-constant token; the SET clause is a hardcoded compile-time literal
        /// chosen by <paramref name="setting"/>. Emits GateSpid/ExecSpid.
        /// </summary>
        Task<DbConfigOutcome> SetDatabaseOptionAsync(string database, DbConfigSetting setting, RemediationIdentity identity, CancellationToken ct);

        /// <summary>
        /// Read-only display probe for one percent-autogrowth file target (database +
        /// file existence, ALTER permission, live current growth / desired-state).
        /// Advisory only — does NOT authorise a mutation. Runs on the monitoring
        /// connection (no retarget).
        /// </summary>
        Task<FileGrowthPreflight> PreflightFileGrowthAsync(string database, string logicalFileName, int growthMb, CancellationToken ct);

        /// <summary>
        /// Self-gating percent-autogrowth ALTER. R2-MOD-1: the gate (parameterized
        /// sys.databases + sys.master_files existence + HAS_PERMS_BY_NAME(@db,'DATABASE',
        /// 'ALTER') + live freshness read) and the
        /// <c>ALTER DATABASE [db] MODIFY FILE (NAME = [logical], FILEGROWTH = N MB)</c>
        /// run on ONE open MONITORING connection (no InitialCatalog retarget — the database
        /// is named in the statement; no re-open between gate and mutation). Both identifiers
        /// are validated (parameterized), then the SAME validated strings are bracketed and
        /// placed as the ONLY non-constant tokens alongside the already-computed int
        /// <paramref name="growthMb"/> literal. Metadata-only, online, non-destructive. Emits
        /// GateSpid/ExecSpid.
        /// </summary>
        Task<FileGrowthOutcome> SetFileGrowthAsync(string database, string logicalFileName, int growthMb, RemediationIdentity identity, CancellationToken ct);

        /// <summary>
        /// Read-only display probe for one server-config target (named/role permission, live
        /// current value, executable flag). Advisory only — does NOT authorise a mutation. Runs on
        /// the monitoring connection to the target server (server-scoped, no retarget).
        /// </summary>
        Task<ServerConfigPreflight> PreflightServerConfigAsync(ServerConfigSetting setting, long recommendedValue, CancellationToken ct);

        /// <summary>
        /// Self-gating server-config apply (WS3). R2-MOD-1: the gate (named ALTER SETTINGS /
        /// sysadmin / serveradmin + live current value) and the
        /// <c>sp_configure 'show advanced options',1; RECONFIGURE; sp_configure '&lt;hardcoded&gt;',@value; RECONFIGURE;</c>
        /// batch run on ONE open connection to the target server (server-scoped — no retarget, no
        /// re-open between gate and mutation). The sp_configure NAME is a HARDCODED literal selected
        /// by <paramref name="setting"/>; the value is bound as @value INT (range-validated first).
        /// ONLY Maxdop / CostThreshold are executable; the two memory settings return an error
        /// outcome WITHOUT touching the server (advise-only). Emits GateSpid/ExecSpid.
        /// </summary>
        Task<ServerConfigOutcome> SetServerConfigAsync(ServerConfigSetting setting, long value, RemediationIdentity identity, CancellationToken ct);

        /// <summary>
        /// Self-gating clear-cached-plan (DBCC FREEPROCCACHE). On ONE open monitoring
        /// connection to the TARGET server (FREEPROCCACHE is server-scoped — no
        /// InitialCatalog retarget), in order: the NAMED ALTER SERVER STATE permission
        /// gate (<c>ISNULL(HAS_PERMS_BY_NAME(NULL,NULL,'ALTER SERVER STATE'),0)=1</c>,
        /// fail-closed — the documented least-privilege login lacks it, so
        /// PermissionDenied is the PRIMARY path); then a LIVE resolve of the current
        /// cached <c>plan_handle(s)</c> for the typed <c>query_hash</c> param (selecting
        /// only <c>plan_handle IS NOT NULL</c>); then — for each non-null, non-zero-length
        /// handle (rejected in C# BEFORE any DBCC string is built) — one
        /// <c>DBCC FREEPROCCACHE(@plan_handle)</c> with a typed <c>varbinary(64)</c> param.
        /// There is NO code path that emits the bare/whole-cache form. An empty resolve
        /// set is a Skip (nothing to clear). Emits GateSpid/ExecSpid (R2-MOD-1).
        /// </summary>
        Task<ClearPlanOutcome> ClearProcCacheAsync(string queryHash, RemediationIdentity identity, CancellationToken ct);

        /// <summary>
        /// Writes one audit row on the MONITORING connection. Returns false if the
        /// INSERT failed against a present table (the O3 applied-but-unlogged case);
        /// the absent-table case never reaches here (hard-blocked earlier).
        /// </summary>
        Task<bool> WriteAuditAsync(RemediationAuditRecord record, CancellationToken ct);
    }
}
