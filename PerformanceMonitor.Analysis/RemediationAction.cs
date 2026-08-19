/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Structured, typed remediation payload persisted alongside the rendered
/// preview SQL so an Apply action executes from typed parameters, never by
/// parsing the preview text. Built by <see cref="FactRemediation.BuildAction"/>
/// from the same drill-down extraction that renders the preview, and round-trips
/// through the alert context (PerformanceMonitor.Notifications) so the in-app
/// dialog can drive a structured, parameterised execution.
///
/// <para>
/// This is data only — it carries no execution authority. The privileged
/// execution path (Dashboard) re-derives its own gate (permission, freshness,
/// correct-database) immediately before each mutation and never trusts these
/// values as anything but typed inputs.
/// </para>
/// </summary>
public sealed record RemediationAction(
    string FactKey,                              // "PLAN_REGRESSION" | "DB_CONFIG" | "RCSI" | "CLEAR_PLAN" | "FILE_AUTOGROWTH_PERCENT" — handler-registry key
    string Action,                              // "force" (plan regression) | "set" (db config / file autogrowth) | "clear" (clear cached plan). Un-apply derives "unforce".
    IReadOnlyList<ForcePlanTarget> Targets,      // force-plan targets (empty list for DB_CONFIG / CLEAR_PLAN)
    IReadOnlyList<DbConfigTarget>? DbConfigTargets = null,  // DB-config targets (null for force-plan)
    RcsiInactionFigures? RcsiFigures = null,     // B3 Phase 3: RCSI risk-of-not-changing figures carried on the persisted action
    IReadOnlyList<ClearPlanTarget>? ClearPlanTargets = null, // clear-cached-plan targets (null for the other fact keys)
    ClearPlanFigures? ClearPlanFigures = null,   // clear-cached-plan risk-of-not-changing figures carried on the persisted action
    IReadOnlyList<FileGrowthTarget>? FileGrowthTargets = null, // WS3: percent-autogrowth files — Apply-able (FileAutogrowthHandler: MODIFY FILE FILEGROWTH) + copy-paste
    IReadOnlyList<RcsiTarget>? RcsiTargets = null, // per-DB RCSI targets CARRIED on a DB_CONFIG action so the reader can fan per-db RCSI cards on read — NEVER executed from here (see RcsiTarget)
    IReadOnlyList<ServerConfigTarget>? ServerConfigTargets = null, // WS3: bad server-level config (MAXDOP/CTFP Apply-able via ServerConfigHandler; max/min memory advise-only) — one card per target
    IReadOnlyList<MissingIndexTarget>? MissingIndexTargets = null); // WS4: missing-index CREATE statements — COPY-PASTE ONLY (no handler -> never Apply); carried so the reader can render the CREATE on read (the drill-down is ephemeral)

/// <summary>
/// The fixed, hardcoded set of SERVER-LEVEL configuration settings WS3 surfaces (and, for
/// the first two, can apply). This enum — NOT any data/operator-supplied string — selects the
/// HARDCODED <c>sp_configure</c> name literal in the executor (see
/// DatabaseService.Remediation server-config arm). <see cref="Maxdop"/> and
/// <see cref="CostThreshold"/> are executable (an online metadata change, not destructive).
/// <see cref="MaxServerMemory"/> and <see cref="MinServerMemory"/> are ADVISE-ONLY: their
/// correct value is RAM/workload-dependent, so the executor REFUSES to apply a guessed value —
/// the cards carry only copy-paste SQL.
/// </summary>
public enum ServerConfigSetting
{
    /// <summary>EXEC sys.sp_configure N'max degree of parallelism', N; RECONFIGURE; — executable.</summary>
    Maxdop,

    /// <summary>EXEC sys.sp_configure N'cost threshold for parallelism', N; RECONFIGURE; — executable.</summary>
    CostThreshold,

    /// <summary>
    /// max server memory (MB) — ADVISE-ONLY. The right value depends on total RAM, other
    /// instances, and the OS footprint; the executor never applies a guessed value.
    /// </summary>
    MaxServerMemory,

    /// <summary>
    /// min server memory (MB) — ADVISE-ONLY. Recommended only when pinned near max (which
    /// starves the OS / stops SQL releasing memory); lowering it is workload-judgement, so the
    /// executor never applies a value.
    /// </summary>
    MinServerMemory
}

/// <summary>
/// One server-level config target (WS3): a (setting, current, recommended) triple. Each maps
/// 1:1 onto a Recommendations card and — for <see cref="ServerConfigSetting.Maxdop"/> /
/// <see cref="ServerConfigSetting.CostThreshold"/> — an independent
/// <c>sp_configure</c>+<c>RECONFIGURE</c> Apply and audit row.
/// <see cref="CurrentValue"/> is a display/audit prior-value snapshot; the executor re-reads
/// the live value at apply. <see cref="RecommendedValue"/> is the already-computed target used
/// verbatim as the bound <c>@value</c> (MAXDOP: edition-aware, capped at cores-per-socket;
/// CTFP: 50). For the two memory settings <see cref="RecommendedValue"/> is carried for the
/// copy-paste/advice only — the executor refuses to apply it.
/// </summary>
public sealed record ServerConfigTarget(
    ServerConfigSetting Setting,
    long CurrentValue,
    long RecommendedValue);

/// <summary>
/// One per-database RCSI target — a (database, inaction-figures) pair carried on the
/// DB_CONFIG <see cref="RemediationAction.RcsiTargets"/> list PURELY so the Recommendations
/// reader can fan one per-database RCSI card on read (the <c>config_issues</c> drill-down they
/// were collected from is ephemeral and is NOT returned by the store read-back).
///
/// <para>
/// These are NEVER executed from the DB_CONFIG action: <c>DbConfigHandler</c> only ever
/// executes the always-safe <see cref="RemediationAction.DbConfigTargets"/> and ignores this
/// list entirely, so no destructive RCSI change can ride the safe action. For each target the
/// reader reconstructs a DISTINCT <c>FactKey="RCSI"</c> <see cref="RemediationAction"/> (a single
/// <see cref="DbConfigSetting.ReadCommittedSnapshotOn"/> target + these <see cref="Figures"/>),
/// which is what runs through <c>RcsiHandler</c> (IsDestructive == true) behind the two-sided
/// informed-consent gate — identical to the action <see cref="FactRemediation.BuildRcsiAction"/>
/// produces for the alert path. <see cref="Figures"/> is carried so that reconstructed action's
/// consent dialog renders the REAL blocking/deadlock/reader-writer numbers.
/// </para>
/// </summary>
public sealed record RcsiTarget(string Database, RcsiInactionFigures Figures);

/// <summary>
/// The risk-of-NOT-changing monitoring figures for a destructive CLEAR_PLAN action
/// (clear cached plan via DBCC FREEPROCCACHE), captured at
/// <see cref="FactRemediation.BuildClearPlanAction"/> time when the finding (and its
/// <c>abnormal_cpu_plans</c> drill-down enrichment) IS available, and CARRIED on the
/// persisted <see cref="RemediationAction"/> so the informed-consent dialog renders the
/// REAL figures at apply time — when only the persisted action survives (the UI apply
/// call site has no finding). <see cref="FactRiskDisclosure.GetForAction"/> reads these
/// in preference to the (often-null at apply time) finding.
///
/// <para>
/// These are DISPLAY/disclosure values only — never an execution input. They mirror the
/// §2 detector enrichment fields: current vs baseline per-exec CPU (ms), the anomaly
/// ratio, the query's window CPU share (%), and whether PLAN_REGRESSION /
/// PARAMETER_SENSITIVITY co-fired (which steer the honest tool-choice disclosure).
/// </para>
/// </summary>
public sealed record ClearPlanFigures(
    double CurrentCpuPerExecMs,
    double BaselineCpuPerExecMs,
    double AnomalyRatio,
    int CpuPercent,
    bool PlanRegressionCoFired,
    bool ParameterSensitivityCoFired);

/// <summary>
/// One clear-cached-plan target: a (database, query_hash) pair. The
/// <see cref="QueryHash"/> (the stable cross-collection key, <c>binary(8)</c> rendered
/// as a hex string like <c>0x...</c>) is the ONLY execution input — the executor
/// re-resolves the current cached <c>plan_handle(s)</c> for it LIVE at apply time
/// (the snapshot <see cref="LatestPlanHandle"/> is display only and is NEVER fed to
/// DBCC). A <c>query_hash</c> is not unique to one logical query, so the live resolve
/// can return several handles spanning distinct databases — disclosed per-handle (M-2).
/// The remaining members are carried for the confirm dialog / disclosure only.
/// </summary>
public sealed record ClearPlanTarget(
    string Database,                            // user DB name (display; the gate is server-scoped, not per-DB)
    string QueryHash,                          // stable key, hex "0x..." (binary(8)); the only execution input
    double CurrentCpuPerExecMs = 0,            // display only
    double BaselineCpuPerExecMs = 0,           // display only
    double AnomalyRatio = 0,                   // display only
    string? LatestPlanHandle = null);          // display only — the apply path re-resolves live

/// <summary>
/// The risk-of-NOT-changing monitoring figures for a destructive RCSI action
/// (B3 Phase 3), captured at <see cref="FactRemediation.BuildRcsiAction"/> time when
/// the finding (and its drill-down enrichment) IS available, and CARRIED on the
/// persisted <see cref="RemediationAction"/> so the informed-consent dialog renders
/// the REAL figures at apply time — when only the persisted action survives (the UI
/// apply call site has no finding). <see cref="FactRiskDisclosure.GetForAction"/>
/// reads these in preference to the (often-null at apply time) finding, falling back
/// to the weak-case baseline only when they are genuinely absent.
///
/// <para>
/// These are DISPLAY/disclosure values only — never an execution input. The mirror
/// the §3.3 drill-down enrichment fields: per-database blocked-process event count,
/// deadlock count, and reader-vs-writer share (0–100, null when no reader/writer
/// blocked-process rows were captured).
/// </para>
/// </summary>
public sealed record RcsiInactionFigures(
    int BlockingEvents,
    int Deadlocks,
    int? ReaderWriterPct);

/// <summary>
/// The fixed, hardcoded set of database settings B3 can apply. This enum — NOT any
/// data/operator-supplied string — selects the SET clause literal in the executor
/// (see DatabaseService.Remediation DB-config arm). The first three are ALWAYS-SAFE
/// online metadata changes (Phase 2). <see cref="ReadCommittedSnapshotOn"/> (Phase 3)
/// is DESTRUCTIVE — it is routed through the distinct "RCSI" fact key + RcsiHandler
/// (IsDestructive == true) behind the informed-consent gate, NEVER through the
/// always-safe DbConfigHandler.
/// </summary>
public enum DbConfigSetting
{
    /// <summary>ALTER DATABASE [db] SET AUTO_SHRINK OFF;</summary>
    AutoShrinkOff,

    /// <summary>ALTER DATABASE [db] SET AUTO_CLOSE OFF;</summary>
    AutoCloseOff,

    /// <summary>ALTER DATABASE [db] SET PAGE_VERIFY CHECKSUM;</summary>
    PageVerifyChecksum,

    /// <summary>
    /// ALTER DATABASE [db] SET READ_COMMITTED_SNAPSHOT ON; — DESTRUCTIVE (B3 Phase 3).
    /// Takes a brief exclusive DB lock to enable, adds tempdb version-store load, and
    /// changes reader/writer concurrency semantics. Routed only through the "RCSI"
    /// fact key + RcsiHandler behind the informed-consent gate.
    /// </summary>
    ReadCommittedSnapshotOn
}

/// <summary>
/// One always-safe database-config target: a (database, setting) pair. Each maps
/// 1:1 onto an independent ALTER DATABASE statement, an audit row, and a confirm
/// row. <see cref="Database"/> is validated non-empty by the extractor and
/// re-validated against sys.databases at apply time; <see cref="Setting"/> is the
/// hardcoded-literal selector (NOT free text). <see cref="CurrentValue"/> is a
/// possibly-stale display/audit prior-value snapshot ONLY — it is never an
/// execution input (the live apply-time sys.databases read drives the skip).
/// </summary>
public sealed record DbConfigTarget(
    string Database,
    DbConfigSetting Setting,
    string? CurrentValue = null);

/// <summary>
/// One percent-autogrowth file target (WS3): a large data/log file set to grow in
/// PERCENTAGE steps, which on a big file is a single huge allocation that stalls
/// writes. This is an APPLY-able payload (FileAutogrowthHandler runs one
/// <c>ALTER DATABASE [db] MODIFY FILE (NAME = [logical], FILEGROWTH = N MB)</c> per
/// target — a metadata-only, online, non-destructive change, same safety class as
/// AUTO_SHRINK OFF) AND a copy-paste payload (the reader renders the same statement).
/// <see cref="Database"/> and <see cref="LogicalFileName"/> are the execution inputs
/// (each re-validated against sys.databases / sys.master_files at apply time, then
/// bracketed); <see cref="RecommendedGrowthMb"/> is the already-computed fixed-MB step
/// (1024 for data/ROWS files, 64 for LOG files) used verbatim as the FILEGROWTH literal.
/// <see cref="CurrentSizeMb"/> / <see cref="CurrentGrowthPercent"/> are display only.
/// </summary>
public sealed record FileGrowthTarget(
    string Database,                            // user DB name (validated + bracketed at apply)
    string LogicalFileName,                     // sys.database_files.name (validated + bracketed at apply)
    double CurrentSizeMb,                       // total_size_mb — display only
    int CurrentGrowthPercent,                   // growth_pct — display only
    int RecommendedGrowthMb);                   // already-computed fixed-MB FILEGROWTH (1024 data / 64 log)

/// <summary>
/// One missing-index suggestion (WS4): the SQL Server-generated <c>CREATE INDEX</c> statement
/// for a query plan's missing-index request, surfaced as COPY-PASTE ONLY. Unlike
/// <see cref="FileGrowthTarget"/>, this carries NO handler and is NEVER Apply-able — creating an
/// index is a judgement call (over-indexing, write/storage cost), so the operator copies the
/// suggested statement and decides. It rides the persisted <see cref="RemediationAction"/> (FactKey
/// "MISSING_INDEX") purely so the Recommendations reader can render <see cref="CreateStatement"/>
/// on read; the drill-down it was extracted from (<c>missing_indexes</c>) is ephemeral. The reader
/// surfaces the statement as the card's copy-paste SQL and leaves the card's <c>Remediation</c>
/// null so no Apply button appears. <see cref="Table"/> (schema-qualified) and <see cref="Impact"/>
/// are display only.
/// </summary>
public sealed record MissingIndexTarget(
    string Table,                               // "schema.table" (display)
    double Impact,                             // estimated % improvement (display)
    string CreateStatement);                    // the SQL Server-suggested CREATE INDEX — the copy-paste payload

/// <summary>
/// One force-plan target. <see cref="Database"/>, <see cref="QueryId"/> and
/// <see cref="PlanId"/> are the only execution inputs (database is applied solely
/// as the connection's InitialCatalog; the IDs are passed as typed BigInt
/// parameters to sys.sp_query_store_force_plan). The remaining members are
/// carried for the freshness re-check and operator display only — they are NOT
/// execution inputs.
/// </summary>
public sealed record ForcePlanTarget(
    string Database,                            // user DB name (validated non-empty by the extractor)
    long QueryId,                              // > 0
    long PlanId,                               // > 0  (== best_plan_id from the finding)
    string? BestPlanHash = null,               // display / freshness only
    string? LatestPlanHash = null,             // display / freshness only
    double LatestCpuPerExecUs = 0,             // display only (renders the preview comment)
    double BestCpuPerExecUs = 0,               // display only (renders the preview comment)
    double RegressionFactor = 0,               // display only (surfaced in the confirm modal)

    /* #1882: WHICH REPLICA'S workload this regression was measured on — sys.query_store_replicas'
       replica_name, verbatim from the collector: "Primary", "Secondary", "Geo Secondary",
       "Geo HA Secondary", or empty/null when the server did not attribute the row (anything below
       SQL Server 2022, a non-AG standalone, or Managed Instance). Display + disclosure only, never
       an execution input — see FactRemediation.ExtractPlanRegressionTargets for why naming a replica
       is not the same as being able to TARGET one. Appended last with a default so the positional
       construction sites and the persisted-action JSON round-trip stay source- and wire-compatible. */
    string? ReplicaRole = null,

    /* #2138 gap 3: this query's hash ALSO carried the PARAMETER_SENSITIVITY detector's plan-cache
       signature in the same analysis window (computed with the detector's own thresholds inside the
       regressed_queries drill-down, so the flag can never disagree with the fact). Steers the caution
       block in the force-plan preview — forcing the "best" plan on a parameter-sensitive query pins
       one shape for ALL parameter values — and is the standing gate for the future auto-force bot:
       a flagged target is never auto-forced, it gets an investigate verdict. Display + disclosure
       only. Appended with a default for the same wire-compatibility reasons as ReplicaRole. */
    bool ParameterSensitivityCoFired = false);
