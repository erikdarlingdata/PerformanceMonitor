/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// What went wrong on a monitored target, named in terms the collection loop cares about rather than
/// in one engine's error numbers. The runners classify a driver exception into one of these so the
/// same handling — SKIPPED vs YIELDED vs PERMISSIONS vs ERROR, reconnect or not — works for any
/// engine.
/// <para>Today's SQL Server behaviour is the definition of correct here: this enum exists to
/// preserve it while making the same decisions expressible for PostgreSQL, whose failures arrive as
/// five-character SQLSTATEs rather than integer error numbers.</para>
/// </summary>
public enum CollectorTargetFault
{
    /// <summary>
    /// Not a recognized fault class. Handled as an ERROR, which is the correct default: an
    /// unclassified failure must be loud, not quietly swallowed.
    /// </summary>
    Unclassified = 0,

    /// <summary>
    /// The monitoring login lacks a grant this collector needs. Expected and benign for a
    /// least-privilege login — logged as PERMISSIONS and skipped, never as an error.
    /// SQL Server: 229, 297, 300, 8189. PostgreSQL: SQLSTATE 42501 <c>insufficient_privilege</c>.
    /// </summary>
    Permissions,

    /// <summary>
    /// A deliberate short lock timeout fired instead of the collector queueing behind a blocking
    /// chain. Evidence about the monitored server, not a monitoring failure, so it is excluded from
    /// error rates and health bands. SQL Server: 1222, and only for a collector that declares
    /// <see cref="ICollectorSchemaInfo.YieldsOnLockTimeout"/>. PostgreSQL: SQLSTATE 55P03
    /// <c>lock_not_available</c>.
    /// </summary>
    LockTimeoutYield,

    /// <summary>
    /// A server-side capture session the collector reads from is not there. SQL Server: an Extended
    /// Events session (297/15151). PostgreSQL has no direct equivalent today, but the concept
    /// generalizes to any absent server-side capture the collector expects.
    /// </summary>
    SessionMissing,

    /// <summary>
    /// The object or function being read does not exist on this target — the normal answer when a
    /// feature is version-gated or an extension was never created. Distinct from
    /// <see cref="Permissions"/> because the remedy differs: install or upgrade, versus grant.
    /// PostgreSQL: SQLSTATE 42P01 <c>undefined_table</c> / 42883 <c>undefined_function</c>.
    /// SQL Server: 208 <c>Invalid object name</c>.
    /// </summary>
    ObjectMissing,

    /// <summary>
    /// The feature exists but is switched off on this target, so it returns an error rather than an
    /// empty set. Aurora does this for <c>aurora_ccm_status()</c> when the cluster cache manager is
    /// disabled, and for Optimized Reads statistics when the feature is off — both raise rather than
    /// return zero rows, which a naive collector records as a failure every cycle.
    /// </summary>
    FeatureDisabled,

    /// <summary>
    /// The command exceeded its timeout. SQL Server surfaces this as error number -2. PostgreSQL:
    /// SQLSTATE 57014 <c>query_canceled</c>, which is also what a <c>statement_timeout</c> produces.
    /// </summary>
    CommandTimeout,

    /// <summary>
    /// The connection itself failed or died. Forces a reconnect and re-probe rather than just
    /// failing the one collector. SQL Server: severity class 20 and above. PostgreSQL: SQLSTATE
    /// class 08 <c>connection_exception</c>, and 57P01 <c>admin_shutdown</c> / 57P02
    /// <c>crash_shutdown</c>.
    /// </summary>
    ConnectionFatal,
}
