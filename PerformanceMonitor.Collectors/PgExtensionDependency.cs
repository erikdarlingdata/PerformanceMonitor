/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

namespace PerformanceMonitor.Collectors;

/// <summary>
/// How an extension a collector depends on gets onto a monitored server. The distinction is the one an
/// operator has to plan around: one of these is a statement, the other is an outage.
/// </summary>
public enum PgExtensionInstallKind
{
    /// <summary>
    /// A <c>CREATE EXTENSION</c> in the database, and nothing else. No restart, no parameter group, no
    /// downtime — the collector starts working on its next cycle.
    /// </summary>
    CreateExtension,

    /// <summary>
    /// The module has to be listed in <c>shared_preload_libraries</c>, which takes a server restart — a
    /// parameter-group change plus a reboot on Aurora and RDS. Until that happens the extension is inert
    /// whatever else has been installed.
    ///
    /// <para>This says the restart is REQUIRED. It deliberately says nothing about whether a
    /// <c>CREATE EXTENSION</c> is also required, because the two are not alternatives — for most preloaded
    /// modules both are needed — and the restart is the half that has to be scheduled.</para>
    /// </summary>
    SharedPreloadLibraries,
}

/// <summary>
/// One PostgreSQL extension a collector cannot function without, declared by the collector that needs it
/// (<see cref="ICollectorSchemaInfo.RequiredPgExtensions"/>).
///
/// <para><b>Why this is not <c>PgExtensionAvailabilityCollector</c>'s roster.</b> That roster answers a
/// different question and its own comment says so: it exists ONLY so that ABSENCE is reportable, which is
/// why it carries <c>hypopg</c>, <c>pg_trgm</c> and <c>pg_cron</c> — extensions worth telling an operator
/// about that no collector reads — and why it deliberately omits <c>pg_wait_sampling</c>, which a collector
/// does read. Reportable-absence and collector-dependency are overlapping sets, not the same set, so a
/// consumer that needs one and reads the other is wrong in both directions at once.</para>
///
/// <para><b>What is deliberately NOT here.</b> Whether the extension has to be created in EVERY database
/// rather than just the connect database, because that is already derivable:
/// <see cref="ICollectorDefinition{TRow}.RunsPerDatabase"/> is what makes it per-database, and a second
/// declaration of the same fact is the copy that drifts.</para>
///
/// <para><b>One entry per extension whose OBJECTS the collector's own query reads.</b> An extension that
/// merely underpins a declared one — <c>pg_stat_kcache</c> is built on <c>pg_stat_statements</c> — is a
/// property of that extension rather than of this collector, and declaring it here would make every
/// consumer re-derive which of two names was the one to install.</para>
/// </summary>
/// <param name="ExtensionName">The extension's name as PostgreSQL knows it, lowercase, exactly as it would
/// appear in <c>CREATE EXTENSION</c> or <c>shared_preload_libraries</c>.</param>
/// <param name="InstallKind">What installing it costs.</param>
public readonly record struct PgExtensionDependency(
    string ExtensionName,
    PgExtensionInstallKind InstallKind);
