/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

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
    PgExtensionInstallKind InstallKind)
{
    /// <summary>
    /// Objects of this extension the collector's query reads BESIDE its base object, each of which exists
    /// only from some extension version on (#3818). Empty for most declarations.
    ///
    /// <para><b>Why the fault mapping needs this.</b> A collector that declares an extension has its
    /// missing-object fault (42P01 / 42883) recorded as <c>EXTENSION_MISSING</c>, with a sentence saying
    /// the extension is not installed and <c>CREATE EXTENSION</c> is the remedy (#3240). That inference
    /// holds when the object the server could not find is the extension's BASE object. It is false when
    /// the missing object is a companion the extension only gains at a later version: on 23 of 50
    /// clusters in one upgraded fleet <c>pg_stat_statements</c> was installed, preloaded and readable, at
    /// catalog version 1.8, and the collector's read of <c>pg_stat_statements_info</c> (created by the
    /// 1.9 update script) failed 42P01 every cycle - recorded as "the pg_stat_statements extension is not
    /// installed", a sentence that sent anyone reading it to check an extension that was there, while the
    /// remedy was <c>ALTER EXTENSION pg_stat_statements UPDATE</c>. Naming the companions here is what
    /// lets the mapping tell the two apart and say the true thing.</para>
    ///
    /// <para><b>The property a declarer must hold.</b> The inference "the base object was found" rests on
    /// PostgreSQL resolving names in order and reporting the FIRST one it cannot: a query that names its
    /// base object in <c>FROM</c> and reads the companion inside the select list (a scalar subquery, as
    /// <c>pg_statement_stats</c> does) has its FROM resolved first, so an error naming the companion means
    /// the base resolved. A collector that reads a companion in a position resolved BEFORE its base object
    /// would make the sentence claim more than the server said, and must not declare it here.</para>
    ///
    /// <para>Declared by bare object name, without a schema: the server's message qualifies the name
    /// exactly as the query text did, and the mapping compares the last segment.</para>
    /// </summary>
    public IReadOnlyList<PgExtensionCompanionObject> Companions { get; init; } = Array.Empty<PgExtensionCompanionObject>();
}

/// <summary>
/// One object an extension gains at a particular catalog version, which a collector reads beside the
/// extension's base object (<see cref="PgExtensionDependency.Companions"/>, #3818). Its absence on a server
/// where the base object resolved means the extension is PRESENT at a version below
/// <paramref name="SinceExtensionVersion"/>, and the remedy is <c>ALTER EXTENSION ... UPDATE</c> - a
/// statement, with no restart and no <c>shared_preload_libraries</c> change - never <c>CREATE EXTENSION</c>.
/// </summary>
/// <param name="ObjectName">The bare relation or function name, lowercase, no schema.</param>
/// <param name="SinceExtensionVersion">The extension catalog version whose update script creates it, as
/// <c>pg_extension.extversion</c> spells it (<c>1.9</c>).</param>
public readonly record struct PgExtensionCompanionObject(
    string ObjectName,
    string SinceExtensionVersion);
