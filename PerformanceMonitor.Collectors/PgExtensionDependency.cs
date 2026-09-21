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
    /// the missing object is a companion the extension only gains at a later version: the collector's read
    /// of <c>pg_stat_statements_info</c> (created by the 1.9 update script) failed 42P01 every cycle on 23
    /// of 50 clusters in one fleet and was recorded as "the pg_stat_statements extension is not installed",
    /// which sent anyone reading it to <c>CREATE EXTENSION</c> plus a preload restart. Naming the companions
    /// here is what lets the mapping tell a companion apart from a base object at all.</para>
    ///
    /// <para><b>Which state that fleet was in is a SEPARATE question, and #3818 answered it wrong</b>
    /// (#3830). Its sentence inferred "present below 1.9" from the base object having resolved; the
    /// store-side read of <c>pg_extension_availability</c> then showed <c>installed_version</c> EMPTY in
    /// every database on all 23 - the extension had never been created anywhere, and the collector had been
    /// productive throughout through Aurora's <c>aurora_stat_statements()</c>, which needs none. Below-1.9
    /// is a real state with a real remedy; it was not theirs. That is why the remedy now comes from
    /// <see cref="PgExtensionRowObservation"/> and not from the inference.</para>
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

    /// <summary>
    /// The Aurora-native source the collector reads INSTEAD of this extension's base object on an Aurora
    /// target, spelled as it would be called (<c>aurora_stat_statements()</c>), or null where the collector
    /// reads the extension on every flavor (#3830). Null for most declarations.
    ///
    /// <para><b>What declaring it changes.</b> Exactly one thing: the remedy an operator is given when
    /// <c>pg_extension</c> has NO row for the extension on an Aurora target. Without it the only sentence
    /// available is "create the extension", which reads as a collector that is dark until someone does -
    /// and on the fleet that motivated #3830 that was false in both directions: the collector had been
    /// productive for months through <c>aurora_stat_statements()</c>, which needs no extension, while the
    /// stored remedy told 23 clusters to UPDATE an extension none of them had. With it declared, the
    /// sentence can say the create is OPTIONAL and name what it actually buys - the companion.</para>
    ///
    /// <para>A declarer must hold the same property the name implies: on an Aurora target the collector's
    /// query does NOT read this extension's base object, so the extension's absence costs the companions
    /// and nothing else. A collector that reads the extension on Aurora too must leave this null, or the
    /// sentence calls a required install optional.</para>
    /// </summary>
    public string? AuroraNativeAlternative { get; init; }
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
    string SinceExtensionVersion)
{
    /// <summary>
    /// What a <see cref="PgExtensionRowObservation"/> says about this companion's availability, which is the
    /// fact a remedy sentence needs and cannot get from the exception (#3830). Four states, because three
    /// remedies and "no answer" are genuinely different things.
    /// </summary>
    /// <param name="observation">The pg_extension row as one read observed it.</param>
    public PgExtensionCompanionVerdict VerdictFrom(PgExtensionRowObservation observation)
    {
        if (!observation.Observed)
        {
            return PgExtensionCompanionVerdict.Undetermined;
        }

        if (observation.ExtensionVersion is null)
        {
            return PgExtensionCompanionVerdict.NoRow;
        }

        if (!TryCompareVersions(observation.ExtensionVersion, SinceExtensionVersion, out var comparison))
        {
            return PgExtensionCompanionVerdict.Undetermined;
        }

        return comparison < 0
            ? PgExtensionCompanionVerdict.BelowCompanionVersion
            : PgExtensionCompanionVerdict.AtOrAboveCompanionVersion;
    }

    /// <summary>
    /// Compares two <c>pg_extension.extversion</c> strings SEGMENT BY SEGMENT AS NUMBERS, and that is the
    /// whole point of its existing: <c>pg_stat_statements</c> shipped 1.9 and then 1.10, and an ordinal
    /// string comparison puts 1.10 BELOW 1.9. A fleet on 1.10 would be told to run an update it ran two
    /// releases ago, which is the same wrong-remedy defect one version earlier.
    ///
    /// <para>False - never a guess - when either side is not a dot-separated run of non-negative integers.
    /// <c>extversion</c> is free text the extension's author chooses, so a version this cannot rank is a
    /// real possibility, and a caller that gets false keeps the answer it would have had before the
    /// observation existed rather than ranking it by a rule nobody checked.</para>
    /// </summary>
    public static bool TryCompareVersions(string? left, string? right, out int comparison)
    {
        comparison = 0;

        if (!TryParseVersion(left, out var leftParts) || !TryParseVersion(right, out var rightParts))
        {
            return false;
        }

        var segments = Math.Max(leftParts.Length, rightParts.Length);
        for (var i = 0; i < segments; i++)
        {
            /* A missing trailing segment is zero: 1.9 and 1.9.0 are the same version. */
            var leftPart = i < leftParts.Length ? leftParts[i] : 0;
            var rightPart = i < rightParts.Length ? rightParts[i] : 0;

            if (leftPart != rightPart)
            {
                comparison = leftPart < rightPart ? -1 : 1;
                return true;
            }
        }

        return true;
    }

    private static bool TryParseVersion(string? version, out int[] parts)
    {
        parts = Array.Empty<int>();

        if (string.IsNullOrWhiteSpace(version))
        {
            return false;
        }

        var segments = version.Split('.');
        var parsed = new int[segments.Length];

        for (var i = 0; i < segments.Length; i++)
        {
            if (!int.TryParse(
                    segments[i],
                    System.Globalization.NumberStyles.None,
                    System.Globalization.CultureInfo.InvariantCulture,
                    out parsed[i]))
            {
                return false;
            }
        }

        parts = parsed;
        return true;
    }
}

/// <summary>
/// What one read of <c>pg_extension</c> saw for an extension, in the database it ran in (#3830) - the ROW,
/// never a conclusion drawn from it. <see cref="PgExtensionCompanionObject.VerdictFrom"/> draws the
/// conclusion fresh, which is the division <c>CollectorMeasurement</c> states for the same reason: a stored
/// verdict is stale the moment an operator acts on it, a stored observation stays true.
///
/// <para><b>The default is NOT OBSERVED, and that is load-bearing.</b> A hand-built target shape, a SQL
/// Server target, a host that never ran the read - all of them produce <c>default</c>, and every consumer
/// must treat that as "no answer" rather than as "no row". The two are opposite remedies.</para>
///
/// <para><b><see cref="Database"/> is carried because <c>pg_extension</c> is per database.</b> A fault
/// raised in a different database than the read ran in says nothing about that database's catalog, so a
/// consumer compares the two names before using the row - see the mapping in <c>DarlingWorker</c>.</para>
/// </summary>
/// <param name="Observed">True when the read actually ran and returned an answer. False is the default and
/// means nothing is known, which is not the same as the extension being absent.</param>
/// <param name="ExtensionVersion">The <c>extversion</c> the row carried, or null when <c>pg_extension</c>
/// had NO row for the extension - which is exactly what the read's scalar returns in that case.</param>
/// <param name="Database">The database the read ran in (<c>current_database()</c>), or null when the host
/// did not record it.</param>
public readonly record struct PgExtensionRowObservation(
    bool Observed,
    string? ExtensionVersion,
    string? Database)
{
    /// <summary>Nothing was read: the default, and the state every consumer must treat as "no answer".</summary>
    public static PgExtensionRowObservation NotObserved => default;

    /// <summary>
    /// The result of a read that RAN: <paramref name="extensionVersion"/> null means <c>pg_extension</c> had
    /// no row for the extension. A blank string is folded to null, because a scalar that came back empty is
    /// not a version.
    /// </summary>
    public static PgExtensionRowObservation From(string? extensionVersion, string? database) =>
        new(true, string.IsNullOrWhiteSpace(extensionVersion) ? null : extensionVersion, database);

    /// <summary>
    /// This observation if it was read in <paramref name="database"/>, and
    /// <see cref="NotObserved"/> otherwise - the per-database guard stated as a method so a consumer cannot
    /// use a row from the wrong database by forgetting to check. An unknown database on either side is not a
    /// match: <c>pg_extension</c> is per database, and "probably the same one" is how the wrong catalog gets
    /// quoted at an operator.
    /// </summary>
    public PgExtensionRowObservation InDatabase(string? database) =>
        Observed
        && !string.IsNullOrWhiteSpace(Database)
        && string.Equals(Database, database, StringComparison.Ordinal)
            ? this
            : NotObserved;
}

/// <summary>
/// Whether a declared companion object (<see cref="PgExtensionDependency.Companions"/>) can exist on the
/// server the fault came from, as <see cref="PgExtensionCompanionObject.VerdictFrom"/> reads the
/// <c>pg_extension</c> row (#3830). Each state carries a DIFFERENT remedy, and #3830 is the record of what
/// shipping one remedy for all of them cost: the 23 clusters that motivated #3818 were the
/// <see cref="NoRow"/> case, and they were told to run <c>ALTER EXTENSION ... UPDATE</c> on an extension
/// that had never been created.
/// </summary>
public enum PgExtensionCompanionVerdict
{
    /// <summary>
    /// No read has answered, or it answered a version nothing can rank. The default, so a consumer that is
    /// handed nothing says what it can prove from the fault alone and claims no catalog state.
    /// </summary>
    Undetermined,

    /// <summary>
    /// <c>pg_extension</c> has NO row for the extension in that database: it was never created there, so
    /// there is nothing to update and <c>ALTER EXTENSION</c> would raise. A collector can still be
    /// productive here - Aurora's <c>aurora_stat_statements()</c> needs no extension at all.
    /// </summary>
    NoRow,

    /// <summary>
    /// The extension IS created, at a catalog version below the one whose update script creates the
    /// companion. This is the case <c>ALTER EXTENSION ... UPDATE</c> is the remedy for.
    /// </summary>
    BelowCompanionVersion,

    /// <summary>
    /// The extension is created at or above the companion's version, so the companion should exist and its
    /// absence is not a version problem - the schema it was created in, or something the error text names.
    /// </summary>
    AtOrAboveCompanionVersion,
}
