/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// How a monitored server's VERSION reads to an operator, given the engine it runs — the single copy of
/// those words, for the same reason <see cref="MonitoredEngineKind.DescribeEngineKind"/> is (#3145).
///
/// <para><b>The defect this exists to make unrepresentable.</b> Two surfaces rendered a version by mapping
/// <c>collect.servers.sql_major_version</c> through a SQL-Server-only table whose fallback arm was
/// <c>$"SQL Server v{n}"</c>, without asking what engine the row described. A PostgreSQL target's
/// <c>sql_major_version</c> is <c>0</c> — <see cref="CollectorTargetInfo.SqlMajorVersion"/> is a
/// non-nullable <c>int</c> that the PostgreSQL connect path never assigns, and the registry upsert writes it
/// unguarded — so the fallback fired and the viewer's fleet sidebar labelled a PostgreSQL 18 server
/// "SQL Server v0". Not a missing version: the wrong ENGINE, stated as fact, on the one line an operator
/// reads to tell their fleet apart.</para>
///
/// <para><b>Two version vocabularies, two parameters, and no way to mix them.</b> The majors are separate
/// arguments rather than one number plus a discriminator because <c>17</c> is a real major in BOTH engines,
/// so a single-number signature lets a caller pass the wrong one and be silently believed —
/// <c>PostgresMajorVersionRegistryTests.TheLookupReadsOnlyThePostgresColumn</c> is the same rule enforced on
/// the store side, and this is it enforced on the render side. The SQL Server table is deliberately PRIVATE:
/// while an engine-blind entry point existed, every new render site could reach it, and two of the four that
/// did got it wrong. There is now no reachable way to render a version without saying which engine it
/// belongs to.</para>
///
/// <para><b>What is NOT changed here.</b> The <c>$"SQL Server v{n}"</c> fallback is kept: a major this build
/// has not been taught yet (SQL Server ships one every two years) is genuinely a SQL Server of unknown
/// vintage, and a bare version tag is the honest label for it. Deleting it to fix PostgreSQL would have
/// traded a wrong label on one engine for a missing one on the other.</para>
/// </summary>
public static class MonitoredEngineVersion
{
    /// <summary>
    /// The version label for a registry row, from its engine discriminator and the major belonging to that
    /// engine's vocabulary. Empty string means "no version to show" — the callers append it only when
    /// non-empty, so a row with nothing collected yet shows the name alone rather than a placeholder.
    ///
    /// <para>Four answers, and the order of the arms is the whole contract:</para>
    /// <list type="bullet">
    /// <item><b>Known PostgreSQL</b> (stock or Aurora) → <see cref="MonitoredEngineKind.DescribeEngineKind"/>'s
    /// words plus <paramref name="postgresMajorVersion"/> when the probe recorded one: "PostgreSQL 18",
    /// "Aurora PostgreSQL 16". A missing or <c>0</c> major degrades to the bare engine name rather than
    /// guessing — <c>0</c> is what a probe that failed before reading <c>server_version_num</c> leaves
    /// behind, and <c>collect.servers.postgres_major_version</c> (V100) is NULL for every target that has
    /// not reconnected since that rung, so "PostgreSQL" has to be a first-class answer.</item>
    /// <item><b>A token this build has never heard of</b> → the token back verbatim, with NO version. A store
    /// written by a NEWER build knows an engine this one does not, and neither major column can be trusted to
    /// belong to a vocabulary we cannot name. Same choice, and the same reasoning, as
    /// <c>FleetServerCard.EngineDescription</c>.</item>
    /// <item><b>Known SQL Server, or NO claim at all</b> → the SQL Server table. Absence falls here on
    /// purpose: it is the pre-#2530 behaviour and the only safe default for rows no connect has stamped since
    /// the engine-kind rung landed, exactly as <see cref="MonitoredEngineKind.IsPostgres"/>'s asymmetry
    /// requires. "Not known to be PostgreSQL" is not a claim of SQL Server, but it is the surface those rows
    /// already get.</item>
    /// </list>
    /// </summary>
    /// <param name="engineKind">The raw <c>collect.servers.engine_kind</c> token, or null when the store
    /// makes no claim.</param>
    /// <param name="sqlMajorVersion">The raw <c>collect.servers.sql_major_version</c>. Consulted only on the
    /// SQL Server arm, so its <c>0</c>-for-PostgreSQL value can no longer be read as a version.</param>
    /// <param name="postgresMajorVersion">The raw <c>collect.servers.postgres_major_version</c> (V100,
    /// #2653). Consulted only on the PostgreSQL arm.</param>
    public static string DescribeEngineVersion(string? engineKind, int? sqlMajorVersion, int? postgresMajorVersion)
    {
        if (MonitoredEngineKind.IsPostgres(engineKind))
        {
            var engine = MonitoredEngineKind.DescribeEngineKind(engineKind);

            return postgresMajorVersion is > 0
                ? engine + " " + postgresMajorVersion.Value.ToString(CultureInfo.InvariantCulture)
                : engine;
        }

        /* Present but unrecognised. Tested before the SQL Server arm because that arm is also where a NULL
           token lands, and these two absences are different facts: nothing stamped the row (keep today's
           behaviour) versus something newer than us stamped it (say what it said, claim nothing). */
        if (!string.IsNullOrWhiteSpace(engineKind) && !MonitoredEngineKind.IsKnown(engineKind))
        {
            return engineKind.Trim();
        }

        return DescribeSqlServerVersion(sqlMajorVersion);
    }

    /// <summary>
    /// Product-name label for a <c>sql_major_version</c>. Private by design — see the class remarks: an
    /// engine-blind version label is the defect, so there is no way to ask for one.
    ///
    /// <para><c>0</c> and below are empty rather than <c>"SQL Server v0"</c>. They are not versions: <c>0</c>
    /// is the unset value of a non-nullable field, which is what a target whose version probe has not run
    /// leaves in the column. Both viewer Add-server dialogs already hand-rolled <c>major == 0 ? null : major</c>
    /// at their call sites for exactly this reason; folding it in here means the two store-fed surfaces get
    /// the same treatment instead of being the only ones without it, and it keeps the worst case on any
    /// FUTURE caller that forgets the engine axis down to a missing label rather than a wrong engine.</para>
    /// </summary>
    private static string DescribeSqlServerVersion(int? sqlMajorVersion) => sqlMajorVersion switch
    {
        null => "",
        <= 0 => "",
        11 => "SQL Server 2012",
        12 => "SQL Server 2014",
        13 => "SQL Server 2016",
        14 => "SQL Server 2017",
        15 => "SQL Server 2019",
        16 => "SQL Server 2022",
        17 => "SQL Server 2025",
        _ => $"SQL Server v{sqlMajorVersion}",
    };
}
