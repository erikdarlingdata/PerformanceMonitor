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
/// What KIND of engine a monitored server is, as the store records it — the discriminator
/// <c>collect.servers.engine_kind</c> holds, and the one vocabulary every reader of that column decodes
/// through (#2530).
///
/// <para><b>Why the store needed a column at all.</b> The registry recorded
/// <c>SERVERPROPERTY('EngineEdition')</c> and the SQL major version, and a PostgreSQL target lands with
/// edition <c>0</c> because <c>SERVERPROPERTY</c> does not exist there. So does a SQL Server that has never
/// completed a connect. Those two are not the same fact and no amount of reading the existing columns can
/// separate them, which is why <see cref="CollectorEngineCapability.UnknownEngineEdition"/> deliberately
/// makes no claim: while nothing distinguished them, "we do not know" was the only honest answer. This
/// column is what makes "known to be PostgreSQL" expressible, and it is deliberately NOT the same statement
/// as "not known to be SQL Server".</para>
///
/// <para><b>Why a token rather than a boolean.</b> <c>is_postgres</c> would have been smaller and would
/// have been wrong within one rung: Aurora versus stock PostgreSQL is already a fact the collectors gate on
/// (<see cref="CollectorTargetInfo.IsAurora"/> — <c>aurora_stat_system_waits()</c> and friends are a large
/// proprietary surface core PostgreSQL has in no version), and a boolean cannot grow a third value. A token
/// can, and the growth is additive: a new engine or flavour appends a constant here, and every reader that
/// asks <see cref="IsPostgres"/> or <see cref="IsSqlServer"/> rather than comparing strings keeps working.
/// The column is nullable for the same reason the enum has no <c>Unknown</c> member — absence IS the
/// unknown, so there is no second spelling of it to get wrong.</para>
///
/// <para><b>Why not read <c>config.config_monitored_servers.engine</c>, which already exists (V70).</b>
/// That is the DESIRED configuration; this is the OBSERVED registry, and the two diverge in ways that
/// matter here. Aurora-ness is not configured at all — it is probed from <c>aurora_version</c> in
/// <c>pg_proc</c>, so the config plane cannot carry the distinction this column exists for. And a server
/// deleted from the desired config keeps its observed row on purpose (#2030 flips it disabled rather than
/// deleting it, because that row anchors collected history), so a reader joining to the config plane would
/// watch a known-PostgreSQL card lose its engine the moment the operator stopped monitoring it.</para>
/// </summary>
public static class MonitoredEngineKind
{
    /// <summary>Microsoft SQL Server in any hosted shape — box, Azure SQL DB, Managed Instance, RDS. The
    /// hosting differences are carried by <c>sql_engine_edition</c>, not here; this axis is the DIALECT.</summary>
    public const string SqlServer = "sqlserver";

    /// <summary>PostgreSQL that is not Aurora — self-hosted, or RDS for PostgreSQL.</summary>
    public const string Postgres = "postgres";

    /// <summary>Amazon Aurora PostgreSQL. A separate token rather than a flag beside
    /// <see cref="Postgres"/> because the collectors already gate on it, and because a UI that greys the
    /// Aurora-only panels needs to know it from the fleet payload without a second round-trip.</summary>
    public const string AuroraPostgres = "aurora-postgres";

    /// <summary>Every token the store may hold, for the tests that hold the vocabulary to the writers and
    /// for anything that wants to enumerate rather than switch. NULL — "not known" — is deliberately absent:
    /// it is the absence of a token, not one of them.</summary>
    public static readonly IReadOnlyList<string> All = new[] { SqlServer, Postgres, AuroraPostgres };

    /// <summary>
    /// The token for a live target, from the facts the connector probed. Total: every
    /// <see cref="CollectorTargetInfo"/> has a kind, so the upsert never has to decide whether to write
    /// NULL — a NULL in the column can then only mean "no connect has completed since the rung landed",
    /// which is exactly what the readers treat it as.
    /// </summary>
    public static string For(CollectorTargetInfo target)
    {
        ArgumentNullException.ThrowIfNull(target);

        return target.Engine switch
        {
            /* Aurora-ness is a PostgreSQL fact only; the SQL Server side has no such flavour split (RDS for
               SQL Server runs the same T-SQL against the same DMVs, which is why it rides on
               CollectorTargetInfo.IsAwsRds and not on this axis). */
            CollectorTargetEngine.PostgreSql => target.IsAurora ? AuroraPostgres : Postgres,
            _ => SqlServer,
        };
    }

    /// <summary>
    /// The dialect a token implies, or <c>null</c> when the token is absent or unrecognised. <c>null</c> is
    /// the "make no claim" answer and every caller must treat it that way — an unknown token is a store
    /// written by a NEWER build than this one, which is a thing to be quiet about rather than to guess at.
    /// </summary>
    public static CollectorTargetEngine? EngineOf(string? engineKind) => Normalize(engineKind) switch
    {
        SqlServer => CollectorTargetEngine.SqlServer,
        Postgres or AuroraPostgres => CollectorTargetEngine.PostgreSql,
        _ => null,
    };

    /// <summary>True only when the store says this target IS PostgreSQL. An absent or unrecognised token is
    /// false — "not known to be PostgreSQL", which is not the same claim as "known to be SQL Server".</summary>
    public static bool IsPostgres(string? engineKind) => EngineOf(engineKind) == CollectorTargetEngine.PostgreSql;

    /// <summary>True only when the store says this target IS SQL Server. Same asymmetry as
    /// <see cref="IsPostgres"/>: absence is not evidence for the other side.</summary>
    public static bool IsSqlServer(string? engineKind) => EngineOf(engineKind) == CollectorTargetEngine.SqlServer;

    /// <summary>True when the store carries a token this build recognises — i.e. some claim can be made.</summary>
    public static bool IsKnown(string? engineKind) => EngineOf(engineKind) is not null;

    /// <summary>True when the store says this target is Amazon Aurora PostgreSQL specifically.</summary>
    public static bool IsAurora(string? engineKind) =>
        string.Equals(Normalize(engineKind), AuroraPostgres, StringComparison.Ordinal);

    /// <summary>
    /// How the token reads in a sentence an operator sees. The single copy, for the same reason
    /// <see cref="CollectorEngineCapability.DescribeEngineEdition"/> is: two description tables in one repo
    /// drift, and the one that drifts is never the one being read.
    /// </summary>
    public static string DescribeEngineKind(string? engineKind) => Normalize(engineKind) switch
    {
        SqlServer => "SQL Server",
        Postgres => "PostgreSQL",
        AuroraPostgres => "Aurora PostgreSQL",
        _ => "an unrecognised engine",
    };

    /// <summary>
    /// Trim + lowercase, so a hand-edited row or a differently-cased writer still decodes. Deliberately does
    /// NOT map unknown text onto a default: a token this build has never heard of stays unknown and makes no
    /// claim, which is what keeps a newer store from being described wrongly by an older reader.
    /// </summary>
    private static string? Normalize(string? engineKind) =>
        string.IsNullOrWhiteSpace(engineKind) ? null : engineKind.Trim().ToLowerInvariant();
}
