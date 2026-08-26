/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// The server's configuration, from <c>pg_settings</c> (#2658). SQL Server answers this three ways
/// (<c>get_server_config</c>, <c>get_server_config_changes</c>, <c>get_database_scoped_config</c>) and
/// PostgreSQL had no answer at all — nothing stored a setting, so "what is <c>work_mem</c> here" and
/// "what changed last Tuesday" were both unanswerable after the fact.
///
/// <para><b>The value alone would not have been worth a collector.</b> What makes this one earn its place is
/// the three columns beside it:</para>
///
/// <list type="bullet">
/// <item><b><c>source</c></b> separates server configuration from session noise. <c>pg_settings</c> is a
/// per-BACKEND view, so it reports what THIS connection sees: on the rig <c>application_name = psql</c> with
/// <c>source = client</c> sits in the same result as <c>auto_explain.log_min_duration</c> from
/// <c>command line</c>. Storing them undifferentiated would record the collector's own session as the
/// server's configuration, and every snapshot would then "change" whenever the collector reconnected.</item>
/// <item><b><c>boot_val</c> and <c>reset_val</c></b> give "differs from the default" without a hardcoded
/// table of defaults that would rot at every major. That is the difference between an answer and a dump:
/// 415 settings on the rig, 28 of them non-default.</item>
/// <item><b><c>pending_restart</c></b> is a production trap with no symptom. Someone edits
/// <c>postgresql.conf</c>, reloads, and the file now says one thing while the running server does another —
/// until a restart months later silently changes behaviour, usually during an unrelated incident. SQL Server
/// has no equivalent column; PostgreSQL hands it over for free and nothing was reading it.</item>
/// </list>
///
/// <para><b>Everything is stored, nothing is filtered here.</b> A <c>client</c>-source row is evidence about
/// the collector's own session and dropping it at collection time makes that unrecoverable; the READ decides
/// what counts as server configuration. The rule that matters is the one the read enforces: a session-scoped
/// row must never be presented as the server's setting.</para>
///
/// <para>Core catalog only, no extension, readable by any login — the same cheap tier as
/// <see cref="PgWraparoundStatsCollector"/>. <c>pg_settings</c> is cluster-wide, so one connection sees
/// everything and there is no per-database fan-out.</para>
/// </summary>
public sealed class PgServerConfigCollector : PostgresCollectorDefinitionBase<PgServerConfigCollector.Row>
{
    public static PgServerConfigCollector Instance { get; } = new();

    private PgServerConfigCollector()
    {
    }

    public readonly record struct Row(
        string Name,
        string? Setting,
        string? Unit,
        string? Category,
        string? Context,
        string? VarType,
        string? Source,
        string? BootValue,
        string? ResetValue,
        string? SourceFile,
        int SourceLine,
        bool PendingRestart,
        string? ShortDescription);

    /* pg_settings is a per-backend VIEW over the GUC table, not a shared catalog, so this reports what the
       collector's own connection sees. That is not a defect to work around — it is why `source` is stored.

       No ORDER BY on the read's behalf: the read sorts by what it is answering (non-default first), and a
       415-row cluster-wide result is small enough that sorting here would only be a second sort.

       sourceline is 0 rather than NULL when a setting did not come from a file, matching what PostgreSQL
       reports; the value is only meaningful alongside a non-null sourcefile. */
    private const string QueryText = @"
SELECT
    s.name                                  AS name,
    s.setting                               AS setting,
    s.unit                                  AS unit,
    s.category                              AS category,
    s.context                               AS context,
    s.vartype                               AS vartype,
    s.source                                AS source,
    s.boot_val                              AS boot_val,
    s.reset_val                             AS reset_val,
    s.sourcefile                            AS sourcefile,
    coalesce(s.sourceline, 0)               AS sourceline,
    s.pending_restart                       AS pending_restart,
    s.short_desc                            AS short_desc
FROM pg_catalog.pg_settings AS s";

    public override string Name => "pg_server_config";

    public override string TargetTable => "pg_server_config";

    /// <summary>Core catalog only — every PostgreSQL target, Aurora or not.</summary>
    public override bool AppliesTo(CollectorTargetInfo target) => true;

    public override CollectorQuery BuildQuery(CollectorContext context) => new(QueryText);

    public override IReadOnlyList<CollectorColumn> PayloadColumns { get; } = new[]
    {
        new CollectorColumn("name", CollectorColumnType.Varchar),
        new CollectorColumn("setting", CollectorColumnType.Varchar),
        new CollectorColumn("unit", CollectorColumnType.Varchar),
        new CollectorColumn("category", CollectorColumnType.Varchar),
        /* postmaster / sighup / superuser / user — whether changing this needs a RESTART, a reload, or
           nothing. Half of what an operator wants to know the moment they decide to change something. */
        new CollectorColumn("context", CollectorColumnType.Varchar),
        new CollectorColumn("vartype", CollectorColumnType.Varchar),
        /* The discriminator between a server setting and this connection's own state. Without it every
           read of this table is a guess about which rows are real. */
        new CollectorColumn("source", CollectorColumnType.Varchar),
        new CollectorColumn("boot_val", CollectorColumnType.Varchar),
        new CollectorColumn("reset_val", CollectorColumnType.Varchar),
        new CollectorColumn("sourcefile", CollectorColumnType.Varchar),
        new CollectorColumn("sourceline", CollectorColumnType.Integer),
        /* The file and the running server disagree, and nothing else says so. */
        new CollectorColumn("pending_restart", CollectorColumnType.Boolean),
        new CollectorColumn("short_desc", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new Row(
                Name: reader.GetString(0),
                Setting: reader.IsDBNull(1) ? null : reader.GetString(1),
                Unit: reader.IsDBNull(2) ? null : reader.GetString(2),
                Category: reader.IsDBNull(3) ? null : reader.GetString(3),
                Context: reader.IsDBNull(4) ? null : reader.GetString(4),
                VarType: reader.IsDBNull(5) ? null : reader.GetString(5),
                Source: reader.IsDBNull(6) ? null : reader.GetString(6),
                BootValue: reader.IsDBNull(7) ? null : reader.GetString(7),
                ResetValue: reader.IsDBNull(8) ? null : reader.GetString(8),
                SourceFile: reader.IsDBNull(9) ? null : reader.GetString(9),
                SourceLine: reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                PendingRestart: !reader.IsDBNull(11) && reader.GetBoolean(11),
                ShortDescription: reader.IsDBNull(12) ? null : reader.GetString(12)));
        }

        return rows;
    }

    public override void WritePayload(Row row, ICollectorRowWriter writer, CollectorContext context)
    {
        /* No deltas. A setting is a LEVEL, and the interesting derivative — that it changed — is a
           comparison between two snapshots of the level, which the changes read does by looking at
           consecutive rows. A delta calculator here would have nothing to subtract: these are strings. */
        writer
            .Value(row.Name)
            .Value(row.Setting)
            .Value(row.Unit)
            .Value(row.Category)
            .Value(row.Context)
            .Value(row.VarType)
            .Value(row.Source)
            .Value(row.BootValue)
            .Value(row.ResetValue)
            .Value(row.SourceFile)
            .Value(row.SourceLine)
            .Value(row.PendingRestart)
            .Value(row.ShortDescription);
    }
}
