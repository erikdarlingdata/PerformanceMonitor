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
///
/// <para><b>Per-database and per-role OVERRIDES ride the same table (#3691, V138).</b> <c>pg_settings</c> is
/// the RESOLVED view for THIS backend, and the collector connects to one database as one role — so a
/// cluster where one database carries <c>ALTER DATABASE … SET work_mem = '256MB'</c>, or one role carries
/// <c>ALTER ROLE … SET statement_timeout = 0</c>, stored nothing about it and every reader presented the
/// collector's own resolved value as "the server's configuration". The override lives in
/// <c>pg_db_role_setting</c>, a cluster-wide catalog nothing was reading, and the read below
/// <c>UNION ALL</c>s it onto the <c>pg_settings</c> rows. The two populations are told apart by the two
/// columns V138 appended: <c>database_name</c> and <c>role_name</c> are NULL on every <c>pg_settings</c>
/// row and carry one or both on an override row. Every shipped reader of this table filters the overrides
/// OUT (<c>AND database_name IS NULL AND role_name IS NULL</c>) because each is a latest-per-setting-name
/// shape a duplicate name would shadow; the one read that asks for them is
/// <c>get_pg_server_config</c>'s <c>database_overrides</c> section.</para>
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
        string? ShortDescription,
        /* NULL on a pg_settings row, the database's name on a per-database override (#3691). Both NULL is
           the server-wide population, which is what this collector stored exclusively before V138. */
        string? DatabaseName,
        string? RoleName);

    /* pg_settings is a per-backend VIEW over the GUC table, not a shared catalog, so this reports what the
       collector's own connection sees. That is not a defect to work around — it is why `source` is stored.

       No ORDER BY on the read's behalf: the read sorts by what it is answering (non-default first), and a
       415-row cluster-wide result is small enough that sorting here would only be a second sort.

       sourceline is 0 rather than NULL when a setting did not come from a file, matching what PostgreSQL
       reports; the value is only meaningful alongside a non-null sourcefile.

       The second arm (#3691, V138) is pg_db_role_setting — the per-database and per-role overrides, which
       pg_settings cannot report because it is already RESOLVED for this backend. Ten of the fifteen columns
       are NULL there and that is not laziness: an override row is not a pg_settings row. The catalog stores
       a name and a text value and nothing else, so unit, category, context, vartype, boot_val, reset_val,
       sourcefile and short_desc have no value to carry (a reader that wants the unit or the default reads
       them off the server-wide row for the same name, which is always present). sourceline is 0 and
       pending_restart false for the same reason the pg_settings arm coalesces them: the payload columns are
       NOT NULL-shaped in the reader, and an override has no file line and cannot be pending a restart -
       ALTER DATABASE/ROLE SET takes effect at the next connect to that database or as that role.

       source carries which KIND of override it is. The spellings ('database', 'role', 'database+role') are
       this collector's, not PostgreSQL's - pg_settings would say 'database', 'user' and 'database user' for
       the resolved forms, and 'database' therefore COLLIDES with a legitimate pg_settings source value on a
       backend whose own database overrides the setting. That collision is why source is not the
       discriminator between the two populations and the two new columns are: a reader asks
       database_name IS NULL AND role_name IS NULL, never a predicate on source.

       setconfig is text[] of 'name=value'. unnest expands it; split_part before the FIRST '=' is the GUC
       name and substr after it is the value, so a value that itself contains '=' (a search_path with a
       quoted element, a connection-ish string) survives intact. A row whose setconfig is NULL - possible
       after every setting on it is RESET - yields zero rows from unnest and the CROSS JOIN LATERAL drops
       it, which is right: it has nothing to say.

       LEFT JOIN, not JOIN, on both catalogs: setdatabase = 0 means ALL databases and setrole = 0 means ALL
       roles, neither matches an oid, and the join's NULL is exactly the NULL this table stores for
       ""not scoped to one"". pg_db_role_setting is a core catalog readable by any login on both flavours,
       Aurora included (Aurora hides no catalog here), so the read is ungated like the rest of this file. */
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
    s.short_desc                            AS short_desc,
    NULL::text                              AS database_name,
    NULL::text                              AS role_name
FROM pg_catalog.pg_settings AS s
UNION ALL
SELECT
    o.name                                  AS name,
    o.setting                               AS setting,
    NULL::text                              AS unit,
    NULL::text                              AS category,
    NULL::text                              AS context,
    NULL::text                              AS vartype,
    CASE
        WHEN drs.setdatabase <> 0 AND drs.setrole <> 0 THEN 'database+role'
        WHEN drs.setrole <> 0                          THEN 'role'
        ELSE                                                'database'
    END                                     AS source,
    NULL::text                              AS boot_val,
    NULL::text                              AS reset_val,
    NULL::text                              AS sourcefile,
    0                                       AS sourceline,
    false                                   AS pending_restart,
    NULL::text                              AS short_desc,
    d.datname                               AS database_name,
    r.rolname                               AS role_name
FROM pg_catalog.pg_db_role_setting AS drs
LEFT JOIN pg_catalog.pg_database AS d
  ON d.oid = drs.setdatabase
LEFT JOIN pg_catalog.pg_roles AS r
  ON r.oid = drs.setrole
CROSS JOIN LATERAL unnest(drs.setconfig) AS cfg(kv)
CROSS JOIN LATERAL (
    SELECT
        split_part(cfg.kv, '=', 1)                  AS name,
        substr(cfg.kv, strpos(cfg.kv, '=') + 1)     AS setting
) AS o";

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
        /* #3691 (V138), appended LAST so the positional COPY writer and an upgraded store's ALTER agree on
           where they sit. NULL on a pg_settings row - the server-wide population, every row before V138 -
           and the scope's name on an override row out of pg_db_role_setting. Together they are the ONE
           discriminator between the two populations: source cannot be, because 'database' is also a
           legitimate pg_settings source value. */
        new CollectorColumn("database_name", CollectorColumnType.Varchar),
        new CollectorColumn("role_name", CollectorColumnType.Varchar),
    };

    public override async ValueTask<List<Row>> ReadAsync(DbDataReader reader, CollectorContext context, CancellationToken cancellationToken)
    {
        var rows = new List<Row>();

        while (await reader.ReadAsync(cancellationToken))
        {
            var name = reader.GetString(0);

            /* #4348: pg_monitor includes pg_read_all_settings, so this connection sees GUC_SUPERUSER_ONLY
               settings too — primary_conninfo on a standby carries a replication password in plain text.
               Every value that can hold a secret is redacted here, before a Row is ever built, in BOTH
               arms of the UNION ALL (they share this one read loop): setting, boot_val and reset_val. The
               query text and every other column are untouched — this is the only place a stored row is
               shaped, so it is the only place that needs to change. */
            rows.Add(new Row(
                Name: name,
                Setting: PgSettingRedactor.Redact(name, reader.IsDBNull(1) ? null : reader.GetString(1)),
                Unit: reader.IsDBNull(2) ? null : reader.GetString(2),
                Category: reader.IsDBNull(3) ? null : reader.GetString(3),
                Context: reader.IsDBNull(4) ? null : reader.GetString(4),
                VarType: reader.IsDBNull(5) ? null : reader.GetString(5),
                Source: reader.IsDBNull(6) ? null : reader.GetString(6),
                BootValue: PgSettingRedactor.Redact(name, reader.IsDBNull(7) ? null : reader.GetString(7)),
                ResetValue: PgSettingRedactor.Redact(name, reader.IsDBNull(8) ? null : reader.GetString(8)),
                SourceFile: reader.IsDBNull(9) ? null : reader.GetString(9),
                SourceLine: reader.IsDBNull(10) ? 0 : reader.GetInt32(10),
                PendingRestart: !reader.IsDBNull(11) && reader.GetBoolean(11),
                ShortDescription: reader.IsDBNull(12) ? null : reader.GetString(12),
                DatabaseName: reader.IsDBNull(13) ? null : reader.GetString(13),
                RoleName: reader.IsDBNull(14) ? null : reader.GetString(14)));
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
            .Value(row.ShortDescription)
            .Value(row.DatabaseName)
            .Value(row.RoleName);
    }
}
