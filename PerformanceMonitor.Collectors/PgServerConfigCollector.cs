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
/// <para><b><c>pg_settings.pending_restart</c> is backend-local, and that is #4251.</b> PostgreSQL flips it
/// true only on the connection that was open when <c>pg_reload_conf()</c> ran; a NEW connection — including
/// the collector's own on its next cycle — reads <c>false</c> for a setting that is genuinely still pending a
/// restart, so this column alone silently loses the fact it exists to record. <c>pg_file_settings</c> names
/// the same fact from the FILE rather than backend state: a row whose <c>error</c> is
/// <c>'setting could not be applied'</c> for a setting whose <c>pg_settings.context</c> is
/// <c>postmaster</c> is pending a restart regardless of which connection asks (the same error text also
/// marks an outright rejected value at a non-<c>postmaster</c> context — PostgreSQL <c>guc.c</c> — which is
/// why <c>context</c> is the discriminator, not the error text alone). Reading <c>pg_file_settings</c>
/// needs a grant a least-privilege monitoring role usually lacks, so <see cref="PgFileSettingsCapability"/>
/// decides — once an hour, never inline — whether <see cref="BuildQuery"/> sends the query below that folds
/// it in or the query that does not; either way this stays the ONE place <c>pending_restart</c> is computed,
/// and every reader is unchanged.</para>
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

    /// <summary>The <see cref="PgSettingRedactor.Redact"/> timeout callback for every value read here
    /// (#4348): a bounded match time can time out on a pathological value, so the whole value is masked and
    /// only the setting's NAME is traced — never the value or any fragment of it.</summary>
    private static void LogRedactorTimeout(string? name) =>
        System.Diagnostics.Trace.TraceWarning($"#4348 PgSettingRedactor timed out matching setting '{name}'; the value was masked whole.");

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

    /* #4251: byte-for-byte QueryText, except the pg_settings arm's pending_restart column also looks at
       pg_file_settings. Only that ONE line differs - duplicated rather than templated because the two are
       sent as two distinct query texts chosen before either runs (PgFileSettingsCapability.IsReadableAsync,
       resolved by the host BEFORE BuildQuery decides), the same shape PgServerLogTail's text/binary routes
       use, and for the same reason: PostgreSQL checks a relation's permission before the plan runs, so a
       CASE or subquery that only reaches pg_file_settings on some rows still throws for a role with no grant
       on it, and this collector must never lose the whole pg_settings snapshot to that.

       The EXISTS matches a pg_file_settings row for a postmaster-context setting (s.context = 'postmaster'):
       either by name, case-insensitively since ParseConfigFp stores the name as written and a hand-edited
       "Shared_Buffers = ..." would otherwise miss it, with error 'setting could not be applied'; or, for the
       row PostgreSQL records with no name when a postmaster-context setting leaves every config file (ALTER
       SYSTEM RESET, or a deleted line), by matching that row's exact error text, since pfs.name is empty on
       it and there is nothing to correlate by name. Neither error string is translated -
       pstrdup("setting could not be applied") and the psprintf building the second, guc-file.l:14 and :18 -
       so lc_messages never changes either one.

       s.context = 'postmaster' does NOT separate "pending" from "rejected": pg_file_settings has no column
       for that, so 'setting could not be applied' is set both for a postmaster-context value truly pending a
       restart and for one PostgreSQL rejected outright (bad syntax, out of range), which a restart would only
       fail to start on. get_pg_server_config's reading guide for pending_restart says so. The override arm
       (pg_db_role_setting) is untouched: an override has no file line and cannot be pending a restart, per its
       own comment above. */
    private const string QueryTextWithFileSettings = @"
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
    (s.pending_restart
        OR (s.context = 'postmaster'
            AND EXISTS (
                SELECT 1
                FROM pg_catalog.pg_file_settings AS pfs
                WHERE (lower(pfs.name) = lower(s.name)
                       AND pfs.error = 'setting could not be applied')
                OR    pfs.error = 'parameter ""' || s.name
                                  || '"" cannot be changed without restarting the server'
            )))                             AS pending_restart,
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

    /// <summary>
    /// #4251: <see cref="CollectorContext.PgFileSettingsReadable"/> — resolved by the host through
    /// <see cref="PgFileSettingsCapability"/> BEFORE this runs — picks which of the two byte-identical (bar
    /// one column) query texts is sent. False, the default every host that never resolves it leaves this at,
    /// keeps today's query: a target whose monitoring role cannot read <c>pg_file_settings</c> collects
    /// exactly what it always has.
    /// </summary>
    public override CollectorQuery BuildQuery(CollectorContext context) =>
        new(context.PgFileSettingsReadable ? QueryTextWithFileSettings : QueryText);

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
               shaped, so it is the only place that needs to change. A pattern that times out on this value
               masks it whole and traces the setting's NAME only, never the value. */
            rows.Add(new Row(
                Name: name,
                Setting: PgSettingRedactor.Redact(name, reader.IsDBNull(1) ? null : reader.GetString(1), LogRedactorTimeout),
                Unit: reader.IsDBNull(2) ? null : reader.GetString(2),
                Category: reader.IsDBNull(3) ? null : reader.GetString(3),
                Context: reader.IsDBNull(4) ? null : reader.GetString(4),
                VarType: reader.IsDBNull(5) ? null : reader.GetString(5),
                Source: reader.IsDBNull(6) ? null : reader.GetString(6),
                BootValue: PgSettingRedactor.Redact(name, reader.IsDBNull(7) ? null : reader.GetString(7), LogRedactorTimeout),
                ResetValue: PgSettingRedactor.Redact(name, reader.IsDBNull(8) ? null : reader.GetString(8), LogRedactorTimeout),
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
