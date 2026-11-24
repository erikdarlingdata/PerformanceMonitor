/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// <c>collect.store_job_runs</c> (#9440): the one door through which the store's read roles see TimescaleDB's per-run job history.
///
/// <para><b>Why a function.</b> <c>timescaledb_information.job_history</c> shows a row only to a member of the database owner's role or of
/// the job's owner role, and the <c>mcp</c> and <c>viewer</c> logins are neither, so read as themselves they see no run at all. The function
/// runs as the store owner (SECURITY DEFINER), so the rows come back. A run row is not secret, so every reader gets it. The error text can
/// name objects and statements, so it comes back only when the CALLER could have read it through the view: the same two tests the view applies,
/// run for <c>session_user</c>, and for the role the login switched to (<c>SET ROLE</c>, or <c>-c role=</c> in the connection) when it did. <c>error_readable</c> says which rows the text was withheld from. This is the rule #9428 uses for
/// <c>last_error_readable</c>.</para>
///
/// <para><b>Created at service start, not in a versioned migration.</b> Like the statement statistics readers (#3899), it is created after
/// provisioning, so the roles it is granted to exist on a fresh store (a migration runs before them), and the hourly pass heals a dropped
/// function or grant. <c>CREATE OR REPLACE</c> is idempotent. The body is PL/pgSQL on purpose: it names <c>timescaledb_information</c>
/// views that a store without TimescaleDB, or one older than 2.15, does not have, and PL/pgSQL resolves them only when the function runs,
/// so the function can always be created. It returns no rows when the history view is missing, and the tool never calls it then.</para>
///
/// <para><b>Hygiene.</b> The search path is pinned to <c>pg_catalog, pg_temp</c> and every object is schema-qualified. EXECUTE is revoked from
/// PUBLIC and granted to the read roles that exist. The function is STABLE and the row count is capped inside it by <see cref="MaxRows"/>.</para>
/// </summary>
public static class StoreJobRunsFunction
{
    public const string FunctionName = "store_job_runs";

    /// <summary>The argument types, spelled once so the grant, the probe and the call cannot name different functions.</summary>
    public const string ArgumentTypes = "timestamp with time zone, timestamp with time zone, text, text, integer";

    /// <summary>The most rows the function returns, whatever limit it is asked for. The MCP read asks for its page plus one, and its page is at most 1,000.</summary>
    public const int MaxRows = 1001;

    /// <summary>The columns, in order, the function returns.</summary>
    public const string SelectList =
        "id, job_id, proc_schema, proc_name, hypertable_name, start_time, finish_time, succeeded, sqlerrcode, error_readable, err_message, oldest_row_at";

    public static string BuildFunctionSql(string collectSchema)
    {
        var collect = QuoteIdentifier(collectSchema);
        var body = $@"
#variable_conflict use_column
DECLARE
    v_limit integer := LEAST(GREATEST(COALESCE(p_limit, 50), 1), {MaxRows});
    v_db_owner oid := (SELECT d.datdba FROM pg_catalog.pg_database AS d WHERE d.datname = pg_catalog.current_database());
BEGIN
    IF pg_catalog.to_regclass('timescaledb_information.job_history') IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT
        h.id::bigint,
        h.job_id::integer,
        h.proc_schema::text,
        h.proc_name::text,
        j.hypertable_name::text,
        h.start_time,
        h.finish_time,
        h.succeeded,
        h.sqlerrcode::text,
        r.readable,
        CASE WHEN r.readable THEN h.err_message::text END,
        (SELECT pg_catalog.min(a.start_time) FROM timescaledb_information.job_history AS a)
    FROM timescaledb_information.job_history AS h
    LEFT JOIN timescaledb_information.jobs AS j
      ON j.job_id = h.job_id
    CROSS JOIN LATERAL
    (
        SELECT
            (
                COALESCE(pg_catalog.pg_has_role(session_user, j.owner, 'MEMBER'), false)
                OR COALESCE(pg_catalog.pg_has_role(session_user, v_db_owner, 'MEMBER'), false)
            )
            AND
            (
                pg_catalog.current_setting('role') = 'none'
                OR COALESCE(pg_catalog.pg_has_role(pg_catalog.current_setting('role')::name, j.owner, 'MEMBER'), false)
                OR COALESCE(pg_catalog.pg_has_role(pg_catalog.current_setting('role')::name, v_db_owner, 'MEMBER'), false)
            ) AS readable
    ) AS r
    WHERE h.start_time >= p_from
    AND   h.start_time < p_to
    AND   (p_job_kind IS NULL OR h.proc_name::text = p_job_kind OR h.proc_name::text = 'policy_' || p_job_kind)
    AND   (p_hypertable IS NULL OR j.hypertable_name::text = p_hypertable)
    ORDER BY h.start_time DESC, h.id DESC
    LIMIT v_limit;
END;
";
        var tag = DollarTag("fn", body);

        return $@"
CREATE OR REPLACE FUNCTION {collect}.{FunctionName}
(
    p_from timestamp with time zone,
    p_to timestamp with time zone,
    p_job_kind text DEFAULT NULL,
    p_hypertable text DEFAULT NULL,
    p_limit integer DEFAULT 50
)
RETURNS TABLE
(
    id bigint,
    job_id integer,
    proc_schema text,
    proc_name text,
    hypertable_name text,
    start_time timestamp with time zone,
    finish_time timestamp with time zone,
    succeeded boolean,
    sqlerrcode text,
    error_readable boolean,
    err_message text,
    oldest_row_at timestamp with time zone
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS {tag}{body}{tag};
REVOKE ALL ON FUNCTION {collect}.{FunctionName}({ArgumentTypes}) FROM PUBLIC;";
    }

    /// <summary>
    /// <c>GRANT EXECUTE</c> to each of <paramref name="roles"/> that EXISTS and has USAGE on the schema. A bring-your-own store has no <c>mcp</c> role and may have renamed
    /// the others, so a missing role is skipped rather than failing the grant for the rest. A role with the same name that cannot reach the schema is not Darling's, so it gets no grant.
    /// </summary>
    public static string BuildGrantSql(string collectSchema, IReadOnlyList<string> roles)
    {
        if (roles is null)
        {
            throw new ArgumentNullException(nameof(roles));
        }

        var array = string.Join(", ", roles.Select(QuoteLiteral));
        var body = $@"
DECLARE
    grantee text;
BEGIN
    FOREACH grantee IN ARRAY ARRAY[{array}]::text[]
    LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee)
        AND pg_catalog.has_schema_privilege(grantee, {QuoteLiteral(collectSchema)}, 'USAGE') THEN
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %I.{FunctionName}({ArgumentTypes}) TO %I', {QuoteLiteral(collectSchema)}, grantee);
        END IF;
    END LOOP;
END
";
        var tag = DollarTag("do", body);
        return $@"
DO {tag}{body}{tag};";
    }

    /// <summary>Creates or refreshes the function and grants it to the read roles that exist. Returns true when it is in place.</summary>
    public static async Task<bool> CreateAndGrantAsync(
        NpgsqlConnection connection, string collectSchema, IReadOnlyList<string> readerRoles, ILogger? logger, CancellationToken cancellationToken)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        await using (var create = new NpgsqlCommand(BuildFunctionSql(collectSchema), connection) { CommandTimeout = 60 })
        {
            await create.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var grant = new NpgsqlCommand(BuildGrantSql(collectSchema, readerRoles), connection) { CommandTimeout = 60 })
        {
            await grant.ExecuteNonQueryAsync(cancellationToken);
        }

        logger?.LogDebug("Store job runs: {Schema}.{Function} is in place.", collectSchema, FunctionName);
        return true;
    }

    private static string DollarTag(string stem, string body)
    {
        for (var n = 0; ; n++)
        {
            var tag = n == 0 ? $"${stem}$" : $"${stem}{n}$";
            if (!body.Contains(tag, StringComparison.Ordinal))
            {
                return tag;
            }
        }
    }

    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrEmpty(identifier))
        {
            throw new ArgumentException("An identifier is required.", nameof(identifier));
        }

        return "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    private static string QuoteLiteral(string value) => "'" + value.Replace("'", "''", StringComparison.Ordinal) + "'";
}
