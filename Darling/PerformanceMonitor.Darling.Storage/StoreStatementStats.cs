/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The store's OWN per-statement timings (#3899): <c>pg_stat_statements</c> in the store's database, and the
/// one reader function the least-privilege roles call to rank them. Field reports that "the web viewer / MCP
/// tools are slow" could not be answered from anything the product recorded; with this they are a ranked list
/// split by the role that ran each statement: <c>viewer</c> (the web viewer), <c>mcp</c> (MCP tools),
/// <c>admin</c> (the Darling Viewer's seat), and the owner (collection, maintenance, alerting).
///
/// <para><b>Runtime setup, not a migration.</b> The same posture as <see cref="TimescaleSupport"/>: the store
/// must work with or without the module, so nothing here bumps the schema version or gates a viewer. The
/// managed store preloads the library through the conf's v13 block. <see cref="EnsureAsync"/> is a step of the
/// store-object convergence list (#3817), so it runs on the start path and on the hourly store-maintenance tick,
/// is idempotent, and leaves any store that cannot have the module in a named state that
/// <c>get_store_query_stats</c> reports with its remedy (a BYO store without the preload, or without a
/// superuser to create the extension).</para>
///
/// <para><b>Why a SECURITY DEFINER function.</b> The view shows every role's statement statistics to anyone,
/// but the TEXT only to superusers, members of <c>pg_read_all_stats</c>, or the role that ran it. Granting
/// <c>pg_read_all_stats</c> to the reader roles would also hand them every live session's query text in
/// <c>pg_stat_activity</c>. The function returns only this database's statements, normalized (constants are
/// replaced with <c>$n</c> in the text the view stores), and refuses role DDL, so the one thing the reader
/// roles gain is the ranking this exists for. The #3334 resolve function is the precedent for the shape.</para>
///
/// <para><b>Passwords never reach the view.</b> Provisioning puts each role's password in an
/// <c>ALTER ROLE ... PASSWORD '...'</c> literal on every start, and the module records a utility statement's
/// text without normalizing that literal. The conf's v13 block sets <c>pg_stat_statements.track_utility =
/// off</c>, so utility statements are never recorded. Two guards stand behind it because a store could have
/// loaded the module before this build: every pass scrubs any recorded role DDL
/// (<see cref="ScrubRoleDdlSql"/>), and the reader function excludes it.</para>
/// </summary>
public static class StoreStatementStats
{
    /// <summary>The extension, and the library the conf's v13 block preloads.</summary>
    public const string ExtensionName = "pg_stat_statements";

    /// <summary>The reader function's name, created in the <c>config</c> schema.</summary>
    public const string FunctionName = "store_statement_stats";

    /// <summary>The companion reader for <c>pg_stat_statements_info</c>: when the counters were last reset and
    /// how many entries the view has evicted since, the two facts that say what window the ranking covers.</summary>
    public const string InfoFunctionName = "store_statement_stats_info";

    /// <summary>
    /// Role DDL as a case-insensitive PostgreSQL regular expression: <c>CREATE</c>/<c>ALTER</c> followed by
    /// <c>ROLE</c>/<c>USER</c> as whole words. Shared by the scrub and the reader function, so the statements the
    /// scrub removes and the statements the function refuses are the same set.
    /// </summary>
    public const string RoleDdlPattern = @"\m(create|alter)\s+(role|user)\M";

    /// <summary>What <see cref="EnsureAsync"/> found and did. An unexpected failure is not an outcome: it
    /// throws, and the store-object convergence runner that calls this tallies and logs it (#3817).</summary>
    public enum SetupOutcome
    {
        /// <summary>Extension, reader function and grants in place, and the library is loaded.</summary>
        Ready,

        /// <summary>Extension and function in place, but the library is not loaded (or this role cannot read
        /// whether it is): the preload is restart-only, so the store needs a restart before the view answers.</summary>
        NotPreloaded,

        /// <summary>This PostgreSQL installation does not ship the module.</summary>
        NotAvailable,

        /// <summary>The extension is not installed and this role cannot install it: the module is untrusted, so
        /// <c>CREATE EXTENSION</c> needs a superuser. A store the product does not manage lands here until its
        /// operator creates the extension; the next pass then builds the rest.</summary>
        NotPermitted,
    }

    /// <summary>Whether the module ships with this installation, whether the extension is installed, whether
    /// this role is a superuser (needed to install it and to scrub), and whether the library is loaded. Reading
    /// <c>shared_preload_libraries</c> needs superuser or <c>pg_read_all_settings</c>; a role without either
    /// reads null there and the preload is treated as unknown.</summary>
    public const string ProbeSql = @"
SELECT
    EXISTS
    (
        SELECT 1
        FROM pg_catalog.pg_available_extensions
        WHERE name = 'pg_stat_statements'
    ) AS available,
    EXISTS
    (
        SELECT 1
        FROM pg_catalog.pg_extension
        WHERE extname = 'pg_stat_statements'
    ) AS installed,
    COALESCE((SELECT r.rolsuper FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user), false) AS superuser,
    CASE
        WHEN pg_catalog.pg_has_role(current_user, 'pg_read_all_settings', 'MEMBER')
          OR (SELECT r.rolsuper FROM pg_catalog.pg_roles AS r WHERE r.rolname = current_user)
        THEN EXISTS
        (
            SELECT 1
            FROM pg_catalog.unnest(pg_catalog.string_to_array(pg_catalog.current_setting('shared_preload_libraries'), ',')) AS l(name)
            WHERE pg_catalog.lower(pg_catalog.btrim(l.name, ' ""')) = 'pg_stat_statements'
        )
    END AS preloaded";

    /// <summary>Created in <c>public</c>, the extension's documented home. Left alone when it already exists
    /// anywhere: <see cref="ExtensionSchemaSql"/> finds it wherever an operator put it.</summary>
    public const string CreateExtensionSql = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public";

    /// <summary>The schema the extension lives in, or no row when it is not installed.</summary>
    public const string ExtensionSchemaSql = @"
SELECT n.nspname
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = e.extnamespace
WHERE e.extname = 'pg_stat_statements'";

    /// <summary>
    /// The reader function (#3899) and its <see cref="InfoFunctionName"/> companion, each with the mandatory
    /// <c>REVOKE ... FROM PUBLIC</c> (a new function is
    /// executable by PUBLIC by default). Definer-safe by construction: owned by whoever runs it (the store
    /// owner), <c>search_path</c> pinned to <c>pg_catalog, pg_temp</c> with every other reference
    /// schema-qualified, no dynamic SQL, and no parameters. Scoped to the current database, and refusing role
    /// DDL (<see cref="RoleDdlPattern"/>) even though <c>track_utility = off</c> keeps it out of the view.
    /// Column names are the reader's contract; <c>total_plan_ms</c> is zero unless
    /// <c>pg_stat_statements.track_planning</c> is on, which the store leaves at its default.
    /// </summary>
    public static string BuildFunctionSql(string configSchema, string extensionSchema)
    {
        var config = QuoteIdentifier(configSchema);
        var extension = QuoteIdentifier(extensionSchema);
        return $@"
CREATE OR REPLACE FUNCTION {config}.{FunctionName}()
RETURNS TABLE
(
    role_name text,
    queryid bigint,
    calls bigint,
    total_exec_ms double precision,
    mean_exec_ms double precision,
    max_exec_ms double precision,
    total_plan_ms double precision,
    rows_returned bigint,
    shared_blks_hit bigint,
    shared_blks_read bigint,
    temp_blks_written bigint,
    query text
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
    SELECT
        r.rolname::text,
        s.queryid,
        s.calls,
        s.total_exec_time,
        s.mean_exec_time,
        s.max_exec_time,
        s.total_plan_time,
        s.rows,
        s.shared_blks_hit,
        s.shared_blks_read,
        s.temp_blks_written,
        s.query
    FROM {extension}.pg_stat_statements AS s
    JOIN pg_catalog.pg_roles AS r
      ON r.oid = s.userid
    WHERE s.dbid =
    (
        SELECT d.oid
        FROM pg_catalog.pg_database AS d
        WHERE d.datname = pg_catalog.current_database()
    )
    AND   s.query !~* '{RoleDdlPattern}';
$fn$;
REVOKE ALL ON FUNCTION {config}.{FunctionName}() FROM PUBLIC;

CREATE OR REPLACE FUNCTION {config}.{InfoFunctionName}()
RETURNS TABLE
(
    stats_reset timestamp with time zone,
    dealloc bigint
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $fn$
    SELECT
        i.stats_reset,
        i.dealloc
    FROM {extension}.pg_stat_statements_info AS i;
$fn$;
REVOKE ALL ON FUNCTION {config}.{InfoFunctionName}() FROM PUBLIC;";
    }

    /// <summary>
    /// <c>GRANT EXECUTE</c> on both reader functions to each of <paramref name="roles"/> that EXISTS. A BYO store
    /// has no <c>mcp</c> role and may have renamed the others, so a missing role is skipped rather than failing
    /// the grant for the rest.
    /// </summary>
    public static string BuildGrantSql(string configSchema, IReadOnlyList<string> roles)
    {
        if (roles is null)
        {
            throw new ArgumentNullException(nameof(roles));
        }

        var array = string.Join(", ", roles.Select(QuoteLiteral));
        return $@"
DO $do$
DECLARE
    grantee text;
BEGIN
    FOREACH grantee IN ARRAY ARRAY[{array}]::text[]
    LOOP
        IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = grantee) THEN
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %I.{FunctionName}() TO %I', {QuoteLiteral(configSchema)}, grantee);
            EXECUTE pg_catalog.format('GRANT EXECUTE ON FUNCTION %I.{InfoFunctionName}() TO %I', {QuoteLiteral(configSchema)}, grantee);
        END IF;
    END LOOP;
END
$do$;";
    }

    /// <summary>
    /// Removes any recorded role DDL from the view and returns how many entries went: the scrub behind the
    /// v13 block's <c>track_utility = off</c>, for a store that loaded the module before that setting was in
    /// force. Needs the library loaded and the right to call <c>pg_stat_statements_reset</c> (superuser by
    /// default). Nothing on a store that never recorded any.
    /// </summary>
    public static string BuildScrubRoleDdlSql(string extensionSchema)
    {
        var extension = QuoteIdentifier(extensionSchema);
        return $@"
SELECT pg_catalog.count(*)
FROM
(
    SELECT {extension}.pg_stat_statements_reset(s.userid, s.dbid, s.queryid)
    FROM {extension}.pg_stat_statements AS s
    WHERE s.query ~* '{RoleDdlPattern}'
) AS scrubbed";
    }

    /// <summary>The scrub, as it runs against the default <c>public</c> install.</summary>
    public static readonly string ScrubRoleDdlSql = BuildScrubRoleDdlSql("public");

    /// <summary>
    /// Creates the extension (when the installation ships it and this role may), the reader function and its
    /// grants, and scrubs any recorded role DDL once the library is loaded. Idempotent; a step of the
    /// store-object convergence list, so it runs on the start path and again on the hourly store-maintenance
    /// tick, and a store whose operator creates the extension by hand, or whose function or grant is dropped,
    /// heals within the hour. Every steady state logs at Debug, because the hourly pass would otherwise repeat
    /// the same line all day; <c>get_store_query_stats</c> is the surface that says which state a store is in
    /// and what fixes it. An unexpected failure THROWS for the convergence runner to tally and log.
    /// </summary>
    public static async Task<SetupOutcome> EnsureAsync(
        NpgsqlConnection connection,
        string configSchema,
        IReadOnlyList<string> readerRoles,
        ILogger? logger,
        CancellationToken cancellationToken = default)
    {
        if (connection is null)
        {
            throw new ArgumentNullException(nameof(connection));
        }

        bool available;
        bool installed;
        bool superuser;
        bool? preloaded;
        await using (var probe = new NpgsqlCommand(ProbeSql, connection) { CommandTimeout = SetupTimeoutSeconds })
        await using (var reader = await probe.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            available = reader.GetBoolean(0);
            installed = reader.GetBoolean(1);
            superuser = reader.GetBoolean(2);
            preloaded = await reader.IsDBNullAsync(3, cancellationToken) ? null : reader.GetBoolean(3);
        }

        if (!available)
        {
            logger?.LogDebug("Statement statistics: this PostgreSQL installation does not ship {Extension}.", ExtensionName);
            return SetupOutcome.NotAvailable;
        }

        if (!installed)
        {
            if (!superuser)
            {
                logger?.LogDebug("Statement statistics: {Extension} is not installed and this role is not a superuser, so it cannot install it.", ExtensionName);
                return SetupOutcome.NotPermitted;
            }

            await using var create = new NpgsqlCommand(CreateExtensionSql, connection) { CommandTimeout = SetupTimeoutSeconds };
            await create.ExecuteNonQueryAsync(cancellationToken);
            logger?.LogInformation("Statement statistics: created the {Extension} extension.", ExtensionName);
        }

        string extensionSchema;
        await using (var schema = new NpgsqlCommand(ExtensionSchemaSql, connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            extensionSchema = await schema.ExecuteScalarAsync(cancellationToken) as string
                ?? throw new InvalidOperationException($"{ExtensionName} is not in pg_extension after CREATE EXTENSION.");
        }

        await using (var function = new NpgsqlCommand(BuildFunctionSql(configSchema, extensionSchema), connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await function.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var grant = new NpgsqlCommand(BuildGrantSql(configSchema, readerRoles), connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await grant.ExecuteNonQueryAsync(cancellationToken);
        }

        if (preloaded != true)
        {
            logger?.LogDebug(
                "Statement statistics: {Extension} is installed but {State}; it loads on the store's next restart once it is in shared_preload_libraries.",
                ExtensionName, preloaded is null ? "whether it is loaded cannot be read with this role" : "not loaded");
            return SetupOutcome.NotPreloaded;
        }

        /* Only a superuser may call pg_stat_statements_reset by default. Skipping it for anyone else loses no
           protection on the read path, because the reader function refuses role DDL on its own. */
        if (superuser)
        {
            await using var scrub = new NpgsqlCommand(BuildScrubRoleDdlSql(extensionSchema), connection) { CommandTimeout = SetupTimeoutSeconds };
            var scrubbed = Convert.ToInt64(await scrub.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            if (scrubbed > 0)
            {
                logger?.LogWarning(
                    "Statement statistics: removed {Count} recorded role-DDL statement(s) from {Extension}. They were recorded before pg_stat_statements.track_utility was off, and role DDL can carry a password literal.",
                    scrubbed, ExtensionName);
            }
        }

        logger?.LogDebug("Statement statistics ready: {Extension} is loaded and {Schema}.{Function}() is granted to the reader roles.", ExtensionName, configSchema, FunctionName);
        return SetupOutcome.Ready;
    }

    /// <summary>Per-statement budget for the setup steps: catalog DDL and a scan of at most
    /// <c>pg_stat_statements.max</c> entries, so generous rather than tight.</summary>
    public const int SetupTimeoutSeconds = 60;

    private static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrEmpty(identifier))
        {
            throw new ArgumentException("An identifier is required.", nameof(identifier));
        }

        return "\"" + identifier.Replace("\"", "\"\"", StringComparison.Ordinal) + "\"";
    }

    private static string QuoteLiteral(string value)
    {
        var builder = new StringBuilder(value.Length + 2);
        builder.Append('\'');
        builder.Append(value.Replace("'", "''", StringComparison.Ordinal));
        builder.Append('\'');
        return builder.ToString();
    }
}
