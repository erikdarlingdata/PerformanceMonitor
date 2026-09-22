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
/// reader functions the least-privilege roles call to rank them. Field reports that "the web viewer / MCP
/// tools are slow" could not be answered from anything the product recorded; with this they are a ranked list
/// split by the role that ran each statement. On a MANAGED store those roles are the surfaces: <c>viewer</c> is
/// the web viewer, remote read-only Darling Viewer seats and the service's custom-alert rule evaluation,
/// <c>mcp</c> is MCP tools, <c>admin</c> is the local Darling Viewer and its Settings window, and the owner is
/// the service's collection, maintenance and alerting. A compose or bring-your-own store runs its web and MCP
/// hosts as the owner login, so there everything they ran is counted under the owner.
///
/// <para><b>Runtime setup, not a migration.</b> The same posture as <see cref="TimescaleSupport"/>: the store
/// must work with or without the module, so nothing here bumps the schema version or gates a viewer. The
/// managed store preloads the library through the conf's v13 block. <see cref="EnsureAsync"/> is a step of the
/// store-object convergence list (#3817), so it runs at every start and again on the hourly store-maintenance
/// tick of a TimescaleDB store (that tick is gated on TimescaleDB, so a plain-PostgreSQL store re-runs it at its
/// next start). It is idempotent, and it leaves any store that cannot have the module in a named state that
/// <c>get_store_query_stats</c> reports with its remedy.</para>
///
/// <para><b>Why a SECURITY DEFINER function.</b> The view shows every role's statement statistics to anyone,
/// but the TEXT only to superusers, members of <c>pg_read_all_stats</c>, or the role that ran it. Granting
/// <c>pg_read_all_stats</c> to the reader roles would also hand them every live session's query text in
/// <c>pg_stat_activity</c>. The function returns only this database's statements, normalized (constants are
/// replaced with <c>$n</c> in the text the view stores), and refuses every statement whose text can carry a
/// credential, so the one thing the reader roles gain is the ranking this exists for. The definer needs the
/// text itself: a store owner that is neither a superuser nor a <c>pg_read_all_stats</c> member reads other
/// roles' rows as <c>&lt;insufficient privilege&gt;</c>, which <c>get_store_query_stats</c> reports. The #3334
/// resolve function is the precedent for the shape.</para>
///
/// <para><b>Secrets never reach the reader.</b> Provisioning puts each role's password in an
/// <c>ALTER ROLE ... PASSWORD '...'</c> literal on every start, and the module records a utility statement's
/// text without normalizing that literal (measured on the bundled 18.4 / 1.12). The conf's v13 block sets
/// <c>pg_stat_statements.track_utility = off</c>, so a managed store records no utility statement at all. Two
/// guards stand behind it, for a store that tracks utility statements anyway: the reader refuses every statement
/// <see cref="SensitiveStatementPattern"/> names, and every pass that may reset entries removes them from the
/// view. The pattern holds no backslash and both functions pin <c>standard_conforming_strings</c>, because a
/// string-body SQL function is parsed when it is CALLED, under the caller's settings: #3904's review showed a
/// backslash pattern switched off by one <c>SET standard_conforming_strings = off</c> from any grantee.</para>
/// </summary>
public static class StoreStatementStats
{
    /// <summary>The extension, and the library the conf's v13 block preloads.</summary>
    public const string ExtensionName = "pg_stat_statements";

    /// <summary>The reader function's name, created in the <c>config</c> schema.</summary>
    public const string FunctionName = "store_statement_stats";

    /// <summary>The companion reader for <c>pg_stat_statements_info</c>: when the counters were last reset and
    /// how many eviction passes the module has run since, the two facts that say what window the ranking
    /// covers.</summary>
    public const string InfoFunctionName = "store_statement_stats_info";

    /// <summary>The oldest extension version the reader can serve. 1.8 (PostgreSQL 13) renamed <c>total_time</c>
    /// to <c>total_exec_time</c> and added the plan-time columns the reader selects. <c>pg_upgrade</c> never
    /// updates an extension, so a store upgraded from PostgreSQL 12 can still carry 1.7; it is reported as
    /// <see cref="SetupOutcome.Outdated"/> rather than failing the function batch on every pass.</summary>
    public static readonly Version MinimumReaderVersion = new(1, 8);

    /// <summary>The version that added <c>pg_stat_statements_info</c>. On 1.8 the info reader answers nulls,
    /// so the ranking still works and the reset stamp reads as unknown.</summary>
    public static readonly Version InfoViewVersion = new(1, 9);

    /// <summary>
    /// The statements whose text can carry a credential when utility statements are tracked, as a
    /// case-insensitive PostgreSQL regular expression: role, user and group DDL (their <c>PASSWORD</c> clause,
    /// and <c>CREATE/ALTER USER MAPPING</c>'s password option), subscription DDL (a connection string), foreign
    /// server DDL (its options), and any <c>PASSWORD</c> keyword followed by a literal. Shared by the scrub and
    /// the reader function, so what the scrub removes and what the function refuses are the same set.
    ///
    /// <para><b>No backslash, on purpose.</b> <c>[[:&lt;:]]</c>, <c>[[:&gt;:]]</c>, <c>[[:space:]]</c> and
    /// <c>[$]</c> spell the word boundaries, whitespace and dollar sign the first version wrote as <c>\m</c>,
    /// <c>\M</c> and <c>\s</c>, so the literal means the same under either <c>standard_conforming_strings</c>
    /// setting. A normalized DML parameter (<c>password = $1</c>) is not a hit: it carries no value.</para>
    /// </summary>
    public const string SensitiveStatementPattern =
        "[[:<:]](create|alter)[[:space:]]+(role|user|group|subscription|server)[[:>:]]"
        + "|[[:<:]]password[[:>:]][[:space:]]*(=|to)?[[:space:]]*(e?'|[$][^0-9])";

    /// <summary>What <see cref="EnsureAsync"/> found and did. An unexpected failure is not an outcome: it
    /// throws, and the store-object convergence runner that calls this tallies and logs it (#3817).</summary>
    public enum SetupOutcome
    {
        /// <summary>Extension, reader functions and grants in place, and the library is loaded.</summary>
        Ready,

        /// <summary>Extension and functions in place, but the library is not loaded: the preload is
        /// restart-only, so the store needs a restart before the view answers.</summary>
        NotPreloaded,

        /// <summary>This PostgreSQL installation does not ship the module.</summary>
        NotAvailable,

        /// <summary>The extension is not installed and this role cannot install it: the module is untrusted, so
        /// <c>CREATE EXTENSION</c> needs a superuser. A store the product does not manage lands here until its
        /// operator creates the extension; the next pass then builds the rest.</summary>
        NotPermitted,

        /// <summary>The installed extension is older than <see cref="MinimumReaderVersion"/>, so the readers are
        /// not built. <c>ALTER EXTENSION pg_stat_statements UPDATE</c>, run by a superuser, is the fix; the next
        /// pass then builds them.</summary>
        Outdated,
    }

    /// <summary>
    /// Whether the module ships with this installation, whether the extension is installed, whether this role
    /// is a superuser (needed to install it and to scrub) or can read every role's statement text, whether the
    /// library is loaded, and whether utility statements are tracked.
    ///
    /// <para>"Loaded" is read from <c>pg_settings</c>: the module's own settings appear there only once the
    /// library is loaded, and a placeholder that a conf line or a <c>SET</c> made before it loaded is hidden
    /// from the view (measured on 18.4). That works for any role. The first version read
    /// <c>shared_preload_libraries</c> instead, which needs <c>pg_read_all_settings</c>, and a <c>CASE</c> did
    /// not protect the read: the planner evaluates it while estimating <c>unnest</c>'s rows, so a role without
    /// that membership failed the whole probe (#3904's review).</para>
    /// </summary>
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
    pg_catalog.pg_has_role(current_user, 'pg_read_all_stats', 'USAGE') AS reads_all_stats,
    EXISTS
    (
        SELECT 1
        FROM pg_catalog.pg_settings
        WHERE name = 'pg_stat_statements.max'
    ) AS loaded,
    (
        SELECT s.setting
        FROM pg_catalog.pg_settings AS s
        WHERE s.name = 'pg_stat_statements.track_utility'
    ) AS track_utility";

    /// <summary>Created in <c>public</c>, the extension's documented home. Left alone when it already exists
    /// anywhere: <see cref="ExtensionSql"/> finds it wherever an operator put it.</summary>
    public const string CreateExtensionSql = "CREATE EXTENSION IF NOT EXISTS pg_stat_statements WITH SCHEMA public";

    /// <summary>The schema the extension lives in and its installed version, or no row when it is not
    /// installed.</summary>
    public const string ExtensionSql = @"
SELECT
    n.nspname,
    e.extversion
FROM pg_catalog.pg_extension AS e
JOIN pg_catalog.pg_namespace AS n
  ON n.oid = e.extnamespace
WHERE e.extname = 'pg_stat_statements'";

    /// <summary>
    /// The reader function (#3899) and its <see cref="InfoFunctionName"/> companion, each with the mandatory
    /// <c>REVOKE ... FROM PUBLIC</c> (a new function is executable by PUBLIC by default). Definer-safe by
    /// construction: owned by whoever runs it (the store owner), <c>search_path</c> pinned to
    /// <c>pg_catalog, pg_temp</c> with every other reference schema-qualified, <c>standard_conforming_strings</c>
    /// pinned on (the body is parsed at call time under the caller's settings otherwise), no dynamic SQL, and no
    /// parameters. Scoped to the current database, and refusing <see cref="SensitiveStatementPattern"/>. A row
    /// whose text is NULL (the module could not read its text file) is kept, so its timings still rank; a row
    /// whose role has since been dropped is kept under the role's oid.
    ///
    /// <para><paramref name="withInfoView"/> is false on extension 1.8, which has no
    /// <c>pg_stat_statements_info</c>: the info reader then answers nulls rather than failing the batch, and
    /// the next pass after <c>ALTER EXTENSION ... UPDATE</c> replaces it with the real body. Column names are
    /// the reader's contract; <c>total_plan_ms</c> is zero unless <c>pg_stat_statements.track_planning</c> is
    /// on, which the store leaves at its default.</para>
    /// </summary>
    public static string BuildFunctionSql(string configSchema, string extensionSchema, bool withInfoView = true)
    {
        var config = QuoteIdentifier(configSchema);
        var extension = QuoteIdentifier(extensionSchema);
        var infoBody = withInfoView
            ? $@"    SELECT
        i.stats_reset,
        i.dealloc
    FROM {extension}.pg_stat_statements_info AS i;"
            : @"    SELECT
        NULL::timestamp with time zone,
        NULL::bigint;";

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
SET standard_conforming_strings = on
AS $fn$
    SELECT
        COALESCE(r.rolname::text, s.userid::text),
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
    LEFT JOIN pg_catalog.pg_roles AS r
      ON r.oid = s.userid
    WHERE s.dbid =
    (
        SELECT d.oid
        FROM pg_catalog.pg_database AS d
        WHERE d.datname = pg_catalog.current_database()
    )
    AND
    (
         s.query IS NULL
      OR s.query !~* {QuoteLiteral(SensitiveStatementPattern)}
    );
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
SET standard_conforming_strings = on
AS $fn$
{infoBody}
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
    /// Removes this database's statements that <see cref="SensitiveStatementPattern"/> names from the view and
    /// returns how many entries went: the scrub behind <c>track_utility = off</c>, for a store that tracks
    /// utility statements anyway (a bring-your-own store's default, or one that loaded the module before the
    /// v13 block). Scoped to the CURRENT database like the reader, so a superuser service login on a shared
    /// cluster never resets another database's entries. An entry whose query id is 0 is left alone, because a
    /// reset with query id 0 means "every statement of that role", and the reader refuses its text anyway. Needs
    /// the library loaded and the right to call <c>pg_stat_statements_reset</c> (superuser by default).
    /// </summary>
    public static string BuildScrubSql(string extensionSchema)
    {
        var extension = QuoteIdentifier(extensionSchema);
        return $@"
SELECT pg_catalog.count(*)
FROM
(
    SELECT {extension}.pg_stat_statements_reset(s.userid, s.dbid, s.queryid)
    FROM {extension}.pg_stat_statements AS s
    WHERE s.dbid =
    (
        SELECT d.oid
        FROM pg_catalog.pg_database AS d
        WHERE d.datname = pg_catalog.current_database()
    )
    AND   s.queryid <> 0
    AND   s.query ~* {QuoteLiteral(SensitiveStatementPattern)}
) AS scrubbed";
    }

    /// <summary>The scrub, as it runs against the default <c>public</c> install.</summary>
    public static readonly string ScrubSql = BuildScrubSql("public");

    /// <summary>
    /// Whether an <c>extversion</c> string is at least <paramref name="minimum"/>. A version that does not parse
    /// (a development build's suffix) is treated as current: refusing to build the readers over a spelling would
    /// be the worse failure, and a genuinely old version parses.
    /// </summary>
    public static bool ExtensionVersionAtLeast(string? extensionVersion, Version minimum)
    {
        ArgumentNullException.ThrowIfNull(minimum);
        return !Version.TryParse(extensionVersion, out var version) || version >= minimum;
    }

    /// <summary>
    /// Creates the extension (when the installation ships it and this role may), the reader functions and their
    /// grants, and scrubs sensitive statements once the library is loaded. Idempotent; a step of the store-object
    /// convergence list, so it runs at every start and, on a TimescaleDB store, again on the hourly
    /// store-maintenance tick, where an extension a DBA creates by hand, or a function or grant that was dropped,
    /// heals within the hour. Every steady state logs at Debug, because the hourly pass would otherwise repeat the
    /// same line all day; <c>get_store_query_stats</c> is the surface that says which state a store is in and
    /// what fixes it. The one Warning a steady state earns, utility tracking left on, is written once per process.
    /// An unexpected failure THROWS for the convergence runner to tally and log.
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
        bool readsAllStats;
        bool loaded;
        string? trackUtility;
        await using (var probe = new NpgsqlCommand(ProbeSql, connection) { CommandTimeout = SetupTimeoutSeconds })
        await using (var reader = await probe.ExecuteReaderAsync(cancellationToken))
        {
            await reader.ReadAsync(cancellationToken);
            available = reader.GetBoolean(0);
            installed = reader.GetBoolean(1);
            superuser = reader.GetBoolean(2);
            readsAllStats = reader.GetBoolean(3);
            loaded = reader.GetBoolean(4);
            trackUtility = await reader.IsDBNullAsync(5, cancellationToken) ? null : reader.GetString(5);
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
        string? extensionVersion;
        await using (var extension = new NpgsqlCommand(ExtensionSql, connection) { CommandTimeout = SetupTimeoutSeconds })
        await using (var reader = await extension.ExecuteReaderAsync(cancellationToken))
        {
            if (!await reader.ReadAsync(cancellationToken))
            {
                throw new InvalidOperationException($"{ExtensionName} is not in pg_extension after CREATE EXTENSION.");
            }

            extensionSchema = reader.GetString(0);
            extensionVersion = await reader.IsDBNullAsync(1, cancellationToken) ? null : reader.GetString(1);
        }

        if (!ExtensionVersionAtLeast(extensionVersion, MinimumReaderVersion))
        {
            logger?.LogDebug(
                "Statement statistics: {Extension} {Version} is older than {Minimum}, the oldest version the store's reader can serve; ALTER EXTENSION pg_stat_statements UPDATE, run by a superuser, lets the next pass build it.",
                ExtensionName, extensionVersion, MinimumReaderVersion);
            return SetupOutcome.Outdated;
        }

        var withInfoView = ExtensionVersionAtLeast(extensionVersion, InfoViewVersion);
        await using (var function = new NpgsqlCommand(BuildFunctionSql(configSchema, extensionSchema, withInfoView), connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await function.ExecuteNonQueryAsync(cancellationToken);
        }

        await using (var grant = new NpgsqlCommand(BuildGrantSql(configSchema, readerRoles), connection) { CommandTimeout = SetupTimeoutSeconds })
        {
            await grant.ExecuteNonQueryAsync(cancellationToken);
        }

        if (!loaded)
        {
            logger?.LogDebug(
                "Statement statistics: {Extension} is installed but not loaded; it loads on the store's next restart once it is in shared_preload_libraries.",
                ExtensionName);
            return SetupOutcome.NotPreloaded;
        }

        if (string.Equals(trackUtility, "on", StringComparison.OrdinalIgnoreCase))
        {
            WarnUtilityTracking(logger, superuser);
        }

        /* Only a superuser may call pg_stat_statements_reset by default. Skipping it for anyone else loses no
           protection on the read path, because the reader function refuses those statements on its own. */
        if (superuser)
        {
            await using var scrub = new NpgsqlCommand(BuildScrubSql(extensionSchema), connection) { CommandTimeout = SetupTimeoutSeconds };
            var scrubbed = Convert.ToInt64(await scrub.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
            if (scrubbed > 0)
            {
                logger?.LogWarning(
                    "Statement statistics: removed {Count} statement(s) from {Extension} in this database whose text can carry a credential (role, user-mapping, subscription or server DDL, or a PASSWORD literal). The module records them only while pg_stat_statements.track_utility is on.",
                    scrubbed, ExtensionName);
            }
        }

        if (!superuser && !readsAllStats)
        {
            logger?.LogDebug(
                "Statement statistics: the reader functions run as this role, which is neither a superuser nor a member of pg_read_all_stats, so other roles' statement text reads <insufficient privilege>. GRANT pg_read_all_stats to it to show that text.");
        }

        logger?.LogDebug("Statement statistics ready: {Extension} is loaded and {Schema}.{Function}() is granted to the reader roles.", ExtensionName, configSchema, FunctionName);
        return SetupOutcome.Ready;
    }

    /// <summary>Per-statement budget for the setup steps: catalog DDL and a scan of at most
    /// <c>pg_stat_statements.max</c> entries, so generous rather than tight.</summary>
    public const int SetupTimeoutSeconds = 60;

    /// <summary>
    /// A statement's text as one line: comments removed, whitespace collapsed, cut at
    /// <paramref name="maxLength"/> with "..." when it was longer. The product's SQL carries long comment
    /// blocks, which is most of what this removes. A single-quoted literal is copied through untouched, so a
    /// <c>--</c> inside one survives. The one comment KEPT is PostgreSQL 18's normalized IN-list marker
    /// (<c>IN ($1 /*, ... */)</c>, measured on 18.4): it is part of what the normalized text says, and stripping
    /// it leaves <c>IN ($1 )</c>, which reads as a one-element list.
    ///
    /// <para>Shared by <c>get_store_query_stats</c>' preview and the store log's slow-statement entries
    /// (<see cref="StoreLogClassifier"/>), which run this BEFORE masking literals: the product's SQL comments
    /// carry apostrophes, and a literal mask run over them first would swallow the SQL between one apostrophe
    /// and the next quote.</para>
    /// </summary>
    public static string CompactStatementText(string? text, int maxLength)
    {
        if (string.IsNullOrEmpty(text))
        {
            return "";
        }

        var builder = new StringBuilder(Math.Min(text.Length, maxLength));
        var inQuote = false;
        var pendingSpace = false;
        for (var i = 0; i < text.Length; i++)
        {
            var c = text[i];
            if (inQuote)
            {
                builder.Append(c);
                inQuote = c != '\'';
                continue;
            }

            if (c == '-' && i + 1 < text.Length && text[i + 1] == '-')
            {
                var newline = text.IndexOf('\n', i);
                i = newline < 0 ? text.Length : newline;
                pendingSpace = true;
                continue;
            }

            if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                if (string.CompareOrdinal(text, i, InListMarker, 0, InListMarker.Length) == 0)
                {
                    if (pendingSpace && builder.Length > 0)
                    {
                        builder.Append(' ');
                    }

                    pendingSpace = false;
                    builder.Append(InListMarker);
                    i += InListMarker.Length - 1;
                    if (builder.Length > maxLength)
                    {
                        break;
                    }

                    continue;
                }

                var close = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = close < 0 ? text.Length : close + 1;
                pendingSpace = true;
                continue;
            }

            if (char.IsWhiteSpace(c))
            {
                pendingSpace = true;
                continue;
            }

            if (pendingSpace && builder.Length > 0)
            {
                builder.Append(' ');
            }

            pendingSpace = false;
            builder.Append(c);
            inQuote = c == '\'';
            if (builder.Length > maxLength)
            {
                break;
            }
        }

        return builder.Length > maxLength
            ? builder.ToString(0, maxLength).TrimEnd() + "..."
            : builder.ToString();
    }

    /// <summary>PostgreSQL 18's marker for a squashed constant list in normalized text.</summary>
    private const string InListMarker = "/*, ... */";

    /* Once per process: a store that tracks utility statements is told at its first pass, not every hour. */
    private static int s_utilityTrackingWarned;

    /// <summary>Re-arms the once-per-process utility-tracking warning, for the test that asserts it.</summary>
    internal static void ResetUtilityTrackingWarning() => Interlocked.Exchange(ref s_utilityTrackingWarned, 0);

    private static void WarnUtilityTracking(ILogger? logger, bool superuser)
    {
        if (Interlocked.Exchange(ref s_utilityTrackingWarned, 1) != 0)
        {
            logger?.LogDebug("Statement statistics: pg_stat_statements.track_utility is still on for this store.");
            return;
        }

        logger?.LogWarning(
            "Statement statistics: pg_stat_statements.track_utility is on for this store, so utility statements are recorded with their literals as typed, role DDL's PASSWORD literal among them. The store's reader refuses those statements{Scrub}, but any other role that can read pg_stat_statements on this cluster sees them. Set pg_stat_statements.track_utility = off in postgresql.conf (a managed store's v13 block does) and reload; this is logged once per service start.",
            superuser ? " and this service removes them on each pass" : "");
    }

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
