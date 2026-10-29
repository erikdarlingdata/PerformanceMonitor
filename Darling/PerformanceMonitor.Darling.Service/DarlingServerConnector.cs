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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service.Targets;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Per-server runtime state the collection loop carries: the resolved connection string, the
/// probed target facts (engine edition, major version — the same detection Lite's ServerManager
/// runs), and the shared-identity server id.
/// </summary>
public sealed class ServerRuntime
{
    public required MonitoredServer Config { get; init; }

    public required string ConnectionString { get; init; }

    public required CollectorTargetInfo Target { get; init; }

    /// <summary>host[:database][:RO] — the shared identity rule, hashed to <see cref="ServerId"/>.</summary>
    public required string StorageName { get; init; }

    public required int ServerId { get; init; }

    /// <summary>
    /// #6771: this server's own plan-capture setting (<see cref="MonitoredServer.CapturePlans"/>): null uses the service-wide
    /// setting. Settable, unlike <see cref="Config"/>, because the worker refreshes it on every registry reload; a plan-capture edit
    /// is not a connection change and must not reconnect the server, so it reaches the next cycle through this property.
    /// </summary>
    public bool? CapturePlans { get; set; }

    /// <summary>
    /// The database this connection ACTUALLY landed in — <c>DB_NAME()</c>, or <c>current_database()</c> on
    /// PostgreSQL — or null when the probe did not return it (#2228).
    ///
    /// <para>Not the same thing as <c>Config.Database</c>, which is what the registration ASKED for. An
    /// Initial Catalog that is absent, misspelled, or overridden by the server lands somewhere else, and every
    /// collected row is then stored under this registration's identity while describing a different database.
    /// Nothing detected that: identity is registration-derived and never checked against the connection, so N
    /// registrations that silently resolve to one database produce N identities and N full copies of the same
    /// rows — the shape #2220 reported as byte-identical deadlock graphs under six ids.</para>
    /// </summary>
    public string? ConnectedDatabase { get; init; }

    /// <summary>#6285: the machine and instance the SQL Server calls itself (<c>SERVERPROPERTY('ServerName')</c>), read by the
    /// same detection query; null on an Azure SQL Database, which names no machine of its own. #6339: on PostgreSQL, the cluster's
    /// <c>system_identifier</c>, a colon and the server port, or null when the role may not read it.</summary>
    public string? MachineIdentity { get; init; }

    public bool HasMsdbAccess { get; init; }

    /// <summary>
    /// True once the ensure of the server-scoped client-timeout session has dropped and recreated a session of that name that had
    /// another definition (#5640). Per connection, so it happens at most once per connect: if the recreated session still read
    /// as another definition, the next ensure leaves it alone and does not drop it again.
    /// </summary>
    public bool AttentionXeDefinitionReplaced { get; set; }

    public bool IsAwsRds { get; init; }

    /// <summary>
    /// The raw SERVERPROPERTY('EngineEdition') value from the detection probe — 1 Personal,
    /// 2 Standard, 3 Enterprise, 4 Express, 5 Azure SQL DB, 8 Managed Instance, etc. — carried
    /// whole so the servers registry records the real edition, not just the 5/8 classification
    /// booleans on <see cref="Target"/>.
    /// </summary>
    public int EngineEdition { get; init; }

    /// <summary>#6893: <c>SERVERPROPERTY('Edition')</c> (for example <c>Web Edition (64-bit)</c>), or null when the probe did not read it. Web and Standard
    /// share EngineEdition 2, so the add-server result line names the edition from this text.</summary>
    public string? EditionName { get; init; }

    /// <summary>
    /// What the PostgreSQL detection query saw in <c>pg_extension</c> for <c>pg_stat_statements</c>, in the
    /// database this connection landed in (#3830) - the ROW, not a verdict drawn from it. Default
    /// <see cref="PgExtensionRowObservation.NotObserved"/> on every SQL Server target and on any runtime a
    /// test builds by hand, which every consumer must read as "no answer" rather than "no row".
    ///
    /// <para><b>What it is for.</b> The companion-object fault sentence (#3818) has the exception and not
    /// the catalog, so it had one remedy - <c>ALTER EXTENSION pg_stat_statements UPDATE</c> - for three
    /// different states. #3830 is the record of the cost: the 23 clusters that motivated #3818 have NO
    /// <c>pg_extension</c> row at all, the collector reads Aurora's <c>aurora_stat_statements()</c> which
    /// needs none, and they were being told to update an extension that was never created. This is the
    /// value that lets <c>DarlingWorker.PostgresFaultOutcome</c> RECEIVE the state instead of guessing it.</para>
    ///
    /// <para><b>Connect-scoped, deliberately, and bounded to a sentence.</b> It moves when the connection is
    /// re-established, so an operator who runs <c>CREATE EXTENSION</c> mid-life is described by the value
    /// read at connect until then. Nothing DISPATCHES on it - it changes no gate and skips no collector, so
    /// the stale direction costs a sentence naming a remedy already applied, on a fault that stops being
    /// raised at the same moment. A gate on this would be the #2559 defect ("run the GRANT, nothing happens
    /// until restart"), which is why it is not one.</para>
    /// </summary>
    public PgExtensionRowObservation PgStatStatementsExtension { get; init; }
}

/// <summary>
/// Opens the first connection to a monitored server and probes the target facts the collector
/// definitions branch on. The detection query is verbatim from Lite's ServerManager connectivity
/// check, so both SKUs classify a server identically.
/// </summary>
public static class DarlingServerConnector
{
    /* The scalar detection query - verbatim (modulo whitespace) from Lite's ServerManager
       connectivity check. Deliberately NO FROM sys.dm_os_sys_info: that DMV requires VIEW DATABASE
       STATE, which an Azure SQL DB monitoring login often lacks, so edition detection must not
       depend on it (#1535). sqlserver_start_time - the one column that needs the DMV - is not read
       here (the service never surfaces a start time), so unlike Lite/Dashboard no best-effort
       start-time read is needed. Columns: 0 sql_version, 1 major_version, 2 utc_offset,
       3 engine_edition, 4 is_aws_rds, 5 has_msdb_access, 6 connected_database (#2228), 7 server_name (#6285), 8 edition_name (#6893).

       #2228 — connected_database is DB_NAME(): the database this connection ACTUALLY reached, which is not
       necessarily the one the registration names. An Initial Catalog that is absent, misspelled or overridden
       lands somewhere else, and every collected row is then stored under this registration's identity while
       describing a different database. DB_NAME() keeps this query's no-permission property: it needs no DMV,
       so it does not reintroduce the VIEW DATABASE STATE dependency #1535 removed. APPENDED, because every
       read above is positional and inserting a column mid-list shifts five other fields onto wrong values.

       Comments inside these probe strings stay to one short line each: the text is sent to the monitored
       server on every connect, so the reasoning belongs here rather than on the wire. */
    public const string DetectionQueryText = @"
SELECT /* PerformanceMonitorLite */
    @@VERSION AS sql_version,
    CONVERT(integer, SERVERPROPERTY('ProductMajorVersion')) AS major_version,
    DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) AS utc_offset_minutes,
    CONVERT(integer, SERVERPROPERTY('EngineEdition')) AS engine_edition,
    CASE WHEN DB_ID('rdsadmin') IS NOT NULL THEN 1 ELSE 0 END AS is_aws_rds,
    HAS_DBACCESS(N'msdb') AS has_msdb_access,
    /* #2228: which database this connection actually landed in. Appended; see the comment above. */
    DB_NAME() AS connected_database,
    /* #6285: the machine and instance the server calls itself, appended like the column above. */
    CONVERT(nvarchar(128), SERVERPROPERTY('ServerName')) AS server_name,
    /* #6893: Web and Standard share EngineEdition 2, so RDS Web and Express is told apart by the edition name. */
    CONVERT(nvarchar(128), SERVERPROPERTY('Edition')) AS edition_name";

    /// <summary>
    /// The tripwire's verdict: the message to raise when a registration is connected to a database it does not
    /// name, or null when there is nothing to say (#2228).
    ///
    /// <para><b>Silent unless BOTH sides name a database.</b> A registration with no <c>database</c> is
    /// server-scoped by design — it is meant to land wherever the login defaults and enumerate from there — so
    /// comparing it to whatever that default turned out to be would fire on every correctly-configured
    /// server-scoped registration in the fleet. That is the failure mode that gets a tripwire ignored, and an
    /// ignored tripwire is worse than none: it trains the operator past the one line that matters.</para>
    ///
    /// <para>Case-insensitive because SQL Server database names are, under every collation the product
    /// supports, and a registration that differs from the server only in case is not a misconfiguration.
    /// PostgreSQL is case-sensitive in principle, but a registration whose case differs there fails to connect
    /// rather than landing elsewhere, so the looser comparison costs nothing and avoids a false positive on
    /// the engine where it would be wrong.</para>
    ///
    /// <para>Names what is WRONG and what to change, in that order, because the log line is the whole
    /// diagnosis: the operator has to be able to act on it without reading the source. Deliberately does not
    /// say "N copies" — this function sees one registration and cannot know whether a sibling collides with
    /// it; claiming otherwise would be a guess dressed as a finding.</para>
    /// </summary>
    public static string? DescribeDatabaseMismatch(string? registeredDatabase, string? connectedDatabase, string displayName)
    {
        if (string.IsNullOrWhiteSpace(registeredDatabase) || string.IsNullOrWhiteSpace(connectedDatabase))
        {
            return null;
        }

        if (string.Equals(registeredDatabase.Trim(), connectedDatabase.Trim(), StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        return $"Registration '{displayName}' is registered for database '{registeredDatabase}' but its connection " +
               $"landed in '{connectedDatabase}'. Everything collected under this registration describes " +
               $"'{connectedDatabase}', stored under this registration's identity — so if another registration " +
               $"names '{connectedDatabase}', both are collecting the same database and its history is duplicated " +
               "under two identities. Check this server's Initial Catalog / database setting in the Viewer's " +
               "Manage Servers, or the 'database' field for it in darling.json.";
    }

    /// <summary>
    /// #6254: the connection string for the server's Query Store replica, null when none is set. The same secret resolution as
    /// <see cref="ResolveConnectionString"/>; the builder is <see cref="MonitoredServerConnection.BuildQueryStoreReplicaConnectionString"/>.
    /// </summary>
    public static string? ResolveQueryStoreReplicaConnectionString(MonitoredServer config, bool isAzureSqlDatabase, ILogger? logger = null) =>
        /* The builder takes the replica from the identity the password seal opens with and resolves the password itself (a stored
           reference only when darling.json declares it), and returns null for an Azure SQL Database. */
        MonitoredServerConnection.BuildQueryStoreReplicaConnectionString(config, isAzureSqlDatabase);

    public static string ResolveConnectionString(MonitoredServer config, ILogger? logger = null)
    {
        string? password = null;
        if (config.RequiresResolvedSecret)
        {
            /* SQL auth (password) and service principal (client secret) both resolve their secret here from
               EncryptedPassword; managed identity and integrated resolve none. #3484. */
            /* #5366: one dispatch on every platform. A reference or a sealed value resolves anywhere; an old-format
               DPAPI value opens only on Windows and says so on any other platform. The password slot takes the same
               env:/file: references as everywhere else (#1804); a literal still works and still warns below. */
            password = DarlingSecrets.ResolvePassword(config, out var usedPlaintext);

            if (usedPlaintext)
            {
                logger?.LogWarning(
                    "Server '{Server}' uses a plaintext password in darling.json — run --encrypt-password and switch to encryptedPassword, or reference it via env:/file:.",
                    config.DisplayName);
            }
        }

        WarnIfRemediationCredentialIsInert(config, logger);

        return MonitoredServerConnection.BuildConnectionString(config, password);
    }

    /// <summary>
    /// Says at connect what a remediation login is for, and when only half of one is set (#2138, #6263).
    ///
    /// <para>Level 3 server actions (#5632) use the login: force and unforce a Query Store plan, evict a cached plan and
    /// kill a session on SQL Server, and kill a session on PostgreSQL. A server action also needs the server's action
    /// switch, which is off until the store owner turns it on, so an armed login alone does nothing. The line says so,
    /// because an operator who entered a login should learn what else must be on. A one-sided credential is reported
    /// separately: "you configured half of one" is a mistake to fix now.</para>
    ///
    /// <para>Once per connect rather than once per sweep: connects are rare, so this cannot become the
    /// every-60-seconds log line #2255 was about.</para>
    /// </summary>
    internal static void WarnIfRemediationCredentialIsInert(MonitoredServer config, ILogger? logger)
    {
        var username = !string.IsNullOrWhiteSpace(config.RemediationUsername);
        var secret = !string.IsNullOrWhiteSpace(config.RemediationEncryptedPassword);

        if (username ^ secret)
        {
            logger?.LogWarning(
                "Server '{Server}' has only one half of a remediation credential ({Half} is set, the other is not), so it counts as unarmed. Both remediationUsername and remediationEncryptedPassword are required.",
                config.DisplayName,
                username ? "remediationUsername" : "remediationEncryptedPassword");
            return;
        }

        if (username && secret)
        {
            /* Plan force, unforce and eviction are SQL Server only (Query Store and the plan cache), so a PostgreSQL
               target's login serves kill_session alone (#6263). */
            if (config.IsPostgres)
            {
                logger?.LogInformation(
                    "Server '{Server}' has a remediation login. On a PostgreSQL server Darling uses it only to end a session (kill_session), and only when that server's kill_session switch is on. The monitoring login stays read-only.",
                    config.DisplayName);
                return;
            }

            logger?.LogInformation(
                "Server '{Server}' has a remediation login. Darling uses it only for the server actions an agent asks for at Level 3 (force or unforce a plan, evict a cached plan, kill a session), and only for an action whose switch is on for this server. The monitoring login stays read-only.",
                config.DisplayName);
        }
    }

    /* The PostgreSQL detection query. Deliberately built only from surfaces a pg_monitor-grade login
       can read on Amazon Aurora, verified against live 16.11 and 17.7 clusters:

         current_setting('server_version_num') -> 160011 / 170007, so the major is a division rather
           than string parsing (version() text formatting has changed across releases).
         pg_is_in_recovery()                   -> reader vs writer. On Aurora every reader endpoint is
           its own instance with its own statistics, so this is identity, not a routing hint.

       The Aurora marker is NOT here. It used to be a pg_proc lookup on this query and was wrong on real
       Aurora (#2340) — it now lives in PostgresAuroraProbeQueryText, which CALLS the function instead.

       No timezone offset column: unlike SQL Server's DATEDIFF-on-GETDATE idiom, Postgres timestamps
       here are read as-is and the store's convention is naive UTC either way. */
    public const string PostgresDetectionQueryText = @"
SELECT
    pg_catalog.version() AS server_version_text,
    pg_catalog.current_setting('server_version_num')::int OPERATOR(pg_catalog./) 10000 AS major_version,
    pg_catalog.pg_is_in_recovery() AS is_in_recovery,
    pg_catalog.current_setting('server_version_num')::int AS server_version_num,
    -- #2228: which database this connection actually landed in. Appended; see the comment above.
    pg_catalog.current_database() AS connected_database,
    -- #3830: the pg_stat_statements catalog version in THIS database, or NULL when pg_extension has no
    -- row for it. A column on the read that was already being made rather than a probe of its own: one
    -- scalar on one statement, and pg_extension is world-readable, so it adds no round trip and no grant.
    -- Named as a literal because a const cannot interpolate. PostgresFaultOutcomeTests compares it against
    -- the collector's own RequiredPgExtensions declaration, so the two cannot drift apart unnoticed.
    (SELECT e.extversion
     FROM pg_catalog.pg_extension AS e
     WHERE e.extname OPERATOR(pg_catalog.=) 'pg_stat_statements') AS pg_stat_statements_extversion,
    -- #6143: the search_path this session actually has. The service sends search_path=pg_catalog,pg_temp at
    -- sign-in; a connection pooler can drop that startup option without an error, and then this differs.
    pg_catalog.current_setting('search_path') AS search_path";

    /// <summary>
    /// #6143: refuses a PostgreSQL target whose monitoring session does not have the search_path the service sent at
    /// sign-in (<see cref="MonitoredServerConnection.PinnedSearchPathOption"/>). The collectors' SQL relies on it, so a
    /// session without it is not collected from at all. Only the whitespace PostgreSQL itself skips between schema
    /// names (space, tab, line feed, carriage return, form feed) is ignored; anything else must match. Other Unicode
    /// spaces, such as U+00A0, are part of a schema name to PostgreSQL, so a search_path that holds one is refused.
    /// </summary>
    internal static void EnsureSearchPathPinned(string? observedSearchPath, string serverName)
    {
        /* The rule is shared with the kill connection (PostgresSearchPathPin.IsPinned). */
        if (PostgresSearchPathPin.IsPinned(observedSearchPath))
        {
            return;
        }

        throw new InvalidOperationException(
            $"precondition: the monitoring session on PostgreSQL target '{serverName}' has search_path "
            + $"'{CleanServerText(observedSearchPath, MaxSearchPathLength)}', not the 'pg_catalog, pg_temp' this service sends when it signs in, so nothing "
            + "is collected from it. The usual cause is a connection pooler that drops the startup 'options' "
            + "parameter, as PgBouncer does when 'options' is listed in ignore_startup_parameters. Monitoring needs a "
            + "direct connection to the PostgreSQL server: point the target at the server's own host and port.");
    }

    /// <summary>#6143: the characters PostgreSQL's identifier-list parser skips (scanner_isspace).</summary>
    internal static bool IsPgListSpace(char c) => PostgresSearchPathPin.IsPgListSpace(c);

    /// <summary>
    /// #6143: an unpooled data source over <paramref name="connectionString"/> that loads no types, so a session opened
    /// from it runs no statement but the caller's own. Npgsql otherwise reads pg_type on the first open, naming its
    /// relations and operators without a schema.
    /// </summary>
    internal static NpgsqlDataSource OpenWithoutTypeLoading(string connectionString)
    {
        var builder = new NpgsqlDataSourceBuilder(connectionString);
        builder.ConnectionStringBuilder.Pooling = false;
        builder.ConfigureTypeLoading(typeLoading => typeLoading.EnableTypeLoading(false));
        return builder.Build();
    }

    /// <summary>The most of the observed search_path a message quotes (#6143).</summary>
    internal const int MaxSearchPathLength = 200;

    /// <summary>The most of a pooler's sign-in error a message quotes (#6143).</summary>
    internal const int MaxPoolerTextLength = 300;

    /// <summary>
    /// #6143: text the monitored server or a pooler sent, made fit for a log line and a precondition message: control
    /// and line-break characters become a space, characters that hide or reorder text are dropped, and the result is
    /// capped at <paramref name="maxLength"/>.
    /// </summary>
    internal static string CleanServerText(string? value, int maxLength) =>
        CustomAlertEvaluator.SanitizeDisplayText(value, maxLength);

    /// <summary>
    /// #6143: PgBouncer refuses a startup parameter it does not know with SQLSTATE 08P01 and "unsupported startup
    /// parameter: options" (PgBouncer's own text, never translated). PostgreSQL itself accepts 'options', so this
    /// error at sign-in means a pooler is in the way.
    /// </summary>
    internal static bool IsStartupParameterRejection(PostgresException ex)
    {
        ArgumentNullException.ThrowIfNull(ex);
        return string.Equals(ex.SqlState, PostgresErrorCodes.ProtocolViolation, StringComparison.Ordinal)
            && ex.MessageText.Contains("startup parameter", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>#6143: the message for a PostgreSQL target reached through a connection pooler.</summary>
    internal static InvalidOperationException PoolerPrecondition(string serverName, string detail, Exception? inner = null) =>
        new(
            $"precondition: PostgreSQL target '{serverName}' is reached through a connection pooler ({detail}). "
            + "Monitoring needs a direct connection to the PostgreSQL server, because a pooler cannot carry the "
            + "search_path setting this service sends when it signs in. Point the target at the server's own host "
            + "and port; on AWS, use the instance or cluster endpoint, not the RDS Proxy endpoint.",
            inner);

    /// <summary>
    /// The Aurora probe (#2340), a SEPARATE statement because it decides by CALLING the marker function
    /// rather than looking it up in a catalog — and that distinction is the whole bug it fixes.
    ///
    /// <para>This used to be a column on the detection query above:
    /// <c>(SELECT count(*) FROM pg_proc WHERE proname = 'aurora_version') &gt; 0</c>. Measured against a live
    /// Aurora PostgreSQL 17.7 cluster as a <c>pg_monitor</c>-only role: that lookup returns <b>0</b> while
    /// <c>SELECT aurora_version()</c> returns <c>17.7.2</c>. So a genuine Aurora target read as stock
    /// PostgreSQL, and because both <see cref="PerformanceMonitor.Collectors.PgWaitStatsCollector"/> and
    /// <see cref="PerformanceMonitor.Collectors.PgStatementStatsCollector"/> gate on
    /// <c>IsAurora</c>, ONE wrong boolean silently dropped the two most valuable PostgreSQL reads — with a
    /// healthy-looking log line and a pre-flight that just printed a smaller collector count.</para>
    ///
    /// <para>Existence-by-catalog-lookup and callability are different questions, and the collectors care
    /// about the second one. Its own statement because that is what lets a stock-PostgreSQL
    /// <c>42883 undefined_function</c> be caught and read as "not Aurora" instead of failing the whole
    /// probe — the wrapping the old column comment claimed but a catalog subquery never actually needed.</para>
    /// </summary>
    /// <para>#6143: <c>{0}</c> is the schema, quoted, from <see cref="ResolveAuroraFunctionSchemaAsync"/>. For
    /// <c>aurora_version</c> that is always pg_catalog: the pg_stat_statements extension does not ship it, so a function
    /// of that name in any other schema is never called. Whether the target is Aurora is still decided by the call.</para>
    /// <para>#6888: the call runs inside a DO block that catches <c>undefined_function</c>, and hands the answer back
    /// through a transaction-local setting read by the second statement. PostgreSQL writes a failed statement to the
    /// server's own log, so the bare call put <c>function pg_catalog.aurora_version() does not exist</c> into every
    /// stock target's log on every connect, where a DBA read it as an error from the monitor. An error that PL/pgSQL
    /// catches is not written to the log. The setting is <c>off</c> unless the call returned a value, so the answer is
    /// the same one the bare call gave. Read-only (set_config with is_local touches no catalog), and plpgsql is a
    /// trusted language every role may run a DO block in.</para>
    public const string PostgresAuroraProbeQueryText = @"DO $aurora_probe$
BEGIN
    PERFORM pg_catalog.set_config('performance_monitor.is_aurora', 'off', true);
    BEGIN
        IF {0}.aurora_version() IS NOT NULL THEN
            PERFORM pg_catalog.set_config('performance_monitor.is_aurora', 'on', true);
        END IF;
    EXCEPTION WHEN undefined_function THEN
        NULL;
    END;
END
$aurora_probe$;
SELECT pg_catalog.current_setting('performance_monitor.is_aurora', true)";

    /// <summary>
    /// Whether this target is Aurora, decided by CALLING <c>aurora_version()</c> (#2340). True when the call
    /// succeeds; false when it raises — <c>42883 undefined_function</c> is stock PostgreSQL's answer and is
    /// the expected negative, so it is caught rather than propagated.
    ///
    /// <para>Any other error is also caught and read as "not Aurora", deliberately: this probe decides which
    /// OPTIONAL collectors apply, and a target that answers the version/recovery questions but trips over
    /// this one must still be monitored for everything else rather than failing to connect. The direction
    /// matters and is the pre-#2340 behaviour anyway — the difference is that a real Aurora cluster now
    /// answers true.</para>
    ///
    /// <para>Logged at debug on failure rather than swallowed silently, so "why is this Aurora cluster
    /// reading as stock PostgreSQL" is answerable from the service log instead of requiring a live psql
    /// session against the target, which is what diagnosing #2340 actually took.</para>
    /// </summary>
    internal static async Task<bool> ProbeAuroraAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken, ILogger? logger = null)
    {
        try
        {
            var schema = await ResolveAuroraFunctionSchemaAsync(connection, "aurora_version", cancellationToken);
            using var command = new NpgsqlCommand(
                PostgresAuroraProbeQueryText.Replace("{0}", schema, StringComparison.Ordinal), connection) { CommandTimeout = 15 };
            /* Both statements go in one batch, so they share one implicit transaction and the is_local setting the DO
               block wrote is still there for the SELECT. The DO block returns no rows: the answer is the SELECT's.
               That holds only because Npgsql sends both statements before ONE Sync, and the server runs everything up
               to a Sync in one implicit transaction, so set_config(..., true) persists from the DO block to the SELECT.
               Two NpgsqlCommands, or anything else that ends the transaction between them, would make current_setting
               return NULL here with no error, and every Aurora cluster would silently read as stock PostgreSQL (#2340:
               pg_wait_stats and pg_statement_stats dropped). Keep them one command. */
            string? answer = null;
            using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                do
                {
                    if (await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0))
                    {
                        answer = reader.GetString(0);
                    }
                }
                while (await reader.NextResultAsync(cancellationToken));
            }

            return string.Equals(answer, "on", StringComparison.Ordinal);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "aurora_version() probe did not succeed; treating the target as stock PostgreSQL");
            return false;
        }
    }

    /// <summary>
    /// #6143: the quoted schema to call an Aurora built-in by, from <see cref="DarlingCollectorRunner.PgExtensionSchemasSql"/>
    /// and <see cref="CollectorContext.ChoosePgFunctionSchemas"/>: pg_catalog, or, only for the function the
    /// pg_stat_statements extension ships and only as a member of it, the schema the extension was created in; never
    /// any other. pg_catalog when the catalog shows neither.
    /// </summary>
    internal static async Task<string> ResolveAuroraFunctionSchemaAsync(
        NpgsqlConnection connection, string functionName, CancellationToken cancellationToken)
    {
        string? statementsSchema = null;
        var functions = new List<(string Function, string Schema)>();
        using (var command = new NpgsqlCommand(DarlingCollectorRunner.PgExtensionSchemasSql, connection)
               { CommandTimeout = DarlingCollectorRunner.PgExtensionSchemasTimeoutSeconds })
        {
            command.Parameters.AddWithValue("functions", new[] { functionName });
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.GetString(0) == "function")
                {
                    functions.Add((reader.GetString(1), reader.GetString(2)));
                }
                else if (reader.GetString(1) == "pg_stat_statements")
                {
                    statementsSchema = reader.GetString(2);
                }
            }
        }

        /* #6143: when pg_proc shows nothing (measured on Aurora for a pg_monitor login, #2340), the call goes to
           pg_catalog: aurora_version is not shipped by pg_stat_statements, so no other schema is looked in. */
        var chosen = CollectorContext.ChoosePgFunctionSchemas(functions, statementsSchema);
        return CollectorContext.QuotePgFunctionSchema(chosen, functionName);
    }

    /// <summary>
    /// Whether the <c>pg_wait_sampling</c> extension is created in the database this connection landed in
    /// (#3604) — the connect-time fact that picks <c>PgWaitSamplingCollector</c>'s arm on a stock target:
    /// the extension's own 10 ms profile when present, the service-side <c>pg_stat_activity</c> sampler when
    /// not. <c>pg_extension</c> is per database, and the collector reads <c>pg_wait_sampling_profile</c> from
    /// exactly this database, so this is the fact that decides whether that read can succeed rather than a
    /// proxy for it (<c>pg_available_extensions</c> would say "installable", which is a different question).
    /// </summary>
    public const string PostgresWaitSamplingProbeQueryText =
        @"SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_wait_sampling')";

    /// <summary>
    /// Runs <see cref="PostgresWaitSamplingProbeQueryText"/>. Fails CLOSED to false on any error other than
    /// cancellation, and for the same reason <see cref="ProbeAuroraAsync"/> does: this probe decides which
    /// arm an optional collector takes, and a target that answered every other connect question must still be
    /// monitored. False routes the target to the sampler arm, which reads only <c>pg_stat_activity</c> — the
    /// floor — so the failure direction produces a coarser wait history rather than none.
    /// </summary>
    private static async Task<bool> ProbeWaitSamplingExtensionAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken, ILogger? logger = null)
    {
        try
        {
            using var command = new NpgsqlCommand(PostgresWaitSamplingProbeQueryText, connection) { CommandTimeout = 15 };
            var present = await command.ExecuteScalarAsync(cancellationToken);
            return present is bool b && b;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "pg_wait_sampling extension probe did not succeed; treating the extension as absent (#3604)");
            return false;
        }
    }

    /// <summary>Connects, probes, and returns the runtime state for one configured server.</summary>
    public static async Task<ServerRuntime> ConnectAsync(MonitoredServer config, ILogger? logger, CancellationToken cancellationToken)
    {
        if (config.IsPostgres)
        {
            return await ConnectPostgresAsync(config, logger, cancellationToken);
        }

        var connectionString = ResolveConnectionString(config, logger);
        var storageName = config.StorageName;

        using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = new SqlCommand(DetectionQueryText, connection) { CommandTimeout = 30 };
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        int majorVersion = 0, engineEdition = 0;
        bool isAwsRds = false, hasMsdbAccess = true;
        string? connectedDatabase = null, machineIdentity = null, editionName = null;
        if (await reader.ReadAsync(cancellationToken))
        {
            // Column indices per DetectionQueryText: 1 major_version, 3 engine_edition,
            // 4 is_aws_rds, 5 has_msdb_access (sqlserver_start_time was dropped in #1535).
            majorVersion = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            engineEdition = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
            isAwsRds = !reader.IsDBNull(4) && reader.GetInt32(4) == 1;
            hasMsdbAccess = reader.IsDBNull(5) || reader.GetInt32(5) == 1;
            connectedDatabase = reader.IsDBNull(6) ? null : reader.GetString(6);   /* #2228 */
            /* #6285: the server's own name, for the add and edit tools' same-machine check. An Azure SQL Database has no
               machine of its own to name, so it reports none. */
            machineIdentity = reader.IsDBNull(7) || engineEdition == 5 ? null : reader.GetString(7);
            editionName = reader.IsDBNull(8) ? null : reader.GetString(8);   /* #6893 */
        }

        /* #6893: Amazon RDS refuses every Extended Events session create on the Web and Express editions. */
        var isAwsRdsWebOrExpress = isAwsRds && CollectorEngineCapability.IsWebOrExpressEditionName(editionName);

        return new ServerRuntime
        {
            Config = config,
            CapturePlans = config.CapturePlans,
            ConnectionString = connectionString,
            Target = new CollectorTargetInfo
            {
                IsAzureSqlDb = engineEdition == 5,
                IsAzureManagedInstance = engineEdition == 8,
                IsAwsRds = isAwsRds,
                IsAwsRdsWebOrExpress = isAwsRdsWebOrExpress,
                SqlMajorVersion = majorVersion,
                /* Already probed above via HAS_DBACCESS(N'msdb'); wiring it into the gate is the fix —
                   before this it rode only on ServerRuntime and never reached the collectors' AppliesTo,
                   so Darling attempted running_jobs/job_history/agent_status every cycle on a no-msdb login. */
                HasMsdbAccess = hasMsdbAccess,
            },
            StorageName = storageName,
            /* #2218: the STORED identity, not a fresh hash of storageName. This is the runtime's only
               identity stamp — DarlingCollectorRunner copies it onto every CollectorContext, so every
               collected row keys on whatever this says — which is why it has to read the registry rather
               than re-derive from the connection fields the operator can edit. */
            ServerId = config.ServerId,
            HasMsdbAccess = hasMsdbAccess,
            IsAwsRds = isAwsRds,
            /* #6893: the registry records an RDS Web or Express instance as its own edition, so the capability reads can say why. */
            EngineEdition = isAwsRdsWebOrExpress ? CollectorEngineCapability.AwsRdsWebOrExpressEngineEdition : engineEdition,
            EditionName = editionName,
            ConnectedDatabase = connectedDatabase,
            MachineIdentity = machineIdentity,
        };
    }

    /// <summary>
    /// The PostgreSQL connect-and-probe. Same contract as the SQL Server path — open, probe, return a
    /// <see cref="ServerRuntime"/> whose <see cref="CollectorTargetInfo"/> is what the collectors' gate
    /// reads — with the SQL Server-only facts left at their defaults.
    /// <para><c>HasMsdbAccess</c> stays <c>true</c> and the Azure flags stay <c>false</c> because they are
    /// meaningless here; no Postgres definition consults them, and the engine check in
    /// <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/> keeps every
    /// T-SQL definition away from this target regardless of their values.</para>
    /// </summary>
    private static Task<ServerRuntime> ConnectPostgresAsync(
        MonitoredServer config, ILogger? logger, CancellationToken cancellationToken) =>
        ConnectPostgresAsync(config, ResolveConnectionString(config, logger), logger, cancellationToken);

    /// <summary><see cref="ConnectPostgresAsync(MonitoredServer, ILogger?, CancellationToken)"/> over a connection
    /// string already resolved from <paramref name="config"/>; a test passes one without the pin, as a pooler that
    /// drops the startup options would leave it (#6143).</summary>
    internal static async Task<ServerRuntime> ConnectPostgresAsync(
        MonitoredServer config, string connectionString, ILogger? logger, CancellationToken cancellationToken)
    {
        var storageName = config.StorageName;

        /* #6143: an RDS Proxy endpoint accepts only the startup parameters AWS lists, and 'options' is not one. */
        var host = new NpgsqlConnectionStringBuilder(connectionString).Host;
        if (RdsEndpoint.IsProxyHost(host))
        {
            throw PoolerPrecondition(config.DisplayName, $"'{host}' is an RDS Proxy endpoint");
        }

        /* #6143: no type loading and no pool, so the only statement this session runs before EnsureSearchPathPinned
           is the detection query itself. Npgsql's type-loading query names pg_type and its operators without a schema,
           and on a session whose pin a pooler dropped it would resolve them through the database's own search_path.
           Unpooled, so this session is never handed to a collector either. The detection and probe reads that follow
           use only built-in types. */
        await using var detectionSource = OpenWithoutTypeLoading(connectionString);
        using var connection = detectionSource.CreateConnection();
        try
        {
            await connection.OpenAsync(cancellationToken);
        }
        catch (PostgresException ex) when (IsStartupParameterRejection(ex))
        {
            throw PoolerPrecondition(
                config.DisplayName, $"it refused the sign-in: {CleanServerText(ex.MessageText, MaxPoolerTextLength)}", ex);
        }

        using var command = new NpgsqlCommand(PostgresDetectionQueryText, connection) { CommandTimeout = 30 };
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        int majorVersion = 0, versionNum = 0;
        bool isInRecovery = false;
        string versionText = "";
        string? connectedDatabase = null;
        var statementsExtension = PgExtensionRowObservation.NotObserved;
        string? observedSearchPath = null;
        if (await reader.ReadAsync(cancellationToken))
        {
            versionText = reader.IsDBNull(0) ? "" : reader.GetString(0);
            majorVersion = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            isInRecovery = !reader.IsDBNull(2) && reader.GetBoolean(2);
            versionNum = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
            connectedDatabase = reader.IsDBNull(4) ? null : reader.GetString(4);   /* #2228 */

            /* #3830: the row, observed. A NULL scalar is "pg_extension has no row for it", which is a
               different answer from "nothing read it" - so the observation is built on the row being
               present in the result, never inferred from the value being null. */
            statementsExtension = PgExtensionRowObservation.From(
                reader.IsDBNull(5) ? null : reader.GetString(5), connectedDatabase);
            observedSearchPath = reader.IsDBNull(6) ? null : reader.GetString(6);   /* #6143 */
        }

        /* The reader must be closed before another command runs on this connection. */
        await reader.CloseAsync();

        /* #6143: before any other read, and before the runtime exists, so no collector runs on this session. */
        EnsureSearchPathPinned(observedSearchPath, config.DisplayName);

        var isAurora = await ProbeAuroraAsync(connection, cancellationToken, logger);

        /* #6339: which cluster this is, for the add and edit tools' same-machine check. After the facts the collectors depend on,
           and on its own statement, so a role that may not read it costs nothing but the identity. */
        var machineIdentity = await ProbePostgresIdentityAsync(connection, logger, cancellationToken, isAurora);

        /* #3604: the wait-tier decision's second fact. Probed AFTER Aurora-ness and independently of it —
           an Aurora cluster cannot load the module, so the answer there is false by construction, but the
           gate reads the two facts separately and the sweep varies them separately. */
        var hasWaitSampling = await ProbeWaitSamplingExtensionAsync(connection, cancellationToken, logger);

        logger?.LogInformation(
            "Connected to PostgreSQL target '{Server}': major {Major} (server_version_num {Num}), {Role}, Aurora: {Aurora}, pg_wait_sampling: {WaitSampling}, pg_stat_statements: {StatementsExtension} — {VersionText}",
            config.DisplayName, majorVersion, versionNum, isInRecovery ? "reader (in recovery)" : "writer", isAurora,
            hasWaitSampling ? "present (extension_sampled tier)" : isAurora ? "n/a (engine_cumulative tier)" : "absent (service_sampled tier)",
            statementsExtension.ExtensionVersion is { } extversion
                ? "extension " + extversion
                : "no pg_extension row in this database",
            versionText);

        /* A Postgres target reached through the SQL Server path would have failed on the detection
           query, so an engine mismatch is loud. The reverse — a SQL Server host configured as
           "postgres" — fails at connect, which is equally loud. */
        return new ServerRuntime
        {
            Config = config,
            CapturePlans = config.CapturePlans,
            ConnectionString = connectionString,
            Target = new CollectorTargetInfo
            {
                Engine = CollectorTargetEngine.PostgreSql,
                PostgresMajorVersion = majorVersion,
                PostgresVersionNum = versionNum,
                IsAurora = isAurora,
                /* #2633: derived from the ENDPOINT, because nothing else here can see it. IsAwsRds is
                   probed with a T-SQL detection query on the SQL Server path, so before this it was
                   silently false for every PostgreSQL target — which made the second half of
                   pg_plan_capture's `IsAurora || IsAwsRds` dispatch unreachable and sent plain RDS
                   PostgreSQL down the pg_read_file route, where a managed instance has no filesystem to
                   read and the failure names a grant that would never have helped. Aurora was unaffected
                   because IsAurora carries it, which is why the fleet never showed this. */
                IsAwsRds = RdsEndpoint.TryParse(
                    new NpgsqlConnectionStringBuilder(connectionString).Host) is not null,
                IsInRecovery = isInRecovery,
                /* #3604: which stock-PostgreSQL wait instrument this target gets, decided here once. */
                HasPgWaitSamplingExtension = hasWaitSampling,
            },
            StorageName = storageName,
            ServerId = config.ServerId,
            ConnectedDatabase = connectedDatabase,
            MachineIdentity = machineIdentity,
            /* #3830: on the runtime rather than on Target, because nothing dispatches on it - it feeds one
               fault sentence - and CollectorTargetInfo is the type whose facts the engine-capability sweep
               has to vary or fix. */
            PgStatStatementsExtension = statementsExtension,
        };
    }

    /// <summary>
    /// #6339: what identifies the PostgreSQL cluster this session reached, as <c>system_identifier</c>, a colon and the port the
    /// server listens on (<c>pg_control_system()</c> and the <c>port</c> setting), so one cluster reached under two host names reads
    /// as one machine while two clusters on a host (a different identifier, or a different port) do not. Null when the role may not
    /// call <c>pg_control_system()</c> (a managed service can refuse it): the caller then matches by host text alone. Never throws
    /// except for cancellation, and never fails the connect. Standalone statement outside any transaction, so a refusal leaves the
    /// session usable. Streaming replicas of one primary share its identifier and usually its port, so they read as one cluster.
    /// </summary>
    internal const string PostgresIdentityQueryText = @"
SELECT s.system_identifier::text AS system_identifier,
       pg_catalog.current_setting('port') AS port
FROM pg_catalog.pg_control_system() AS s";

    /// <summary>
    /// #6888: asked first, on its own round trip, whether this role may call <c>pg_control_system()</c>. PostgreSQL checks a
    /// function's privilege when the statement starts, so a <c>CASE</c> or <c>WHERE</c> around the call in <see cref="PostgresIdentityQueryText"/>
    /// would still raise 42501 on a role that is refused, and the server would write <c>permission denied for function
    /// pg_control_system</c> into its own log on every connect. <c>has_function_privilege</c> is callable by any role and
    /// <c>to_regprocedure</c> makes a missing function read as NULL, so the answer is false either way.
    /// </summary>
    internal const string PostgresIdentityPrivilegeQueryText =
        "SELECT COALESCE(pg_catalog.has_function_privilege(current_user, "
        + "pg_catalog.to_regprocedure('pg_catalog.pg_control_system()'), 'EXECUTE'), false)";

    internal static async Task<string?> ProbePostgresIdentityAsync(NpgsqlConnection connection, ILogger? logger, CancellationToken cancellationToken, bool isAurora = false)
    {
        /* #6340 round 2 (N4): none on Aurora. No AWS documentation shows system_identifier to be unique per Aurora cluster (the control
           file is virtual there, and one report has RDS instances sharing one), so a value that two clusters could share would only
           make the add and edit tools refuse unrelated clusters. The host-text match decides there. */
        if (isAurora)
        {
            return null;
        }

        try
        {
            /* #6888: a refused role is told apart BEFORE the call, so the server never raises (and logs) 42501 for it. */
            using (var privilege = new NpgsqlCommand(PostgresIdentityPrivilegeQueryText, connection) { CommandTimeout = 30 })
            {
                if (await privilege.ExecuteScalarAsync(cancellationToken) is not true)
                {
                    logger?.LogDebug("pg_control_system() is not callable by this role; the target is matched by host text alone");
                    return null;
                }
            }

            using var command = new NpgsqlCommand(PostgresIdentityQueryText, connection) { CommandTimeout = 30 };
            using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken) && !reader.IsDBNull(0) && !reader.IsDBNull(1))
            {
                return reader.GetString(0) + ":" + reader.GetString(1);
            }

            return null;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger?.LogDebug("pg_control_system() is not readable on this target ({Message}); it is matched by host text alone", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// Non-throwing connect-and-probe: runs <see cref="ConnectAsync"/> and packages the outcome as a
    /// <see cref="ConnectionProbeResult"/> — success carries the probed version/edition/engine facts, a
    /// failure carries the error message (never plaintext credentials). Shared by the <c>test_connect</c>
    /// command (the Stage-3 Add-dialog validates a server BEFORE saving; the SERVICE holds the network
    /// path + credentials) and the <c>--test-connection</c>/<c>--validate-config</c> CLI verb, so both
    /// classify a server identically. <see cref="OperationCanceledException"/> propagates (shutdown).
    /// </summary>
    public static async Task<ConnectionProbeResult> ProbeAsync(MonitoredServer config, ILogger? logger, CancellationToken cancellationToken, bool runsAsConsoleUser = false)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        try
        {
            var runtime = await ConnectAsync(config, logger, cancellationToken);
            var isPostgres = runtime.Target.Engine == CollectorTargetEngine.PostgreSql;
            /* #6254: a server with a Query Store replica is tested on both connections, so a replica that cannot be reached shows now
               and not as a collection that quietly reads the primary. Its failure is a warning: the primary answered. */
            var (replicaHost, replicaError) = await ProbeQueryStoreReplicaAsync(config, runtime.Target.IsAzureSqlDb, logger, cancellationToken);
            /* #6459: an Azure SQL Database has no readable-replica Query Store read, so the setting is saved and never used. The
               replica is not opened (nothing to test), and the result says so, so the operator is told at save time and not left
               to notice that read_source never fills in. */
            var ignoredReplica = IgnoredQueryStoreReplica(config, runtime.Target.IsAzureSqlDb);
            var replicaIgnored = ignoredReplica is not null;
            replicaHost ??= ignoredReplica;

            /* #6472: the base grants the connecting login lacks, each with the statement that adds it. A warning only: the connection worked. */
            /* The CLI runs this in its own process: with Windows authentication the connecting login is the console user, not the account
               the service collects as, so a grant check would name the wrong principal. It says nothing then; the service's own test
               (test_connect) checks the right one. */
            var checksTheWrongPrincipal = runsAsConsoleUser && !config.UsesSqlAuth && !config.UsesEntra;
            var missingGrants = isPostgres || checksTheWrongPrincipal
                ? null
                : await ProbeMissingGrantsAsync(runtime.ConnectionString, runtime.Target.IsAzureSqlDb, logger, cancellationToken, isAwsRds: runtime.Target.IsAwsRds);

            return new ConnectionProbeResult(
                Success: true,
                MajorVersion: runtime.Target.SqlMajorVersion,
                EngineEdition: runtime.EngineEdition,
                /* No edition on a PostgreSQL target, and DescribeEngineEdition(0) would say
                   "Unknown (0)" — which reads as a probe that half-failed rather than one that
                   succeeded against a different engine. */
                EngineEditionDescription: isPostgres ? null : DescribeEditionForProbe(runtime.EngineEdition, runtime.EditionName),
                IsAzureSqlDb: runtime.Target.IsAzureSqlDb,
                IsAzureManagedInstance: runtime.Target.IsAzureManagedInstance,
                IsAwsRds: runtime.IsAwsRds,
                HasMsdbAccess: runtime.HasMsdbAccess,
                Error: null,
                ConnectedDatabase: runtime.ConnectedDatabase,
                MachineIdentity: runtime.MachineIdentity,
                Engine: runtime.Target.Engine,
                PostgresMajorVersion: runtime.Target.PostgresMajorVersion,
                PostgresVersionNum: runtime.Target.PostgresVersionNum,
                IsAurora: runtime.Target.IsAurora,
                IsInRecovery: runtime.Target.IsInRecovery,
                HasPgWaitSamplingExtension: runtime.Target.HasPgWaitSamplingExtension,
                QueryStoreReplica: replicaHost,
                QueryStoreReplicaError: replicaError,
                QueryStoreReplicaIgnoredOnAzureSqlDb: replicaIgnored,
                MissingGrantsNote: missingGrants);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            return new ConnectionProbeResult(
                Success: false,
                MajorVersion: 0,
                EngineEdition: 0,
                EngineEditionDescription: null,
                IsAzureSqlDb: false,
                IsAzureManagedInstance: false,
                IsAwsRds: false,
                HasMsdbAccess: false,
                /* #6183: SqlClient's native library failing to load reads "The type initializer for ... TdsParser
                   threw an exception" and says nothing about why. One shared explanation, for every caller. */
                Error: SqlClientNativeLoadFailure.Describe(ex, AppContext.BaseDirectory) ?? ex.Message,
                SqlClientNativeLoadFailed: SqlClientNativeLoadFailure.IsMatch(ex));
        }
    }

    /// <summary>
    /// #6472: checks the required base grants of <see cref="PerformanceMonitor.Darling.Storage.MonitoringGrantScript"/> on the login the connection test connected with
    /// (<c>HAS_PERMS_BY_NAME</c>: the server grants, or the database grants on an Azure SQL Database) and returns the sentence that
    /// names each missing one with the statement that adds it, or null when none is missing. It never throws except for cancellation and
    /// says nothing when it cannot read the answer (an old version that does not know a permission name answers NULL, which is no claim):
    /// the connection worked, and a missing grant is a warning beside that result, not a failure of it.
    /// <paramref name="createConnection"/> is a test seam; null opens a SqlConnection. <paramref name="isAwsRds"/> leaves out the grants RDS refuses (#6889).
    /// </summary>
    internal static async Task<string?> ProbeMissingGrantsAsync(
        string connectionString, bool isAzureSqlDatabase, ILogger? logger, CancellationToken cancellationToken,
        Func<string, System.Data.Common.DbConnection>? createConnection = null,
        bool isAwsRds = false)
    {
        /* #6889: Amazon RDS refuses CONNECT ANY DATABASE to its master user, so asking for it there names a statement that cannot succeed. */
        var entries = isAzureSqlDatabase
            ? PerformanceMonitor.Darling.Storage.MonitoringGrantScript.RequiredAzureGrants
            : isAwsRds
                ? PerformanceMonitor.Darling.Storage.MonitoringGrantScript.RequiredRdsServerGrants
                : PerformanceMonitor.Darling.Storage.MonitoringGrantScript.RequiredServerGrants;
        try
        {
            await using var connection = createConnection is null ? new SqlConnection(connectionString) : createConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = PerformanceMonitor.Darling.Storage.MonitoringGrantScript.BuildPermissionCheckSql(entries);
            command.CommandTimeout = 15;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            var held = new List<int?>();
            for (var i = 0; i < entries.Count; i++)
            {
                held.Add(reader.IsDBNull(i) ? null : Convert.ToInt32(reader.GetValue(i), System.Globalization.CultureInfo.InvariantCulture));
            }

            var who = reader.IsDBNull(entries.Count) ? PerformanceMonitor.Darling.Storage.MonitoringGrantScript.PlaceholderLogin : reader.GetString(entries.Count);
            var missingText = PerformanceMonitor.Darling.Storage.MonitoringGrantScript.DescribeMissingGrants(PerformanceMonitor.Darling.Storage.MonitoringGrantScript.MissingGrants(entries, held, who));

            /* #6889: the count of online user databases the login cannot enter rides after the login name (server scope only). */
            long noUserDatabases = 0;
            if (reader.FieldCount > entries.Count + 1 && !reader.IsDBNull(entries.Count + 1))
            {
                noUserDatabases = Convert.ToInt64(reader.GetValue(entries.Count + 1), System.Globalization.CultureInfo.InvariantCulture);
            }

            var noUserText = PerformanceMonitor.Darling.Storage.MonitoringGrantScript.DescribeNoUserDatabases(noUserDatabases, who, isAwsRds);
            return missingText is null ? noUserText : noUserText is null ? missingText : missingText + " " + noUserText;
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger?.LogDebug("The grant check did not run ({Message}); the connection result stands without it", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// #6254: opens the server's Query Store replica connection the way collection opens it: the string comes from
    /// <see cref="ResolveQueryStoreReplicaConnectionString"/> (the same builder: the replica host, <c>ApplicationIntent=ReadOnly</c>,
    /// the server's login and authentication) and the connection is the SQL Server target provider's own <see cref="SqlConnection"/>.
    /// Returns <c>(null, null)</c> for a server with no replica, a PostgreSQL target or an Azure SQL Database, <c>(host, null)</c> when the
    /// replica opened, and <c>(host, error)</c> when it did not (a refused login, an unreachable host, a password the builder would not
    /// release). Never throws except for cancellation. <paramref name="createConnection"/> is a test seam; null opens a SqlConnection.
    /// </summary>
    internal static async Task<(string? Host, string? Error)> ProbeQueryStoreReplicaAsync(
        MonitoredServer config, bool isAzureSqlDatabase, ILogger? logger, CancellationToken cancellationToken,
        Func<string, System.Data.Common.DbConnection>? createConnection = null)
    {
        if (config.IsPostgres || isAzureSqlDatabase
            || PerformanceMonitor.Darling.Storage.QueryStoreReplicaSetting.Normalize(config.ConnectionIdentity.QueryStoreReplica) is not { } host)
        {
            return (null, null);
        }

        try
        {
            var connectionString = ResolveQueryStoreReplicaConnectionString(config, isAzureSqlDatabase, logger);
            if (connectionString is null)
            {
                return (null, null);
            }

            await using var connection = createConnection is null ? new SqlConnection(connectionString) : createConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            return (host, null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return (host, SqlClientNativeLoadFailure.Describe(ex, AppContext.BaseDirectory) ?? ex.Message);
        }
    }

    /// <summary>
    /// #6459: the Query Store replica a SQL Server target has set and an Azure SQL Database will never read, or null (no replica set,
    /// a PostgreSQL target, or a target that is not an Azure SQL Database). The one place the "set but unusable" rule is decided, so
    /// the connection test and its callers cannot disagree about it (<see cref="ProbeQueryStoreReplicaAsync"/> is its twin: the
    /// same targets, opened).
    /// </summary>
    internal static string? IgnoredQueryStoreReplica(MonitoredServer config, bool isAzureSqlDatabase) =>
        isAzureSqlDatabase && !config.IsPostgres
            ? PerformanceMonitor.Darling.Storage.QueryStoreReplicaSetting.Normalize(config.ConnectionIdentity.QueryStoreReplica)
            : null;

    /// <summary>
    /// #6254: the one line a connection test says about the Query Store replica, or null when the probed server has none.
    /// <paramref name="revealError"/> says whether the driver's text may be shown (the CLI and the Viewer, the operator's own
    /// surfaces, as for the primary); a caller that must not echo it (MCP and web replies) gets a fixed sentence and logs the text.
    /// A failing replica is a warning: collection reads the primary, and the line says so.
    /// </summary>
    public static string? DescribeQueryStoreReplica(ConnectionProbeResult probe, bool revealError = true)
    {
        ArgumentNullException.ThrowIfNull(probe);
        if (string.IsNullOrEmpty(probe.QueryStoreReplica))
        {
            return null;
        }

        if (probe.QueryStoreReplicaIgnoredOnAzureSqlDb)
        {
            return $"Query Store replica {probe.QueryStoreReplica}: not used. Azure SQL Database has no readable-replica Query Store read, " +
                   "so this setting has no effect and collection reads Query Store from the database itself. Remove query_store_replica.";
        }

        if (string.IsNullOrEmpty(probe.QueryStoreReplicaError))
        {
            return $"Query Store replica {probe.QueryStoreReplica}: connected.";
        }

        return revealError
            ? $"Query Store replica {probe.QueryStoreReplica}: could not connect ({probe.QueryStoreReplicaError}). Collection reads Query Store from the primary until it can."
            : $"Query Store replica {probe.QueryStoreReplica}: could not connect. Collection reads Query Store from the primary until it can. The service log has the details.";
    }

    /// <summary>
    /// The probed facts for a REACHABLE target, as one clause — shared by the <c>--test-connection</c>
    /// PASS line (<c>DarlingCliCommands.FormatProbeLine</c>) and the <c>add_servers</c> MCP tool's detail
    /// text, which previously each formatted their own and could drift.
    /// <para>The engine decides what is worth saying. A SQL Server target reports version, edition and
    /// msdb access, because msdb access gates three collectors — except Azure SQL Database, which has no
    /// SQL Agent and no real msdb surface, so either msdb clause would claim a capability the platform
    /// cannot have. Edition 5 reports the Agent surface as not applicable instead, matching the dispatch
    /// gate that already keeps the Agent family off those targets (#3237). A PostgreSQL target has none of those,
    /// so it reports version, writer-vs-reader, Aurora-vs-not — and then the number that actually
    /// answers "will this target give me what I expect", which is how many of the PostgreSQL collectors
    /// clear the gate. A stock-PostgreSQL reader clears three of seven, and finding that out at
    /// pre-flight is the point of the verb.</para>
    /// </summary>
    public static string DescribeProbeFacts(ConnectionProbeResult probe)
    {
        ArgumentNullException.ThrowIfNull(probe);

        if (probe.Engine != CollectorTargetEngine.PostgreSql)
        {
            var edition = string.IsNullOrEmpty(probe.EngineEditionDescription)
                ? DescribeEngineEdition(probe.EngineEdition)
                : probe.EngineEditionDescription;
            var agentClause = probe.IsAzureSqlDb
                ? "Agent surface: not applicable (Azure SQL Database)"
                : probe.HasMsdbAccess
                    ? "msdb access: yes"
                    : "msdb access: NO (failed-job alerts unavailable)";
            return $"SQL major version {probe.MajorVersion}, {edition}, {agentClause}";
        }

        var target = probe.ToTargetInfo();
        var postgresDefinitions = CollectorCatalog.All
            .Where(d => d.TargetEngine == CollectorTargetEngine.PostgreSql)
            .ToList();
        var skipped = postgresDefinitions
            .Where(d => !CollectorCatalog.AppliesTo(d, target))
            .Select(d => d.Name)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToList();

        var role = probe.IsInRecovery ? "reader (in recovery)" : "writer";
        var flavour = probe.IsAurora ? "Aurora" : "not Aurora";
        var applies = skipped.Count == 0
            ? $"all {postgresDefinitions.Count} PostgreSQL collectors apply"
            : $"{postgresDefinitions.Count - skipped.Count} of {postgresDefinitions.Count} PostgreSQL collectors apply " +
              $"(skipped: {string.Join(", ", skipped)})";

        return $"PostgreSQL {probe.PostgresMajorVersion} (server_version_num {probe.PostgresVersionNum}), " +
            $"{role}, {flavour} — {applies}";
    }

    /// <summary>Human-readable SERVERPROPERTY('EngineEdition') description for the probe result.
    /// <para>Delegates to <see cref="CollectorEngineCapability.DescribeEngineEdition"/> (#2511) rather than
    /// keeping a second switch: the capability messages both MCP surfaces return name the edition, and two
    /// edition tables in one repo drift — with the copy nobody is reading being the one that drifts.</para>
    /// </summary>
    public static string DescribeEngineEdition(int engineEdition) =>
        CollectorEngineCapability.DescribeEngineEdition(engineEdition);

    /// <summary>
    /// #6893: the edition the add-server result line names. EngineEdition 2 is Standard AND Web, so a box that says <c>Web Edition</c> or
    /// <c>Express Edition</c> in <c>SERVERPROPERTY('Edition')</c> is named from that text. Anything else keeps the engine edition's description.
    /// </summary>
    public static string DescribeEditionForProbe(int engineEdition, string? editionName)
    {
        if (!string.IsNullOrWhiteSpace(editionName))
        {
            var trimmed = editionName.TrimStart();
            if (trimmed.StartsWith("Web Edition", StringComparison.OrdinalIgnoreCase))
            {
                return "Web";
            }

            if (trimmed.StartsWith("Express Edition", StringComparison.OrdinalIgnoreCase))
            {
                return "Express";
            }
        }

        return DescribeEngineEdition(engineEdition);
    }
}

/// <summary>
/// The outcome of a connect-and-probe attempt (<see cref="DarlingServerConnector.ProbeAsync"/>): the
/// success flag plus the probed target facts, or the error message on failure. Deliberately carries NO
/// credentials so it is safe to serialize into <c>config_command.result_json</c> and print from the CLI.
/// <para>The SQL Server facts come first because they came first; the PostgreSQL ones are trailing
/// optional parameters so every existing construction site — including the tests — keeps compiling and
/// keeps meaning "a SQL Server target". <see cref="Engine"/> is what a reader should branch on: on a
/// PostgreSQL target <see cref="MajorVersion"/> and <see cref="EngineEdition"/> are 0 and
/// <see cref="HasMsdbAccess"/> is meaningless, so reporting them would be worse than silence.</para>
/// </summary>
public sealed record ConnectionProbeResult(
    bool Success,
    int MajorVersion,
    int EngineEdition,
    string? EngineEditionDescription,
    bool IsAzureSqlDb,
    bool IsAzureManagedInstance,
    bool IsAwsRds,
    bool HasMsdbAccess,
    string? Error,
    CollectorTargetEngine Engine = CollectorTargetEngine.SqlServer,
    int PostgresMajorVersion = 0,
    int PostgresVersionNum = 0,
    bool IsAurora = false,
    bool IsInRecovery = false,
    /* #2280: the database the connection ACTUALLY reached, so a registration-time collision check can compare
       what the SERVER says against what other registrations claim, rather than comparing two claims. Trailing
       and defaulted, so every existing construction of this record still compiles unchanged. */
    string? ConnectedDatabase = null,
    /* #3604: the wait-tier fact, so --test-connection's "which collectors would run" count is computed
       against the same shape the gate reads. Trailing and defaulted for the #2280 reason. */
    bool HasPgWaitSamplingExtension = false,
    /* #6183: the failure was SqlClient's native library not loading. No SQL Server connection can work from this
       install, so a caller that probes several servers says so once instead of once per server. Trailing and
       defaulted, like the fields above. */
    bool SqlClientNativeLoadFailed = false,
    /* #6285: the machine and instance name the SQL Server reached calls itself (SERVERPROPERTY('ServerName')), read on this
       same connection, so add_servers and edit_server can tell one machine reached under two names. Null when the probe
       read none: a failed connection, an Azure SQL Database, or a PostgreSQL role that may not read the cluster identity (#6339). Trailing and defaulted, like the fields above. */
    string? MachineIdentity = null,
    /* #6254: the Query Store replica host the test also opened, and the error when it would not open. Null host: the server has no
       replica (or is a PostgreSQL target or an Azure SQL Database). A set error leaves Success alone, because collection reads
       the primary when the replica cannot be reached. Trailing and defaulted, like the fields above. */
    string? QueryStoreReplica = null,
    string? QueryStoreReplicaError = null,
    /* #6459: the server has a Query Store replica set and is an Azure SQL Database, which has no readable-replica Query Store read:
       the setting is saved and never used, the replica was not opened, and the note says so. Trailing and defaulted. */
    bool QueryStoreReplicaIgnoredOnAzureSqlDb = false,
    /* #6472: the sentence naming each missing base grant with the statement that adds it, or null when none is missing (or the check could
       not read). A warning beside a successful connection. Trailing and defaulted, like the fields above. */
    string? MissingGrantsNote = null)
{
    /// <summary>
    /// Rebuilds the gate's-eye view of this target, so a caller can ask which collectors would actually
    /// run against it. These are the same fields <see cref="CollectorCatalog.AppliesTo(ICollectorSchemaInfo, CollectorTargetInfo)"/>
    /// reads, which is why a count derived from this is a real answer and not an estimate.
    /// </summary>
    public CollectorTargetInfo ToTargetInfo() => new()
    {
        Engine = Engine,
        IsAzureSqlDb = IsAzureSqlDb,
        IsAzureManagedInstance = IsAzureManagedInstance,
        IsAwsRds = IsAwsRds,
        SqlMajorVersion = MajorVersion,
        HasMsdbAccess = HasMsdbAccess,
        PostgresMajorVersion = PostgresMajorVersion,
        PostgresVersionNum = PostgresVersionNum,
        IsAurora = IsAurora,
        IsInRecovery = IsInRecovery,
        HasPgWaitSamplingExtension = HasPgWaitSamplingExtension,
    };
}
