/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
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

    public bool HasMsdbAccess { get; init; }

    public bool IsAwsRds { get; init; }

    /// <summary>
    /// The raw SERVERPROPERTY('EngineEdition') value from the detection probe — 1 Personal,
    /// 2 Standard, 3 Enterprise, 4 Express, 5 Azure SQL DB, 8 Managed Instance, etc. — carried
    /// whole so the servers registry records the real edition, not just the 5/8 classification
    /// booleans on <see cref="Target"/>.
    /// </summary>
    public int EngineEdition { get; init; }
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
       3 engine_edition, 4 is_aws_rds, 5 has_msdb_access, 6 connected_database (#2228).

       #2228 — connected_database is DB_NAME(): the database this connection ACTUALLY reached, which is not
       necessarily the one the registration names. An Initial Catalog that is absent, misspelled or overridden
       lands somewhere else, and every collected row is then stored under this registration's identity while
       describing a different database. DB_NAME() keeps this query's no-permission property: it needs no DMV,
       so it does not reintroduce the VIEW DATABASE STATE dependency #1535 removed. APPENDED, because every
       read above is positional and inserting a column mid-list shifts five other fields onto wrong values.

       Comments inside these probe strings stay to one short line each: the text is sent to the monitored
       server on every connect, so the reasoning belongs here rather than on the wire. */
    public const string DetectionQueryText = @"
SELECT
    @@VERSION AS sql_version,
    CONVERT(integer, SERVERPROPERTY('ProductMajorVersion')) AS major_version,
    DATEDIFF(MINUTE, GETUTCDATE(), GETDATE()) AS utc_offset_minutes,
    CONVERT(integer, SERVERPROPERTY('EngineEdition')) AS engine_edition,
    CASE WHEN DB_ID('rdsadmin') IS NOT NULL THEN 1 ELSE 0 END AS is_aws_rds,
    HAS_DBACCESS(N'msdb') AS has_msdb_access,
    -- #2228: which database this connection actually landed in. Appended; see the comment above.
    DB_NAME() AS connected_database";

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

    public static string ResolveConnectionString(MonitoredServer config, ILogger? logger = null)
    {
        string? password = null;
        if (config.UsesSqlAuth)
        {
            bool usedPlaintext;
            if (OperatingSystem.IsWindows())
            {
                password = DarlingSecrets.ResolvePassword(config, out usedPlaintext);
            }
            else
            {
                /* Non-Windows: DPAPI (DarlingSecrets) is unavailable, so only the password slot applies —
                   inlined here to keep the DPAPI call provably Windows-only for the platform analyzer.
                   The slot takes the same env:/file: references as everywhere else (#1804), which is the
                   supported non-Windows shape; a literal still works and still warns below. */
                if (!string.IsNullOrWhiteSpace(config.EncryptedPassword))
                {
                    throw new PlatformNotSupportedException(
                        "encryptedPassword requires Windows (DPAPI); use password with an env:/file: reference on other platforms.");
                }

                if (string.IsNullOrWhiteSpace(config.Password))
                {
                    throw new InvalidOperationException(
                        $"Server '{config.DisplayName}' uses sql auth but has neither encryptedPassword nor password.");
                }

                usedPlaintext = !DarlingSecretSource.IsReference(config.Password);
                password = DarlingSecretSource.Resolve(config.Password, $"servers['{config.DisplayName}'].password");
            }

            if (usedPlaintext)
            {
                logger?.LogWarning(
                    "Server '{Server}' uses a plaintext password in darling.json — run --encrypt-password and switch to encryptedPassword, or reference it via env:/file:.",
                    config.DisplayName);
            }
        }

        return MonitoredServerConnection.BuildConnectionString(config, password);
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
    version() AS server_version_text,
    current_setting('server_version_num')::int / 10000 AS major_version,
    pg_is_in_recovery() AS is_in_recovery,
    current_setting('server_version_num')::int AS server_version_num,
    -- #2228: which database this connection actually landed in. Appended; see the comment above.
    current_database() AS connected_database";

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
    public const string PostgresAuroraProbeQueryText = @"SELECT aurora_version()";

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
    private static async Task<bool> ProbeAuroraAsync(
        NpgsqlConnection connection, CancellationToken cancellationToken, ILogger? logger = null)
    {
        try
        {
            using var command = new NpgsqlCommand(PostgresAuroraProbeQueryText, connection) { CommandTimeout = 15 };
            var version = await command.ExecuteScalarAsync(cancellationToken);
            return version is not null and not DBNull;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            logger?.LogDebug(ex, "aurora_version() probe did not succeed; treating the target as stock PostgreSQL");
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
        string? connectedDatabase = null;
        if (await reader.ReadAsync(cancellationToken))
        {
            // Column indices per DetectionQueryText: 1 major_version, 3 engine_edition,
            // 4 is_aws_rds, 5 has_msdb_access (sqlserver_start_time was dropped in #1535).
            majorVersion = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            engineEdition = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
            isAwsRds = !reader.IsDBNull(4) && reader.GetInt32(4) == 1;
            hasMsdbAccess = reader.IsDBNull(5) || reader.GetInt32(5) == 1;
            connectedDatabase = reader.IsDBNull(6) ? null : reader.GetString(6);   /* #2228 */
        }

        return new ServerRuntime
        {
            Config = config,
            ConnectionString = connectionString,
            Target = new CollectorTargetInfo
            {
                IsAzureSqlDb = engineEdition == 5,
                IsAzureManagedInstance = engineEdition == 8,
                IsAwsRds = isAwsRds,
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
            EngineEdition = engineEdition,
            ConnectedDatabase = connectedDatabase,
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
    private static async Task<ServerRuntime> ConnectPostgresAsync(
        MonitoredServer config, ILogger? logger, CancellationToken cancellationToken)
    {
        var connectionString = ResolveConnectionString(config, logger);
        var storageName = config.StorageName;

        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(cancellationToken);

        using var command = new NpgsqlCommand(PostgresDetectionQueryText, connection) { CommandTimeout = 30 };
        using var reader = await command.ExecuteReaderAsync(cancellationToken);

        int majorVersion = 0, versionNum = 0;
        bool isInRecovery = false;
        string versionText = "";
        string? connectedDatabase = null;
        if (await reader.ReadAsync(cancellationToken))
        {
            versionText = reader.IsDBNull(0) ? "" : reader.GetString(0);
            majorVersion = reader.IsDBNull(1) ? 0 : reader.GetInt32(1);
            isInRecovery = !reader.IsDBNull(2) && reader.GetBoolean(2);
            versionNum = reader.IsDBNull(3) ? 0 : reader.GetInt32(3);
            connectedDatabase = reader.IsDBNull(4) ? null : reader.GetString(4);   /* #2228 */
        }

        /* The reader must be closed before another command runs on this connection. */
        await reader.CloseAsync();

        var isAurora = await ProbeAuroraAsync(connection, cancellationToken, logger);

        logger?.LogInformation(
            "Connected to PostgreSQL target '{Server}': major {Major} (server_version_num {Num}), {Role}, Aurora: {Aurora} — {VersionText}",
            config.DisplayName, majorVersion, versionNum, isInRecovery ? "reader (in recovery)" : "writer", isAurora,
            versionText);

        /* A Postgres target reached through the SQL Server path would have failed on the detection
           query, so an engine mismatch is loud. The reverse — a SQL Server host configured as
           "postgres" — fails at connect, which is equally loud. */
        return new ServerRuntime
        {
            Config = config,
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
            },
            StorageName = storageName,
            ServerId = config.ServerId,
            ConnectedDatabase = connectedDatabase,
        };
    }

    /// <summary>
    /// Non-throwing connect-and-probe: runs <see cref="ConnectAsync"/> and packages the outcome as a
    /// <see cref="ConnectionProbeResult"/> — success carries the probed version/edition/engine facts, a
    /// failure carries the error message (never plaintext credentials). Shared by the <c>test_connect</c>
    /// command (the Stage-3 Add-dialog validates a server BEFORE saving; the SERVICE holds the network
    /// path + credentials) and the <c>--test-connection</c>/<c>--validate-config</c> CLI verb, so both
    /// classify a server identically. <see cref="OperationCanceledException"/> propagates (shutdown).
    /// </summary>
    public static async Task<ConnectionProbeResult> ProbeAsync(MonitoredServer config, ILogger? logger, CancellationToken cancellationToken)
    {
        if (config is null)
        {
            throw new ArgumentNullException(nameof(config));
        }

        try
        {
            var runtime = await ConnectAsync(config, logger, cancellationToken);
            var isPostgres = runtime.Target.Engine == CollectorTargetEngine.PostgreSql;
            return new ConnectionProbeResult(
                Success: true,
                MajorVersion: runtime.Target.SqlMajorVersion,
                EngineEdition: runtime.EngineEdition,
                /* No edition on a PostgreSQL target, and DescribeEngineEdition(0) would say
                   "Unknown (0)" — which reads as a probe that half-failed rather than one that
                   succeeded against a different engine. */
                EngineEditionDescription: isPostgres ? null : DescribeEngineEdition(runtime.EngineEdition),
                IsAzureSqlDb: runtime.Target.IsAzureSqlDb,
                IsAzureManagedInstance: runtime.Target.IsAzureManagedInstance,
                IsAwsRds: runtime.IsAwsRds,
                HasMsdbAccess: runtime.HasMsdbAccess,
                Error: null,
                ConnectedDatabase: runtime.ConnectedDatabase,
                Engine: runtime.Target.Engine,
                PostgresMajorVersion: runtime.Target.PostgresMajorVersion,
                PostgresVersionNum: runtime.Target.PostgresVersionNum,
                IsAurora: runtime.Target.IsAurora,
                IsInRecovery: runtime.Target.IsInRecovery);
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
                Error: ex.Message);
        }
    }

    /// <summary>
    /// The probed facts for a REACHABLE target, as one clause — shared by the <c>--test-connection</c>
    /// PASS line (<c>DarlingCliCommands.FormatProbeLine</c>) and the <c>add_servers</c> MCP tool's detail
    /// text, which previously each formatted their own and could drift.
    /// <para>The engine decides what is worth saying. A SQL Server target reports version, edition and
    /// msdb access, because msdb access gates three collectors. A PostgreSQL target has none of those,
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
            var msdb = probe.HasMsdbAccess ? "msdb access: yes" : "msdb access: NO (failed-job alerts unavailable)";
            return $"SQL major version {probe.MajorVersion}, {edition}, {msdb}";
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
    string? ConnectedDatabase = null)
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
    };
}
