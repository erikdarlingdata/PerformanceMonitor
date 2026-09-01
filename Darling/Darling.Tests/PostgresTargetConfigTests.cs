/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the config-to-connection path for a PostgreSQL target: how the engine is declared, that
/// omitting it cannot change an existing server's behaviour, and the connection posture the builder
/// produces.
/// </summary>
public class PostgresTargetConfigTests
{
    private static MonitoredServer PgServer() => new()
    {
        Name = "aurora-writer",
        Engine = "postgres",
        Host = "segments-multi-1.cluster-x.us-east-1.rds.amazonaws.com",
        Auth = "sql",
        Username = "collector",
    };

    /// <summary>
    /// The default matters more than the parsing: every darling.json in the field omits "engine", and
    /// omitting it must keep those entries on the SQL Server path exactly as before.
    /// </summary>
    [Fact]
    public void DefaultsToSqlServerWhenEngineIsAbsent()
    {
        var server = new MonitoredServer { Host = "SQL2022" };

        Assert.Equal(CollectorTargetEngine.SqlServer, server.TargetEngine);
        Assert.False(server.IsPostgres);
    }

    [Theory]
    [InlineData("postgres")]
    [InlineData("postgresql")]
    [InlineData("pg")]
    [InlineData("aurora-postgresql")]
    [InlineData("aurora")]
    [InlineData("  Postgres  ")]
    [InlineData("POSTGRESQL")]
    public void RecognizesThePostgresSpellings(string engine)
    {
        Assert.Equal(CollectorTargetEngine.PostgreSql, new MonitoredServer { Engine = engine }.TargetEngine);
    }

    /// <summary>
    /// A typo resolves to SQL Server rather than throwing: one bad server entry must not stop the
    /// service from starting and monitoring every other server. The mismatch is loud anyway — the SQL
    /// Server detection query fails immediately against a Postgres host.
    /// </summary>
    [Theory]
    [InlineData("postgrez")]
    [InlineData("mysql")]
    [InlineData("")]
    public void FallsBackToSqlServerOnAnUnrecognizedEngine(string engine)
    {
        Assert.Equal(CollectorTargetEngine.SqlServer, new MonitoredServer { Engine = engine }.TargetEngine);
    }

    [Fact]
    public void BuildsAPostgresConnectionStringWithTheIntendedPosture()
    {
        var raw = MonitoredServerConnection.BuildConnectionString(PgServer(), "secret");
        var built = new NpgsqlConnectionStringBuilder(raw);

        Assert.Equal("segments-multi-1.cluster-x.us-east-1.rds.amazonaws.com", built.Host);
        Assert.Equal("postgres", built.Database);          // maintenance DB when none is configured
        Assert.Equal("collector", built.Username);
        Assert.Equal("PerformanceMonitorDarling", built.ApplicationName);
        Assert.Equal(15, built.Timeout);                    // same connect budget as the SQL Server path
        Assert.Equal(60, built.CommandTimeout);
        Assert.Equal(SslMode.VerifyFull, built.SslMode);    // fail-closed by default
    }

    [Fact]
    public void UsesTheConfiguredDatabaseWhenOneIsGiven()
    {
        var server = PgServer();
        server.Database = "payment_processing";

        var built = new NpgsqlConnectionStringBuilder(MonitoredServerConnection.BuildConnectionString(server, "secret"));

        Assert.Equal("payment_processing", built.Database);
    }

    /// <summary>
    /// pg_stat_statements lives in the app database on some of our clusters rather than in postgres,
    /// so pointing an entry at a specific database is a supported configuration, not an edge case.
    /// </summary>
    [Fact]
    public void HonorsANonDefaultPort()
    {
        var server = PgServer();
        server.Port = 5433;

        Assert.Equal(5433, new NpgsqlConnectionStringBuilder(
            MonitoredServerConnection.BuildConnectionString(server, "secret")).Port);
    }

    [Fact]
    public void DefaultsToTheStandardPortWhenNoneIsConfigured()
    {
        Assert.Equal(5432, new NpgsqlConnectionStringBuilder(
            MonitoredServerConnection.BuildConnectionString(PgServer(), "secret")).Port);
    }

    /// <summary>
    /// TrustServerCertificate relaxes verification without abandoning TLS — Aurora presents an RDS CA
    /// a stock trust store does not know, which is exactly the case this covers.
    /// </summary>
    [Fact]
    public void TrustServerCertificateRelaxesVerificationButKeepsTls()
    {
        var server = PgServer();
        server.TrustServerCertificate = true;

        Assert.Equal(SslMode.Require, new NpgsqlConnectionStringBuilder(
            MonitoredServerConnection.BuildConnectionString(server, "secret")).SslMode);
    }

    [Fact]
    public void OptionalEncryptModeDowngradesToPrefer()
    {
        var server = PgServer();
        server.EncryptMode = "optional";

        Assert.Equal(SslMode.Prefer, new NpgsqlConnectionStringBuilder(
            MonitoredServerConnection.BuildConnectionString(server, "secret")).SslMode);
    }

    /// <summary>
    /// Integrated auth is rejected loudly rather than producing a connection string that cannot
    /// authenticate and failing later, further from the cause.
    /// </summary>
    [Fact]
    public void RejectsIntegratedAuthForPostgresTargets()
    {
        var server = PgServer();
        server.Auth = "integrated";

        var ex = Assert.Throws<InvalidOperationException>(
            () => MonitoredServerConnection.BuildConnectionString(server, null));
        Assert.Contains("PostgreSQL", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void RejectsSqlAuthWithNoResolvedPassword()
    {
        Assert.Throws<InvalidOperationException>(
            () => MonitoredServerConnection.BuildConnectionString(PgServer(), null));
    }

    /// <summary>
    /// A Postgres entry still derives its identity through the shared rule, so it lands in the same
    /// server registry with a server_id computed the same way as every SQL Server entry.
    /// </summary>
    [Fact]
    public void DerivesStorageIdentityThroughTheSharedRule()
    {
        var server = PgServer();
        server.Database = "segments_horizon";
        server.ReadOnlyIntent = true;

        /* #2218: ":pg" sits between the database and ":RO". A PostgreSQL instance and a SQL Server on one host
           used to derive ONE server_id and interleave their histories; the engine token is what separates them,
           and its position is fixed so two callers with the same facts cannot produce two names. */
        Assert.Equal(
            "segments-multi-1.cluster-x.us-east-1.rds.amazonaws.com:segments_horizon:pg:RO",
            server.StorageName);

        /* A port appends after the engine when one is set — the second half of #2218, for two PostgreSQL
           instances on one host. */
        server.Port = 6432;
        Assert.Equal(
            "segments-multi-1.cluster-x.us-east-1.rds.amazonaws.com:segments_horizon:pg:6432:RO",
            server.StorageName);
    }

    /// <summary>
    /// The detection query must only touch surfaces a pg_monitor-grade login can read, and must not
    /// depend on version() text formatting.
    /// </summary>
    [Fact]
    public void PostgresDetectionQueryUsesPortableSurfaces()
    {
        var sql = DarlingServerConnector.PostgresDetectionQueryText;

        Assert.Contains("server_version_num", sql, StringComparison.Ordinal);
        Assert.Contains("pg_is_in_recovery()", sql, StringComparison.Ordinal);
        // No T-SQL leaked into the Postgres path.
        Assert.DoesNotContain("SERVERPROPERTY", sql, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("@@VERSION", sql, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// #2340: Aurora is detected by CALLING <c>aurora_version()</c>, never by looking it up in a catalog.
    ///
    /// <para>The catalog form this replaces — <c>count(*) FROM pg_proc WHERE proname = 'aurora_version'</c>
    /// — was measured returning <b>0</b> on a live Aurora PostgreSQL 17.7 cluster as a <c>pg_monitor</c>
    /// role whose <c>SELECT aurora_version()</c> returned <c>17.7.2</c>. Because
    /// <c>PgWaitStatsCollector</c> and <c>PgStatementStatsCollector</c> both gate on <c>IsAurora</c>, that
    /// one wrong boolean silently disabled the two most valuable PostgreSQL reads on every Aurora target.
    /// Pinned as an ABSENCE as well as a presence, because the tempting "just add pg_proc back as a
    /// fallback" would restore a check that is wrong precisely where it matters.</para>
    /// </summary>
    [Fact]
    public void AuroraIsDetectedByCallingTheFunction_NotByACatalogLookup()
    {
        var probe = DarlingServerConnector.PostgresAuroraProbeQueryText;

        Assert.Contains("aurora_version()", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("pg_proc", probe, StringComparison.Ordinal);
        Assert.DoesNotContain("count(", probe, StringComparison.OrdinalIgnoreCase);

        /* Its own statement: that is what lets a stock-PostgreSQL 42883 be caught and read as "not
           Aurora" rather than failing the whole probe. Folded back into the detection query, the
           failure would take version and recovery detection down with it. */
        Assert.DoesNotContain("aurora", DarlingServerConnector.PostgresDetectionQueryText, StringComparison.OrdinalIgnoreCase);
    }

    private static DarlingConfig ConfigWith(MonitoredServer server)
    {
        var config = new DarlingConfig();
        config.Postgres.ConnectionString = "Host=localhost;Database=darling;Username=x;Password=y";
        config.Servers.Add(server);
        return config;
    }

    /// <summary>
    /// The pre-flight has to reject integrated auth on a PostgreSQL target. The connection builder
    /// throws on it too, but that fires at first connect — for a service, hours after deployment and
    /// only in a log, where --test-connection would have said so before install.
    /// </summary>
    [Fact]
    public void ValidationRejectsIntegratedAuthOnAPostgresTarget()
    {
        var problems = ConfigWith(new MonitoredServer
        {
            Name = "aurora-writer",
            Engine = "postgres",
            Host = "aurora.cluster-x.us-east-1.rds.amazonaws.com",
            Auth = "integrated",
        }).Validate();

        Assert.Contains(problems, p => p.Contains("PostgreSQL target requires auth 'sql'", StringComparison.Ordinal));
    }

    /// <summary>Integrated auth stays perfectly valid on a SQL Server entry — the new rule is engine-scoped.</summary>
    [Fact]
    public void ValidationStillAllowsIntegratedAuthOnASqlServerTarget()
    {
        var problems = ConfigWith(new MonitoredServer { Name = "SQL2022", Host = "SQL2022", Auth = "integrated" })
            .Validate();

        Assert.Empty(problems);
    }

    /// <summary>A fully specified Postgres entry passes, so the new rules cannot reject a good config.</summary>
    [Fact]
    public void ValidationAcceptsAWellFormedPostgresTarget()
    {
        var server = PgServer();
        server.Password = "env:PGPASSWORD";
        server.Port = 5432;

        Assert.Empty(ConfigWith(server).Validate());
    }

    /// <summary>
    /// 0 is the documented "use the driver's default" value and must not be flagged; a real out-of-range
    /// port must be. Left unvalidated, a typo'd port surfaces as a connect timeout, which reads like a
    /// firewall problem and gets escalated to the wrong team.
    /// </summary>
    [Theory]
    [InlineData(0, false)]
    [InlineData(5432, false)]
    [InlineData(65535, false)]
    [InlineData(-1, true)]
    [InlineData(65536, true)]
    public void ValidationRangeChecksTheOptionalPort(int port, bool expectProblem)
    {
        var server = PgServer();
        server.Password = "env:PGPASSWORD";
        server.Port = port;

        var problems = ConfigWith(server).Validate();

        Assert.Equal(expectProblem, problems.Any(p => p.Contains("port must be between", StringComparison.Ordinal)));
    }

    /* ─────────────── the store round-trip: darling.json is NOT the live source of truth ─────────────── */

    /// <summary>
    /// Every connection-affecting <see cref="MonitoredServer"/> property must have a column in
    /// <c>config.config_monitored_servers</c>, because that registry — not darling.json — is what the worker
    /// reads once the store has been seeded.
    /// <para>This test exists because <c>Engine</c> and <c>Port</c> did not have columns. A PostgreSQL entry
    /// was therefore written to the store without its engine, read back as the <c>"sqlserver"</c> property
    /// default, and connected to with <c>SqlConnection</c> — on the FIRST start, since the seed is immediately
    /// followed by the load that replaces the file's list. Nothing failed to compile and no test covered it;
    /// the whole PostgreSQL feature simply did not survive its own registration. A property-driven check is
    /// the only kind that catches the NEXT one.</para>
    /// </summary>
    [Fact]
    public void EveryRoundTripCriticalServerPropertyHasAStoreColumn()
    {
        /* Property name -> column name. Deliberately explicit rather than a PascalCase-to-snake_case
           convention, because the mapping is the thing under test: a convention would "prove" a column
           exists by deriving its name from the property. */
        var mustRoundTrip = new (string Property, string Column)[]
        {
            ("Name", "name"),
            ("Engine", "engine"),
            ("Host", "host"),
            ("Port", "port"),
            ("Database", "database"),
            ("Auth", "auth"),
            ("Username", "username"),
            ("EncryptedPassword", "encrypted_password"),
            ("ReadOnlyIntent", "read_only_intent"),
            ("TrustServerCertificate", "trust_server_certificate"),
            ("EncryptMode", "encrypt_mode"),
            ("MultiSubnetFailover", "multi_subnet_failover"),
            ("ExcludedDatabases", "excluded_databases"),
            ("MonthlyCostUsd", "monthly_cost_usd"),
            ("AlertDeliveryModeOverride", "alert_delivery_mode_override"),
            /* #2218. The one entry whose property name deliberately does not resemble its column: the
               property says STORED because that is the whole point — it is the registry's value, and null
               means "no store row yet", which is what makes MonitoredServer.ServerId fall back to the
               derivation. It belongs in this list rather than beside Password's exemption because it really
               does round-trip, through the table's own PRIMARY KEY. That column existed from V17 and this
               read simply did not select it, which is the class of bug this test was written for: a property
               and a column that fail to meet, with nothing failing to compile. */
            ("StoredServerId", "server_id"),
            /* V107 (#2138): write-gate 2 for the force-plan bot. Store-only like StoredServerId — the
               seed always writes FALSE and the property is [JsonIgnore], because a darling.json knob
               for a WRITE authorization would be a silent no-op on every seeded box (#2254). It
               round-trips through the registry read, which is exactly what this test checks. */
            ("PlanForceBotEnabled", "plan_force_bot_enabled"),
        };

        /* Password is the deliberate exception: a plaintext dev password is never persisted, and is
           backfilled from the in-memory bootstrap config at read time instead. */
        var properties = typeof(MonitoredServer).GetProperties()
            .Where(p => p.CanWrite)
            .Select(p => p.Name)
            .ToHashSet(StringComparer.Ordinal);
        var accountedFor = mustRoundTrip.Select(m => m.Property).Append("Password").ToHashSet(StringComparer.Ordinal);

        Assert.Empty(properties.Except(accountedFor));

        /* The columns, as the ladder actually leaves them: the CREATE plus every later ADD COLUMN. */
        var ladder = string.Join("\n", PgMigrations.Scripts.Select(s => s.Sql));
        foreach (var (property, column) in mustRoundTrip)
        {
            Assert.True(
                ladder.Contains($"    {column} ", StringComparison.Ordinal)
                || ladder.Contains($"ADD COLUMN IF NOT EXISTS {column} ", StringComparison.Ordinal),
                $"MonitoredServer.{property} has no '{column}' column in config_monitored_servers — it will not " +
                "survive the store round-trip, and the store is authoritative once seeded.");
        }
    }

    /// <summary>
    /// The two write paths into the registry must both carry the engine. The seed covers a fresh install; the
    /// <c>add_servers</c> tool covers every install after the first seed, which is the ONLY path there — a
    /// darling.json edit does not add a server to an already-seeded store.
    /// </summary>
    [Fact]
    public void TheOnboardingToolPersistsTheEngineAndPort()
    {
        Assert.Contains("engine", DarlingMcpServerAdminTools.InsertServerSql, StringComparison.Ordinal);
        Assert.Contains("port", DarlingMcpServerAdminTools.InsertServerSql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, "sqlserver")]
    [InlineData("", "sqlserver")]
    [InlineData("sqlserver", "sqlserver")]
    [InlineData("postgres", "postgres")]
    [InlineData("POSTGRESQL", "postgres")]
    [InlineData("  aurora  ", "postgres")]
    public void OnboardingNormalizesTheEngine(string? raw, string expected)
    {
        var (engine, error) = DarlingMcpServerAdminTools.ResolveEngine(raw);

        Assert.Null(error);
        Assert.Equal(expected, engine);
    }

    /// <summary>
    /// A typo must be REFUSED here, not silently resolved to SQL Server the way the file parser does. The
    /// parser's leniency protects a whole fleet from one bad line at startup; onboarding is a single
    /// deliberate act, and "postgress" quietly becoming a SQL Server target yields a connection failure
    /// against port 5432 with nothing pointing at the real mistake.
    /// </summary>
    [Theory]
    [InlineData("postgress")]
    [InlineData("mysql")]
    [InlineData("cockroach")]
    public void OnboardingRefusesAnUnrecognizedEngineRatherThanDefaultingIt(string raw)
    {
        var (_, error) = DarlingMcpServerAdminTools.ResolveEngine(raw);

        Assert.NotNull(error);
        Assert.Contains("postgres", error, StringComparison.Ordinal);
    }
}
