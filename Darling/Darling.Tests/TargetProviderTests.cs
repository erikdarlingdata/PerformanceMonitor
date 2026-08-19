/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the engine-execution seam: the right driver types come out of each provider, the collector
/// parameter mapping is total for both engines, and a driver failure is named the same way for both.
/// </summary>
public class TargetProviderTests
{
    private static CollectorQuery Query(params CollectorParameter[] parameters)
        => new("SELECT 1", parameters);

    [Fact]
    public void ResolvesAProviderForEveryDeclaredEngine()
    {
        foreach (CollectorTargetEngine engine in Enum.GetValues<CollectorTargetEngine>())
        {
            var provider = TargetProviders.For(engine);
            Assert.Equal(engine, provider.Engine);
        }
    }

    [Fact]
    public void ProducesTheDriverTypesEachEngineNeeds()
    {
        Assert.IsType<SqlConnection>(SqlServerTargetProvider.Instance.CreateConnection("Server=nowhere"));
        Assert.IsType<NpgsqlConnection>(PostgresTargetProvider.Instance.CreateConnection("Host=nowhere"));
    }

    /// <summary>
    /// Every parameter type a definition can declare must map on BOTH engines. An unmapped type
    /// throwing at runtime, inside a sweep, is the failure this prevents.
    /// </summary>
    [Fact]
    public void MapsEveryCollectorParameterTypeOnBothEngines()
    {
        foreach (CollectorParameterType type in Enum.GetValues<CollectorParameterType>())
        {
            var query = Query(new CollectorParameter("@p", Value(type), type));

            using var sqlConnection = new SqlConnection("Server=nowhere");
            using var sqlCommand = SqlServerTargetProvider.Instance.CreateCommand(query, sqlConnection, 60);
            Assert.Single(sqlCommand.Parameters);

            using var pgConnection = new NpgsqlConnection("Host=nowhere");
            using var pgCommand = PostgresTargetProvider.Instance.CreateCommand(query, pgConnection, 60);
            Assert.Single(pgCommand.Parameters);
        }

        static object Value(CollectorParameterType type) => type switch
        {
            CollectorParameterType.DateTime2 => new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Unspecified),
            CollectorParameterType.Int32 => 1,
            CollectorParameterType.BigInt => 1L,
            _ => "x",
        };
    }

    /// <summary>A null parameter value becomes DBNull, not a null reference, on both engines.</summary>
    [Fact]
    public void MapsNullParameterValuesToDbNull()
    {
        var query = Query(new CollectorParameter("@p", null, CollectorParameterType.NVarChar128));

        using var sqlConnection = new SqlConnection("Server=nowhere");
        using var sqlCommand = SqlServerTargetProvider.Instance.CreateCommand(query, sqlConnection, 60);
        Assert.Equal(DBNull.Value, sqlCommand.Parameters[0].Value);

        using var pgConnection = new NpgsqlConnection("Host=nowhere");
        using var pgCommand = PostgresTargetProvider.Instance.CreateCommand(query, pgConnection, 60);
        Assert.Equal(DBNull.Value, pgCommand.Parameters[0].Value);
    }

    [Fact]
    public void RejectsAConnectionFromTheWrongEngine()
    {
        using var pgConnection = new NpgsqlConnection("Host=nowhere");
        Assert.Throws<ArgumentException>(() =>
            SqlServerTargetProvider.Instance.CreateCommand(Query(), pgConnection, 60));

        using var sqlConnection = new SqlConnection("Server=nowhere");
        Assert.Throws<ArgumentException>(() =>
            PostgresTargetProvider.Instance.CreateCommand(Query(), sqlConnection, 60));
    }

    [Fact]
    public void AppliesTheCommandTimeoutOnBothEngines()
    {
        using var sqlConnection = new SqlConnection("Server=nowhere");
        using var sqlCommand = SqlServerTargetProvider.Instance.CreateCommand(Query(), sqlConnection, 300);
        Assert.Equal(300, sqlCommand.CommandTimeout);

        using var pgConnection = new NpgsqlConnection("Host=nowhere");
        using var pgCommand = PostgresTargetProvider.Instance.CreateCommand(Query(), pgConnection, 300);
        Assert.Equal(300, pgCommand.CommandTimeout);
    }

    /// <summary>
    /// The Postgres SQLSTATEs here are the ones actually observed while probing our Aurora fleet:
    /// 42501 from a function needing rds_replication, 42P01 from pg_stat_statements in a database
    /// where the view was never created, 0A000 from pg_stat_wal (Aurora blocks it), and a 55-class
    /// error from a feature that is switched off rather than empty.
    /// </summary>
    [Theory]
    [InlineData("42501", CollectorTargetFault.Permissions)]
    [InlineData("42P01", CollectorTargetFault.ObjectMissing)]
    [InlineData("42883", CollectorTargetFault.ObjectMissing)]
    [InlineData("0A000", CollectorTargetFault.FeatureDisabled)]
    [InlineData("55000", CollectorTargetFault.FeatureDisabled)]
    [InlineData("57014", CollectorTargetFault.CommandTimeout)]
    [InlineData("08006", CollectorTargetFault.ConnectionFatal)]
    [InlineData("08000", CollectorTargetFault.ConnectionFatal)]
    [InlineData("57P01", CollectorTargetFault.ConnectionFatal)]
    [InlineData("XX000", CollectorTargetFault.Unclassified)]
    public void ClassifiesPostgresSqlStates(string sqlState, CollectorTargetFault expected)
    {
        var exception = new PostgresException("boom", "ERROR", "ERROR", sqlState);
        Assert.Equal(expected, PostgresTargetProvider.Instance.Classify(exception, yieldsOnLockTimeout: false));
    }

    /// <summary>
    /// A lock timeout is a yield only for a collector that deliberately set a short lock timeout.
    /// Same rule, both engines — it is a property of the collector, not of the database.
    /// </summary>
    [Fact]
    public void TreatsALockTimeoutAsAYieldOnlyForCollectorsThatOptIn()
    {
        var pgLockTimeout = new PostgresException("boom", "ERROR", "ERROR", "55P03");

        Assert.Equal(
            CollectorTargetFault.LockTimeoutYield,
            PostgresTargetProvider.Instance.Classify(pgLockTimeout, yieldsOnLockTimeout: true));
        Assert.Equal(
            CollectorTargetFault.Unclassified,
            PostgresTargetProvider.Instance.Classify(pgLockTimeout, yieldsOnLockTimeout: false));
    }

    [Fact]
    public void ClassifiesUnrecognizedExceptionsAsUnclassifiedSoTheyStayLoud()
    {
        var boom = new InvalidOperationException("something else entirely");

        Assert.Equal(CollectorTargetFault.Unclassified, SqlServerTargetProvider.Instance.Classify(boom, false));
        Assert.Equal(CollectorTargetFault.Unclassified, PostgresTargetProvider.Instance.Classify(boom, false));
    }

    [Fact]
    public void ClassifiesATimeoutExceptionAsACommandTimeoutOnPostgres()
    {
        Assert.Equal(
            CollectorTargetFault.CommandTimeout,
            PostgresTargetProvider.Instance.Classify(new TimeoutException(), false));
    }

    /// <summary>
    /// Per-database fan-out must change ONLY the database. Every other setting — credentials, timeouts,
    /// TLS posture — has to survive, or a per-database collector would silently connect on different terms
    /// than the collector that probed the server.
    /// </summary>
    [Fact]
    public void WithDatabase_ChangesOnlyTheDatabase()
    {
        var sql = SqlServerTargetProvider.Instance.WithDatabase(
            "Server=sql1;Initial Catalog=master;User ID=mon;Password=p;Encrypt=Strict;Connect Timeout=15", "AdventureWorks");
        var sqlBuilder = new SqlConnectionStringBuilder(sql);

        Assert.Equal("AdventureWorks", sqlBuilder.InitialCatalog);
        Assert.Equal("sql1", sqlBuilder.DataSource);
        Assert.Equal("mon", sqlBuilder.UserID);
        Assert.Equal(SqlConnectionEncryptOption.Strict, sqlBuilder.Encrypt);
        Assert.Equal(15, sqlBuilder.ConnectTimeout);

        var pg = PostgresTargetProvider.Instance.WithDatabase(
            "Host=aurora;Database=postgres;Username=mon;Password=p;SSL Mode=VerifyFull;Timeout=15", "appdb");
        var pgBuilder = new NpgsqlConnectionStringBuilder(pg);

        Assert.Equal("appdb", pgBuilder.Database);
        Assert.Equal("aurora", pgBuilder.Host);
        Assert.Equal("mon", pgBuilder.Username);
        Assert.Equal(SslMode.VerifyFull, pgBuilder.SslMode);
        Assert.Equal(15, pgBuilder.Timeout);
    }

    /// <summary>
    /// SQL Server enumerates from master — on an Azure SQL DB logical server the configured entry points
    /// at one user database, where sys.databases lists only itself. PostgreSQL enumerates from wherever it
    /// already is, because pg_database is a shared catalog.
    /// </summary>
    [Fact]
    public void DatabaseListPlan_EnumeratesFromTheRightPlacePerEngine()
    {
        var (sqlConnectionString, sqlQuery) = SqlServerTargetProvider.Instance.BuildDatabaseListPlan(
            "Server=sql1;Initial Catalog=CustomerDb;User ID=mon;Password=p", null);

        Assert.Equal("master", new SqlConnectionStringBuilder(sqlConnectionString).InitialCatalog);
        Assert.Contains("sys.databases", sqlQuery.Text, StringComparison.Ordinal);

        const string pgConnectionString = "Host=aurora;Database=postgres;Username=mon;Password=p";
        var (pgConnection, pgQuery) = PostgresTargetProvider.Instance.BuildDatabaseListPlan(pgConnectionString, null);

        Assert.Equal(pgConnectionString, pgConnection);
        Assert.Contains("pg_database", pgQuery.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Both filters keep the fan-out from attempting connections that cannot succeed. template0 is frozen
    /// and refuses connections outright, so including it would guarantee one failed connection per cycle
    /// forever; datallowconn = false is a database an administrator has deliberately closed.
    /// </summary>
    [Fact]
    public void PostgresDatabaseList_SkipsTemplatesAndClosedDatabases()
    {
        var (_, query) = PostgresTargetProvider.Instance.BuildDatabaseListPlan("Host=aurora;Database=postgres", null);

        Assert.Contains("datallowconn", query.Text, StringComparison.Ordinal);
        Assert.Contains("NOT datistemplate", query.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// The exclusion list has to reach the right column name on each engine, parameterized rather than
    /// interpolated — a database named with a quote must not be able to alter the enumeration query.
    /// </summary>
    [Fact]
    public void DatabaseListPlan_AppliesTheExclusionListPerEngineColumn()
    {
        var excluded = new[] { "tempdb_clone", "scratch" };

        var (_, sqlQuery) = SqlServerTargetProvider.Instance.BuildDatabaseListPlan("Server=sql1", excluded);
        Assert.Contains("name NOT IN (@excl_db_0, @excl_db_1)", sqlQuery.Text, StringComparison.Ordinal);
        Assert.Equal(2, sqlQuery.Parameters.Count);

        var (_, pgQuery) = PostgresTargetProvider.Instance.BuildDatabaseListPlan("Host=aurora", excluded);
        Assert.Contains("datname NOT IN (@excl_db_0, @excl_db_1)", pgQuery.Text, StringComparison.Ordinal);
        Assert.Equal(2, pgQuery.Parameters.Count);

        /* Values travel as parameters, never as text in the query. */
        Assert.DoesNotContain("scratch", pgQuery.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("scratch", sqlQuery.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// An empty exclusion list must produce no clause and no parameters, not a dangling AND.
    /// <para>Each engine gets a connection string in ITS OWN dialect. A single string for both looks tidier
    /// and does not work: <c>BuildDatabaseListPlan</c> hops to another database via the provider's
    /// <c>WithDatabase</c>, which parses through that engine's real builder — so
    /// <c>SqlConnectionStringBuilder</c> rejects <c>Host=</c> outright ("Keyword not supported"), and the
    /// test fails on its own fixture rather than on the thing it is checking.</para>
    /// </summary>
    [Theory]
    [InlineData(CollectorTargetEngine.SqlServer, "Server=sql1;Database=master;Integrated Security=true")]
    [InlineData(CollectorTargetEngine.PostgreSql, "Host=pg1;Database=postgres;Username=monitor")]
    public void DatabaseListPlan_WithNoExclusionsIsClean(CollectorTargetEngine engine, string connectionString)
    {
        var (_, query) = TargetProviders.For(engine).BuildDatabaseListPlan(connectionString, Array.Empty<string>());

        Assert.Empty(query.Parameters);
        Assert.DoesNotContain("NOT IN", query.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("@excl_db_", query.Text, StringComparison.Ordinal);
    }

    /// <summary>
    /// Every engine in the enum has a provider. Split out from the case above, which needs an
    /// engine-specific connection string and so cannot loop the enum — this is the part that genuinely
    /// must cover all of them, and it fails when a new engine is added without one.
    /// </summary>
    [Fact]
    public void EveryEngineHasAProvider()
    {
        foreach (CollectorTargetEngine engine in Enum.GetValues<CollectorTargetEngine>())
        {
            Assert.NotNull(TargetProviders.For(engine));
        }
    }

    /// <summary>
    /// The enumeration query has to be executable through each provider's own command factory — the same
    /// parameter mapping every collector query goes through, so an exclusion parameter cannot be mapped
    /// one way here and another way there.
    /// </summary>
    [Fact]
    public void DatabaseListPlan_ParametersMapThroughTheProvidersOwnCommandFactory()
    {
        var excluded = new[] { "scratch" };

        var (_, sqlQuery) = SqlServerTargetProvider.Instance.BuildDatabaseListPlan("Server=sql1", excluded);
        using var sqlConnection = new SqlConnection("Server=nowhere");
        using var sqlCommand = SqlServerTargetProvider.Instance.CreateCommand(sqlQuery, sqlConnection, 60);
        Assert.Single(sqlCommand.Parameters);

        var (_, pgQuery) = PostgresTargetProvider.Instance.BuildDatabaseListPlan("Host=aurora", excluded);
        using var pgConnection = new NpgsqlConnection("Host=nowhere");
        using var pgCommand = PostgresTargetProvider.Instance.CreateCommand(pgQuery, pgConnection, 60);
        Assert.Single(pgCommand.Parameters);
    }
}
