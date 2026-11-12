/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #7643: what the schema pass asks of one connection to a monitored server: on premises the connection to the server, on Azure SQL
/// Database the connection to one database. The seam a test replaces to run the pass without a SQL Server.
/// </summary>
internal interface ISchemaCaptureConnection : IAsyncDisposable
{
    /// <summary>Ensures the change-tracking session. Never throws for a failure of the ensure: the state carries the fault.</summary>
    Task<SchemaSessionState> EnsureSessionAsync(string sessionName, CancellationToken cancellationToken);

    /// <summary>The databases the server (on Azure, the connected database) reports.</summary>
    Task<IReadOnlyList<SchemaDatabaseProbe>> ReadProbesAsync(CancellationToken cancellationToken);

    /// <summary>The session's events after <paramref name="cutoffUtc"/>.</summary>
    Task<SchemaEventBatch> ReadEventsAsync(string sessionName, DateTime cutoffUtc, CancellationToken cancellationToken);

    /// <summary>One database's catalog: the objects <paramref name="objectIds"/> name, or every object when null.</summary>
    Task<SchemaCatalogSnapshot> ReadCatalogAsync(string database, IReadOnlyCollection<int>? objectIds, CancellationToken cancellationToken);
}

/// <summary>Opens <see cref="ISchemaCaptureConnection"/>s: <paramref name="azureDatabase"/> is null for a server, the database name on Azure SQL Database.</summary>
internal interface ISchemaCaptureSource
{
    Task<ISchemaCaptureConnection> OpenAsync(ServerRuntime server, string? azureDatabase, CancellationToken cancellationToken);
}

/// <summary>The real thing: SqlClient connections, the T3 and T5 statements and the T4 session ensure.</summary>
internal sealed class SqlSchemaCaptureSource : ISchemaCaptureSource
{
    private const int CommandTimeoutSeconds = 120;
    private readonly DarlingCollectorRunner _runner;

    public SqlSchemaCaptureSource(DarlingCollectorRunner runner) => _runner = runner;

    public async Task<ISchemaCaptureConnection> OpenAsync(ServerRuntime server, string? azureDatabase, CancellationToken cancellationToken)
    {
        if (azureDatabase is not null)
        {
            return new Connection(await _runner.OpenAzureDatabaseConnectionAsync(server, azureDatabase, cancellationToken), databaseScoped: true, _runner);
        }

        var connection = new SqlConnection(DarlingXeSessions.WithoutReadOnlyIntent(server.ConnectionString));
        try
        {
            await connection.OpenAsync(cancellationToken);
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }

        return new Connection(connection, databaseScoped: false, _runner);
    }

    private sealed class Connection : ISchemaCaptureConnection
    {
        private readonly SqlConnection _connection;
        private readonly bool _databaseScoped;
        private readonly DarlingCollectorRunner _runner;

        public Connection(SqlConnection connection, bool databaseScoped, DarlingCollectorRunner runner)
        {
            _connection = connection;
            _databaseScoped = databaseScoped;
            _runner = runner;
        }

        public ValueTask DisposeAsync() => _connection.DisposeAsync();

        public Task<SchemaSessionState> EnsureSessionAsync(string sessionName, CancellationToken cancellationToken) =>
            DarlingSchemaXeSession.EnsureAsync(_connection, sessionName, _databaseScoped, _runner.Logger, cancellationToken);

        public async Task<IReadOnlyList<SchemaDatabaseProbe>> ReadProbesAsync(CancellationToken cancellationToken)
        {
            await using var command = Command(_databaseScoped ? SchemaServerProbe.BuildAzureSql() : SchemaServerProbe.BuildSql());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await SchemaServerProbe.ReadAsync(reader, cancellationToken);
        }

        public async Task<SchemaEventBatch> ReadEventsAsync(string sessionName, DateTime cutoffUtc, CancellationToken cancellationToken)
        {
            await using var command = Command(SchemaEventQuery.Build(sessionName, _databaseScoped));
            command.Parameters.Add(new SqlParameter("@cutoff_time", System.Data.SqlDbType.DateTime2) { Value = DateTime.SpecifyKind(cutoffUtc, DateTimeKind.Unspecified) });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await SchemaEventReader.ReadAsync(reader, cancellationToken);
        }

        public async Task<SchemaCatalogSnapshot> ReadCatalogAsync(string database, IReadOnlyCollection<int>? objectIds, CancellationToken cancellationToken)
        {
            var batch = objectIds is null ? SchemaCatalogQueries.BuildFullRead() : SchemaCatalogQueries.BuildTargetedRead(objectIds);
            await using var command = Command(_databaseScoped ? batch : Wrap(database, batch));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await SchemaCatalogReader.ReadAsync(reader, cancellationToken);
        }

        private SqlCommand Command(string text) => new(text, _connection) { CommandTimeout = CommandTimeoutSeconds };

        /* On premises the batch runs in the database's own context through its sp_executesql, the way IndexObjectStatsCollector reaches a
           database: the statement text is one nested string, so its quotes are doubled. Azure SQL Database has no three-part names, and its
           connection is already in the database. */
        internal static string Wrap(string database, string batch) =>
            string.Create(
                CultureInfo.InvariantCulture,
                $"EXECUTE [{database.Replace("]", "]]", StringComparison.Ordinal)}].sys.sp_executesql N'{batch.Replace("'", "''", StringComparison.Ordinal)}';");
    }
}
