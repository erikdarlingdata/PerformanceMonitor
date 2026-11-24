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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The statistics the candidate read left out, by reason (#8798): each statistic is counted in one bucket only.</summary>
internal sealed record SkipCounts(int Filtered, int MemoryOptimized, int XmlSpatialColumnstore, int NeverUpdated, int OverCap)
{
    public static readonly SkipCounts None = new(0, 0, 0, 0, 0);
}

/// <summary>One statistic the read batch could not read: the error SQL Server raised for it.</summary>
internal sealed record StatError(int ObjectId, int StatsId, int Number, string Message);

/// <summary>One statistic the read batch read: its header and its all-density values in vector order.</summary>
internal sealed record ColumnStatsReadRow(int ObjectId, int StatsId, StatHeader Header, IReadOnlyList<double> AllDensities);

/// <summary>
/// #8798: what the <c>column_stats</c> run asks of one connection to a monitored server: on premises the connection to the server, on
/// Azure SQL Database the connection to one database. The seam a test replaces to run the collector without a SQL Server.
/// </summary>
internal interface IColumnStatsConnection : IAsyncDisposable
{
    /// <summary>The server's version (major.minor.build), which decides the shape of the STAT_HEADER result.</summary>
    Version ServerVersion { get; }

    /// <summary>The databases the server (on Azure, the connected database) reports.</summary>
    Task<IReadOnlyList<SchemaDatabaseProbe>> ReadProbesAsync(CancellationToken cancellationToken);

    /// <summary>Whether the login holds SELECT on the database, which DBCC SHOW_STATISTICS needs.</summary>
    Task<bool> HasSelectAsync(string database, CancellationToken cancellationToken);

    /// <summary>
    /// The candidate statistics, largest tables first, and the counts of those left out. Over the cap, <c>AllIds</c> holds the ids of every
    /// candidate (the capped list is not the whole database); otherwise it is empty.
    /// </summary>
    Task<(IReadOnlyList<StatCandidate> Candidates, SkipCounts Skipped, IReadOnlyList<(int ObjectId, int StatsId)> AllIds)> ReadCandidatesAsync(
        string database, CancellationToken cancellationToken);

    /// <summary>The header and density vector of each named statistic, and an error row for each one that could not be read.</summary>
    Task<(IReadOnlyList<ColumnStatsReadRow> Rows, IReadOnlyList<StatError> Errors)> ReadStatisticsAsync(
        string database, IReadOnlyList<(int ObjectId, int StatsId)> stats, bool headerHasPersistedSamplePercent, CancellationToken cancellationToken);
}

/// <summary>Opens <see cref="IColumnStatsConnection"/>s: <paramref name="azureDatabase"/> is null for a server, the database name on Azure SQL Database.</summary>
internal interface IColumnStatsSource
{
    Task<IColumnStatsConnection> OpenAsync(ServerRuntime server, string? azureDatabase, CancellationToken cancellationToken);
}

/// <summary>The real thing: the schema pass's SqlClient connection and per-database wrapper, with the <see cref="ColumnStatsQueries"/> batches.</summary>
internal sealed class SqlColumnStatsSource : IColumnStatsSource
{
    /* A read batch of 200 statistics can wait up to LOCK_TIMEOUT on each; the per-database budget cancels it first. */
    private const int CommandTimeoutSeconds = 300;
    private readonly DarlingCollectorRunner _runner;

    public SqlColumnStatsSource(DarlingCollectorRunner runner) => _runner = runner;

    public async Task<IColumnStatsConnection> OpenAsync(ServerRuntime server, string? azureDatabase, CancellationToken cancellationToken) =>
        new Connection(await SqlSchemaCaptureSource.OpenSqlAsync(_runner, server, azureDatabase, cancellationToken), databaseScoped: azureDatabase is not null);

    private sealed class Connection : IColumnStatsConnection
    {
        private readonly SqlConnection _connection;
        private readonly bool _databaseScoped;

        public Connection(SqlConnection connection, bool databaseScoped)
        {
            _connection = connection;
            _databaseScoped = databaseScoped;
            ServerVersion = Version.TryParse(connection.ServerVersion, out var version) ? version : new Version(0, 0, 0);
        }

        public Version ServerVersion { get; }

        public ValueTask DisposeAsync() => _connection.DisposeAsync();

        public async Task<IReadOnlyList<SchemaDatabaseProbe>> ReadProbesAsync(CancellationToken cancellationToken)
        {
            await using var command = Command(_databaseScoped ? SchemaServerProbe.BuildAzureSql() : SchemaServerProbe.BuildSql());
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            return await SchemaServerProbe.ReadAsync(reader, cancellationToken);
        }

        public async Task<bool> HasSelectAsync(string database, CancellationToken cancellationToken)
        {
            await using var command = Command(InDatabase(database, ColumnStatsQueries.HasSelectSql));
            var value = await command.ExecuteScalarAsync(cancellationToken);
            return value is not null && value is not DBNull && Convert.ToInt32(value, CultureInfo.InvariantCulture) == 1;
        }

        public async Task<(IReadOnlyList<StatCandidate> Candidates, SkipCounts Skipped, IReadOnlyList<(int ObjectId, int StatsId)> AllIds)> ReadCandidatesAsync(
            string database, CancellationToken cancellationToken)
        {
            await using var command = Command(InDatabase(database, ColumnStatsQueries.CandidatesSql));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            var candidates = new List<StatCandidate>();
            while (await reader.ReadAsync(cancellationToken))
            {
                candidates.Add(new StatCandidate(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetInt32(3),
                    reader.GetString(4),
                    reader.GetString(5),
                    reader.GetDateTime(6),
                    reader.GetDateTime(10),
                    SchemaCatalogReader.SplitList(reader.IsDBNull(8) ? null : reader.GetString(8)),
                    SchemaCatalogReader.SplitList(reader.IsDBNull(9) ? null : reader.GetString(9)),
                    reader.GetInt64(7)));
            }

            var skipped = SkipCounts.None;
            if (await reader.NextResultAsync(cancellationToken) && await reader.ReadAsync(cancellationToken))
            {
                skipped = new SkipCounts(Int(reader, 0), Int(reader, 1), Int(reader, 2), Int(reader, 3), Int(reader, 4));
            }

            /* Set 3: every candidate's ids, only over the cap. */
            var allIds = new List<(int ObjectId, int StatsId)>();
            if (await reader.NextResultAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    allIds.Add((reader.GetInt32(0), reader.GetInt32(1)));
                }
            }

            return (candidates, skipped, allIds);
        }

        public async Task<(IReadOnlyList<ColumnStatsReadRow> Rows, IReadOnlyList<StatError> Errors)> ReadStatisticsAsync(
            string database, IReadOnlyList<(int ObjectId, int StatsId)> stats, bool headerHasPersistedSamplePercent, CancellationToken cancellationToken)
        {
            await using var command = Command(InDatabase(database, ColumnStatsQueries.BuildReadBatch(stats, headerHasPersistedSamplePercent)));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            /* Set 1: object_id, stats_id, Rows, Rows Sampled, Steps, Density, Average key length, String Index, Unfiltered Rows
               [, Persisted Sample Percent]. */
            var headers = new List<(int ObjectId, int StatsId, StatHeader Header)>();
            while (await reader.ReadAsync(cancellationToken))
            {
                float? persisted = headerHasPersistedSamplePercent && !reader.IsDBNull(9) ? (float)reader.GetDouble(9) : null;
                headers.Add((reader.GetInt32(0), reader.GetInt32(1), new StatHeader(
                    reader.IsDBNull(2) ? 0 : reader.GetInt64(2),
                    reader.IsDBNull(3) ? 0 : reader.GetInt64(3),
                    persisted)));
            }

            /* Set 2: object_id, stats_id, ord, all_density, average_length, ordered by statistic then ord. */
            var densities = new Dictionary<(int, int), List<double>>();
            if (await reader.NextResultAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    var key = (reader.GetInt32(0), reader.GetInt32(1));
                    if (!densities.TryGetValue(key, out var list))
                    {
                        densities[key] = list = [];
                    }

                    list.Add(reader.IsDBNull(3) ? 0 : reader.GetDouble(3));
                }
            }

            /* Set 3: object_id, stats_id, error_number, error_message. */
            var errors = new List<StatError>();
            if (await reader.NextResultAsync(cancellationToken))
            {
                while (await reader.ReadAsync(cancellationToken))
                {
                    errors.Add(new StatError(reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2), reader.IsDBNull(3) ? string.Empty : reader.GetString(3)));
                }
            }

            var rows = headers
                .Select(h => new ColumnStatsReadRow(h.ObjectId, h.StatsId, h.Header, densities.TryGetValue((h.ObjectId, h.StatsId), out var d) ? d : []))
                .ToList();
            return (rows, errors);
        }

        private string InDatabase(string database, string batch) => _databaseScoped ? batch : SqlSchemaCaptureSource.Wrap(database, batch);

        private SqlCommand Command(string text) => new(text, _connection) { CommandTimeout = CommandTimeoutSeconds };

        private static int Int(DbDataReader reader, int ordinal) =>
            reader.IsDBNull(ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
    }
}
