/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6356: runs the host's REAL <see cref="DarlingCollectorRunner.RunAsync"/> on a fake target and asserts the
/// collection note that reaches <see cref="CollectorRunResult.Note"/> (and so the collection_log row). This
/// replaces a source-text pin on the success return: a note dropped or overwritten anywhere between the
/// enumeration read and that return fails here, which a string match on the final constructor call cannot see.
/// </summary>
[Collection("live-postgres")]
public sealed class DarlingRunnerCollectionNoteLiveTests
{
    private const int LiveServerId = -490003;

    private static readonly object[] BetaDenied = { "beta", "The server principal is not able to access the database" };

    [Fact]
    public async Task EnumeratedRun_That_Collects_Rows_Carries_The_ProbeFailure_Note_Onto_The_Result()
    {
        /* One database enumerated and read, one the enumeration could not probe: the run collects normally and
           returns through the success path at the bottom of the method, which must still carry the note. */
        var result = await RunAsync(
            DatabaseScopedConfigCollector.Instance,
            new[] { new object[][] { new object[] { "alpha" } }, new object[][] { BetaDenied } });

        Assert.Equal(1, result.Rows);
        Assert.Equal(EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 1), result.Note);
        Assert.NotNull(result.Note);
    }

    [Fact]
    public async Task EmptyEnumeration_With_A_ProbeFailure_Carries_Both_Notes_Onto_The_Result()
    {
        var result = await RunAsync(
            DatabaseScopedConfigCollector.Instance,
            new[] { Array.Empty<object[]>(), new object[][] { BetaDenied } });

        Assert.Equal(0, result.Rows);
        Assert.Equal(EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: true, probeFailureCount: 1), result.Note);
        Assert.Contains(EnumeratedCollectorDriver.EmptyEnumerationMessage, result.Note);
    }

    [Fact]
    public async Task PayloadRun_Carries_The_Trailing_ProbeFailure_Set_Onto_The_Result()
    {
        /* database_size_stats on a box server: the one result set is the payload (empty here), followed by the
           optional (item_name, error_text) set the #1851 contract reads after the payload. */
        var result = await RunAsync(
            DatabaseSizeStatsCollector.Instance,
            new[] { Array.Empty<object[]>(), new object[][] { BetaDenied } });

        Assert.Equal(EnumeratedCollectorDriver.BuildNote(enumerationWasEmpty: false, probeFailureCount: 1), result.Note);
        Assert.NotNull(result.Note);
    }

    private static async Task<CollectorRunResult> RunAsync<TRow>(ICollectorDefinition<TRow> definition, object[][][] defaultSets)
    {
        var connectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(connectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live runner collection-note test.");

        var ct = TestContext.Current.CancellationToken;

        /* #1776 own-store: rows are keyed by a distinctive fake server id and removed in the cleanup. */
        using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await PgMigrations.MigrateAsync(connection, ct);
        await DeleteAsync(connection, ct);

        await using var postgres = NpgsqlDataSource.Create(connectionString!);
        var runner = new DarlingCollectorRunner(postgres, new CollectorDeltaCalculator());

        var bodySucceeded = false;
        try
        {
            runner.TargetProviderOverrideForTests = _ => new FakeTargetProvider(defaultSets);
            var server = new ServerRuntime
            {
                Config = new MonitoredServer { Name = "t", Host = "h" },
                ConnectionString = "Server=onprem;Database=master",
                Target = new CollectorTargetInfo { Engine = CollectorTargetEngine.SqlServer },
                StorageName = "h",
                ServerId = LiveServerId,
            };

            var result = await runner.RunAsync(definition, server, ct);
            bodySucceeded = true;
            return result;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(connectionString!, bodySucceeded, async (cleanup, cleanupCt) =>
                await DeleteAsync(cleanup, cleanupCt));
        }
    }

    private static async Task DeleteAsync(NpgsqlConnection connection, CancellationToken ct)
    {
        foreach (var table in new[] { "database_scoped_config", "database_size_stats" })
        {
            using var command = new NpgsqlCommand(
                $"DELETE FROM {table} WHERE server_id = {LiveServerId.ToString(CultureInfo.InvariantCulture)}", connection);
            command.CommandTimeout = 30;
            await command.ExecuteNonQueryAsync(ct);
        }
    }

    private sealed class FakeTargetProvider(object[][][] defaultSets) : ITargetProvider
    {
        public CollectorTargetEngine Engine => CollectorTargetEngine.SqlServer;

        public DbConnection CreateConnection(string connectionString) => new FakeConnection(connectionString);

        public DbCommand CreateCommand(CollectorQuery query, DbConnection connection, int commandTimeoutSeconds) =>
            new FakeCommand(query.Text, (FakeConnection)connection, defaultSets);

        public CollectorTargetFault Classify(Exception exception, bool yieldsOnLockTimeout) => CollectorTargetFault.Unclassified;

        public string WithDatabase(string connectionString, string databaseName) => "db=" + databaseName;

        public (string ConnectionString, CollectorQuery Query) BuildDatabaseListPlan(
            string connectionString, IReadOnlyList<string>? excludedDatabases, IReadOnlyList<string>? databaseScope) =>
            throw new NotSupportedException();
    }

    private sealed class FakeConnection(string connectionString) : DbConnection
    {
        private string _cs = connectionString;

        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string ConnectionString { get => _cs; set => _cs = value ?? string.Empty; }
        public override string Database => string.Empty;
        public override string DataSource => string.Empty;
        public override string ServerVersion => string.Empty;
        public override ConnectionState State => ConnectionState.Open;
        public override void ChangeDatabase(string databaseName) { }
        public override void Close() { }
        public override void Open() { }
        public override Task OpenAsync(CancellationToken cancellationToken) => Task.CompletedTask;
        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();
        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }

    private sealed class FakeCommand(string text, FakeConnection owner, object[][][] defaultSets) : DbCommand
    {
        [System.Diagnostics.CodeAnalysis.AllowNull]
        public override string CommandText { get; set; } = text;
        public override int CommandTimeout { get; set; }
        public override CommandType CommandType { get; set; }
        public override bool DesignTimeVisible { get; set; }
        public override UpdateRowSource UpdatedRowSource { get; set; }
        protected override DbConnection? DbConnection { get => owner; set { } }
        protected override DbParameterCollection DbParameterCollection => throw new NotSupportedException();
        protected override DbTransaction? DbTransaction { get; set; }
        public override void Cancel() { }
        public override int ExecuteNonQuery() => throw new NotSupportedException();
        public override object? ExecuteScalar() => null;
        public override void Prepare() { }
        protected override DbParameter CreateDbParameter() => throw new NotSupportedException();
        protected override DbDataReader ExecuteDbDataReader(CommandBehavior behavior) => Open();
        protected override Task<DbDataReader> ExecuteDbDataReaderAsync(CommandBehavior behavior, CancellationToken cancellationToken) =>
            Task.FromResult(Open());

        /* The per-item query is the EXECUTE [db].sys.sp_executesql form; everything else that reads a reader gets the
           result sets the test chose (an enumeration, or a payload plus its optional trailing failure set). */
        private DbDataReader Open() => CommandText.TrimStart().StartsWith("EXECUTE [", StringComparison.Ordinal)
            ? new Reader(new[] { new object[][] { new object[] { "MAXDOP", "0", DBNull.Value } } })
            : new Reader(defaultSets);
    }

    private sealed class Reader(object[][][] sets) : DbDataReader
    {
        private int _set;
        private int _row = -1;
        private object[] Cur => sets[_set][_row];

        public override bool Read() => ++_row < sets[_set].Length;
        public override bool NextResult() { if (_set + 1 >= sets.Length) { return false; } _set++; _row = -1; return true; }
        public override string GetString(int o) => (string)Cur[o];
        public override bool IsDBNull(int o) => Cur[o] is DBNull;
        public override object GetValue(int o) => Cur[o];
        public override int FieldCount => sets[_set].Length == 0 ? 0 : sets[_set][0].Length;
        public override bool HasRows => sets[_set].Length > 0;
        public override bool IsClosed => false;
        public override int Depth => 0;
        public override int RecordsAffected => -1;
        public override object this[int o] => Cur[o];
        public override object this[string n] => throw new NotSupportedException();
        public override bool GetBoolean(int o) => throw new NotSupportedException();
        public override byte GetByte(int o) => throw new NotSupportedException();
        public override long GetBytes(int o, long d, byte[]? b, int bo, int l) => throw new NotSupportedException();
        public override char GetChar(int o) => throw new NotSupportedException();
        public override long GetChars(int o, long d, char[]? b, int bo, int l) => throw new NotSupportedException();
        public override string GetDataTypeName(int o) => throw new NotSupportedException();
        public override DateTime GetDateTime(int o) => throw new NotSupportedException();
        public override decimal GetDecimal(int o) => throw new NotSupportedException();
        public override double GetDouble(int o) => throw new NotSupportedException();
        public override IEnumerator GetEnumerator() => throw new NotSupportedException();
        public override Type GetFieldType(int o) => throw new NotSupportedException();
        public override float GetFloat(int o) => throw new NotSupportedException();
        public override Guid GetGuid(int o) => throw new NotSupportedException();
        public override short GetInt16(int o) => throw new NotSupportedException();
        public override int GetInt32(int o) => throw new NotSupportedException();
        public override long GetInt64(int o) => throw new NotSupportedException();
        public override string GetName(int o) => throw new NotSupportedException();
        public override int GetOrdinal(string n) => throw new NotSupportedException();
        public override int GetValues(object[] v) => throw new NotSupportedException();
    }
}
