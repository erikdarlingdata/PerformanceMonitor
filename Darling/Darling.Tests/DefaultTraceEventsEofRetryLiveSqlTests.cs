/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6381: the default trace collector's REAL batch run against a live SQL Server. SQL Server on Linux answers a read of
/// the active trace file with Msg 568 ("Encountered an error or an unexpected end of trace file") for 5-7 seconds each time
/// it grows that file by a megabyte. The batch now reads the trace into a table variable inside TRY...CATCH and retries that
/// one error on the server (5 times, 2 seconds apart), so the host never sees it. Msg 568 cannot be raised on demand
/// (RAISERROR refuses system message numbers, and the file is the server's own), so the retry itself was proved against a
/// throwaway container by truncating a trace file of its own mid-record, recorded in the pull request. What this test holds
/// is the other half of the contract: the wrapped batch still returns the shape
/// <see cref="DefaultTraceEventsCollector.ReadAsync"/> reads (the event set first, the trace-path set second) and records the path.
///
/// <para>Gated on <c>DARLING_TEST_SQL</c> (the SQL Server host; optional <c>DARLING_TEST_SQL_USER</c> and
/// <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth), so it skips where the runner has no SQL Server. It only reads.</para>
/// </summary>
public sealed class DefaultTraceEventsEofRetryLiveSqlTests
{
    private static string? ConnectionString()
    {
        var host = Environment.GetEnvironmentVariable("DARLING_TEST_SQL");
        if (string.IsNullOrEmpty(host))
        {
            return null;
        }

        var user = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_USER");
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = host,
            InitialCatalog = "master",
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "pm-6381-default-trace-eof-live-test",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 1,
            ServerName = "live-sql",
            CollectionTime = DateTime.UtcNow,
            Deltas = null!,
            Target = new CollectorTargetInfo(),
            State = new Dictionary<string, string>(StringComparer.Ordinal),
            HasCollectedBefore = false,
        };

    [Fact]
    public async Task WrappedBatch_ReadsThroughReadAsync_AndRecordsThePath()
    {
        var connectionString = ConnectionString();
        Assert.SkipWhen(connectionString is null, "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live default trace EOF retry test.");
        var ct = TestContext.Current.CancellationToken;

        var context = MakeContext();
        var query = DefaultTraceEventsCollector.Instance.BuildQuery(context);

        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);

        /* The first run reads the whole rollover set, the widest read the collector makes. */
        await using (var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            var rows = await DefaultTraceEventsCollector.Instance.ReadAsync(reader, context, ct);
            Assert.NotNull(rows);
        }

        /* The second result set was reached and read: the path the next cycle compares against. */
        Assert.True(
            context.PendingState.ContainsKey(DefaultTraceEventsCollector.LastTraceFilePathStateKey),
            "the batch's trailing trace-path result set was not read; the wrapped read broke the result-set order ReadAsync expects");
    }
}
