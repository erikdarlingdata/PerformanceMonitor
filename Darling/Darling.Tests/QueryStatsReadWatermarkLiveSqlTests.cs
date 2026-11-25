/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service.Targets;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9533 against a live SQL Server: the statement the collector sends, built by <see cref="QueryStatsCollector.BuildQuery"/> and
/// bound by the host's own command builder, run twice. The first read arms the watermark. The second read must not return a
/// statement that completed before it, and must return one that STARTED before the first read and COMPLETED after it.
///
/// <para>The slow statement is a SELECT blocked by a table lock another connection holds, until the test commits that connection
/// after the first read. Its <c>last_execution_time</c> is its start, before the watermark, and its row appears (a WAITFOR leaves
/// none) only when it completes. A test on the start time alone would lose it. The second read is read raw, not through <c>ReadAsync</c>, because the host's idle
/// filter would drop an idle row and hide the statement-level exclusion this proves.</para>
///
/// <para>The test creates one throwaway database, and drops it, even on failure. Gated on
/// <c>DARLING_TEST_SQL</c> (optional <c>DARLING_TEST_SQL_USER</c> and <c>DARLING_TEST_SQL_PASSWORD</c> for SQL auth).</para>
/// </summary>
public sealed class QueryStatsReadWatermarkLiveSqlTests
{
    private const string SkipMessage =
        "Set DARLING_TEST_SQL (and, for SQL auth, DARLING_TEST_SQL_USER / DARLING_TEST_SQL_PASSWORD) to run the live query_stats watermark test.";

    /// <summary>The ordinal of <c>query_text</c> in the candidate query's select list, with the text read inline.</summary>
    private const int QueryTextOrdinal = 38;

    private static string? ConnectionString(string database)
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
            InitialCatalog = database,
            TrustServerCertificate = true,
            Encrypt = SqlConnectionEncryptOption.Optional,
            ApplicationName = "darling-qs-watermark-live",
            IntegratedSecurity = string.IsNullOrEmpty(user),
        };

        if (!string.IsNullOrEmpty(user))
        {
            builder.UserID = user;
            builder.Password = Environment.GetEnvironmentVariable("DARLING_TEST_SQL_PASSWORD") ?? string.Empty;
        }

        return builder.ConnectionString;
    }

    private static async Task ExecuteAsync(SqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new SqlCommand(sql, connection) { CommandTimeout = 60 };
        await command.ExecuteNonQueryAsync(ct);
    }

    private static CollectorContext Context(CollectorDeltaCalculator deltas, bool capturePlan) => new()
    {
        ServerId = 9533,
        ServerName = "live-sql",
        CollectionTime = DateTime.UtcNow,
        Deltas = deltas,
        Target = new CollectorTargetInfo(),
        CapturePlanXml = capturePlan,
        DeferPlanXmlFetch = false,
    };

    /// <summary>The first read: the real build and the real <c>ReadAsync</c>, which arms the watermark for the next build.</summary>
    private static async Task<List<QueryStatsCollector.Row>> FirstReadAsync(
        string connectionString, CollectorDeltaCalculator deltas, bool capturePlan, CancellationToken ct)
    {
        var context = Context(deltas, capturePlan);
        var query = QueryStatsCollector.Instance.BuildQuery(context);
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await QueryStatsCollector.Instance.ReadAsync(reader, context, ct);
    }

    /// <summary>The second read: the real build, which takes the watermark, read raw. Returns the statement texts the server returned.</summary>
    private static async Task<(List<string> Texts, ReadWatermark? Taken)> SecondReadAsync(
        string connectionString, CollectorDeltaCalculator deltas, bool capturePlan, CancellationToken ct)
    {
        var context = Context(deltas, capturePlan);
        var query = QueryStatsCollector.Instance.BuildQuery(context);
        var taken = context.ReadWatermarkTaken;
        var texts = new List<string>();
        await using var connection = new SqlConnection(connectionString);
        await connection.OpenAsync(ct);
        await using var command = SqlServerTargetProvider.Instance.CreateCommand(query, connection, 60);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (!await reader.IsDBNullAsync(QueryTextOrdinal, ct))
            {
                texts.Add(reader.GetString(QueryTextOrdinal));
            }
        }

        return (texts, taken);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TheSecondRead_SkipsWhatCompletedBeforeTheWatermark_AndKeepsWhatStartedBeforeTheFirstReadAndCompletedAfterIt(bool capturePlan)
    {
        var master = ConnectionString("master");
        Assert.SkipWhen(master is null, SkipMessage);

        var ct = TestContext.Current.CancellationToken;
        var tag = Guid.NewGuid().ToString("N");
        var database = "qs9533_" + tag[..12];
        var completedBefore = "qs9533A" + tag;
        var spanning = "qs9533B" + tag;

        await using var admin = new SqlConnection(master!);
        await admin.OpenAsync(ct);
        await using var blocker = new SqlConnection(master!);
        await blocker.OpenAsync(ct);
        var created = false;
        Task? slow = null;
        try
        {
            await ExecuteAsync(admin, $"CREATE DATABASE [{database}];", ct);
            created = true;

            await using (var work = new SqlConnection(ConnectionString(database)))
            {
                await work.OpenAsync(ct);
                await ExecuteAsync(work, "CREATE TABLE dbo.t (id integer NOT NULL);", ct);
                await ExecuteAsync(blocker, $"USE [{database}]; BEGIN TRANSACTION; INSERT dbo.t WITH (TABLOCKX) (id) VALUES (1);", ct);

                /* The spanning statement starts now and waits for the table lock. */
                var slowConnection = new SqlConnection(ConnectionString(database));
                slow = Task.Run(async () =>
                {
                    await using (slowConnection)
                    {
                        await slowConnection.OpenAsync(ct);
                        await using var command = new SqlCommand($"SELECT c = COUNT_BIG(*) /* {spanning} */ FROM dbo.t;", slowConnection) { CommandTimeout = 60 };
                        await command.ExecuteScalarAsync(ct);
                    }
                }, ct);

                await Task.Delay(TimeSpan.FromSeconds(1), ct);
                for (var i = 0; i < 2; i++)
                {
                    await ExecuteAsync(work, $"SELECT c = COUNT_BIG(*) /* {completedBefore} */ FROM sys.objects;", ct);
                }
            }

            /* Five seconds after the completed statement: its completion is before the first read's clock minus two seconds,
               and the spanning statement is still running. */
            await Task.Delay(TimeSpan.FromSeconds(5), ct);
            var deltas = new CollectorDeltaCalculator();
            var first = await FirstReadAsync(master!, deltas, capturePlan, ct);
            Assert.True(
                first.Any(r => r.QueryText is not null && r.QueryText.Contains(completedBefore, StringComparison.Ordinal)),
                $"The first read did not return the statement that completed before it. Rows: {first.Count}.");
            Assert.DoesNotContain(first, r => r.QueryText is not null && r.QueryText.Contains(spanning, StringComparison.Ordinal));

            await ExecuteAsync(blocker, "COMMIT TRANSACTION;", ct);
            await slow;
            slow = null;
            await Task.Delay(TimeSpan.FromSeconds(1), ct);

            var (texts, taken) = await SecondReadAsync(master!, deltas, capturePlan, ct);
            Assert.NotNull(taken);
            Assert.True(
                texts.Any(t => t.Contains(spanning, StringComparison.Ordinal)),
                $"The second read did not return the spanning statement. Rows: {texts.Count}. Watermark: {taken}.");
            Assert.DoesNotContain(texts, t => t.Contains(completedBefore, StringComparison.Ordinal));
        }
        finally
        {
            if (slow is not null)
            {
                try
                {
                    await slow.WaitAsync(TimeSpan.FromSeconds(30), CancellationToken.None);
                }
                catch (Exception)
                {
                    /* The cleanup below drops the database whatever state the statement is in. */
                }
            }

            if (blocker.State == System.Data.ConnectionState.Open)
            {
                try
                {
                    await ExecuteAsync(blocker, "IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;", CancellationToken.None);
                }
                catch (Exception)
                {
                    /* The drop below rolls the connection back if this did not. */
                }
            }

            if (created)
            {
                await ExecuteAsync(admin, $"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE [{database}];", CancellationToken.None);
            }
        }
    }
}
