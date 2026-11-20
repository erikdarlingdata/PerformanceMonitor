/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8570 LIVE: a read's driver <c>CommandTimeout</c> (30 s, <c>StorageCommandDeadlines.McpReadSeconds</c>) fires before the role's
/// <c>statement_timeout</c> (60 s), so the store never reports SQLSTATE 57014 for a read held that long. The tool catches the driver's
/// <see cref="TimeoutException"/> and answers with <c>McpHelpers.FormatError</c>'s sentence, and the web mapping and the read latency
/// record used to find no timeout in it: a generic 500, counted as an error.
/// <para>The hold is a lock, not a sleep: one session holds <c>ACCESS EXCLUSIVE</c> on a table, and a second session's read of it waits for a
/// lock that is never released until the test ends, with a one second <c>CommandTimeout</c>. Nothing races. The exception is caught and
/// formatted the way every tool's catch does, and the answer is classified by the web's own mapping and by the read outcome classifier.</para>
/// </summary>
/* #1776 own-store: every test here boots its own database through ScratchPostgres and works entirely inside
   it, so it cannot race the shared store's live collection and is left out of [Collection("live-postgres")]
   deliberately (the next sweep should not "fix" that). */
public sealed class ClientCommandTimeoutAnswersAsTimeoutLiveTests
{
    private static string? BaseConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task AReadHeldPastItsCommandTimeout_AnswersAs503WithTheTimeoutBody_AndCountsAsATimeout()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(BaseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the live #8570 client timeout test.");

        var ct = TestContext.Current.CancellationToken;
        await using var scratch = await ScratchPostgres.CreateAsync(BaseConnectionString!, ct);
        await using var holder = new NpgsqlConnection(scratch.ConnectionString);
        await using var reader = new NpgsqlConnection(scratch.ConnectionString);
        await holder.OpenAsync(ct);
        await reader.OpenAsync(ct);

        await ExecAsync(holder, "CREATE TABLE held_read (id int)", ct);
        await using var transaction = await holder.BeginTransactionAsync(ct);
        await ExecAsync(holder, "LOCK TABLE held_read IN ACCESS EXCLUSIVE MODE", ct);

        NpgsqlException? caught = null;
        try
        {
            await using var command = new NpgsqlCommand("SELECT count(*) FROM held_read", reader) { CommandTimeout = 1 };
            await command.ExecuteScalarAsync(ct);
        }
        catch (NpgsqlException ex)
        {
            caught = ex;
        }
        finally
        {
            await transaction.RollbackAsync(ct);
        }

        Assert.NotNull(caught);
        Assert.IsNotType<PostgresException>(caught);

        var raw = McpHelpers.FormatError("get_slow_thing", caught);

        var (status, body) = await RunAsync(DarlingWebEndpoints.ToHttpResult(raw, "/api/read/get_slow_thing", NullLogger.Instance, 1, CancellationToken.None));
        Assert.Equal(StatusCodes.Status503ServiceUnavailable, status);
        using var json = JsonDocument.Parse(body);
        Assert.Equal(DarlingWebFailureLog.TimeoutMessage, json.RootElement.GetProperty("error").GetString());

        Assert.Equal(ReadOutcome.Timeout, ReadOutcomeClassifier.ClassifySentence(McpHelpers.ErrorMessageOf(raw), CancellationToken.None));
        Assert.Equal(ReadOutcome.Timeout, ReadOutcomeClassifier.Classify(caught, CancellationToken.None));
    }

    private static async Task ExecAsync(NpgsqlConnection connection, string sql, CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(ct);
    }

    private static async Task<(int Status, string Body)> RunAsync(IResult result)
    {
        var context = new DefaultHttpContext
        {
            RequestServices = new ServiceCollection().AddLogging().BuildServiceProvider(),
        };
        context.Response.Body = new MemoryStream();
        await result.ExecuteAsync(context);
        context.Response.Body.Position = 0;
        return (context.Response.StatusCode, Encoding.UTF8.GetString(((MemoryStream)context.Response.Body).ToArray()));
    }
}
