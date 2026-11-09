/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8400: what <see cref="PgStatementTable.RunAsync"/> surfaces when the counter read after a body fails. The reads around
/// the body are supplied, so no server is behind the connection and the counter fails where the test says.
/// </summary>
public sealed class PgStatementTableTests
{
    private static Task NoHeadroom(NpgsqlConnection connection, CancellationToken ct) => Task.CompletedTask;

    [Fact]
    public async Task ABodyWhoseFailureBrokeTheConnection_SurfacesThatFailure_NotTheCounterReadsRefusalAfterIt()
    {
        var bodyFailure = new NpgsqlException("Exception while reading from stream");
        var counterFailure = new InvalidOperationException("Connection is not open");
        await using var connection = new NpgsqlConnection();
        var counterReads = 0;

        var thrown = await Assert.ThrowsAsync<AggregateException>(() => PgStatementTable.RunAsync(
            connection, CancellationToken.None, _ => Task.FromException(bodyFailure), forcePass: false, NoHeadroom,
            (_, _) => ++counterReads == 1 ? Task.FromResult<long?>(0) : Task.FromException<long?>(counterFailure)));

        Assert.Equal(2, counterReads);
        Assert.Equal(new Exception[] { bodyFailure, counterFailure }, thrown.InnerExceptions);
    }

    [Fact]
    public async Task ACounterReadThatFailsAfterABodyThatDidNot_SurfacesAsItself()
    {
        var counterFailure = new InvalidOperationException("Connection is not open");
        await using var connection = new NpgsqlConnection();
        var counterReads = 0;

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => PgStatementTable.RunAsync(
            connection, CancellationToken.None, _ => Task.CompletedTask, forcePass: false, NoHeadroom,
            (_, _) => ++counterReads == 1 ? Task.FromResult<long?>(0) : Task.FromException<long?>(counterFailure)));

        Assert.Same(counterFailure, thrown);
    }

    [Fact]
    public async Task ABodysFailureWithNoPassInside_SurfacesAsItself()
    {
        var bodyFailure = new NpgsqlException("Exception while reading from stream");
        await using var connection = new NpgsqlConnection();

        var thrown = await Assert.ThrowsAsync<NpgsqlException>(() => PgStatementTable.RunAsync(
            connection, CancellationToken.None, _ => Task.FromException(bodyFailure), forcePass: false, NoHeadroom,
            (_, _) => Task.FromResult<long?>(7)));

        Assert.Same(bodyFailure, thrown);
    }
}
