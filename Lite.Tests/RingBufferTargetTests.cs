/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8146: the ring_buffer target clause every deadlock, blocked-process and long-query create writes, the clamp of the
/// setting that feeds it, and what the on-prem creates of Lite send.
/// </summary>
public sealed class RingBufferTargetTests
{
    [Fact]
    public void Sql_WithLimit_CarriesBothOptions()
    {
        var sql = RingBufferTarget.Sql(5000);

        Assert.Contains("max_memory = 4096", sql, StringComparison.Ordinal);
        Assert.Contains("max_events_limit = 5000", sql, StringComparison.Ordinal);
    }

    private static IEnumerable<int> Numbers(Exception ex) => ex is SqlException sql ? [sql.Number] : [];

    /* The Azure SQL Database long-query creates (both apps) go through CreateWithoutLimitOnRefusalAsync, because
       max_events_limit is unverified there: a refusal gets one retry without the option, so the session still exists. */
    [Theory]
    [InlineData(25629)]
    [InlineData(25713)]
    public async Task AzureCreate_OptionRefused_IsRetriedOnceWithoutTheLimit(int refusal)
    {
        var sent = new List<int?>();

        await RingBufferTarget.CreateWithoutLimitOnRefusalAsync(5000, limit =>
        {
            sent.Add(limit);
            return limit is null ? Task.CompletedTask : Task.FromException(SqlExceptionFactory.Create(refusal, errorClass: 16, message: "refused"));
        }, Numbers);

        Assert.Equal(new int?[] { 5000, null }, sent);
    }

    [Fact]
    public async Task AzureCreate_Accepted_IsSentOnceWithTheLimit()
    {
        var sent = new List<int?>();

        await RingBufferTarget.CreateWithoutLimitOnRefusalAsync(5000, limit =>
        {
            sent.Add(limit);
            return Task.CompletedTask;
        }, Numbers);

        Assert.Equal(new int?[] { 5000 }, sent);
    }


    /* #8146: only the engine refusing the option is worth a retry without it. A create that fails for another reason (a read-only
       database, a missing permission) would fail again, and sending it twice doubles the load on a server that said no. */
    [Theory]
    [InlineData(3906)]
    [InlineData(262)]
    [InlineData(1088)]
    public async Task AzureCreate_FailureUnrelatedToTheOption_IsNotRetried_AndReachesTheCaller(int number)
    {
        var sent = new List<int?>();

        var ex = await Assert.ThrowsAsync<SqlException>(() => RingBufferTarget.CreateWithoutLimitOnRefusalAsync(5000, limit =>
        {
            sent.Add(limit);
            return Task.FromException(SqlExceptionFactory.Create(number, errorClass: 16, message: "not about the limit"));
        }, Numbers));

        Assert.Equal(number, ex.Number);
        Assert.Equal(new int?[] { 5000 }, sent);
    }
    [Fact]
    public async Task AzureCreate_AlreadyPresent_IsNotRetried_AndReachesTheCaller()
    {
        var sent = new List<int?>();

        var ex = await Assert.ThrowsAsync<SqlException>(() => RingBufferTarget.CreateWithoutLimitOnRefusalAsync(5000, limit =>
        {
            sent.Add(limit);
            return Task.FromException(SqlExceptionFactory.Create(25631, errorClass: 16, message: "already exists"));
        }, Numbers));

        Assert.Equal(25631, ex.Number);
        Assert.Equal(new int?[] { 5000 }, sent);
    }

    [Fact]
    public async Task AzureCreate_Cancelled_IsNotRetried()
    {
        var sent = new List<int?>();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => RingBufferTarget.CreateWithoutLimitOnRefusalAsync(5000, limit =>
        {
            sent.Add(limit);
            return Task.FromCanceled(new CancellationToken(canceled: true));
        }, Numbers));

        Assert.Equal(new int?[] { 5000 }, sent);
    }

    [Fact]
    public async Task AzureCreate_RetryAlsoRefused_SurfacesTheRetrysException()
    {
        var ex = await Assert.ThrowsAsync<SqlException>(() => RingBufferTarget.CreateWithoutLimitOnRefusalAsync(5000, limit =>
            Task.FromException(SqlExceptionFactory.Create(limit is null ? 1088 : 25629, errorClass: 16, message: "refused")), Numbers));

        Assert.Equal(1088, ex.Number);
    }

    /* #8146: the create and the start go as one batch, so a refused first attempt may have left a session behind. When the retry
       reports it as already there (25631) or already started (25705), the callers would swallow that as benign and the session
       could stay stopped with nothing logged, so the first attempt's exception surfaces instead. */
    [Theory]
    [InlineData(25631)]
    [InlineData(25705)]
    public async Task AzureCreate_OptionRefused_RetryReportsAlreadyPresent_SurfacesTheFirstException(int alreadyPresent)
    {
        var sent = new List<int?>();

        var ex = await Assert.ThrowsAsync<SqlException>(() => RingBufferTarget.CreateWithoutLimitOnRefusalAsync(5000, limit =>
        {
            sent.Add(limit);
            return Task.FromException(SqlExceptionFactory.Create(limit is null ? alreadyPresent : 25629, errorClass: 16, message: "refused"));
        }, Numbers));

        Assert.Equal(25629, ex.Number);
        Assert.Equal(new int?[] { 5000, null }, sent);
    }

    [Fact]
    public void Sql_Null_HasNoLimit()
    {
        var sql = RingBufferTarget.Sql(null);

        Assert.Contains("SET max_memory = 4096", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("max_events_limit", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(null, 1000)]
    [InlineData(500, 1000)]
    [InlineData(1000, 1000)]
    [InlineData(20000, 10000)]
    [InlineData(4321, 4321)]
    public void Clamp_Bounds(int? configured, int expected)
    {
        Assert.Equal(expected, RingBufferTarget.Clamp(configured));
    }

    [Fact]
    public void OnPremDeadlockCreate_CarriesLimit()
    {
        var sql = RemoteCollectorService.BuildDeadlockCreateSql(7500);

        Assert.Contains("max_events_limit = 7500", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE EVENT SESSION [" + DeadlocksCollector.XeSessionName + "]", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void OnPremBlockedProcessCreate_CarriesLimit()
    {
        var sql = RemoteCollectorService.BuildBlockedProcessCreateSql(7500);

        Assert.Contains("max_events_limit = 7500", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE EVENT SESSION [" + BlockedProcessReportCollector.XeSessionName + "]", sql, StringComparison.Ordinal);
    }
}
