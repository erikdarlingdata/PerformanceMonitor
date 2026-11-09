/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Reflection;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8451: a daily-summary block is keyed by the STORE a data source reaches, not by the data source itself. The worker,
/// the web host and the MCP host each hold their own pool over one store, with their own login and application name,
/// and the warmer builds a block through the worker's pool while a visitor reads through the web host's. No store is
/// opened here: <c>NpgsqlDataSource.Create</c> connects nothing.
/// </summary>
public sealed class DailySummaryRangeCacheStoreKeyTests
{
    private const string WorkerPool =
        "Host=store.example;Port=5433;Database=darling;Username=darling_service;Application Name=Darling collector;Maximum Pool Size=20";

    private const string WebPool =
        "Host=store.example;Port=5433;Database=darling;Username=darling_viewer;Application Name=Darling web;Maximum Pool Size=8;Search Path=collect,config,public";

    [Fact]
    public void StoreKeyFor_TwoPoolsOverOneStore_ShareOneKey()
    {
        using var worker = NpgsqlDataSource.Create(WorkerPool);
        using var web = NpgsqlDataSource.Create(WebPool);

        Assert.Equal(StoreKeyFor(worker), StoreKeyFor(web));
    }

    [Theory]
    [InlineData("Host=store.example;Port=5433;Database=darling_other;Username=darling_service")]
    [InlineData("Host=other.example;Port=5433;Database=darling;Username=darling_service")]
    [InlineData("Host=store.example;Port=5434;Database=darling;Username=darling_service")]
    public void StoreKeyFor_AnotherDatabaseHostOrPort_IsAnotherKey(string otherStore)
    {
        using var worker = NpgsqlDataSource.Create(WorkerPool);
        using var other = NpgsqlDataSource.Create(otherStore);

        Assert.NotEqual(StoreKeyFor(worker), StoreKeyFor(other));
    }

    private static object StoreKeyFor(NpgsqlDataSource postgres)
    {
        var method = typeof(DailySummaryRangeCache).GetMethod(
            "StoreKeyFor", BindingFlags.Public | BindingFlags.Static, [typeof(NpgsqlDataSource)]);
        Assert.True(method is not null,
            "DailySummaryRangeCache has no StoreKeyFor(NpgsqlDataSource): the service keys a block by the data source instance, so a block built through one pool over a store is never found by a read through another pool over the same store.");
        return method!.Invoke(null, [postgres])!;
    }
}
