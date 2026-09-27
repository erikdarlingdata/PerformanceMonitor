/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4479: every Darling STORE connection names its surface in <c>ApplicationName</c>, the way the web/MCP
/// pools already did (#4442). <see cref="DarlingStoreConnection.WithApplicationName"/> is the one place that
/// picks the name — set-if-ABSENT, so a bring-your-own connection string's own <c>ApplicationName</c> wins.
/// </summary>
public sealed class StoreApplicationNamePinTests
{
    [Fact]
    public void WithApplicationName_SetsTheNameWhenAbsent()
    {
        var input = "Host=127.0.0.1;Port=5432;Username=darling;Password=pw;Database=darling";

        var output = DarlingStoreConnection.WithApplicationName(input, DarlingManagedPostgres.ServiceApplicationName);

        var parsed = new NpgsqlConnectionStringBuilder(output);
        Assert.Equal(DarlingManagedPostgres.ServiceApplicationName, parsed.ApplicationName);
    }

    [Fact]
    public void WithApplicationName_KeepsAnOperatorSuppliedValue()
    {
        var input = "Host=127.0.0.1;Port=5432;Username=darling;Password=pw;Database=darling;Application Name=OperatorsOwnMonitor";

        var output = DarlingStoreConnection.WithApplicationName(input, DarlingManagedPostgres.ServiceApplicationName);

        var parsed = new NpgsqlConnectionStringBuilder(output);
        Assert.Equal("OperatorsOwnMonitor", parsed.ApplicationName);
    }

    [Fact]
    public void WithApplicationName_ThrowsOnAMalformedString()
    {
        Assert.ThrowsAny<Exception>(() =>
            DarlingStoreConnection.WithApplicationName("Host=127.0.0.1;NotAKeyword=;;;", DarlingManagedPostgres.ServiceApplicationName));
    }

    /// <summary>Each surface's constant is the four-way #4442 pattern generalized (#4479): distinct suffixes
    /// so the same store's <c>pg_stat_activity</c> tells the four surfaces apart at a glance.</summary>
    [Fact]
    public void EachSurfaceConstant_IsDistinctAndCarriesThePrefix()
    {
        var names = new[]
        {
            DarlingManagedPostgres.WebApplicationName,
            DarlingManagedPostgres.McpApplicationName,
            DarlingManagedPostgres.ServiceApplicationName,
            DarlingManagedPostgres.CliApplicationName,
            DarlingManagedPostgres.UpgradeApplicationName,
            ViewerSettings.ApplicationName,
        };

        Assert.Equal(names.Length, new HashSet<string>(names, StringComparer.Ordinal).Count);
        Assert.All(names, name => Assert.StartsWith("PerformanceMonitorDarling-", name, StringComparison.Ordinal));
    }

    /// <summary>
    /// Pins the HELPER and the SHAPE the worker's own site wires (<see cref="DarlingStoreConnection.WithApplicationName"/>
    /// wrapped in <see cref="DarlingStoreConnection.PinSessionTimeZoneUtc"/> straight into
    /// <c>NpgsqlDataSource.Create</c>), read back live from <c>pg_stat_activity</c> — not a product call
    /// site: <see cref="StoreApplicationNameCensusTests"/> covers every real STORE site (including this
    /// one, by name, in its roster), and <c>ViewerApplicationNameLiveTests</c> is the pin that goes through
    /// a real product constructor for the Viewer's own site.
    /// </summary>
    [Fact]
    public async Task ServiceApplicationName_ThroughTheHelper_NamesTheBackendInPgStatActivity()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4479 ApplicationName live pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        try
        {
            /* The same shape DarlingWorker.RunCollectionLoopAsync wires: PinSessionTimeZoneUtc wrapping
               WithApplicationName, straight into NpgsqlDataSource.Create. */
            await using var postgres = NpgsqlDataSource.Create(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    DarlingStoreConnection.WithApplicationName(scratch.ConnectionString, DarlingManagedPostgres.ServiceApplicationName)));

            await using var connection = await postgres.OpenConnectionAsync(ct);
            await using var check = new NpgsqlCommand(
                "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()", connection);
            var applicationName = (string)(await check.ExecuteScalarAsync(ct))!;

            Assert.Equal(DarlingManagedPostgres.ServiceApplicationName, applicationName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }

    /// <summary>
    /// Pins the HELPER and the SHAPE for the Viewer's constant, the same way the sibling test above pins
    /// the worker's — not a product call site: <c>ViewerApplicationNameLiveTests</c> is the pin that
    /// constructs the real <c>ViewerDataService</c> and reads the same fact through its own constructor.
    /// </summary>
    [Fact]
    public async Task ViewerApplicationName_ThroughTheHelper_NamesTheBackendInPgStatActivity()
    {
        var baseConnectionString = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(baseConnectionString),
            "Set DARLING_TEST_PG to a Postgres connection string to run the #4479 Viewer ApplicationName live pin.");

        var ct = TestContext.Current.CancellationToken;
        var bodySucceeded = false;

        await using var scratch = await ScratchPostgres.CreateAsync(baseConnectionString!, ct);

        try
        {
            await using var postgres = NpgsqlDataSource.Create(
                DarlingStoreConnection.PinSessionTimeZoneUtc(
                    DarlingStoreConnection.WithApplicationName(scratch.ConnectionString, ViewerSettings.ApplicationName)));

            await using var connection = await postgres.OpenConnectionAsync(ct);
            await using var check = new NpgsqlCommand(
                "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()", connection);
            var applicationName = (string)(await check.ExecuteScalarAsync(ct))!;

            Assert.Equal(ViewerSettings.ApplicationName, applicationName);

            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
