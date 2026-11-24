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
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9477: a read tool registers its web handler, catalog entry and Custom Views parameters from its own file. The moved tool
/// (<c>get_store_maintenance_readiness</c>) is in none of the shared lists, so every assertion here proves the registration path.
/// </summary>
public sealed class ReadToolRegistrationTests
{
    private const string MovedTool = "get_store_maintenance_readiness";

    private sealed class FakeRegistration(string name) : IReadToolRegistration
    {
        public ReadToolRegistration Read { get; } = new(
            name,
            (c, pg, an) => Task.FromResult("{}"),
            new(DarlingWebEndpoints.CatOverview, "test", Array.Empty<DarlingWebEndpoints.CatalogParam>()));
    }

    [Fact]
    public void ARegisteredTool_IsInTheDispatch_TheCatalog_AndCustomViewsValidation()
    {
        Assert.Contains(MovedTool, DarlingWebEndpoints.BuildReadDispatch().Keys);
        Assert.True(DarlingWebEndpoints.CatalogDescriptors.ContainsKey(MovedTool));
        Assert.Contains(MovedTool, CustomViewValidation.KnownReadNames);
        Assert.NotNull(CustomViewValidation.ReadParams(MovedTool));
        Assert.NotNull(DarlingWebEndpoints.ReadPanelParams(MovedTool));
    }

    [Fact]
    public void ARegisteredTool_CanBeNamedByACustomViewPanel()
    {
        var result = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"read\":\"" + MovedTool + "\",\"viz\":\"table\",\"span\":1}]}");
        Assert.True(result.IsValid, result.Error);
    }

    [Fact]
    public void TheViewerSideTable_CarriesTheRegisteredToolsParameters_InTheCatalogsOrder()
    {
        ReadToolRegistrations.EnsureRegistered();
        var catalog = DarlingWebEndpoints.ReadPanelParams(MovedTool);
        Assert.NotNull(catalog);
        Assert.True(catalog!.SequenceEqual(CustomViewValidation.KnownReadParams[MovedTool]));
    }

    [Fact]
    public async Task ARegisteredTool_AnswersAWebReadCall_Live()
    {
        var cs = Environment.GetEnvironmentVariable("DARLING_TEST_PG");
        Assert.SkipWhen(string.IsNullOrEmpty(cs), "Set DARLING_TEST_PG to run the registered tool's web read test.");

        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create(cs!);
        var raw = await DarlingWebEndpoints.BuildReadDispatch()[MovedTool](new DefaultHttpContext { RequestAborted = ct }, postgres, null!);
        using var doc = JsonDocument.Parse(raw);

        Assert.True(doc.RootElement.GetProperty("safe").ValueKind is JsonValueKind.True or JsonValueKind.False);
        Assert.False(string.IsNullOrEmpty(doc.RootElement.GetProperty("verdict").GetString()));
    }

    [Fact]
    public void ATool_RegisteredTwice_ThrowsAndNamesIt()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ReadToolRegistrations.Collect(new IReadToolRegistration[] { new FakeRegistration("get_twice"), new FakeRegistration("get_twice") }));
        Assert.Contains("get_twice", ex.Message);
    }

    [Fact]
    public void ARegisteredTool_AlsoInASharedList_ThrowsAndNamesIt()
    {
        var shared = new Dictionary<string, DarlingWebEndpoints.ReadToolHandler>(StringComparer.Ordinal)
        {
            ["get_wait_stats"] = (c, pg, an) => Task.FromResult("{}"),
        };
        var ex = Assert.Throws<InvalidOperationException>(() =>
            ReadToolRegistrations.AddTo(shared, ReadToolRegistrations.Collect(new IReadToolRegistration[] { new FakeRegistration("get_wait_stats") }), r => r.Handler));
        Assert.Contains("get_wait_stats", ex.Message);
    }

    [Fact]
    public void StorageRefusesARegistration_ThatDuplicatesASharedRead_AndKeepsTheLastGoodSet()
    {
        ReadToolRegistrations.EnsureRegistered();
        var none = (IReadOnlyList<ReadPanelParam>)Array.Empty<ReadPanelParam>();

        var shared = Assert.Throws<InvalidOperationException>(() =>
            CustomViewValidation.RegisterReads(new[] { new KeyValuePair<string, IReadOnlyList<ReadPanelParam>>("get_wait_stats", none) }));
        Assert.Contains("get_wait_stats", shared.Message);

        var twice = Assert.Throws<InvalidOperationException>(() =>
            CustomViewValidation.RegisterReads(new[]
            {
                new KeyValuePair<string, IReadOnlyList<ReadPanelParam>>("get_twice", none),
                new KeyValuePair<string, IReadOnlyList<ReadPanelParam>>("get_twice", none),
            }));
        Assert.Contains("get_twice", twice.Message);

        Assert.Contains(MovedTool, CustomViewValidation.KnownReadNames);
        Assert.DoesNotContain("get_twice", CustomViewValidation.KnownReadNames);
    }
}
