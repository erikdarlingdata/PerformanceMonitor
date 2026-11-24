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
[Collection("live-postgres")]
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

    private sealed class FakeParams(string read, params ReadPanelParam[] declared) : IReadParamsRegistration
    {
        public string Read => read;

        public IReadOnlyList<ReadPanelParam> Params { get; } = declared;
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

    /// <summary>The two halves of a registration are written in two projects, so this is what stops them drifting: every read either side
    /// registers is registered by the other, with the same parameter names and required flags.</summary>
    [Fact]
    public void EveryRegisteredRead_HasBothHalves_WithTheSameParameters()
    {
        var service = ReadToolRegistrations.All.ToDictionary(r => r.Name, StringComparer.Ordinal);
        var storage = ReadParamsRegistrations.Discover().ToDictionary(r => r.Read, StringComparer.Ordinal);

        Assert.True(
            new HashSet<string>(service.Keys, StringComparer.Ordinal).SetEquals(storage.Keys),
            "The Service registers " + string.Join(", ", service.Keys) + " and Storage registers " + string.Join(", ", storage.Keys));
        foreach (var (name, registration) in service)
        {
            var catalogParams = registration.Catalog.Params.Select(p => new ReadPanelParam(p.Name, p.Required)).ToList();
            Assert.True(catalogParams.SequenceEqual(storage[name].Params), $"The catalog and Storage disagree on the parameters of '{name}'.");
        }
    }

    [Fact]
    public void StorageRefusesARegistration_ThatDuplicatesASharedRead_OrAnotherRegistration_AndNamesIt()
    {
        var sharedNames = new[] { "get_wait_stats" };
        var sharedParams = new Dictionary<string, IReadOnlyList<ReadPanelParam>>(StringComparer.Ordinal)
        {
            ["get_wait_stats"] = Array.Empty<ReadPanelParam>(),
        };

        var shared = Assert.Throws<InvalidOperationException>(() =>
            ReadParamsRegistrations.Merge(sharedNames, sharedParams, new IReadParamsRegistration[] { new FakeParams("get_wait_stats") }));
        Assert.Contains("get_wait_stats", shared.Message);

        var twice = Assert.Throws<InvalidOperationException>(() =>
            ReadParamsRegistrations.Merge(sharedNames, sharedParams, new IReadParamsRegistration[] { new FakeParams("get_twice"), new FakeParams("get_twice") }));
        Assert.Contains("get_twice", twice.Message);

        var merged = ReadParamsRegistrations.Merge(sharedNames, sharedParams, new IReadParamsRegistration[] { new FakeParams("get_once", new ReadPanelParam("hours", true)) });
        Assert.Equal(new[] { "get_wait_stats", "get_once" }, merged.Names);
        Assert.True(merged.Params["get_once"].Single().Required);
    }
}
