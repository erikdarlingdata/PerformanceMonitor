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
using System.Net;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The ungated (no-live-store) contract for the #1563 custom-views backend: the V31 migration shape, the pure
/// definition validator (the authority — a bad doc can never be stored), the read catalog's key-parity pin
/// (CatalogDescriptors.Keys == BuildReadDispatch().Keys, so a new read forces a catalog entry) + its WIRE query
/// keys, the loopback determination (host auth arm: the CIDR exemption, NOT a credential exemption — #1649), and the store's pure argument/optimistic-concurrency contract.
/// The live CRUD round-trip + the DB-side grant proof are in the gated <see cref="DarlingCustomViewsLiveTests"/>
/// and <see cref="DarlingSecuritySplitLiveTests"/>.
/// </summary>
public sealed class DarlingCustomViewsTests
{
    /* ── V31 migration shape ── */

    [Fact]
    public void V31_IsRegistered_ConfigQualified_JsonbIdentity_NoBumpTrigger()
    {
        var v31 = PgMigrations.Scripts.Single(s => s.Version == 31);
        Assert.Equal("custom-views-table", v31.Name);

        /* Schema-qualified: a bare CREATE resolves to collect (first in the migrate search_path) — wrong ACL. */
        Assert.Contains("CREATE TABLE IF NOT EXISTS config.custom_views (", v31.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TABLE IF NOT EXISTS custom_views", v31.Sql, StringComparison.Ordinal);

        /* IDENTITY id (no sequence USAGE grant needed), UNIQUE name (dup -> 409), jsonb definition (the DB is the
           second line of defense behind the app validator), an in-place version column (optimistic concurrency). */
        Assert.Contains("id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY", v31.Sql, StringComparison.Ordinal);
        Assert.Contains("name text NOT NULL UNIQUE", v31.Sql, StringComparison.Ordinal);
        Assert.Contains("definition jsonb NOT NULL", v31.Sql, StringComparison.Ordinal);
        Assert.Contains("version integer NOT NULL DEFAULT 1", v31.Sql, StringComparison.Ordinal);

        /* Deliberately NO reload-beacon trigger: custom_views feeds the web renderer, not the collector loop. */
        Assert.DoesNotContain("config_bump_version", v31.Sql, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE TRIGGER", v31.Sql, StringComparison.Ordinal);
    }

    [Fact]
    public void V31_IsTheCustomViewsMigration_AndStorageVersionTracksTheLastOne()
    {
        /* This file's concern is that V31 IS custom-views; newer migrations simply append after it.
           The global "StorageVersion == the last script" invariant is asserted version-agnostically so a
           future migration does not have to churn this test (ScaffoldTests pins the same rule, and
           DarlingObservabilityTests carries the full index->version ladder). */
        Assert.Equal(31, PgMigrations.Scripts[30].Version);
        Assert.Equal(StorageVersion.SchemaVersion, PgMigrations.Scripts[^1].Version);
    }

    /* ── ValidateDefinition: the authority ── */

    [Fact]
    public void ValidateDefinition_AcceptsAWellFormedDefinition()
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\",\"span\":1},{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"span\":2}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Theory]
    [InlineData("#1a2b3c")]   // lower-case hex
    [InlineData("#ABCDEF")]   // upper-case hex (case-insensitive)
    [InlineData("#000000")]
    public void ValidateDefinition_AcceptsAValidSeriesColor(string color)
    {
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"series\":[{\"key\":\"x\",\"label\":\"X\",\"color\":\"" + color + "\"}]}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Fact]
    public void ValidateDefinition_AcceptsAnAbsentSeriesColor()
    {
        /* A series with no color is fine — the renderer falls back to the palette (normalizeColor). */
        var ok = DarlingWebEndpoints.ValidateDefinition(
            "{\"panels\":[{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"series\":[{\"key\":\"x\",\"label\":\"X\"}]}]}");
        Assert.True(ok.IsValid, ok.Error);
    }

    [Theory]
    [InlineData(null, "definition is required")]
    [InlineData("", "definition is required")]
    [InlineData("   ", "definition is required")]
    [InlineData("not json", "not valid JSON")]
    [InlineData("[]", "must be a JSON object")]
    [InlineData("{}", "panels must be an array")]
    [InlineData("{\"panels\":{}}", "panels must be an array")]
    [InlineData("{\"panels\":[]}", "at least one panel")]
    [InlineData("{\"panels\":[123]}", "must be an object")]
    [InlineData("{\"panels\":[{\"viz\":\"table\"}]}", "missing 'read'")]
    [InlineData("{\"panels\":[{\"read\":\"nope\",\"viz\":\"table\"}]}", "unknown read")]
    [InlineData("{\"panels\":[{\"read\":\"get_wait_stats\"}]}", "missing 'viz'")]
    [InlineData("{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"pie\"}]}", "unknown viz")]
    [InlineData("{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\",\"span\":3}]}", "invalid span")]
    [InlineData("{\"panels\":[{\"path\":\"/api/read/get_wait_stats\",\"viz\":\"table\"}]}", "raw 'path' mode")]
    /* series color is a presentation field ValidateDefinition now pins (stored CSS-injection / air-gap break). */
    [InlineData("{\"panels\":[{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"series\":[{\"key\":\"x\",\"color\":\"url(//evil.com/beacon)\"}]}]}", "invalid color")]
    [InlineData("{\"panels\":[{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"series\":[{\"key\":\"x\",\"color\":\"red\"}]}]}", "invalid color")]
    [InlineData("{\"panels\":[{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"series\":[{\"key\":\"x\",\"color\":\"#abc\"}]}]}", "invalid color")]
    [InlineData("{\"panels\":[{\"read\":\"get_cpu_utilization\",\"viz\":\"line\",\"series\":[{\"key\":\"x\",\"color\":123}]}]}", "invalid color")]
    public void ValidateDefinition_RejectsBadDefinitions_NamingTheReason(string? definition, string expectedFragment)
    {
        var result = DarlingWebEndpoints.ValidateDefinition(definition);
        Assert.False(result.IsValid);
        Assert.Contains(expectedFragment, result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsAnOversizedDefinition()
    {
        var huge = "{\"panels\":[{\"read\":\"get_wait_stats\",\"viz\":\"table\",\"title\":\"" +
            new string('x', DarlingWebEndpoints.MaxDefinitionBytes + 1) + "\"}]}";
        var result = DarlingWebEndpoints.ValidateDefinition(huge);
        Assert.False(result.IsValid);
        Assert.Contains("maximum size", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateDefinition_RejectsTooManyPanels()
    {
        var panels = string.Join(",", Enumerable.Repeat("{\"read\":\"get_wait_stats\",\"viz\":\"table\"}", DarlingWebEndpoints.MaxPanelCount + 1));
        var result = DarlingWebEndpoints.ValidateDefinition("{\"panels\":[" + panels + "]}");
        Assert.False(result.IsValid);
        Assert.Contains("maximum is", result.Error!, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("#1a2b3c", true)]
    [InlineData("#ABCDEF", true)]           // case-insensitive
    [InlineData("#000000", true)]
    [InlineData("#abc", false)]             // short hex
    [InlineData("#1a2b3", false)]           // 5 digits
    [InlineData("#1a2b3cd", false)]         // 7 digits
    [InlineData("1a2b3c", false)]           // no leading '#'
    [InlineData("#12345g", false)]          // non-hex digit
    [InlineData("red", false)]              // named color
    [InlineData("url(//evil.com/beacon)", false)] // CSS function (the air-gap-break payload)
    [InlineData("", false)]
    [InlineData(null, false)]
    public void IsHexColor_AcceptsOnlySixDigitHex(string? color, bool expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.IsHexColor(color));

    /* ── catalog: input truth, key-parity with the dispatch ── */

    [Fact]
    public void CatalogDescriptors_Keys_EqualTheReadDispatchKeys()
    {
        var descriptors = DarlingWebEndpoints.CatalogDescriptors.Keys.ToHashSet(StringComparer.Ordinal);
        var dispatch = DarlingWebEndpoints.BuildReadDispatch().Keys.ToHashSet(StringComparer.Ordinal);

        var missing = dispatch.Except(descriptors).OrderBy(n => n, StringComparer.Ordinal).ToArray();
        var extra = descriptors.Except(dispatch).OrderBy(n => n, StringComparer.Ordinal).ToArray();

        Assert.True(missing.Length == 0, "reads with no catalog descriptor: " + string.Join(", ", missing));
        Assert.True(extra.Length == 0, "catalog descriptors with no matching read: " + string.Join(", ", extra));
    }

    [Fact]
    public void Catalog_Viz_IsExactlyKnownViz()
    {
        Assert.Equal(new[] { "table", "line", "stat", "bandlist" }, DarlingWebEndpoints.KnownVizList);
        Assert.True(DarlingWebEndpoints.KnownViz.SetEquals(DarlingWebEndpoints.KnownVizList));

        /* The emitted catalog serves exactly that vocabulary + a read per dispatch entry. */
        var catalog = DarlingWebEndpoints.BuildCatalogNode();
        var viz = ((JsonArray)catalog["viz"]!).Select(n => (string)n!).ToArray();
        Assert.Equal(new[] { "table", "line", "stat", "bandlist" }, viz);
        Assert.Equal(DarlingWebEndpoints.BuildReadDispatch().Count, ((JsonArray)catalog["reads"]!).Count);
    }

    [Fact]
    public void Catalog_ParamsNameTheActualWireQueryKeys_NotCSharpParamNames()
    {
        /* Builder 2 binds these keys onto ?query=strings, so they MUST be the literal keys the dispatch lambdas
           read — the whole reason the metadata is hand-authored (reflection would emit the C# param names). */
        AssertParamKeys("get_query_store_top", "server", "hours", "top", "database_name", "as_of");
        AssertParamKeys("get_wait_stats", "server", "hours", "limit", "as_of");
        AssertParamKeys("get_wait_trend", "wait_type", "server", "hours", "as_of");
        AssertParamKeys("get_plan_xml", "query_hash", "server", "database_name");
        AssertParamKeys("get_top_queries_by_cpu", "server", "hours", "top", "database_name", "parallel_only", "min_dop", "as_of");
        AssertParamKeys("compare_analysis", "server", "hours", "baseline_hours_back", "as_of");
        AssertParamKeys("get_fleet_overview", "hours");
        AssertParamKeys("list_servers");
    }

    [Fact]
    public void Catalog_RequiredFlags_MarkTheRequireTextParams()
    {
        Assert.True(RequiredParam("get_wait_trend", "wait_type"));
        Assert.True(RequiredParam("get_perfmon_trend", "counter_name"));
        Assert.True(RequiredParam("get_query_trend", "query_hash"));
        Assert.True(RequiredParam("get_query_trend", "database_name"));
        Assert.True(RequiredParam("get_plan_xml", "query_hash"));

        /* An optional text param (Str, has no default) is NOT required. */
        Assert.False(RequiredParam("get_query_store_top", "database_name"));
        /* The 'server' selector is optional (a tool auto-selects a sole server). */
        Assert.False(RequiredParam("get_wait_stats", "server"));
    }

    private static void AssertParamKeys(string read, params string[] expected)
    {
        var descriptor = DarlingWebEndpoints.CatalogDescriptors[read];
        Assert.Equal(expected, descriptor.Params.Select(p => p.Name).ToArray());
    }

    private static bool RequiredParam(string read, string paramName) =>
        DarlingWebEndpoints.CatalogDescriptors[read].Params.Single(p => p.Name == paramName).Required;

    /* ── loopback determination (host auth arm: exempts the CIDR test only, never the credential — #1649) ── */

    [Theory]
    [InlineData("127.0.0.1", true)]
    [InlineData("::1", true)]
    [InlineData("::ffff:127.0.0.1", true)]   // IPv4-mapped-IPv6 loopback
    [InlineData("192.168.1.50", false)]
    [InlineData("10.0.0.5", false)]
    [InlineData("::ffff:192.168.1.50", false)] // IPv4-mapped-IPv6 non-loopback
    public void IsLoopbackRemote_DecisionTable(string ip, bool expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.IsLoopbackRemote(IPAddress.Parse(ip)));

    [Fact]
    public void IsLoopbackRemote_NullRemote_IsNotLoopback() =>
        Assert.False(DarlingWebEndpoints.IsLoopbackRemote(null));

    /* ── mutation Content-Type gate (the CSRF defense: POST/PUT/DELETE demand application/json -> 415 else) ── */

    [Theory]
    [InlineData("application/json", true)]
    [InlineData("application/json; charset=utf-8", true)]  // a charset parameter is ignored
    [InlineData("application/json;charset=utf-8", true)]
    [InlineData("APPLICATION/JSON", true)]                 // case-insensitive
    [InlineData("  application/json  ", true)]             // surrounding whitespace tolerated
    [InlineData("text/plain", false)]                      // the simple-request CSRF content type
    [InlineData("text/plain;charset=UTF-8", false)]
    [InlineData("application/x-www-form-urlencoded", false)]
    [InlineData("multipart/form-data; boundary=x", false)]
    [InlineData("application/jsonx", false)]               // not exactly application/json
    [InlineData("", false)]
    [InlineData(null, false)]
    public void IsJsonContentType_RequiresApplicationJson(string? contentType, bool expected) =>
        Assert.Equal(expected, DarlingWebEndpoints.IsJsonContentType(contentType));

    /* ── store: pure argument + optimistic-concurrency contract ── */

    [Theory]
    [InlineData(null, "'name' is required")]
    [InlineData("", "'name' is required")]
    [InlineData("   ", "'name' is required")]
    public void ValidateArgs_RejectsEmptyName(string? name, string fragment)
    {
        var result = CustomViewStore.ValidateArgs(name, "{}");
        var invalid = Assert.IsType<CustomViewResult.Invalid>(result);
        Assert.Contains(fragment, invalid.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateArgs_RejectsOverlongName()
    {
        var result = CustomViewStore.ValidateArgs(new string('n', CustomViewStore.MaxNameLength + 1), "{}");
        var invalid = Assert.IsType<CustomViewResult.Invalid>(result);
        Assert.Contains("maximum length", invalid.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateArgs_RejectsEmptyDefinition()
    {
        var result = CustomViewStore.ValidateArgs("name", "  ");
        var invalid = Assert.IsType<CustomViewResult.Invalid>(result);
        Assert.Contains("'definition' is required", invalid.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void ValidateArgs_PassesAValidPair()
    {
        Assert.Null(CustomViewStore.ValidateArgs("dashboard", "{\"panels\":[]}"));
    }

    [Fact]
    public void UpdateSql_CarriesTheOptimisticConcurrencyContract()
    {
        /* The version-conflict backbone: the UPDATE only touches the row when the caller's version still matches,
           and bumps it in place. A 0-rowcount is what the store disambiguates into NotFound vs stale Conflict. */
        Assert.Contains("WHERE id = $1 AND version = $6", CustomViewStore.UpdateSql, StringComparison.Ordinal);
        Assert.Contains("version = version + 1", CustomViewStore.UpdateSql, StringComparison.Ordinal);
        Assert.Contains("RETURNING", CustomViewStore.UpdateSql, StringComparison.Ordinal);

        /* The list read extracts only the kind SCALAR (definition->>'kind') for the badge/route — it must still
           NOT select the (potentially large) definition BODY column. Positive guard so it stays meaningful:
           after removing the scalar extraction, no other 'definition' reference may remain in the query (so it
           still fails if someone later drags the whole body into the list SELECT). */
        Assert.Contains("definition->>'kind'", CustomViewStore.ListSql, StringComparison.Ordinal);
        var listWithoutKindScalar = CustomViewStore.ListSql.Replace("definition->>'kind'", string.Empty, StringComparison.Ordinal);
        Assert.DoesNotContain("definition", listWithoutKindScalar, StringComparison.Ordinal);
    }

    [Fact]
    public void CustomViewResult_IsAClosedDiscriminatedUnion()
    {
        CustomViewResult ok = new CustomViewResult.Ok(null);
        CustomViewResult notFound = new CustomViewResult.NotFound();
        CustomViewResult conflict = new CustomViewResult.Conflict("stale");
        CustomViewResult invalid = new CustomViewResult.Invalid("bad");

        Assert.IsType<CustomViewResult.Ok>(ok);
        Assert.IsType<CustomViewResult.NotFound>(notFound);
        Assert.Equal("stale", Assert.IsType<CustomViewResult.Conflict>(conflict).Message);
        Assert.Equal("bad", Assert.IsType<CustomViewResult.Invalid>(invalid).Message);
    }
}
