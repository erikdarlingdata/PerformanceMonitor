/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7643: what the three schema history reads refuse before they touch the store: a window or page out of range, a missing or
/// malformed name, and a definition text search at a data-detail level that hides the text. Each refusal names the parameter and no
/// store is needed, so none of them can be answered by a read.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingMcpSchemaToolsTests
{
    private static readonly DateTime s_now = new(2026, 11, 7, 12, 0, 0, DateTimeKind.Utc);

    private static void AssertRefusal(string json, string parameter, string messagePart)
    {
        using var doc = JsonDocument.Parse(json);
        Assert.Equal("invalid", doc.RootElement.GetProperty("status").GetString());
        Assert.Equal(parameter, doc.RootElement.GetProperty("hints").GetProperty("parameter").GetString());
        Assert.Contains(messagePart, doc.RootElement.GetProperty("message").GetString(), StringComparison.Ordinal);
    }

    private static Task<string> Definition(string? database = "SalesDb", string? name = "dbo.Orders", string? asOf = null, int offset = 0, int limit = 200) =>
        DarlingMcpSchemaTools.ReadObjectDefinition(null!, "srv", database, name, asOf, offset, limit, CancellationToken.None);

    private static Task<string> Changes(int hoursBack = 24, int limit = 50, int offset = 0, string? name = null) =>
        DarlingMcpSchemaTools.ReadSchemaChanges(null!, "srv", null, name, hoursBack, null, false, limit, offset, CancellationToken.None);

    private static Task<string> Find(
        string? database = null, string? prefix = "Ord", string? column = null, string? references = null, string? text = null, int limit = 50,
        McpDataDetail detail = McpDataDetail.Full) =>
        DarlingMcpSchemaTools.FindSchemaObjects(
            null!, "srv", database, prefix, null, column, references, text, limit, detail, DarlingMcpSchemaTools.BudgetMilliseconds, s_now, CancellationToken.None);

    [Fact]
    public async Task GetObjectDefinition_RefusesAMissingDatabaseOrName()
    {
        AssertRefusal(await Definition(database: " "), "database_name", "required");
        AssertRefusal(await Definition(name: null), "object_name", "required");
        AssertRefusal(await Definition(name: "dbo."), "object_name", "required");
    }

    [Fact]
    public async Task GetObjectDefinition_RefusesAnOutOfRangeColumnPageAndABadAsOf()
    {
        AssertRefusal(await Definition(offset: -1), "column_offset", "zero or a positive");
        AssertRefusal(await Definition(limit: 0), "column_limit", "positive integer");
        AssertRefusal(await Definition(limit: DarlingMcpSchemaTools.MaxColumnLimit + 1), "column_limit", "exceeds maximum");
        AssertRefusal(await Definition(asOf: "last tuesday"), "as_of", "Invalid as_of");
    }

    [Fact]
    public async Task GetSchemaChanges_RefusesAWindowOrPageOutOfRange()
    {
        AssertRefusal(await Changes(hoursBack: 169), "hours_back", "168");
        AssertRefusal(await Changes(hoursBack: 0), "hours_back", "Invalid");
        AssertRefusal(await Changes(limit: 0), "limit", "positive integer");
        AssertRefusal(await Changes(limit: DarlingMcpSchemaTools.MaxChangesLimit + 1), "limit", "exceeds maximum");
        AssertRefusal(await Changes(offset: -5), "offset", "zero or a positive");
        AssertRefusal(await Changes(name: "."), "object_name", "schema.name");
    }

    [Fact]
    public async Task FindSchemaObjects_RefusesDefinitionTextAtALevelThatHidesIt()
    {
        AssertRefusal(await Find(database: "SalesDb", text: "secret", detail: McpDataDetail.MaskLiterals), "definition_contains", "maskLiterals");
        AssertRefusal(await Find(database: "SalesDb", text: "secret", detail: McpDataDetail.MetadataOnly), "definition_contains", "metadataOnly");
    }

    [Fact]
    public async Task FindSchemaObjects_RefusesWhatItCannotServe()
    {
        AssertRefusal(await Find(text: "secret"), "database_name", "needs database_name");
        AssertRefusal(await Find(database: "SalesDb", text: "ab"), "definition_contains", "at least 3");
        AssertRefusal(await Find(limit: 0), "limit", "positive integer");
        AssertRefusal(await Find(limit: DarlingMcpSchemaTools.MaxFindLimit + 1), "limit", "exceeds maximum");
        AssertRefusal(await Find(prefix: null), "name_prefix", "at least one filter");
        AssertRefusal(await Find(prefix: null, references: "."), "references_table", "schema.table");
    }
}
