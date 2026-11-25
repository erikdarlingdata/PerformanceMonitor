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
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <c>docs/darling/mcp-tool-tables.md</c> lists every tool the service can serve exactly once, in the table its kind belongs in. The doc is
/// read as data: its three tool tables, one row per tool. A tool that changes something (a write or an investigation tool) is in the third
/// table and every other tool is in one of the first two, so the doc and the write policy cannot disagree about which tools write.
/// </summary>
public sealed class DarlingMcpToolTablesDocTests
{
    private const string ReadTable = "Tools that read collected data";
    private const string SelfTable = "Tools about Darling itself";
    private const string ChangeTable = "Tools that change something";

    private static readonly Regex Row = new(@"^\| `([a-z0-9_]+)` \|", RegexOptions.CultureInvariant);

    /// <summary>Each "## " heading of the doc with the tool names of the rows under it, in file order.</summary>
    private static List<(string Heading, List<string> Tools)> Tables()
    {
        var tables = new List<(string Heading, List<string> Tools)>();
        foreach (var line in RepoFile.ReadRepoFileLf("docs", "darling", "mcp-tool-tables.md").Split('\n'))
        {
            if (line.StartsWith("## ", StringComparison.Ordinal))
            {
                tables.Add((line[3..].Trim(), new List<string>()));
            }
            else if (tables.Count > 0 && Row.Match(line) is { Success: true } row)
            {
                tables[^1].Tools.Add(row.Groups[1].Value);
            }
        }

        return tables;
    }

    private static IReadOnlySet<string> Registered() =>
        DarlingMcpToolTypes.Tools.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);

    [Fact]
    public void TheDoc_HasTheThreeToolTables_AndTheirHeadingsCarryNoCount()
    {
        var withRows = Tables().Where(t => t.Tools.Count > 0).Select(t => t.Heading).ToList();

        Assert.Equal(new[] { ReadTable, SelfTable, ChangeTable }, withRows);
    }

    [Fact]
    public void EveryRegisteredTool_HasExactlyOneRow_AndEveryRowIsARegisteredTool()
    {
        var rows = Tables().SelectMany(t => t.Tools).ToList();
        var registered = Registered();
        Assert.True(registered.Count > 200, $"the registry walk found only {registered.Count} tools");

        var missing = registered.Where(n => !rows.Contains(n, StringComparer.Ordinal)).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var twice = rows.GroupBy(n => n, StringComparer.Ordinal).Where(g => g.Count() > 1).Select(g => g.Key).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var stale = rows.Where(n => !registered.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(missing.Count == 0, $"These tools have no row in docs/darling/mcp-tool-tables.md: {string.Join(", ", missing)}");
        Assert.True(twice.Count == 0, $"These tools have more than one row in docs/darling/mcp-tool-tables.md: {string.Join(", ", twice)}");
        Assert.True(stale.Count == 0, $"These rows in docs/darling/mcp-tool-tables.md name no registered tool: {string.Join(", ", stale)}");
    }

    [Fact]
    public void TheChangeTable_HoldsExactlyTheWriteAndInvestigationTools()
    {
        var change = Tables().Single(t => t.Heading == ChangeTable).Tools.ToHashSet(StringComparer.Ordinal);
        var writes = DarlingMcpWritePolicy.WriteTools.Concat(DarlingMcpWritePolicy.InvestigationTools)
            .Where(Registered().Contains)
            .ToHashSet(StringComparer.Ordinal);

        var notWrites = change.Except(writes).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var misplaced = writes.Except(change).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(
            notWrites.Count == 0,
            $"These tools are in the \"{ChangeTable}\" table and in neither DarlingMcpWritePolicy.WriteTools nor InvestigationTools. A tool that writes "
            + $"belongs in the write policy's set, and a read belongs in one of the read tables: {string.Join(", ", notWrites)}");
        Assert.True(
            misplaced.Count == 0,
            $"These write or investigation tools have their row outside the \"{ChangeTable}\" table: {string.Join(", ", misplaced)}");
    }
}
