/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4677: the eviction caveat is worded in one place and every surface reads it. Null passes means unknown, never 0.
/// </summary>
public sealed class PgStatementEvictionNoteTests
{
    [Fact]
    public void Known_WithPasses_NamesThePassesAndTheMax()
    {
        var note = PgStatementEvictionNote.Build(new DarlingPgStatementReader.PgEvictionInfo(true, 5, 5000));

        Assert.NotNull(note);
        Assert.Contains("evicted entries 5 time(s)", note, StringComparison.Ordinal);
        Assert.Contains("currently 5000", note, StringComparison.Ordinal);
    }

    [Fact]
    public void Known_WithPassesAndNoMax_OmitsTheMax()
    {
        var note = PgStatementEvictionNote.Build(new DarlingPgStatementReader.PgEvictionInfo(true, 2, null));

        Assert.NotNull(note);
        Assert.Contains("evicted entries 2 time(s)", note, StringComparison.Ordinal);
        Assert.DoesNotContain("currently", note, StringComparison.Ordinal);
    }

    [Fact]
    public void Known_WithZeroPasses_HasNoNote() =>
        Assert.Null(PgStatementEvictionNote.Build(new DarlingPgStatementReader.PgEvictionInfo(true, 0, 5000)));

    [Theory]
    [InlineData(false, null)]
    [InlineData(false, 3L)]
    [InlineData(true, null)]
    public void Unknown_SaysUnknown_NeverZeroPasses(bool known, long? passes)
    {
        var note = PgStatementEvictionNote.Build(new DarlingPgStatementReader.PgEvictionInfo(known, passes, 5000));

        Assert.NotNull(note);
        Assert.Contains("unknown", note, StringComparison.Ordinal);
        Assert.DoesNotContain("time(s)", note, StringComparison.Ordinal);
    }

    [Fact]
    public void NoInfo_IsUnknown()
    {
        var note = PgStatementEvictionNote.Build(null);

        Assert.NotNull(note);
        Assert.Contains("unknown", note, StringComparison.Ordinal);
    }

    [Fact]
    public void McpEvictions_CarryTheSharedNote_ByteForByte()
    {
        foreach (var info in new DarlingPgStatementReader.PgEvictionInfo?[]
        {
            null,
            new(false, null, 5000),
            new(true, 0, 5000),
            new(true, 7, 5000),
        })
        {
            var json = JsonSerializer.SerializeToElement(DarlingMcpPgStatementTools.BuildEvictions(info));
            var expected = PgStatementEvictionNote.Build(info);
            var actual = json.GetProperty("note");
            if (expected is null)
            {
                Assert.Equal(JsonValueKind.Null, actual.ValueKind);
            }
            else
            {
                Assert.Equal(expected, actual.GetString());
            }
        }
    }

    [Fact]
    public void EverySurfaceReadsTheSharedNote()
    {
        var mcp = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpPgStatementTools.cs");
        var viewer = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerServerTab.Postgres.cs");
        var web = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js");

        Assert.Contains("PgStatementEvictionNote.Build(", mcp, StringComparison.Ordinal);
        Assert.Contains("PgStatementEvictionNote.Build(", viewer, StringComparison.Ordinal);
        Assert.DoesNotContain("evicted entries", mcp, StringComparison.Ordinal);
        Assert.DoesNotContain("evicted entries", viewer, StringComparison.Ordinal);

        var at = web.IndexOf("fanout(\"get_pg_top_queries\"", StringComparison.Ordinal);
        Assert.True(at >= 0, "the Top Query Shapes panel moved");
        Assert.Contains("noteKey: \"evictions.note\"", web.Substring(at, 1500), StringComparison.Ordinal);
    }
}
