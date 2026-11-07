/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4677: the eviction caveat is worded in one place and every surface reads it. Null passes means unknown, never 0.
/// #8075: on Aurora the collector reads aurora_stat_statements(), which needs no extension, so the counter is unknown
/// because pg_stat_statements is not created in the collector's database, not because the major or the extension is old.
/// </summary>
public sealed class PgStatementEvictionNoteTests
{
    /// <summary>
    /// #8075: the eviction inputs for an Aurora target whose collector database has no pg_stat_statements row, built by
    /// name so the failure names the member the record needs rather than refusing to compile.
    /// </summary>
    internal static DarlingPgStatementReader.PgEvictionInfo AuroraWithoutExtension(long? maxEntries = 5000)
    {
        var ctor = typeof(DarlingPgStatementReader.PgEvictionInfo).GetConstructors()
            .FirstOrDefault(c => c.GetParameters().Any(p => p.Name == "AuroraWithoutExtension"));
        Assert.True(ctor is not null,
            "PgEvictionInfo has no AuroraWithoutExtension parameter, so the note cannot tell an Aurora target without "
            + "the extension from one on an old major or an old extension version.");

        var args = ctor!.GetParameters().Select(p => p.Name switch
        {
            "Known" => (object?)false,
            "EvictionPasses" => null,
            "MaxEntries" => maxEntries,
            "AuroraWithoutExtension" => true,
            _ => p.HasDefaultValue ? p.DefaultValue : null,
        }).ToArray();
        return (DarlingPgStatementReader.PgEvictionInfo)ctor.Invoke(args);
    }

    [Fact]
    public void Unknown_OnAuroraWithoutTheExtension_NamesTheMissingExtension_NotTheOldMajorOrVersion()
    {
        var note = PgStatementEvictionNote.Build(AuroraWithoutExtension());

        Assert.NotNull(note);
        Assert.Contains("unknown", note, StringComparison.Ordinal);
        Assert.Contains("CREATE EXTENSION pg_stat_statements", note, StringComparison.Ordinal);
        Assert.Contains("aurora_stat_statements()", note, StringComparison.Ordinal);
        Assert.DoesNotContain("before 14", note, StringComparison.Ordinal);
        Assert.DoesNotContain("below 1.9", note, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER EXTENSION", note, StringComparison.Ordinal);
        Assert.DoesNotContain("time(s)", note, StringComparison.Ordinal);
    }

    [Fact]
    public void Unknown_Elsewhere_KeepsTheOldMajorAndOldVersionCauses()
    {
        var note = PgStatementEvictionNote.Build(new DarlingPgStatementReader.PgEvictionInfo(false, null, 5000));

        Assert.NotNull(note);
        Assert.Contains("before 14", note, StringComparison.Ordinal);
        Assert.Contains("below 1.9", note, StringComparison.Ordinal);
        Assert.DoesNotContain("CREATE EXTENSION", note, StringComparison.Ordinal);
    }

    [Fact]
    public void McpEvictions_OnAuroraWithoutTheExtension_FlagIt_AndCarryTheSharedNote()
    {
        var info = AuroraWithoutExtension();
        var json = JsonSerializer.SerializeToElement(DarlingMcpPgStatementTools.BuildEvictions(info));

        Assert.False(json.GetProperty("known").GetBoolean());
        Assert.Equal(JsonValueKind.Null, json.GetProperty("eviction_passes_in_window").ValueKind);
        Assert.True(json.TryGetProperty("aurora_without_extension", out var flag) && flag.ValueKind == JsonValueKind.True,
            "evictions carries no aurora_without_extension flag, so the web's short sentence has nothing to branch on");
        Assert.Equal(PgStatementEvictionNote.Build(info), json.GetProperty("note").GetString());
    }

    [Theory]
    [InlineData(false, null)]
    [InlineData(true, 0L)]
    [InlineData(true, 7L)]
    public void McpEvictions_Elsewhere_SayTheTargetIsNotAuroraWithoutTheExtension(bool known, long? passes)
    {
        var json = JsonSerializer.SerializeToElement(DarlingMcpPgStatementTools.BuildEvictions(new DarlingPgStatementReader.PgEvictionInfo(known, passes, 5000)));

        Assert.True(json.TryGetProperty("aurora_without_extension", out var flag), "evictions carries no aurora_without_extension flag");
        Assert.Equal(JsonValueKind.False, flag.ValueKind);
    }

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
