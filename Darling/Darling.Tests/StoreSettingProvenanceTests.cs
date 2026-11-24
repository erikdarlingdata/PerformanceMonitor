/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9418: the pure parts of store setting provenance, with no store behind them: how the running value is shown,
/// the pending-restart rule, the cap on hidden lines named per setting, and what a remote MCP caller is allowed to
/// see of a file path.
/// </summary>
public sealed class StoreSettingProvenanceTests
{
    private static readonly string DataDirectory = Path.Combine(Path.GetTempPath(), "darling-provenance-tests", "pg");
    private static readonly string OutsideFile = Path.Combine(Path.GetTempPath(), "darling-provenance-outside", "extra.conf");

    private static SettingProvenance Provenance(
        string? sourceFile = null, bool pendingRestart = false, IReadOnlyList<UnappliedFileSetting>? rows = null)
        => new(
            "work_mem", "78848", "kB", "configuration file", sourceFile, sourceFile is null ? null : 3,
            pendingRestart, rows ?? [], "77MB", "77MB", "user");

    [Fact]
    public void ValueDisplay_IsTheShowText_NeverTheFigureJoinedToAPageUnit()
    {
        /* shared_buffers: 262144 pages of 8kB is 2GB. Joining the figure to the unit reads 2621448kB. */
        var sharedBuffers = new SettingProvenance(
            "shared_buffers", "262144", "8kB", "configuration file", null, null, false, [], null, "2GB", "postmaster");
        Assert.Equal("2GB", sharedBuffers.ValueDisplay);

        var withoutShow = new SettingProvenance(
            "shared_buffers", "262144", "8kB", "configuration file", null, null, false, []);
        Assert.Equal("262144", withoutShow.ValueDisplay);
    }

    [Fact]
    public void IsPendingRestart_NeedsAPostmasterSettingWithALineTheServerCouldNotApply()
    {
        var notApplied = new[] { new UnappliedFileSetting("a.conf", 4, "256MB", StoreSettingProvenance.FileSettingNotAppliedError) };
        var hiddenOnly = new[] { new UnappliedFileSetting("a.conf", 4, "256MB", null) };

        Assert.True(StoreSettingProvenance.IsPendingRestart("postmaster", notApplied));
        Assert.False(StoreSettingProvenance.IsPendingRestart("sighup", notApplied));
        Assert.False(StoreSettingProvenance.IsPendingRestart("postmaster", hiddenOnly));
        Assert.False(StoreSettingProvenance.IsPendingRestart("postmaster", []));
        Assert.False(StoreSettingProvenance.IsPendingRestart(null, notApplied));
    }

    [Fact]
    public void Describe_NamesFiveHiddenLinesAndCountsTheRest()
    {
        var rows = Enumerable.Range(1, 6)
            .Select(i => new UnappliedFileSetting("c.conf", i * 10, "value-" + i, null))
            .ToList();
        var sentence = StoreSettingProvenance.Describe(Provenance(rows: rows), file => file)!;

        for (var i = 1; i <= 5; i++)
        {
            Assert.Contains("value-" + i, sentence, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("value-6", sentence, StringComparison.Ordinal);
        Assert.Contains("1 more", sentence, StringComparison.Ordinal);

        var five = StoreSettingProvenance.Describe(Provenance(rows: rows.Take(5).ToList()), file => file)!;
        Assert.DoesNotContain("more", five, StringComparison.Ordinal);
    }

    [Fact]
    public void ShapeProvenance_GivesARemoteCallerOnlyRelativePaths_AndTheFieldsTheToolDescriptionNames()
    {
        var winner = Path.Combine(DataDirectory, "postgresql.auto.conf");
        var hiddenInside = Path.Combine(DataDirectory, "conf.d", "managed.conf");
        var provenance = Provenance(
            sourceFile: winner, pendingRestart: true,
            rows:
            [
                new UnappliedFileSetting(hiddenInside, 12, "64MB", null),
                new UnappliedFileSetting(OutsideFile, 7, "32MB", "setting could not be applied"),
                new UnappliedFileSetting(null, null, null, null),
            ]);

        var shaped = DarlingMcpStoreHostTools.ShapeProvenance(provenance, DataDirectory);
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(shaped));
        var root = document.RootElement;

        /* Never an absolute path: every file the caller gets is relative to the data directory, or a bare name. */
        var files = new List<string?>
        {
            root.GetProperty("source_file").GetString(),
            root.GetProperty("not_applied")[0].GetProperty("file").GetString(),
            root.GetProperty("not_applied")[1].GetProperty("file").GetString(),
        };
        foreach (var file in files)
        {
            Assert.False(Path.IsPathRooted(file!), file);
            Assert.DoesNotContain("..", file!, StringComparison.Ordinal);
        }

        var summary = root.GetProperty("summary").GetString()!;
        Assert.DoesNotContain(Path.GetTempPath(), summary, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("postgresql.auto.conf", files[0]);
        Assert.Equal(Path.Combine("conf.d", "managed.conf"), files[1]);
        Assert.Equal("extra.conf", files[2]);
        Assert.Equal(JsonValueKind.Null, root.GetProperty("not_applied")[2].GetProperty("file").ValueKind);

        /* The fields the get_store_host description lists. */
        Assert.Equal("77MB", root.GetProperty("value").GetString());
        Assert.Equal("77MB", root.GetProperty("file_value").GetString());
        Assert.Equal("configuration file", root.GetProperty("source").GetString());
        Assert.Equal(3, root.GetProperty("source_line").GetInt32());
        Assert.True(root.GetProperty("pending_restart").GetBoolean());
        Assert.Equal(12, root.GetProperty("not_applied")[0].GetProperty("line").GetInt32());
        Assert.Equal("64MB", root.GetProperty("not_applied")[0].GetProperty("value").GetString());
        Assert.Equal("setting could not be applied", root.GetProperty("not_applied")[1].GetProperty("error").GetString());
        Assert.Contains("postgresql.auto.conf", summary, StringComparison.Ordinal);
    }

    [Fact]
    public void ShapeProvenance_WithoutADataDirectory_KeepsOnlyFileNames_AndIsNullForASettingNotSeen()
    {
        var provenance = Provenance(sourceFile: Path.Combine(DataDirectory, "postgresql.auto.conf"));
        var shaped = DarlingMcpStoreHostTools.ShapeProvenance(provenance, null);
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(shaped));
        Assert.Equal("postgresql.auto.conf", document.RootElement.GetProperty("source_file").GetString());

        Assert.Null(DarlingMcpStoreHostTools.ShapeProvenance(null, DataDirectory));
    }
}
