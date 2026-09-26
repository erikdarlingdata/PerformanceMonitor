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
using PerformanceMonitor.Darling.Service;
using Xunit;
using static PerformanceMonitor.Darling.Service.ManagedConfMigration;

namespace Darling.Tests;

/// <summary>
/// Pins <c>ManagedConfMigration.Rewrite</c> against the committed rehearsal fixture (#4336): a synthetic
/// <c>postgresql.conf</c> built to reproduce a real one's shape — initdb's PG18 stock conf, every product block v1 through v14 appended in
/// order (v8 written FOUR times, at 16 GB/66 hypertables then 16 GB/68 then 31 GB/70 then 31 GB/72, so the
/// last-occurrence-wins reduction has real work to do), plus three hand edits: <c>max_connections = 300</c>
/// between v4 and v5 (the LAST assignment of that key anywhere in the file, so it is effective and this
/// rewrite must move it below the include); <c>work_mem = 70MB</c> spliced into the second v8 copy with no
/// blank line before it (a later v8 copy still sets <c>work_mem</c>, so this one is NOT effective and must
/// stay exactly where it is); and <c>log_timezone = 'UTC'</c> at the end (a key no managed block owns, so it
/// stays exactly where it is too).
///
/// <para><b>This fixture's numbers</b> are 69 lines removed / 24 managed keys / 1 operator line moved.
/// The "1 operator line moved" is exactly what the rewrite logic requires. The counts below explain why they
/// are 69/24 rather than some other plausible pair, given how today's builders emit their blocks:
/// <list type="bullet">
/// <item><b>managed keys 24 vs 23.</b> <see cref="ManagedConfFile.RenderBody"/> emits one line per key
/// <see cref="DarlingManagedPostgres.ParseConfText"/> reads back from calling every <c>Build*ConfAppend</c> in
/// order with THIS PR's inputs (RAM authoritative, disk authoritative, PostgreSQL 18) — today that is 24 keys.
/// The extra key is <c>min_wal_size</c>: v12's WAL-sizing block
/// (<see cref="DarlingManagedPostgres.BuildWalSizingConfAppend"/>) writes both <c>max_wal_size</c> AND
/// <c>min_wal_size</c>, and nothing later in the v1-v14 order overwrites the second one, so it survives the
/// reduction as its own key.</item>
/// <item><b>lines removed 69.</b> The initdb stock conf, v1-v14
/// each appended once except v8 four times, the three hand edits — this fixture reproduces, so the shapes
/// match. The absolute REMOVAL count is a function of how many total OURS lines the classifier drops (three
/// extra v8 copies alone contribute several lines each) plus exactly how many bytes today's builders emit per
/// block. This fixture pins its OWN actual numbers, verified against the rewrite logic's requirements: keys
/// migrate, and one operator line moves.</item>
/// </list>
/// </para>
/// </summary>
public sealed class ManagedConfRehearsalTests
{
    private const string FixturePath = "Darling.Tests/Fixtures/ManagedConf/rehearsal-postgresql.conf";

    /// <summary>The managed file's own key/value map, built the same way <see cref="ManagedConfFile.RenderBody"/>
    /// derives it — parsed back from calling every v1-v14 builder in order with the same inputs the fixture's
    /// LAST v8/v12 copies used (31 GB RAM, 72 hypertables, 16 GB free of 200 GB disk, PostgreSQL 18) — so the
    /// map this test hands to <c>Rewrite</c> is exactly what the product would render for a store shaped like
    /// the fixture.</summary>
    private static IReadOnlyDictionary<string, string> BuildManagedValues()
    {
        var inputs = new ManagedConfFile.RenderInputs(
            FormulaVersion: ManagedConfFile.CurrentFormulaVersion,
            Platform: "windows",
            RamBytes: 31L * 1024 * 1024 * 1024,
            RamAuthoritative: true,
            ProcessorCount: 8,
            HypertableCount: 72,
            PostgresMajor: 18,
            DataVolumeFreeBytes: 16L * 1024 * 1024 * 1024,
            DataVolumeTotalBytes: 200L * 1024 * 1024 * 1024,
            DataVolumeAuthoritative: true,
            Port: 5432,
            EffectivePreloadList: "timescaledb");

        var body = ManagedConfFile.RenderBody(inputs);
        var values = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (_, key, value) in DarlingManagedPostgres.ParseConfText(body))
        {
            values[key] = value;
        }

        return values;
    }

    private static string ReadFixture()
    {
        var path = FindRepoRootedFile(FixturePath);
        return File.ReadAllText(path);
    }

    /// <summary>Resolves a repo-rooted relative path from wherever the test assembly happens to run, the same
    /// upward-walk idiom other fixture-reading tests in this project use.</summary>
    private static string FindRepoRootedFile(string relativePath)
    {
        var dir = AppContext.BaseDirectory;
        for (var i = 0; i < 10 && dir is not null; i++)
        {
            var candidate = Path.Combine(dir, relativePath);
            if (File.Exists(candidate))
            {
                return candidate;
            }

            dir = Path.GetDirectoryName(dir.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar));
        }

        throw new FileNotFoundException($"Could not locate '{relativePath}' walking up from '{AppContext.BaseDirectory}'.");
    }

    /// <summary>The rehearsal's headline numbers (see the class doc comment for why 69/24 rather than the
    /// design's 48/23): lines removed, managed keys in the map handed to <c>Rewrite</c>, and operator lines
    /// moved below the include — the last of which holds exactly at 1, as required.</summary>
    [Fact]
    public void Rewrite_RehearsalFixture_LinesRemovedManagedKeysAndOperatorLinesMoved()
    {
        var conf = ReadFixture();
        var managedValues = BuildManagedValues();

        var result = Rewrite(conf, managedValues, configuredPort: 5432);

        var beforeLineCount = conf.Split('\n').Length;
        var afterLineCount = result.NewConfText.Split('\n').Length;

        Assert.Equal(69, beforeLineCount - afterLineCount);
        Assert.Equal(24, managedValues.Count);
        Assert.Single(result.ExcludedKeys);
        Assert.Contains("max_connections", result.ExcludedKeys);
    }

    /// <summary><c>max_connections = 300</c> (between v4 and v5, the last assignment of that key anywhere in
    /// the fixture) is the one effective operator line, and it moves below the include.</summary>
    [Fact]
    public void Rewrite_RehearsalFixture_MaxConnections300_MovesBelowInclude()
    {
        var result = Rewrite(ReadFixture(), BuildManagedValues(), configuredPort: 5432);
        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        var maxConnIndex = Array.IndexOf(lines, "max_connections = 300");

        Assert.True(includeIndex >= 0);
        Assert.True(maxConnIndex >= 0);
        Assert.True(maxConnIndex > includeIndex);
        Assert.Equal(1, lines.Count(l => l == "max_connections = 300"));
    }

    /// <summary><c>work_mem = 70MB</c> (spliced into the second v8 copy with no blank line) is NOT the
    /// effective assignment — a later v8 copy still sets <c>work_mem</c> — so it stays exactly where it is,
    /// above the include.</summary>
    [Fact]
    public void Rewrite_RehearsalFixture_WorkMem70MB_StaysAboveInclude_NotEffective()
    {
        var result = Rewrite(ReadFixture(), BuildManagedValues(), configuredPort: 5432);
        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        var workMemIndex = Array.IndexOf(lines, "work_mem = 70MB");

        Assert.True(includeIndex >= 0);
        Assert.True(workMemIndex >= 0);
        Assert.True(workMemIndex < includeIndex);
        Assert.DoesNotContain("work_mem", result.ExcludedKeys);
    }

    /// <summary><c>log_timezone = 'UTC'</c> (an unowned key, at the end of the original file) stays exactly
    /// where it is, above the include.</summary>
    [Fact]
    public void Rewrite_RehearsalFixture_LogTimezone_StaysInPlace_AboveInclude()
    {
        var result = Rewrite(ReadFixture(), BuildManagedValues(), configuredPort: 5432);
        var lines = result.NewConfText.Split('\n');
        var includeIndex = Array.IndexOf(lines, ManagedConfFile.IncludeLine);
        var logTzIndex = Array.IndexOf(lines, "log_timezone = 'UTC'");

        Assert.True(includeIndex >= 0);
        Assert.True(logTzIndex >= 0);
        Assert.True(logTzIndex < includeIndex);
    }

    /// <summary>Exactly one include line in the result.</summary>
    [Fact]
    public void Rewrite_RehearsalFixture_ExactlyOneIncludeLine()
    {
        var result = Rewrite(ReadFixture(), BuildManagedValues(), configuredPort: 5432);
        Assert.Equal(1, result.NewConfText.Split('\n').Count(l => l == ManagedConfFile.IncludeLine));
    }

    /// <summary>Rule 6: a second run against the first run's output is byte-identical, with an empty log —
    /// nothing left to migrate.</summary>
    [Fact]
    public void Rewrite_RehearsalFixture_SecondRun_IsByteIdentical_LogEmpty()
    {
        var managedValues = BuildManagedValues();
        var first = Rewrite(ReadFixture(), managedValues, configuredPort: 5432);
        var second = Rewrite(first.NewConfText, managedValues, configuredPort: 5432);

        Assert.Equal(first.NewConfText, second.NewConfText);
        Assert.Empty(second.Log);
    }
}
