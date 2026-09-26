/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Drift guard between <c>Darling/compose/docker-compose.yml</c>'s hand-written <c>store.command:</c>
/// flags and the product's own sizing rules (#4322), the same shape as
/// <see cref="CiClusterWorkerSizingTests"/> for the gated-live CI cluster.
///
/// <para><b>Why this exists.</b> On Linux the service never manages the store container (#4215 review M7),
/// so <c>timescaledb-tune</c> — which the official image runs on every start — is the ONLY thing sizing
/// this store, and it sizes worker counts from CPU count and <c>work_mem</c> from the container's memory
/// limit, neither of which matches this product's needs (measured, #4322: 16/34 workers at 2-8 GB, a
/// PostgreSQL-default shape TimescaleDB's own per-hypertable policy jobs starve under; ~4 MB work_mem,
/// below the managed store's own 16 MB floor at these sizes and the measured spill cost at #4310). Path B
/// (#4322, claude-desktop 2026-09-26 07:17Z) is to override ONLY those two, explicitly, on the command
/// line — which cannot call into the product, so the numbers are literals that go stale the moment a
/// collector is added. This test reads the REAL compose file (copied beside the test binary by the csproj)
/// and requires the worker-sizing numbers to equal what <see cref="TimescaleSupport.HypertableCount"/>
/// produces today, and requires the memory/preload flags this file's PR body documents to still be
/// present within their stated floor/cap.</para>
///
/// <para><b>The guard is itself guarded.</b> <see cref="ParsedCommand_Comparison_FailsOnAnInjectedDrift"/>
/// runs the identical parse over a mutated copy and asserts it reports the difference, so a regex that
/// silently matched nothing can never pass as "no drift".</para>
/// </summary>
public sealed class CiComposeWorkerSizingTests
{
    /* The product's formula, restated ONCE — same as CiClusterWorkerSizingTests. Both this test and the
       compose file are re-derived from the live catalog count on every run, so neither can be right about
       a stale hypertable count. */
    private static int ExpectedBackgroundWorkers => TimescaleSupport.HypertableCount + 2;

    private static int ExpectedWorkerProcesses => 3 + ExpectedBackgroundWorkers + 8;

    /// <summary>
    /// work_mem's fixed floor (#4322): the managed store's own clamp(RAM/512, 16 MB, 64 MB) already floors
    /// to 16 MB for any host at or under 8 GB, which covers every size this container is measured at. Not
    /// host-scaled, so a single literal is the correct answer here — the compose file does not need a
    /// second TS_TUNE_MAX_CONNS override to make tune's own number come out right.
    /// </summary>
    private const string ExpectedWorkMem = "16MB";

    private const string ExpectedPreloadLibraries = "shared_preload_libraries=timescaledb,pg_stat_statements";

    [Fact]
    public void ComposeStoreCommand_SizesWorkersFromTheProductsFormula()
    {
        var flags = ParseStoreCommandFlags(ReadComposeFile());

        Assert.True(flags.ContainsKey("timescaledb.max_background_workers"),
            "docker-compose.yml's store.command no longer sets timescaledb.max_background_workers. "
            + "Without it the Linux compose store runs on timescaledb-tune's CPU-derived default, which "
            + "starves TimescaleDB's per-hypertable compression/retention/CAGG policy jobs the same way "
            + "PostgreSQL's own max_worker_processes default did before #1888 (#4322).");
        Assert.True(flags.ContainsKey("max_worker_processes"),
            "docker-compose.yml's store.command no longer sets max_worker_processes. Without it the "
            + "compose store's background-worker ceiling reverts to tune's CPU-derived default, not the "
            + "hypertable-derived one the managed store always writes (#4322).");

        Assert.Equal(
            ExpectedBackgroundWorkers.ToString(CultureInfo.InvariantCulture),
            flags["timescaledb.max_background_workers"]);

        Assert.Equal(
            ExpectedWorkerProcesses.ToString(CultureInfo.InvariantCulture),
            flags["max_worker_processes"]);
    }

    [Fact]
    public void ComposeStoreCommand_SetsWorkMemAtTheFixedFloor()
    {
        var flags = ParseStoreCommandFlags(ReadComposeFile());

        Assert.True(flags.ContainsKey("work_mem"),
            "docker-compose.yml's store.command no longer sets work_mem. Without it timescaledb-tune's "
            + "container-memory-derived value (measured ~4 MB at 2-8 GB, #4322) undercuts the managed "
            + "store's own 16 MB floor and the #4310 spill evidence it was set to avoid.");

        Assert.Equal(ExpectedWorkMem, flags["work_mem"]);
    }

    [Fact]
    public void ComposeStoreCommand_StillPreloadsBothLibraries()
    {
        var commandText = ExtractStoreCommandText(ReadComposeFile());

        Assert.Contains(ExpectedPreloadLibraries, commandText, StringComparison.Ordinal);
    }

    /// <summary>
    /// The parser must actually be reading the file. Mutating an appended value has to be REPORTED — if
    /// this passes, the regex matched nothing and every assertion above was vacuous.
    /// </summary>
    [Fact]
    public void ParsedCommand_Comparison_FailsOnAnInjectedDrift()
    {
        var real = ReadComposeFile();
        var parsed = ParseStoreCommandFlags(real);
        Assert.NotEmpty(parsed);
        Assert.Equal(ExpectedWorkerProcesses.ToString(CultureInfo.InvariantCulture), parsed["max_worker_processes"]);

        var mutated = real.Replace(
            $"max_worker_processes={ExpectedWorkerProcesses}",
            "max_worker_processes=8",
            StringComparison.Ordinal);
        Assert.NotEqual(real, mutated);

        Assert.Equal("8", ParseStoreCommandFlags(mutated)["max_worker_processes"]);
    }

    /// <summary>Pulls the YAML scalar value of <c>store.command:</c> as one line of text.</summary>
    private static readonly Regex StoreCommandLine = new(
        @"command:\s*\[(?<args>.*)\]",
        RegexOptions.Compiled);

    /// <summary>Each <c>""-c"", ""setting=value""</c> pair inside the parsed command array.</summary>
    private static readonly Regex FlagPair = new(
        @"""-c""\s*,\s*""(?<setting>[a-zA-Z_.]+)\s*=\s*(?<value>[^""]+)""",
        RegexOptions.Compiled);

    private static string ExtractStoreCommandText(string composeText)
    {
        var match = StoreCommandLine.Match(composeText);
        Assert.True(match.Success,
            "docker-compose.yml's store service no longer has a command: [...] array. The comparisons in "
            + "this test class assume the flags live there, per the file's own comment that the command "
            + "line outranks the image conf and ALTER SYSTEM.");
        return match.Groups["args"].Value;
    }

    private static Dictionary<string, string> ParseStoreCommandFlags(string composeText)
    {
        var commandText = ExtractStoreCommandText(composeText);
        var flags = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match match in FlagPair.Matches(commandText))
        {
            /* Last occurrence wins, exactly as postgresql.conf itself resolves duplicates. */
            flags[match.Groups["setting"].Value] = match.Groups["value"].Value.Trim();
        }

        return flags;
    }

    private static string ReadComposeFile()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "docker-compose.yml");
        Assert.True(File.Exists(path),
            "docker-compose.yml was not copied beside the test binary. Darling.Tests.csproj links it into "
            + "Fixtures\\ so this guard parses the real file; restore that item rather than pointing the "
            + "test at a copy that can go stale.");
        return File.ReadAllText(path);
    }
}
