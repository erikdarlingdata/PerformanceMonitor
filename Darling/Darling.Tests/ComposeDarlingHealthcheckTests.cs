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
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4733: drift guard between <c>Darling/compose/docker-compose.yml</c>'s <c>darling</c> service and the
/// stopped-collection marker <see cref="PerformanceMonitor.Darling.Service.Mcp.CollectorRuntimeState"/>
/// writes. After a terminal startup verdict the process stays up on purpose, so the container shows as
/// running although it never collects. The service is told where to write the marker
/// (<c>DARLING_STOPPED_MARKER</c>) and its healthcheck tests for that file; if the two paths ever differ, or
/// either is dropped, the container goes back to looking healthy after collection has stopped for good, and
/// nothing else fails.
///
/// <para>Reads the REAL compose file, copied beside the test binary by the csproj (the same fixture
/// <see cref="CiComposeWorkerSizingTests"/> reads), and is itself guarded:
/// <see cref="TheComparison_FailsOnAnInjectedDrift"/> runs the identical parse over a mutated copy and
/// asserts it reports the difference, so a parse that silently matched nothing can never pass.</para>
/// </summary>
public sealed class ComposeDarlingHealthcheckTests
{
    private const string MarkerVariable = "DARLING_STOPPED_MARKER";

    [Fact]
    public void TheDarlingService_SetsTheMarkerVariable_AndItsHealthcheckTestsTheSamePath()
    {
        var (markerPath, checkedPath) = ParseMarkerAndCheckedPath(ReadComposeFile());

        Assert.Equal("/tmp/darling-stopped", markerPath);
        Assert.Equal(markerPath, checkedPath);
    }

    /// <summary>
    /// The healthcheck is the file's own shape: a shell command (the image is Debian with /bin/sh and no
    /// curl, which is why it reads a file and not <c>/api/ping</c>), with the timing keys the store's
    /// healthcheck sets. <c>start_period</c> in particular keeps a slow start from counting as a failure.
    /// </summary>
    [Fact]
    public void TheDarlingHealthcheck_IsAShellFileTest_WithTheTimingKeys()
    {
        var healthcheck = SubBlock(ServiceBlock(ReadComposeFile(), "darling"), "healthcheck");

        Assert.Matches(@"(?m)^\s+test:\s*\[\s*""CMD-SHELL""\s*,\s*""test ! -e /[^""]+""\s*\]\s*$", healthcheck);
        foreach (var key in new[] { "interval", "timeout", "retries", "start_period" })
        {
            Assert.Matches($@"(?m)^\s+{key}:\s*\S+", healthcheck);
        }
    }

    [Fact]
    public void TheComparison_FailsOnAnInjectedDrift()
    {
        var real = ReadComposeFile();
        var mutated = real.Replace("test ! -e /tmp/darling-stopped", "test ! -e /tmp/somewhere-else", StringComparison.Ordinal);
        Assert.NotEqual(real, mutated);

        var (markerPath, checkedPath) = ParseMarkerAndCheckedPath(mutated);

        Assert.NotEqual(markerPath, checkedPath);
    }

    private static (string MarkerPath, string CheckedPath) ParseMarkerAndCheckedPath(string composeText)
    {
        var darling = ServiceBlock(composeText, "darling");

        var environment = SubBlock(darling, "environment");
        var marker = Regex.Match(environment, $@"(?m)^\s+{MarkerVariable}:\s*""?(?<path>/[^\s""]+)""?\s*$");
        Assert.True(marker.Success,
            $"docker-compose.yml's darling service no longer sets {MarkerVariable} in its environment. Without it "
            + "the service writes no marker, and a container that stopped collecting still looks healthy.");

        var healthcheck = SubBlock(darling, "healthcheck");
        var check = Regex.Match(healthcheck, @"(?m)^\s+test:\s*\[\s*""CMD-SHELL""\s*,\s*""test ! -e (?<path>/[^""]+)""\s*\]\s*$");
        Assert.True(check.Success,
            "docker-compose.yml's darling service no longer has a healthcheck of the form "
            + "test: [\"CMD-SHELL\", \"test ! -e <marker path>\"]. Without it the container reports running after "
            + "a startup failure has stopped collection.");

        return (marker.Groups["path"].Value, check.Groups["path"].Value);
    }

    /// <summary>
    /// The lines of one service, from its <c>  name:</c> key (two-space indent) to the next line that is
    /// neither blank, a comment, nor indented deeper than the key.
    /// </summary>
    private static string ServiceBlock(string composeText, string service)
    {
        var lines = composeText.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var start = Array.FindIndex(lines, line => line == $"  {service}:");
        Assert.True(start >= 0, $"docker-compose.yml no longer has a `{service}` service at two-space indent.");

        return string.Join("\n", lines.Skip(start + 1).TakeWhile(IsInside(indent: 2)));
    }

    /// <summary>
    /// The lines under one key of a service (four-space indent), up to the next key at that indent. A key
    /// that is not there gives an empty block, which the caller's match then reports as missing.
    /// </summary>
    private static string SubBlock(string serviceBlock, string key)
    {
        var lines = serviceBlock.Split('\n');
        var start = Array.FindIndex(lines, line => line == $"    {key}:");
        if (start < 0)
        {
            return string.Empty;
        }

        return string.Join("\n", lines.Skip(start + 1).TakeWhile(IsInside(indent: 4)));
    }

    private static Func<string, bool> IsInside(int indent) => line =>
    {
        if (line.Trim().Length == 0 || line.TrimStart().StartsWith('#'))
        {
            return true;
        }

        return line.Length - line.TrimStart().Length > indent;
    };

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
