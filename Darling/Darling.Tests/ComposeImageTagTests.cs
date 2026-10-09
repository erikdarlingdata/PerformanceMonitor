/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5629: the compose quickstart's <c>darling</c> service names the <c>:latest</c> image. It named
/// <c>:nightly</c> after 3.4.0, so a release user who followed the README's compose steps ran the newest dev
/// build. The release job pushes <c>:&lt;version&gt;</c> and <c>:latest</c> (never <c>:nightly</c>), and a
/// version pin would break the quickstart on <c>dev</c>, because the next version's image does not exist
/// until the release ships. So the tag must be exactly <c>latest</c>.
///
/// <para>Reads the REAL compose file, copied beside the test binary by the csproj (the same fixture
/// <see cref="ComposeDarlingHealthcheckTests"/> reads), and is itself guarded:
/// <see cref="TheTagParse_ReportsNightly_WhenTheFileNamesIt"/> runs the identical parse over a mutated copy, so
/// a parse that silently matched nothing can never pass.</para>
/// </summary>
public sealed class ComposeImageTagTests
{
    private const string ImageRepository = "ghcr.io/erikdarlingdata/performancemonitor-darling";

    [Fact]
    public void TheDarlingService_NamesTheLatestImage()
    {
        var image = ParseDarlingImage(ReadComposeFile());

        Assert.Equal($"{ImageRepository}:latest", image);
    }

    [Fact]
    public void TheTagParse_ReportsNightly_WhenTheFileNamesIt()
    {
        var mutated = ReadComposeFile().Replace(
            $"{ImageRepository}:latest", $"{ImageRepository}:nightly", StringComparison.Ordinal);

        Assert.Equal($"{ImageRepository}:nightly", ParseDarlingImage(mutated));
    }

    /// <summary>
    /// The value of the <c>darling</c> service's <c>image:</c> key (four-space indent), from the service's
    /// <c>  darling:</c> line to the next key at two-space indent.
    /// </summary>
    private static string ParseDarlingImage(string composeText)
    {
        var lines = composeText.Replace("\r\n", "\n", StringComparison.Ordinal).Split('\n');
        var start = Array.FindIndex(lines, line => line == "  darling:");
        Assert.True(start >= 0, "docker-compose.yml no longer has a `darling` service at two-space indent.");

        var block = lines.Skip(start + 1)
            .TakeWhile(line => line.Trim().Length == 0 || line.TrimStart().StartsWith('#')
                || line.Length - line.TrimStart().Length > 2);
        var matches = block.Select(line => Regex.Match(line, @"^    image:\s*(\S+)\s*$"))
            .Where(m => m.Success).ToList();
        Assert.True(matches.Count == 1,
            "docker-compose.yml's darling service no longer has exactly one `image:` key at four-space indent.");

        return matches[0].Groups[1].Value;
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
