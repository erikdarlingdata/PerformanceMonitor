/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Text.Json;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #8730: the web UI smoke opens each PostgreSQL server's pages as the browser's second visit (<c>smoke.mjs</c> records the engine the way
/// the page does), because a first visit starts SQL Server panel reads that the fleet card then cancels, and the check reports a cancelled
/// read as a failed request. The set it does that for is <c>postgresServers</c> in <c>pages.json</c>. It has to stay equal to the PostgreSQL
/// servers the smoke seeds (<c>darling.ui-smoke.json</c>), or a new PostgreSQL server brings the nightly flake back.
/// </summary>
public sealed class UiSmokeConfigTests
{
    private static readonly JsonDocumentOptions Lenient = new() { CommentHandling = JsonCommentHandling.Skip, AllowTrailingCommas = true };

    [Fact]
    public void ThePostgresServersTheSmokeOpensAsASecondVisit_AreTheOnesItSeedsAsPostgres()
    {
        using var seeded = JsonDocument.Parse(Regex.Replace(File.ReadAllText(PathTo("tools", "ui-smoke", "darling.ui-smoke.json")), "__[A-Z_]+__", "0"), Lenient);
        var seededPostgres = seeded.RootElement.GetProperty("servers").EnumerateArray()
            .Where(s => s.TryGetProperty("engine", out var e) && e.GetString() == "postgres")
            .Select(s => s.GetProperty("name").GetString()!)
            .OrderBy(n => n).ToArray();

        using var pages = JsonDocument.Parse(File.ReadAllText(PathTo("tools", "ui-smoke", "pages.json")), Lenient);
        var listed = pages.RootElement.GetProperty("postgresServers").EnumerateArray().Select(e => e.GetString()!).OrderBy(n => n).ToArray();

        Assert.NotEmpty(seededPostgres);
        Assert.Equal(seededPostgres, listed);
    }
}
