/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.IO;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5735: the two products read one log row, so the collector name and status are spelled the same in Lite's
/// <c>CollectionCycleSkip</c> and Darling's <c>CollectionCycleRow</c>. A drift would make each product's readers miss the
/// other's rows (a peer disclosure, the shared get_collection_health text, a store read by both). Reads a Lite source file, so
/// it carries the Lite trait and a Lite-only change runs it.
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class CollectionCycleLiteSpellingTests
{
    [Fact]
    public void LiteAndDarling_SpellTheCollectorNameAndStatus_Identically()
    {
        var lite = File.ReadAllText(Path.Combine(CycleSkipFeedTests.RepoRoot(), "Lite", "Services", "CollectionCycleSkip.cs"));
        var name = Regex.Match(lite, "const string CollectorName = \"([^\"]+)\";");
        var status = Regex.Match(lite, "const string Status = \"([^\"]+)\";");
        Assert.True(name.Success && status.Success, "Lite's CollectionCycleSkip no longer declares the two constants as string literals");
        Assert.Equal(CollectionCycleRow.CollectorName, name.Groups[1].Value);
        Assert.Equal(CollectionCycleRow.Status, status.Groups[1].Value);
    }
}
