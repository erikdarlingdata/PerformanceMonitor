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
using System.Reflection;
using System.Text.Json;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5669: the Darling PG shards are cut by recorded class duration (.github/scripts/darling-shard-pack.py, from the
/// committed table darling-shard-weights.json) instead of a class-name hash. A table that stops covering the suite
/// does not lose a class (an unknown class weighs the mean, and under 80 percent coverage the packer falls back to
/// the hash), but it silently costs the balance this change buys, so these pins hold the table's shape and its
/// coverage of the classes this assembly really has, and the shard script's use of the packer.
/// </summary>
public sealed class DarlingShardWeightsTests
{
    /// <summary>Above the packer's own 80 percent floor, so the table is refreshed before it degrades the cut.</summary>
    private const double MinimumCoverage = 0.9;

    private static string Fixture(string name)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", name);
        Assert.True(File.Exists(path), $"{name} was not copied beside the test binary (Darling.Tests.csproj links it into Fixtures\\).");
        return File.ReadAllText(path);
    }

    private static System.Collections.Generic.Dictionary<string, double> Weights()
    {
        using var doc = JsonDocument.Parse(Fixture("darling-shard-weights.json"));
        var table = doc.RootElement.GetProperty("classes");
        return table.EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetDouble(), StringComparer.Ordinal);
    }

    [Fact]
    public void TheWeightsTable_IsWellFormed_AndHoldsRealClasses()
    {
        var weights = Weights();

        Assert.True(weights.Count >= 1500, $"the table holds {weights.Count} classes; refresh it (see darling-shard-pack.py)");
        Assert.All(weights, w =>
        {
            Assert.Contains(".Darling.Tests.", "." + w.Key, StringComparison.Ordinal);
            Assert.True(double.IsFinite(w.Value) && w.Value >= 0, $"{w.Key} has the weight {w.Value}");
        });
        Assert.True(weights.Values.Sum() > 0);
    }

    [Fact]
    public void TheWeightsTable_CoversTheClassesThisAssemblyHas()
    {
        var weights = Weights();
        var classes = typeof(DarlingShardWeightsTests).Assembly.GetTypes()
            .Where(t => t.FullName is not null && t.GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly)
                .Any(m => m.GetCustomAttributes<FactAttribute>(inherit: true).Any()))
            .Select(t => t.FullName!)
            .ToList();

        Assert.True(classes.Count > 1500, $"only {classes.Count} test classes were found by reflection");
        var known = classes.Count(weights.ContainsKey);
        var coverage = (double)known / classes.Count;
        Assert.True(coverage >= MinimumCoverage,
            $"the weights table covers {known} of {classes.Count} test classes ({coverage:P1}), under {MinimumCoverage:P0}. "
            + "Refresh it from recent full dev runs: python .github/scripts/darling-shard-pack.py refresh (its docstring has the steps).");
    }

    [Fact]
    public void TheShardScript_CutsThroughThePacker_AndNeverFallsBackToADifferentCutOnItsOwn()
    {
        var script = Fixture("run-darling-pg-shard.ps1").Replace("\r\n", "\n");

        Assert.Contains("python .github/scripts/darling-shard-pack.py pack --classes $classesFile --shards $ShardCount --shard $Shard", script, StringComparison.Ordinal);
        Assert.DoesNotContain("SHA256", script, StringComparison.Ordinal);
        /* A packer failure stops the shard: a per-shard fallback could cut this shard differently from the others. */
        Assert.Contains("if ($LASTEXITCODE -ne 0) { throw \"darling-shard-pack.py failed", script, StringComparison.Ordinal);
        /* The zero-class refusal that follows the cut stays: a runner with no -class arguments runs the whole suite. */
        Assert.Contains("selected zero of", script, StringComparison.Ordinal);
    }
}
