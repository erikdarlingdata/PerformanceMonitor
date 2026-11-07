/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every <c>[Collection("name")]</c> in Lite.Tests names a collection with exactly one
/// <see cref="CollectionDefinitionAttribute"/>, and a collection whose members swap a process-wide static is defined
/// with <see cref="CollectionDefinitionAttribute.DisableParallelization"/> (#8164). A name with no definition still
/// groups its members, and xunit runs the members of one collection one at a time, but it constrains nothing else: the
/// rest of the assembly runs beside them, and product code that any class can reach reads the statics the members set
/// (<c>ServerTimeHelper</c>'s clock and display mode, <c>AppLogger</c>'s one drain queue, <c>App</c>'s alert settings).
/// #4776, #5863 and #6113 were that failure, each with a different victim. The test reflects over the xunit attributes
/// and reads no source text.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CollectionDefinitionCensusTests
{
    /// <summary>
    /// The collections whose members set a process-wide static that product code outside the collection reads. Each
    /// must run alone, after the parallel classes, or it protects its members only from each other.
    /// </summary>
    private static readonly string[] s_guardsAProcessWideStatic =
    {
        "server-time-helper",
        "app-logger-statics",
        "app-alert-statics",
    };

    /// <summary>Anti-vacuity: Lite.Tests uses at least this many named collections, so a scan that stopped seeing them fails.</summary>
    private const int NamesInUseFloor = 5;

    [Fact]
    public void EveryCollectionNameInUse_HasExactlyOneDefinition()
    {
        var types = TestTypes();
        var inUse = NamesInUse(types);
        var definitions = Definitions(types);

        var undefined = inUse.Where(n => !definitions.ContainsKey(n)).ToList();
        Assert.True(
            undefined.Count == 0,
            "A Lite.Tests collection name is in use with no CollectionDefinition (#8164). Without one, xunit serializes the members among "
          + "themselves and runs every other class beside them, so a process-wide static they set is read mid-swap by product code those "
          + "classes reach. Add a class with CollectionDefinition(\"<name>\", DisableParallelization = true) (CredentialManagerCollection "
          + "is the shape), or without DisableParallelization and a comment saying why the members share nothing process-wide:\n  "
          + string.Join("\n  ", undefined));

        var duplicated = definitions
            .Where(d => d.Value.Count > 1)
            .Select(d => $"{d.Key} ({string.Join(", ", d.Value.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal))})")
            .OrderBy(d => d, StringComparer.Ordinal)
            .ToList();
        Assert.True(
            duplicated.Count == 0,
            "A Lite.Tests collection name has more than one CollectionDefinition; xunit takes one of them, so the others say nothing:\n  "
          + string.Join("\n  ", duplicated));

        Assert.True(
            inUse.Count >= NamesInUseFloor,
            $"only {inUse.Count} collection names are in use in Lite.Tests; the scan has stopped seeing them");
    }

    [Fact]
    public void EveryCollectionThatGuardsAProcessWideStatic_RunsAlone()
    {
        var definitions = Definitions(TestTypes());

        var problems = new List<string>();
        foreach (var name in s_guardsAProcessWideStatic)
        {
            if (!definitions.TryGetValue(name, out var defining))
            {
                problems.Add($"{name}: no CollectionDefinition");
                continue;
            }

            foreach (var type in defining)
            {
                if (!type.GetCustomAttribute<CollectionDefinitionAttribute>()!.DisableParallelization)
                {
                    problems.Add($"{name}: {type.Name} is defined without DisableParallelization = true");
                }
            }
        }

        Assert.True(
            problems.Count == 0,
            "A Lite.Tests collection whose members set a process-wide static does not run alone (#8164). Define it with "
          + "CollectionDefinition(\"<name>\", DisableParallelization = true) so xunit runs it after the parallel classes, with no other "
          + "class mid-assertion while the static is swapped:\n  " + string.Join("\n  ", problems));
    }

    private static Type[] TestTypes() => typeof(CollectionDefinitionCensusTests).Assembly.GetTypes();

    /// <summary>The names the <c>[Collection("name")]</c> attributes use. A <c>[Collection(typeof(T))]</c> names its definition by type and needs no name.</summary>
    private static IReadOnlyList<string> NamesInUse(IEnumerable<Type> types) => types
        .SelectMany(t => t.GetCustomAttributes<CollectionAttribute>())
        .Where(a => a.Type is null && a.Name is not null)
        .Select(a => a.Name!)
        .Distinct(StringComparer.Ordinal)
        .OrderBy(n => n, StringComparer.Ordinal)
        .ToList();

    /// <summary>Each defined name and the classes that carry its <see cref="CollectionDefinitionAttribute"/>.</summary>
    private static Dictionary<string, List<Type>> Definitions(IEnumerable<Type> types)
    {
        var definitions = new Dictionary<string, List<Type>>(StringComparer.Ordinal);
        foreach (var type in types)
        {
            var attribute = type.GetCustomAttribute<CollectionDefinitionAttribute>();
            if (attribute?.Name is null)
            {
                continue;
            }

            if (!definitions.TryGetValue(attribute.Name, out var defining))
            {
                definitions[attribute.Name] = defining = new List<Type>();
            }

            defining.Add(type);
        }

        return definitions;
    }
}
