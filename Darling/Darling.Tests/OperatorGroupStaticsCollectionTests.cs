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
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// The tests that set the operator-group stand-in (<c>DarlingFileSecurity.UseOperatorGroupForTests</c>) or the
/// file-access sink (<c>DarlingSecretSource.ReportFileAccessTo</c>) change statics the whole test process shares.
/// Run beside the classes that harden real files, one test's stand-in lands inside another's ACL assertion. Every
/// class that sets either joins the <c>darling-config-env</c> collection, which is defined with parallelization off
/// so it runs alone.
/// </summary>
public sealed class OperatorGroupStaticsCollectionTests
{
    private const string CollectionName = "darling-config-env";

    private static readonly Type[] s_classesThatSetTheStatics =
    {
        typeof(DarlingFileSecurityTests),
        typeof(DarlingSecretFileAccessTests),
        typeof(DarlingHardenFilesJunctionTests),
    };

    [Fact]
    public void TheClassesThatSetTheStatics_ShareTheCollectionThatRunsAlone()
    {
        foreach (var type in s_classesThatSetTheStatics)
        {
            var collection = type.GetCustomAttribute<CollectionAttribute>();
            Assert.True(
                collection is not null && collection.Name == CollectionName,
                $"{type.Name} sets a static shared by the whole test process, so it must carry "
                + $"[Collection(\"{CollectionName}\")]; found "
                + $"{(collection is null ? "no [Collection] attribute" : $"[Collection(\"{collection.Name}\")]")}.");
        }

        var definition = typeof(DarlingConfigEnvironmentCollection).GetCustomAttribute<CollectionDefinitionAttribute>();
        Assert.NotNull(definition);
        Assert.Equal(CollectionName, definition.Name);
        Assert.True(
            definition.DisableParallelization,
            $"The \"{CollectionName}\" collection must disable parallelization: without it its members are serialized "
            + "against each other only, and the classes that harden real files still run beside them.");
    }

    [Fact]
    public void OnlyTheListedTestFiles_SetTheStatics()
    {
        var expected = new[]
        {
            "DarlingFileSecurityTests.cs",
            "DarlingHardenFilesVerbTests.cs",
            "DarlingSecretFileAccessTests.cs",
        };

        var directory = PathTo("Darling", "Darling.Tests");
        var setters = Directory.GetFiles(directory, "*.cs", SearchOption.TopDirectoryOnly)
            .Where(file =>
            {
                var text = File.ReadAllText(file);
                return text.Contains("UseOperatorGroupForTests(", StringComparison.Ordinal)
                    || text.Contains("ReportFileAccessTo(", StringComparison.Ordinal);
            })
            .Select(Path.GetFileName)
            .Where(name => name != nameof(OperatorGroupStaticsCollectionTests) + ".cs")
            .OrderBy(name => name, StringComparer.Ordinal)
            .ToArray();

        Assert.True(
            expected.SequenceEqual(setters),
            "A test file that sets the operator-group stand-in or the file-access sink must have its class in the "
            + $"\"{CollectionName}\" collection and be listed here. Files that set them now: {string.Join(", ", setters)}.");
    }
}
