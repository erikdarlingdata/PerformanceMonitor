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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4773: <c>ManagedConfMigrationSteps.FailBetweenSteps</c> is one static field for the whole test process, and
/// two test classes set it to throw. If they run at the same time, one class's simulated crash lands inside the
/// other class's <c>WriteTwoSteps</c> call and fails a test that never asked for it. Both classes carry
/// <c>[Collection("managed-conf-crash-hook")]</c>, a collection that also runs alone, so nothing else can meet
/// the hook. These two tests fail when that stops being true, whether by removing the attribute, by dropping
/// <c>DisableParallelization</c>, or by adding a third class that sets the hook without joining the collection.
/// </summary>
public sealed class ManagedConfCrashHookCollectionTests
{
    private const string CollectionName = "managed-conf-crash-hook";

    /// <summary>An assignment of anything but <c>null</c> to the hook: the assignment that arms a crash.</summary>
    private static readonly Regex ArmsTheHook = new(
        @"ManagedConfMigrationSteps\s*\.\s*FailBetweenSteps\s*=\s*(?!null\b)",
        RegexOptions.CultureInvariant);

    [Fact]
    public void TheClassesThatArmTheHook_ShareOneCollection_ThatRunsAlone()
    {
        foreach (var type in new[] { typeof(ManagedConfMigrationStepsTests), typeof(ManagedConfMigrationRunnerTests) })
        {
            var collection = type.GetCustomAttribute<CollectionAttribute>();
            Assert.True(
                collection is not null && collection.Name == CollectionName,
                $"{type.Name} sets ManagedConfMigrationSteps.FailBetweenSteps, a static shared by the whole test "
                + $"process, so it must carry [Collection(\"{CollectionName}\")]; found "
                + $"{(collection is null ? "no [Collection] attribute" : $"[Collection(\"{collection.Name}\")]")}.");
        }

        var definition = typeof(ManagedConfCrashHookCollection).GetCustomAttribute<CollectionDefinitionAttribute>();
        Assert.NotNull(definition);
        Assert.Equal(CollectionName, definition.Name);
        Assert.True(
            definition.DisableParallelization,
            $"The \"{CollectionName}\" collection must disable parallelization: without it the two classes are "
            + "serialized against each other only, and any other class that reaches WriteTwoSteps can still meet "
            + "a crash that was armed for someone else.");
    }

    [Fact]
    public void EveryTestFileThatArmsTheHook_CarriesTheCollection()
    {
        var thisFile = ThisFile();
        var offenders = new List<string>();
        var filesThatArmTheHook = 0;

        foreach (var path in Directory.EnumerateFiles(Path.GetDirectoryName(thisFile)!, "*.cs", SearchOption.TopDirectoryOnly))
        {
            if (string.Equals(Path.GetFileName(path), Path.GetFileName(thisFile), StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var text = File.ReadAllText(path);
            if (!ArmsTheHook.IsMatch(text))
            {
                continue;
            }

            filesThatArmTheHook++;
            if (!text.Contains($"[Collection(\"{CollectionName}\")]", StringComparison.Ordinal))
            {
                offenders.Add(Path.GetFileName(path));
            }
        }

        Assert.True(
            filesThatArmTheHook >= 2,
            $"Expected the scan to find at least the two known classes that arm the hook, found {filesThatArmTheHook}; "
            + "the scan has stopped seeing the test sources.");
        Assert.True(
            offenders.Count == 0,
            "These test files arm ManagedConfMigrationSteps.FailBetweenSteps without "
            + $"[Collection(\"{CollectionName}\")], so they can crash another class's WriteTwoSteps call: "
            + string.Join(", ", offenders));
    }

    private static string ThisFile([CallerFilePath] string path = "") => path;
}
