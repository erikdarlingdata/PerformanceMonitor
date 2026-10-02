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
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Source pins for who may make the install id's row (#4961). The service makes it once at start, after the store's
/// migrations and before any worker runs; the CLI and the Viewer only read it. A reader that quietly inserted would
/// make an id for a store nobody started, and two makers racing is what the row's one-row CHECK and the make's
/// race-safe update exist to survive, not to invite.
/// </summary>
public sealed class StoreInstallIdSourcePinTests
{
    private static readonly string[] ProductProjects =
    {
        "PerformanceMonitor.Darling.Service",
        "PerformanceMonitor.Darling.Viewer",
        "PerformanceMonitor.Darling.Storage",
        "PerformanceMonitor.Darling.Analysis",
    };

    private static IEnumerable<string> ProductSourceFiles() =>
        ProductProjects
            .SelectMany(project => Directory.EnumerateFiles(Path.Combine(RepoFile.Root, "Darling", project), "*.cs", SearchOption.AllDirectories))
            .Where(path => !path.Contains(Path.DirectorySeparatorChar + "obj" + Path.DirectorySeparatorChar, StringComparison.Ordinal)
                && !path.Contains(Path.DirectorySeparatorChar + "bin" + Path.DirectorySeparatorChar, StringComparison.Ordinal));

    [Fact]
    public void OnlyTheServiceWorkerCallsTheEnsure_SoTheCliAndTheViewerNeverMakeTheRow()
    {
        var callers = ProductSourceFiles()
            .Where(path => CSharpSourceWalker.StripCommentsAndStrings(File.ReadAllText(path))
                .Contains("StoreInstallId.EnsureAsync(", StringComparison.Ordinal))
            .Select(Path.GetFileName)
            .ToList();

        Assert.Equal(new[] { "DarlingWorker.cs" }, callers);

        /* The two readers, named, so a failure says which surface broke the rule. */
        var viewerCallers = Directory.EnumerateFiles(Path.Combine(RepoFile.Root, "Darling", "PerformanceMonitor.Darling.Viewer"), "*.cs", SearchOption.AllDirectories)
            .Where(path => File.ReadAllText(path).Contains("StoreInstallId.EnsureAsync", StringComparison.Ordinal))
            .ToList();
        Assert.Empty(viewerCallers);

        foreach (var cliFile in new[] { "DarlingCliCommands.cs", "Program.cs" })
        {
            var cli = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", cliFile);
            Assert.DoesNotContain("StoreInstallId.EnsureAsync", cli, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheReadPathIsASelect_AndItsMethodsReferenceNoWriteStatement()
    {
        var sql = StoreInstallId.ReadSql.TrimStart();
        Assert.StartsWith("SELECT", sql, StringComparison.OrdinalIgnoreCase);
        foreach (var verb in new[] { "INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER", "CREATE", "GRANT" })
        {
            Assert.DoesNotContain(verb, sql, StringComparison.OrdinalIgnoreCase);
        }

        var code = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Storage", "StoreInstallId.cs"));
        var searchFrom = 0;
        var readers = 0;
        while (true)
        {
            var at = code.IndexOf("TryReadAsync(", searchFrom, StringComparison.Ordinal);
            if (at < 0)
            {
                break;
            }

            var open = code.IndexOf('{', at);
            var semicolon = code.IndexOf(';', at);
            searchFrom = at + 1;
            if (open < 0 || (semicolon >= 0 && semicolon < open))
            {
                continue;   // an expression-bodied member or a call: the declarations with a block are the ones to read
            }

            var body = CSharpSourceWalker.BraceBalanced(code, open);
            readers++;
            foreach (var write in new[] { "InsertSql", "ReplaceSql", "ExecuteNonQueryAsync", "EnsureAsync" })
            {
                Assert.DoesNotContain(write, body, StringComparison.Ordinal);
            }
        }

        Assert.True(readers > 0, "no TryReadAsync body was found to pin");
    }

    [Fact]
    public void TheWorkerMakesTheIdAfterTheMigrationsAndBeforeTheWorkers_AndKeepsIt()
    {
        var worker = CSharpSourceWalker.StripCommentsAndStrings(
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs"));

        var migrate = worker.IndexOf("PgMigrations.MigrateAsync(migrateConnection", StringComparison.Ordinal);
        var ensure = worker.IndexOf("StoreInstallId.EnsureAsync(", StringComparison.Ordinal);
        Assert.True(migrate >= 0, "the worker no longer migrates the store at start, so this pin is comparing against nothing");
        Assert.True(ensure > migrate, "the install id is made after the store's migrations, never before them");

        /* Inside the startup retry's try, before its `break`: a failure to make the id is retried and triaged like a
           failed migration, and no worker has started by then. */
        var leave = worker.IndexOf("break;", migrate, StringComparison.Ordinal);
        Assert.True(leave > ensure, "the install id is made inside the startup retry's try, before the loop is left");
        var catches = worker.IndexOf("catch (", migrate, StringComparison.Ordinal);
        Assert.True(catches > ensure, "the make sits in the same try as the migration");

        Assert.Contains("_installId =", worker, StringComparison.Ordinal);
        Assert.Contains("internal string? InstallId", worker, StringComparison.Ordinal);
    }
}
