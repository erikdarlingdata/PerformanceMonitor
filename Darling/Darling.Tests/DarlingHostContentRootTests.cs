/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Microsoft.Extensions.Hosting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9837: the tests of this collection change the process's working folder, which every other class reads, so the collection runs
/// alone.
/// </summary>
[CollectionDefinition("process-working-folder", DisableParallelization = true)]
public sealed class ProcessWorkingFolderCollection
{
}

/// <summary>
/// #9837: a host the service builds takes the folder that holds the binaries as its content root, whatever folder the process was
/// started in. A host on the working folder watches that folder and everything under it, and under systemd the working folder is
/// <c>/</c>. Each test starts in a folder of its own, so the working folder is never the binaries' folder by accident.
/// </summary>
[Collection("process-working-folder")]
public sealed class DarlingHostContentRootTests
{
    [Fact]
    public void ProcessHost_IsBuiltOnTheBinariesFolder_NotTheWorkingFolder()
    {
        InAWorkingFolderOfItsOwn(workingFolder =>
        {
            var builder = DarlingProcessServices.CreateHostBuilder([]);

            AssertOnTheBinariesFolder(builder.Environment, workingFolder);
        });
    }

    [Fact]
    public void McpHost_IsBuiltOnTheBinariesFolder_NotTheWorkingFolder()
    {
        InAWorkingFolderOfItsOwn(workingFolder =>
        {
            var builder = DarlingMcpHostService.CreateHostBuilder();

            AssertOnTheBinariesFolder(builder.Environment, workingFolder);
        });
    }

    private static void AssertOnTheBinariesFolder(IHostEnvironment environment, string workingFolder)
    {
        var contentRoot = Trimmed(environment.ContentRootPath);
        Assert.Equal(Trimmed(AppContext.BaseDirectory), contentRoot);
        Assert.NotEqual(Trimmed(workingFolder), contentRoot);
    }

    private static string Trimmed(string path) => Path.TrimEndingDirectorySeparator(Path.GetFullPath(path));

    private static void InAWorkingFolderOfItsOwn(Action<string> body)
    {
        var before = Directory.GetCurrentDirectory();
        var workingFolder = Directory.CreateTempSubdirectory("darling-9837-").FullName;
        try
        {
            Directory.SetCurrentDirectory(workingFolder);
            body(Directory.GetCurrentDirectory());
        }
        finally
        {
            Directory.SetCurrentDirectory(before);
            Directory.Delete(workingFolder, recursive: true);
        }
    }
}
