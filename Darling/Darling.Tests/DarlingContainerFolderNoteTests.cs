/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9734: a tarball run in a container built from the stock .NET image gets <c>DOTNET_RUNNING_IN_CONTAINER=true</c> from the
/// image, so the service takes the compose key folder and, when it cannot use it, refused with no word about why. The refusal
/// names the variable only when the variable is what chose the folder, and the choice of folder is the same as before.
/// </summary>
[Collection("darling-config-env")]
public sealed class DarlingContainerFolderNoteTests
{
    private const string Variable = "DOTNET_RUNNING_IN_CONTAINER";
    private const string Unmanaged = "{\"postgres\":{\"connectionString\":\"Host=h\",\"managed\":false}}";

    [Fact]
    public void TheNote_NamesTheVariableAndTheComposeFolder_OnlyWhenTheVariableChoseTheFolder()
    {
        var previous = Environment.GetEnvironmentVariable(Variable);
        var folder = Path.Combine(Path.GetTempPath(), "darling-folder-note-" + Guid.NewGuid().ToString("N"));
        try
        {
            var config = DarlingConfig.Parse(Unmanaged);
            var configPath = Path.Combine(folder, "darling.json");

            Environment.SetEnvironmentVariable(Variable, "true");
            var inContainer = DarlingLogHashKeyFile.ContainerFolderNote(config);
            Assert.NotNull(inContainer);
            Assert.Contains(Variable, inContainer, StringComparison.Ordinal);
            Assert.Contains(DarlingManagedRoles.ComposeStoreCredentialDirectory, inContainer, StringComparison.Ordinal);
            Assert.Contains("darling-keys", inContainer, StringComparison.Ordinal);
            Assert.Equal(DarlingManagedRoles.ComposeStoreCredentialDirectory, DarlingLogHashKeyFile.DirectoryFor(config, configPath));

            Environment.SetEnvironmentVariable(Variable, null);
            Assert.Null(DarlingLogHashKeyFile.ContainerFolderNote(config));
            Assert.Equal(
                Path.Combine(folder, DarlingLogHashKeyFile.BringYourOwnDirectoryName),
                DarlingLogHashKeyFile.DirectoryFor(config, configPath));
        }
        finally
        {
            Environment.SetEnvironmentVariable(Variable, previous);
        }
    }

    [Fact]
    public void ALogHashKeyRefusal_CarriesTheNote_WhenGiven_AndNotWhenTheFolderWasNotTheContainerChoice()
    {
        // A file where the folder should be: the folder cannot be created or used, which is the refusal the issue saw.
        var blocker = Path.Combine(Path.GetTempPath(), "darling-folder-note-" + Guid.NewGuid().ToString("N"));
        File.WriteAllText(blocker, "not a folder");
        try
        {
            var note = DarlingLogHashKeyFile.LoadWithNote(blocker, "DOTNET_RUNNING_IN_CONTAINER is true, so the folder is the compose folder.", NullLogger.Instance);
            Assert.Null(note.Key);
            Assert.EndsWith("so the folder is the compose folder.", note.Refusal, StringComparison.Ordinal);

            var plain = DarlingLogHashKeyFile.LoadWithNote(blocker, null, NullLogger.Instance);
            Assert.Null(plain.Key);
            Assert.False(string.IsNullOrEmpty(plain.Refusal));
            Assert.DoesNotContain(Variable, plain.Refusal, StringComparison.Ordinal);
        }
        finally
        {
            File.Delete(blocker);
        }
    }
}
