/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6470: when the managed store's credential is not there, the Viewer says whether the service is still starting or has
/// never run, from the Windows service's state and whether the store exists yet. The decision is a pure function of those
/// two facts; the state is read from <c>sc.exe query</c> by its numeric code and exit code, which a localized Windows does
/// not translate.
/// </summary>
public sealed class ViewerServiceStartingTests
{
    private const string Subject = "darling.json uses the managed Postgres, but the credential file for the 'admin' role (C:\\x\\admin.cred)";

    [Theory]
    [InlineData(0, "SERVICE_NAME: PerformanceMonitor Darling\n        TYPE               : 10  WIN32_OWN_PROCESS\n        STATE              : 4  RUNNING\n", ViewerServiceRunState.Running)]
    [InlineData(0, "        STATE              : 2  START_PENDING\n", ViewerServiceRunState.Running)]
    [InlineData(0, "        STATE              : 4  EN COURS D'EXECUTION\n", ViewerServiceRunState.Running)]
    [InlineData(0, "        STATE              : 1  STOPPED\n", ViewerServiceRunState.NotRunning)]
    [InlineData(0, "        STATE              : 3  STOP_PENDING\n", ViewerServiceRunState.NotRunning)]
    [InlineData(1060, "[SC] EnumQueryServicesStatus:OpenService FAILED 1060:\n", ViewerServiceRunState.NotInstalled)]
    [InlineData(5, "[SC] OpenService FAILED 5:\n\nAccess is denied.\n", ViewerServiceRunState.Unknown)]
    [InlineData(0, "something else entirely", ViewerServiceRunState.Unknown)]
    public void TheServiceState_IsReadFromTheExitCodeAndTheStateNumber(int exitCode, string output, ViewerServiceRunState expected) =>
        Assert.Equal(expected, ViewerServiceState.Parse(exitCode, output));

    /// <summary>A running service with no credential yet is still starting, whether or not it has begun making the store; a
    /// part-made store under a running service is not called a failed bootstrap.</summary>
    [Fact]
    public void ARunningService_IsStillStarting_EvenWithAPartMadeStore()
    {
        var noStore = ViewerSettings.DescribeMissingCredential(Subject, ViewerServiceRunState.Running, evidence: null);
        Assert.Contains("still starting", noStore, StringComparison.Ordinal);
        Assert.Contains("has not created the store yet", noStore, StringComparison.Ordinal);

        var partMade = ViewerSettings.DescribeMissingCredential(Subject, ViewerServiceRunState.Running, "the cluster in C:\\x\\pg is already initialized");
        Assert.Contains("still starting", partMade, StringComparison.Ordinal);
        Assert.Contains("part-way through creating the store", partMade, StringComparison.Ordinal);
        Assert.DoesNotContain("NOT a first run", partMade, StringComparison.Ordinal);
    }

    /// <summary>A service that is not running says which of "never ran" and "ran and its bootstrap failed" it is.</summary>
    [Fact]
    public void AServiceThatIsNotRunning_SaysNeverRan_OrFailedBootstrap()
    {
        var notInstalled = ViewerSettings.DescribeMissingCredential(Subject, ViewerServiceRunState.NotInstalled, evidence: null);
        Assert.Contains("is not installed on this machine", notInstalled, StringComparison.Ordinal);
        Assert.DoesNotContain("still starting", notInstalled, StringComparison.Ordinal);

        var neverStarted = ViewerSettings.DescribeMissingCredential(Subject, ViewerServiceRunState.NotRunning, evidence: null);
        Assert.Contains("installed but is not running", neverStarted, StringComparison.Ordinal);
        Assert.Contains("never created the store", neverStarted, StringComparison.Ordinal);

        var failed = ViewerSettings.DescribeMissingCredential(Subject, ViewerServiceRunState.NotRunning, "the cluster in C:\\x\\pg is already initialized");
        Assert.Contains("NOT a first run", failed, StringComparison.Ordinal);
        Assert.DoesNotContain("still starting", failed, StringComparison.Ordinal);
    }

    /// <summary>When the state cannot be read, the earlier wording stays, with its hedge for a service already started.</summary>
    [Fact]
    public void AnUnknownServiceState_KeepsTheEarlierWording()
    {
        var message = ViewerSettings.DescribeMissingCredential(Subject, ViewerServiceRunState.Unknown, evidence: null);
        Assert.Contains("Start the PerformanceMonitor Darling service once", message, StringComparison.Ordinal);
        Assert.Contains("If you have ALREADY started it", message, StringComparison.Ordinal);
    }

    /// <summary>End to end through the parse: the state is asked for only when the credential is missing, and the message
    /// the main window shows is the one for that state.</summary>
    [Fact]
    public void Parse_AsksTheServiceState_AndShowsTheMatchingMessage()
    {
        var root = Directory.CreateTempSubdirectory("darling-viewer-starting-");
        try
        {
            var dataDirectory = Path.Combine(root.FullName, "store", "pg");
            Directory.CreateDirectory(dataDirectory);
            File.WriteAllText(Path.Combine(dataDirectory, "PG_VERSION"), "17");
            var json = $$"""{ "postgres": { "managed": true, "dataDirectory": {{JsonSerializer.Serialize(dataDirectory)}} } }""";

            var starting = Assert.Throws<InvalidDataException>(() => ViewerSettings.Parse(json, null, () => ViewerServiceRunState.Running));
            Assert.Contains("still starting", starting.Message, StringComparison.Ordinal);

            var failed = Assert.Throws<InvalidDataException>(() => ViewerSettings.Parse(json, null, () => ViewerServiceRunState.NotRunning));
            Assert.Contains("NOT a first run", failed.Message, StringComparison.Ordinal);
        }
        finally
        {
            root.Delete(recursive: true);
        }
    }
}
