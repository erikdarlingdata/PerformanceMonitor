/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4733: the stopped-collection marker file. After a terminal startup verdict the process stays up on
/// purpose, so a container that never collects still shows as running. <see cref="CollectorRuntimeState"/>
/// therefore writes a small file the compose healthcheck can test for, when it is given a path, and does
/// nothing at all when it is not (the Windows service and every non-compose deployment).
///
/// <para>The file is a side effect of a verdict that is already published, so the tests pin it from both
/// ends: the file appears with the detail and the time when the phase becomes <c>Stopped</c> and goes away
/// when the phase leaves it or a new state object is created; and a fault in either direction is logged once
/// and never reaches the worker, because a marker that could throw would cost the operator the very verdict
/// it exists to report.</para>
/// </summary>
public sealed class CollectorStoppedMarkerTests : IDisposable
{
    private readonly string _directory =
        Path.Combine(Path.GetTempPath(), "darling-stopped-marker-" + Guid.NewGuid().ToString("N"));

    public CollectorStoppedMarkerTests() => Directory.CreateDirectory(_directory);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_directory, recursive: true);
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            /* Temp-directory cleanup only; a leftover directory under the OS temp path harms nothing. */
        }
    }

    private string MarkerPath => Path.Combine(_directory, "darling-stopped");

    [Fact]
    public void AStoppedVerdict_WritesTheMarker_WithTheDetailAndTheUtcTime()
    {
        var state = new CollectorRuntimeState(MarkerPath);
        Assert.False(File.Exists(MarkerPath), "nothing is written before a verdict");

        state.PublishStopped(CollectorRuntimeState.StartupStep.Store);

        var snapshot = state.Read()!;
        Assert.Equal(CollectorRuntimeState.CollectorPhase.Stopped, snapshot.Phase);
        Assert.True(File.Exists(MarkerPath), "a Stopped verdict must leave the marker the healthcheck tests for");

        var lines = File.ReadAllLines(MarkerPath);
        Assert.Equal(2, lines.Length);
        Assert.Equal(snapshot.AsOfUtc.ToString("O", CultureInfo.InvariantCulture), lines[0]);
        Assert.Equal(snapshot.Detail, lines[1]);
        Assert.Equal(CollectorRuntimeState.FirstLineOf(CollectorRuntimeState.FailureDetailFor(CollectorRuntimeState.StartupStep.Store)), lines[1]);
    }

    /// <summary>
    /// All three ways to reach <c>Stopped</c> write it: the exception-path stand-down, the rejected
    /// configuration, and the managed-store-on-a-non-Windows-host gate. A marker written only by
    /// <c>PublishStopped</c> would leave a container with a bad config looking healthy.
    /// </summary>
    [Fact]
    public void EveryRouteToStopped_WritesTheMarker()
    {
        var routes = new (string Name, Action<CollectorRuntimeState> Publish)[]
        {
            ("exception path", s => s.PublishStopped(CollectorRuntimeState.StartupStep.ManagedStore)),
            ("rejected configuration", s => s.PublishConfigurationProblems(new DarlingConfig
            {
                Postgres = null!,
                Servers = [new MonitoredServer { Host = "test-host" }],
            })),
            ("managed store on a non-Windows host", s => s.PublishManagedStoreNeedsWindows()),
        };

        for (var i = 0; i < routes.Length; i++)
        {
            var path = Path.Combine(_directory, $"marker-{i}");
            var state = new CollectorRuntimeState(path);

            routes[i].Publish(state);

            Assert.Equal(CollectorRuntimeState.CollectorPhase.Stopped, state.Read()!.Phase);
            Assert.True(File.Exists(path), $"{routes[i].Name} reached Stopped without writing the marker");
            Assert.Equal(state.Read()!.Detail, File.ReadAllLines(path)[1]);
        }
    }

    /// <summary>
    /// Retrying and Collecting are the healthy side of the line: a store that is briefly down is being
    /// retried, so the container must keep reporting healthy while it is.
    /// </summary>
    [Fact]
    public void RetryingAndCollecting_WriteNoMarker()
    {
        var state = new CollectorRuntimeState(MarkerPath);

        state.PublishRetrying(CollectorRuntimeState.StartupStep.Store, attempt: 3, attempts: 25);
        state.PublishRetrying(CollectorRuntimeState.StartupStep.Store, attempt: 30, attempts: 25, sustained: true);
        state.PublishCollecting();

        Assert.False(File.Exists(MarkerPath));
    }

    [Fact]
    public void ANewStateObject_RemovesALeftoverMarker()
    {
        /* A restarted container keeps its /tmp, so the previous run's marker is still there. */
        File.WriteAllText(MarkerPath, "left behind by the previous run");

        var state = new CollectorRuntimeState(MarkerPath);

        Assert.False(File.Exists(MarkerPath), "a new run must not inherit the previous run's verdict");
        Assert.Null(state.Read());
    }

    [Fact]
    public void LeavingTheStoppedPhase_RemovesTheMarker()
    {
        var state = new CollectorRuntimeState(MarkerPath);

        state.PublishStopped(CollectorRuntimeState.StartupStep.Store);
        Assert.True(File.Exists(MarkerPath));
        state.PublishCollecting();
        Assert.False(File.Exists(MarkerPath), "collecting again must take the marker down with it");

        state.PublishStopped(CollectorRuntimeState.StartupStep.Store);
        Assert.True(File.Exists(MarkerPath));
        state.PublishRetrying(CollectorRuntimeState.StartupStep.Store, attempt: 1, attempts: 25);
        Assert.False(File.Exists(MarkerPath), "retrying again must take the marker down with it");
    }

    /// <summary>
    /// The Windows service and every non-compose deployment: no path, no file, and nothing removed. A file
    /// that happens to sit where a marker would be is left alone by a state that was given no path.
    /// </summary>
    [Fact]
    public void WithNoPath_NothingIsWrittenOrRemoved()
    {
        File.WriteAllText(MarkerPath, "unrelated");

        foreach (var state in new[]
        {
            new CollectorRuntimeState(),
            new CollectorRuntimeState(null),
            new CollectorRuntimeState(string.Empty),
            new CollectorRuntimeState("   "),
        })
        {
            state.PublishStopped(CollectorRuntimeState.StartupStep.Store);
            state.PublishRetrying(CollectorRuntimeState.StartupStep.Store, attempt: 1, attempts: 25);
            state.PublishCollecting();
        }

        Assert.Equal("unrelated", File.ReadAllText(MarkerPath));
        Assert.Equal(new[] { MarkerPath }, Directory.GetFileSystemEntries(_directory));
    }

    [Fact]
    public void AFaultWritingTheMarker_IsLoggedOnce_AndNeverThrows()
    {
        var logger = new CapturingTestLogger();
        var unwritable = Path.Combine(_directory, "no-such-directory", "darling-stopped");
        var state = new CollectorRuntimeState(unwritable, logger);

        state.PublishStopped(CollectorRuntimeState.StartupStep.Store);
        state.PublishStopped(CollectorRuntimeState.StartupStep.Configuration);

        Assert.Equal(CollectorRuntimeState.CollectorPhase.Stopped, state.Read()!.Phase);
        Assert.Equal(CollectorRuntimeState.StartupStep.Configuration, state.Read()!.Step);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains(unwritable, logger.Lines[0], StringComparison.Ordinal);
    }

    [Fact]
    public void AFaultRemovingALeftoverMarker_IsLoggedOnce_AndNeverThrows()
    {
        /* A directory where the marker file belongs: deleting it as a file is refused on every platform. */
        Directory.CreateDirectory(MarkerPath);
        var logger = new CapturingTestLogger();

        var state = new CollectorRuntimeState(MarkerPath, logger);
        state.PublishStopped(CollectorRuntimeState.StartupStep.Store);
        state.PublishCollecting();

        Assert.Equal(CollectorRuntimeState.CollectorPhase.Collecting, state.Read()!.Phase);
        Assert.Equal(1, logger.CountAtLevel(LogLevel.Warning));
        Assert.Contains(MarkerPath, logger.Lines[0], StringComparison.Ordinal);
    }

    /// <summary>
    /// The path is read once, in <c>Program.cs</c>, where the singleton is registered, and handed to the
    /// state object. Nothing else reads the variable, so the compose file's name for it and the code's
    /// cannot drift apart in a second place.
    /// </summary>
    [Fact]
    public void ProgramReadsTheMarkerVariableOnce_AndHandsItToTheState()
    {
        /* The raw reader, not the LF one: every anchor below sits on a single line or crosses a break with \s*, so
           it reads the same under either line ending and needs no place in the LF-reader census. */
        var program = ReadRepoFile("Darling/PerformanceMonitor.Darling.Service/Program.cs");

        Assert.Single(Regex.Matches(program, Regex.Escape("GetEnvironmentVariable(\"DARLING_STOPPED_MARKER\")")));
        Assert.Matches(@"new CollectorRuntimeState\(\s*stoppedMarkerPath\b", program);
        Assert.DoesNotContain("AddSingleton<CollectorRuntimeState>()", program, StringComparison.Ordinal);
    }
}
