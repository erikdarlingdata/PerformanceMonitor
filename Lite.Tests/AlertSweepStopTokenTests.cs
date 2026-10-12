/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.RegularExpressions;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #4752, Lite's half: closing the app cancels an alert post in flight. The engine and the deliverer already hand a
/// caller's token to every webhook post, but Lite's sweep called <c>EvaluateServerAsync(snapshot)</c> with no token,
/// so nothing ever raised the cancel and a post to an endpoint that never answers ran out its full timeout while the
/// window closed. The sweep now passes the app-lifetime token (<c>_backgroundCts</c>, cancelled once in
/// <c>MainWindow_Closing</c>), and a cancel while that source is cancelled ends the sweep quietly instead of
/// reaching the catch that logs a failed sweep as an error.
/// <para>
/// Read from the source, like the other pins on <c>MainWindow</c> wiring in this project: the window cannot be built
/// in a test, and what this guards is a wiring omission that compiles either way (the token parameter is optional).
/// </para>
/// </summary>
public sealed class AlertSweepStopTokenTests
{
    private static string SweepSource() =>
        Regex.Replace(Lite.Tests.ParitySource.ReadFile("Lite/MainWindow.AlertEngine.cs"), @"\s+", " ");

    [Fact]
    public void TheSweep_PassesTheAppLifetimeToken_ToTheEngine()
    {
        var sweep = SweepSource();

        Assert.Contains("var stopping = _backgroundCts?.Token ?? CancellationToken.None;", sweep, StringComparison.Ordinal);
        Assert.Contains("await _alertEngine.EvaluateServerAsync(snapshot, stopping);", sweep, StringComparison.Ordinal);

        /* The control: the old spelling, which gave the engine nothing to cancel. */
        Assert.DoesNotContain("EvaluateServerAsync(snapshot);", sweep, StringComparison.Ordinal);
    }

    [Fact]
    public void TheAppLifetimeSource_IsCancelledOnce_WhenTheWindowCloses()
    {
        var window = Lite.Tests.ParitySource.ReadFile("Lite/MainWindow.xaml.cs");

        Assert.Single(Regex.Matches(window, Regex.Escape("_backgroundCts?.Cancel();")));
        Assert.Contains("_backgroundCts = new CancellationTokenSource();", window, StringComparison.Ordinal);
    }

    [Fact]
    public void ACloseTimeCancel_EndsTheSweepQuietly_AndIsNotLoggedAsAnError()
    {
        var sweep = SweepSource();

        var quiet = sweep.IndexOf(
            "catch (OperationCanceledException) when (stopping.IsCancellationRequested)", StringComparison.Ordinal);
        Assert.True(quiet >= 0, "the sweep has no catch for a cancel while the app-lifetime source is cancelled");

        var generic = sweep.IndexOf("catch (Exception ex)", quiet, StringComparison.Ordinal);
        Assert.True(generic > quiet, "the quiet catch has to come before the catch that logs a failed sweep");

        var body = sweep[quiet..generic];
        Assert.Contains("return;", body, StringComparison.Ordinal);
        Assert.DoesNotContain("AppLogger", body, StringComparison.Ordinal);

        /* The control: the catch that does log is the one after it, so the check above is not passing because the
           logger is spelled some other way. */
        Assert.Contains("AppLogger.Error(\"Alerts\", $\"Alert sweep failed for", sweep[generic..], StringComparison.Ordinal);
    }
}
