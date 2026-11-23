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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9365: a refresh whose read failed showed the driver's own text in the status bar ("refresh failed: Exception while reading from
/// stream") and wrote nothing to the Viewer log. The failure now writes one warning line naming the tab, the read and the time, the status
/// bar says what did not load in plain words, and a chart read that fails alone is tried once more inside the same refresh while the grid
/// stays filled. The store here is a port nothing listens on, so the reads that are not under test fail at once.
/// </summary>
[Collection("viewer-logger-statics")]
public sealed class ViewerRefreshFailureTests : IDisposable
{
    private const string RawDriverText = "Exception while reading from stream";

    private readonly string _logDir =
        Path.Combine(Path.GetTempPath(), "darling-refreshfailure-tests", Guid.NewGuid().ToString("N"));

    public void Dispose()
    {
        try
        {
            ViewerLogger.Shutdown();
            if (Directory.Exists(_logDir))
            {
                Directory.Delete(_logDir, recursive: true);
            }
        }
        catch
        {
            /* Best-effort test cleanup. */
        }
    }

    [Fact]
    public void PlainText_NamesWhatDidNotLoad_AndNeverTheRawExceptionMessage()
    {
        var chart = ViewerRefreshFailure.PlainText("Queries > Top Queries by Duration", "slicer read", ViewerRefreshFailure.GridIsCurrent);
        var grid = ViewerRefreshFailure.PlainText("Queries > Top Queries by Duration", "grid read");
        var tabOnly = ViewerRefreshFailure.PlainText("Overview", null);

        Assert.Equal("The time-range chart did not load. The grid is current.", chart);
        Assert.Equal("The grid did not load.", grid);
        Assert.Equal("Overview did not load. The Viewer log has the cause.", tabOnly);
        Assert.DoesNotContain(RawDriverText, chart + grid + tabOnly, StringComparison.Ordinal);
    }

    [Fact]
    public void Report_WritesOneWarnLine_WithTabReadTimeAndTheExceptionChain_AndReturnsPlainWords()
    {
        var tag = Guid.NewGuid().ToString("N");
        ViewerLogger.Initialize(_logDir);
        var failure = new InvalidOperationException(RawDriverText, new IOException("Unable to read data from the transport connection"));

        var text = ViewerRefreshFailure.Report($"{tag}: Queries > Top Queries by Duration", "slicer read", 15442, failure, ViewerRefreshFailure.GridIsCurrent);

        Assert.DoesNotContain(RawDriverText, text, StringComparison.Ordinal);
        var lines = TaggedLines(tag);
        var line = Assert.Single(lines);
        Assert.Contains("[WARN ]", line, StringComparison.Ordinal);
        Assert.Contains("Top Queries by Duration", line, StringComparison.Ordinal);
        Assert.Contains("slicer read", line, StringComparison.Ordinal);
        Assert.Contains("15442 ms", line, StringComparison.Ordinal);
        Assert.Contains("InvalidOperationException: " + RawDriverText, line, StringComparison.Ordinal);
        Assert.Contains("IOException: Unable to read data", line, StringComparison.Ordinal);
    }

    [Fact]
    public void RefreshedStatus_NamesThePartialFailure_AndKeepsTheStamp_AndOtherwiseSaysRefreshed()
    {
        Assert.Equal("srv: refreshed 12:00:01", ViewerServerTab.RefreshedStatus("srv", null, "12:00:01"));
        Assert.Equal(
            "srv: The time-range chart did not load. The grid is current. (12:00:01)",
            ViewerServerTab.RefreshedStatus("srv", "The time-range chart did not load. The grid is current.", "12:00:01"));
    }

    [Fact]
    public void AChartRead_ThatFailsTwice_LeavesPlainWordsAndExactlyOneWarnLine_AndIsReadAgainOnce()
    {
        var tag = "chart-" + Guid.NewGuid().ToString("N");
        string? partial = null;
        List<TimeSliceBucket>? answer = new();
        var agains = 0;
        var statuses = new List<string>();
        RunThemed(async () =>
        {
            ViewerLogger.Initialize(_logDir);
            var tab = NewTab(21, tag, statuses);
            answer = await tab.ReadSlicerWithOneRetryAsync(
                "Queries > Top Queries by Duration",
                Task.FromException<List<TimeSliceBucket>>(new IOException(RawDriverText)),
                () =>
                {
                    agains++;
                    return Task.FromException<List<TimeSliceBucket>>(new IOException(RawDriverText));
                });
            partial = tab.PartialFailureText;
        });

        Assert.Null(answer);
        Assert.Equal(1, agains);
        Assert.Equal("The time-range chart did not load. The grid is current.", partial);
        var line = Assert.Single(TaggedLines(tag));
        Assert.Contains("slicer read", line, StringComparison.Ordinal);
        Assert.Contains("after one retry", line, StringComparison.Ordinal);
        Assert.Empty(statuses);
    }

    [Fact]
    public void AChartRead_ThatFailsOnceThenSucceeds_IsFilledAfterTheOneRetry_WithNoFailureTextAndNoLine()
    {
        var tag = "retry-" + Guid.NewGuid().ToString("N");
        string? partial = "unset";
        List<TimeSliceBucket>? answer = null;
        var agains = 0;
        var good = new List<TimeSliceBucket> { new() { BucketTime = new DateTime(2026, 11, 23, 12, 0, 0, DateTimeKind.Utc) } };
        RunThemed(async () =>
        {
            ViewerLogger.Initialize(_logDir);
            var tab = NewTab(22, tag, new List<string>());
            answer = await tab.ReadSlicerWithOneRetryAsync(
                "Queries > Top Queries by Duration",
                Task.FromException<List<TimeSliceBucket>>(new IOException(RawDriverText)),
                () =>
                {
                    agains++;
                    return Task.FromResult(good);
                });
            partial = tab.PartialFailureText;
        });

        Assert.Same(good, answer);
        Assert.Equal(1, agains);
        Assert.Null(partial);
        Assert.Empty(TaggedLines(tag));
    }

    [Fact]
    public void AChartRead_ThatFailsAfterTheLoadWasCancelled_IsNotRetried_AndWritesNothing()
    {
        var tag = "cancel-" + Guid.NewGuid().ToString("N");
        Exception? thrown = null;
        var agains = 0;
        RunThemed(async () =>
        {
            ViewerLogger.Initialize(_logDir);
            var tab = NewTab(23, tag, new List<string>());
            using var cts = new CancellationTokenSource();
            cts.Cancel();
            ViewerReadScope.Enter(cts.Token);
            try
            {
                await tab.ReadSlicerWithOneRetryAsync(
                    "Queries > Top Queries by Duration",
                    Task.FromException<List<TimeSliceBucket>>(new IOException(RawDriverText)),
                    () =>
                    {
                        agains++;
                        return Task.FromResult(new List<TimeSliceBucket>());
                    });
            }
            catch (Exception ex)
            {
                thrown = ex;
            }
            finally
            {
                ViewerReadScope.Clear();
            }
        });

        Assert.IsType<IOException>(thrown);
        Assert.Equal(0, agains);
        Assert.Empty(TaggedLines(tag));
    }

    [Fact]
    public void AFailedLoad_ShowsPlainWordsInTheStatusBar_AndWritesOneWarnLineNamingTheTab()
    {
        var tag = "load-" + Guid.NewGuid().ToString("N");
        var statuses = new List<string>();
        RunThemed(async () =>
        {
            ViewerLogger.Initialize(_logDir);
            var tab = NewTab(24, tag, statuses);
            await tab.LoadInnerTabAsync(ViewerServerTab.QueriesInnerTabIndex);
        });

        var status = Assert.Single(statuses);
        Assert.Contains("did not load", status, StringComparison.Ordinal);
        Assert.DoesNotContain("refresh failed", status, StringComparison.Ordinal);
        var line = Assert.Single(TaggedLines(tag));
        Assert.Contains("Queries", line, StringComparison.Ordinal);
        Assert.Contains(" ms", line, StringComparison.Ordinal);
    }

    /// <summary>The warning lines the Refresh source wrote for a tab whose display name carries <paramref name="tag"/>.</summary>
    private List<string> TaggedLines(string tag)
    {
        ViewerLogger.Flush();
        return Directory.Exists(_logDir)
            ? Directory.GetFiles(_logDir)
                .SelectMany(File.ReadAllLines)
                .Where(l => l.Contains("[WARN ]", StringComparison.Ordinal) && l.Contains("[Refresh]", StringComparison.Ordinal) && l.Contains(tag, StringComparison.Ordinal))
                .ToList()
            : new List<string>();
    }

    private static ViewerServerTab NewTab(int serverId, string alias, List<string> statuses)
    {
        var probe = new TcpListener(IPAddress.Loopback, 0);
        probe.Start();
        var port = ((IPEndPoint)probe.LocalEndpoint).Port;
        probe.Stop();
        var tab = new ViewerServerTab(
            new ViewerDataService($"Host=127.0.0.1;Port={port};Database=darling;Username=darling;Timeout=1"),
            new DarlingServer(serverId, alias + "-host.invalid", alias, true, null))
        {
            SlowLoadSink = _ => { },
        };
        tab.StatusChanged += statuses.Add;
        return tab;
    }

    private static void RunThemed(Func<Task> body) => StaTestThread.Run(async () =>
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary
            {
                Source = new Uri("pack://application:,,,/PerformanceMonitor.Darling.Viewer;component/Themes/DarkTheme.xaml"),
            };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            await body();
        }
        finally
        {
            if (added is not null)
            {
                app.Resources.MergedDictionaries.Remove(added);
            }
        }
    });
}
