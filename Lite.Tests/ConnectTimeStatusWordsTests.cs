/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Windows;
using System.Windows.Controls;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// What Lite says about a server in the half minute after launch, before its first connection check has finished. The collectors
/// have run and the Overview card's rows are green, so a status word that reads as a verdict ("Unknown") contradicts the rows beside
/// it. The word, its headline and the sidebar dot's tooltip say the server is connecting. The server tab's header says the same, and
/// that the first collection is pending, so an Overview chart with no data in the window reads as loading, not as empty.
/// </summary>
[InMemoryCredentials]
[Collection("application-theme")]
public sealed class ConnectTimeStatusWordsTests
{
    [Fact]
    public void AServerNotYetChecked_SaysConnecting_NotUnknown()
    {
        var status = ServerCardStatusRules.Classify(isOnline: null, hasCollectorErrors: false);

        Assert.Equal("Connecting", status.Word());
        Assert.StartsWith("Connecting", status.Headline(), StringComparison.Ordinal);
        Assert.DoesNotContain("Unknown", status.Headline(), StringComparison.Ordinal);
    }

    [Fact]
    public void TheSidebarDot_OfAServerNotYetChecked_SaysConnecting()
    {
        var connection = new ServerConnection { Id = "s1", ServerName = "sql-a", DisplayName = "sql-a", IsEnabled = true, IsOnline = null };

        Assert.Equal("Connecting", connection.DotStatus);
        Assert.Contains("Connecting: the first connection check has not finished yet", connection.DotTooltip, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServerTabHeader_WhileConnecting_SaysTheFirstCollectionIsPending_NotThatTheChartsAreEmpty()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ConnectTimeStatus_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var text = StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "connect-time-test", DisplayName = "connect-time-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                try
                {
                    return ((TextBlock)tab.FindName("ConnectionStatusText")).Text;
                }
                finally
                {
                    tab.StopRefresh();
                }
            }));

            Assert.StartsWith("Connecting", text, StringComparison.Ordinal);
            Assert.Contains("first collection", text, StringComparison.Ordinal);
            /* #8621: the charts may already show the data the store holds, so the line must not promise that they are still to fill. */
            Assert.DoesNotContain("charts", text, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>When the host says the first collection has finished, with or without a collection error, the header stops saying
    /// "Connecting" at once and words itself from the refresh, instead of waiting for the next refresh pass.</summary>
    [Fact]
    public void TheServerTabHeader_StopsSayingConnecting_WhenTheFirstCollectionFinishes()
    {
        var dir = Path.Combine(Path.GetTempPath(), "ConnectTimeFinished_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        try
        {
            using var duckDb = new DuckDbInitializer(Path.Combine(dir, "test.duckdb"));
            var text = StaTestThread.Run(() => WithTheme(() =>
            {
                var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = "connect-finished-test", DisplayName = "connect-finished-test", IsEnabled = true };
                var tab = new ServerTab(server, duckDb, new CredentialResolver(new CredentialService(), null));
                try
                {
                    tab.FirstCollectionFinished();
                    return ((TextBlock)tab.FindName("ConnectionStatusText")).Text;
                }
                finally
                {
                    tab.StopRefresh();
                }
            }));

            Assert.DoesNotContain("Connecting", text, StringComparison.Ordinal);
            Assert.StartsWith("Last refresh:", text, StringComparison.Ordinal);
        }
        finally
        {
            try { Directory.Delete(dir, recursive: true); } catch { /* best-effort cleanup */ }
        }
    }

    /// <summary>The tab's markup names the theme's static resources, so the theme is merged for the build and removed after it.</summary>
    private static T WithTheme<T>(Func<T> body)
    {
        var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
        ResourceDictionary? added = null;
        if (!app.Resources.Contains("NumericCell"))
        {
            added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
            app.Resources.MergedDictionaries.Add(added);
        }

        try
        {
            return body();
        }
        finally
        {
            if (added is not null) app.Resources.MergedDictionaries.Remove(added);
        }
    }
}
