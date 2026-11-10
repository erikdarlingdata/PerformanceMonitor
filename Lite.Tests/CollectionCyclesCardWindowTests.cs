/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Windows.Controls;
using Darling.Tests;
using DuckDB.NET.Data;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;
using static PerformanceMonitorLite.Tests.PageLayoutProbe;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8533: the Collection Cycles card carries two lines read over different windows. The skipped-cycles line is a 24 hour read and is
/// always shown. The connection-failure line reads the grid's rows, which hold each collector's runs for the trailing seven days,
/// and is shown only when those rows hold a failure that reached several collectors. Each fact builds the real server tab on the
/// shared STA thread with the theme loaded over a store of its own, runs the tab's real Collection Health refresh, and reads what
/// the card draws.
/// </summary>
[Collection("credential-manager")]
public sealed class CollectionCyclesCardWindowTests
{
    private const string PreLoginHandshake =
        "A connection was successfully established with the server, but then an error occurred during the pre-login handshake. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)";

    private static CredentialResolver Credentials() => new(new CredentialService(), null);

    /// <summary>
    /// With no failure to word the card draws the skipped-cycles line alone, so a subtitle that also captioned connection failures
    /// over seven days would read as an all-clear the rows cannot give: they hold each collector's newest error only.
    /// </summary>
    [Theory]
    [InlineData(1280, 800)]
    [InlineData(900, 600)]
    public void WithNoFailureToWord_TheSubtitleCaptionsTheSkippedSlotsAlone_DrawnWhole(int width, int height)
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(width), height, Credentials(), (tab, window, _) =>
        {
            var found = new List<string>();
            Refresh(tab);
            var skipped = Find<TextBlock>(tab, "SkippedCyclesSummary");
            Reveal(skipped);
            Settle(window);

            if (!skipped.Text.Contains("24 h", StringComparison.Ordinal))
            {
                found.Add($"the skipped-cycles line \"{skipped.Text}\" does not give its 24 hours");
            }

            if (Find<TextBlock>(tab, "ConnectionFailureSummary").IsVisible)
            {
                found.Add("a connection-failure line is shown over a store that holds no run");
            }

            var card = Find<ChartPanel>(tab, "CollectionHealthCard");
            if (card.Template?.FindName("PART_Subtitle", card) is not TextBlock subtitle || !subtitle.IsVisible)
            {
                found.Add("the Collection Cycles card drew no subtitle");
                return found;
            }

            if (!subtitle.Text.Contains("24 hours", StringComparison.Ordinal))
            {
                found.Add($"the subtitle \"{subtitle.Text}\" does not give the 24 hours the skipped cycles are read over");
            }

            if (subtitle.Text.Contains("connection failure", StringComparison.OrdinalIgnoreCase))
            {
                found.Add($"the subtitle \"{subtitle.Text}\" captions connection failures over a card that shows no connection-failure line");
            }

            var natural = ClippedTextProbe.NaturalWidth(subtitle);
            if (natural > subtitle.ActualWidth + 0.5)
            {
                found.Add($"the subtitle needs {natural:F0} px and is trimmed to {subtitle.ActualWidth:F0} px");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// Two collectors fail the same open 43 hours ago, and each also holds an ERROR run eight days old. The refresh reads the rows
    /// and words the line itself: the drop is on the card, said to be in the last 7 days, and the run past those days is not counted,
    /// so each collector "has no other error".
    /// </summary>
    [Fact]
    public void ADropTheRowsHold_IsWordedByTheRefresh_AgainstItsOwnSevenDays_AndARunPastThemIsNotCounted()
    {
        var problems = WithServerTab("DarkTheme", PaneWidth(900), 600, Credentials(), (tab, window, duckDb) =>
        {
            var found = new List<string>();
            var serverId = (int)typeof(ServerTab).GetField("_serverId", Instance)!.GetValue(tab)!;
            var now = DateTime.UtcNow;
            var dropped = now.AddHours(-43);
            Seed(duckDb, serverId, 1, "wait_stats", now.AddDays(-8), PreLoginHandshake);
            Seed(duckDb, serverId, 2, "query_stats", now.AddDays(-8).AddSeconds(3), PreLoginHandshake);
            Seed(duckDb, serverId, 3, "wait_stats", dropped, PreLoginHandshake);
            Seed(duckDb, serverId, 4, "query_stats", dropped.AddSeconds(3), PreLoginHandshake);

            Refresh(tab);
            var failure = Find<TextBlock>(tab, "ConnectionFailureSummary");
            Reveal(failure);
            Settle(window);

            if (!failure.IsVisible)
            {
                found.Add("the refresh shows no connection-failure line for a drop that reached two collectors");
            }
            else if (!failure.Text.Contains("reached 2 collectors: each missed that one run and has no other error in the last 7 days", StringComparison.Ordinal))
            {
                found.Add($"the line \"{failure.Text}\" does not word the drop against the rows' seven days");
            }

            return found;
        });

        Assert.True(problems.Count == 0, string.Join(Environment.NewLine, problems));
    }

    /// <summary>
    /// Runs the tab's own Collection Health refresh to its end on the STA thread. The refresh starts under the dispatcher's context, as the
    /// app starts it from the tab's own handlers, so its code after each await resumes on the tab's thread and writes the card. Started
    /// with no context it resumes on a worker, the first write to the card throws, and the refresh logs that and leaves the card as it was.
    /// </summary>
    private static void Refresh(ServerTab tab)
    {
        Task refresh = Task.CompletedTask;
        StaTestThread.WithDispatcherContext(() => refresh = (Task)Invoke(tab, "RefreshCollectionHealthAsync", 24, null, null, CancellationToken.None)!);
        Pump(refresh);
    }

    private static void Seed(DuckDbInitializer duckDb, int serverId, long logId, string collector, DateTime collectionTimeUtc, string error)
    {
        using var readLock = duckDb.AcquireReadLock();
        using var connection = duckDb.CreateConnection();
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO collection_log
    (log_id, server_id, server_name, collector_name, collection_time,
     duration_ms, status, error_message, rows_collected, sql_duration_ms, duckdb_duration_ms)
VALUES ($1, $2, 'page-layout-test', $3, $4, 100, 'ERROR', $5, 0, 50, 50)";
        command.Parameters.Add(new DuckDBParameter { Value = logId });
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = collector });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new DuckDBParameter { Value = error });
        command.ExecuteNonQuery();
    }
}
