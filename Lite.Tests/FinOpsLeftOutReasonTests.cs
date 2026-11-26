/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9769: the Server Inventory logs why a server is left out when its live read failed and the store holds no collected row for
/// it, so a reader of that line does not have to find the refusal logged once at start. A server Lite never connected to names the
/// last connection error (a host this user has not confirmed names the confirmation rule), and one it has connected to says the
/// collector has stored nothing for it yet. Driven through <see cref="FinOpsServerInventory.BuildAsync"/>, the path the tab uses.
///
/// <para>Collection <c>app-logger-statics</c>: the assertions drain the process-wide log buffer the other logger tests share.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class FinOpsLeftOutReasonTests
{
    private const string LeftOutText = "has no collected server properties";

    private static ServerConnection Server(string name) => new()
    {
        Id = Guid.NewGuid().ToString(),
        ServerName = name,
        DisplayName = name + " (display)",
    };

    private static Task<ServerPropertiesRow?> NoCollected(ServerConnection server) =>
        Task.FromResult<ServerPropertiesRow?>(null);

    private static Task<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>> NoMetrics() =>
        Task.FromResult<IReadOnlyDictionary<int, LocalDataService.ServerMetricsRow>>(new Dictionary<int, LocalDataService.ServerMetricsRow>());

    /// <summary>Builds the inventory for one server whose live read throws <paramref name="liveFailure"/> and whose store holds no row,
    /// and returns the line that says it is left out.</summary>
    private static async Task<string> LeftOutLineAsync(ServerConnection server, Exception liveFailure, ServerConnectionStatus? status)
    {
        AppLogger.DrainBufferedLines();
        var build = await FinOpsServerInventory.BuildAsync(
            new[] { server },
            _ => throw liveFailure,
            NoCollected,
            NoMetrics,
            readStatus: status == null ? null : _ => status);
        var log = AppLogger.DrainBufferedLines();

        Assert.Empty(build.Rows);
        return Assert.Single(log, l => l.Contains(server.DisplayName + " " + LeftOutText, StringComparison.Ordinal));
    }

    [Fact]
    public async Task AServerWhoseHostIsNotConfirmed_NamesTheConfirmationRule()
    {
        var server = Server("unconfirmed");
        const string rule = "RULE-SENTENCE: the host has not been confirmed from this account";
        var status = new ServerConnectionStatus { ServerId = server.Id, IsOnline = null, ErrorMessage = rule };

        var line = await LeftOutLineAsync(server, new ServerHostNotConfirmedException(rule), status);

        Assert.Contains(rule, line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerTheConnectionCheckFoundDown_NamesTheLastConnectionError_AsTheSidebarSaysIt()
    {
        var server = Server("down");
        var status = new ServerConnectionStatus
        {
            ServerId = server.Id,
            IsOnline = false,
            ErrorMessage = "ERROR-FROM-THE-CONNECTION-CHECK",
            StatusChangedAt = new DateTime(2026, 11, 26, 8, 0, 0, DateTimeKind.Utc),
        };

        var line = await LeftOutLineAsync(server, new InvalidOperationException("ERROR-FROM-THE-LIVE-READ"), status);

        Assert.Contains(status.UnreachableReason!, line, StringComparison.Ordinal);
        Assert.DoesNotContain("ERROR-FROM-THE-LIVE-READ", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerThatHasConnected_SaysNothingIsCollectedYet_NotThatItCannotBeReached()
    {
        var server = Server("connected");
        var status = new ServerConnectionStatus { ServerId = server.Id, IsOnline = true, ErrorMessage = "A-STALE-ERROR-FROM-AN-EARLIER-CHECK" };

        var connected = await LeftOutLineAsync(server, new InvalidOperationException("ERROR-FROM-THE-LIVE-READ"), status);
        var down = await LeftOutLineAsync(server, new InvalidOperationException("ERROR-FROM-THE-LIVE-READ"), new ServerConnectionStatus { ServerId = server.Id, IsOnline = false, ErrorMessage = "DOWN" });

        Assert.DoesNotContain("A-STALE-ERROR-FROM-AN-EARLIER-CHECK", connected, StringComparison.Ordinal);
        Assert.Contains("ERROR-FROM-THE-LIVE-READ", connected, StringComparison.Ordinal);
        Assert.NotEqual(connected, down);
    }

    [Fact]
    public async Task AServerNotYetChecked_NamesTheLiveReadsFailure()
    {
        var server = Server("unchecked");

        var line = await LeftOutLineAsync(server, new InvalidOperationException("ERROR-FROM-THE-LIVE-READ"), new ServerConnectionStatus { ServerId = server.Id });

        Assert.Contains("ERROR-FROM-THE-LIVE-READ", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WithNoStatusRead_TheLiveReadsFailureIsStillNamed()
    {
        var server = Server("nostatus");

        var line = await LeftOutLineAsync(server, new ServerHostNotConfirmedException("RULE-SENTENCE-WITHOUT-A-STATUS"), status: null);

        Assert.Contains("RULE-SENTENCE-WITHOUT-A-STATUS", line, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AServerWithACollectedRow_IsNotLeftOut_AndLogsNoLeftOutLine()
    {
        var server = Server("collected");
        AppLogger.DrainBufferedLines();

        var build = await FinOpsServerInventory.BuildAsync(
            new[] { server },
            _ => throw new InvalidOperationException("planted live failure"),
            _ => Task.FromResult<ServerPropertiesRow?>(new ServerPropertiesRow { Edition = "Standard Edition", CollectionTime = DateTime.UtcNow }),
            NoMetrics,
            readStatus: _ => new ServerConnectionStatus { ServerId = server.Id, IsOnline = false, ErrorMessage = "DOWN" });
        var log = AppLogger.DrainBufferedLines();

        Assert.Single(build.Rows);
        Assert.DoesNotContain(log, l => l.Contains(server.DisplayName + " " + LeftOutText, StringComparison.Ordinal));
    }
}
