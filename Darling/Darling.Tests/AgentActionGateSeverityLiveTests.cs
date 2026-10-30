/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/* #1776 own-store: the fact mints its own scratch database through ScratchPostgres, so the class is deliberately NOT [Collection("live-postgres")]. */

/// <summary>
/// #6928: the agent-action gate re-resolves a request's routes at the tier each alert arrived at, so a Critical alert dispatched under a
/// fleet route is not refused because a server route that takes only Warning (agent Off) would have won the lookup with no tier.
/// </summary>
public sealed class AgentActionGateSeverityLiveTests
{
    private static string? ConnectionString => Environment.GetEnvironmentVariable("DARLING_TEST_PG");

    [Fact]
    public async Task ACriticalAlertDispatchedUnderTheFleetRoute_IsNotRefusedByAWarningOnlyServerRouteThatIsOff()
    {
        Assert.SkipWhen(string.IsNullOrEmpty(ConnectionString), "Set DARLING_TEST_PG to run the agent gate severity pin (it mints its own scratch database).");
        var ct = TestContext.Current.CancellationToken;

        await using var scratch = await ScratchPostgres.CreateAsync(ConnectionString!, ct);
        await using (var migrate = new NpgsqlConnection(scratch.ConnectionString))
        {
            await migrate.OpenAsync(ct);
            await PgMigrations.MigrateAsync(migrate, ct);
        }

        await using var owner = NpgsqlDataSource.Create(scratch.ConnectionString);
        var bodySucceeded = false;
        try
        {
            var proxies = new WebhookProxies(null, null, null, null);
            /* A server-scoped Blocking route that takes Warning only, with the agent Off, and a fleet-wide Blocking route with the agent at Act. */
            await using (var seed = owner.CreateCommand("INSERT INTO config.config_monitored_servers (server_id, name, host) VALUES (7, 'srv-7', 'host-a') ON CONFLICT DO NOTHING"))
            {
                await seed.ExecuteNonQueryAsync(ct);
            }

            await NotificationRouteWriter.SaveAsync(
                owner, new NotificationRouteDraft { MetricMatch = "Blocking", ScopeServerId = 7, Severities = "warning", AgentLevel = 0 },
                null, proxies, null, null, 30, ct);
            await NotificationRouteWriter.SaveAsync(
                owner, new NotificationRouteDraft { MetricMatch = "Blocking", AgentLevel = 2 }, null, proxies, null, null, 30, ct);

            var store = new InvestigationRequestStore(owner, 30);
            /* A Critical alert dispatches under the fleet route: request level 2. */
            var opened = await store.OpenOrJoinAsync(
                new InvestigationAlert(1, 7, "srv-7", "blocking", "Blocking", DateTime.UtcNow, Array.Empty<string>(), null, 20, 100, LeaseMinutes: 15, ExpiryMinutes: 240)
                { AutonomyLevel = 2, Severity = "critical" }, ct);
            var request = opened.RequestId!.Value;
            var claim = (await store.ClaimAsync(request, "agent-a", ct)).ClaimId!.Value;

            var writes = 0;
            var hooks = new AgentActionHooks
            {
                LoadLevels = (connection, _, token) => DarlingAgentActionGate.LoadLevelsAsync(connection, NullLogger.Instance, token),
                ReadBefore = (_, _, _) => Task.FromResult<string?>(null),
                Write = (_, _, _) => { writes++; return Task.FromResult<string?>("{}"); },
            };
            var call = new AgentActionCall("create_mute_rule", "mute", new AgentAction(request, claim, "agent-a", "known batch",
                "[{\"tool\":\"get_blocking\",\"arguments\":{},\"key_numbers\":\"12\"}]"), new AgentActionTarget(7, "srv-7", null, null));

            var done = await store.RunAgentActionAsync(call, hooks, ct);

            Assert.True(done.Done, done.Refusal?.Message);
            Assert.Equal(1, writes);
            bodySucceeded = true;
        }
        finally
        {
            await LiveStoreCleanup.RunAsync(scratch.ConnectionString, bodySucceeded, static (_, _) => Task.CompletedTask);
        }
    }
}
