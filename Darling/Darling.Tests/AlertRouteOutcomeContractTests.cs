/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4750: the shape of the per-channel outcome on the alert-history route record. The end-to-end behaviour
/// (a failing channel beside a delivering one) is <see cref="AlertRouteChannelOutcomeTests"/>; these pin the
/// contract that behaviour writes into: the spellings, the projection from a fan-out's outcome map, and a
/// row written before the member existed.
/// </summary>
public sealed class AlertRouteOutcomeContractTests
{
    /// <summary>The column outlives any build, so the spellings are a contract: a value is added, never renamed.</summary>
    [Fact]
    public void TheOutcomeSpellings_AreThePersistedContract()
    {
        Assert.Equal("delivered", AlertRouteOutcomes.Delivered);
        Assert.Equal("failed", AlertRouteOutcomes.Failed);
        Assert.Equal("not attempted", AlertRouteOutcomes.NotAttempted);
    }

    /// <summary>A caller with no per-channel result records none: the outcome is null, "this row does not say",
    /// not a claim that anything was attempted or not.</summary>
    [Fact]
    public void ToDto_WithoutAnOutcomeMap_LeavesEveryOutcomeNull()
    {
        var dto = Decision().ToDto();

        Assert.NotEmpty(dto.Destinations);
        Assert.All(dto.Destinations, d => Assert.Null(d.Outcome));
    }

    /// <summary>With a map every channel the route resolved to says one of the three words: delivered and
    /// failed as the map has them, and anything else (a throttle, a fold) or a channel the map does not name
    /// as "not attempted". A channel that resolved to nothing is still omitted.</summary>
    [Fact]
    public void ToDto_WithAnOutcomeMap_SpellsEachResolvedChannel_AndStillOmitsTheUnresolvedOnes()
    {
        var map = new Dictionary<string, AlertChannelOutcome>
        {
            [NotificationRouter.SlackChannel] = AlertChannelOutcome.Delivered,
            [NotificationRouter.GenericChannel] = AlertChannelOutcome.Failed,
            [NotificationRouter.TeamsChannel] = AlertChannelOutcome.Throttled,
            /* PagerDuty resolved to nothing on this store: an entry for it changes nothing. */
            [NotificationRouter.PagerDutyChannel] = AlertChannelOutcome.Delivered,
        };

        var dto = Decision().ToDto(map);

        Assert.Equal(
            new[]
            {
                (NotificationRouter.TeamsChannel, AlertRouteOutcomes.NotAttempted),
                (NotificationRouter.SlackChannel, AlertRouteOutcomes.Delivered),
                (NotificationRouter.GenericChannel, AlertRouteOutcomes.Failed),
                (NotificationRouter.EmailChannel, AlertRouteOutcomes.NotAttempted),
            },
            dto.Destinations.Select(d => (d.Channel, d.Outcome!)).ToArray());
    }

    /// <summary>A row written before the member existed has no Outcome and reads as null through both readers
    /// — the same way a row before the route record has no Route.</summary>
    [Fact]
    public void ARouteRecordWrittenBeforeOutcomesExisted_StillReads_WithANullOutcome()
    {
        var older =
            "{\"Details\":[],\"Route\":{\"Family\":\"" + AlertFamily.Performance + "\",\"RouteId\":null,"
            + "\"Destinations\":[{\"Channel\":\"Slack\",\"RouteId\":null,\"Source\":\"Default\"}]}}";

        var read = AlertContextSerializer.TryReadRoute(older);
        Assert.NotNull(read);
        var destination = Assert.Single(read!.Destinations);
        Assert.Equal(("Slack", (int?)null, "Default"), (destination.Channel, destination.RouteId, destination.Source));
        Assert.Null(destination.Outcome);

        Assert.True(AlertContextSerializer.TryDeserialize(older, out var context));
        Assert.Null(Assert.Single(context.Route!.Destinations).Outcome);
    }

    /// <summary>The word survives the serializer both ways, and a null Outcome stays null.</summary>
    [Fact]
    public void TheOutcome_RoundTripsThroughTheContextJson()
    {
        var map = new Dictionary<string, AlertChannelOutcome>
        {
            [NotificationRouter.SlackChannel] = AlertChannelOutcome.Failed,
        };

        var withOutcomes = new AlertContext { Route = Decision().ToDto(map) };
        var read = AlertContextSerializer.TryReadRoute(AlertContextSerializer.Serialize(withOutcomes));
        Assert.Equal(
            "failed", read!.Destinations.Single(d => d.Channel == NotificationRouter.SlackChannel).Outcome);

        var without = new AlertContext { Route = Decision().ToDto() };
        var readWithout = AlertContextSerializer.TryReadRoute(AlertContextSerializer.Serialize(without));
        Assert.All(readWithout!.Destinations, d => Assert.Null(d.Outcome));
    }

    /// <summary>get_alert_history hands the word to callers as <c>outcome</c>, null on a row that predates it.</summary>
    [Fact]
    public void TheAlertHistoryRouteProjection_CarriesTheOutcome()
    {
        var map = new Dictionary<string, AlertChannelOutcome>
        {
            [NotificationRouter.SlackChannel] = AlertChannelOutcome.Delivered,
            [NotificationRouter.GenericChannel] = AlertChannelOutcome.Failed,
        };

        AssertProjectedOutcomes(
            Decision().ToDto(map),
            (NotificationRouter.TeamsChannel, "not attempted"),
            (NotificationRouter.SlackChannel, "delivered"),
            (NotificationRouter.GenericChannel, "failed"),
            (NotificationRouter.EmailChannel, "not attempted"));

        AssertProjectedOutcomes(
            Decision().ToDto(),
            (NotificationRouter.TeamsChannel, null),
            (NotificationRouter.SlackChannel, null),
            (NotificationRouter.GenericChannel, null),
            (NotificationRouter.EmailChannel, null));
    }

    private static void AssertProjectedOutcomes(AlertRouteDto dto, params (string Channel, string? Outcome)[] expected)
    {
        var payload = DarlingMcpAlertTools.RouteHistoryPayload(dto);
        using var document = JsonDocument.Parse(JsonSerializer.Serialize(payload));
        var actual = document.RootElement.GetProperty("destinations").EnumerateArray()
            .Select(d => (
                d.GetProperty("channel").GetString()!,
                d.GetProperty("outcome").ValueKind == JsonValueKind.Null ? null : d.GetProperty("outcome").GetString()))
            .ToArray();
        Assert.Equal(expected, actual);
    }

    /// <summary>Teams, Slack, Generic and Email resolve from the parent settings (source Default); PagerDuty
    /// has no key, so it resolves to nothing.</summary>
    private static NotificationRouteDecision Decision()
    {
        var config = new DarlingConfig();
        config.Webhooks.TeamsUrl = "https://teams.example.invalid/hook";
        config.Webhooks.SlackUrl = "https://slack.example.invalid/hook";
        config.Webhooks.GenericUrl = "https://generic.example.invalid/hook";
        config.Smtp.Host = "smtp.example.invalid";
        config.Smtp.From = "monitor@example.invalid";
        config.Smtp.To = "operator@example.invalid";

        return NotificationRouter.Resolve("Deadlocks Detected", null, new DarlingAlertSettings(config));
    }
}
