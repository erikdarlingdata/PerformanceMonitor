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
using System.Reflection;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitor.Darling.Tests;

/// <summary>
/// #6444: the routes event delivery and alert delivery read (<see cref="DarlingAlertSettings.NotificationRoutes"/>) come from a
/// cache keyed by <see cref="DarlingWebhookSecrets.Signature"/>. The key left out the fallback route, the agent level, the limits
/// and the lease and expiry minutes, so after a control-plane reload that changed only one of those the cache kept answering with
/// the old route: an expired investigation request still went to a fallback that had been cleared.
/// </summary>
public sealed class DarlingRouteCacheReloadTests
{
    private static NotificationRoute Route(int id = 2) =>
        new(id, "High CPU", "", "", "", "", "", true)
        {
            AgentLevel = 1,
            FallbackRouteId = 3,
            AgentMaxPerHour = 20,
            AgentMaxPerDay = 100,
            AgentLeaseMinutes = 15,
            AgentExpiryMinutes = 60,
        };

    private static DarlingAlertSettings Settings(DarlingConfig config) =>
        new(config, null, DarlingPasswordKey.Refusing("The password key is not available here."));

    /// <summary>The reload path the service runs after a config change (the by-reference apply), then the read event delivery makes.</summary>
    private static IReadOnlyList<NotificationRoute> ReadAfterReload(
        DarlingAlertSettings settings, DarlingConfig config, IReadOnlyList<NotificationRoute> reloaded)
    {
        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            NotificationRoutes = reloaded,
            Alerts = config.Alerts,
            Analysis = config.Analysis,
            Smtp = config.Smtp,
            Webhooks = config.Webhooks,
        });
        return settings.NotificationRoutes;
    }

    public static IEnumerable<object[]> Edits() => new[]
    {
        new object[] { "fallback cleared", (Func<NotificationRoute, NotificationRoute>)(r => r with { FallbackRouteId = null }) },
        new object[] { "fallback moved", (Func<NotificationRoute, NotificationRoute>)(r => r with { FallbackRouteId = 4 }) },
        new object[] { "agent level", (Func<NotificationRoute, NotificationRoute>)(r => r with { AgentLevel = 0 }) },
        new object[] { "agent level cleared", (Func<NotificationRoute, NotificationRoute>)(r => r with { AgentLevel = null }) },
        new object[] { "max per hour", (Func<NotificationRoute, NotificationRoute>)(r => r with { AgentMaxPerHour = 5 }) },
        new object[] { "max per day", (Func<NotificationRoute, NotificationRoute>)(r => r with { AgentMaxPerDay = 7 }) },
        new object[] { "lease minutes", (Func<NotificationRoute, NotificationRoute>)(r => r with { AgentLeaseMinutes = 30 }) },
        new object[] { "expiry minutes", (Func<NotificationRoute, NotificationRoute>)(r => r with { AgentExpiryMinutes = 90 }) },
        new object[] { "enabled", (Func<NotificationRoute, NotificationRoute>)(r => r with { Enabled = false }) },
    };

    [Theory]
    [MemberData(nameof(Edits))]
    public void AReloadThatChangesOnlyOneRouteField_ReachesTheRoutesDeliveryReads(string edit, Func<NotificationRoute, NotificationRoute> change)
    {
        var config = new DarlingConfig { NotificationRoutes = new[] { Route() } };
        var settings = Settings(config);
        var before = settings.NotificationRoutes.Single();
        Assert.Equal(3, before.FallbackRouteId);

        var after = ReadAfterReload(settings, config, new[] { change(Route()) }).Single();

        var expected = change(Route());
        Assert.True(
            expected.FallbackRouteId == after.FallbackRouteId && expected.AgentLevel == after.AgentLevel
            && expected.AgentMaxPerHour == after.AgentMaxPerHour && expected.AgentMaxPerDay == after.AgentMaxPerDay
            && expected.AgentLeaseMinutes == after.AgentLeaseMinutes && expected.AgentExpiryMinutes == after.AgentExpiryMinutes
            && expected.Enabled == after.Enabled,
            $"After a reload that changed only the {edit}, the routes delivery reads still carry the old row.");
    }

    [Fact]
    public void EveryWritableRouteMember_ChangesTheCacheKey()
    {
        /* The guard against drift: a member added to NotificationRoute later must move the key too, or the next one of these
           is the same defect. Every writable member is changed in turn and must change the signature. */
        var clone = typeof(NotificationRoute).GetMethod("<Clone>$", BindingFlags.Instance | BindingFlags.Public)!;
        var status = DarlingPasswordKey.Refusing("The password key is not available here.").Status;
        var webhooks = new WebhooksConfig();
        var baseRoute = Route();
        var baseSignature = DarlingWebhookSecrets.Signature(webhooks, new[] { baseRoute }, status);

        var untouched = new List<string>();
        foreach (var property in typeof(NotificationRoute).GetProperties(BindingFlags.Instance | BindingFlags.Public).Where(p => p.SetMethod is not null))
        {
            var changed = ChangedValue(property.PropertyType, property.GetValue(baseRoute));
            var copy = (NotificationRoute)clone.Invoke(baseRoute, null)!;
            property.SetValue(copy, changed);
            if (string.Equals(baseSignature, DarlingWebhookSecrets.Signature(webhooks, new[] { copy }, status), StringComparison.Ordinal))
            {
                untouched.Add(property.Name);
            }
        }

        Assert.True(untouched.Count == 0, "These route members do not change the cache key: " + string.Join(", ", untouched));
    }

    private static object? ChangedValue(Type type, object? current)
    {
        if (type == typeof(string))
        {
            return ((string?)current ?? "") + "x";
        }

        if (type == typeof(int))
        {
            return (int)current! + 1;
        }

        if (type == typeof(int?))
        {
            return ((int?)current ?? 0) + 1;
        }

        if (type == typeof(bool))
        {
            return !(bool)current!;
        }

        if (type == typeof(bool?))
        {
            return current is null ? true : !(bool)current;
        }

        if (type == typeof(IReadOnlyCollection<string>))
        {
            return new[] { "slack" };
        }

        if (type == typeof(IReadOnlySet<int>))
        {
            return new HashSet<int> { 9 };
        }

        if (type == typeof(WebhookSigningKeys))
        {
            return new WebhookSigningKeys("k", "", null);
        }

        throw new InvalidOperationException("The drift guard does not know how to change a " + type.Name + "; teach it here.");
    }
}
