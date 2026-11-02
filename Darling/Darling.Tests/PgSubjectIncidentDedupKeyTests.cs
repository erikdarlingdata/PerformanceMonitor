/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7157: a PostgreSQL Tier 0 or Poison Wait page fired with the bare subject (a database, a slot, a wait event)
/// as its dedup key, so the PagerDuty dedup_key had no server in it. Two servers sharing a subject then shared one
/// PagerDuty incident: the second page merged into the first, and the first server's Cleared resolved it.
/// </summary>
public sealed class PgSubjectIncidentDedupKeyTests
{
    private const string Wraparound = "PostgreSQL Wraparound";
    private const string PoisonWait = "PostgreSQL Poison Waits";

    private static string PagerDutyKey(string serverName, string metric, string subject)
    {
        var context = new AlertContext
        {
            Incidents = new List<AlertIncident>
            {
                DarlingWorker.BuildPgSubjectIncident(serverName, metric, subject, subject),
            },
        };
        // The send derives the key from the context, serverId is only the no-incident fallback.
        return WebhookAlertService.DerivePagerDutyDedupKey(serverName, metric, context);
    }

    [Fact]
    public void Two_servers_with_the_same_database_get_different_pagerduty_keys()
    {
        var a = PagerDutyKey("aurora-a", Wraparound, "app");
        var b = PagerDutyKey("aurora-b", Wraparound, "app");

        Assert.NotEqual(a, b);
        Assert.NotEqual("app", a);
    }

    [Fact]
    public void Two_servers_in_the_same_poison_wait_get_different_pagerduty_keys()
    {
        Assert.NotEqual(
            PagerDutyKey("aurora-a", PoisonWait, "IPC:BufferIo"),
            PagerDutyKey("aurora-b", PoisonWait, "IPC:BufferIo"));
    }

    [Fact]
    public void One_server_keeps_its_key_across_sweeps()
    {
        Assert.Equal(
            PagerDutyKey("aurora-a", PoisonWait, "IPC:BufferIo"),
            PagerDutyKey("aurora-a", PoisonWait, "IPC:BufferIo"));
    }

    [Fact]
    public void A_slot_and_a_database_with_one_name_on_one_server_stay_apart()
    {
        Assert.NotEqual(
            PagerDutyKey("aurora-a", Wraparound, "app"),
            PagerDutyKey("aurora-a", "PostgreSQL Replication Slot", "app"));
    }

    [Fact]
    public void The_history_seed_reads_the_key_the_alert_fires_with()
    {
        var incident = DarlingWorker.BuildPgSubjectIncident("aurora-a", PoisonWait, "IPC:BufferIo", "12.5");

        Assert.Equal(incident.DedupKey, DarlingWorker.PgSubjectDedupKey("aurora-a", PoisonWait, "IPC:BufferIo"));
        Assert.NotEqual("IPC:BufferIo", incident.DedupKey);
    }
}
