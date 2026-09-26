/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4223 — the authored <c>authored/pg-wraparound</c>, <c>authored/pg-xmin-horizon</c> and
/// <c>authored/pg-replication-slot</c> alert-notebook templates (<c>BuildPgWraparoundCells</c> /
/// <c>BuildPgXminHorizonCells</c> / <c>BuildPgReplicationSlotCells</c>). The shared theories in
/// <see cref="AlertNotebookAuthoredTemplateTests"/> already cover validation, dispatch, compose-catalog and
/// budget/binding/viz for every registered metric, including these three (its <c>AllAuthoredMetrics</c> is
/// data-driven off <see cref="AlertNotebookEndpoint.s_authoredTemplates"/>). This file pins what's specific to
/// this family: routing, the exact cell shape, and the no-matched-incident degrade.
/// </summary>
public sealed class AlertNotebookTemplatePostgresTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private static AlertIncident IncidentWithDatabase(string database) =>
        new("dedup-key", new[] { "obj1" }, Database: database);

    /* ═══════════════════════════ routing ═══════════════════════════ */

    [Theory]
    [InlineData("PostgreSQL Wraparound Risk", "authored/pg-wraparound")]
    [InlineData("PostgreSQL Vacuum Horizon Blocked", "authored/pg-xmin-horizon")]
    [InlineData("PostgreSQL Replication Slot Retention", "authored/pg-replication-slot")]
    public void AuthoredTemplate_RoutesEachPostgresFamily_ToItsOwnTemplate(string metric, string expectedId)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);

        Assert.NotNull(template);
        Assert.Equal(expectedId, template!.Value.Id);
    }

    /* ═══════════════════════════ exact cell shape ═══════════════════════════ */

    [Fact]
    public void WraparoundRisk_BuildsHeaderStatusTwoReadsAndOneTrendPanel()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("PostgreSQL Wraparound Risk");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "PostgreSQL Wraparound Risk", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(5, cells.Count);
        Assert.Equal("header", ((JsonObject)cells[0]!)["type"]!.GetValue<string>());
        Assert.Equal("status", ((JsonObject)cells[1]!)["type"]!.GetValue<string>());

        var firstRead = (JsonObject)cells[2]!;
        Assert.Equal("read", firstRead["type"]!.GetValue<string>());
        Assert.Equal("get_pg_wraparound_risk", firstRead["read"]!.GetValue<string>());

        var secondRead = (JsonObject)cells[3]!;
        Assert.Equal("read", secondRead["type"]!.GetValue<string>());
        Assert.Equal("get_pg_autovacuum_health", secondRead["read"]!.GetValue<string>());
        Assert.Equal("20", secondRead["params"]!["limit"]!.GetValue<string>());

        var trend = (JsonObject)cells[4]!;
        Assert.Equal("panel", trend["type"]!.GetValue<string>());
        Assert.Equal("pg_wraparound_stats", trend["source"]!.GetValue<string>());
        Assert.Equal("pg_xids_remaining", trend["measure"]!.GetValue<string>());
        Assert.Equal("min", trend["aggregate"]!.GetValue<string>());
        Assert.Equal("SalesDb", trend["filters"]![0]!["value"]!.GetValue<string>());
    }

    [Fact]
    public void VacuumHorizonBlocked_BuildsHeaderStatusTwoReadsAndOneTrendPanel_NoDatabaseFilter()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("PostgreSQL Vacuum Horizon Blocked");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "PostgreSQL Vacuum Horizon Blocked", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(5, cells.Count);

        var firstRead = (JsonObject)cells[2]!;
        Assert.Equal("get_pg_xmin_horizon", firstRead["read"]!.GetValue<string>());

        var secondRead = (JsonObject)cells[3]!;
        Assert.Equal("get_pg_session_states", secondRead["read"]!.GetValue<string>());
        Assert.Equal("25", secondRead["params"]!["limit"]!.GetValue<string>());

        var trend = (JsonObject)cells[4]!;
        Assert.Equal("panel", trend["type"]!.GetValue<string>());
        Assert.Equal("pg_xmin_horizon", trend["source"]!.GetValue<string>());
        Assert.Equal("pg_xmin_age", trend["measure"]!.GetValue<string>());
        Assert.Equal("max", trend["aggregate"]!.GetValue<string>());

        // pg_xmin_horizon has no database_name dimension (its own dimension is "source") -- no filter, even
        // with a matched incident that carries a database.
        Assert.Null(trend["filters"]);
    }

    [Fact]
    public void ReplicationSlotRetention_BuildsHeaderStatusTwoReadsAndOneTrendPanel()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("PostgreSQL Replication Slot Retention");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "PostgreSQL Replication Slot Retention", "SRV1", AsOf, WindowStart, WindowEnd,
            IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(5, cells.Count);

        var firstRead = (JsonObject)cells[2]!;
        Assert.Equal("get_pg_replication_slots", firstRead["read"]!.GetValue<string>());

        var secondRead = (JsonObject)cells[3]!;
        Assert.Equal("get_pg_replication_stats", secondRead["read"]!.GetValue<string>());
        Assert.Equal("25", secondRead["params"]!["limit"]!.GetValue<string>());

        var trend = (JsonObject)cells[4]!;
        Assert.Equal("panel", trend["type"]!.GetValue<string>());
        Assert.Equal("pg_replication_slot_stats", trend["source"]!.GetValue<string>());
        Assert.Equal("pg_slot_retained_wal_bytes", trend["measure"]!.GetValue<string>());
        Assert.Equal("max", trend["aggregate"]!.GetValue<string>());
        Assert.Equal("SalesDb", trend["filters"]![0]!["value"]!.GetValue<string>());
    }

    /* ═══════════════════════════ no-matched-incident degrade ═══════════════════════════ */

    [Theory]
    [InlineData("PostgreSQL Wraparound Risk")]
    [InlineData("PostgreSQL Vacuum Horizon Blocked")]
    [InlineData("PostgreSQL Replication Slot Retention")]
    public void EachFamily_WithNoMatchedIncident_KeepsTheSameCellCount(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var withoutIncident = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, null, null, "Unknown");
        var withIncident = template.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(withIncident.Count, withoutIncident.Count);
    }
}
