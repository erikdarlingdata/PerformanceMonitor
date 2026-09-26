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
/// Pins for #4223's authored <c>authored/cpu</c> alert-notebook template
/// (<see cref="AlertNotebookEndpoint.AuthoredTemplate"/> / <c>BuildCpuCells</c>): routing off the "High CPU"
/// metric, the exact cell list (both engines' CPU timelines present, titled), and the no-matched-incident
/// degrade. The shared theories in <c>AlertNotebookAuthoredTemplateTests</c> (validation, dispatch names, the
/// compose catalog, budget, binding, viz) already cover this family via its
/// <see cref="AlertNotebookEndpoint.s_authoredTemplates"/> row — these pins are the family-specific shape only.
/// </summary>
public sealed class AlertNotebookTemplateCpuTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private static AlertIncident IncidentWithDatabase(string database) =>
        new("dedup-key", new[] { "obj1" }, Database: database);

    [Fact]
    public void HighCpu_RoutesToTheAuthoredCpuTemplate()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("High CPU");

        Assert.NotNull(template);
        Assert.Equal("authored/cpu", template!.Value.Id);
    }

    [Fact]
    public void BuildCpuCells_HasTheExactCellListWithBothEnginesPanelsTitled()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("High CPU");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "High CPU", "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(7, cells.Count);

        var header = Assert.IsType<JsonObject>(cells[0]);
        Assert.Equal("header", (string)header["type"]!);

        var status = Assert.IsType<JsonObject>(cells[1]);
        Assert.Equal("status", (string)status["type"]!);

        var sqlServerCpu = Assert.IsType<JsonObject>(cells[2]);
        Assert.Equal("panel", (string)sqlServerCpu["type"]!);
        Assert.Equal("SQL Server CPU", (string)sqlServerCpu["title"]!);
        Assert.Equal("cpu_utilization_stats", (string)sqlServerCpu["source"]!);
        Assert.Equal("sqlserver_cpu_utilization", (string)sqlServerCpu["measure"]!);

        var pgCpu = Assert.IsType<JsonObject>(cells[3]);
        Assert.Equal("panel", (string)pgCpu["type"]!);
        Assert.Equal("PostgreSQL CPU", (string)pgCpu["title"]!);
        Assert.Equal("pg_cpu_utilization", (string)pgCpu["source"]!);
        Assert.Equal("pg_acu_utilization_pct", (string)pgCpu["measure"]!);

        var topQueries = Assert.IsType<JsonObject>(cells[4]);
        Assert.Equal("read", (string)topQueries["type"]!);
        Assert.Equal("get_top_queries_by_cpu", (string)topQueries["read"]!);
        Assert.Equal("Top queries by CPU", (string)topQueries["title"]!);

        var topProcedures = Assert.IsType<JsonObject>(cells[5]);
        Assert.Equal("read", (string)topProcedures["type"]!);
        Assert.Equal("get_top_procedures_by_cpu", (string)topProcedures["read"]!);
        Assert.Equal("Top procedures by CPU", (string)topProcedures["title"]!);

        var scheduler = Assert.IsType<JsonObject>(cells[6]);
        Assert.Equal("read", (string)scheduler["type"]!);
        Assert.Equal("get_cpu_scheduler_pressure", (string)scheduler["read"]!);
        Assert.Equal("Scheduler pressure", (string)scheduler["title"]!);
    }

    [Fact]
    public void BuildCpuCells_WithNoMatchedIncident_StillDegradesWithTheSameCellCount()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("High CPU");
        Assert.NotNull(template);

        var withoutIncident = template!.Value.BuildCells(
            "High CPU", "SRV1", AsOf, WindowStart, WindowEnd, null, null,
            "Unknown (not collected since " + AsOf + ")");

        var withIncident = template.Value.BuildCells(
            "High CPU", "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        Assert.Equal(withIncident.Count, withoutIncident.Count);

        var definition = new JsonObject { ["kind"] = "notebook", ["cells"] = withoutIncident };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);
        Assert.True(validation.IsValid, validation.Error);
    }
}
