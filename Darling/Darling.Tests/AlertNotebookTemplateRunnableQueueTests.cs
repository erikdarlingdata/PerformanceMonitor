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
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #5641's authored <c>authored/runnable-queue</c> alert-notebook template
/// (<see cref="AlertNotebookEndpoint.AuthoredTemplate"/> / <c>BuildRunnableQueueCells</c>): routing off the "Runnable
/// Queue" metric (it used to fall back to the generic notebook), the exact cell list (the scheduler evidence first,
/// no PostgreSQL panel) and the no-matched-incident degrade. The shared theories in
/// <c>AlertNotebookAuthoredTemplateTests</c> (validation, dispatch names, the compose catalog, budget, binding, viz)
/// cover this family through its <see cref="AlertNotebookEndpoint.s_authoredTemplates"/> row.
/// </summary>
public sealed class AlertNotebookTemplateRunnableQueueTests
{
    private static readonly DateTime WindowEnd = new(2026, 10, 9, 16, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-10-09T16:00:00Z";

    [Fact]
    public void RunnableQueue_RoutesToItsOwnAuthoredTemplate_NotTheCpuOne()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Runnable Queue");

        Assert.NotNull(template);
        Assert.Equal("authored/runnable-queue", template!.Value.Id);
        Assert.NotEqual(AlertNotebookEndpoint.AuthoredTemplate("High CPU")!.Value.Id, template.Value.Id);
    }

    [Fact]
    public void BuildRunnableQueueCells_HasTheExactCellList_SchedulerEvidenceFirst_AndNoPostgresPanel()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Runnable Queue");
        Assert.NotNull(template);

        var buildCells = template!.Value.BuildCells;
        Assert.NotNull(buildCells);

        var cells = buildCells("Runnable Queue", "SRV1", AsOf, WindowStart, WindowEnd, null, null, "Unknown");

        Assert.Equal(6, cells.Count);
        Assert.Equal("header", (string)Assert.IsType<JsonObject>(cells[0])["type"]!);
        Assert.Equal("status", (string)Assert.IsType<JsonObject>(cells[1])["type"]!);

        var runnable = Assert.IsType<JsonObject>(cells[2]);
        Assert.Equal("panel", (string)runnable["type"]!);
        Assert.Equal("cpu_scheduler_stats", (string)runnable["source"]!);
        Assert.Equal("scheduler_runnable_tasks", (string)runnable["measure"]!);

        var scheduler = Assert.IsType<JsonObject>(cells[3]);
        Assert.Equal("read", (string)scheduler["type"]!);
        Assert.Equal("get_cpu_scheduler_pressure", (string)scheduler["read"]!);

        var cpu = Assert.IsType<JsonObject>(cells[4]);
        Assert.Equal("cpu_utilization_stats", (string)cpu["source"]!);
        Assert.Equal("sqlserver_cpu_utilization", (string)cpu["measure"]!);

        var topQueries = Assert.IsType<JsonObject>(cells[5]);
        Assert.Equal("get_top_queries_by_cpu", (string)topQueries["read"]!);

        foreach (var cell in cells)
        {
            Assert.NotEqual("pg_cpu_utilization", (string?)cell!["source"]);
        }
    }

    [Fact]
    public void BuildRunnableQueueCells_ValidatesAsANotebookDefinition()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Runnable Queue");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells!(
            "Runnable Queue", "SRV1", AsOf, WindowStart, WindowEnd, null, null,
            "Unknown (not collected since " + AsOf + ")");

        var definition = new JsonObject { ["kind"] = "notebook", ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);
        Assert.True(validation.IsValid, validation.Error);
    }
}
