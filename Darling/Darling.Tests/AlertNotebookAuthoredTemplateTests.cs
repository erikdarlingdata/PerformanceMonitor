/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins for #4222 slice b — the authored <c>authored/blocking</c> and <c>authored/deadlocks</c> alert-notebook
/// templates (<see cref="AlertNotebookEndpoint.AuthoredTemplate"/> / <c>BuildBlockingCells</c> /
/// <c>BuildDeadlockCells</c>). Ported from a throwaway reflection harness (the opening PR's own gap) into real
/// <c>[Fact]</c>/<c>[Theory]</c> methods, calling the endpoint's <c>internal</c> members directly —
/// <c>InternalsVisibleTo="Darling.Tests"</c> already grants this project that visibility, so no reflection is
/// needed once the members themselves are <c>internal</c> rather than <c>private</c> (this PR widens
/// <c>AuthoredTemplate</c> and <c>AuthoredTemplateEntry</c> to <c>internal</c> for exactly this reason).
///
/// <para>The compose-catalog check (measures/breakdowns exist) is NEW relative to the throwaway harness — it
/// runs every composed cell (<c>type: "panel"</c>) both templates emit through
/// <see cref="ComposeSpec.TryParsePanel"/>, the same authority a saved dashboard/notebook panel is checked
/// against, so a renamed measure or dimension reds this test exactly like it would red a real save.</para>
/// </summary>
public sealed class AlertNotebookAuthoredTemplateTests
{
    private static readonly DateTime WindowEnd = new(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc);
    private static readonly DateTime WindowStart = WindowEnd - TimeSpan.FromHours(24);
    private const string AsOf = "2026-01-01T12:00:00Z";

    private static AlertIncident IncidentWithDatabase(string database) =>
        new("dedup-key", new[] { "obj1" }, Database: database);

    /* ═══════════════════════════ metric routing ═══════════════════════════ */

    /// <summary>Data-driven off <see cref="AlertNotebookEndpoint.s_authoredTemplates"/> itself: every
    /// registered metric must route to its OWN row's <c>Entry.Id</c>, so a new family's row is covered here
    /// without an added <c>InlineData</c> line.</summary>
    public static IEnumerable<object[]> AllAuthoredMetricsWithExpectedId() =>
        AlertNotebookEndpoint.s_authoredTemplates
            .SelectMany(row => row.Metrics.Select(metric => new object[] { metric, row.Entry.Id }));

    [Theory]
    [MemberData(nameof(AllAuthoredMetricsWithExpectedId))]
    public void AuthoredTemplate_RoutesEveryRegisteredMetric_ToItsOwnTemplate(string metric, string expectedId)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);

        Assert.NotNull(template);
        Assert.Equal(expectedId, template!.Value.Id);
    }

    [Fact]
    public void AuthoredTemplate_NonAuthoredMetric_StaysMechanical()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Poison Wait");

        Assert.Null(template);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void AuthoredTemplate_NullOrBlankMetric_StaysMechanical(string? metric)
    {
        Assert.Null(AlertNotebookEndpoint.AuthoredTemplate(metric));
    }

    /* ═══════════════════════════ both templates validate ═══════════════════════════ */

    /// <summary>Every metric <see cref="AlertNotebookEndpoint.s_authoredTemplates"/> registers, flattened
    /// from its rows so the shared theories below cover a new authored family (#4223) the moment its row
    /// lands here — nothing in this file has to change to pick it up.</summary>
    public static IEnumerable<object[]> AllAuthoredMetrics() =>
        AlertNotebookEndpoint.s_authoredTemplates
            .SelectMany(row => row.Metrics)
            .Select(metric => new object[] { metric });

    [Theory]
    [MemberData(nameof(AllAuthoredMetrics))]
    public void AuthoredTemplate_BuiltAgainstAMatchedIncident_PassesValidateNotebookDefinition(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var incident = IncidentWithDatabase("SalesDb");
        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, incident, null, "Unknown (not collected since " + AsOf + ")");

        var definition = new JsonObject
        {
            ["kind"] = "notebook",
            ["cells"] = cells,
        };

        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);

        Assert.True(validation.IsValid, validation.Error);
    }

    [Theory]
    [MemberData(nameof(AllAuthoredMetrics))]
    public void AuthoredTemplate_WithNoMatchedIncident_StillDegradesAndValidates(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, null, null, "Unknown (not collected since " + AsOf + ")");

        var withIncident = template.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        // Degrade: same cell count with or without a matched incident -- only the bound database filter differs.
        Assert.Equal(withIncident.Count, cells.Count);

        var definition = new JsonObject { ["kind"] = "notebook", ["cells"] = cells };
        var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);

        Assert.True(validation.IsValid, validation.Error);
    }

    /* ═══════════════════════════ every read cell (mechanical AND authored) names a dispatch entry ═══════════════════════════ */

    /// <summary>The gap the opening PR's report flagged explicitly: <c>EveryMechanicalReadCell_NamesAKnownDispatchEntry</c>
    /// only walks <see cref="DarlingTriageEndpoint.SectionsFor"/>, which the authored templates bypass entirely,
    /// so an authored read cell naming a read with no dispatch entry was invisible. This walks BOTH authored
    /// templates' actual built cells instead of the section table.</summary>
    [Theory]
    [MemberData(nameof(AllAuthoredMetrics))]
    public void EveryAuthoredReadCell_NamesAKnownDispatchEntry(string metric)
    {
        var dispatch = DarlingWebEndpoints.BuildReadDispatch();
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        var offenders = cells
            .OfType<JsonObject>()
            .Where(cell => (string?)cell["type"] == "read")
            .Select(cell => (string)cell["read"]!)
            .Where(read => !dispatch.ContainsKey(read))
            .ToArray();

        Assert.True(offenders.Length == 0,
            "these authored read cells name a read with no BuildReadDispatch entry: " + string.Join(", ", offenders));
    }

    /* ═══════════════════════════ composed cells' measures/breakdowns exist in the compose catalog ═══════════════════════════ */

    /// <summary>NEW relative to the throwaway harness (per the brief): every composed (<c>panel</c>) cell in
    /// both templates must name a measure/source/groupBy dimension that actually exists in
    /// <see cref="ComposeSpec"/>'s measure catalog. Runs the SAME <see cref="ComposeSpec.TryParsePanel"/>
    /// authority a saved dashboard/notebook panel is checked against, so a renamed measure or dimension reds
    /// this test the same way it would red a real save -- exactly the gap the brief calls out as missing from
    /// the harness.</summary>
    [Theory]
    [MemberData(nameof(AllAuthoredMetrics))]
    public void EveryComposedCell_NamesAMeasureAndBreakdownInTheComposeCatalog(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        var panelCells = cells.OfType<JsonObject>().Where(cell => (string?)cell["type"] == "panel").ToArray();
        Assert.NotEmpty(panelCells);

        foreach (var panel in panelCells)
        {
            var (plan, error) = ComposeSpec.TryParsePanel(panel, declaredVariables: Array.Empty<string>());
            Assert.True(error is null, $"panel '{panel["title"]}' failed catalog validation: {error}");
            Assert.NotNull(plan);
        }
    }

    /// <summary>A renamed/removed measure must red this test — proving the check above actually exercises the
    /// catalog rather than trivially passing. Simulates the "renamed measure" failure mode the brief calls
    /// out by name.</summary>
    [Fact]
    public void ComposeCatalogCheck_RedsOnARenamedMeasure()
    {
        var badPanel = new JsonObject
        {
            ["type"] = "panel",
            ["title"] = "Bogus",
            ["source"] = "blocked_process_reports",
            ["measure"] = "this_measure_does_not_exist",
            ["aggregate"] = "count",
            ["viz"] = "line",
        };

        var (_, error) = ComposeSpec.TryParsePanel(badPanel, declaredVariables: Array.Empty<string>());

        Assert.NotNull(error);
        Assert.Contains("unknown measure", error, StringComparison.OrdinalIgnoreCase);
    }

    /* ═══════════════════════════ budget ═══════════════════════════ */

    /// <summary>Every read cell has an explicit <c>limit</c>, except <c>get_deadlock_trend</c> (a bucketed
    /// trend read, exempted by name); no read names <c>audit_config</c>/an <c>analyze_*</c> compute read; every
    /// composed cell's window is <![CDATA[<=]]> 24h.</summary>
    [Theory]
    [MemberData(nameof(AllAuthoredMetrics))]
    public void Budget_ReadCellsHaveLimitsExceptTheTrendRead_ComposedWindowsAreAtMost24h(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        foreach (var cellNode in cells)
        {
            var cell = Assert.IsType<JsonObject>(cellNode);
            var type = (string?)cell["type"];

            if (type == "read")
            {
                var read = (string)cell["read"]!;
                Assert.False(read == "audit_config" || read.StartsWith("analyze_", StringComparison.Ordinal),
                    $"authored read cell names a barred read: {read}");

                var parameters = Assert.IsType<JsonObject>(cell["params"]);
                if (read == "get_deadlock_trend")
                {
                    Assert.False(parameters.ContainsKey("limit"), "get_deadlock_trend must not carry a limit param");
                }
                else if (read == "get_top_queries_by_cpu" || read == "get_top_procedures_by_cpu")
                {
                    /* #4223: these two declare no 'limit' param -- they cap with 'top' instead. */
                    Assert.True(parameters.ContainsKey("top"), $"read cell '{read}' must carry a top param");
                }
                else if (read == "get_cpu_scheduler_pressure")
                {
                    /* #4223: a newest-snapshot read (DarlingWebEndpoints' own catalog entry) -- no row cap to carry. */
                    Assert.False(parameters.ContainsKey("limit"), "get_cpu_scheduler_pressure must not carry a limit param");
                }
                else
                {
                    Assert.True(parameters.ContainsKey("limit"), $"read cell '{read}' must carry a limit param");
                }
            }
            else if (type == "panel")
            {
                var range = Assert.IsType<JsonObject>(cell["range"]);
                var start = DateTime.Parse((string)range["windowStart"]!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
                var end = DateTime.Parse((string)range["windowEnd"]!, CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal | DateTimeStyles.AssumeUniversal);
                Assert.True(end - start <= TimeSpan.FromHours(24), $"panel '{cell["title"]}' window exceeds 24h");
            }
        }
    }

    /* ═══════════════════════════ binding ═══════════════════════════ */

    [Theory]
    [MemberData(nameof(AllAuthoredMetrics))]
    public void Binding_ComposedCellRangeEqualsWindow_ReadCellAsOfEqualsWindowEnd(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        foreach (var cellNode in cells)
        {
            var cell = Assert.IsType<JsonObject>(cellNode);
            var type = (string?)cell["type"];

            if (type == "panel")
            {
                var range = Assert.IsType<JsonObject>(cell["range"]);
                Assert.Equal(WindowStart.ToString("o", CultureInfo.InvariantCulture), (string)range["windowStart"]!);
                Assert.Equal(WindowEnd.ToString("o", CultureInfo.InvariantCulture), (string)range["windowEnd"]!);
            }
            else if (type == "read")
            {
                var parameters = Assert.IsType<JsonObject>(cell["params"]);
                Assert.Equal(AsOf, (string)parameters["as_of"]!);
            }
        }
    }

    /* ═══════════════════════════ explicit viz ═══════════════════════════ */

    [Theory]
    [MemberData(nameof(AllAuthoredMetrics))]
    public void EveryReadOrPanelCell_HasAnExplicitViz(string metric)
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate(metric);
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            metric, "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        foreach (var cellNode in cells)
        {
            var cell = Assert.IsType<JsonObject>(cellNode);
            var type = (string?)cell["type"];
            if (type is "read" or "panel")
            {
                var viz = (string?)cell["viz"];
                Assert.False(string.IsNullOrEmpty(viz), $"cell '{cell["title"]}' ({type}) has no explicit viz");
            }
        }
    }

    /* ═══════════════════════════ the by-database breakdown never binds a database filter ═══════════════════════════ */

    [Fact]
    public void BlockingByDatabaseBreakdown_NeverBindsADatabaseFilter_EvenWithAMatchedIncident()
    {
        var template = AlertNotebookEndpoint.AuthoredTemplate("Blocking Detected");
        Assert.NotNull(template);

        var cells = template!.Value.BuildCells(
            "Blocking Detected", "SRV1", AsOf, WindowStart, WindowEnd, IncidentWithDatabase("SalesDb"), null, "Unknown");

        var byDatabase = cells.OfType<JsonObject>().Single(c => (string?)c["title"] == "Blocking by database");

        Assert.Null(byDatabase["filters"]);
    }

    /* ═══════════════════════════ the registration table (#4223) ═══════════════════════════ */

    /// <summary>Every family in <see cref="AlertNotebookEndpoint.s_authoredTemplates"/> resolves to a
    /// non-null entry for each of its own metric names, the ids are unique, and every entry's built cells
    /// still pass <see cref="DarlingWebEndpoints.ValidateNotebookDefinition"/> against a fixed alert — the
    /// pin the split into one-file-per-family (#4223) was ruled to require, so a future family can be added
    /// to the table without anyone re-checking these by hand.</summary>
    [Fact]
    public void RegistrationTable_EveryFamilyResolvesAndValidates()
    {
        var table = AlertNotebookEndpoint.s_authoredTemplates;
        Assert.NotEmpty(table);

        var incident = IncidentWithDatabase("SalesDb");
        var seenIds = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (metrics, entry) in table)
        {
            Assert.NotEmpty(metrics);
            Assert.True(seenIds.Add(entry.Id), $"duplicate template id: {entry.Id}");

            foreach (var metric in metrics)
            {
                var resolved = AlertNotebookEndpoint.AuthoredTemplate(metric);
                Assert.NotNull(resolved);
                Assert.Equal(entry.Id, resolved!.Value.Id);

                var cells = resolved.Value.BuildCells(
                    metric, "SRV1", AsOf, WindowStart, WindowEnd, incident, null,
                    "Unknown (not collected since " + AsOf + ")");

                var definition = new JsonObject { ["kind"] = "notebook", ["cells"] = cells };
                var validation = DarlingWebEndpoints.ValidateNotebookDefinition(definition);

                Assert.True(validation.IsValid, validation.Error);
            }
        }
    }

    /// <summary>The table is sorted case-insensitively by each row's first metric name — keeps the table an
    /// easy-to-scan, mergeable list as families are added in parallel (#4223's own stated point).</summary>
    [Fact]
    public void RegistrationTable_IsSortedByFirstMetricName()
    {
        var table = AlertNotebookEndpoint.s_authoredTemplates;
        var firstMetrics = table.Select(row => row.Metrics[0]).ToArray();
        var sorted = firstMetrics.OrderBy(m => m, StringComparer.OrdinalIgnoreCase).ToArray();

        Assert.Equal(sorted, firstMetrics);
    }
}
