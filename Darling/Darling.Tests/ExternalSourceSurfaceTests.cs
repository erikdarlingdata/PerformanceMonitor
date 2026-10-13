/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Text.Json.Nodes;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Notifications;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5654: the source of an external alert on the web views. The persisted context carries it, the triage page and the
/// notebook read it back as <c>external_source</c> (the key <c>get_alert_history</c> serves), and the alerts page shows it
/// in a column that appears only while a fetched row has one. Every built-in alert reads null.
/// </summary>
[Trait("Reads", "Lite")]
public sealed class ExternalSourceSurfaceTests
{
    private const string AlertsJsPath = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/pages/alerts.js";

    private static DarlingAlertReader.AlertHistoryReadRow Row(string? contextJson) =>
        new(new DateTime(2026, 10, 11, 15, 48, 49, DateTimeKind.Utc), 1, "SRV1", "External:etl:nightly", 1, 0, true, "email",
            null, false, null, false, contextJson);

    private static string ContextWithSource(string source) =>
        AlertContextSerializer.Serialize(new AlertContext { External = new AlertExternalOrigin(source, "nightly", 42) });

    [Fact]
    public void TheTriageAndNotebookRows_CarryTheSameExternalSource_ReadFromThePersistedContext()
    {
        var json = ContextWithSource("etl");

        var triage = DarlingTriageEndpoint.AlertRowNode(Row(json));
        var notebook = AlertNotebookEndpoint.AlertRowNode(Row(json), incident: null);

        Assert.Equal("etl", (string?)triage["external_source"]);
        Assert.Equal("etl", (string?)notebook["external_source"]);
    }

    [Fact]
    public void TheWebRows_HaveANullExternalSource_ForABuiltInAlert_AndForNoContext()
    {
        var builtIn = AlertContextSerializer.Serialize(new AlertContext());
        foreach (var json in new string?[] { null, string.Empty, "not json", "{}", builtIn })
        {
            var triage = DarlingTriageEndpoint.AlertRowNode(Row(json));
            var notebook = AlertNotebookEndpoint.AlertRowNode(Row(json), incident: null);

            /* The key is present (the shape is stable) and its value is null. */
            Assert.True(triage.ContainsKey("external_source"));
            Assert.True(notebook.ContainsKey("external_source"));
            Assert.Null(triage["external_source"]);
            Assert.Null(notebook["external_source"]);
        }
    }

    [Fact]
    public void TheAlertsPage_HasAnExternalSourceColumn_ShownOnlyWhileAFetchedRowHasOne()
    {
        var js = ReadRepoFile(AlertsJsPath);

        Assert.Contains("{ key: \"external_source\", label: \"External source\" }", js, StringComparison.Ordinal);

        /* The column is added to the list only when showExternal is set... */
        var columns = js.IndexOf("function alertColumns()", StringComparison.Ordinal);
        Assert.True(columns >= 0);
        var body = js[columns..js.IndexOf("\n}", columns, StringComparison.Ordinal)];
        Assert.Contains("showExternal", body, StringComparison.Ordinal);
        Assert.Contains("EXTERNAL_COLUMN", body, StringComparison.Ordinal);

        /* ...and drawAlerts sets it from the fetched rows and rebuilds the table (the keyed refresh leaves the header alone). */
        var draw = js.IndexOf("function drawAlerts(", StringComparison.Ordinal);
        Assert.True(draw >= 0);
        var drawBody = js[draw..];
        var set = drawBody.IndexOf("state.alerts.some((a) => a.external_source)", StringComparison.Ordinal);
        var reconcile = drawBody.IndexOf("reconcileRows(state.tbody", StringComparison.Ordinal);
        Assert.True(set >= 0 && reconcile > set, "showExternal must be decided from the rows before the keyed refresh runs");
        Assert.Contains("state.tbody = null;", drawBody[set..reconcile], StringComparison.Ordinal);
    }
}
