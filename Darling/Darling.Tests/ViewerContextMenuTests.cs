/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the chart + grid right-click menus restored from Lite (the last Phase B parity cluster): the composed
/// per-chart menu order (drill-down item + copy/save/export coexisting in ONE menu, so #1409's drill-downs
/// still resolve), the chart-data CSV shape, the "Copy Repro Script" STORED-fields mapping (no live
/// connection), and the read-only-seat server-list SQL (no encrypted_password). Pure logic + SQL pins only —
/// the same no-WPF discipline as the rest of Darling.Tests.
/// </summary>
public sealed class ViewerChartContextMenuTests
{
    private const string DrillLabel = "Show Active Queries at This Time";

    [Fact]
    public void ChartMenuHeaderOrder_DrillDownChart_CarriesBothTheDrillDownAndTheCopyExportItems()
    {
        /* The coexistence pin: a drill-down chart's menu = [drill-down] [separator] [Copy/Save/Export]. This is
           what keeps the B1 (#1409) drill-downs working AFTER the copy/export menu was merged into the same
           ContextMenu instance. */
        var order = ViewerServerTab.ChartMenuHeaderOrder(DrillLabel, includeDataSource: false);

        /* Drill-down is FIRST, then a separator, so the drill-down still shows and resolves. */
        Assert.Equal(DrillLabel, order[0]);
        Assert.Equal(ViewerServerTab.MenuSeparatorMarker, order[1]);

        /* ...AND every copy/save/export item is present in the SAME menu. */
        Assert.Contains(ViewerServerTab.CopyImageHeader, order);
        Assert.Contains(ViewerServerTab.SaveImageHeader, order);
        Assert.Contains(ViewerServerTab.OpenInNewWindowHeader, order);
        Assert.Contains(ViewerServerTab.RevertHeader, order);
        Assert.Contains(ViewerServerTab.ExportDataToCsvHeader, order);
    }

    [Fact]
    public void ChartMenuHeaderOrder_NoDrillDownChart_IsJustTheCopyExportBlock()
    {
        var order = ViewerServerTab.ChartMenuHeaderOrder(drillDownLabel: null, includeDataSource: false);

        /* No drill-down → the menu opens straight into Copy Image (no leading item / separator). */
        Assert.Equal(ViewerServerTab.CopyImageHeader, order[0]);
        Assert.DoesNotContain(DrillLabel, order);
        Assert.Contains(ViewerServerTab.ExportDataToCsvHeader, order);
        Assert.DoesNotContain(ViewerServerTab.ShowDataSourceHeader, order);
    }

    [Fact]
    public void ChartMenuHeaderOrder_WithDataSource_AppendsShowDataSource()
    {
        var order = ViewerServerTab.ChartMenuHeaderOrder(DrillLabel, includeDataSource: true);

        Assert.Equal(ViewerServerTab.ShowDataSourceHeader, order[^1]);
        Assert.Contains(DrillLabel, order);
    }

    [Theory]
    [InlineData(",", "DateTime,Series,Value")]
    [InlineData(";", "DateTime;Series;Value")]
    [InlineData("\t", "DateTime\tSeries\tValue")]
    public void ChartCsvHeaderLine_IsDateTimeSeriesValue_InTheChosenSeparator(string separator, string expected)
    {
        Assert.Equal(expected, ViewerServerTab.ChartCsvHeaderLine(separator));
    }

    [Fact]
    public void FormatChartCsvLine_EmitsInvariantTimestampSeriesAndValue()
    {
        var line = ViewerServerTab.FormatChartCsvLine(new DateTime(2026, 7, 6, 14, 20, 5), "CPU %", 42.5, ",");
        Assert.Equal("2026-07-06 14:20:05,CPU %,42.5", line);
    }

    [Fact]
    public void FormatChartCsvLine_QuotesASeriesNameContainingTheSeparator()
    {
        /* A series label with the separator must be RFC-4180 quoted so the CSV column count stays right. */
        var line = ViewerServerTab.FormatChartCsvLine(new DateTime(2026, 7, 6, 0, 0, 0), "reads, writes", 3, ",");
        Assert.Equal("2026-07-06 00:00:00,\"reads, writes\",3", line);
    }

    [Theory]
    [InlineData("plain", ",", "plain")]
    [InlineData("a,b", ",", "\"a,b\"")]
    [InlineData("a;b", ";", "\"a;b\"")]
    [InlineData("has\"quote", ",", "\"has\"\"quote\"")]
    public void CsvEscape_QuotesOnlyWhenNeeded_AndDoublesEmbeddedQuotes(string value, string separator, string expected)
    {
        Assert.Equal(expected, ViewerServerTab.CsvEscape(value, separator));
    }
}

/// <summary>
/// "Copy Repro Script" builds from STORED row fields with NO live connection (the inventory found the
/// "needs a live connection" rationale OVERSTATED): Active Queries carries its isolation in-row and, like Top
/// Queries / Query Store, takes a best-effort STORED plan the caller resolved (in-row first, else a Postgres
/// fetch by natural key — #4239) as <c>enrichedPlanXml</c>. Pure mapping, so the pin never opens a connection.
/// </summary>
public sealed class ViewerReproScriptTests
{
    private const string Product = "SQL Server Performance Monitor Test";

    [Fact]
    public void ActiveQueriesRow_BuildsFromInRowStoredFields_NoConnection()
    {
        var row = new ViewerQuerySnapshotRow
        {
            QueryText = "SELECT * FROM dbo.Users WHERE Id = 1",
            DatabaseName = "tpcc",
            /* The real value the snapshot collector records (CASE ... WHEN 2 THEN 'Read Committed'), which
               uppercases to the valid T-SQL 'READ COMMITTED' — the repro builder only emits a SET for a
               recognized isolation level. */
            TransactionIsolationLevel = "Read Committed",
            QueryPlan = null,
        };

        var script = ViewerServerTab.BuildReproScriptForRow(row, enrichedPlanXml: null, Product);

        Assert.NotNull(script);
        Assert.Contains("SELECT * FROM dbo.Users WHERE Id = 1", script!, StringComparison.Ordinal);
        Assert.Contains("USE [tpcc];", script, StringComparison.Ordinal);
        Assert.Contains("Source: Active Queries", script, StringComparison.Ordinal);
        Assert.Contains(Product, script, StringComparison.Ordinal);
        /* The stored isolation level rides through to the SET statement as VALID T-SQL. */
        Assert.Contains("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;", script, StringComparison.Ordinal);
    }

    [Fact]
    public void ActiveQueriesRow_UsesTheCallersEnrichedPlan_NotItsOwnPlanProperty()
    {
        /* #4239: BuildReproScriptForRow is a pure mapper — the caller (CopyReproScript_Click) already resolved
           in-row-first / fetch-fallback into enrichedPlanXml before calling here, so the snapshot arm now uses
           enrichedPlanXml like every other row type, regardless of the row's own (now usually-null,
           fetch-on-demand) QueryPlan property. A null row.QueryPlan with a non-null enrichedPlanXml still
           produces a plan-bearing repro, proving the snapshot branch reads the caller's plan, not the row's. */
        var row = new ViewerQuerySnapshotRow { QueryText = "SELECT 1", DatabaseName = "db", QueryPlan = null };

        var script = ViewerServerTab.BuildReproScriptForRow(row, enrichedPlanXml: "<ShowPlanXML/>", Product);

        Assert.NotNull(script);
        Assert.DoesNotContain("plan XML not available", script!, StringComparison.Ordinal);
        Assert.Contains("No parameters found in plan cache", script!, StringComparison.Ordinal);
    }

    [Fact]
    public void TopQueriesRow_BuildsFromStoredFields_WithBestEffortStoredPlan()
    {
        var row = new ViewerQueryStatsRow
        {
            QueryText = "SELECT COUNT(*) FROM dbo.Orders",
            DatabaseName = "sales",
            QueryHash = "0xABCD",
        };

        /* enrichedPlanXml is the STORED plan the caller read from Postgres (a read, not a live exec). Passing
           null still produces a plan-less repro. */
        var script = ViewerServerTab.BuildReproScriptForRow(row, enrichedPlanXml: null, Product);

        Assert.NotNull(script);
        Assert.Contains("SELECT COUNT(*) FROM dbo.Orders", script!, StringComparison.Ordinal);
        Assert.Contains("Source: Top Queries (dm_exec_query_stats)", script, StringComparison.Ordinal);
        Assert.Contains("USE [sales];", script, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreRow_BuildsFromStoredFields()
    {
        var row = new ViewerQueryStoreRow
        {
            QueryText = "UPDATE dbo.Inventory SET Qty = Qty - 1",
            DatabaseName = "wh",
            QueryId = 42,
            PlanId = 7,
        };

        var script = ViewerServerTab.BuildReproScriptForRow(row, enrichedPlanXml: null, Product);

        Assert.NotNull(script);
        Assert.Contains("UPDATE dbo.Inventory SET Qty = Qty - 1", script!, StringComparison.Ordinal);
        Assert.Contains("Source: Query Store", script, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreRegressionsRow_BuildsPlanLessReproFromTheQueryTextSample()
    {
        /* The regression row carries the query-text sample but no plan (it aggregates over plans), so the
           repro is plan-less even though the caller could pass an enriched plan — the branch ignores it. */
        var row = new ViewerQueryStoreRegressionRow
        {
            QueryTextSample = "SELECT SUM(Amount) FROM dbo.Ledger",
            DatabaseName = "fin",
            QueryId = 99,
        };

        var script = ViewerServerTab.BuildReproScriptForRow(row, enrichedPlanXml: "<ShowPlanXML/>", Product);

        Assert.NotNull(script);
        Assert.Contains("SELECT SUM(Amount) FROM dbo.Ledger", script!, StringComparison.Ordinal);
        Assert.Contains("Source: Query Store Regressions", script, StringComparison.Ordinal);
        Assert.Contains("USE [fin];", script, StringComparison.Ordinal);
        Assert.Contains("plan XML not available", script, StringComparison.Ordinal);
    }

    [Fact]
    public void QueryStoreRegressionsRow_WithNoQueryText_ReturnsNull_SoTheHandlerNoOps()
        => Assert.Null(ViewerServerTab.BuildReproScriptForRow(
            new ViewerQueryStoreRegressionRow { QueryTextSample = "", DatabaseName = "db" }, enrichedPlanXml: null, Product));

    [Fact]
    public void RowWithNoQueryText_ReturnsNull_SoTheHandlerNoOps()
    {
        var empty = new ViewerQuerySnapshotRow { QueryText = "", DatabaseName = "db" };
        Assert.Null(ViewerServerTab.BuildReproScriptForRow(empty, enrichedPlanXml: null, Product));
    }

    [Fact]
    public void UnsupportedRowType_ReturnsNull()
    {
        /* Comparison-aggregate rows / config rows / anything else: no repro, the handler no-ops (Lite's
           default branch). */
        Assert.Null(ViewerServerTab.BuildReproScriptForRow(new object(), enrichedPlanXml: null, Product));
    }
}

/// <summary>
/// The read-only-seat fix (#1416 revoked the viewer role's SELECT on <c>encrypted_password</c>): the server
/// LIST read must not reference the secret column, or a read-only <c>connectAs: viewer</c> seat 42501s on the
/// Manage Servers grid + sidebar. The secret stays only in the by-id edit-load read (an admin action).
/// </summary>
public sealed class ViewerServerListReadOnlyTests
{
    [Fact]
    public void MonitoredServersSelectSql_DoesNotSelectTheSecret_SoAReadOnlySeatCanListServers()
    {
        var sql = ViewerDataService.MonitoredServersSelectSql;
        Assert.DoesNotContain("encrypted_password", sql, StringComparison.Ordinal);
        /* Still selects everything the grid displays. */
        Assert.Contains("server_id", sql, StringComparison.Ordinal);
        Assert.Contains("name", sql, StringComparison.Ordinal);
        Assert.Contains("host", sql, StringComparison.Ordinal);
        Assert.Contains("created_at", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void ManagedServersSql_DoesNotSelectTheSecret()
    {
        /* The sidebar's config-authoritative read never carried the secret; pin it stays that way. */
        Assert.DoesNotContain("encrypted_password", ViewerDataService.ManagedServersSql, StringComparison.Ordinal);
    }

    [Fact]
    public void MonitoredServerByIdSql_KeepsTheSecret_ForTheEditDialogPrefill()
    {
        /* Editing (an admin-role action) reloads the row by id and needs the DPAPI blob to keep the password
           when the user doesn't retype it. */
        Assert.Contains("encrypted_password", ViewerDataService.MonitoredServerByIdSql, StringComparison.Ordinal);
    }
}
