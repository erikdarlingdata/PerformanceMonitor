/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.Linq;
using System.Text.RegularExpressions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The desktop viewer's per-server AWS role fields (#5452). The writes name <c>aws_role_arn</c> and <c>aws_external_id</c> and
/// never the generated <c>aws_external_id_set</c>; no read selects the external ID, only whether one is set, and an edit
/// keeps the stored ID unless a new one is typed or the clear box is ticked; and every read's column count agrees with the
/// ordinals its reader uses, which a live test would only catch on a store with the V168 columns.
/// </summary>
public sealed class ViewerAwsRoleFieldsTests
{
    private static readonly Regex ExternalIdColumn = new(@"\baws_external_id\b", RegexOptions.CultureInvariant);

    /// <summary>The comma-separated column list of a SELECT, between <c>SELECT</c> and <c>FROM</c>.</summary>
    private static string[] SelectedColumns(string sql)
    {
        var select = sql.IndexOf("SELECT", StringComparison.Ordinal) + "SELECT".Length;
        var from = sql.IndexOf("FROM config_monitored_servers", StringComparison.Ordinal);
        return sql[select..from].Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
    }

    private static int HighestOrdinal(string source, string methodName)
    {
        var start = source.IndexOf(methodName + "(NpgsqlDataReader reader)", StringComparison.Ordinal);
        Assert.True(start >= 0, methodName);
        var end = source.IndexOf("};", start, StringComparison.Ordinal);
        return Regex.Matches(source[start..end], @"reader\.\w+(?:<[^>]+>)?\((\d+)\)")
            .Select(m => int.Parse(m.Groups[1].Value, CultureInfo.InvariantCulture)).Max();
    }

    [Fact]
    public void TheWrites_NameTheRoleAndTheExternalId_AndNeverTheGeneratedFlag()
    {
        foreach (var sql in new[] { ViewerDataService.MonitoredServerUpsertSql, ViewerDataService.MonitoredServerInsertIfAbsentSql })
        {
            Assert.Contains("aws_role_arn", sql, StringComparison.Ordinal);
            Assert.Matches(ExternalIdColumn, sql);
            Assert.DoesNotContain("aws_external_id_set", sql, StringComparison.Ordinal);

            /* One value per column: the column list and the VALUES list agree (the two server-side timestamps are columns too). */
            var columnsStart = sql.IndexOf('(', StringComparison.Ordinal) + 1;
            var columns = sql[columnsStart..sql.IndexOf(')', StringComparison.Ordinal)].Split(',').Length;
            var valuesStart = sql.IndexOf("VALUES (", StringComparison.Ordinal) + "VALUES (".Length;
            var values = sql[valuesStart..sql.IndexOf("ON CONFLICT", StringComparison.Ordinal)];
            Assert.Equal(columns, Regex.Matches(values, @"\$\d+|\(now\(\) AT TIME ZONE 'UTC'\)").Count);
            Assert.Contains("$20", sql, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("$21", ViewerDataService.MonitoredServerInsertIfAbsentSql, StringComparison.Ordinal);
        Assert.Contains("aws_role_arn = EXCLUDED.aws_role_arn", ViewerDataService.MonitoredServerUpsertSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheUpsert_KeepsTheStoredExternalId_UnlessOneWasTypedOrTheBoxWasCleared()
    {
        var sql = ViewerDataService.MonitoredServerUpsertSql;
        Assert.DoesNotContain("aws_external_id = EXCLUDED.aws_external_id,", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN $21 THEN EXCLUDED.aws_external_id", sql, StringComparison.Ordinal);
        Assert.Contains("ELSE config_monitored_servers.aws_external_id END", sql, StringComparison.Ordinal);
        Assert.Contains("WHEN EXCLUDED.aws_role_arn IS NULL THEN NULL", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void NoRead_SelectsTheExternalId_EachCarriesTheRoleAndTheFlag()
    {
        foreach (var sql in new[]
        {
            ViewerDataService.MonitoredServerByIdSql,
            ViewerDataService.MonitoredServersSelectSql,
            ViewerDataService.MonitoredServerByIdNoSecretSql,
            ViewerDataService.MonitoredServerByAddressSql,
        })
        {
            Assert.DoesNotMatch(ExternalIdColumn, sql);
            Assert.Contains("aws_role_arn", sql, StringComparison.Ordinal);
            Assert.Contains("aws_external_id_set", sql, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void EveryRead_HasAsManyColumnsAsItsReaderUses()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "ViewerDataService.MonitoredServers.cs");

        Assert.Equal(HighestOrdinal(source, "ReadMonitoredServerRow") + 1, SelectedColumns(ViewerDataService.MonitoredServerByIdSql).Length);

        var noSecret = HighestOrdinal(source, "ReadMonitoredServerRowNoSecret") + 1;
        Assert.Equal(noSecret, SelectedColumns(ViewerDataService.MonitoredServersSelectSql).Length);
        Assert.Equal(noSecret, SelectedColumns(ViewerDataService.MonitoredServerByIdNoSecretSql).Length);
        Assert.Equal(noSecret, SelectedColumns(ViewerDataService.MonitoredServerByAddressSql).Length);
    }

    [Fact]
    public void TheRowDefaults_AreNoRole_SoEveryExistingWriterIsUnchanged()
    {
        var row = new MonitoredServerRow();
        Assert.Null(row.AwsRoleArn);
        Assert.Null(row.AwsExternalId);
        Assert.False(row.AwsExternalIdSent);
        Assert.False(row.AwsExternalIdSet);
    }

    [Fact]
    public void TheBulkDialog_TakesNoRole()
    {
        var source = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddMultipleServersDialog.xaml.cs");
        Assert.DoesNotContain("Aws", source, StringComparison.Ordinal);
    }

    private const string Role = "arn:aws:iam::123456789012:role/darling-monitor";
    private const string OtherRole = "arn:aws:iam::123456789012:role/darling-other";

    [Fact]
    public void Resolve_BlankBoxes_AreNotSet_AndOnAnEditClearTheStoredRole()
    {
        Assert.Equal((null, null, false, null), AddServerDialog.ResolveAwsRole("  ", "", false, null, false));
        Assert.Equal((null, null, false, null), AddServerDialog.ResolveAwsRole("", "", false, Role, false));
    }

    [Fact]
    public void Resolve_TheSharedChecksApply_WithTheirFixedSentences()
    {
        Assert.Equal(AwsRoleSettings.InvalidRoleMessage, AddServerDialog.ResolveAwsRole("not-an-arn", "", false, null, false).Error);
        Assert.Equal(AwsRoleSettings.InvalidExternalIdMessage, AddServerDialog.ResolveAwsRole(Role, "has space", false, null, false).Error);
        Assert.Equal(AwsRoleSettings.ExternalIdNeedsRoleMessage, AddServerDialog.ResolveAwsRole("", "ext-1234", false, null, false).Error);
        Assert.Equal((Role, "ext-1234", true, null), AddServerDialog.ResolveAwsRole($" {Role} ", " ext-1234 ", false, null, false));
    }

    [Fact]
    public void Resolve_AnEditWithTheBoxUntouched_KeepsTheStoredExternalId()
    {
        /* Nothing typed and the clear box not ticked: nothing is sent, so the upsert keeps the stored ID. */
        Assert.Equal((Role, null, false, null), AddServerDialog.ResolveAwsRole(Role, "", false, Role, true));
        Assert.Equal((Role, null, false, null), AddServerDialog.ResolveAwsRole(Role, "   ", false, Role, true));
    }

    [Fact]
    public void Resolve_ATypedExternalId_Replaces_AndTheClearBoxRemoves()
    {
        Assert.Equal((Role, "ext-5678", true, null), AddServerDialog.ResolveAwsRole(Role, "ext-5678", false, Role, true));
        Assert.Equal((Role, null, true, null), AddServerDialog.ResolveAwsRole(Role, "", true, Role, true));
        Assert.Equal(AddServerDialog.AwsExternalIdTypedAndClearedMessage, AddServerDialog.ResolveAwsRole(Role, "ext-5678", true, Role, true).Error);
    }

    [Fact]
    public void Resolve_ANewRole_NeedsTheStoredExternalIdChangedOrCleared()
    {
        /* The box is untouched and one is stored: the store's trigger would refuse it (PW004), so the dialog says so first. */
        Assert.Equal(AwsRoleSettings.RoleChangeNeedsExternalIdMessage,
            AddServerDialog.ResolveAwsRole(OtherRole, "", false, Role, true).Error);

        /* A new ID, or a cleared one, goes through; an unchanged role never needs it. */
        Assert.Null(AddServerDialog.ResolveAwsRole(OtherRole, "ext-5678", false, Role, true).Error);
        Assert.Null(AddServerDialog.ResolveAwsRole(OtherRole, "", true, Role, true).Error);
        Assert.Null(AddServerDialog.ResolveAwsRole(Role, "", false, Role, true).Error);

        /* A target with no stored ID takes a new role freely. */
        Assert.Null(AddServerDialog.ResolveAwsRole(OtherRole, "", false, Role, false).Error);
    }

    [Fact]
    public void TheDialog_ShowsWhetherAnExternalIdIsSet_AndOffersAClearBox_AndNeverFillsTheBox()
    {
        var xaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml");
        Assert.Contains("x:Name=\"AwsClearExternalIdBox\" Content=\"Clear external ID\"", xaml, StringComparison.Ordinal);

        var code = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs");
        Assert.Contains("AwsExternalIdState.Text = existing.AwsExternalIdSet ? AwsExternalIdSetNote : AwsExternalIdNoneNote;", code, StringComparison.Ordinal);
        Assert.DoesNotContain("AwsExternalIdBox.Password =", code, StringComparison.Ordinal);
        Assert.Contains("Leave the box blank to keep it", AddServerDialog.AwsExternalIdSetNote, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSaveNote_SaysTheServiceUsesTheRoleOnlyWhenItIsAllowed()
    {
        Assert.Equal(
            "The service uses this AWS role only if it is listed in allowedAwsRoles in darling.json, or a server in darling.json uses it. "
            + "A role saved here does not take effect until then.",
            AddServerDialog.AwsRoleSaveNote);
    }

    [Fact]
    public void TheDialog_PutsTheBoxesInThePostgresPanel_AndBindsTheNoteFromTheConstant()
    {
        var xaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml");
        var panel = xaml.IndexOf("x:Name=\"PostgresOptionsPanel\"", StringComparison.Ordinal);
        var panelEnd = xaml.IndexOf("<!-- Authentication.", panel, StringComparison.Ordinal);
        Assert.True(panel > 0 && panelEnd > panel);

        foreach (var name in new[] { "AwsRoleBox", "AwsExternalIdBox", "AwsRoleNote" })
        {
            var at = xaml.IndexOf($"x:Name=\"{name}\"", StringComparison.Ordinal);
            Assert.True(at > panel && at < panelEnd, $"{name} must sit inside the PostgreSQL options panel");
        }

        Assert.Contains("AwsRoleNote.Text = AwsRoleSaveNote;",
            RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheStoreRefusalSqlState_IsThePW004TheStoreRaises()
    {
        Assert.Equal("PW004", ViewerDataService.StoreAwsRoleNeedsExternalIdSqlState);
        Assert.Contains("USING ERRCODE = 'PW004'", DarlingManagedRoles.BuildServerPasswordRulesSql("config"), StringComparison.Ordinal);
    }
}
