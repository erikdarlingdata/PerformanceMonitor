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
/// never the generated <c>aws_external_id_set</c>; the by-id read an admin seat makes is the one read of the external ID,
/// because the read-only roles are denied the column; and every read's column count agrees with the ordinals its reader
/// uses, which a live test would only catch on a store with the V168 columns.
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
            Assert.DoesNotContain("$21", sql, StringComparison.Ordinal);
        }

        Assert.Contains("aws_role_arn = EXCLUDED.aws_role_arn", ViewerDataService.MonitoredServerUpsertSql, StringComparison.Ordinal);
        Assert.Contains("aws_external_id = EXCLUDED.aws_external_id", ViewerDataService.MonitoredServerUpsertSql, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyTheByIdRead_SelectsTheExternalId_TheOthersCarryTheRoleAndTheFlag()
    {
        Assert.Matches(ExternalIdColumn, ViewerDataService.MonitoredServerByIdSql);
        Assert.Contains("aws_external_id_set", ViewerDataService.MonitoredServerByIdSql, StringComparison.Ordinal);

        /* A read-only seat is denied the aws_external_id column: any read it makes must not name it, or it fails with 42501. */
        foreach (var sql in new[]
        {
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
        Assert.Equal((null, null, null), AddServerDialog.ResolveAwsRole("  ", "", null, null, false));
        Assert.Equal((null, null, null), AddServerDialog.ResolveAwsRole("", "", Role, null, false));
    }

    [Fact]
    public void Resolve_TheSharedChecksApply_WithTheirFixedSentences()
    {
        Assert.Equal(AwsRoleSettings.InvalidRoleMessage, AddServerDialog.ResolveAwsRole("not-an-arn", "", null, null, false).Error);
        Assert.Equal(AwsRoleSettings.InvalidExternalIdMessage, AddServerDialog.ResolveAwsRole(Role, "has space", null, null, false).Error);
        Assert.Equal(AwsRoleSettings.ExternalIdNeedsRoleMessage, AddServerDialog.ResolveAwsRole("", "ext-1234", null, null, false).Error);
        Assert.Equal((Role, "ext-1234", null), AddServerDialog.ResolveAwsRole($" {Role} ", " ext-1234 ", null, null, false));
    }

    [Fact]
    public void Resolve_ANewRole_NeedsTheStoredExternalIdChangedOrCleared()
    {
        /* The box still holds the stored ID: the store's trigger would refuse it (PW004), so the dialog says so first. */
        Assert.Equal(AwsRoleSettings.RoleChangeNeedsExternalIdMessage,
            AddServerDialog.ResolveAwsRole(OtherRole, "ext-1234", Role, "ext-1234", true).Error);

        /* A new ID, or a cleared one, goes through; an unchanged role never needs it. */
        Assert.Null(AddServerDialog.ResolveAwsRole(OtherRole, "ext-5678", Role, "ext-1234", true).Error);
        Assert.Null(AddServerDialog.ResolveAwsRole(OtherRole, "", Role, "ext-1234", true).Error);
        Assert.Null(AddServerDialog.ResolveAwsRole(Role, "ext-1234", Role, "ext-1234", true).Error);

        /* A target with no stored ID takes a new role freely. */
        Assert.Null(AddServerDialog.ResolveAwsRole(OtherRole, "", Role, null, false).Error);
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
