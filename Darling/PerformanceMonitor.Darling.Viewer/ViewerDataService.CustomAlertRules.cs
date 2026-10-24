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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's custom alert rule management (#6390, S4). Every read and write goes through the Storage types the web
/// dashboard and the MCP tools use: <see cref="CustomAlertRuleStore"/> for the rows (its optimistic version, enabled-rule
/// cap and duplicate-name refusals are the store's own, and the Viewer shows their text unchanged),
/// <see cref="CustomAlertTeardown.ResolveAndDeleteRuleAsync(NpgsqlDataSource, long, Microsoft.Extensions.Logging.ILogger?, int, int, CancellationToken)"/>
/// for Delete (so a deleted rule leaves no open incident), and <see cref="CustomAlertRuleDefinition.TryParse"/> for Validate.
/// Nothing here copies the store's SQL.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// The write gate for custom alert rules: true only when the store login ITSELF holds INSERT, UPDATE and DELETE on the
    /// rules table (the privilege the store's writes need), not whatever <see cref="IsReadOnly"/> says about mute rules and
    /// alert dismissal. A login that can mute alerts but was never granted this table cannot save a rule, so its buttons are
    /// off and say why instead of failing on click.
    /// </summary>
    public const string CustomAlertRulesWriteProbeSql =
        "SELECT has_table_privilege('custom_alert_rules', 'INSERT') " +
        "AND has_table_privilege('custom_alert_rules', 'UPDATE') " +
        "AND has_table_privilege('custom_alert_rules', 'DELETE')";

    /// <summary>What the write buttons say when the gate is off.</summary>
    public const string CustomAlertRulesReadOnlyMessage =
        "This connection cannot change custom alert rules: the store login has no INSERT, UPDATE and DELETE on custom_alert_rules. " +
        "Rules can be viewed but not changed here. Connect as a login that holds that grant (postgres.connectAs = \"admin\") and restart the viewer.";

    /// <summary>The name a write stamps into <c>updated_by</c>: the Windows user running the viewer, as every other viewer write does.</summary>
    public static string CustomAlertRuleUpdatedBy() => Environment.UserName;

    private CustomAlertRuleStore CustomAlertRules() =>
        new(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds);

    /// <summary>Probes <see cref="CustomAlertRulesWriteProbeSql"/>. A probe that cannot answer reads as false, so the buttons are off
    /// rather than offered and refused.</summary>
    public async Task<bool> CanWriteCustomAlertRulesAsync(CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = _dataSource.CreateCommand(CustomAlertRulesWriteProbeSql);
            command.CommandTimeout = ViewerCommandDeadlines.ConnectGateSeconds;
            return await command.ExecuteScalarAsync(cancellationToken) is true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return false;
        }
    }

    public Task<IReadOnlyList<CustomAlertRuleSummary>> ListCustomAlertRulesAsync(CancellationToken cancellationToken = default) =>
        CustomAlertRules().ListAsync(cancellationToken);

    public Task<CustomAlertRuleResult> GetCustomAlertRuleAsync(long id, CancellationToken cancellationToken = default) =>
        CustomAlertRules().GetAsync(id, cancellationToken);

    /// <summary>How many servers each rule is firing on right now, from the same state rows the evaluator keeps. A rule absent from the map is quiet.</summary>
    public async Task<IReadOnlyDictionary<long, int>> GetCustomAlertFiringCountsAsync(CancellationToken cancellationToken = default)
    {
        var rows = await new CustomAlertStateStore(_dataSource, ViewerCommandDeadlines.CurrentInteractiveReadSeconds)
            .ListAllWithRuleAsync(cancellationToken);
        return rows.Where(r => r.Firing).GroupBy(r => r.RuleId).ToDictionary(g => g.Key, g => g.Count());
    }

    public Task<CustomAlertRuleResult> CreateCustomAlertRuleAsync(
        string name, string? description, string definitionJson, bool enabled, DateTime? expiresAtUtc,
        CancellationToken cancellationToken = default) =>
        TranslateRulePrivilegeAsync(() => CustomAlertRules().CreateAsync(
            name, description, definitionJson, enabled, CustomAlertRuleUpdatedBy(), expiresAtUtc, cancellationToken));

    /// <summary>A save under the version the caller loaded; a stale version comes back as the store's own <see cref="CustomAlertRuleResult.Conflict"/> text.</summary>
    public Task<CustomAlertRuleResult> UpdateCustomAlertRuleAsync(
        long id, string name, string? description, string definitionJson, bool enabled, int expectedVersion, DateTime? expiresAtUtc,
        CancellationToken cancellationToken = default) =>
        TranslateRulePrivilegeAsync(() => CustomAlertRules().UpdateAsync(
            id, name, description, definitionJson, enabled, expectedVersion, CustomAlertRuleUpdatedBy(), expiresAtUtc, cancellationToken));

    /// <summary>Pause or resume: the rule is read in full and saved back with only <c>enabled</c> changed, under the version the list showed.</summary>
    public async Task<CustomAlertRuleResult> SetCustomAlertRuleEnabledAsync(
        long id, bool enabled, int expectedVersion, CancellationToken cancellationToken = default)
    {
        var current = await GetCustomAlertRuleAsync(id, cancellationToken);
        if (current is not CustomAlertRuleResult.Ok { Rule: { } rule })
        {
            return current;
        }

        return await UpdateCustomAlertRuleAsync(
            id, rule.Name, rule.Description, rule.DefinitionJson, enabled, expectedVersion, rule.ExpiresAtUtc, cancellationToken);
    }

    /// <summary>Deletes through the teardown the web and MCP deletes use: any open incident gets its recovery row first.</summary>
    public Task<CustomAlertRuleResult> DeleteCustomAlertRuleAsync(long id, CancellationToken cancellationToken = default) =>
        TranslateRulePrivilegeAsync(() => CustomAlertTeardown.ResolveAndDeleteRuleAsync(
            _dataSource, id, null,
            ViewerCommandDeadlines.CurrentInteractiveReadSeconds, ViewerCommandDeadlines.CurrentInteractiveReadSeconds, cancellationToken));

    private static async Task<CustomAlertRuleResult> TranslateRulePrivilegeAsync(Func<Task<CustomAlertRuleResult>> write)
    {
        try
        {
            return await write();
        }
        catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
        {
            throw new ViewerReadOnlyException(ex);
        }
    }

    /// <summary>The text a refused store result carries, or null for Ok. NotFound reads as the rule having been deleted elsewhere.</summary>
    public static string? CustomAlertRuleRefusal(CustomAlertRuleResult result) => result switch
    {
        CustomAlertRuleResult.Conflict c => c.Message,
        CustomAlertRuleResult.Invalid i => i.Message,
        CustomAlertRuleResult.NotFound => "That rule no longer exists; it was deleted elsewhere. Refresh the list.",
        _ => null,
    };
}
