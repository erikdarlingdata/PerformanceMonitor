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
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>What the threshold-overrides view-model reads and writes: the store, as a seam a test can stand in for.</summary>
public interface IThresholdOverrideGateway
{
    Task<IReadOnlyList<ThresholdOverrideRow>> ListThresholdOverridesAsync(CancellationToken cancellationToken = default);

    Task SaveThresholdOverrideAsync(int serverId, string? database, string key, int value, string setBy, CancellationToken cancellationToken = default);

    Task RemoveThresholdOverrideAsync(int serverId, string? database, string key, CancellationToken cancellationToken = default);

    /// <summary>Sets a tag's override (#6926): the value for every server under the tag and the tags below it.</summary>
    Task SaveThresholdOverrideForTagAsync(int tagId, string key, int value, string setBy, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();

    Task RemoveThresholdOverrideForTagAsync(int tagId, string key, CancellationToken cancellationToken = default) =>
        throw new NotSupportedException();
}

/// <summary>One scope the add form can pick: a server, or (<paramref name="TagId"/> set, #6926) a server tag, whose <see cref="Name"/>
/// reads <c>Tag: name</c> and whose <paramref name="ServerId"/> is 0.</summary>
public sealed record ThresholdOverrideServerChoice(int ServerId, string Name, int? TagId = null);

/// <summary>The grid's row: the stored override as the window shows it.</summary>
public sealed class ThresholdOverrideViewRow
{
    public ThresholdOverrideViewRow(ThresholdOverrideRow row) => Row = row;

    public ThresholdOverrideRow Row { get; }

    public int ServerId => Row.ServerId;
    public string ServerName => Row.ServerName;
    public bool IsTag => Row.IsTag;
    public int? TagId => Row.TagId;
    /// <summary>The Scope column (#6926): the server's name, or <c>Tag: name</c> for a tag override. A tag override whose tag was deleted reads
    /// <c>Tag: (deleted) - orphaned, matches nothing</c>, as an orphaned notification route is marked.</summary>
    public string ScopeText => !Row.IsTag ? Row.ServerName : Row.Orphaned ? "Tag: (deleted) - orphaned, matches nothing" : "Tag: " + Row.ServerName;
    public string? Database => Row.Database;
    public string DatabaseText => Row.Database ?? "Whole server";
    public string Key => Row.Key;
    public string ValueText => Row.Value.ToString("0.##", CultureInfo.CurrentCulture);
    public string GlobalValueText => Row.GlobalValue is { } g ? g.ToString("0.##", CultureInfo.CurrentCulture) : "";
    public string SetBy => Row.SetBy;
    public string RequestText => Row.RequestId is { } id ? id.ToString("N")[..8] : "";
    /// <summary>The override's last change, as every fleet-wide window words a time (#6457): the viewer's Local or UTC
    /// reading with the zone's name after it. It was this machine's time with no zone named, beside two windows that
    /// read other conventions.</summary>
    public string UpdatedText => ViewerTimeHelper.FormatFleetWide(Row.UpdatedAt, "yyyy-MM-dd HH:mm");
}

/// <summary>The outcome of Save or Remove: <see cref="Ok"/> and the sentence the status line shows.</summary>
public sealed record ThresholdOverrideOutcome(bool Ok, string Message);

/// <summary>
/// The threshold-overrides window's logic (#5916 part 4), with no WPF in it. Save checks the key and the bounds with the same
/// registry and rules the <c>set_alert_threshold_override</c> tool applies (<see cref="DarlingThresholdKeys"/>) before it
/// calls the store, so a refused entry never reaches a write; Remove checks that a row is chosen. A read-only role's refusal
/// surfaces here as a sentence; any other store failure (a dropped connection, a timeout) is caught by the window's own handlers.
/// </summary>
public sealed class ThresholdOverridesViewModel
{
    private readonly IThresholdOverrideGateway _gateway;
    private readonly string _setBy;

    public ThresholdOverridesViewModel(IThresholdOverrideGateway gateway, string login)
    {
        _gateway = gateway ?? throw new ArgumentNullException(nameof(gateway));
        _setBy = ThresholdOverrideStore.SetByFor(login, "viewer");
    }

    /// <summary>The keys the form offers, in display order.</summary>
    public static IReadOnlyList<string> Keys => DarlingThresholdKeys.Entries.Select(e => e.Wire).ToList();

    /// <summary>Why this entry is refused, or null: a server must be chosen, the key must be a tunable threshold, the value a whole
    /// number inside that key's bounds, and a database only on a key that takes one. <paramref name="value"/> is the parsed number.</summary>
    public static string? Validate(int? serverId, string? key, string? valueText, string? databaseText, out int value)
    {
        value = 0;
        if (serverId is null)
            return "Choose a server.";
        if (DarlingThresholdKeys.RefusalFor(key) is { } keyRefusal)
            return keyRefusal.Message;
        DarlingThresholdKeys.TryGet(key, out var entry);
        if (!int.TryParse(valueText?.Trim(), NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value))
            return $"'{entry.Wire}' needs a whole number as its value.";
        var database = string.IsNullOrWhiteSpace(databaseText) ? null : databaseText.Trim();
        return DarlingThresholdKeys.ValidateValue(entry, value, database)?.Message;
    }

    /// <summary>Why a tag entry is refused, or null: a tag must be chosen, the key must be a tunable threshold, the value a whole number
    /// inside its bounds, and no database is given, because a tag value is the server-level value for every server under the tag.</summary>
    public static string? ValidateForTag(int? tagId, string? key, string? valueText, string? databaseText, out int value)
    {
        value = 0;
        if (tagId is null)
            return "Choose a tag.";
        if (!string.IsNullOrWhiteSpace(databaseText))
            return "A tag override holds a server-level value, so it takes no database. Clear the Database box.";
        return Validate(0, key, valueText, null, out value);
    }

    public async Task<IReadOnlyList<ThresholdOverrideViewRow>> LoadAsync(CancellationToken cancellationToken = default) =>
        (await _gateway.ListThresholdOverridesAsync(cancellationToken)).Select(r => new ThresholdOverrideViewRow(r)).ToList();

    /// <summary>Checks the entry, then writes it. An entry that fails the check changes nothing.</summary>
    public async Task<ThresholdOverrideOutcome> SaveAsync(
        int? serverId, string? key, string? valueText, string? databaseText, CancellationToken cancellationToken = default)
    {
        if (Validate(serverId, key, valueText, databaseText, out var value) is { } refusal)
            return new ThresholdOverrideOutcome(false, refusal);

        DarlingThresholdKeys.TryGet(key, out var entry);
        var database = string.IsNullOrWhiteSpace(databaseText) ? null : databaseText.Trim();
        try
        {
            await _gateway.SaveThresholdOverrideAsync(serverId!.Value, database, entry.Wire, value, _setBy, cancellationToken);
        }
        catch (ViewerReadOnlyException ex)
        {
            return new ThresholdOverrideOutcome(false, ex.Message);
        }

        return new ThresholdOverrideOutcome(true, $"Override saved: {entry.Wire} = {value.ToString(CultureInfo.CurrentCulture)}. The service applies it within its reload interval.");
    }

    /// <summary>Checks a tag entry, then writes it (#6926). An entry that fails the check changes nothing.</summary>
    public async Task<ThresholdOverrideOutcome> SaveForTagAsync(
        int? tagId, string? key, string? valueText, string? databaseText, CancellationToken cancellationToken = default)
    {
        if (ValidateForTag(tagId, key, valueText, databaseText, out var value) is { } refusal)
            return new ThresholdOverrideOutcome(false, refusal);

        DarlingThresholdKeys.TryGet(key, out var entry);
        try
        {
            await _gateway.SaveThresholdOverrideForTagAsync(tagId!.Value, entry.Wire, value, _setBy, cancellationToken);
        }
        catch (ViewerReadOnlyException ex)
        {
            return new ThresholdOverrideOutcome(false, ex.Message);
        }

        return new ThresholdOverrideOutcome(true, $"Tag override saved: {entry.Wire} = {value.ToString(CultureInfo.CurrentCulture)}. The service applies it within its reload interval to every server under the tag that has no override of its own.");
    }

    /// <summary>Removes the chosen override so the global value applies again.</summary>
    public async Task<ThresholdOverrideOutcome> RemoveAsync(ThresholdOverrideViewRow? row, CancellationToken cancellationToken = default)
    {
        if (row is null)
            return new ThresholdOverrideOutcome(false, "Select an override to remove.");
        try
        {
            if (row.TagId is { } tagId)
                await _gateway.RemoveThresholdOverrideForTagAsync(tagId, row.Key, cancellationToken);
            else
                await _gateway.RemoveThresholdOverrideAsync(row.ServerId, row.Database, row.Key, cancellationToken);
        }
        catch (ViewerReadOnlyException ex)
        {
            return new ThresholdOverrideOutcome(false, ex.Message);
        }

        return new ThresholdOverrideOutcome(true, $"Override removed: {row.Key} on {row.ScopeText} uses the global value again.");
    }
}
