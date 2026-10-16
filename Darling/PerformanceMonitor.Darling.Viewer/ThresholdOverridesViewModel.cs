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
}

/// <summary>One server the add form can pick.</summary>
public sealed record ThresholdOverrideServerChoice(int ServerId, string Name);

/// <summary>The grid's row: the stored override as the window shows it.</summary>
public sealed class ThresholdOverrideViewRow
{
    public ThresholdOverrideViewRow(ThresholdOverrideRow row) => Row = row;

    public ThresholdOverrideRow Row { get; }

    public int ServerId => Row.ServerId;
    public string ServerName => Row.ServerName;
    public string? Database => Row.Database;
    public string DatabaseText => Row.Database ?? "Whole server";
    public string Key => Row.Key;
    public string ValueText => Row.Value.ToString("0.##", CultureInfo.CurrentCulture);
    public string GlobalValueText => Row.GlobalValue is { } g ? g.ToString("0.##", CultureInfo.CurrentCulture) : "";
    public string SetBy => Row.SetBy;
    public string RequestText => Row.RequestId is { } id ? id.ToString("N")[..8] : "";
    public string UpdatedText => Row.UpdatedAt.ToLocalTime().ToString("g", CultureInfo.CurrentCulture);
}

/// <summary>The outcome of Save or Remove: <see cref="Ok"/> and the sentence the status line shows.</summary>
public sealed record ThresholdOverrideOutcome(bool Ok, string Message);

/// <summary>
/// The threshold-overrides window's logic (#5916 part 4), with no WPF in it. Save checks the key and the bounds with the same
/// registry and rules the <c>set_alert_threshold_override</c> tool applies (<see cref="DarlingThresholdKeys"/>) before it
/// calls the store, so a refused entry never reaches a write; Remove checks that a row is chosen. The store's own refusals
/// (a read-only role) surface as a sentence, never as an exception to the window.
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

    /// <summary>Removes the chosen override so the global value applies again.</summary>
    public async Task<ThresholdOverrideOutcome> RemoveAsync(ThresholdOverrideViewRow? row, CancellationToken cancellationToken = default)
    {
        if (row is null)
            return new ThresholdOverrideOutcome(false, "Select an override to remove.");
        try
        {
            await _gateway.RemoveThresholdOverrideAsync(row.ServerId, row.Database, row.Key, cancellationToken);
        }
        catch (ViewerReadOnlyException ex)
        {
            return new ThresholdOverrideOutcome(false, ex.Message);
        }

        return new ThresholdOverrideOutcome(true, $"Override removed: {row.Key} on {row.ServerName} uses the global value again.");
    }
}
