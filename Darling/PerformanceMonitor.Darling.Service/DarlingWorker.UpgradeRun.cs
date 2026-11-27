/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

public sealed partial class DarlingWorker
{
    /* #9432: records upgrade-run.json beside the install into collect.upgrade_runs. One instance for the life of the process, because it
       remembers which problems it already warned about and which run and state it last logged as recorded (#9841). */
    private UpgradeRunPickup? _upgradeRunPickup;

    /* Next due time for the upgrade run pickup. It rides the store-side pass that runs every s_storeSettingsCheckInterval, on its own stamp
       so it also runs when the alert engine is off. The upgrade restarts the service before its script ends, so the result and the
       verified state arrive as a file change that only a pass sees. */
    private DateTime _nextUpgradeRunCheckUtc = DateTime.MinValue;

    /// <summary>
    /// Records the upgrade run file on an open store connection (#9432). Failure-isolated: a store or file problem is logged and the file
    /// stays for the next pass, and nothing here fails a start or the pass that called it.
    /// </summary>
    internal async Task<UpgradeRunPickupResult?> TryRecordUpgradeRunAsync(
        NpgsqlConnection connection, string installDirectory, CancellationToken cancellationToken)
    {
        try
        {
            _upgradeRunPickup ??= new UpgradeRunPickup(_logger);
            var result = await _upgradeRunPickup.PickUpAsync(connection, installDirectory, DateTime.UtcNow, cancellationToken);
            _upgradeRunPickup.LogRecordedOnce(result);
            return result;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "The upgrade run file could not be recorded in the store this time, so it stays for the next pass: {Message}", ex.Message);
            return null;
        }
    }

    /// <summary>
    /// The store-side pass (#9432): when due, opens one connection and records the upgrade run file. The cheap case, no file, costs
    /// one file check and opens no connection.
    /// </summary>
    internal async Task RunUpgradeRunPassAsync(string installDirectory, CancellationToken cancellationToken)
    {
        if (_postgres is null || !StampIsDue(_nextUpgradeRunCheckUtc, s_storeSettingsCheckInterval, DateTime.UtcNow))
        {
            return;
        }

        _nextUpgradeRunCheckUtc = DateTime.UtcNow.Add(s_storeSettingsCheckInterval);
        if (!System.IO.File.Exists(System.IO.Path.Combine(installDirectory, UpgradeRunPickup.FileName)))
        {
            return;
        }

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(cancellationToken);
            await TryRecordUpgradeRunAsync(connection, installDirectory, cancellationToken);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            _logger.LogWarning(
                "The upgrade run file could not be recorded because the store could not be reached, so it stays for the next pass: {Message}", ex.Message);
        }
    }
}
