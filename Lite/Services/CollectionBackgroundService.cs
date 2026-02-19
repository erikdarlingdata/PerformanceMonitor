/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Background service that runs data collection on a 1-minute timer,
/// plus periodic archival and retention cleanup.
/// </summary>
public class CollectionBackgroundService : BackgroundService
{
    private readonly RemoteCollectorService _collectorService;
    private readonly DuckDbInitializer? _duckDb;
    private readonly ServerManager? _serverManager;
    private readonly ArchiveService? _archiveService;
    private readonly RetentionService? _retentionService;
    private readonly ILogger<CollectionBackgroundService>? _logger;

    private static readonly TimeSpan CollectionInterval = TimeSpan.FromMinutes(1);
    private DateTime _lastArchiveTime = DateTime.MinValue;
    private DateTime _lastRetentionTime = DateTime.MinValue;
    private DateTime _lastCompactionTime = DateTime.MinValue;

    /* Archive every hour, retention + compaction once per day */
    private static readonly TimeSpan ArchiveInterval = TimeSpan.FromHours(1);
    private static readonly TimeSpan RetentionInterval = TimeSpan.FromHours(24);
    private static readonly TimeSpan CompactionInterval = TimeSpan.FromHours(24);

    /* Warn if database exceeds this size between compaction cycles */
    private const double SizeWarningThresholdMb = 1024;

    public bool IsPaused { get; set; }
    public DateTime? LastCollectionTime { get; private set; }
    public bool IsCollecting { get; private set; }

    public CollectionBackgroundService(
        RemoteCollectorService collectorService,
        DuckDbInitializer? duckDb = null,
        ArchiveService? archiveService = null,
        RetentionService? retentionService = null,
        ServerManager? serverManager = null,
        ILogger<CollectionBackgroundService>? logger = null)
    {
        _collectorService = collectorService;
        _duckDb = duckDb;
        _serverManager = serverManager;
        _archiveService = archiveService;
        _retentionService = retentionService;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger?.LogInformation("Collection background service started");

        /* Seed delta calculator from DuckDB so restarts don't lose baselines */
        await _collectorService.SeedDeltaCacheAsync();

        /* Wait a few seconds before first collection to let the app initialize */
        await Task.Delay(TimeSpan.FromSeconds(5), stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            if (!IsPaused)
            {
                /* Check all server connections before collecting */
                if (_serverManager != null)
                {
                    try
                    {
                        await _serverManager.CheckAllConnectionsAsync();
                    }
                    catch (Exception ex)
                    {
                        _logger?.LogError(ex, "Connection check failed");
                    }
                }

                try
                {
                    IsCollecting = true;
                    await _collectorService.RunDueCollectorsAsync(stoppingToken);
                    LastCollectionTime = DateTime.UtcNow;

                    /* Flush WAL during idle time instead of letting auto-checkpoint
                       stall collectors mid-write with 2-3s stop-the-world pauses */
                    await _collectorService.CheckpointAsync();
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _logger?.LogError(ex, "Collection cycle failed");
                }
                finally
                {
                    IsCollecting = false;
                }

                /* Periodic archival */
                await RunArchivalIfDueAsync();

                /* Periodic retention cleanup */
                RunRetentionIfDue();

                /* Periodic database compaction to prevent bloat */
                await RunCompactionIfDueAsync();
            }

            try
            {
                await Task.Delay(CollectionInterval, stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
        }

        _logger?.LogInformation("Collection background service stopped");
    }

    private async Task RunArchivalIfDueAsync()
    {
        if (_archiveService == null || DateTime.UtcNow - _lastArchiveTime < ArchiveInterval)
        {
            return;
        }

        try
        {
            await _archiveService.ArchiveOldDataAsync(hotDataDays: 7);
            _lastArchiveTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Archival cycle failed");
        }
    }

    private void RunRetentionIfDue()
    {
        if (_retentionService == null || DateTime.UtcNow - _lastRetentionTime < RetentionInterval)
        {
            return;
        }

        try
        {
            _retentionService.CleanupOldArchives(retentionDays: 90);
            _lastRetentionTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Retention cleanup failed");
        }
    }

    private async Task RunCompactionIfDueAsync()
    {
        if (_duckDb == null || DateTime.UtcNow - _lastCompactionTime < CompactionInterval)
        {
            /* Size watchdog: warn if database is large even between compaction cycles */
            if (_duckDb != null)
            {
                var sizeMb = _duckDb.GetDatabaseSizeMb();
                if (sizeMb > SizeWarningThresholdMb)
                {
                    _logger?.LogWarning("Database size is {SizeMb:F0} MB (threshold: {Threshold} MB) — compaction will run at next scheduled interval",
                        sizeMb, SizeWarningThresholdMb);
                }
            }
            return;
        }

        try
        {
            IsPaused = true;
            await _duckDb.CompactAsync();
            _lastCompactionTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Database compaction failed");
        }
        finally
        {
            IsPaused = false;
        }
    }
}
