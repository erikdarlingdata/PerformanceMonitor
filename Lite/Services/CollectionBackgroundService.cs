/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
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
    /* Start at UtcNow so maintenance tasks don't all fire on the very first cycle. */
    private DateTime _lastArchiveTime = DateTime.UtcNow;
    private DateTime _lastRetentionTime = DateTime.UtcNow;

    /* Archive every hour, retention once per day */
    private static readonly TimeSpan ArchiveInterval = TimeSpan.FromHours(1);
    private static readonly TimeSpan RetentionInterval = TimeSpan.FromHours(24);

    /* Size-based trigger — when the database exceeds this size, archive ALL data
       to parquet and reset the database. INSERT performance degrades badly with
       large tables (33x slower at 667MB in testing). Data remains fully queryable
       through the archive views (hot UNION parquet). */
    private const double ArchiveSizeThresholdMb = 512;

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

                /* Periodic archival (time-based or size-based) */
                await RunArchivalIfDueAsync();

                /* Periodic retention cleanup */
                RunRetentionIfDue();

                /* Log process memory at the end of each cycle. Lets bug reporters
                   self-report memory without Task Manager, gives us a continuous
                   memory trace for diagnosis, and surfaces regressions in the log
                   that would otherwise need external sampling to detect. Three
                   property reads — negligible overhead at 1-minute cadence. */
                LogProcessMemory();
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
        if (_archiveService == null)
        {
            return;
        }

        var timeDue = DateTime.UtcNow - _lastArchiveTime >= ArchiveInterval;
        var sizeDue = _duckDb != null && _duckDb.GetDatabaseSizeMb() >= ArchiveSizeThresholdMb;

        if (!timeDue && !sizeDue)
        {
            return;
        }

        try
        {
            if (sizeDue)
            {
                _logger?.LogInformation("Database size ({SizeMb:F0} MB) exceeds {Threshold} MB — archiving all data and resetting database",
                    _duckDb!.GetDatabaseSizeMb(), ArchiveSizeThresholdMb);
                await _archiveService.ArchiveAllAndResetAsync();
            }
            else
            {
                await _archiveService.ArchiveOldDataAsync(hotDataDays: 7);
            }
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
            _retentionService.CleanupOldArchives(retentionMonths: 3);
            _lastRetentionTime = DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _logger?.LogError(ex, "Retention cleanup failed");
        }
    }

    private void LogProcessMemory()
    {
        try
        {
            using var process = Process.GetCurrentProcess();
            var wsMb = process.WorkingSet64 / 1024 / 1024;
            var privMb = process.PrivateMemorySize64 / 1024 / 1024;
            var gcMb = GC.GetTotalMemory(forceFullCollection: false) / 1024 / 1024;
            _logger?.LogInformation(
                "Process memory: WS={WorkingSetMb} MB, Private={PrivateMb} MB, GC heap={GcMb} MB",
                wsMb, privMb, gcMb);
        }
        catch (Exception ex)
        {
            _logger?.LogDebug(ex, "Failed to read process memory stats");
        }
    }

}
