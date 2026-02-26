/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Cleans up old Parquet archive files beyond the retention period.
/// </summary>
public class RetentionService
{
    private readonly string _archivePath;
    private readonly ILogger<RetentionService>? _logger;

    public RetentionService(string archivePath, ILogger<RetentionService>? logger = null)
    {
        _archivePath = archivePath;
        _logger = logger;
    }

    /// <summary>
    /// Deletes Parquet files older than the specified retention period.
    /// Supports two naming formats:
    ///   - Timestamped: "20260221_1328_wait_stats.parquet" (yyyyMMdd prefix)
    ///   - Legacy monthly: "2026-02_wait_stats.parquet" (yyyy-MM prefix)
    /// </summary>
    public void CleanupOldArchives(int retentionDays = 90)
    {
        if (!Directory.Exists(_archivePath))
        {
            return;
        }

        var cutoffDate = DateTime.UtcNow.AddDays(-retentionDays);

        foreach (var file in Directory.GetFiles(_archivePath, "*.parquet"))
        {
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);

                /* Try timestamped format first: "20260221_1328_wait_stats" -> "20260221" */
                if (fileName.Length >= 8 &&
                    DateTime.TryParseExact(
                        fileName[..8],
                        "yyyyMMdd",
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.None,
                        out var fileDate))
                {
                    if (fileDate < cutoffDate)
                    {
                        File.Delete(file);
                        _logger?.LogInformation("Deleted expired archive: {File}", file);
                    }
                }
                /* Fall back to legacy monthly format: "2026-02_wait_stats" -> "2026-02" */
                else if (fileName.Length >= 7 &&
                    DateTime.TryParseExact(
                        fileName[..7],
                        "yyyy-MM",
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.None,
                        out var fileMonth))
                {
                    if (fileMonth < cutoffDate)
                    {
                        File.Delete(file);
                        _logger?.LogInformation("Deleted expired archive: {File}", file);
                    }
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to evaluate/delete archive file: {File}", file);
            }
        }
    }
}
