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
    /// <summary>
    /// How long an archived Parquet file is kept, in months — Lite's ONE retention horizon (#3541 A9).
    ///
    /// <para>Lite does not purge per collector: hot rows leave DuckDB for Parquet after the archive service's
    /// hot-data week, the <c>v_*</c> views union live and archive so every read sees both, and this is the
    /// age at which an archive file is deleted. Because every table — the signal tables, the collection log
    /// and the alert log alike — shares it, a day older than this is gone from every source at once, which is
    /// why the daily summary's retention horizon on Lite is this single number rather than the shortest of
    /// several. Named so the horizon the daily-summary reader publishes and the horizon the cleanup enforces
    /// are the same constant, not two literals that happen to agree.</para>
    /// </summary>
    public const int ArchiveRetentionMonths = 3;

    /// <summary>
    /// The oldest instant whose rows Lite is certain to still hold at <paramref name="utcNow"/>: what a read
    /// with no window can still see, and the <c>searchedFromUtc</c> the Overview card's freshness band is
    /// handed so a server whose whole history has aged out reads Offline rather than never collected (#3967).
    ///
    /// <para><b>Why a month start and not the cutoff itself.</b> An archive file is named for the month (or
    /// the day) it was WRITTEN, and <see cref="CleanupOldArchives"/> deletes a file once that date is before
    /// the cutoff, <see cref="ArchiveRetentionMonths"/> back. A month's file therefore goes whole, holding rows
    /// up to a month younger than the cutoff. A row is written into a file of its own month or a later one,
    /// because archival never runs before the row is collected, so it is certain to survive only from the
    /// first month start at or after the cutoff.</para>
    /// </summary>
    public static DateTime OldestRetainedInstant(DateTime utcNow)
    {
        var cutoff = DateTime.SpecifyKind(utcNow.AddMonths(-ArchiveRetentionMonths), DateTimeKind.Unspecified);
        var monthStart = new DateTime(cutoff.Year, cutoff.Month, 1, 0, 0, 0, DateTimeKind.Unspecified);
        return monthStart == cutoff ? monthStart : monthStart.AddMonths(1);
    }

    private readonly string _archivePath;
    private readonly ILogger<RetentionService>? _logger;

    public RetentionService(string archivePath, ILogger<RetentionService>? logger = null)
    {
        _archivePath = archivePath;
        _logger = logger;
    }

    /// <summary>
    /// Deletes Parquet files older than the specified retention period.
    /// Supports naming formats:
    ///   - Monthly compacted: "202602_wait_stats.parquet" (yyyyMM prefix)
    ///   - Timestamped: "20260221_1328_wait_stats.parquet" (yyyyMMdd prefix)
    ///   - Consolidated daily: "20260221_wait_stats.parquet" (yyyyMMdd prefix)
    ///   - Legacy monthly: "2026-02_wait_stats.parquet" (yyyy-MM prefix)
    /// </summary>
    public void CleanupOldArchives(int retentionMonths = ArchiveRetentionMonths)
    {
        if (!Directory.Exists(_archivePath))
        {
            return;
        }

        var cutoffDate = DateTime.UtcNow.AddMonths(-retentionMonths);

        foreach (var file in Directory.GetFiles(_archivePath, "*.parquet"))
        {
            try
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                DateTime? fileDate = null;

                /* Monthly compacted format: "202602_wait_stats" -> "202602" */
                if (fileName.Length >= 6 &&
                    DateTime.TryParseExact(
                        fileName[..6],
                        "yyyyMM",
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.None,
                        out var monthDate) &&
                    fileName.Length > 6 && fileName[6] == '_')
                {
                    fileDate = monthDate;
                }
                /* Timestamped or daily format: "20260221..." -> "20260221" */
                else if (fileName.Length >= 8 &&
                    DateTime.TryParseExact(
                        fileName[..8],
                        "yyyyMMdd",
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.None,
                        out var dayDate))
                {
                    fileDate = dayDate;
                }
                /* Legacy monthly format: "2026-02_wait_stats" -> "2026-02" */
                else if (fileName.Length >= 7 &&
                    DateTime.TryParseExact(
                        fileName[..7],
                        "yyyy-MM",
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.None,
                        out var legacyMonth))
                {
                    fileDate = legacyMonth;
                }

                if (fileDate.HasValue && fileDate.Value < cutoffDate)
                {
                    File.Delete(file);
                    _logger?.LogInformation("Deleted expired archive: {File}", file);
                }
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Failed to evaluate/delete archive file: {File}", file);
            }
        }
    }
}
