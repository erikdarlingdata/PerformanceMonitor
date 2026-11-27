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

namespace PerformanceMonitor.Analysis;

/// <summary>One tempdb file as the newest <c>database_size_stats</c> collection stored it (#8883).</summary>
public sealed record TempDbSetupFile(
    string FileType,
    string FileName,
    double? TotalSizeMb,
    double? AutoGrowthMb,
    bool IsPercentGrowth,
    double? GrowthPct,
    double? MaxSizeMb,
    bool? IsAutogrowAllFiles,
    DateTime CollectedAt)
{
    public bool IsData => string.Equals(FileType, "ROWS", StringComparison.OrdinalIgnoreCase);
    public bool IsLog => string.Equals(FileType, "LOG", StringComparison.OrdinalIgnoreCase);

    /// <summary>The growth setting as one comparable value: the percent for a percent file, the megabytes for a fixed one.
    /// A fixed file's stored percent (0 or NULL) and a percent file's stored megabytes are not part of its setting.</summary>
    internal (bool Percent, double Step) GrowthSetting =>
        IsPercentGrowth ? (true, GrowthPct ?? 0) : (false, AutoGrowthMb ?? 0);

    /// <summary>The growth setting in words, for the drill-down.</summary>
    public string GrowthText =>
        IsPercentGrowth ? $"{(GrowthPct ?? 0):0.##}%" : (AutoGrowthMb is > 0 ? $"{AutoGrowthMb.Value:N0} MB" : "off");
}

/// <summary>The newest visible online scheduler count and the collection that stored it (#8883).</summary>
public sealed record TempDbSchedulerCount(int Count, DateTime CollectedAt);

/// <summary>
/// The TEMPDB_FILE_SETUP fact (#8883): tempdb file layouts that cause allocation contention or uneven file use. It is built
/// from the newest tempdb collection and the visible online scheduler count, by the same code on both products, so Lite and
/// Darling cannot disagree about which checks a server fails. It is advisory: the scorer gives it the 0.3 config base and no
/// alert reads it.
/// </summary>
public static class TempDbFileSetup
{
    public const string FactKey = "TEMPDB_FILE_SETUP";

    /// <summary>Fact metadata: when the tempdb files were collected, in Unix seconds (the unit the other facts' time keys use).</summary>
    public const string MetaFilesCollectedUnix = "files_collected_unix";

    /// <summary>Fact metadata: when the scheduler count was collected, in Unix seconds. Absent when no scheduler count was collected.</summary>
    public const string MetaSchedulerCollectedUnix = "scheduler_collected_unix";

    /// <summary>The data file count past which more files stop helping: the lesser of this and the schedulers is the target.</summary>
    public const int MaxRecommendedDataFiles = 8;

    /// <summary>The largest and the smallest data file may differ by this fraction of the largest before the sizes count as unequal.</summary>
    public const double SizeSpreadTolerance = 0.01;

    /// <summary>
    /// Builds the fact, or returns null when the server fails no check or has no tempdb rows (an Azure SQL Database stores none).
    /// <paramref name="scheduler"/> is the visible online scheduler count with its collection time, null when none was collected,
    /// which skips the file-count check. The fact's value is the number of checks failed, and its metadata says when the files
    /// and the scheduler count were collected.
    /// </summary>
    public static Fact? BuildFact(AnalysisContext context, IReadOnlyList<TempDbSetupFile> files, TempDbSchedulerCount? scheduler)
    {
        if (files.Count == 0)
            return null;

        var data = files.Where(f => f.IsData).ToList();
        var logs = files.Where(f => f.IsLog).ToList();

        var metadata = new Dictionary<string, double>
        {
            ["data_file_count"] = data.Count,
            ["log_file_count"] = logs.Count,
            [MetaFilesCollectedUnix] = Unix(files.Max(f => f.CollectedAt)),
        };

        var failed = 0;
        void Check(string name, bool fails)
        {
            metadata[name] = fails ? 1 : 0;
            if (fails)
                failed++;
        }

        /* 1. Fewer data files than the lesser of 8 and the schedulers. */
        if (scheduler is { Count: > 0 })
        {
            var target = Math.Min(MaxRecommendedDataFiles, scheduler.Count);
            metadata["scheduler_count"] = scheduler.Count;
            metadata[MetaSchedulerCollectedUnix] = Unix(scheduler.CollectedAt);
            metadata["recommended_data_file_count"] = target;
            Check("check_data_file_count", data.Count < target);
        }

        /* 2. Unequal data file sizes: the proportional fill sends most writes to the biggest free file. */
        var sizes = data.Where(f => f.TotalSizeMb.HasValue).Select(f => f.TotalSizeMb!.Value).ToList();
        if (sizes.Count > 0)
        {
            var largest = sizes.Max();
            var smallest = sizes.Min();
            metadata["data_file_largest_mb"] = largest;
            metadata["data_file_smallest_mb"] = smallest;
            Check("check_data_file_sizes", sizes.Count > 1 && largest - smallest > largest * SizeSpreadTolerance);
        }

        /* 3. Data files whose growth settings differ. */
        var growthKinds = data.Select(f => f.GrowthSetting).Distinct().Count();
        metadata["data_file_growth_setting_count"] = growthKinds;
        Check("check_data_file_growth", growthKinds > 1);

        /* 4. Percent growth on any tempdb file, data or log. */
        var percentFiles = files.Count(f => f.IsPercentGrowth);
        metadata["percent_growth_file_count"] = percentFiles;
        Check("check_percent_growth", percentFiles > 0);

        /* 5. AUTOGROW_ALL_FILES off on the data files. NULL is unknown (before SQL Server 2016), never a failure. */
        var flags = data.Where(f => f.IsAutogrowAllFiles.HasValue).Select(f => f.IsAutogrowAllFiles!.Value).ToList();
        if (flags.Count > 0)
        {
            var off = flags.Count(on => !on);
            metadata["autogrow_all_files_off_count"] = off;
            Check("check_autogrow_all_files", off > 0);
        }

        /* 6. More than one log file: the log is written serially, so a second file adds nothing. */
        Check("check_log_file_count", logs.Count > 1);

        if (failed == 0)
            return null;

        return new Fact
        {
            Source = "config",
            Key = FactKey,
            Value = failed,
            ServerId = context.ServerId,
            Metadata = metadata
        };
    }

    /// <summary>Naive UTC to Unix seconds. The store's timestamps are naive UTC, so the kind is stamped rather than converted.</summary>
    private static double Unix(DateTime utc) =>
        new DateTimeOffset(DateTime.SpecifyKind(utc, DateTimeKind.Utc)).ToUnixTimeSeconds();

    /// <summary>The drill-down rows: one per tempdb file with its size and growth, data files first, each with the time of the
    /// collection it came from.</summary>
    public static List<object> DrillDownRows(IReadOnlyList<TempDbSetupFile> files) =>
        files
            .OrderBy(f => f.IsData ? 0 : 1)
            .ThenBy(f => f.FileName, StringComparer.OrdinalIgnoreCase)
            .Select(f => (object)new
            {
                file_name = f.FileName,
                file_type = f.FileType,
                total_size_mb = f.TotalSizeMb,
                growth = f.GrowthText,
                is_percent_growth = f.IsPercentGrowth,
                max_size_mb = f.MaxSizeMb,
                autogrow_all_files = f.IsAutogrowAllFiles,
                collected_at = f.CollectedAt
            })
            .ToList();
}
