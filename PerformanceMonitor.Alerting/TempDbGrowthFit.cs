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

namespace PerformanceMonitor.Alerting;

/// <summary>One volume (and one kind of file on it) where tempdb's next autogrowth is larger than the free space.</summary>
/// <param name="Volume">The volume's mount point.</param>
/// <param name="IsLog">True for the log file's growth, false for the data files'.</param>
/// <param name="FileNames">The files whose growth is judged: every growing data file when they grow together, the one with the largest step otherwise.</param>
/// <param name="NeededMb">The next growth, in MB.</param>
/// <param name="FreeMb">The volume's free space, in MB.</param>
public sealed record TempDbGrowthShortfall(string Volume, bool IsLog, IReadOnlyList<string> FileNames, double NeededMb, double FreeMb)
{
    public double ShortByMb => NeededMb - FreeMb;
}

/// <summary>
/// What the newest tempdb file collection says about the next autogrowth (#8882).
/// </summary>
public sealed class TempDbGrowthFitResult
{
    public IReadOnlyList<TempDbGrowthShortfall> Shortfalls { get; init; } = Array.Empty<TempDbGrowthShortfall>();

    /// <summary>Every file in the collection, data and log.</summary>
    public int FileCount { get; init; }

    /// <summary>The files whose next growth is zero: growth turned off, or the file is at its ceiling. Files with no growth figure are not counted.</summary>
    public IReadOnlyList<string> FilesUnableToGrow { get; init; } = Array.Empty<string>();

    public int DataFileCount { get; init; }
    public int DataFilesUnableToGrow { get; init; }

    /// <summary>True when the server has data files and none of them can grow. TempDB Space is what fires when such a tempdb fills.</summary>
    public bool NoDataFileCanGrow => DataFileCount > 0 && DataFilesUnableToGrow == DataFileCount;

    /// <summary>The number of files that carried both a growth figure and a readable volume, so the fit could be judged for them.</summary>
    public int JudgedFileCount { get; init; }

    public DateTime? CollectionTimeUtc { get; init; }
}

/// <summary>
/// The arithmetic behind the TempDB Growth Won't Fit alert (#8882), kept apart from the engine so both stores' reads feed one rule.
///
/// <para><b>A file's next growth.</b> The fixed step, or the percent step applied to the file's current size. When the file has a ceiling,
/// the step is cut to the room left under it, which is zero at the ceiling. A file whose step is zero cannot grow. A file with no step
/// figure (a row stored before the column was read, a percent step with no size) is unknown, not zero.</para>
///
/// <para><b>Per volume.</b> Data files that grow together (<see cref="TempDbFileInfo.IsAutogrowAllFiles"/>) need the sum of their steps at
/// once. Otherwise the file that fills first grows alone, so the largest single step is what has to fit. The log file's step is judged on
/// its own. Each is held against the volume's free space from the same collection. Files with no readable volume are skipped.</para>
/// </summary>
public static class TempDbGrowthFit
{
    /// <summary>
    /// The size of the file's next autogrowth in MB: 0 when it cannot grow, null when the row does not say.
    /// </summary>
    public static double? NextGrowthMb(TempDbFileInfo file)
    {
        if (file is null) throw new ArgumentNullException(nameof(file));

        double? step;
        if (file.IsPercentGrowth)
        {
            step = file.TotalSizeMb is double size && file.GrowthPct is double pct ? size * pct / 100.0 : null;
        }
        else
        {
            step = file.AutoGrowthMb;
        }

        if (step is not double growth)
        {
            return null;
        }

        if (growth <= 0)
        {
            return 0;
        }

        /* A ceiling of -1 is unlimited. 0 or more is a real cap, and 0 means the file may not grow at all. The cap needs the current size. */
        if (file.MaxSizeMb is double max && max >= 0 && file.TotalSizeMb is double total)
        {
            growth = Math.Min(growth, Math.Max(0, max - total));
        }

        return growth;
    }

    public static TempDbGrowthFitResult Evaluate(IReadOnlyList<TempDbFileInfo> files)
    {
        if (files is null) throw new ArgumentNullException(nameof(files));

        var unable = new List<string>();
        int dataFiles = 0;
        int dataUnable = 0;
        int judged = 0;
        var steps = new List<(TempDbFileInfo File, double Step)>();

        foreach (var file in files)
        {
            if (!file.IsLog)
            {
                dataFiles++;
            }

            var step = NextGrowthMb(file);
            bool hasVolume = !string.IsNullOrEmpty(file.VolumeMountPoint) && file.VolumeFreeMb is not null;
            if (step is double known)
            {
                if (known <= 0)
                {
                    unable.Add(file.FileName);
                    if (!file.IsLog)
                    {
                        dataUnable++;
                    }
                }
                else if (hasVolume)
                {
                    steps.Add((file, known));
                }

                if (hasVolume)
                {
                    judged++;
                }
            }
        }

        var shortfalls = new List<TempDbGrowthShortfall>();
        foreach (var volume in steps.GroupBy(s => s.File.VolumeMountPoint!, StringComparer.OrdinalIgnoreCase))
        {
            double free = volume.Min(s => s.File.VolumeFreeMb!.Value);

            var data = volume.Where(s => !s.File.IsLog).OrderBy(s => s.File.FileId).ToList();
            if (data.Count > 0)
            {
                bool together = data.Any(s => s.File.IsAutogrowAllFiles == true);
                double needed;
                List<string> named;
                if (together)
                {
                    needed = data.Sum(s => s.Step);
                    named = data.Select(s => s.File.FileName).ToList();
                }
                else
                {
                    var largest = data.OrderByDescending(s => s.Step).First();
                    needed = largest.Step;
                    named = new List<string> { largest.File.FileName };
                }

                if (needed > free)
                {
                    shortfalls.Add(new TempDbGrowthShortfall(volume.Key, false, named, needed, free));
                }
            }

            foreach (var log in volume.Where(s => s.File.IsLog).OrderBy(s => s.File.FileId))
            {
                if (log.Step > free)
                {
                    shortfalls.Add(new TempDbGrowthShortfall(volume.Key, true, new List<string> { log.File.FileName }, log.Step, free));
                }
            }
        }

        DateTime? newest = null;
        foreach (var file in files)
        {
            if (file.CollectionTimeUtc is DateTime t && (newest is null || t > newest))
            {
                newest = t;
            }
        }

        return new TempDbGrowthFitResult
        {
            Shortfalls = shortfalls.OrderByDescending(s => s.ShortByMb).ThenBy(s => s.Volume, StringComparer.OrdinalIgnoreCase).ToList(),
            FileCount = files.Count,
            FilesUnableToGrow = unable,
            DataFileCount = dataFiles,
            DataFilesUnableToGrow = dataUnable,
            JudgedFileCount = judged,
            CollectionTimeUtc = newest,
        };
    }

    /// <summary>One line for the alert text: the largest shortfall, and how many more volumes are short.</summary>
    public static string Describe(TempDbGrowthFitResult result)
    {
        if (result is null) throw new ArgumentNullException(nameof(result));
        if (result.Shortfalls.Count == 0)
        {
            return "The next tempdb growth fits";
        }

        var worst = result.Shortfalls[0];
        var kind = worst.IsLog ? "log" : "data";
        var text = string.Format(
            CultureInfo.InvariantCulture,
            "tempdb {0} growth of {1:F0} MB won't fit on {2} ({3:F0} MB free)",
            kind, worst.NeededMb, worst.Volume, worst.FreeMb);
        return result.Shortfalls.Count > 1
            ? text + string.Format(CultureInfo.InvariantCulture, ", and {0} more", result.Shortfalls.Count - 1)
            : text;
    }
}
