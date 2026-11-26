/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9744: the zip ships config\collection_schedule.json and a fresh install seeds it into its config folder
/// (<see cref="ConfigSeeder"/>) before <see cref="ScheduleManager"/> loads it. A shipped file in the old v1 shape,
/// or one missing collectors the code now schedules, makes the first start of every new install migrate it and add
/// the missing collectors, so the first log an operator reads shows a migration of settings they never had. These
/// tests take the shipped file down the fresh-install path and require that the load leaves it alone.
/// </summary>
public sealed class ShippedScheduleFileTests : IDisposable
{
    private const string FileName = "collection_schedule.json";

    private readonly string _configDir = Directory.CreateTempSubdirectory("pm-lite-shipped-schedule-").FullName;

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch { /* best effort */ }
    }

    private static string ShippedDir() => Path.Combine(AppContext.BaseDirectory, "Fixtures");

    private string SeedFreshInstall()
    {
        Assert.True(
            File.Exists(Path.Combine(ShippedDir(), FileName)),
            "collection_schedule.json was not copied beside the test binary: check the csproj None/Link item.");
        Assert.Equal(1, ConfigSeeder.SeedMissing(ShippedDir(), _configDir, [FileName]));
        return Path.Combine(_configDir, FileName);
    }

    /// <summary>
    /// A fresh install's first load must not migrate the file: a v1 file is rewritten as v2 and its original is kept
    /// as <c>.v1.bak</c>, and a file missing collectors is rewritten with them added. Neither happens when the
    /// shipped file is already the current shape with every collector, so the seeded bytes are unchanged afterwards.
    /// </summary>
    [Fact]
    public void FreshInstall_LoadingTheShippedFile_MigratesNothingAndAddsNothing()
    {
        var path = SeedFreshInstall();
        var before = File.ReadAllBytes(path);

        _ = new ScheduleManager(_configDir);

        Assert.False(File.Exists(path + ".v1.bak"), "the shipped file is in the v1 shape, so the first start migrates it");
        Assert.True(
            before.AsSpan().SequenceEqual(File.ReadAllBytes(path)),
            "the first load rewrote the shipped file: it is missing collectors, carries a retired one, or needed a clamp");
    }

    /// <summary>
    /// What the first load hands the collection loop is what the code defaults say: the same collectors, each with the
    /// same enabled state, cadence and retention. A fresh install runs the shipped values, so a default changed in code
    /// without the shipped file following would make new installs collect on a different cadence than the code intends.
    /// </summary>
    [Fact]
    public void FreshInstall_LoadedDefaultSchedule_MatchesTheCodeDefaults()
    {
        SeedFreshInstall();

        var loaded = new ScheduleManager(_configDir).GetDefaultSchedule();
        var defaults = ScheduleManager.GetDefaultSchedules();

        Assert.Equal(
            defaults.Select(d => d.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase),
            loaded.Select(s => s.Name).OrderBy(n => n, StringComparer.OrdinalIgnoreCase));

        var drifted = defaults
            .Where(d => loaded.Single(s => string.Equals(s.Name, d.Name, StringComparison.OrdinalIgnoreCase)) is var s
                && (s.Enabled != d.Enabled || s.FrequencyMinutes != d.FrequencyMinutes
                    || s.RetentionDays != d.RetentionDays || s.RunAt != d.RunAt))
            .Select(d => d.Name)
            .ToList();
        Assert.True(
            drifted.Count == 0,
            "the shipped collection_schedule.json disagrees with the code defaults for: " + string.Join(", ", drifted)
                + ". Regenerate it: start Lite once on an empty config folder and copy the file it writes.");
    }
}
