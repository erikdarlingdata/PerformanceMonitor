/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Analysis;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #1743: the two DuckDB-dialect facts the robust baseline scaffold rests on, pinned against a
/// LIVE DuckDB rather than assumed from docs. (1) <c>mad(x)</c> is the RAW median absolute
/// deviation — no consistency-constant scaling — matching BaselineBucket.Mad's unscaled contract;
/// the 0.6745 folds in at consumption (EffectiveRobustSigma). If a DuckDB upgrade ever changed
/// mad()'s scaling, every Lite modified-z would silently shift by 1.48x — this is the tripwire.
/// (2) GROUPING SETS ((hh,dw),(hh),()) produces exactly the Full + hour-only-sentinel + flat-sentinel
/// tier rows the shared SelectBucket contract expects, with NULLs COALESCEd to the -1 sentinels.
/// </summary>
public sealed class RobustBaselineDuckDbTests : IDisposable
{
    private readonly string _dbPath = Path.Combine(Path.GetTempPath(), $"robust_baseline_{Guid.NewGuid():N}.duckdb");

    public void Dispose()
    {
        if (File.Exists(_dbPath)) File.Delete(_dbPath);
    }

    [Fact]
    public void Mad_IsTheRawMedianAbsoluteDeviation_Unscaled()
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        connection.Open();
        using var cmd = connection.CreateCommand();

        /* Hand-computed set: median(1,2,3,4,100) = 3; |x-3| = {2,1,0,1,97}; median of that = 1.
           A scaled mad() (1.4826 * raw) would return ~1.48 and fail. The outlier 100 is the point:
           it drags the mean to 22 and the stddev past 43, while median/MAD shrug — the robustness
           property in one row set. */
        cmd.CommandText = "SELECT median(x), mad(x), AVG(x) FROM (VALUES (1.0), (2.0), (3.0), (4.0), (100.0)) AS t(x)";
        using var reader = cmd.ExecuteReader();
        Assert.True(reader.Read());
        Assert.Equal(3.0, Convert.ToDouble(reader.GetValue(0)), precision: 6);
        Assert.Equal(1.0, Convert.ToDouble(reader.GetValue(1)), precision: 6);
        Assert.Equal(22.0, Convert.ToDouble(reader.GetValue(2)), precision: 6);
    }

    [Fact]
    public void GroupingSetsScaffold_ProducesFullHourOnlyAndFlatSentinelRows()
    {
        using var connection = new DuckDBConnection($"Data Source={_dbPath}");
        connection.Open();
        using (var seed = connection.CreateCommand())
        {
            /* Two hours x two days of samples, distinct values per (hour, dow) so the tiers are
               distinguishable: the hour-14 sentinel must aggregate BOTH dows of hour 14, and the
               flat sentinel must aggregate everything. */
            seed.CommandText = @"
CREATE TABLE clean_fixture (collection_time TIMESTAMP, v DOUBLE);
INSERT INTO clean_fixture VALUES
  ('2026-07-06 14:10:00', 10), ('2026-07-06 14:20:00', 12), ('2026-07-06 14:30:00', 14),
  ('2026-07-07 14:10:00', 20), ('2026-07-07 14:20:00', 22), ('2026-07-07 14:30:00', 24),
  ('2026-07-06 15:10:00', 100), ('2026-07-06 15:20:00', 102),
  ('2026-07-07 15:10:00', 200), ('2026-07-07 15:20:00', 202);";
            seed.ExecuteNonQuery();
        }

        using var cmd = connection.CreateCommand();
        /* The Lite scaffold ITSELF (BaselineProvider.RobustTierScaffold, internal since #3653 Q6), over the
           fixture — this test carried a hand copy before, which is how a scaffold edit could pass here and fail
           in the product. The scaffold keys on the target's clock through $4..$6 (#3653 Q6); binding the
           transition at the window end with 0/0 offsets is UTC keying, which is what this fixture's expected
           rows were computed on. The clean CTE takes the provider's $1..$3 window shape so the numbering is the
           product's (DuckDB binds positionally and cannot reach $4 in a statement with no $1). */
        cmd.CommandText = "WITH clean AS (SELECT collection_time, v FROM clean_fixture WHERE $1 = 0 AND collection_time >= $2 AND collection_time < $3)," + BaselineProvider.RobustTierScaffold;
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = new DateTime(2026, 7, 1) });
        cmd.Parameters.Add(new DuckDBParameter { Value = new DateTime(2026, 7, 8) });
        cmd.Parameters.Add(new DuckDBParameter { Value = new DateTime(2026, 7, 8) });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });
        cmd.Parameters.Add(new DuckDBParameter { Value = 0 });

        var rows = new Dictionary<(int Hour, int Dow), (double Median, double Mad, long Samples, long Days)>();
        using (var reader = cmd.ExecuteReader())
        {
            while (reader.Read())
            {
                rows[(Convert.ToInt32(reader.GetValue(0)), Convert.ToInt32(reader.GetValue(1)))] =
                    (Convert.ToDouble(reader.GetValue(6)), Convert.ToDouble(reader.GetValue(7)),
                     Convert.ToInt64(reader.GetValue(4)), Convert.ToInt64(reader.GetValue(5)));
            }
        }

        /* 4 full buckets (2 hours x 2 dows) + 2 hour-only sentinels + 1 flat sentinel. */
        Assert.Equal(7, rows.Count);

        /* A full bucket: hour 14 on Monday (2026-07-06, DOW 1) = {10,12,14}. */
        var full = rows[(14, 1)];
        Assert.Equal(12.0, full.Median, precision: 6);
        Assert.Equal(2.0, full.Mad, precision: 6);
        Assert.Equal(3, full.Samples);
        Assert.Equal(1, full.Days);

        /* The hour-14 sentinel pools BOTH days: {10,12,14,20,22,24} → median 17, MAD 5 —
           values a per-bucket pooling could never produce, which is the whole reason the tiers
           are computed in SQL. */
        var hourOnly = rows[(14, -1)];
        Assert.Equal(17.0, hourOnly.Median, precision: 6);
        Assert.Equal(5.0, hourOnly.Mad, precision: 6);
        Assert.Equal(6, hourOnly.Samples);
        Assert.Equal(2, hourOnly.Days);

        /* The flat sentinel covers all ten samples, with an EXACT distinct-day count.
           Sorted {10,12,14,20,22,24,100,102,200,202} → median (22+24)/2 = 23. */
        var flat = rows[(-1, -1)];
        Assert.Equal(10, flat.Samples);
        Assert.Equal(2, flat.Days);
        Assert.Equal(23.0, flat.Median, precision: 6);
    }
}
