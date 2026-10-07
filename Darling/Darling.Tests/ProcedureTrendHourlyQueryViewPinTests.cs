/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5449: the procedure hourly read adds the hours a collector run fell in with no raw row (<c>ProcedureIdleHoursSql</c>); the
/// query views' rollups hold a row for every hour the server was monitored and keep their own rollup-only read. Their hourly
/// statements are pinned here by SHA-256 of the text the code produced on origin/dev, before the procedure change, line endings
/// normalised, so a change meant for the procedure grain cannot reach them. A deliberate change to the query views updates the
/// hash with its reason in the commit.
/// </summary>
public sealed class ProcedureTrendHourlyQueryViewPinTests
{
    public static TheoryData<string, string> Statements() => new()
    {
        { "BuildHourlyTrendSql(query_stats_hourly, unfiltered)", DurationTrendRouting.BuildHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView, false) },
        { "BuildHourlyTrendSql(query_stats_hourly, filtered)", DurationTrendRouting.BuildHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView, true) },
        { "BuildBucketedHourlyTrendSql(query_stats_hourly, unfiltered)", DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView) },
        { "BuildBucketedHourlyTrendSql(query_stats_hourly, filtered)", DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView, withDatabaseFilter: true) },
        { "BuildHourlyTrendSql(query_store_stats_hourly, unfiltered)", DurationTrendRouting.BuildHourlyTrendSql(TimescaleSupport.QueryStoreStatsHourlyView, false) },
        { "BuildHourlyTrendSql(query_store_stats_hourly, filtered)", DurationTrendRouting.BuildHourlyTrendSql(TimescaleSupport.QueryStoreStatsHourlyView, true) },
        { "BuildBucketedHourlyTrendSql(query_store_stats_hourly, unfiltered)", DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.QueryStoreStatsHourlyView) },
        { "BuildBucketedHourlyTrendSql(query_store_stats_hourly, filtered)", DurationTrendRouting.BuildBucketedHourlyTrendSql(TimescaleSupport.QueryStoreStatsHourlyView, withDatabaseFilter: true) },
        { "QueryDurationTrendHourlySql(unfiltered)", DurationTrendRouting.QueryDurationTrendHourlySql(withDatabaseFilter: false) },
        { "QueryDurationTrendHourlySql(filtered)", DurationTrendRouting.QueryDurationTrendHourlySql(withDatabaseFilter: true) },
    };

    private static readonly Dictionary<string, string> Pins = new()
    {
        ["BuildHourlyTrendSql(query_stats_hourly, unfiltered)"] = "94329B673BFAFA22248DA5D53EFE874662038CDED525C936579D54221C91F2E5",
        ["BuildHourlyTrendSql(query_stats_hourly, filtered)"] = "CCD84C371E46D72BF536118A8A492C0946552BF818E4B00518B6F8F3CEDDED09",
        ["BuildBucketedHourlyTrendSql(query_stats_hourly, unfiltered)"] = "AA3776E17212DF0B3DC9C1A059C8D9BDE4988DA16E9883F5414A67608340F372",
        ["BuildBucketedHourlyTrendSql(query_stats_hourly, filtered)"] = "5086B47CEC900CAC398A9702F37DAB86150C273DE015A5418B2754D96C5A6432",
        ["BuildHourlyTrendSql(query_store_stats_hourly, unfiltered)"] = "ABAC1BCC6EB7EA5160B70A8F4B44EDC799E151DB216D8A5BB705E986663688F2",
        ["BuildHourlyTrendSql(query_store_stats_hourly, filtered)"] = "44B2F1ACB7EFBBA3479A97E1504B6BD85E28523A3528743BC811FDBD805B927D",
        ["BuildBucketedHourlyTrendSql(query_store_stats_hourly, unfiltered)"] = "E8F5E099757ABE0272508D5734C19486406522EB5A8C63DA6EDC4B2C58E8BB2C",
        ["BuildBucketedHourlyTrendSql(query_store_stats_hourly, filtered)"] = "AD2CA98B90F33B449C946E2F8D7C55EB38F60E4CA2198302DD48A0BE1CA09225",
        ["QueryDurationTrendHourlySql(unfiltered)"] = "94329B673BFAFA22248DA5D53EFE874662038CDED525C936579D54221C91F2E5",
        ["QueryDurationTrendHourlySql(filtered)"] = "CCD84C371E46D72BF536118A8A492C0946552BF818E4B00518B6F8F3CEDDED09",
    };

    [Theory]
    [MemberData(nameof(Statements))]
    public void QueryViewHourlyReads_AreUnchangedByTheProcedureIdleHours(string label, string sql)
    {
        var normalized = sql.Replace("\r\n", "\n", StringComparison.Ordinal);
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(normalized)));

        Assert.True(Pins.TryGetValue(label, out var expected), $"no pin for {label}: {hash}");
        Assert.Equal(expected, hash);
    }
}
