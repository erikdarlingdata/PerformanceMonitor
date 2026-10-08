/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Byte-for-byte pins on the full <c>query_stats</c> SQL text a host sends, so a change to the query that
/// is not meant to move it fails loudly. Capture OFF is Lite's text: Lite never sets
/// <see cref="CollectorContext.CapturePlanXml"/>, and its query must stay identical whatever Darling does
/// with plan fetching (#5158). Capture ON with the fetch inline is today's Darling text. The text is
/// hashed after normalizing line endings, because the checkout's line endings follow the platform.
/// To move one of these on purpose, run the test and copy the actual hash from the failure message.
/// </summary>
public sealed class QueryStatsSqlGoldenTests
{
    private const string StandardCaptureOffSha256 = "84E8B903A904229A301E615818649D2C3E24F03B36812A3052F09B51085F3E72";
    private const string AzureSqlDbCaptureOffSha256 = "239B78B45A759FD45FFC4E606F4094E79842B821751FE8A9F349C6E07A007BD5";
    private const string StandardCaptureOnSha256 = "94933C5C62B54AE2B36DC514522C2890AB91AD38BFA92958B322B646A9968403";
    private const string AzureSqlDbCaptureOnSha256 = "55B7EABEA53660F11453E4ACE6FA7FC8E9073BBF2731BD0EB3DAB3C74DEA6A08";

    private static string Sha256Of(bool azure, bool capture)
    {
        var context = new CollectorContext
        {
            ServerId = 42,
            ServerName = "test-server",
            CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
            Deltas = new NoDeltas(),
            Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
            CapturePlanXml = capture,
        };

        var text = QueryStatsCollector.Instance.BuildQuery(context).Text.ReplaceLineEndings("\n");
        return Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text)));
    }

    [Fact]
    public void Standard_CaptureOff_IsTheLiteText_ByteForByte()
        => Assert.Equal(StandardCaptureOffSha256, Sha256Of(azure: false, capture: false));

    [Fact]
    public void AzureSqlDb_CaptureOff_IsTheLiteText_ByteForByte()
        => Assert.Equal(AzureSqlDbCaptureOffSha256, Sha256Of(azure: true, capture: false));

    [Fact]
    public void Standard_CaptureOn_InlineFetch_IsTodaysDarlingText_ByteForByte()
        => Assert.Equal(StandardCaptureOnSha256, Sha256Of(azure: false, capture: true));

    [Fact]
    public void AzureSqlDb_CaptureOn_InlineFetch_IsTodaysDarlingText_ByteForByte()
        => Assert.Equal(AzureSqlDbCaptureOnSha256, Sha256Of(azure: true, capture: true));

    /// <summary>BuildQuery never reads the calculator; the context just requires one.</summary>
    private sealed class NoDeltas : ICollectorDeltaCalculator
    {
        public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
            DateTime? collectionTime = null, int maxGapSeconds = 0) => currentValue;

        public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
            out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        {
            intervalSeconds = 0;
            return currentValue;
        }
    }
}
