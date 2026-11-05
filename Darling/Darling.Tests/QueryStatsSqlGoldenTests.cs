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
    private const string StandardCaptureOffSha256 = "7E2EC442D569F84393550F93374C2F8F2C747B0088C66DCD0514EA96A67D01CE";
    private const string AzureSqlDbCaptureOffSha256 = "2078F82E7FD4036F9C0035C1BCFABE124938F74AC57523D09A7A5BA00666C3A5";
    private const string StandardCaptureOnSha256 = "D237398D0744AB6C3EFA57026E5AA961BB8B1CD37B04FBE396389D5F91F91600";
    private const string AzureSqlDbCaptureOnSha256 = "B265E41F8223B19921B220A40EDC0841C9786F8B5208C5344BAAF73168DA565F";

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
