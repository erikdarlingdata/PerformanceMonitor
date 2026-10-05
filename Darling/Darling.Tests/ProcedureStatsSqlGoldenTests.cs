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
/// Byte-for-byte pins on the full <c>procedure_stats</c> SQL text a host sends today (#5158). Capture OFF is
/// Lite's text; capture ON with the fetch inline is today's Darling text. The deferred-fetch mode is
/// new and is pinned structurally in <see cref="ProcedureStatsPlanFetchTests"/>. The text is hashed after
/// normalizing line endings. To move one of these on purpose, run the test and copy the actual hash from
/// the failure message.
/// </summary>
public sealed class ProcedureStatsSqlGoldenTests
{
    private const string StandardCaptureOffSha256 = "E9A83752EE21D8E2F418EF31BD6684FC402A680F3A010B926FDA166A92924080";
    private const string AzureSqlDbCaptureOffSha256 = "9BFB38A709D0CC549857AEEE4841853E127D44D2F0950E07FBD1758D58B5A755";
    private const string StandardCaptureOnSha256 = "6CA8B83A2C682EA62EF0A4BE0CEC7B2263DE57ABF396D1D48C0458A2672B446B";
    private const string AzureSqlDbCaptureOnSha256 = "B2F6396CF2CCD1FC6BF4720BB5ECD71711C9999DDDBAC46932FBBADF387ED35E";

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

        var text = ProcedureStatsCollector.Instance.BuildQuery(context).Text.ReplaceLineEndings("\n");
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
