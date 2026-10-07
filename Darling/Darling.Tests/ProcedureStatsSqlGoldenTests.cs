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
/// Byte-for-byte pins on the full <c>procedure_stats</c> SQL text a host sends (#5158, #5449). The main query is
/// numbers only on every host in every plan mode (#5449), so one hash per variant covers all of them and the modes are
/// asserted equal to it; the plan phase's second query is pinned for each mode (Off: plan, Shadow: plan and identity,
/// On: identity). The text is hashed after normalizing line endings. To move one of these on purpose, run the test and
/// copy the actual hash from the failure message.
/// </summary>
public sealed class ProcedureStatsSqlGoldenTests
{
    private const string StandardMainSha256 = "D17F6086018221BDD3A9FC86613D3DCF7FE6E6FC22B57B5AE448D9DF8CB80CC8";
    private const string AzureSqlDbMainSha256 = "49949F221E2694D5B64985931C02D42006DDDCC068DBBFB7F69B206EC22C851F";
    private const string PlanPhaseOffSha256 = "3FF53D1CD3D34AE923BB95B316B0F108F45CDE0CBDB2441F2C39062F95796566";
    private const string PlanPhaseShadowSha256 = "62209CDCF1C50BB084FE61E728956A3C5D83EC34ECBD713FB33E82B11EE89BE1";
    private const string PlanPhaseOnSha256 = "7AC301A9C279899632C2332A9F7BFBD149DBB6D2D81CAC416A527E242E620D5E";

    private static readonly byte[][] Handles = { new byte[] { 0x05, 0x00, 0xAB }, new byte[] { 0x06, 0x00, 0xCD, 0xEF } };

    private static CollectorContext ContextFor(bool azure, bool capture, bool defer = false, bool identity = false) => new()
    {
        ServerId = 42,
        ServerName = "test-server",
        CollectionTime = new DateTime(2026, 7, 2, 12, 0, 0, DateTimeKind.Utc),
        Deltas = new NoDeltas(),
        Target = new CollectorTargetInfo { IsAzureSqlDb = azure },
        CapturePlanXml = capture,
        DeferPlanXmlFetch = defer,
        PlanIdentityColumns = identity,
    };

    private static string Sha256OfText(string text) =>
        Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(text.ReplaceLineEndings("\n"))));

    /* Assert.Equal truncates a long string in its message; this puts the whole actual hash in it. */
    private static void Check(string expected, string actual) => Assert.True(expected == actual, "actual hash: " + actual);

    private static string MainSha256(bool azure, bool capture, bool defer = false, bool identity = false) =>
        Sha256OfText(ProcedureStatsCollector.Instance.BuildQuery(ContextFor(azure, capture, defer, identity)).Text);

    [Fact]
    public void Standard_MainQuery_IsTheNumbersOnlyText_ByteForByte()
        => Check(StandardMainSha256, MainSha256(azure: false, capture: false));

    [Fact]
    public void AzureSqlDb_MainQuery_IsTheNumbersOnlyText_ByteForByte()
        => Check(AzureSqlDbMainSha256, MainSha256(azure: true, capture: false));

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void TheMainQuery_IsTheSameText_InEveryPlanMode(bool azure)
    {
        var off = MainSha256(azure, capture: false);
        Assert.Equal(off, MainSha256(azure, capture: true));
        Assert.Equal(off, MainSha256(azure, capture: true, identity: true));
        Assert.Equal(off, MainSha256(azure, capture: true, defer: true, identity: true));
        Assert.Equal(off, MainSha256(azure, capture: false, identity: true));
    }

    [Fact]
    public void PlanPhase_Off_IsThePlanFragmentOverAValuesList_ByteForByte()
        => Check(PlanPhaseOffSha256, Sha256OfText(
            ProcedureStatsCollector.BuildPlanPhaseQuery(ContextFor(false, capture: true), Handles).Text));

    [Fact]
    public void PlanPhase_Shadow_IsThePlanAndIdentityFragments_ByteForByte()
        => Check(PlanPhaseShadowSha256, Sha256OfText(
            ProcedureStatsCollector.BuildPlanPhaseQuery(ContextFor(false, capture: true, identity: true), Handles).Text));

    [Fact]
    public void PlanPhase_On_IsTheIdentityFragmentAlone_ByteForByte()
        => Check(PlanPhaseOnSha256, Sha256OfText(
            ProcedureStatsCollector.BuildPlanPhaseQuery(ContextFor(false, capture: true, defer: true, identity: true), Handles).Text));

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
