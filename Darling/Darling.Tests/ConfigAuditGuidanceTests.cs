using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7400: the config audit suggested a negative max server memory on a server under 4 GB of RAM
/// (<c>total - max(4096, 10%)</c> with no floor), and its above-guidance MAXDOP note named logical processors in a NUMA node
/// while the number came from cores per socket. One shared helper now serves the Darling audit, the Lite audit and the max
/// memory fact card, so these tests drive the helper and the card text.
/// </summary>
public sealed class ConfigAuditGuidanceTests
{
    /// <summary>The reserve is the larger of 4,096 MB and 10% of RAM, never more than half of RAM. At 8 GB and above it is the
    /// old reserve; below that it is half of RAM, so the suggestion is positive and under the RAM.</summary>
    [Theory]
    [InlineData(1023, 511.5, 511)]
    [InlineData(2048, 1024, 1024)]
    [InlineData(4096, 2048, 2048)]
    [InlineData(8192, 4096, 4096)]
    [InlineData(65536, 6553.6, 58982)]
    public void OsReserve_AndSuggestion_AreCappedAtHalfOfRam_AndUnchangedFrom8Gb(double totalMb, double expectedReserve, int expectedSuggested)
    {
        Assert.Equal(expectedReserve, ConfigAuditGuidance.OsReserveMb(totalMb), 3);
        Assert.Equal(expectedSuggested, ConfigAuditGuidance.SuggestedMaxMemoryMb(totalMb));
        Assert.True(ConfigAuditGuidance.SuggestedMaxMemoryMb(totalMb) > 0);
    }

    [Fact]
    public void OsReserve_OfUnknownRam_IsZero()
    {
        Assert.Equal(0, ConfigAuditGuidance.OsReserveMb(0));
        Assert.Equal(0, ConfigAuditGuidance.SuggestedMaxMemoryMb(0));
    }

    /// <summary>The audit row for the server in the report: 92,160 MB configured on 1,023 MB of RAM read "reduce to ~-3,073 MB".</summary>
    [Fact]
    public void AuditMaxMemory_WithMaxAboveRamOnA1023MbServer_ReducesToAPositiveFigure()
    {
        var audit = ConfigAuditGuidance.AuditMaxMemory(92160, 1023);

        Assert.Equal("review", audit.Status);
        Assert.Equal(511, audit.Suggested);
        Assert.Contains("Consider reducing to ~511 MB", audit.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("~-", audit.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void AuditMaxMemory_AtTheDefaultOnA1023MbServer_StatesTheReserveItUsed()
    {
        var audit = ConfigAuditGuidance.AuditMaxMemory(2147483647, 1023);

        Assert.Equal("warning", audit.Status);
        Assert.Equal(511, audit.Suggested);
        Assert.Contains("~511 MB (leaving 512 MB for the OS)", audit.Message, StringComparison.Ordinal);
    }

    /// <summary>At or under the smaller suggestion there is nothing to reduce: the row is ok and keeps the configured value.</summary>
    [Theory]
    [InlineData(511)]
    [InlineData(400)]
    public void AuditMaxMemory_AtOrUnderTheSuggestionOnA1023MbServer_IsOk(int maxMemory)
    {
        var audit = ConfigAuditGuidance.AuditMaxMemory(maxMemory, 1023);

        Assert.Equal("ok", audit.Status);
        Assert.Equal(maxMemory, audit.Suggested);
        Assert.DoesNotContain("reduc", audit.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Nothing changes at 8 GB and above: the same suggestion and sentence the audit always gave.</summary>
    [Fact]
    public void AuditMaxMemory_On64GbServer_IsTheOldFigureAndSentence()
    {
        var atDefault = ConfigAuditGuidance.AuditMaxMemory(2147483647, 65536);
        Assert.Equal(58982, atDefault.Suggested);
        Assert.Contains("~58,982 MB (leaving 6,554 MB for the OS)", atDefault.Message, StringComparison.Ordinal);

        var over = ConfigAuditGuidance.AuditMaxMemory(65536, 65536);
        Assert.Equal("review", over.Status);
        Assert.Contains("Consider reducing to ~58,982 MB to leave room for the OS.", over.Message, StringComparison.Ordinal);

        Assert.Equal("ok", ConfigAuditGuidance.AuditMaxMemory(58982, 65536).Status);
    }

    /// <summary>With no RAM collected the text states the rule, which now includes the half-of-RAM cap.</summary>
    [Fact]
    public void AuditMaxMemory_WithNoRamCollected_StatesTheRule()
    {
        var audit = ConfigAuditGuidance.AuditMaxMemory(2147483647, 0);

        Assert.Equal("warning", audit.Status);
        Assert.Contains("the larger of 4 GB and 10% of RAM, but never more than half of RAM", audit.Message, StringComparison.Ordinal);
    }

    /// <summary>The max memory fact card (Darling and Lite share it) states the same figure and the reserve it used.</summary>
    [Fact]
    public void MaxMemoryFactCard_OnA1023MbServer_SuggestsAPositiveCap_AndStatesTheReserve()
    {
        var facts = new Dictionary<string, Fact>
        {
            ["MEMORY_TOTAL_PHYSICAL_MB"] = new() { Source = "memory", Key = "MEMORY_TOTAL_PHYSICAL_MB", Value = 1023 },
            ["CONFIG_MAX_MEMORY_MB"] = new() { Source = "config", Key = "CONFIG_MAX_MEMORY_MB", Value = 2147483647 },
        };

        var advice = FactAdvice.Compose("CONFIG_MAX_MEMORY_MB", facts)!;

        Assert.Contains("~511 MB (1,023 MB total minus 512 MB for the OS)", advice.Remediation, StringComparison.Ordinal);
        Assert.Contains("'max server memory (MB)', 511", advice.Remediation, StringComparison.Ordinal);
    }

    /// <summary>The above-guidance MAXDOP note names the figure it used, as the MAXDOP 0 warning does, including the vCores case.</summary>
    [Theory]
    [InlineData(false, "this server's cores-per-socket")]
    [InlineData(true, "this database's vCores")]
    public void MaxdopMessages_NameTheFigureTheyUsed_TheSameWayInBothBranches(bool fromVcores, string expectedSource)
    {
        var basis = new FactRemediation.MaxdopBasis(4, fromVcores);

        var above = ConfigAuditGuidance.MaxdopAboveMessage(8, FactRemediation.RecommendedMaxdop(basis.Cores), basis);
        var unlimited = ConfigAuditGuidance.MaxdopUnlimitedMessage(FactRemediation.RecommendedMaxdop(basis.Cores), basis);

        Assert.Contains($"above the topology-based guidance of 4 ({expectedSource}, capped at 8)", above, StringComparison.Ordinal);
        Assert.Contains($"Start with 4 ({expectedSource}, capped at 8)", unlimited, StringComparison.Ordinal);
        Assert.DoesNotContain("logical processors", above, StringComparison.Ordinal);
    }
}
