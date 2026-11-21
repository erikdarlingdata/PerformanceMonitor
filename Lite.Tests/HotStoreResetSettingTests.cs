using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9238: the hot store reset point is the <c>hot_store_reset_mb</c> setting, 512 MB by default, clamped to a floor and a
/// ceiling, and logged once when it was clamped. None of these tests installs the limit as the process-wide value, so they do not
/// move the one the other store tests read.
/// </summary>
[Collection("app-logger-statics")]
public sealed class HotStoreResetSettingTests : IDisposable
{
    public HotStoreResetSettingTests()
    {
        AppLogger.SetMinimumLevel(LogLevel.Information);
        AppLogger.DrainBufferedLines();
    }

    public void Dispose() => AppLogger.SetMinimumLevel(AppLogger.DefaultMinimumLevel);

    private static HotStoreResetLimit Resolve(string? text, out string? note) => HotStoreResetSetting.Resolve(text, out note);

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("{}")]
    [InlineData("{\"other_key\": 1}")]
    [InlineData("[1, 2]")]
    [InlineData("{ not json")]
    public void NoSetting_IsTheDefault512_FromTheDefault(string? text)
    {
        var limit = Resolve(text, out var note);

        Assert.Equal(512, limit.Mb);
        Assert.Equal("default", limit.Source);
        Assert.Null(note);
    }

    [Fact]
    public void AValidSetting_IsUsed_AndSaysItCameFromTheSetting()
    {
        var limit = Resolve("{\"hot_store_reset_mb\": 768}", out var note);

        Assert.Equal(768, limit.Mb);
        Assert.Equal("setting", limit.Source);
        Assert.Null(note);
    }

    [Theory]
    [InlineData(128)]
    [InlineData(1024)]
    public void TheBoundsThemselves_AreAccepted_WithoutANote(int mb)
    {
        var limit = Resolve($"{{\"hot_store_reset_mb\": {mb}}}", out var note);

        Assert.Equal(mb, limit.Mb);
        Assert.Equal("setting", limit.Source);
        Assert.Null(note);
    }

    [Theory]
    [InlineData(64, 128)]
    [InlineData(0, 128)]
    [InlineData(-5, 128)]
    [InlineData(2048, 1024)]
    [InlineData(99999999999, 1024)]
    public void AValueOutOfRange_IsClampedToTheNearestBound_AndSaysSo(long given, double expected)
    {
        var limit = Resolve($"{{\"hot_store_reset_mb\": {given}}}", out var note);

        Assert.Equal(expected, limit.Mb);
        Assert.Equal("setting", limit.Source);
        Assert.NotNull(note);
        Assert.Contains("hot_store_reset_mb", note);
    }

    [Theory]
    [InlineData("\"big\"")]
    [InlineData("null")]
    [InlineData("true")]
    public void AValueThatIsNotANumber_IsTheDefault_AndSaysSo(string value)
    {
        var limit = Resolve($"{{\"hot_store_reset_mb\": {value}}}", out var note);

        Assert.Equal(512, limit.Mb);
        Assert.Equal("default", limit.Source);
        Assert.NotNull(note);
    }

    [Fact]
    public void AClampedValue_IsLoggedOnce()
    {
        var limit = HotStoreResetSetting.ResolveAndLog("{\"hot_store_reset_mb\": 4096}");

        var lines = AppLogger.DrainBufferedLines().Where(l => l.Contains("hot_store_reset_mb", StringComparison.Ordinal)).ToList();
        Assert.Equal(1024, limit.Mb);
        var line = Assert.Single(lines);
        Assert.Contains("WARN", line, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void AnInRangeValue_IsLoggedOnceAsInformation_NotAsAClamp()
    {
        HotStoreResetSetting.ResolveAndLog("{\"hot_store_reset_mb\": 640}");

        var lines = AppLogger.DrainBufferedLines().Where(l => l.Contains("hot store reset point", StringComparison.Ordinal)).ToList();
        var line = Assert.Single(lines);
        Assert.Contains("640", line);
        Assert.DoesNotContain("outside", line);
    }

    /// <summary>The size check. A store just over the 512 MB default is due a reset, and with a 1024 MB setting it is not until it
    /// passes 1024 MB.</summary>
    [Theory]
    [InlineData(520, 512, true)]
    [InlineData(511.9, 512, false)]
    [InlineData(520, 1024, false)]
    [InlineData(1023.9, 1024, false)]
    [InlineData(1024, 1024, true)]
    [InlineData(1100, 1024, true)]
    public void TheSizeCheck_UsesTheEffectiveLimit(double fileMb, double limitMb, bool due)
    {
        var limit = new HotStoreResetLimit(limitMb, "setting");

        Assert.Equal(due, limit.IsReached(fileMb));
    }

    [Fact]
    public void TheSizeCheck_OfASettingOf1024_LeavesA520MbStoreAlone_WhereTheDefaultWouldReset()
    {
        var fromSetting = Resolve("{\"hot_store_reset_mb\": 1024}", out _);
        var fromDefault = Resolve("{}", out _);

        Assert.False(fromSetting.IsReached(520));
        Assert.True(fromDefault.IsReached(520));
        Assert.True(fromSetting.IsReached(1030));
    }
}
