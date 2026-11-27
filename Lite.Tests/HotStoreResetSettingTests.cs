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
    [InlineData(2048)]
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
    [InlineData(3000, 2048)]
    [InlineData(99999999999, 2048)]
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
        Assert.Equal(2048, limit.Mb);
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

    /// <summary>The size check (#9238). The data in the store counts, not the file: the file keeps its freed blocks and runs well
    /// past its data. A store whose data is under the limit is left alone however large its file is, up to twice the limit.</summary>
    [Theory]
    [InlineData(300, 540, 512, false)]     // data under the limit, file over it: the old check reset here
    [InlineData(511.9, 700, 512, false)]
    [InlineData(512, 540, 512, true)]      // data at the limit
    [InlineData(600, 700, 512, true)]      // data over the limit
    [InlineData(300, 1023.9, 512, false)]  // file just under twice the limit
    [InlineData(300, 1024, 512, true)]     // file at twice the limit resets whatever the data
    [InlineData(300, 1500, 512, true)]
    [InlineData(1500, 1900, 2048, false)]
    public void TheSizeCheck_CountsTheDataInTheStore_AndBoundsTheFileAtTwiceTheLimit(double usedMb, double fileMb, double limitMb, bool due)
    {
        var limit = new HotStoreResetLimit(limitMb, "setting");

        Assert.Equal(due, limit.IsReached(usedMb, fileMb));
    }

    /// <summary>A failed or timed-out used read decides nothing: the file length is not the data. Only the file's own ceiling, twice
    /// the reset point, resets without the data read.</summary>
    [Theory]
    [InlineData(520, 512, false)]
    [InlineData(1023.9, 512, false)]
    [InlineData(1024, 512, true)]
    [InlineData(1100, 512, true)]
    [InlineData(2047.9, 1024, false)]
    [InlineData(2048, 1024, true)]
    public void TheSizeCheck_WhenTheUsedReadIsNull_OnlyTheFileCeilingResets(double fileMb, double limitMb, bool due)
    {
        var limit = new HotStoreResetLimit(limitMb, "setting");

        Assert.Equal(due, limit.IsReached(null, fileMb));
    }

    [Fact]
    public void TheSizeCheck_OfASettingOf1024_LeavesAFileAndDataThatWouldResetTheDefaultAlone()
    {
        var fromSetting = Resolve("{\"hot_store_reset_mb\": 1024}", out _);
        var fromDefault = Resolve("{}", out _);

        Assert.False(fromSetting.IsReached(520, 600));
        Assert.True(fromDefault.IsReached(520, 600));
        Assert.False(fromSetting.IsReached(null, 1030));
        Assert.True(fromDefault.IsReached(null, 1030));
        Assert.True(fromSetting.IsReached(null, 2050));
    }
}
