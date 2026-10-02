/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <see cref="PgSettingValue"/> (#3542 step 2): the one piece of arithmetic in the PostgreSQL-target analysis
/// nobody had written before — normalising a <c>pg_settings</c> row's text by its unit — pinned exhaustively,
/// because every <c>CONFIG_PG_*</c> bar in the scorer compares against what this returns. The cases are the
/// real rows: <c>shared_buffers = 16384 / 8kB</c> IS 128 MB; <c>work_mem = 4096 / kB</c> IS 4 MB;
/// <c>max_wal_size = 1024 / MB</c> IS 1 GB; <c>checkpoint_timeout = 300 / s</c> IS 300 000 ms; and
/// <c>-1</c> in any unit is <c>-1</c>, never <c>-8192</c>.
/// </summary>
public sealed class PgSettingValueTests
{
    /* ── memory ── */

    [Theory]
    [InlineData("16384", "8kB", 134_217_728L)]        // shared_buffers default: 16384 blocks = 128 MB
    [InlineData("1024", "8kB", 8_388_608L)]           // the 8 MB boot_val fallback
    [InlineData("524288", "8kB", 4_294_967_296L)]     // effective_cache_size default: 4 GB
    [InlineData("4096", "kB", 4_194_304L)]            // work_mem default: 4 MB
    [InlineData("65536", "kB", 67_108_864L)]          // maintenance_work_mem default: 64 MB
    [InlineData("1024", "MB", 1_073_741_824L)]        // max_wal_size default: 1 GB
    [InlineData("80", "MB", 83_886_080L)]             // min_wal_size default
    [InlineData("2", "GB", 2_147_483_648L)]
    [InlineData("1", "TB", 1_099_511_627_776L)]
    [InlineData("16777216", "B", 16_777_216L)]        // wal_segment_size: unit B
    [InlineData("100", "16kB", 1_638_400L)]           // a 16 kB block build
    [InlineData("3", "32kB", 98_304L)]
    [InlineData("0", "8kB", 0L)]
    public void ToBytes_NormalisesEveryMemoryUnit_IncludingBlockMultiples(string setting, string unit, long expected)
    {
        Assert.Equal(expected, PgSettingValue.ToBytes(setting, unit));
    }

    [Theory]
    [InlineData("-1", "kB")]
    [InlineData("-1", "8kB")]
    [InlineData("-1", "MB")]
    [InlineData("-1", null)]
    [InlineData(" -1 ", "kB")]
    public void ToBytes_PreservesTheMinusOneSentinel_InEveryUnit(string setting, string? unit)
    {
        /* max_slot_wal_keep_size = -1 is "unbounded"; multiplied by its unit it would read as minus one kilobyte. */
        Assert.Equal(-1L, PgSettingValue.ToBytes(setting, unit));
    }

    [Theory]
    [InlineData("128MB", null, 134_217_728L)]
    [InlineData("128 MB", null, 134_217_728L)]
    [InlineData("1GB", null, 1_073_741_824L)]
    [InlineData("4096kB", null, 4_194_304L)]
    [InlineData("128MB", "8kB", 1_048_576L)]         // the unit COLUMN wins over the rendered suffix: 128 blocks of 8 kB, not 128 MB
    public void ToBytes_HonoursAnEmbeddedUnit_WhenTheRowHasNone_AndTheColumnWinsWhenBothExist(string setting, string? unit, long expected)
    {
        Assert.Equal(expected, PgSettingValue.ToBytes(setting, unit));
    }

    [Theory]
    [InlineData("16384", "KB", 16_777_216L)]
    [InlineData("16384", "kb", 16_777_216L)]
    [InlineData("2", "gb", 2_147_483_648L)]
    [InlineData("100", "8KB", 819_200L)]
    public void ToBytes_IsCaseInsensitiveOnTheUnitLetters(string setting, string unit, long expected)
    {
        Assert.Equal(expected, PgSettingValue.ToBytes(setting, unit));
    }

    [Theory]
    [InlineData(null, "kB")]
    [InlineData("", "kB")]
    [InlineData("   ", "kB")]
    [InlineData("abc", "kB")]
    [InlineData("16384", null)]        // a number with no unit at all is not bytes
    [InlineData("16384", "")]
    [InlineData("16384", "ms")]        // a time unit is not a memory unit
    [InlineData("16384", "pages")]     // an unknown unit is unknown, not bytes
    [InlineData("on", "kB")]
    [InlineData("1e400", "kB")]        // infinity is not a size
    public void ToBytes_ReturnsNull_ForAnythingItCannotHonestlyNormalise(string? setting, string? unit)
    {
        Assert.Null(PgSettingValue.ToBytes(setting, unit));
    }

    /* ── time ── */

    [Theory]
    [InlineData("300", "s", 300_000.0)]        // checkpoint_timeout default
    [InlineData("200", "ms", 200.0)]           // bgwriter_delay default
    [InlineData("1", "min", 60_000.0)]         // autovacuum_naptime in min
    [InlineData("60", "s", 60_000.0)]
    [InlineData("1", "h", 3_600_000.0)]
    [InlineData("1", "d", 86_400_000.0)]
    [InlineData("1500", "us", 1.5)]
    [InlineData("2", "ms", 2.0)]               // autovacuum_vacuum_cost_delay: a real
    [InlineData("2.5", "ms", 2.5)]
    [InlineData("0", "ms", 0.0)]
    public void ToMs_NormalisesEveryTimeUnit(string setting, string unit, double expected)
    {
        Assert.Equal(expected, PgSettingValue.ToMs(setting, unit)!.Value, precision: 9);
    }

    [Theory]
    [InlineData("-1", "ms")]
    [InlineData("-1", "s")]
    [InlineData("-1", "min")]
    public void ToMs_PreservesTheMinusOneSentinel(string setting, string unit)
    {
        /* log_min_duration_statement = -1 is "disabled"; in seconds it would read as minus a thousand ms. */
        Assert.Equal(-1.0, PgSettingValue.ToMs(setting, unit));
    }

    [Theory]
    [InlineData("300", "s", 300.0)]
    [InlineData("500", "ms", 0.5)]
    [InlineData("5", "min", 300.0)]
    [InlineData("-1", "s", -1.0)]
    public void ToSeconds_IsToMsOverAThousand_WithTheSentinelPreserved(string setting, string unit, double expected)
    {
        Assert.Equal(expected, PgSettingValue.ToSeconds(setting, unit)!.Value, precision: 9);
    }

    [Theory]
    [InlineData("300", "kB")]      // a memory unit is not a time unit
    [InlineData("300", null)]
    [InlineData("soon", "s")]
    [InlineData(null, "s")]
    public void ToMs_ReturnsNull_ForAnythingItCannotHonestlyNormalise(string? setting, string? unit)
    {
        Assert.Null(PgSettingValue.ToMs(setting, unit));
    }

    /* ── numbers and booleans ── */

    [Theory]
    [InlineData("4", 4.0)]
    [InlineData("1.1", 1.1)]
    [InlineData("0.9", 0.9)]
    [InlineData("100", 100.0)]
    [InlineData(" 3 ", 3.0)]
    [InlineData("-1", -1.0)]
    public void ToNumber_ParsesInvariantly(string setting, double expected)
    {
        Assert.Equal(expected, PgSettingValue.ToNumber(setting)!.Value, precision: 12);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("on")]
    [InlineData("1,5")]     // a comma is not the invariant decimal separator
    [InlineData("NaN")]
    public void ToNumber_ReturnsNull_ForNonNumbers(string? setting)
    {
        Assert.Null(PgSettingValue.ToNumber(setting));
    }

    [Theory]
    [InlineData("on", true)]
    [InlineData("off", false)]
    [InlineData("ON", true)]
    [InlineData("true", true)]
    [InlineData("false", false)]
    [InlineData("yes", true)]
    [InlineData("no", false)]
    [InlineData("1", true)]
    [InlineData("0", false)]
    public void ToBool_AcceptsEverySpellingTheEngineAccepts(string setting, bool expected)
    {
        Assert.Equal(expected, PgSettingValue.ToBool(setting));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("lz4")]     // wal_compression grew an enum: the caller decides
    [InlineData("pglz")]
    [InlineData("maybe")]
    public void ToBool_ReturnsNull_ForNonBooleans(string? setting)
    {
        Assert.Null(PgSettingValue.ToBool(setting));
    }

    /* ── rendering ── */

    [Theory]
    [InlineData(134_217_728L, "128 MB")]
    [InlineData(1_073_741_824L, "1 GB")]
    [InlineData(4_294_967_296L, "4 GB")]
    [InlineData(1_610_612_736L, "1.5 GB")]
    [InlineData(4_194_304L, "4 MB")]
    [InlineData(8_192L, "8 kB")]
    [InlineData(512L, "512 B")]
    [InlineData(0L, "0 B")]
    [InlineData(-1L, "-1")]
    public void FormatBytes_WritesTheUnitAnOperatorWouldPutInPostgresqlConf(long bytes, string expected)
    {
        Assert.Equal(expected, PgSettingValue.FormatBytes(bytes));
    }

    [Fact]
    public void TheDefaultBlockSize_IsEightKilobytes_AndTheUnitStringCarriesTheMultiple()
    {
        Assert.Equal(8192L, PgSettingValue.DefaultBlockBytes);
        /* The constant is documentation; the arithmetic reads the multiple from the unit, so a 16 kB build
           normalises without it changing. */
        Assert.Equal(2 * PgSettingValue.DefaultBlockBytes, PgSettingValue.ToBytes("1", "16kB"));
    }
}
