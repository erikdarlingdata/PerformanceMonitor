/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using PerformanceMonitor.Collectors;

namespace Lite.Tests.Helpers;

/// <summary>
/// Shared fakes for collector-definition parity tests (headless plan v5.1): an in-memory
/// DbDataReader (multi-result-set capable), a recording row writer, and a recording delta
/// calculator that returns value × 10 so delta emissions are distinguishable from raw values.
/// </summary>
internal sealed class FakeCollectorDataReader : DbDataReader
{
    private readonly object[][][] _sets;
    private int _setIndex;
    private int _rowIndex = -1;

    /// <summary>Single result set.</summary>
    public FakeCollectorDataReader(params object[][] rows) => _sets = new[] { rows };

    private FakeCollectorDataReader(object[][][] sets) => _sets = sets;

    /// <summary>Multiple result sets (e.g. the tempdb_stats two-SELECT batch).</summary>
    public static FakeCollectorDataReader WithResultSets(params object[][][] sets) => new(sets);

    private object[][] CurrentSet => _sets[_setIndex];

    public override bool Read() => ++_rowIndex < CurrentSet.Length;

    public override bool NextResult()
    {
        if (_setIndex + 1 >= _sets.Length)
        {
            return false;
        }

        _setIndex++;
        _rowIndex = -1;
        return true;
    }

    public override string GetString(int ordinal) => (string)CurrentSet[_rowIndex][ordinal];

    public override long GetInt64(int ordinal) => (long)CurrentSet[_rowIndex][ordinal];

    public override int GetInt32(int ordinal) => (int)CurrentSet[_rowIndex][ordinal];

    public override short GetInt16(int ordinal) => (short)CurrentSet[_rowIndex][ordinal];

    public override decimal GetDecimal(int ordinal) => (decimal)CurrentSet[_rowIndex][ordinal];

    public override double GetDouble(int ordinal) => (double)CurrentSet[_rowIndex][ordinal];

    public override DateTime GetDateTime(int ordinal) => (DateTime)CurrentSet[_rowIndex][ordinal];

    public override bool GetBoolean(int ordinal) => (bool)CurrentSet[_rowIndex][ordinal];

    public override bool IsDBNull(int ordinal) => CurrentSet[_rowIndex][ordinal] is DBNull;

    public override object GetValue(int ordinal) => CurrentSet[_rowIndex][ordinal];

    public override int FieldCount => CurrentSet.Length == 0 ? 0 : CurrentSet[0].Length;

    public override bool HasRows => CurrentSet.Length > 0;

    public override bool IsClosed => false;

    public override int Depth => 0;

    public override int RecordsAffected => -1;

    public override object this[int ordinal] => CurrentSet[_rowIndex][ordinal];

    public override object this[string name] => throw new NotSupportedException();

    public override byte GetByte(int ordinal) => (byte)CurrentSet[_rowIndex][ordinal];
    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
    public override char GetChar(int ordinal) => (char)CurrentSet[_rowIndex][ordinal];
    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length) => throw new NotSupportedException();
    public override string GetDataTypeName(int ordinal) => throw new NotSupportedException();
    public override IEnumerator GetEnumerator() => CurrentSet.GetEnumerator();
    public override Type GetFieldType(int ordinal) => CurrentSet[_rowIndex][ordinal].GetType();
    public override float GetFloat(int ordinal) => (float)CurrentSet[_rowIndex][ordinal];
    public override Guid GetGuid(int ordinal) => (Guid)CurrentSet[_rowIndex][ordinal];
    public override string GetName(int ordinal) => throw new NotSupportedException();
    public override int GetOrdinal(string name) => throw new NotSupportedException();
    public override int GetValues(object[] values) => throw new NotSupportedException();
}

internal sealed class RecordingCollectorRowWriter : ICollectorRowWriter
{
    public List<object?> Values { get; } = new();

    public ICollectorRowWriter Value(string? value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(long value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(long? value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(int value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(int? value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(short value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(short? value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(double value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(double? value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(decimal value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(decimal? value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(bool value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(bool? value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(DateTime value) { Values.Add(value); return this; }
    public ICollectorRowWriter Value(DateTime? value) { Values.Add(value); return this; }
    public ICollectorRowWriter NullValue() { Values.Add(null); return this; }
}

internal sealed class RecordingCollectorDeltaCalculator : ICollectorDeltaCalculator
{
    public List<(string Group, string Key, long Value, DateTime? Time, int MaxGap)> Calls { get; } = new();

    public int LastServerId { get; private set; }

    /// <summary>What <see cref="CalculateDeltaWithInterval"/> hands back. Left at 0 so every existing
    /// pin is unaffected; set it to something distinctive to prove a collector writes the MEASURED
    /// interval rather than a constant of its own (#2234, where perfmon wrote a literal 60).</summary>
    public int ReportedInterval { get; set; }

    public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
        DateTime? collectionTime = null, int maxGapSeconds = 0)
    {
        LastServerId = serverId;
        Calls.Add((collectorName, key, currentValue, collectionTime, maxGapSeconds));
        return currentValue * 10;
    }

    public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
        out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
    {
        intervalSeconds = ReportedInterval;
        return CalculateDelta(serverId, collectorName, key, currentValue, collectionTime, maxGapSeconds);
    }

    /// <summary>
    /// Series ages seen by <see cref="CalculateDeltaWithSeriesAge"/> (#2235), in call order.
    ///
    /// <para>Overridden rather than left to the interface's default implementation on purpose: the default
    /// forwards to <see cref="CalculateDeltaWithInterval"/> and DROPS the age, so a collector that stopped
    /// passing it would keep every existing pin green. Recording it here is what makes that regression
    /// visible.</para>
    /// </summary>
    public List<int?> SeriesAges { get; } = new();

    public long CalculateDeltaWithSeriesAge(int serverId, string collectorName, string key, long currentValue,
        int? seriesAgeSeconds, out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
    {
        SeriesAges.Add(seriesAgeSeconds);
        return CalculateDeltaWithInterval(serverId, collectorName, key, currentValue, out intervalSeconds,
            collectionTime, maxGapSeconds);
    }
}
