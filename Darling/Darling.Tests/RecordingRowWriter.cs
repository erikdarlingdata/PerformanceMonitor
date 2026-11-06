/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Collectors;

namespace Darling.Tests;

/// <summary>A row writer that records each value a collector's <c>WritePayload</c> hands it, in payload-column order, so a test reads a stored column by position.</summary>
internal sealed class RecordingRowWriter : ICollectorRowWriter
{
    public List<object?> Values { get; } = new();
    private ICollectorRowWriter Add(object? v) { Values.Add(v); return this; }
    public ICollectorRowWriter Value(string? value) => Add(value);
    public ICollectorRowWriter Value(long value) => Add(value);
    public ICollectorRowWriter Value(long? value) => Add(value);
    public ICollectorRowWriter Value(int value) => Add(value);
    public ICollectorRowWriter Value(int? value) => Add(value);
    public ICollectorRowWriter Value(short value) => Add(value);
    public ICollectorRowWriter Value(short? value) => Add(value);
    public ICollectorRowWriter Value(double value) => Add(value);
    public ICollectorRowWriter Value(double? value) => Add(value);
    public ICollectorRowWriter Value(decimal value) => Add(value);
    public ICollectorRowWriter Value(decimal? value) => Add(value);
    public ICollectorRowWriter Value(bool value) => Add(value);
    public ICollectorRowWriter Value(bool? value) => Add(value);
    public ICollectorRowWriter Value(DateTime value) => Add(value);
    public ICollectorRowWriter Value(DateTime? value) => Add(value);
    public ICollectorRowWriter NullValue() => Add(null);
}
