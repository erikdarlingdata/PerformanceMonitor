/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Collectors;

namespace Darling.Tests;

/// <summary>
/// #4348: an <see cref="ICollectorRowWriter"/> that remembers every value a definition's <c>WritePayload</c> wrote, in
/// order, so the collection census can plant a canary in the collector's input, run the read and the write, and
/// assert on what was written: no written value holds a secret needle, and a plain statement is the SAME instance the
/// reader returned. A payload written through <see cref="ICollectorRowWriter.PayloadOrDigest"/> is recorded as its
/// content (the interface's default), with the digest the caller offered kept beside it.
/// </summary>
internal sealed class StatementScrubRecordingWriter : ICollectorRowWriter
{
    private readonly List<object?> _values = new();
    private readonly List<(string? Content, string? KnownDigest)> _payloads = new();

    /// <summary>Every value written, in order. A null is a null column.</summary>
    public IReadOnlyList<object?> Values => _values;

    /// <summary>Every <c>PayloadOrDigest</c> call: the content and the digest the caller already knew.</summary>
    public IReadOnlyList<(string? Content, string? KnownDigest)> Payloads => _payloads;

    /// <summary>The string values written, in order.</summary>
    public IEnumerable<string> Strings => _values.OfType<string>();

    /// <summary>True when any written string contains <paramref name="needle"/> (ordinal).</summary>
    public bool AnyStringContains(string needle) =>
        Strings.Any(s => s.Contains(needle, StringComparison.Ordinal));

    private ICollectorRowWriter Add(object? v)
    {
        _values.Add(v);
        return this;
    }

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

    public ICollectorRowWriter PayloadOrDigest(string? content, string? knownDigest)
    {
        _payloads.Add((content, knownDigest));
        return Add(content);
    }
}
