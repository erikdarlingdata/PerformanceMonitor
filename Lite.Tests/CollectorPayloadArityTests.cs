/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Lite.Tests.Helpers;
using PerformanceMonitor.Collectors;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Every collector writes exactly as many payload values as it declares payload columns.
///
/// <para><b>The runtime already checks this — and that was not enough.</b> <c>EndPayload</c> throws when the
/// counts disagree, but only when the collector actually RUNS against a real target. #2599 added
/// <c>database_name</c> to <c>pg_extension_availability</c>'s columns, its SQL, its row and its reader, and
/// missed <c>WritePayload</c>. The collector is DAILY, so it did not re-run after the upgrade that shipped
/// the change, and the store simply had no rows from it — which looks exactly like a quiet server. It was
/// found by pointing the service at a live self-hosted PostgreSQL, three schema versions later.</para>
///
/// <para>A build-time check turns that into a compile-cycle failure. The columns and the writes are two
/// halves of one statement about the table; nothing but a test holds them together.</para>
/// </summary>
public class CollectorPayloadArityTests
{
    /// <summary>Counts writes and accepts anything, so a collector's own value types never matter here.</summary>
    private sealed class CountingWriter : ICollectorRowWriter
    {
        public int Count { get; private set; }

        private ICollectorRowWriter Bump()
        {
            Count++;
            return this;
        }

        public ICollectorRowWriter Value(string? value) => Bump();
        public ICollectorRowWriter Value(long value) => Bump();
        public ICollectorRowWriter Value(long? value) => Bump();
        public ICollectorRowWriter Value(int value) => Bump();
        public ICollectorRowWriter Value(int? value) => Bump();
        public ICollectorRowWriter Value(short value) => Bump();
        public ICollectorRowWriter Value(short? value) => Bump();
        public ICollectorRowWriter Value(double value) => Bump();
        public ICollectorRowWriter Value(double? value) => Bump();
        public ICollectorRowWriter Value(decimal value) => Bump();
        public ICollectorRowWriter Value(decimal? value) => Bump();
        public ICollectorRowWriter Value(bool value) => Bump();
        public ICollectorRowWriter Value(bool? value) => Bump();
        public ICollectorRowWriter Value(DateTime value) => Bump();
        public ICollectorRowWriter Value(DateTime? value) => Bump();
        public ICollectorRowWriter Value(byte[]? value) => Bump();
        public ICollectorRowWriter NullValue() => Bump();

        public void BeginPayload() => Count = 0;
        public void EndPayload(int expectedPayloadColumns) { }
    }

    [Fact]
    public void EveryCollectorWritesAsManyValuesAsItDeclaresColumns()
    {
        var mismatches = new List<string>();
        var skipped = new List<string>();

        foreach (var schema in CollectorCatalog.All)
        {
            var write = schema.GetType().GetMethods(BindingFlags.Public | BindingFlags.Instance)
                .FirstOrDefault(m => m.Name == "WritePayload" && m.GetParameters().Length == 3);

            if (write is null)
            {
                skipped.Add(schema.Name);
                continue;
            }

            var rowType = write.GetParameters()[0].ParameterType;

            /* A default row is enough: this counts WRITES, not values. A collector that branched on row
               CONTENT to decide how many values to write would already be broken against the fixed column
               list, so the default is not a weaker test — it is the same test. */
            var row = rowType.IsValueType ? Activator.CreateInstance(rowType) : null;

            if (row is null)
            {
                skipped.Add(schema.Name);
                continue;
            }

            var writer = new CountingWriter();
            writer.BeginPayload();

            try
            {
                write.Invoke(schema, new[] { row, writer, MakeContext() });
            }
            catch (TargetInvocationException)
            {
                /* A default row is not valid for every collector - the delta-based SQL Server ones
                   dereference fields the default leaves null. That is the PROBE being too blunt for
                   those, not a defect in them, so they count as uncovered rather than as failures. The
                   coverage assertion below is what keeps that from quietly swallowing the catalog. */
                skipped.Add(schema.Name);
                continue;
            }

            if (writer.Count != schema.PayloadColumns.Count)
            {
                mismatches.Add(
                    $"{schema.Name}: writes {writer.Count} value(s) but declares "
                    + $"{schema.PayloadColumns.Count} column(s)");
            }
        }

        Assert.True(
            mismatches.Count == 0,
            "Collector(s) write a different number of payload values than they declare columns. Every row "
            + "they produce will be rejected at EndPayload, and a collector that runs rarely can hide that "
            + "for a long time (#2599 hid for three schema versions): "
            + string.Join(" | ", mismatches));

        /* The guard is only worth having if it actually covered the catalog. */
        Assert.True(
            skipped.Count < CollectorCatalog.All.Count / 4,
            $"{skipped.Count} of {CollectorCatalog.All.Count} collectors could not be exercised, so this "
            + "guard is covering far less than it appears to: " + string.Join(", ", skipped));
    }

    private static CollectorContext MakeContext()
        => new()
        {
            ServerId = 1,
            ServerName = "arity-probe",
            CollectionTime = new DateTime(2026, 8, 26, 0, 0, 0, DateTimeKind.Utc),
            Deltas = new RecordingCollectorDeltaCalculator(),
            Target = new CollectorTargetInfo(),
        };
}
