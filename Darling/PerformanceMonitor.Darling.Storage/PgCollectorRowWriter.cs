/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Text;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Adapts the collector definitions' positional row writes onto Postgres binary COPY — Darling's
/// counterpart of Lite's DuckDB appender adapter. The host opens one binary import per
/// collection batch (<see cref="CopyCommandFor"/>), calls StartRow + writes the prefix columns,
/// then hands the writer to the definition's WritePayload; column ORDER is the contract, exactly
/// as with the appender.
/// All DateTimes in the product are UTC by convention but stored in naive `timestamp` columns
/// (mirroring Lite's DuckDB TIMESTAMP), so every DateTime is written with
/// DateTimeKind.Unspecified: since Npgsql 6.0 a Kind=Utc DateTime maps strictly to timestamptz
/// and throws InvalidCastException against `timestamp without time zone`
/// (https://www.npgsql.org/doc/types/datetime.html).
/// </summary>
public sealed class PgCollectorRowWriter : ICollectorRowWriter
{
    /// <summary>The active binary importer; the host sets this once per COPY batch.</summary>
    public NpgsqlBinaryImporter? Importer { get; set; }

    private NpgsqlBinaryImporter Target
        => Importer ?? throw new InvalidOperationException("Importer not set — open a binary import first.");

    private IReadOnlyDictionary<int, PayloadDimensions.PayloadDimension> _diversionPlan =
        new Dictionary<int, PayloadDimensions.PayloadDimension>();

    private PayloadDimensionBatch? _dimensions;

    private int _payloadIndex;

    /// <summary>
    /// Routes the large text payloads of this collector into the hash-keyed dimension tables instead
    /// of inline onto every row (#1767). The host sets this once per COPY batch, from the SAME
    /// schema it built <see cref="CopyCommandFor(ICollectorSchemaInfo)"/> from — the plan decides
    /// which payload POSITIONS carry a digest, and the COPY column list must name the digest columns
    /// at exactly those positions. Leaving it unset writes every payload inline, which is the
    /// pre-#1767 behaviour and remains correct for every collector that declares no dimension.
    /// </summary>
    public void UseDimensions(
        IReadOnlyDictionary<int, PayloadDimensions.PayloadDimension> diversionPlan,
        PayloadDimensionBatch dimensions)
    {
        _diversionPlan = diversionPlan ?? throw new ArgumentNullException(nameof(diversionPlan));
        _dimensions = dimensions ?? throw new ArgumentNullException(nameof(dimensions));
    }

    /// <summary>
    /// Opens one row's payload run: the diversion plan is indexed by PAYLOAD column ordinal, so the
    /// counter must restart after the prefix columns (which the host writes through this same
    /// writer). Call immediately before the definition's WritePayload.
    /// </summary>
    public void BeginPayload() => _payloadIndex = 0;

    /// <summary>
    /// Closes one row's payload run and asserts the positional contract the whole binary COPY rests
    /// on: the definition must have written exactly one value per declared payload column. A drift
    /// here silently shifts every later column by one — and with a diversion plan active it would
    /// hash the WRONG column and write a digest where the store expects text. Checked for every
    /// collector, not just diverted ones, because the failure mode without it is an opaque Npgsql
    /// type error several columns downstream of the actual mistake.
    /// </summary>
    public void EndPayload(int expectedPayloadColumns)
    {
        if (_payloadIndex != expectedPayloadColumns)
        {
            throw new InvalidOperationException(
                $"Collector wrote {_payloadIndex} payload values but declares {expectedPayloadColumns} payload " +
                "columns — WritePayload and PayloadColumns must stay in lockstep; column ORDER is the COPY contract.");
        }
    }

    /// <summary>Maps an engine-neutral collector column type to its binary-COPY parameter type.</summary>
    public static NpgsqlDbType DbTypeFor(CollectorColumnType type)
    {
        switch (type)
        {
            case CollectorColumnType.BigInt: return NpgsqlDbType.Bigint;
            case CollectorColumnType.Integer: return NpgsqlDbType.Integer;
            case CollectorColumnType.SmallInt: return NpgsqlDbType.Smallint;
            case CollectorColumnType.Varchar: return NpgsqlDbType.Text;
            case CollectorColumnType.Timestamp: return NpgsqlDbType.Timestamp;
            case CollectorColumnType.Double: return NpgsqlDbType.Double;
            case CollectorColumnType.Decimal: return NpgsqlDbType.Numeric;
            case CollectorColumnType.Boolean: return NpgsqlDbType.Boolean;
            default:
                throw new ArgumentOutOfRangeException(nameof(type), type, "Unmapped collector column type");
        }
    }

    /// <summary>
    /// The binary COPY command for one collector's destination table, prefix columns first in
    /// the exact order the host writes them (id column omitted for running_jobs' no-id prefix).
    ///
    /// <para>A payload column that diverts into a dimension table (#1767) is named by its DIGEST
    /// column instead, in place, so the list still lines up one-for-one with the definition's
    /// positional WritePayload. The diversion is derived from the schema here rather than taken as
    /// an argument, so the command and the writer cannot be built from different plans: any caller
    /// that opens a COPY from this string gets the deduplicating shape automatically, and there is
    /// no overload left that quietly reinstates inline payload writes. Physical column order in the
    /// table is irrelevant — the list is explicit.</para>
    /// </summary>
    public static string CopyCommandFor(ICollectorSchemaInfo schema)
    {
        if (schema is null)
        {
            throw new ArgumentNullException(nameof(schema));
        }

        var diversionPlan = PayloadDimensions.DiversionPlanFor(schema);

        var sb = new StringBuilder();
        sb.Append("COPY ").Append(schema.TargetTable).Append(" (");

        if (schema.IncludesCollectionId)
        {
            sb.Append(schema.PrefixIdColumnName).Append(", ");
        }

        sb.Append(schema.PrefixTimeColumnName).Append(", server_id, server_name");

        for (var i = 0; i < schema.PayloadColumns.Count; i++)
        {
            sb.Append(", ").Append(
                diversionPlan.TryGetValue(i, out var dimension)
                    ? dimension.DigestColumn
                    : schema.PayloadColumns[i].Name);
        }

        sb.Append(") FROM STDIN (FORMAT BINARY)");
        return sb.ToString();
    }

    /// <summary>Naive-UTC storage: strip the Kind so Npgsql accepts the value for `timestamp`.</summary>
    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    /// <summary>
    /// Postgres `text` cannot hold NUL (0x00), which SQL Server NVARCHAR allows — one NUL-laden
    /// query text from dm_exec_sql_text fails the whole COPY batch with 22021 "invalid byte
    /// sequence for encoding UTF8: 0x00" (#1614). Every collector string funnels through
    /// <see cref="Value(string?)"/>, so stripping here covers all of them. String.Replace returns
    /// the original instance when nothing matches, so clean strings (the vast majority) don't allocate.
    /// </summary>
    public static string StripEmbeddedNuls(string value) => value.Replace("\0", string.Empty);

    /// <summary>
    /// Consumes one payload position. EVERY value-writing entry point calls this exactly once, so
    /// the ordinal stays aligned with the definition's declared payload columns — the diversion plan
    /// and <see cref="EndPayload"/> both key off it.
    /// </summary>
    private int NextPayloadIndex() => _payloadIndex++;

    /// <summary>
    /// Writes a text payload, diverting it into its dimension table when this position declares one:
    /// the row stores the CONTENT DIGEST and the text itself is accumulated for the batch's dim
    /// upsert, so identical text collected every cycle is stored once instead of per row (#1767).
    /// NULs are stripped BEFORE hashing so the digest keys exactly the bytes the dim will hold.
    /// A NULL payload stays NULL — there is nothing to dedupe and no dim row to point at.
    /// </summary>
    public ICollectorRowWriter Value(string? value)
    {
        var index = NextPayloadIndex();

        if (_diversionPlan.Count > 0 && _diversionPlan.TryGetValue(index, out var dimension))
        {
            if (value is null)
            {
                Target.WriteNull();
                return this;
            }

            var payload = StripEmbeddedNuls(value);
            var digest = PayloadDimensions.Digest(payload);

            var dimensions = _dimensions ?? throw new InvalidOperationException(
                "A diversion plan is set but no dimension batch — call UseDimensions with both.");
            dimensions.Add(dimension.DimTable, digest, payload);

            Target.Write(digest, NpgsqlDbType.Bytea);
            return this;
        }

        if (value is null) { Target.WriteNull(); } else { Target.Write(StripEmbeddedNuls(value), NpgsqlDbType.Text); }
        return this;
    }

    /// <summary>
    /// A payload the host already knows the store holds: when <paramref name="knownDigest"/> is set and
    /// <paramref name="content"/> is null, the row's digest column gets that digest directly, with no plan
    /// text and no dim insert, and the digest joins the batch's touch set so the flush keeps the dim row
    /// alive. Anything else is exactly <see cref="Value(string?)"/>. <paramref name="knownDigest"/> is the
    /// 64-character hex of the SHA-256 <see cref="PayloadDimensions.Digest"/> produced; a malformed one
    /// throws rather than writing a digest that resolves to nothing.
    /// </summary>
    public ICollectorRowWriter PayloadOrDigest(string? content, string? knownDigest)
    {
        if (knownDigest is null || content is not null)
        {
            return Value(content);
        }

        var digest = ParseKnownDigest(knownDigest);
        if (!_diversionPlan.TryGetValue(_payloadIndex, out var dimension))
        {
            return Value(content);
        }

        var dimensions = _dimensions ?? throw new InvalidOperationException(
            "A diversion plan is set but no dimension batch — call UseDimensions with both.");
        NextPayloadIndex();
        dimensions.AddTouch(dimension.DimTable, digest);
        Target.Write(digest, NpgsqlDbType.Bytea);
        return this;
    }

    private static byte[] ParseKnownDigest(string knownDigest)
    {
        byte[] digest;
        try
        {
            digest = Convert.FromHexString(knownDigest);
        }
        catch (FormatException ex)
        {
            throw new ArgumentException("A known payload digest must be hex text.", nameof(knownDigest), ex);
        }

        if (digest.Length != PayloadDimensions.DigestLengthBytes)
        {
            throw new ArgumentException(
                $"A known payload digest must be {PayloadDimensions.DigestLengthBytes} bytes, got {digest.Length}.",
                nameof(knownDigest));
        }

        return digest;
    }

    public ICollectorRowWriter Value(long value)
    {
        NextPayloadIndex();
        Target.Write(value, NpgsqlDbType.Bigint);
        return this;
    }

    public ICollectorRowWriter Value(long? value)
    {
        NextPayloadIndex();
        if (value is null) { Target.WriteNull(); } else { Target.Write(value.Value, NpgsqlDbType.Bigint); }
        return this;
    }

    public ICollectorRowWriter Value(int value)
    {
        NextPayloadIndex();
        Target.Write(value, NpgsqlDbType.Integer);
        return this;
    }

    public ICollectorRowWriter Value(int? value)
    {
        NextPayloadIndex();
        if (value is null) { Target.WriteNull(); } else { Target.Write(value.Value, NpgsqlDbType.Integer); }
        return this;
    }

    public ICollectorRowWriter Value(short value)
    {
        NextPayloadIndex();
        Target.Write(value, NpgsqlDbType.Smallint);
        return this;
    }

    public ICollectorRowWriter Value(short? value)
    {
        NextPayloadIndex();
        if (value is null) { Target.WriteNull(); } else { Target.Write(value.Value, NpgsqlDbType.Smallint); }
        return this;
    }

    public ICollectorRowWriter Value(double value)
    {
        NextPayloadIndex();
        Target.Write(value, NpgsqlDbType.Double);
        return this;
    }

    public ICollectorRowWriter Value(double? value)
    {
        NextPayloadIndex();
        if (value is null) { Target.WriteNull(); } else { Target.Write(value.Value, NpgsqlDbType.Double); }
        return this;
    }

    public ICollectorRowWriter Value(decimal value)
    {
        NextPayloadIndex();
        Target.Write(value, NpgsqlDbType.Numeric);
        return this;
    }

    public ICollectorRowWriter Value(decimal? value)
    {
        NextPayloadIndex();
        if (value is null) { Target.WriteNull(); } else { Target.Write(value.Value, NpgsqlDbType.Numeric); }
        return this;
    }

    public ICollectorRowWriter Value(bool value)
    {
        NextPayloadIndex();
        Target.Write(value, NpgsqlDbType.Boolean);
        return this;
    }

    public ICollectorRowWriter Value(bool? value)
    {
        NextPayloadIndex();
        if (value is null) { Target.WriteNull(); } else { Target.Write(value.Value, NpgsqlDbType.Boolean); }
        return this;
    }

    public ICollectorRowWriter Value(DateTime value)
    {
        NextPayloadIndex();
        Target.Write(Naive(value), NpgsqlDbType.Timestamp);
        return this;
    }

    public ICollectorRowWriter Value(DateTime? value)
    {
        NextPayloadIndex();
        if (value is null) { Target.WriteNull(); } else { Target.Write(Naive(value.Value), NpgsqlDbType.Timestamp); }
        return this;
    }

    public ICollectorRowWriter NullValue()
    {
        NextPayloadIndex();
        Target.WriteNull();
        return this;
    }
}
