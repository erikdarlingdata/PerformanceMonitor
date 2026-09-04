using System;
using System.Collections;
using System.Data.Common;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// A pass-through <see cref="DbDataReader"/> that counts what the drain actually delivered (#2864).
///
/// <para><b>Why a wrapper and not a counter in each collector.</b> Sixty-six collectors write their own
/// <c>while (await reader.ReadAsync(...))</c> loop, so the alternative was editing every one of them and
/// hoping the sixty-seventh remembers. Decorating the reader instead means a collector cannot opt out, cannot
/// forget, and cannot drift — the count comes from the same call the collector already makes. Nothing in
/// <c>PerformanceMonitor.Collectors</c> casts a reader to a provider type (verified before this was written),
/// so wrapping is transparent to every consumer.</para>
///
/// <para><b>What it exists to answer.</b> A cycle abandoned by the #2673 wall-clock budget stores
/// <c>rows_collected = 0</c>, and that is rows STORED — an abandoned run ships nothing by definition. So the
/// stored row could not distinguish "the target never sent row 1" from "the target sent 149 rows and then
/// stopped", which is precisely the split between a target that could not execute and a stream that stalled.
/// The first production capture with V108's phases showed <c>open:104ms drain:119,945ms rows=0</c> and could
/// go no further: the phase split proved the time was in the drain, and nothing could say whether the drain
/// was slow or simply silent.</para>
///
/// <para><b><see cref="LastReadElapsedMs"/> is the diagnostic, not the row count.</b> Rows-read alone still
/// cannot separate "streaming steadily but slowly" from "delivered everything then hung": both end at the
/// budget with a positive count. The elapsed reading at the LAST successful read does — subtract it from the
/// drain and you have how long the reader sat with nothing arriving. A run that delivered 149 rows in 500 ms
/// and then waited 119 s is a stalled stream; one that dribbled 149 rows across the whole budget is a slow
/// one, and they want different fixes.</para>
///
/// <para><b>Bytes are the string payload only, and the name says so.</b> <see cref="PayloadBytes"/> counts
/// UTF-16 bytes returned by the string and binary getters — it is NOT the wire size, which no
/// <see cref="DbDataReader"/> exposes, and it excludes numeric columns and protocol framing entirely. That is
/// the honest scope and it is the useful one: the collectors this instrumentation exists for are dominated by
/// one enormous text column (plan XML at a 260 KB mean), so string bytes ARE the payload to within a rounding
/// error, and a count that pretended to be the wire size would be a worse number wearing a better name.</para>
///
/// <para><b>Cost.</b> One increment and one <see cref="Stopwatch"/> read per row, plus a length add per string
/// column. The stopwatch is the caller's, already running for the drain phase it measures, so this adds no
/// timer of its own.</para>
/// </summary>
[System.Diagnostics.CodeAnalysis.SuppressMessage(
    "Design", "CA1010:Generic interface should also be implemented",
    Justification = "The non-generic IEnumerable comes from DbDataReader itself, which this type decorates. " +
                    "Adding IEnumerable<T> would give the wrapper a shape its inner reader does not have.")]
public sealed class DrainCountingDataReader : DbDataReader
{
    private readonly DbDataReader _inner;
    private readonly Stopwatch _drainWatch;

    /// <param name="inner">The provider's reader. Every member forwards to it unchanged.</param>
    /// <param name="drainWatch">
    /// The already-running stopwatch measuring the drain phase. Shared rather than owned so
    /// <see cref="LastReadElapsedMs"/> is on the same clock as the <c>drain:</c> figure it is subtracted
    /// from — two stopwatches started microseconds apart would make "time since last row" a difference of
    /// two nearly-equal numbers from different origins, which is how a silent skew becomes a wrong answer.
    /// </param>
    public DrainCountingDataReader(DbDataReader inner, Stopwatch drainWatch)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _drainWatch = drainWatch ?? throw new ArgumentNullException(nameof(drainWatch));
    }

    /// <summary>Rows the reader actually handed back — successful <c>Read</c>/<c>ReadAsync</c> calls only.</summary>
    public long RowsRead { get; private set; }

    /// <summary>
    /// UTF-16 bytes returned by the string and binary getters. See the type remarks: string payload, not wire
    /// size, and deliberately named for what it counts.
    /// </summary>
    public long PayloadBytes { get; private set; }

    /// <summary>
    /// The drain stopwatch's reading at the last successful read, or -1 when no row was ever returned.
    ///
    /// <para>-1 rather than 0 because 0 is a real, reachable answer — a reader whose first row arrives
    /// instantly — and the two cases lead opposite ways: "row 1 arrived at t=0" says the target answered,
    /// "no row ever arrived" says it did not. A sentinel that collides with a legitimate measurement is how
    /// an absence gets read as a fast success, which is the failure this whole change exists to end.</para>
    /// </summary>
    public long LastReadElapsedMs { get; private set; } = -1;

    private bool ObserveRead(bool read)
    {
        if (read)
        {
            RowsRead++;
            LastReadElapsedMs = _drainWatch.ElapsedMilliseconds;
        }

        return read;
    }

    private string ObserveString(string value)
    {
        PayloadBytes += (long)value.Length * sizeof(char);
        return value;
    }

    public override bool Read() => ObserveRead(_inner.Read());

    public override async Task<bool> ReadAsync(CancellationToken cancellationToken) =>
        ObserveRead(await _inner.ReadAsync(cancellationToken).ConfigureAwait(false));

    public override string GetString(int ordinal) => ObserveString(_inner.GetString(ordinal));

    public override object GetValue(int ordinal)
    {
        var value = _inner.GetValue(ordinal);
        if (value is string s)
        {
            PayloadBytes += (long)s.Length * sizeof(char);
        }

        return value;
    }

    public override T GetFieldValue<T>(int ordinal)
    {
        var value = _inner.GetFieldValue<T>(ordinal);
        if (value is string s)
        {
            PayloadBytes += (long)s.Length * sizeof(char);
        }

        return value;
    }

    public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
    {
        var read = _inner.GetBytes(ordinal, dataOffset, buffer, bufferOffset, length);
        PayloadBytes += read;
        return read;
    }

    public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
    {
        var read = _inner.GetChars(ordinal, dataOffset, buffer, bufferOffset, length);
        PayloadBytes += read * sizeof(char);
        return read;
    }

    /* Everything below is straight forwarding. Present so the wrapper cannot silently fall back to
       DbDataReader's default implementations, which route through GetValue and would double-count a string
       column read via a typed getter. */
    public override object this[int ordinal] => _inner[ordinal];
    public override object this[string name] => _inner[name];
    public override int Depth => _inner.Depth;
    public override int FieldCount => _inner.FieldCount;
    public override bool HasRows => _inner.HasRows;
    public override bool IsClosed => _inner.IsClosed;
    public override int RecordsAffected => _inner.RecordsAffected;
    public override int VisibleFieldCount => _inner.VisibleFieldCount;
    public override bool GetBoolean(int ordinal) => _inner.GetBoolean(ordinal);
    public override byte GetByte(int ordinal) => _inner.GetByte(ordinal);
    public override char GetChar(int ordinal) => _inner.GetChar(ordinal);
    public override string GetDataTypeName(int ordinal) => _inner.GetDataTypeName(ordinal);
    public override DateTime GetDateTime(int ordinal) => _inner.GetDateTime(ordinal);
    public override decimal GetDecimal(int ordinal) => _inner.GetDecimal(ordinal);
    public override double GetDouble(int ordinal) => _inner.GetDouble(ordinal);
    public override IEnumerator GetEnumerator() => _inner.GetEnumerator();
    public override Type GetFieldType(int ordinal) => _inner.GetFieldType(ordinal);
    public override float GetFloat(int ordinal) => _inner.GetFloat(ordinal);
    public override Guid GetGuid(int ordinal) => _inner.GetGuid(ordinal);
    public override short GetInt16(int ordinal) => _inner.GetInt16(ordinal);
    public override int GetInt32(int ordinal) => _inner.GetInt32(ordinal);
    public override long GetInt64(int ordinal) => _inner.GetInt64(ordinal);
    public override string GetName(int ordinal) => _inner.GetName(ordinal);
    public override int GetOrdinal(string name) => _inner.GetOrdinal(name);
    public override Stream GetStream(int ordinal) => _inner.GetStream(ordinal);
    public override TextReader GetTextReader(int ordinal) => _inner.GetTextReader(ordinal);
    public override int GetValues(object[] values) => _inner.GetValues(values);
    public override bool IsDBNull(int ordinal) => _inner.IsDBNull(ordinal);
    public override Task<bool> IsDBNullAsync(int ordinal, CancellationToken cancellationToken) => _inner.IsDBNullAsync(ordinal, cancellationToken);
    public override bool NextResult() => _inner.NextResult();
    public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => _inner.NextResultAsync(cancellationToken);
    public override System.Data.DataTable? GetSchemaTable() => _inner.GetSchemaTable();

    /* Close/Dispose do NOT reach the inner reader: the caller owns it through its own `using`, and closing it
       from here would close it twice — harmless on today's providers, but it makes the wrapper an owner of
       something it was only ever handed. base.Dispose still runs, which on DbDataReader means Close(), and
       Close() above is the no-op that keeps that from touching the inner reader. */
    public override void Close() { }

    protected override void Dispose(bool disposing) => base.Dispose(disposing);
}
