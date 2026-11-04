using System.IO.Compression;
using System.Text;

namespace PerformanceMonitor.Exporter;

/// <summary>
/// Small pure helpers used by <see cref="DimensionTableExporter"/>: hex formatting of a
/// digest, decompressing the gzip fallback column, and splitting a set of digests into
/// batches for the `WHERE digest = ANY(@digests)` lookup.
/// </summary>
public static class DimensionContent
{
    /// <summary>Formats a digest as lowercase hex, the form recorded in the output tables' example_digest column.</summary>
    public static string ToHexDigest(byte[] digest) => Convert.ToHexStringLower(digest);

    /// <summary>
    /// Resolves a plan_dim row's content: the XML column if present, else gunzip of the gz
    /// column (standard gzip of the UTF-8 XML text), else null if both are missing (the
    /// dimension row itself was never fetched, or both columns are NULL, which should not
    /// happen for a row that exists but is treated as missing content either way).
    /// </summary>
    public static string? ResolvePlanXml(string? xml, byte[]? gzipped)
    {
        if (xml is not null)
        {
            return xml;
        }

        if (gzipped is not null)
        {
            return GunzipUtf8(gzipped);
        }

        return null;
    }

    /// <summary>Decompresses a standard gzip byte buffer to UTF-8 text.</summary>
    public static string GunzipUtf8(byte[] gzipped)
    {
        using var input = new MemoryStream(gzipped);
        using var gzip = new GZipStream(input, CompressionMode.Decompress);
        using var output = new MemoryStream();
        gzip.CopyTo(output);
        return Encoding.UTF8.GetString(output.ToArray());
    }

    /// <summary>
    /// Splits a sequence of digests into batches of at most <paramref name="batchSize"/>,
    /// preserving order, for the digest content lookup query (kept small so the store never
    /// gets a query with an unbounded IN/ANY list).
    /// </summary>
    public static IEnumerable<List<byte[]>> Batch(IEnumerable<byte[]> digests, int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "batchSize must be positive");
        }

        var batch = new List<byte[]>(batchSize);
        foreach (var digest in digests)
        {
            batch.Add(digest);
            if (batch.Count == batchSize)
            {
                yield return batch;
                batch = new List<byte[]>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            yield return batch;
        }
    }

    /// <summary>Same batching as <see cref="Batch"/>, generic over any element type (used for batching Query Store keys rather than raw digests).</summary>
    public static IEnumerable<List<T>> BatchGeneric<T>(IEnumerable<T> items, int batchSize)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize), "batchSize must be positive");
        }

        var batch = new List<T>(batchSize);
        foreach (var item in items)
        {
            batch.Add(item);
            if (batch.Count == batchSize)
            {
                yield return batch;
                batch = new List<T>(batchSize);
            }
        }

        if (batch.Count > 0)
        {
            yield return batch;
        }
    }
}
