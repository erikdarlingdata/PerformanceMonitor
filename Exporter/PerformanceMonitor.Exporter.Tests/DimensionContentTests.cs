using System.IO.Compression;
using System.Text;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

public class DimensionContentTests
{
    [Fact]
    public void ToHexDigest_formats_lowercase_hex()
    {
        Assert.Equal("deadbeef", DimensionContent.ToHexDigest([0xDE, 0xAD, 0xBE, 0xEF]));
    }

    [Fact]
    public void ResolvePlanXml_prefers_the_xml_column_when_present()
    {
        var result = DimensionContent.ResolvePlanXml("<plan/>", Gzip("<other/>"));
        Assert.Equal("<plan/>", result);
    }

    [Fact]
    public void ResolvePlanXml_falls_back_to_gunzipping_the_gz_column()
    {
        var result = DimensionContent.ResolvePlanXml(null, Gzip("<plan/>"));
        Assert.Equal("<plan/>", result);
    }

    [Fact]
    public void ResolvePlanXml_is_null_when_both_columns_are_missing()
    {
        Assert.Null(DimensionContent.ResolvePlanXml(null, null));
    }

    [Fact]
    public void GunzipUtf8_round_trips_utf8_text()
    {
        var text = "<QueryPlan xmlns=\"urn:test\">select 1;</QueryPlan>";
        Assert.Equal(text, DimensionContent.GunzipUtf8(Gzip(text)));
    }

    [Fact]
    public void Batch_splits_into_batches_of_the_requested_size_preserving_order()
    {
        var digests = Enumerable.Range(0, 1201).Select(i => BitConverter.GetBytes(i)).ToList();

        var batches = DimensionContent.Batch(digests, 500).ToList();

        Assert.Equal(3, batches.Count);
        Assert.Equal(500, batches[0].Count);
        Assert.Equal(500, batches[1].Count);
        Assert.Equal(201, batches[2].Count);
        Assert.Equal(digests[0], batches[0][0]);
        Assert.Equal(digests[1200], batches[2][^1]);
    }

    [Fact]
    public void Batch_of_an_exact_multiple_has_no_trailing_empty_batch()
    {
        var digests = Enumerable.Range(0, 1000).Select(i => BitConverter.GetBytes(i)).ToList();

        var batches = DimensionContent.Batch(digests, 500).ToList();

        Assert.Equal(2, batches.Count);
    }

    [Fact]
    public void Batch_rejects_a_non_positive_batch_size()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => DimensionContent.Batch([[0x01]], 0).ToList());
    }

    private static byte[] Gzip(string text)
    {
        using var output = new MemoryStream();
        using (var gzip = new GZipStream(output, CompressionMode.Compress, leaveOpen: true))
        {
            var bytes = Encoding.UTF8.GetBytes(text);
            gzip.Write(bytes, 0, bytes.Length);
        }
        return output.ToArray();
    }
}
