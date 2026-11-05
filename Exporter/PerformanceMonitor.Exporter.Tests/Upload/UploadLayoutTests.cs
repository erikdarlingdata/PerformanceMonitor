using PerformanceMonitor.Exporter.Upload;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests.Upload;

/// <summary>The S3 key tests the exporter has always had, ported onto <see cref="UploadSettings.FromUri"/> + <see cref="UploadLayout"/>.</summary>
public class UploadLayoutTests
{
    private static UploadSettings.S3 S3(string uri) => Assert.IsType<UploadSettings.S3>(UploadSettings.FromUri(uri, null));

    [Fact]
    public void Parse_reads_bucket_and_prefix()
    {
        var target = S3("s3://example-bucket/exports");

        Assert.Equal("example-bucket", target.Bucket);
        Assert.Equal("exports", target.Prefix);
    }

    [Fact]
    public void Parse_strips_a_trailing_slash_from_the_prefix() => Assert.Equal("exports", S3("s3://bucket/exports/").Prefix);

    [Fact]
    public void Parse_allows_an_empty_prefix() => Assert.Equal("", S3("s3://bucket").Prefix);

    [Fact]
    public void Parse_allows_a_multi_segment_prefix() => Assert.Equal("a/b/c", S3("s3://bucket/a/b/c").Prefix);

    [Fact]
    public void Parse_rejects_a_uri_without_a_known_scheme() =>
        Assert.Throws<ArgumentException>(() => UploadSettings.FromUri("https://bucket/exports", null));

    [Fact]
    public void Parse_rejects_an_empty_bucket() => Assert.Throws<ArgumentException>(() => UploadSettings.FromUri("s3:///exports", null));

    [Fact]
    public void ParquetKey_joins_prefix_store_and_relative_path_with_forward_slash_only()
    {
        var key = S3("s3://bucket/exports").Layout.ParquetKey("store_a", "plan_shape/plan_shape__store_a__20260926T0800Z__20260926T0900Z.parquet");

        Assert.Equal("exports/store_a/plan_shape/plan_shape__store_a__20260926T0800Z__20260926T0900Z.parquet", key);
        Assert.DoesNotContain('\\', key);
    }

    [Fact]
    public void ParquetKey_omits_the_prefix_segment_when_the_prefix_is_empty()
    {
        var key = S3("s3://bucket").Layout.ParquetKey("store_a", "plan_shape/plan_shape__store_a__x__y.parquet");

        Assert.Equal("store_a/plan_shape/plan_shape__store_a__x__y.parquet", key);
    }

    [Fact]
    public void ManifestKey_puts_the_manifest_under_underscore_manifests()
    {
        var key = S3("s3://bucket/exports").Layout.ManifestKey("store_a", "store_a__20260926T0800Z__20260926T0900Z.json");

        Assert.Equal("exports/store_a/_manifests/store_a__20260926T0800Z__20260926T0900Z.json", key);
        Assert.DoesNotContain('\\', key);
    }

    [Fact]
    public void Hive_store_folder_is_off_by_default_so_keys_are_unchanged()
    {
        Assert.False(new UploadLayout("p").HiveStoreFolder);
        Assert.Equal("p/store_a/t/f.parquet", new UploadLayout("p").ParquetKey("store_a", "t/f.parquet"));
    }

    [Fact]
    public void Hive_store_folder_renames_the_store_folder_for_parquet_and_manifest_keys()
    {
        var layout = new UploadLayout("p", HiveStoreFolder: true);

        Assert.Equal("p/store=store_a/t/f.parquet", layout.ParquetKey("store_a", "t/f.parquet"));
        Assert.Equal("p/store=store_a/_manifests/m.json", layout.ManifestKey("store_a", "m.json"));
        Assert.Equal("store=store_a/t/f.parquet", new UploadLayout("", true).ParquetKey("store_a", "t/f.parquet"));
    }

    [Fact]
    public void A_trailing_slash_on_the_prefix_never_doubles_the_separator() =>
        Assert.Equal("p/store_a/t/f.parquet", new UploadLayout("p/").ParquetKey("store_a", "t/f.parquet"));
}
