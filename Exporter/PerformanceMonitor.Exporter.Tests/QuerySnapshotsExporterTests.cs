using System.Security.Cryptography;
using System.Text;
using PerformanceMonitor.Exporter;
using Xunit;

namespace PerformanceMonitor.Exporter.Tests;

/// <summary>
/// Unit tests for lane O5's query_snapshots exporter (docs/optional-tables.md §7, roadmap item 7 group 7b).
/// </summary>
public class QuerySnapshotsExporterTests
{
    /// <summary>
    /// Darling's own digest recipe (PayloadDimensions.Digest + PgCollectorRowWriter.StripEmbeddedNuls),
    /// copied here as an independent oracle: SHA-256 of the UTF-8 bytes of the payload with embedded NULs
    /// stripped, and nothing else. If QuerySnapshotsExporter.DarlingDigest ever drifts from this, the
    /// coordinator's decision 2 (§7 digests) is broken and `query_snapshots` rows would no longer be
    /// comparable to a real `query_plan_dim.digest`.
    /// </summary>
    private static byte[] OracleDigest(string payload) =>
        SHA256.HashData(Encoding.UTF8.GetBytes(payload.Replace("\0", string.Empty)));

    [Fact]
    public void DarlingDigest_matches_the_oracle_recipe_for_plain_xml()
    {
        const string xml = "<ShowPlanXML><BatchSequence><Batch><Statements/></Batch></BatchSequence></ShowPlanXML>";

        var actual = QuerySnapshotsExporter.DarlingDigest(xml);
        var expected = OracleDigest(xml);

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void DarlingDigest_strips_embedded_nuls_before_hashing()
    {
        var withNuls = "<ShowPlanXML>\0\0plan\0</ShowPlanXML>";
        var withoutNuls = "<ShowPlanXML>plan</ShowPlanXML>";

        var actual = QuerySnapshotsExporter.DarlingDigest(withNuls);
        var expected = OracleDigest(withoutNuls);

        Assert.Equal(expected, actual);
        // And it must NOT equal a naive hash of the un-stripped bytes -- proves stripping actually ran.
        Assert.NotEqual(SHA256.HashData(Encoding.UTF8.GetBytes(withNuls)), actual);
    }

    [Fact]
    public void DarlingDigest_is_stable_and_content_sensitive()
    {
        var a = QuerySnapshotsExporter.DarlingDigest("<ShowPlanXML>A</ShowPlanXML>");
        var aAgain = QuerySnapshotsExporter.DarlingDigest("<ShowPlanXML>A</ShowPlanXML>");
        var b = QuerySnapshotsExporter.DarlingDigest("<ShowPlanXML>B</ShowPlanXML>");

        Assert.Equal(a, aAgain);
        Assert.NotEqual(a, b);
    }

}
