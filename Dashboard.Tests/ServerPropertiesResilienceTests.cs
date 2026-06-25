using PerformanceMonitorDashboard.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// WS5 version-skew resilience: the server_properties SELECT references the server-health columns
/// only when the DB has them, so a not-yet-upgraded server (Dashboard updated before that server's
/// PerformanceMonitor DB got the WS5 upgrade) reads the core hardware columns without a bind error
/// — keeping SERVER_HARDWARE flowing — instead of the whole collector throwing.
/// </summary>
public class ServerPropertiesResilienceTests
{
    [Fact]
    public void Query_AllHealthColumns_SelectsThemPlusCore()
    {
        var sql = SqlServerFactCollector.BuildServerPropertiesQuery(hasLpim: true, hasIfi: true, hasDumps: true);

        Assert.Contains("lock_pages_in_memory", sql);
        Assert.Contains("instant_file_initialization_enabled", sql);
        Assert.Contains("memory_dump_count", sql);
        Assert.Contains("cpu_count", sql);
        Assert.Contains("FROM collect.server_properties", sql);
    }

    [Fact]
    public void Query_NoHealthColumns_OmitsThem_ButKeepsCore()
    {
        var sql = SqlServerFactCollector.BuildServerPropertiesQuery(hasLpim: false, hasIfi: false, hasDumps: false);

        // The WS5 columns must NOT be referenced — that is what avoids the bind error on a
        // not-yet-upgraded server.
        Assert.DoesNotContain("lock_pages_in_memory", sql);
        Assert.DoesNotContain("instant_file_initialization_enabled", sql);
        Assert.DoesNotContain("memory_dump_count", sql);

        // The core hardware columns are still selected, so SERVER_HARDWARE keeps flowing.
        Assert.Contains("cpu_count", sql);
        Assert.Contains("product_version", sql);
        Assert.Contains("FROM collect.server_properties", sql);
    }

    // The point of per-column probing: a partial / out-of-order schema (some columns present, some
    // not) selects EXACTLY the present ones and never references an absent one — so the previous
    // all-or-nothing assumption (and its coupling to the migration's column order) can't bite.
    [Theory]
    [InlineData(true, false, true)]   // lpim + dumps present, ifi absent
    [InlineData(false, true, false)]  // ifi only
    [InlineData(true, false, false)]  // lpim only
    [InlineData(false, false, true)]  // dumps only
    public void Query_PartialSubset_ReferencesOnlyPresentColumns(bool hasLpim, bool hasIfi, bool hasDumps)
    {
        var sql = SqlServerFactCollector.BuildServerPropertiesQuery(hasLpim, hasIfi, hasDumps);

        Assert.Equal(hasLpim, sql.Contains("lock_pages_in_memory"));
        Assert.Equal(hasIfi, sql.Contains("instant_file_initialization_enabled"));
        Assert.Equal(hasDumps, sql.Contains("memory_dump_count"));
        Assert.Contains("cpu_count", sql);  // core columns always present
    }
}
