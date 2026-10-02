using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #1240: the wait-stats tab filters ignored waits at display time via
/// <see cref="IgnoredWaitTypes.BuildExclusionClause"/>, so benign waits already collected into the
/// DuckDB don't surface in the tab/picker. Guards the SQL fragment + its injection-safety sanitization.
/// </summary>
public class IgnoredWaitTypesTests
{
    [Fact]
    public void BuildExclusionClause_Empty_ReturnsEmptyString()
    {
        Assert.Equal("", IgnoredWaitTypes.BuildExclusionClause(new string[0]));
        Assert.Equal("", IgnoredWaitTypes.BuildExclusionClause(null));
    }

    [Fact]
    public void BuildExclusionClause_BuildsNotInList()
    {
        var clause = IgnoredWaitTypes.BuildExclusionClause(new[] { "SOS_WORK_DISPATCHER", "DISPATCHER_QUEUE_SEMAPHORE" });
        Assert.Equal("AND rtrim(wait_type) NOT IN ('SOS_WORK_DISPATCHER','DISPATCHER_QUEUE_SEMAPHORE')", clause);
    }

    [Fact]
    public void BuildExclusionClause_DropsUnsafeNames()
    {
        // A real wait_type is always an identifier; drop anything else so the inlined SQL can't be injected.
        var clause = IgnoredWaitTypes.BuildExclusionClause(new[] { "GOOD_WAIT", "bad'); DROP TABLE x;--", "ALSO_OK" });
        Assert.Equal("AND rtrim(wait_type) NOT IN ('GOOD_WAIT','ALSO_OK')", clause);
    }
}
