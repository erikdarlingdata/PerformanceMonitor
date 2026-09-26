namespace Installer.Tests;

/// <summary>
/// Source guard for the #4213 execution_count gate in the deprecated Dashboard's blocked-process and
/// deadlock collectors.
///
/// <para>#4200 found that <c>install/22_collect_blocked_processes.sql</c> and
/// <c>install/24_collect_deadlock_xml.sql</c> converted the whole Extended Events ring buffer to XML and
/// shredded it on every run, even when nothing new had arrived. #4212 fixed the same defect in Darling and
/// Lite by reading the session target's own <c>execution_count</c> before the cast and skipping the
/// conversion when it matches the value stored from the prior cycle. #4213 is that same gate, in T-SQL,
/// for the deprecated Dashboard's collectors — the only ones #4212 could not reach.</para>
///
/// <para>This is a SOURCE guard for the same reason <c>QueryStoreSliceAggregationSqlTests</c> is: the gated
/// query lives inside a dynamically assembled <c>@sql</c> string, so the compiler proves nothing about it,
/// the <c>sql-validation</c> workflow only proves the batch installs and runs, and the DB-touching test
/// classes that would execute this collector against a live server are exactly the ones CI filters out.
/// A guard that reads the file is what would have caught either collector shipping without the gate, or
/// shipping it wired to the wrong table, the wrong direction, or the wrong session's counter.</para>
/// </summary>
public class XeShredGateSqlTests
{
    private static string RepoRoot()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.sln")))
        {
            dir = dir.Parent;
        }

        Assert.True(dir is not null, "could not locate the repository root from " + AppContext.BaseDirectory);

        return dir!.FullName;
    }

    private static string CollectorSql(string fileName)
    {
        var path = Path.Combine(RepoRoot(), "install", fileName);
        Assert.True(File.Exists(path), "expected the collector script at " + path);
        return File.ReadAllText(path);
    }

    /// <summary>
    /// Both collectors carry a durable place to remember the last count they saw. Without
    /// <c>config.xe_shred_state</c>, every run would have no <c>@last_execution_count</c> to compare
    /// against and the gate below would always fall through to "shred" — the pre-#4213 behavior, just
    /// with extra code around it.
    /// </summary>
    [Fact]
    public void ConfigTablesScriptCreatesTheShredStateTable()
    {
        var sql = CollectorSql("03_create_config_tables.sql");

        Assert.Contains("config.xe_shred_state", sql, StringComparison.Ordinal);
        Assert.Contains("last_execution_count bigint NOT NULL,", sql, StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT PK_xe_shred_state PRIMARY KEY CLUSTERED (collector_name)", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The blocked-process collector reads <c>execution_count</c> before the cast, on both the Azure SQL
    /// DB and server-scoped paths, and only shreds when the count is unavailable, no prior count is
    /// stored, or the count has changed. On the OLD file (before #4213) neither
    /// <c>xet.execution_count</c> nor <c>@shred_needed</c> appears at all — the INSERT into the ring
    /// buffer table variable runs unconditionally on every execution, which is exactly the defect #4213
    /// closes. This assertion fails against that old file.
    /// </summary>
    [Fact]
    public void BlockedProcessCollectorReadsExecutionCountBeforeShredding()
    {
        var sql = CollectorSql("22_collect_blocked_processes.sql");

        Assert.Contains("@execution_count = xet.execution_count", sql, StringComparison.Ordinal);
        Assert.Contains("@shred_needed =", sql, StringComparison.Ordinal);
        Assert.Contains("IF @shred_needed = 1", sql, StringComparison.Ordinal);
        Assert.Contains("config.xe_shred_state", sql, StringComparison.Ordinal);

        /* The gate must be evaluated on both the Azure SQL DB branch and the server-scoped branch — one
           instance per branch, so the shred is skippable regardless of platform. */
        var gateInstances = System.Text.RegularExpressions.Regex.Matches(sql, "IF @shred_needed = 1").Count;
        Assert.Equal(2, gateInstances);
    }

    /// <summary>
    /// The deadlock collector carries the same gate, on the same two platform branches. On the OLD file
    /// the ring-buffer INSERT ran unconditionally in both <c>AzureQueryText</c>-equivalent and
    /// server-scoped SQL text, with no <c>execution_count</c> read anywhere — this assertion fails
    /// against that old file.
    /// </summary>
    [Fact]
    public void DeadlockCollectorReadsExecutionCountBeforeShredding()
    {
        var sql = CollectorSql("24_collect_deadlock_xml.sql");

        Assert.Contains("@execution_count = xet.execution_count", sql, StringComparison.Ordinal);
        Assert.Contains("@shred_needed =", sql, StringComparison.Ordinal);
        Assert.Contains("IF @shred_needed = 1", sql, StringComparison.Ordinal);
        Assert.Contains("config.xe_shred_state", sql, StringComparison.Ordinal);

        var gateInstances = System.Text.RegularExpressions.Regex.Matches(sql, "IF @shred_needed = 1").Count;
        Assert.Equal(2, gateInstances);
    }

    /// <summary>
    /// Both collectors persist THIS cycle's own execution_count after the read, regardless of which
    /// branch the gate took, so the next cycle's comparison reflects reality. The old file never writes
    /// to <c>config.xe_shred_state</c> at all — there was nothing to persist, since every run always
    /// shredded.
    /// </summary>
    [Theory]
    [InlineData("22_collect_blocked_processes.sql", "blocked_process_xml_collector")]
    [InlineData("24_collect_deadlock_xml.sql", "deadlock_xml_collector")]
    public void CollectorsPersistTheGateResultAfterEveryRun(string fileName, string collectorName)
    {
        var sql = CollectorSql(fileName);

        Assert.Contains("IF @execution_count IS NOT NULL", sql, StringComparison.Ordinal);
        Assert.Contains(
            $"IF EXISTS (SELECT 1/0 FROM config.xe_shred_state WHERE collector_name = N'{collectorName}')",
            sql,
            StringComparison.Ordinal);
        Assert.Contains("last_execution_count = @execution_count,", sql, StringComparison.Ordinal);
    }

    /// <summary>
    /// The stored rows themselves must stay the same shape as before the gate: both collectors still
    /// insert into their payload table (<c>collect.blocked_process_xml</c> / <c>collect.deadlock_xml</c>)
    /// with the same columns, only now guarded by the shred check rather than unconditional.
    /// </summary>
    [Fact]
    public void BlockedProcessRowShapeIsUnchanged()
    {
        var sql = CollectorSql("22_collect_blocked_processes.sql");

        Assert.Contains("collect.blocked_process_xml", sql, StringComparison.Ordinal);
        Assert.Contains("event_time,", sql, StringComparison.Ordinal);
        Assert.Contains("blocked_process_xml", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void DeadlockRowShapeIsUnchanged()
    {
        var sql = CollectorSql("24_collect_deadlock_xml.sql");

        Assert.Contains("collect.deadlock_xml", sql, StringComparison.Ordinal);
        Assert.Contains("event_time,", sql, StringComparison.Ordinal);
        Assert.Contains("deadlock_xml", sql, StringComparison.Ordinal);
    }
}
