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
        Assert.Contains("IF OBJECT_ID(N'config.xe_shred_state', N'U') IS NULL", sql, StringComparison.Ordinal);
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

        /* A MISSING state row (no prior cycle recorded for this collector) must mean a full read, not
           "nothing new" — the WHEN branch below is what makes the gate fail open on the first run and
           after any row loss, instead of silently skipping real data. */
        Assert.Contains("WHEN @last_execution_count IS NULL THEN 1", sql, StringComparison.Ordinal);
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

        /* Same fail-open requirement as the blocked-process collector: a missing row means "shred
           everything", never "nothing new". */
        Assert.Contains("WHEN @last_execution_count IS NULL THEN 1", sql, StringComparison.Ordinal);
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

    /// <summary>
    /// Both collectors' payload INSERT, inside the gated dynamic SQL, excludes any event whose
    /// event_time already exists in the target table -- so a changed execution_count that triggers a
    /// full re-shred of the ring buffer does not re-insert events the table already holds. Without this
    /// predicate a full re-shred (by design, on every counter change) would duplicate every row the
    /// prior cycle already stored, not just the truly new ones. This assertion pins the predicate's
    /// presence and its target: <c>collect.blocked_process_xml</c> / <c>collect.deadlock_xml</c>,
    /// compared on <c>event_time</c>.
    /// </summary>
    [Theory]
    [InlineData("22_collect_blocked_processes.sql", "blocked_process_xml", "bx")]
    [InlineData("24_collect_deadlock_xml.sql", "deadlock_xml", "dx")]
    public void CollectorsDedupeReShreddedEventsOnInsert(string fileName, string tableSuffix, string alias)
    {
        var sql = CollectorSql(fileName);

        Assert.Contains(
            $"FROM collect.{tableSuffix} AS {alias}",
            sql,
            StringComparison.Ordinal);
        Assert.Contains(
            $"WHERE {alias}.event_time = evt.value(''(@timestamp)[1]'', ''datetime2(7)'')",
            sql,
            StringComparison.Ordinal);

        /* The NOT EXISTS wrapping that WHERE must appear once per platform branch (Azure SQL DB,
           server-scoped), same as the shred gate itself -- one dedupe check per INSERT, not shared
           across branches. */
        var dedupeInstances = System.Text.RegularExpressions.Regex.Matches(
            sql,
            $"WHERE {alias}\\.event_time = evt\\.value\\(''\\(@timestamp\\)\\[1\\]'', ''datetime2\\(7\\)''\\)").Count;
        Assert.Equal(2, dedupeInstances);
    }

    /// <summary>
    /// The dynamic SQL strings assembled for <c>sp_executesql</c> are their own scope: a variable
    /// referenced inside one that is neither DECLAREd inside that same string nor passed in as an
    /// <c>sp_executesql</c> parameter does not exist at execution time and the batch throws "Must
    /// declare the scalar variable" straight into the outer CATCH. #4213's own gate hit exactly this --
    /// <c>@shred_needed</c> was read and SET inside all four dynamic batches (Azure + server-scoped, both
    /// collectors) but only ever DECLAREd in the OUTER procedure scope, which the dynamic string cannot
    /// see. This pin parses each <c>N'...'</c> string passed to <c>sp_executesql</c> and asserts that
    /// every <c>@variable</c> it references is covered by a local DECLARE or a parameter listed in that
    /// same call's <c>@params</c> definition.
    ///
    /// <para>The parser is deliberately simple: it strips <c>/* ... */</c> comments and single-line
    /// <c>--</c> comments, then walks the T-SQL string literal using its own escaping rule (a doubled
    /// <c>''</c> is a literal quote, not a terminator) to find the matching close quote. It does not
    /// understand nested dynamic SQL, does not evaluate control flow, and treats any <c>@name</c> token
    /// as a variable reference even inside a string literal that happens to contain one (none of the
    /// four batches here do). It is a source guard against exactly the defect class #4213 shipped with,
    /// not a T-SQL parser.</para>
    /// </summary>
    [Theory]
    [InlineData("22_collect_blocked_processes.sql")]
    [InlineData("24_collect_deadlock_xml.sql")]
    public void DynamicSqlBatchesDeclareOrParameterizeEveryVariableTheyReference(string fileName)
    {
        var sql = CollectorSql(fileName);
        var batches = ExtractSpExecuteSqlBatches(sql);

        Assert.True(batches.Count >= 4, $"expected at least 4 sp_executesql batches in {fileName}, found {batches.Count}");

        foreach (var batch in batches)
        {
            var declared = DeclaredVariables(batch.SqlText);
            var parameterized = ParameterNames(batch.ParamsText);
            var referenced = ReferencedVariables(batch.SqlText);

            var uncovered = referenced
                .Where(v => !declared.Contains(v) && !parameterized.Contains(v))
                .ToList();

            Assert.True(
                uncovered.Count == 0,
                $"{fileName}: batch near offset {batch.Offset} references {string.Join(", ", uncovered)} " +
                "without a local DECLARE or an sp_executesql parameter");
        }
    }

    private readonly record struct SpExecuteSqlBatch(int Offset, string SqlText, string ParamsText);

    /// <summary>
    /// Finds each <c>EXECUTE sys.sp_executesql @sql, N'...params...', ...</c> call, resolves the two
    /// string literals it needs -- the SQL text built into <c>@sql</c> just above the call, and the
    /// inline params string passed as the second argument -- and returns both as plain text with
    /// T-SQL's doubled-quote escaping already collapsed.
    /// </summary>
    private static List<SpExecuteSqlBatch> ExtractSpExecuteSqlBatches(string sql)
    {
        var stripped = StripComments(sql);
        var results = new List<SpExecuteSqlBatch>();

        foreach (System.Text.RegularExpressions.Match setMatch in
            System.Text.RegularExpressions.Regex.Matches(stripped, @"SET\s+@sql\s*=\s*N'"))
        {
            var sqlTextStart = setMatch.Index + setMatch.Length;
            var sqlTextEnd = FindStringLiteralEnd(stripped, sqlTextStart);
            var sqlText = UnescapeLiteral(stripped.Substring(sqlTextStart, sqlTextEnd - sqlTextStart));

            var execMatch = System.Text.RegularExpressions.Regex.Match(
                stripped.Substring(sqlTextEnd),
                @"EXECUTE\s+sys\.sp_executesql\s*@sql\s*,\s*N'");

            if (!execMatch.Success)
            {
                continue;
            }

            var paramsTextStart = sqlTextEnd + execMatch.Index + execMatch.Length;
            var paramsTextEnd = FindStringLiteralEnd(stripped, paramsTextStart);
            var paramsText = UnescapeLiteral(stripped.Substring(paramsTextStart, paramsTextEnd - paramsTextStart));

            results.Add(new SpExecuteSqlBatch(setMatch.Index, sqlText, paramsText));
        }

        return results;
    }

    /// <summary>
    /// From <c>SET @sql = N'...'</c> at <paramref name="start"/> (the position right after the opening
    /// quote), walks forward respecting T-SQL's doubled single-quote escape (<c>''</c> inside an N'...'
    /// literal is a literal quote character, not the end of the string) until the true closing quote.
    /// </summary>
    private static int FindStringLiteralEnd(string text, int start)
    {
        var i = start;
        while (i < text.Length)
        {
            if (text[i] == '\'')
            {
                if (i + 1 < text.Length && text[i + 1] == '\'')
                {
                    i += 2;
                    continue;
                }

                return i;
            }

            i++;
        }

        Assert.Fail("unterminated string literal while scanning sp_executesql batch");
        return -1;
    }

    private static string UnescapeLiteral(string text) => text.Replace("''", "'", StringComparison.Ordinal);

    private static string StripComments(string sql)
    {
        var noBlockComments = System.Text.RegularExpressions.Regex.Replace(
            sql,
            @"/\*.*?\*/",
            string.Empty,
            System.Text.RegularExpressions.RegexOptions.Singleline);

        return System.Text.RegularExpressions.Regex.Replace(
            noBlockComments,
            @"--[^\n]*",
            string.Empty);
    }

    private static HashSet<string> DeclaredVariables(string sqlText)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (System.Text.RegularExpressions.Match m in
            System.Text.RegularExpressions.Regex.Matches(sqlText, @"DECLARE\s+(@\w+)"))
        {
            names.Add(m.Groups[1].Value);
        }

        return names;
    }

    private static HashSet<string> ParameterNames(string paramsText)
    {
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (System.Text.RegularExpressions.Match m in
            System.Text.RegularExpressions.Regex.Matches(paramsText, @"(@\w+)\s+\w"))
        {
            names.Add(m.Groups[1].Value);
        }

        return names;
    }

    /// <summary>
    /// <c>@name</c> tokens inside the XQuery/XPath string literals these batches build (e.g.
    /// <c>''(@timestamp)[1]''</c>, <c>event[@name="blocked_process_report"]</c>) are XML attribute
    /// references, not T-SQL variables, and must not be treated as ones. By the time this runs, the
    /// doubled <c>''</c> escaping has already been collapsed to plain <c>'</c>, so those XQuery
    /// fragments are ordinary single-quoted string literals in the batch text -- stripping quoted
    /// literals before scanning for <c>@</c> tokens removes them along with any other literal text that
    /// happens to contain an <c>@</c>.
    /// </summary>
    private static HashSet<string> ReferencedVariables(string sqlText)
    {
        var withoutLiterals = System.Text.RegularExpressions.Regex.Replace(sqlText, @"'[^']*'", string.Empty);
        var names = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (System.Text.RegularExpressions.Match m in
            System.Text.RegularExpressions.Regex.Matches(withoutLiterals, @"(@\w+)"))
        {
            names.Add(m.Groups[1].Value);
        }

        return names;
    }
}
