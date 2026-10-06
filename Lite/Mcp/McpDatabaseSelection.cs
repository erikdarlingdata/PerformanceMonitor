namespace PerformanceMonitorLite.Mcp;

/// <summary>
/// How an empty answer names the databases a read was limited to (#5244 PR3). Darling's twin is
/// <c>DatabaseFilter.Describe()</c>: the name for one database, "the chosen databases" for two or more, nothing for all, and
/// its empty-answer sentences are built from those words. Lite's MCP tools take ONE <c>database_name</c> today, so the
/// many-name branch has no tool caller yet; it exists so a later multi-name Lite read says what Darling says, and a test pins
/// both apps to the same words.
/// </summary>
internal static class McpDatabaseSelection
{
    /// <summary>The words for two or more names. Byte-identical with <c>DatabaseFilter.ManyDatabasesDescription</c>.</summary>
    internal const string ManyDatabasesDescription = "the chosen databases";

    /// <summary>The name for one database, <see cref="ManyDatabasesDescription"/> for two or more, null for none (every database).</summary>
    internal static string? Describe(IReadOnlyList<string>? names) =>
        names == null || names.Count == 0 ? null
        : names.Count == 1 ? names[0]
        : ManyDatabasesDescription;

    /// <summary>The wait-tasks and blocking-stats form: "database 'X'" for one, the many-name words for two or more.</summary>
    internal static string Quoted(IReadOnlyList<string> names) =>
        names.Count == 1 ? $"database '{names[0]}'" : ManyDatabasesDescription;

    /// <summary>
    /// What a selection adds to an empty answer's sentence: nothing for every database, " for the database X" for one and
    /// " for the chosen databases" for two or more (Darling's <c>ForChosenDatabases</c>).
    /// </summary>
    internal static string ForChosen(IReadOnlyList<string>? names) =>
        Describe(names) switch
        {
            null => string.Empty,
            ManyDatabasesDescription => " for " + ManyDatabasesDescription,
            var one => " for the database " + one,
        };
}
