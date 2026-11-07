namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8115: the Query Tag Usage window's answer on a store that defines no query tag. Every row of every section would be
/// untagged and every untagged figure would equal its total, so the window says that once instead of drawing one
/// "untagged = total" table per server per section. The sentence follows the first sentence of the MCP tools' and web
/// page's answer, <c>QueryTagUsageLabels.NoTagsMessage</c> (#8083), in the window's own words.
/// </summary>
public static class QueryTagUsageText
{
    public const string NoTagsMessage =
        "No query tags are defined, so there is nothing to roll up by tag: every row in the window would be untagged. "
        + "Define a query tag and a rule that puts work under it, then open this window again.";

    /// <summary>The whole answer when <paramref name="tagCount"/> is zero, else null and the tables are built.</summary>
    public static string? NoTagsAnswer(int tagCount) => tagCount <= 0 ? NoTagsMessage : null;
}
