namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8115: the count beside the FinOps Recommendations sub-tab's caption. While the read is in flight it says so, because
/// column headers under a finished-looking caption are the same picture as an empty result.
/// </summary>
public static class FinOpsRecommendationsCountText
{
    public const string Loading = "Loading recommendations…";

    public static string For(int count) => count > 0 ? $"{count} recommendation(s)" : "no recommendations";
}
