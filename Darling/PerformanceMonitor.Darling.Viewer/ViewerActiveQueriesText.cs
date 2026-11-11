using System.Globalization;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>#8576: the words of the Active Queries sub-tab that tests hold to.</summary>
public static class ViewerActiveQueriesText
{
    /// <summary>The header over the Active Queries grid.</summary>
    public const string SectionHeader = "Currently Running Queries";

    /// <summary>The Plan Viewer tab title for a plan opened from an Active Queries row.</summary>
    public static string PlanTabLabel(bool live, int sessionId) =>
        string.Create(CultureInfo.InvariantCulture, $"{(live ? "Live" : "Estimated")} Plan — Session {sessionId}");
}
