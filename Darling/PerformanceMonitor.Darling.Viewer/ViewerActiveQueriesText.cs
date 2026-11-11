using System.Globalization;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8576: the words of the Active Queries sub-tab that tests hold to. The grid under the header lists the stored captures of the
/// selected range, so the header names those, and a plan tab opened from one of its rows is titled in the plain-hyphen shape every
/// other plan tab uses ("QS Plan - Q1/P2", "Actual Plan - SPID 5").
/// </summary>
public static class ViewerActiveQueriesText
{
    /// <summary>The header over the Active Queries grid: what the grid shows, the captures stored for the range, not what runs now.</summary>
    public const string SectionHeader = "Captured queries";

    /// <summary>
    /// The Plan Viewer tab title (and its tooltip) for a plan opened from an Active Queries row: "Estimated Plan - Session 126", or
    /// "Live Plan - Session 126" for the live plan. A plain hyphen, as the other plan tab titles use.
    /// </summary>
    public static string PlanTabLabel(bool live, int sessionId) =>
        string.Create(CultureInfo.InvariantCulture, $"{(live ? "Live" : "Estimated")} Plan - Session {sessionId}");
}
