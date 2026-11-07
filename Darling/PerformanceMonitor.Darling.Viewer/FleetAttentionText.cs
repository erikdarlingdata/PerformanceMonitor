using System;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// #8115: the words beside the fleet's attention counts. A Healthy-banded server the problem list holds for a tile no band
/// reads (#7561, #7729) stays under the Healthy tile, so every count that includes such a server says how many of them it
/// holds; "9 Healthy" above "showing 12 of 12" otherwise reads as a contradiction. The web page's fleet view says it the
/// same way (#8095).
/// </summary>
public static class FleetAttentionText
{
    /// <summary>" (3 healthy with advisories)", " (1 healthy with an advisory)", or nothing when there are none.</summary>
    public static string AdvisorySplit(int healthyWithAdvisories) =>
        healthyWithAdvisories > 0
            ? $" ({healthyWithAdvisories} healthy with {(healthyWithAdvisories == 1 ? "an advisory" : "advisories")})"
            : "";

    /// <summary>The overflow line under the ranked list: the servers past its cap, and how many of those are Healthy with an advisory.</summary>
    public static string MoreText(int additional, int healthyWithAdvisories) =>
        additional > 0 ? $"+{additional} more need attention" + AdvisorySplit(Math.Min(healthyWithAdvisories, additional)) : "";
}
