using System.Collections.Generic;
using System.Globalization;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// The format numbers of Level 1 investigations (#5632 Phase A, plan section 7): the shape of the brief Darling composes
/// (<see cref="BriefFormat"/>) and the report shapes it accepts back (<see cref="AcceptedReportFormats"/>). Claim and submit
/// check the report format first, before any store work, so an agent built for another shape is told plainly which one this
/// Darling speaks. When a format 2 exists, the previous format stays accepted for at least one minor release after it.
/// </summary>
public static class InvestigationFormats
{
    /// <summary>The shape of every brief this build composes.</summary>
    public const short BriefFormat = 1;

    /// <summary>The newest report shape this build accepts.</summary>
    public const short ReportFormat = 1;

    /// <summary>Every report shape this build accepts. History: format 1 (Phase A, #5632).</summary>
    public static IReadOnlyList<short> AcceptedReportFormats { get; } = new short[] { 1 };

    /// <summary>True when <paramref name="reportFormat"/> is a report shape this build accepts.</summary>
    public static bool IsAcceptedReportFormat(short reportFormat) => AcceptedReportFormats.Contains(reportFormat);

    /// <summary>The refusal for a report format this build does not accept, naming what it does accept and the brief format.</summary>
    public static string UnsupportedReportFormatMessage(short reportFormat) => UnsupportedReportFormatMessage(reportFormat, "Darling");

    /// <summary>The same refusal naming <paramref name="product"/> (#5820: Lite's tools say "Lite accepts"; Darling's bytes are unchanged).</summary>
    public static string UnsupportedReportFormatMessage(short reportFormat, string product) =>
        string.Create(CultureInfo.InvariantCulture,
            $"report_format {reportFormat} is not supported. {product} accepts report_format {string.Join(", ", AcceptedReportFormats)}. The brief you claimed is brief_format {BriefFormat}.");
}
