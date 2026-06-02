using System;
using System.Collections.Generic;

namespace PerformanceMonitorDashboard
{
    /// <summary>
    /// Simple input DTO used by the aggregator.
    /// </summary>
    public sealed record AggregationInput(
        bool Success,
        int UpgradesSucceeded,
        int UpgradesFailed,
        int StepsSucceeded,
        int StepsFailed);

    /// <summary>
    /// Aggregation result returned to callers and used by tests/UI.
    /// </summary>
    public sealed record AggregationResult(
        int ServerSuccessCount,
        int ServerFailCount,
        int StepsSucceeded,
        int StepsFailed,
        string Summary);

    /// <summary>
    /// Aggregates per-server upgrade results into server counts and total step/file counts.
    /// </summary>
    public static class UpgradeAggregator
    {
        public static AggregationResult Aggregate(IEnumerable<AggregationInput> inputs)
        {
            if (inputs == null) throw new ArgumentNullException(nameof(inputs));

            int serverSuccess = 0;
            int serverFail = 0;
            int stepsSucceeded = 0;
            int stepsFailed = 0;

            foreach (var it in inputs)
            {
                if (it.Success) serverSuccess++;
                else serverFail++;

                stepsSucceeded += it.UpgradesSucceeded + it.StepsSucceeded;
                stepsFailed += it.UpgradesFailed + it.StepsFailed;
            }

            string serversPart = serverSuccess == 1 ? "1 server upgraded" : $"{serverSuccess} servers upgraded";
            string stepsPart = stepsFailed == 1
                ? $"{stepsSucceeded} steps succeeded, 1 step failed"
                : $"{stepsSucceeded} steps succeeded, {stepsFailed} steps failed";

            string summary = $"{serversPart} ({stepsPart})";

            return new AggregationResult(serverSuccess, serverFail, stepsSucceeded, stepsFailed, summary);
        }
    }
}