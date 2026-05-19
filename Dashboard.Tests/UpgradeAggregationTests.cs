using PerformanceMonitorDashboard;
using Xunit;

namespace Dashboard.Tests
{
    public class UpgradeAggregationTests
    {
        [Fact]
        public void AggregatesServerAndStepCounts_Correctly()
        {
            // Arrange: three servers, two successes, one failure
            var inputs = new[]
            {
                // success: 2 upgrade steps succeeded, 1 step succeeded
                new AggregationInput(true, 2, 0, 1, 0),
                // failure: 0 succeeded, 1 upgrade failed
                new AggregationInput(false, 0, 1, 0, 0),
                // success: 1 upgrade succeeded
                new AggregationInput(true, 1, 0, 0, 0)
            };

            // Act
            var result = UpgradeAggregator.Aggregate(inputs);

            // Assert
            Assert.Equal(2, result.ServerSuccessCount);
            Assert.Equal(1, result.ServerFailCount);

            // Steps succeeded: (2+1) + (0) + (1) = 4
            Assert.Equal(4, result.StepsSucceeded);
            // Steps failed: 1
            Assert.Equal(1, result.StepsFailed);

            // Summary contains expected fragments
            Assert.Contains("2 servers upgraded", result.Summary);
            Assert.Contains("4 steps succeeded", result.Summary);
            Assert.Contains("1 step failed", result.Summary);
        }

        [Fact]
        public void Aggregator_FormatsSingularPlural_Correctly()
        {
            var singleSuccess = new[] { new AggregationInput(true, 1, 0, 0, 0) };
            var r1 = UpgradeAggregator.Aggregate(singleSuccess);
            Assert.Equal(1, r1.ServerSuccessCount);
            Assert.Contains("1 server upgraded", r1.Summary);

            var singleFail = new[] { new AggregationInput(false, 0, 0, 0, 1) };
            var r2 = UpgradeAggregator.Aggregate(singleFail);
            Assert.Equal(1, r2.ServerFailCount);
            Assert.Contains("1 step failed", r2.Summary);
        }
    }
}