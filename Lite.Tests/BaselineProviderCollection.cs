using Xunit;

namespace PerformanceMonitorLite.Tests;

/* BaselineProviderTests and AnomalyDetectorTests both mutate the static
   BaselineProvider.CacheTtl field. With assembly-level parallelization
   enabled, those two classes must share a collection so they don't run
   concurrently and stomp on each other's TTL writes. */
[CollectionDefinition(Name, DisableParallelization = true)]
public class BaselineProviderCollection
{
    public const string Name = "BaselineProvider";
}
