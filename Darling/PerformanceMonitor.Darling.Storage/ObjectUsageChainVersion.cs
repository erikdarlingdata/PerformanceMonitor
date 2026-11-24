namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #9349: the version of the chain rules a pair's stored chain rows were built with. Bump it when the shared stepper's
/// rules change: the builder then deletes and rebuilds each pair, and the reader takes the full fold for rows of an
/// older version.
/// </summary>
public static class ObjectUsageChainVersion
{
    public const short Current = 1;
}
