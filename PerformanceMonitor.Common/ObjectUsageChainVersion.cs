namespace PerformanceMonitor.Common;

/// <summary>
/// #9349, #9596: the version of the chain rules a pair's stored chain rows were built with, shared by Darling and Lite.
/// Bump it when the shared stepper's rules change: each product's builder then deletes and rebuilds each pair, and its
/// reader takes the full fold for rows of an older version.
/// </summary>
public static class ObjectUsageChainVersion
{
    public const short Current = 1;
}
