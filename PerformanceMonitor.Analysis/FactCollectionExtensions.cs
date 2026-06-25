using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Helpers for turning a fact list into a keyed lookup.
/// </summary>
public static class FactCollectionExtensions
{
    /// <summary>
    /// Builds a <c>Key → Fact</c> lookup, keeping the first fact for each key.
    /// <para>
    /// Unlike a raw <c>ToDictionary(f =&gt; f.Key)</c>, this never throws on a duplicate key.
    /// A duplicate fact key should not normally occur — collectors are expected to emit one fact
    /// per key — but a collector bug or unusual source data can produce two, and a raw
    /// <c>ToDictionary</c> then throws, aborting the ENTIRE analysis for that server. The
    /// exception is swallowed and logged upstream, so a single duplicated key silently blanks
    /// every finding for the run (this exact failure was observed when a config collector
    /// returned two rows for the same setting). Deduping here keeps analysis resilient: the
    /// misbehaving key collapses to its first occurrence instead of taking down the whole run.
    /// Fix the duplicate at the collector; this is the backstop.
    /// </para>
    /// </summary>
    public static Dictionary<string, Fact> ToFactLookup(this IEnumerable<Fact> facts) =>
        facts.GroupBy(f => f.Key).ToDictionary(g => g.Key, g => g.First());
}
