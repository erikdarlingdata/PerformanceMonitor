/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// The fact keys one finding is ABOUT, typed (#3859 items 4+5) — the chain the traversal walked
/// (<see cref="AnalysisFinding.PathKeys"/>) and the config levers hanging off it
/// (<see cref="AnalysisFinding.SideLeafKeys"/>), in one object because every consumer of the first wants the
/// second and there is exactly one rule for reading them together (<see cref="All"/>).
///
/// <para><b>Why this type exists at all.</b> Until this change the consumers recovered the chain by splitting the
/// RENDERED path — <c>finding.StoryPath.Split(" → ")</c> — at six sites across both SKUs: the two
/// <c>ToolRecommendations.GetForStoryPath</c> bodies and the three drill-down collectors (plus the frozen
/// Dashboard copy). The engine HAD the typed list the whole time (<see cref="AnalysisStory.Path"/>); it was
/// joined into a display string in <c>InferenceEngine.BuildStory</c> / <c>SameStatementPileupDetector</c> and then
/// dropped on the way to <see cref="AnalysisFinding"/>, so six readers re-parsed a string to get back what the
/// producer had thrown away. That is a corruption surface, not a style complaint: a fact key that ever contains
/// the arrow, or any change to the join string, silently corrupts all six at once — each one would split a key in
/// half or stop matching, and nothing in the product would say so. The typed list cannot be mis-parsed because it
/// is never parsed.</para>
///
/// <para><b>Ephemeral, exactly like <see cref="AnalysisFinding.SideLeafKeys"/>.</b> Neither list has an
/// <c>analysis_findings</c> column and this change adds none — no schema, no migration rung. Both are populated
/// on the WRITE path (the two finding stores copy them off the story) and both are empty on a finding read back
/// from the store, which is safe because both consumers are write-path-only: the drill-down collectors enrich the
/// findings the pass just produced, and <c>next_tools</c> is rendered by <c>analyze_server</c>, which renders the
/// pass's own findings. The RENDERED <see cref="AnalysisFinding.StoryPath"/> string is unchanged everywhere and
/// remains the persisted, read-back spelling of the same chain.</para>
/// </summary>
public sealed class StoryKeys
{
    /// <summary>No keys — the shape a caller with neither list passes, so no consumer needs a null check.</summary>
    public static readonly StoryKeys None = new([], []);

    /// <summary>
    /// The fact keys ON the chain, root first, in traversal order — <see cref="AnalysisStory.Path"/> as the engine
    /// recorded it, carried through <see cref="AnalysisFinding.PathKeys"/>. The THREADPOOL attribution relabel is
    /// already applied (the story's effective path is what rides), so these are the keys the rendered
    /// <see cref="AnalysisFinding.StoryPath"/> names.
    /// </summary>
    public IReadOnlyList<string> PathKeys { get; }

    /// <summary>
    /// The config levers hanging off the chain by an active edge the greedy walk did not follow (#3691) —
    /// <see cref="AnalysisFinding.SideLeafKeys"/>. Beside the path, never in it: they take no part in the story's
    /// identity, and a consumer that only wants the chain reads <see cref="PathKeys"/>.
    /// </summary>
    public IReadOnlyList<string> SideLeafKeys { get; }

    private StoryKeys(IReadOnlyList<string> pathKeys, IReadOnlyList<string> sideLeafKeys)
    {
        PathKeys = pathKeys;
        SideLeafKeys = sideLeafKeys;
    }

    /// <summary>
    /// One finding's keys: its chain and its levers, both as the write path left them. A finding read back from
    /// the store carries neither (see this class's summary), and yields <see cref="None"/>'s empty lists rather
    /// than a null.
    /// </summary>
    public static StoryKeys ForFinding(AnalysisFinding finding)
    {
        ArgumentNullException.ThrowIfNull(finding);
        return new StoryKeys(finding.PathKeys, finding.SideLeafKeys);
    }

    /// <summary>
    /// A chain with no levers, named directly — for a caller that HAS the typed keys and no finding (a test
    /// exercising one key's recommendation row, a single-fact lookup). Never a rendered path: nothing in this type
    /// splits a string, which is the point of it.
    /// </summary>
    public static StoryKeys OfPath(params string[] pathKeys) =>
        new(pathKeys ?? [], []);

    /// <summary>
    /// A chain and its levers, named directly.
    /// </summary>
    public static StoryKeys Of(IReadOnlyList<string> pathKeys, IReadOnlyList<string> sideLeafKeys) =>
        new(pathKeys ?? [], sideLeafKeys ?? []);

    /// <summary>
    /// Every key this finding is about, ONE order for every consumer: the chain root-first, then the levers in the
    /// engine's severity order, each key once (ordinal). Chain first because a recommendation list is read
    /// top-down and the incident is what the operator came for; the lever's reads follow it rather than displacing
    /// it. The de-duplication matters because a lever CAN also be a hop on another finding's chain, and a
    /// recommendation row that appeared twice would read as two different suggestions.
    /// </summary>
    public IEnumerable<string> All =>
        PathKeys.Concat(SideLeafKeys).Distinct(StringComparer.Ordinal);

    /// <summary>The chain as a set, for the drill-down collectors' "is this key on the path" tests.</summary>
    public HashSet<string> PathKeySet() => PathKeys.ToHashSet(StringComparer.Ordinal);
}
