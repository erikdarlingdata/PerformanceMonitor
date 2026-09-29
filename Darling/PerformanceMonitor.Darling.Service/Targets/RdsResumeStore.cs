/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service.Targets;

/// <summary>
/// The saved form of an <see cref="RdsLogSource"/> position (#4708): <c>rds|1|&lt;instance&gt;|&lt;marker&gt;|&lt;file&gt;</c>,
/// kept in <c>collect.collector_state</c> under the same keys the self-hosted tail keeps its own marker in
/// (<see cref="PgServerLogTail.ResumeStateKey"/> for the stderr file, <see cref="PgServerLogTail.ResumeStateKeyCsv"/>
/// for the csv file).
///
/// <para><b>Self-describing, because the two transports share the keys.</b> The self-hosted value is
/// <c>&lt;offset&gt;|&lt;file name&gt;</c> and starts with digits. This one starts with <c>rds|1|</c>, so neither reader
/// can take the other's value for its own: the self-hosted parser needs a number before the first bar and finds
/// <c>rds</c>, and this parser needs the whole prefix. A value neither side recognises is ignored, which is the
/// conservative path (a first contact with the newest file), never a guess at a position. The <c>1</c> is the
/// format version; a later layout takes a new prefix and today's reader ignores it.</para>
///
/// <para><b>The marker sits before the file name</b> so that a bar inside a file name cannot break the parse, the
/// reason the self-hosted value puts its file name last. An instance identifier and an RDS marker never contain a
/// bar; a value that would is not saved at all.</para>
/// </summary>
public static class RdsResumeState
{
    /// <summary>What every saved RDS position starts with: the transport and the format version.</summary>
    public const string Prefix = "rds|1|";

    /// <summary>The <c>collector_state</c> key a position of this kind is kept under: the self-hosted tail's own keys.</summary>
    public static string StateKeyFor(RdsLogSource.LogFileKind kind)
        => kind == RdsLogSource.LogFileKind.Csv ? PgServerLogTail.ResumeStateKeyCsv : PgServerLogTail.ResumeStateKey;

    /// <summary>The saved text for a position, or null when a part is empty or would break the parse.</summary>
    public static string? Format(string? instance, string? marker, string? file)
        => string.IsNullOrEmpty(instance) || string.IsNullOrEmpty(marker) || string.IsNullOrEmpty(file)
            || instance.Contains('|', StringComparison.Ordinal) || marker.Contains('|', StringComparison.Ordinal)
            ? null
            : Prefix + instance + "|" + marker + "|" + file;

    /// <summary>Parses a saved position; false for anything that is not an RDS position of this version.</summary>
    public static bool TryParse(string? value, out string instance, out string marker, out string file)
    {
        instance = marker = file = string.Empty;

        if (string.IsNullOrEmpty(value) || !value.StartsWith(Prefix, StringComparison.Ordinal))
        {
            return false;
        }

        var parts = value[Prefix.Length..].Split('|', 3);

        if (parts.Length != 3 || parts[0].Length == 0 || parts[1].Length == 0 || parts[2].Length == 0)
        {
            return false;
        }

        instance = parts[0];
        marker = parts[1];
        file = parts[2];
        return true;
    }
}

/// <summary>
/// Keeps an <see cref="RdsLogSource"/>'s positions across a restart (#4708) in <c>collect.collector_state</c>, per
/// server, under the owning collector's name: <c>pg_plan_capture</c>, <c>pg_deadlocks</c> or <c>pg_log_events</c>,
/// each of which already declares the two keys (<see cref="PgServerLogTail.ResumeStateKeys"/>). No schema step: the
/// table is the one the self-hosted tail keeps its markers in.
///
/// <para><b>Load once per server, before its first read.</b> <see cref="RestoreAsync"/> seeds the source with what the
/// last process saved and never overwrites a position the source already holds, so a restore that runs late cannot
/// move a source backwards. The source then treats a restored position like any other: a file RDS no longer lists is a
/// missing resume file (the newest file's last lines are read and the chunk says so), an older file is finished before
/// the newest is opened, and a position for another instance is not used.</para>
///
/// <para><b>Save only after the chunk's rows are stored</b> (<see cref="SaveAsync"/>, called after
/// <see cref="RdsLogSource.CommitResume"/>). A crash between the store write and the save re-reads the window on the
/// next start; the rows dedupe on their identity hashes, so a repeat costs a re-store of rows the store already has
/// and a loss cannot happen. Both delegates are the runner's own <c>GetCollectorStateAsync</c> and
/// <c>SaveCollectorStateAsync</c>, which fail toward "no state" and "keep the older value".</para>
/// </summary>
public sealed class RdsResumeStore
{
    private static readonly RdsLogSource.LogFileKind[] Kinds = { RdsLogSource.LogFileKind.Stderr, RdsLogSource.LogFileKind.Csv };

    private readonly string _collectorName;
    private readonly Func<int, string, CancellationToken, Task<Dictionary<string, string>>> _load;
    private readonly Func<int, string, IReadOnlyDictionary<string, string>, CancellationToken, Task> _save;
    private readonly ConcurrentDictionary<int, bool> _restored = new();

    public RdsResumeStore(
        string collectorName,
        Func<int, string, CancellationToken, Task<Dictionary<string, string>>> load,
        Func<int, string, IReadOnlyDictionary<string, string>, CancellationToken, Task> save)
    {
        _collectorName = collectorName ?? throw new ArgumentNullException(nameof(collectorName));
        _load = load ?? throw new ArgumentNullException(nameof(load));
        _save = save ?? throw new ArgumentNullException(nameof(save));
    }

    /// <summary>
    /// Seeds <paramref name="source"/> with the positions saved for <paramref name="serverId"/>, once per server. A
    /// value that is not an RDS position (a self-hosted marker, an unknown version) is ignored.
    /// </summary>
    public async Task RestoreAsync(RdsLogSource source, int serverId, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(source);

        if (_restored.ContainsKey(serverId))
        {
            return;
        }

        var state = await _load(serverId, _collectorName, cancellationToken);

        foreach (var kind in Kinds)
        {
            if (state.TryGetValue(RdsResumeState.StateKeyFor(kind), out var value)
                && RdsResumeState.TryParse(value, out var instance, out var marker, out var file))
            {
                source.RestorePosition(kind, instance, file, marker);
            }
        }

        _restored[serverId] = true;
    }

    /// <summary>
    /// Saves the position <paramref name="committed"/> left the source at, for the stderr or csv file. Call it only
    /// after the chunk's rows are stored and <see cref="RdsLogSource.CommitResume"/> has run. Does nothing when the
    /// chunk carried no position.
    /// </summary>
    public Task SaveAsync(
        int serverId, RdsLogSource.LogFileKind kind, RdsLogSource.ResumeMarker committed, CancellationToken cancellationToken)
    {
        var position = RdsLogSource.CommittedPosition(committed);

        if (position is null)
        {
            return Task.CompletedTask;
        }

        var value = RdsResumeState.Format(position.Value.Instance, position.Value.Marker, position.Value.File);

        return value is null
            ? Task.CompletedTask
            : _save(
                serverId,
                _collectorName,
                new Dictionary<string, string>(StringComparer.Ordinal) { [RdsResumeState.StateKeyFor(kind)] = value },
                cancellationToken);
    }
}
