using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;

namespace PerformanceMonitorLite.Database;

/// <summary>
/// One holder of the process-wide store lock, as the registry saw it at a moment (#5650).
/// </summary>
/// <param name="Name">The caller, as <c>File.Method</c> (for example <c>ArchiveService.ExportAsync</c>).</param>
/// <param name="Write">True for the write lock, false for a read lock.</param>
/// <param name="Held">How long it had held the lock when the snapshot was taken.</param>
public sealed record StoreLockHolder(string Name, bool Write, TimeSpan Held);

/// <summary>
/// Who holds <c>DuckDbInitializer</c>'s process-wide lock right now (#5650). The lock is one
/// <see cref="ReaderWriterLockSlim"/> and carries no owner, so a timeout could say only "something holds it".
/// Every acquisition registers its caller here (the name comes from the compiler through
/// <c>[CallerMemberName]</c>, so no call site changed) and the release removes it, which lets a timeout name the
/// holders and how long each has held. Read-only bookkeeping: it never decides anything about the lock itself.
/// </summary>
internal static class StoreLockHolders
{
    private sealed record Entry(string Name, bool Write, long StartedTimestamp);

    private static readonly ConcurrentDictionary<long, Entry> s_holds = new();
    private static long s_nextId;

    /// <summary>The name shown for a holder: the file's name without its extension, then the member.</summary>
    internal static string Name(string? member, string? file)
    {
        var type = string.IsNullOrEmpty(file) ? null : Path.GetFileNameWithoutExtension(file);
        var method = string.IsNullOrEmpty(member) ? null : member;
        if (type is null) return method ?? "unknown caller";
        return method is null ? type : type + "." + method;
    }

    /// <summary>
    /// A hold older than this is almost certainly a leaked <c>LockReleaser</c> (a release that never ran), not a real
    /// hold: the longest real ones, an archive export or a compaction, run for minutes. The registry cannot tell the two
    /// apart, so it does not delete the entry; it marks it and lists it after every fresher holder, so a phantom cannot
    /// take the first place in a timeout's blame while a real holder exists.
    /// </summary>
    internal static readonly TimeSpan StaleAfter = TimeSpan.FromMinutes(10);

    /// <summary>The mark added to a stale holder's name.</summary>
    internal const string StaleMark = " (stale?)";

    internal static long Register(string? member, string? file, bool write)
        => Register(member, file, write, Stopwatch.GetTimestamp());

    /// <summary>The start time is a parameter so a test can register an entry that is already old.</summary>
    internal static long Register(string? member, string? file, bool write, long startedTimestamp)
    {
        var id = Interlocked.Increment(ref s_nextId);
        s_holds[id] = new Entry(Name(member, file), write, startedTimestamp);
        return id;
    }

    internal static void Unregister(long id) => s_holds.TryRemove(id, out _);

    /// <summary>
    /// The current holders, longest-held first, except that a hold past <see cref="StaleAfter"/> is marked
    /// <see cref="StaleMark"/> and listed after the others.
    /// </summary>
    internal static IReadOnlyList<StoreLockHolder> Snapshot() => Snapshot(Stopwatch.GetTimestamp());

    /// <summary>The snapshot as of <paramref name="now"/> (a <see cref="Stopwatch"/> timestamp), so a test can age the registry.</summary>
    internal static IReadOnlyList<StoreLockHolder> Snapshot(long now)
        => s_holds.Values
            .Select(e => (Held: Stopwatch.GetElapsedTime(e.StartedTimestamp, now), Entry: e))
            .Select(x => (x.Held, Stale: x.Held > StaleAfter, x.Entry))
            .OrderBy(x => x.Stale)
            .ThenByDescending(x => x.Held)
            .Select(x => new StoreLockHolder(x.Stale ? x.Entry.Name + StaleMark : x.Entry.Name, x.Entry.Write, x.Held))
            .ToList();
}

/// <summary>
/// The write lock was not free within the caller's budget (#5650). Still a <see cref="TimeoutException"/>, so every
/// existing catch keeps working; it adds what the old text could not say: how long the caller waited, and who held
/// the lock then and for how long. The message says nothing the code does not know: with no registered holder it
/// states only that the lock was not free.
/// </summary>
public sealed class StoreLockTimeoutException : TimeoutException
{
    /// <summary>How many holders the text names before summarizing the rest.</summary>
    private const int MaxNamedHolders = 3;

    public StoreLockTimeoutException(TimeSpan waited, IReadOnlyList<StoreLockHolder> holders, string requester)
        : base(BuildMessage(waited, holders, requester))
    {
        Waited = waited;
        Holders = holders;
        Requester = requester;
    }

    /// <summary>How long the caller waited before giving up.</summary>
    public TimeSpan Waited { get; }

    /// <summary>Who held the lock when the wait ended, longest-held first. Empty when none was registered.</summary>
    public IReadOnlyList<StoreLockHolder> Holders { get; }

    /// <summary>The caller that gave up, as <c>File.Method</c>.</summary>
    public string Requester { get; }

    /// <summary>"Held by A (read lock, held 41 s), B (...)", or an empty string when nothing was registered.</summary>
    public static string FormatHolders(IReadOnlyList<StoreLockHolder> holders)
    {
        if (holders.Count == 0) return string.Empty;
        var named = holders.Take(MaxNamedHolders)
            .Select(h => string.Create(CultureInfo.InvariantCulture,
                $"{h.Name} ({(h.Write ? "write" : "read")} lock, held {h.Held.TotalSeconds:0.#} s)"));
        var more = holders.Count > MaxNamedHolders ? $" and {holders.Count - MaxNamedHolders} more" : string.Empty;
        return "Held by " + string.Join(", ", named) + more + ".";
    }

    private static string BuildMessage(TimeSpan waited, IReadOnlyList<StoreLockHolder> holders, string requester)
    {
        var text = string.Create(CultureInfo.InvariantCulture,
            $"Could not acquire the local database write lock for {requester} within {waited.TotalSeconds:0.#} s.");
        var who = FormatHolders(holders);
        return who.Length == 0 ? text : text + " " + who;
    }
}
