/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Security.Principal;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// Lite's install id (#4961): the eight characters (see <see cref="InstallId"/>) that tell this install's
/// Extended Events sessions from another install's, kept in its own file at the data root,
/// <c>install-id.json</c>, beside the machine name and Windows user SID it was made for.
///
/// <para><b>Made once.</b> A process-wide lock and a cache make the first resolve happen once however many
/// collectors ask at the same moment. Across processes a new id is written to a temp file in the same folder and
/// moved to its place without replacing anything, which only one creator can win and which a crash cannot leave
/// half done: the final path never holds a partial file. A loser re-reads the winner's file and uses its id. A reader
/// that finds the file locked waits for the holder instead of treating the lock as a damaged file.</para>
///
/// <para><b>Rebuilt when its content does not fit.</b> A value that is not eight lowercase hex digits, a file that is
/// not the id record (not JSON, or no record), and a file made for a different machine or Windows user (a data folder
/// copied to another machine or opened by another user) each make a new id, and one Warning that names the old id (or
/// "invalid") and the new one. The rewrite goes through a temp file and a replace, so a reader never sees half a file.
/// Sessions made under the old id are left alone: the install it belonged to may still be running.</para>
///
/// <para><b>Never replaced unread, never used unsaved.</b> A file that exists but cannot be read (an I/O error, or a lock
/// held past the wait) says nothing about the id in it, so it is left alone and the store has no id: the callers create,
/// start and drop no session, and ask again on a later cycle, no sooner than a minute after the last try. A new id that
/// cannot be saved is not used either, because sessions made under it could not be found again after a restart. The first
/// failure is one Warning with the error; the repeats are Debug until an id is read.</para>
///
/// <para>Import Settings never carries this file (<see cref="SettingsImport"/>): two installs that shared an id
/// would drop each other's sessions.</para>
/// </summary>
public sealed class InstallIdStore
{
    /// <summary>The file at the data root that holds the id and the binding.</summary>
    public const string FileName = "install-id.json";

    private const string LogSource = "InstallId";

    /// <summary>
    /// How long a read waits for another process that still holds the file open: the creator's few
    /// milliseconds of writing, with room for a scanner that opens a new file for a moment. Past this the file is
    /// read as unreadable.
    /// </summary>
    private static readonly TimeSpan s_lockedReadBudget = TimeSpan.FromSeconds(2);

    private static readonly TimeSpan s_lockedReadPause = TimeSpan.FromMilliseconds(20);

    /// <summary>How long a resolve that ended with no id waits before the next call goes back to the disk.</summary>
    private static readonly TimeSpan s_retryInterval = TimeSpan.FromSeconds(60);

    /// <summary>The process-wide lock the first resolve runs under, whichever store object asks.</summary>
    private static readonly object s_resolveLock = new();

    private static readonly JsonSerializerOptions s_writeOptions = new() { WriteIndented = true };

    private readonly string _directory;
    private readonly string _machineName;
    private readonly string _userSid;
    private string? _id;
    private string? _failure;
    private DateTime _retryAt = DateTime.MinValue;
    private bool _failureWarned;
    private int _resolveCount;

    /// <summary>
    /// The store takes the folder, the machine name and the Windows user SID rather than reading them itself, so a
    /// test never touches a real data root. <see cref="ForCurrentUser"/> builds the one the app uses.
    /// </summary>
    public InstallIdStore(string directory, string machineName, string? userSid)
    {
        _directory = directory;
        _machineName = machineName;
        _userSid = userSid ?? "";
        FilePath = Path.Combine(directory, FileName);
    }

    /// <summary>The full path of the id file.</summary>
    public string FilePath { get; }

    /// <summary>How many times this store has gone to the disk to resolve its id. A test reads it.</summary>
    internal int ResolveCount => Volatile.Read(ref _resolveCount);

    /// <summary>
    /// Runs in the moment between this store finding no file and creating one, so a test can make another
    /// process win that race at the exact point it matters. Nothing in the app sets it.
    /// </summary>
    internal Action? BeforeCreate { get; set; }

    /// <summary>
    /// Runs between a new id's temp file being written and its move into place, so a test can stop the create at the
    /// point a crash would. Nothing in the app sets it.
    /// </summary>
    internal Action? BeforeMove { get; set; }

    /// <summary>How long a read waits for a locked file. A test shortens it; the app keeps the default.</summary>
    internal TimeSpan LockedReadBudget { get; set; } = s_lockedReadBudget;

    /// <summary>The clock the retry interval runs on. A test sets it; the app keeps the real one.</summary>
    internal Func<DateTime> UtcNowForTests { get; set; } = () => DateTime.UtcNow;

    /// <summary>
    /// Why this store has no id right now (the file could not be read, or a new id could not be saved), or null while it
    /// has one or has not tried. The collection service puts it in the fault that says why no session was made.
    /// </summary>
    internal string? Failure => Volatile.Read(ref _failure);

    /// <summary>
    /// A store for the running user on this machine: the real machine name and the current Windows user's SID.
    /// </summary>
    public static InstallIdStore ForCurrentUser(string directory)
    {
        string? sid = null;
        try
        {
            using var identity = WindowsIdentity.GetCurrent();
            sid = identity.User?.Value;
        }
        catch (Exception ex)
        {
            /* The binding then rests on the machine alone; the id still works. */
            AppLogger.Warn(LogSource,
                $"The Windows user could not be read for the install id ({ex.GetType().Name}); the id is bound to the machine only.");
        }

        return new InstallIdStore(directory, Environment.MachineName, sid);
    }

    /// <summary>
    /// This install's id, or null while it cannot be had. The first call resolves it (reads the file, or makes it) under
    /// the process-wide lock, so parallel first sweeps share one resolve; every later call returns the cached value
    /// without touching the disk. A resolve that ends with no id (the file exists and cannot be read, or a new id cannot
    /// be saved) is not cached: calls within a minute answer null without touching the file, and the first call after it
    /// tries again.
    /// </summary>
    public string? GetId()
    {
        var cached = Volatile.Read(ref _id);
        if (cached is not null)
        {
            return cached;
        }

        lock (s_resolveLock)
        {
            if (_id is not null)
            {
                return _id;
            }

            if (UtcNowForTests() < _retryAt)
            {
                return null;
            }

            Interlocked.Increment(ref _resolveCount);
            var id = Resolve(out var failure);
            if (id is null)
            {
                _retryAt = UtcNowForTests().Add(s_retryInterval);
                Volatile.Write(ref _failure, failure);

                var line = $"This install has no id for now: {failure}. No Extended Events session is created, started or dropped without one. The next cycle tries again.";
                if (_failureWarned)
                {
                    AppLogger.Debug(LogSource, line);
                }
                else
                {
                    _failureWarned = true;
                    AppLogger.Warn(LogSource, line);
                }

                return null;
            }

            Volatile.Write(ref _failure, null);
            Volatile.Write(ref _id, id);
            AppLogger.Info(LogSource, $"This install's id is {id}.");
            return id;
        }
    }

    /// <summary>
    /// Reads the file, or makes it. Null (with <paramref name="failure"/> saying why) when the file exists and cannot be
    /// read, or when a new id cannot be saved.
    /// </summary>
    private string? Resolve(out string? failure)
    {
        failure = null;
        var read = ReadFile();

        if (read.Kind == ReadKind.Missing)
        {
            BeforeCreate?.Invoke();

            var candidate = InstallId.NewId();
            var created = TryCreate(candidate, out var saveFailure);
            if (created == CreateOutcome.Created)
            {
                return candidate;
            }

            if (created == CreateOutcome.NotSaved)
            {
                failure = $"a new id could not be saved to {FilePath} ({saveFailure})";
                return null;
            }

            /* Another process made the file first. Its id is the install's id: read it back. */
            read = ReadFile();
        }

        if (read.Kind == ReadKind.Unreadable)
        {
            /* The file is there and cannot be read right now: nothing is known about the id in it, so it is not replaced. */
            failure = $"the id file {FilePath} could not be read ({read.Detail})";
            return null;
        }

        if (read.File is { } file && Matches(file))
        {
            return file.Id;
        }

        return Replace(read, out failure);
    }

    /// <summary>
    /// Makes a new id for a file whose content does not fit, writes it over the file, and says so in one Warning that
    /// names both ids. Null (with <paramref name="failure"/>) when the new id cannot be saved: it is not used.
    /// </summary>
    private string? Replace(ReadResult read, out string? failure)
    {
        failure = null;
        var oldId = read.File is { } old && InstallId.IsValid(old.Id) ? old.Id! : null;
        var newId = InstallId.NewId();

        var why = read.Kind switch
        {
            ReadKind.Invalid => $"The file does not hold an id record ({read.Detail}).",
            ReadKind.Missing => "The file went missing while it was being made.",
            _ when oldId is null => "The id in the file is not eight lowercase hex digits.",
            _ => "The file was made for a different machine or Windows user.",
        };

        try
        {
            WriteReplacing(newId);
        }
        catch (Exception ex)
        {
            failure = $"a replacement id could not be saved to {FilePath} ({ex.GetType().Name}: {ex.Message})";
            return null;
        }

        var tail = oldId is null ? "" : " Sessions made under the old id are left alone.";
        AppLogger.Warn(LogSource,
            $"Install id replaced in {FilePath}: old id {oldId ?? "invalid"}, new id {newId}. {why}{tail}");

        return newId;
    }

    private bool Matches(IdFile file) =>
        InstallId.IsValid(file.Id)
        && string.Equals(file.Machine, _machineName, StringComparison.OrdinalIgnoreCase)
        && string.Equals(file.UserSid, _userSid, StringComparison.OrdinalIgnoreCase);

    private byte[] Serialize(string id) =>
        JsonSerializer.SerializeToUtf8Bytes(new IdFile { Id = id, Machine = _machineName, UserSid = _userSid }, s_writeOptions);

    /// <summary>
    /// Writes the id to a temp file beside the id file and moves it into place without replacing anything, so the final
    /// path never holds a partial file, whatever stops the write. "Conflict" means the final path already exists, which
    /// is the other process winning (its file is left as it is); anything else that goes wrong is "not saved".
    /// </summary>
    private CreateOutcome TryCreate(string id, out string? failure)
    {
        failure = null;
        string? temp = null;
        try
        {
            temp = WriteTemp(id);
            BeforeMove?.Invoke();
            File.Move(temp, FilePath, overwrite: false);
            temp = null;
            return CreateOutcome.Created;
        }
        catch (IOException) when (File.Exists(FilePath))
        {
            return CreateOutcome.Conflict;
        }
        catch (Exception ex)
        {
            failure = $"{ex.GetType().Name}: {ex.Message}";
            return CreateOutcome.NotSaved;
        }
        finally
        {
            if (temp is not null)
            {
                try { File.Delete(temp); } catch { /* best effort: a leftover temp is never read as the id file */ }
            }
        }
    }

    /// <summary>Writes the id to a new temp file beside the id file and returns its path.</summary>
    private string WriteTemp(string id)
    {
        Directory.CreateDirectory(_directory);
        var temp = Path.Combine(_directory, $"{FileName}.{Guid.NewGuid():N}.tmp");
        try
        {
            File.WriteAllBytes(temp, Serialize(id));
            return temp;
        }
        catch
        {
            try { File.Delete(temp); } catch { /* best effort */ }
            throw;
        }
    }

    /// <summary>Writes a temp file beside the id file and moves it over, so a reader never sees half a file.</summary>
    private void WriteReplacing(string id)
    {
        var temp = WriteTemp(id);
        try
        {
            File.Move(temp, FilePath, overwrite: true);
        }
        catch
        {
            try { File.Delete(temp); } catch { /* best effort */ }
            throw;
        }
    }

    /// <summary>
    /// Reads the file. A file that is locked (the other process is still writing it) is waited for, up to a couple of
    /// seconds, rather than read as damaged; a missing file is a first start. A file that is there and still cannot be
    /// read after the wait (an I/O error, a lock held past it, access refused) is unreadable, which is never a reason to
    /// replace it. A file that is read but is not the id record is invalid, which is.
    /// </summary>
    private ReadResult ReadFile()
    {
        var waited = Stopwatch.StartNew();
        string text;
        while (true)
        {
            try
            {
                using var stream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
                using var reader = new StreamReader(stream);
                text = reader.ReadToEnd();
                break;
            }
            catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
            {
                return new ReadResult(ReadKind.Missing, null, "");
            }
            catch (IOException) when (waited.Elapsed < LockedReadBudget)
            {
                Thread.Sleep(s_lockedReadPause);
            }
            catch (Exception ex)
            {
                return new ReadResult(ReadKind.Unreadable, null, $"{ex.GetType().Name}: {ex.Message}");
            }
        }

        try
        {
            var file = JsonSerializer.Deserialize<IdFile>(text);
            return file is null
                ? new ReadResult(ReadKind.Invalid, null, "the file holds no record")
                : new ReadResult(ReadKind.Read, file, "");
        }
        catch (JsonException ex)
        {
            return new ReadResult(ReadKind.Invalid, null, ex.GetType().Name);
        }
    }

    private enum ReadKind { Missing, Unreadable, Invalid, Read }

    private enum CreateOutcome { Created, Conflict, NotSaved }

    private readonly record struct ReadResult(ReadKind Kind, IdFile? File, string Detail);

    /// <summary>The file's record: the id, and the machine and Windows user it was made for.</summary>
    private sealed class IdFile
    {
        [JsonPropertyName("id")]
        public string? Id { get; set; }

        [JsonPropertyName("machine")]
        public string? Machine { get; set; }

        [JsonPropertyName("userSid")]
        public string? UserSid { get; set; }
    }
}
