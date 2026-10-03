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
/// collectors ask at the same moment. Across processes the file is created with
/// <see cref="FileMode.CreateNew"/>, which only one creator can win; a loser re-reads the winner's file and uses
/// its id. The creator holds the file exclusively while it writes, so a reader that finds it locked waits
/// for the writer instead of treating the lock as a damaged file.</para>
///
/// <para><b>Rebuilt when it does not fit.</b> A value that is not eight lowercase hex digits, a file that cannot be
/// read, and a file made for a different machine or Windows user (a data folder copied to another machine or
/// opened by another user) each make a new id, and one Warning that names the old id (or "invalid") and the new
/// one. The rewrite goes through a temp file and a replace, so a reader never sees half a file. Sessions made
/// under the old id are left alone: the install it belonged to may still be running.</para>
///
/// <para><b>Never lost to a bad folder.</b> If the new id cannot be saved the store still answers, with the same id
/// for the rest of the run, and says so in its Warning; the next start makes another.</para>
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

    /// <summary>The process-wide lock the first resolve runs under, whichever store object asks.</summary>
    private static readonly object s_resolveLock = new();

    private static readonly JsonSerializerOptions s_writeOptions = new() { WriteIndented = true };

    private readonly string _directory;
    private readonly string _machineName;
    private readonly string _userSid;
    private string? _id;
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

    /// <summary>Why this store has no id right now, or null while it has one (or has not tried).</summary>
    internal string? Failure => null;

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
    /// This install's id. The first call resolves it (reads the file, or makes it) under the process-wide lock, so
    /// parallel first sweeps share one resolve; every later call returns the cached value without touching the
    /// disk.
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
            if (_id is null)
            {
                Interlocked.Increment(ref _resolveCount);
                _id = Resolve();
                AppLogger.Info(LogSource, $"This install's id is {_id}.");
            }

            return _id;
        }
    }

    private string Resolve()
    {
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
                AppLogger.Warn(LogSource,
                    $"A new install id {candidate} was made but could not be saved to {FilePath} ({saveFailure}), " +
                    "so it holds for this run only and the next start makes another.");
                return candidate;
            }

            /* Another process made the file first. Its id is the install's id: read it back. */
            read = ReadFile();
        }

        if (read.File is { } file && Matches(file))
        {
            return file.Id!;
        }

        return Replace(read);
    }

    /// <summary>Makes a new id, writes it over the file, and says so in one Warning that names both ids.</summary>
    private string Replace(ReadResult read)
    {
        var oldId = read.File is { } old && InstallId.IsValid(old.Id) ? old.Id! : null;
        var newId = InstallId.NewId();

        var why = read.Kind switch
        {
            ReadKind.Unreadable => $"The file could not be read ({read.Detail}).",
            ReadKind.Missing => "The file went missing while it was being made.",
            _ when oldId is null => "The id in the file is not eight lowercase hex digits.",
            _ => "The file was made for a different machine or Windows user.",
        };

        string? saveFailure = null;
        try
        {
            WriteReplacing(newId);
        }
        catch (Exception ex)
        {
            saveFailure = $"{ex.GetType().Name}: {ex.Message}";
        }

        var tail = saveFailure is null
            ? (oldId is null ? "" : " Sessions made under the old id are left alone.")
            : $" The new id could not be saved ({saveFailure}), so it holds for this run only and the next start makes another.";
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
    /// Creates the file with <see cref="FileMode.CreateNew"/> and writes the id while holding it exclusively.
    /// "Conflict" means the file already exists, which is the other process winning; anything else that goes
    /// wrong is "not saved".
    /// </summary>
    private CreateOutcome TryCreate(string id, out string? failure)
    {
        failure = null;
        FileStream stream;
        try
        {
            Directory.CreateDirectory(_directory);
            stream = new FileStream(FilePath, FileMode.CreateNew, FileAccess.Write, FileShare.None);
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

        try
        {
            using (stream)
            {
                stream.Write(Serialize(id));
            }

            return CreateOutcome.Created;
        }
        catch (Exception ex)
        {
            /* Do not leave a half-written file for the next start to trip over. */
            try { File.Delete(FilePath); } catch { /* best effort: the next start replaces a damaged file */ }
            failure = $"{ex.GetType().Name}: {ex.Message}";
            return CreateOutcome.NotSaved;
        }
    }

    /// <summary>Writes a temp file beside the id file and moves it over, so a reader never sees half a file.</summary>
    private void WriteReplacing(string id)
    {
        Directory.CreateDirectory(_directory);
        var temp = Path.Combine(_directory, $"{FileName}.{Guid.NewGuid():N}.tmp");
        try
        {
            File.WriteAllBytes(temp, Serialize(id));
            File.Move(temp, FilePath, overwrite: true);
        }
        catch
        {
            try { File.Delete(temp); } catch { /* best effort */ }
            throw;
        }
    }

    /// <summary>
    /// Reads the file. A file that is locked (the other process is still writing it) is waited for, up to a
    /// couple of seconds, rather than read as damaged; a missing file is a first start; anything else that cannot
    /// be read as the id record is unreadable.
    /// </summary>
    private ReadResult ReadFile()
    {
        var waited = Stopwatch.StartNew();
        while (true)
        {
            try
            {
                string text;
                using (var stream = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete))
                using (var reader = new StreamReader(stream))
                {
                    text = reader.ReadToEnd();
                }

                var file = JsonSerializer.Deserialize<IdFile>(text);
                return file is null
                    ? new ReadResult(ReadKind.Unreadable, null, "the file holds no record")
                    : new ReadResult(ReadKind.Read, file, "");
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
                return new ReadResult(ReadKind.Unreadable, null, ex.GetType().Name);
            }
        }
    }

    private enum ReadKind { Missing, Unreadable, Read }

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
