/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The password key this viewer has seen for each store it connects to (#5366): the full SHA-256 of the key's public
/// part, saved once per store connection and compared at every save of a password. A file that cannot be read counts
/// as no saved keys, is kept as <c>.bad</c> when the next key is saved, and <see cref="Find"/> says so, so the connect can tell the operator. Every read and write is
/// guarded: a failure never stops the viewer.
/// </summary>
public sealed class ViewerPasswordKeyPins
{
    private const string LogSource = "ViewerPasswordKeyPins";

    private static readonly JsonSerializerOptions s_json = new() { WriteIndented = true };

    private readonly string _path;

    public ViewerPasswordKeyPins(string? path = null) => _path = path ?? DefaultPath();

    /// <summary>%APPDATA%\PerformanceMonitorDarling\password-key-pins.json.</summary>
    public static string DefaultPath() => Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData),
        "PerformanceMonitorDarling", "password-key-pins.json");

    /// <summary>One saved key: the store it belongs to, the full fingerprint as 64 lowercase hex characters, the key id and
    /// when it was saved.</summary>
    public sealed class PinEntry
    {
        public string Store { get; set; } = "";
        public string Fingerprint { get; set; } = "";
        public string KeyId { get; set; } = "";
        public DateTime PinnedAtUtc { get; set; }
    }

    /// <summary>
    /// The saved fingerprint (64 lowercase hex characters) for <paramref name="store"/>, or null when there is none.
    /// <paramref name="unreadable"/> is true when the answer is null because the file could not be read or holds no usable
    /// list, because the entry for this store is not a 64-character hex fingerprint, or because an earlier unreadable file
    /// was set aside as <c>.bad</c> and this store has no entry since.
    /// </summary>
    public string? Find(string store, out bool unreadable)
    {
        var entries = ReadAll(out unreadable);
        var entry = entries.FirstOrDefault(e => string.Equals(e.Store, store, StringComparison.Ordinal));
        if (entry is not null)
        {
            if (IsFingerprint(entry.Fingerprint))
            {
                unreadable = false;
                return entry.Fingerprint;
            }

            unreadable = true;
            return null;
        }

        unreadable |= File.Exists(BadPath);
        return null;
    }

    /// <summary>Saves (or replaces) the fingerprint for <paramref name="store"/>. Returns false when the file could not be
    /// written. A file that cannot be read is renamed to <c>.bad</c> (replacing an older one) before the new list, which
    /// holds only this entry, is written.</summary>
    public bool Save(string store, byte[] fingerprint, string keyId)
    {
        try
        {
            var all = ReadAll(out var unreadable);
            var entries = all.Where(e => !string.Equals(e.Store, store, StringComparison.Ordinal)).ToList();
            var current = all.FirstOrDefault(e => string.Equals(e.Store, store, StringComparison.Ordinal));
            var directory = Path.GetDirectoryName(_path);
            if (!string.IsNullOrEmpty(directory))
            {
                Directory.CreateDirectory(directory);
            }

            if ((unreadable || (current is not null && !IsFingerprint(current.Fingerprint))) && File.Exists(_path))
            {
                File.Move(_path, BadPath, overwrite: true);
            }

            entries.Add(new PinEntry
            {
                Store = store,
                Fingerprint = Convert.ToHexString(fingerprint).ToLowerInvariant(),
                KeyId = keyId,
                PinnedAtUtc = DateTime.UtcNow,
            });

            var temp = _path + ".tmp";
            File.WriteAllText(temp, JsonSerializer.Serialize(entries, s_json));
            File.Move(temp, _path, overwrite: true);
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or NotSupportedException)
        {
            ViewerLogger.Warn(LogSource, "The saved password keys could not be written: " + ex.Message);
            return false;
        }
    }

    private string BadPath => _path + ".bad";

    private static bool IsFingerprint(string? text) =>
        text is { Length: 64 } && text.All(c => c is (>= '0' and <= '9') or (>= 'a' and <= 'f'));

    private List<PinEntry> ReadAll(out bool unreadable)
    {
        unreadable = false;
        try
        {
            if (!File.Exists(_path))
            {
                return [];
            }

            return JsonSerializer.Deserialize<List<PinEntry>>(File.ReadAllText(_path)) ?? [];
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or NotSupportedException)
        {
            ViewerLogger.Warn(LogSource, "The saved password keys could not be read: " + ex.Message);
            unreadable = true;
            return [];
        }
    }
}
