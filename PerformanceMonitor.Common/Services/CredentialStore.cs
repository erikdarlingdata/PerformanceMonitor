/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.ComponentModel;
using System.Runtime.InteropServices;
using CredentialManagement;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Common;

/// <summary>
/// Secure credential storage backed by Windows Credential Manager. Credentials are
/// encrypted by Windows using DPAPI and stored per-user. NEVER stores passwords in plain text.
///
/// <para>
/// Shared by Dashboard and Lite. The <c>credentialPrefix</c> namespaces each app's stored
/// secrets so they never collide in Windows Credential Manager: Dashboard uses
/// <c>"PerformanceMonitor_"</c>, Lite uses <c>"PerformanceMonitorLite_"</c>. Those prefixes
/// are LOAD-BEARING and must stay distinct — unifying them would let one app read the other's
/// credentials. Each app wraps this type in a thin adapter that supplies its own prefix.
/// </para>
/// </summary>
public class CredentialStore
{
    /// <summary>
    /// The largest credential blob Windows accepts (CRED_MAX_CREDENTIAL_BLOB_SIZE: 5 * 512 bytes).
    /// </summary>
    public const int MaxSecretBytes = 2560;

    /// <summary>
    /// The longest secret this store can save: <see cref="MaxSecretBytes"/> divided by the two bytes each
    /// UTF-16 character takes (1,280 characters). The CredentialManagement package this store used to write
    /// through stopped at 512 characters (<c>Password.Length &gt; 512</c>), which is below the Windows limit
    /// (#5772).
    /// </summary>
    public const int MaxSecretLength = MaxSecretBytes / 2;

    private const string DefaultDescription = "SQL Server credentials for Performance Monitor";

    private readonly string _credentialPrefix;
    private readonly string _description;
    private readonly ILogger? _logger;

    /// <summary>
    /// Creates a credential store for the given prefix using the default credential description.
    /// </summary>
    /// <param name="credentialPrefix">
    /// Per-app prefix prepended to every credential target (e.g. <c>"PerformanceMonitor_"</c>).
    /// Must be non-empty and distinct per app.
    /// </param>
    /// <param name="logger">Optional logger; failures are logged as warnings.</param>
    public CredentialStore(string credentialPrefix, ILogger? logger = null)
        : this(credentialPrefix, DefaultDescription, logger)
    {
    }

    /// <summary>
    /// Creates a credential store for the given prefix and credential description.
    /// </summary>
    /// <param name="credentialPrefix">
    /// Per-app prefix prepended to every credential target (e.g. <c>"PerformanceMonitor_"</c>).
    /// Must be non-empty and distinct per app.
    /// </param>
    /// <param name="description">Description stored on each credential entry in Credential Manager.</param>
    /// <param name="logger">Optional logger; failures are logged as warnings.</param>
    public CredentialStore(string credentialPrefix, string description, ILogger? logger = null)
    {
        if (string.IsNullOrWhiteSpace(credentialPrefix))
        {
            throw new ArgumentException("Credential prefix cannot be null or empty", nameof(credentialPrefix));
        }

        _credentialPrefix = credentialPrefix;
        _description = description;
        _logger = logger;
    }

    /// <summary>
    /// Saves SQL Server credentials to Windows Credential Manager.
    /// Credentials are encrypted automatically by Windows. A secret of up to
    /// <see cref="MaxSecretLength"/> characters is saved; use <see cref="TrySaveCredential"/> when the
    /// caller needs to tell the user why a save failed.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <param name="username">SQL Server username</param>
    /// <param name="password">SQL Server password (will be encrypted)</param>
    /// <returns>True if successful, false otherwise</returns>
    public bool SaveCredential(string serverId, string username, string password)
        => TrySaveCredential(serverId, username, password).Succeeded;

    /// <summary>
    /// Saves a credential through CredWriteW and reports why it failed, so a caller can show the user the
    /// reason instead of a bare false (#5772). The entry is laid out exactly as the CredentialManagement
    /// package's <c>Credential.Save()</c> lays it out - target <c>{prefix}{serverId}</c>, type Generic,
    /// persistence LocalComputer, this store's description as the comment, and the secret as UTF-16LE with no
    /// terminating null (blob size = 2 * length) - so entries written by older builds read back here and
    /// entries written here read back through the package. The only difference is the length ceiling:
    /// <see cref="MaxSecretLength"/> characters instead of 512.
    /// <para>
    /// The failure text never contains the secret: it names the length and the Windows error only.
    /// </para>
    /// </summary>
    /// <param name="serverId">Unique key; the Credential Manager target is the prefix plus this.</param>
    /// <param name="username">User name stored on the entry (not secret).</param>
    /// <param name="password">The secret to store; may be empty.</param>
    public CredentialSaveResult TrySaveCredential(string serverId, string username, string password)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }

        if (string.IsNullOrWhiteSpace(username))
        {
            throw new ArgumentException("Username cannot be null or empty", nameof(username));
        }

        if (password == null)
        {
            throw new ArgumentNullException(nameof(password));
        }

        if (password.Length > MaxSecretLength)
        {
            var tooLong = $"it is {password.Length:N0} characters long, and Windows Credential Manager "
                + $"stores at most {MaxSecretLength:N0}";
            _logger?.LogWarning("Failed to save credential for server {ServerId}: {Error}", serverId, tooLong);
            return CredentialSaveResult.Failed(tooLong);
        }

        IntPtr blob = IntPtr.Zero;
        try
        {
            /* Same blob the package builds: StringToCoTaskMemUni's buffer, but sized to the characters only
               (the terminating null it appends is not counted). */
            blob = Marshal.StringToCoTaskMemUni(password);
            var native = new NativeCredential
            {
                Flags = 0,
                Type = CredTypeGeneric,
                TargetName = GetCredentialTarget(serverId),
                Comment = _description,
                CredentialBlobSize = password.Length * 2,
                CredentialBlob = blob,
                Persist = CredPersistLocalMachine,
                UserName = username
            };

            if (NativeMethods.CredWrite(ref native, 0))
            {
                return CredentialSaveResult.Ok;
            }

            var error = Marshal.GetLastWin32Error();
            var reason = $"Windows returned error {error} ({new Win32Exception(error).Message.TrimEnd('.', ' ', '\r', '\n')})";
            _logger?.LogWarning("Failed to save credential for server {ServerId}: {Error}", serverId, reason);
            return CredentialSaveResult.Failed(reason);
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to save credential for server {ServerId}: {Error}", serverId, ex.Message);
            return CredentialSaveResult.Failed(ex.Message);
        }
        finally
        {
            if (blob != IntPtr.Zero)
            {
                Marshal.ZeroFreeCoTaskMemUnicode(blob);
            }
        }
    }

    /// <summary>
    /// Retrieves SQL Server credentials from Windows Credential Manager, reading the entry with CredReadW
    /// and decoding the blob as UTF-16LE exactly as the CredentialManagement package's
    /// <c>Credential.Load()</c> does, so entries written by either writer come back (#5772).
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <returns>Tuple of (username, password) or null if not found</returns>
    public (string Username, string Password)? GetCredential(string serverId)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }

        try
        {
            if (!NativeMethods.CredRead(GetCredentialTarget(serverId), CredTypeGeneric, 0, out var ptr))
            {
                var error = Marshal.GetLastWin32Error();
                if (error != ErrorNotFound)
                {
                    _logger?.LogWarning("Failed to load credential for server {ServerId}: Windows returned error {Error}", serverId, error);
                }

                return null;
            }

            try
            {
                var native = Marshal.PtrToStructure<NativeCredential>(ptr);
                var password = native.CredentialBlobSize > 0
                    ? Marshal.PtrToStringUni(native.CredentialBlob, native.CredentialBlobSize / 2)
                    : string.Empty;
                return (native.UserName ?? string.Empty, password);
            }
            finally
            {
                NativeMethods.CredFree(ptr);
            }
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to load credential for server {ServerId}: {Error}", serverId, ex.Message);
        }

        return null;
    }

    /// <summary>
    /// Deletes SQL Server credentials from Windows Credential Manager.
    /// Should be called when a server is removed.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <returns>True if deleted or didn't exist, false on error</returns>
    public bool DeleteCredential(string serverId)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            throw new ArgumentException("Server ID cannot be null or empty", nameof(serverId));
        }

        try
        {
            using var credential = new Credential
            {
                Target = GetCredentialTarget(serverId),
                Type = CredentialType.Generic
            };

            return credential.Delete();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to delete credential for server {ServerId}: {Error}", serverId, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Checks if credentials exist for a server.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <returns>True if credentials exist in Credential Manager</returns>
    public bool CredentialExists(string serverId)
    {
        if (string.IsNullOrWhiteSpace(serverId))
        {
            return false;
        }

        try
        {
            using var credential = new Credential
            {
                Target = GetCredentialTarget(serverId),
                Type = CredentialType.Generic
            };

            return credential.Exists();
        }
        catch (Exception ex)
        {
            _logger?.LogWarning(ex, "Failed to check credential existence for server {ServerId}: {Error}", serverId, ex.Message);
            return false;
        }
    }

    /// <summary>
    /// Updates existing credentials. If credentials don't exist, creates them.
    /// </summary>
    /// <param name="serverId">Unique server identifier</param>
    /// <param name="username">SQL Server username</param>
    /// <param name="password">SQL Server password (will be encrypted)</param>
    /// <returns>True if successful, false otherwise</returns>
    public bool UpdateCredential(string serverId, string username, string password)
    {
        DeleteCredential(serverId);
        return SaveCredential(serverId, username, password);
    }

    /// <summary>
    /// Generates the credential target name for Windows Credential Manager.
    /// Format: {credentialPrefix}{serverId}
    /// </summary>
    private string GetCredentialTarget(string serverId)
    {
        return $"{_credentialPrefix}{serverId}";
    }

    private const int CredTypeGeneric = 1;
    private const int CredPersistLocalMachine = 2;
    private const int ErrorNotFound = 1168;

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct NativeCredential
    {
        public int Flags;
        public int Type;
        public string TargetName;
        public string Comment;
        public long LastWritten;
        public int CredentialBlobSize;
        public IntPtr CredentialBlob;
        public int Persist;
        public int AttributeCount;
        public IntPtr Attributes;
        public string TargetAlias;
        public string UserName;
    }

    private static class NativeMethods
    {
        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, EntryPoint = "CredWriteW", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool CredWrite([In] ref NativeCredential credential, uint flags);

        [DllImport("advapi32.dll", CharSet = CharSet.Unicode, EntryPoint = "CredReadW", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static extern bool CredRead(string target, int type, int reservedFlag, out IntPtr credential);

        [DllImport("advapi32.dll", SetLastError = true)]
        internal static extern void CredFree(IntPtr buffer);
    }
}

/// <summary>
/// The outcome of <see cref="CredentialStore.TrySaveCredential"/>. <see cref="Failure"/> is a short reason
/// fit to put after "could not be saved because"; it never contains the secret.
/// </summary>
public sealed class CredentialSaveResult
{
    private CredentialSaveResult(bool succeeded, string? failure)
    {
        Succeeded = succeeded;
        Failure = failure;
    }

    /// <summary>True when the credential reached Windows Credential Manager.</summary>
    public bool Succeeded { get; }

    /// <summary>Why the save failed; null when it succeeded.</summary>
    public string? Failure { get; }

    internal static CredentialSaveResult Ok { get; } = new(true, null);

    internal static CredentialSaveResult Failed(string reason) => new(false, reason);
}
