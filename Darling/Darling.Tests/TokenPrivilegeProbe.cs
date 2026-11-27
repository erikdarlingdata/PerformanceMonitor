/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Security.Principal;
using System.Text;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Reads which of SeBackupPrivilege, SeRestorePrivilege and SeTakeOwnershipPrivilege are ENABLED on this process's token (#9898).
/// A normal elevated token holds the three switched off, and the ACL tests assume that: a folder with no grant for the caller cannot
/// be listed, a deny on WRITE_DAC and WRITE_OWNER holds, and the installer's owner step is what turns the privileges on. A token that
/// already has them enabled (an OpenSSH logon on Windows hands one out) passes every ACL check, so those tests cannot hold there and
/// skip with the privilege named. On a normal token nothing is skipped.
/// </summary>
internal static class TokenPrivilegeProbe
{
    private static readonly string[] Watched = ["SeBackupPrivilege", "SeRestorePrivilege", "SeTakeOwnershipPrivilege"];

    private const int TokenPrivilegesClass = 3;
    private const uint SePrivilegeEnabled = 0x2;
    private const int PrivilegeEntryOffset = 4;
    private const int PrivilegeEntrySize = 12;
    private const int PrivilegeAttributesOffset = 8;

    [DllImport("advapi32.dll", SetLastError = true)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetTokenInformation(IntPtr token, int infoClass, IntPtr info, int length, out int returned);

    [DllImport("advapi32.dll", SetLastError = true, CharSet = CharSet.Unicode, EntryPoint = "LookupPrivilegeNameW")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool LookupPrivilegeName(string? systemName, IntPtr luid, StringBuilder name, ref int length);

    /// <summary>The watched privileges that are enabled on this process's token, empty on a normal token or off Windows.</summary>
    public static IReadOnlyList<string> EnabledWatched()
    {
        var enabled = new List<string>();
        if (!OperatingSystem.IsWindows())
        {
            return enabled;
        }

        using var identity = WindowsIdentity.GetCurrent();
        var token = identity.AccessToken.DangerousGetHandle();
        GetTokenInformation(token, TokenPrivilegesClass, IntPtr.Zero, 0, out var needed);
        if (needed <= 0)
        {
            throw new InvalidOperationException($"GetTokenInformation(TokenPrivileges) reported no size (error {Marshal.GetLastWin32Error()}).");
        }

        var buffer = Marshal.AllocHGlobal(needed);
        try
        {
            if (!GetTokenInformation(token, TokenPrivilegesClass, buffer, needed, out _))
            {
                throw new InvalidOperationException($"GetTokenInformation(TokenPrivileges) failed (error {Marshal.GetLastWin32Error()}).");
            }

            var count = Marshal.ReadInt32(buffer);
            for (var i = 0; i < count; i++)
            {
                var entry = IntPtr.Add(buffer, PrivilegeEntryOffset + (i * PrivilegeEntrySize));
                var attributes = unchecked((uint)Marshal.ReadInt32(entry, PrivilegeAttributesOffset));
                if ((attributes & SePrivilegeEnabled) == 0)
                {
                    continue;
                }

                var name = new StringBuilder(64);
                var length = name.Capacity;
                if (LookupPrivilegeName(null, entry, name, ref length) && Array.IndexOf(Watched, name.ToString()) >= 0)
                {
                    enabled.Add(name.ToString());
                }
            }
        }
        finally
        {
            Marshal.FreeHGlobal(buffer);
        }

        return enabled;
    }

    /// <summary>Skips the calling test when any of the three privileges is enabled on this token, naming each one.</summary>
    public static void SkipWhenAnyEnabled()
    {
        var enabled = EnabledWatched();
        Assert.SkipWhen(
            enabled.Count > 0,
            $"this process's token has {string.Join(", ", enabled)} enabled, which passes the ACL checks this test relies on (a normal elevated token holds them disabled)");
    }
}
