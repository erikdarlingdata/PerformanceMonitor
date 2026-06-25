/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Principal;

namespace PerformanceMonitor.Ui
{
    /// <summary>
    /// Measures the facts the upgrade handoff needs about another running process — its release
    /// version (read from the on-disk exe, which works cross-integrity for the same user) and its
    /// mandatory-integrity level — plus our own integrity and whether we can elevate same-user.
    /// Behind an interface so <see cref="SingleInstanceCoordinator"/> can be tested with a fake
    /// and the brittle Win32 stays isolated. Every method fails CLOSED (returns null/false) so a
    /// denied or unexpected call degrades to "surface", never a crash or a false elevation prompt.
    /// </summary>
    public interface IProcessInspector
    {
        /// <summary>Release version (ProductVersion of the on-disk exe) for the given pid, or null if unreadable.</summary>
        Version? GetReleaseVersion(int pid);

        /// <summary>Mandatory-integrity RID (e.g. 0x3000 = High) for the given pid, or null if it couldn't be measured.</summary>
        int? GetIntegrityLevel(int pid);

        /// <summary>This process's mandatory-integrity RID; falls back to Medium (0x2000) if it can't be read.</summary>
        int CurrentIntegrityLevel();

        /// <summary>True if the current user could elevate in place (already elevated, split-token admin, or a UAC-off / built-in admin).</summary>
        bool CanElevateSameUser();
    }

    /// <summary>Real Win32-backed <see cref="IProcessInspector"/>.</summary>
    public sealed class Win32ProcessInspector : IProcessInspector
    {
        public const int IntegrityMedium = 0x2000;
        public const int IntegrityHigh = 0x3000;

        public Version? GetReleaseVersion(int pid)
        {
            try
            {
                var path = QueryImagePath(pid);
                if (string.IsNullOrEmpty(path))
                {
                    return null;
                }

                /* Read the version from the on-disk file (world-readable), NOT process memory —
                   FileVersionInfo here, not Process.MainModule, so an elevated same-user instance
                   is still readable from a non-elevated launcher. */
                var info = FileVersionInfo.GetVersionInfo(path);
                return SingleInstanceDecision.ParseProductVersion(info.ProductVersion)
                    ?? SingleInstanceDecision.ParseProductVersion(info.FileVersion);
            }
            catch
            {
                return null;
            }
        }

        public int? GetIntegrityLevel(int pid)
        {
            IntPtr process = IntPtr.Zero;
            try
            {
                process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, (uint)pid);
                if (process == IntPtr.Zero)
                {
                    return null;
                }

                return ReadIntegrityLevel(process);
            }
            catch
            {
                return null;
            }
            finally
            {
                if (process != IntPtr.Zero)
                {
                    CloseHandle(process);
                }
            }
        }

        public int CurrentIntegrityLevel()
        {
            try
            {
                var level = ReadIntegrityLevel(GetCurrentProcess());
                return level ?? IntegrityMedium;
            }
            catch
            {
                return IntegrityMedium;
            }
        }

        public bool CanElevateSameUser()
        {
            try
            {
                /* Already running elevated → we can act as admin. */
                using var identity = WindowsIdentity.GetCurrent();
                if (new WindowsPrincipal(identity).IsInRole(WindowsBuiltInRole.Administrator))
                {
                    return true;
                }

                /* Not elevated: a split-token admin can elevate in place; a true standard user cannot
                   (runas would require *different* admin credentials → a different user/profile). */
                var type = GetElevationType();
                return type == TokenElevationType.Limited;
            }
            catch
            {
                return false;
            }
        }

        private static string? QueryImagePath(int pid)
        {
            IntPtr process = IntPtr.Zero;
            try
            {
                process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, false, (uint)pid);
                if (process == IntPtr.Zero)
                {
                    return null;
                }

                var buffer = new char[1024];
                uint size = (uint)buffer.Length;
                return QueryFullProcessImageName(process, 0, buffer, ref size) ? new string(buffer, 0, (int)size) : null;
            }
            finally
            {
                if (process != IntPtr.Zero)
                {
                    CloseHandle(process);
                }
            }
        }

        private static int? ReadIntegrityLevel(IntPtr process)
        {
            IntPtr token = IntPtr.Zero;
            IntPtr info = IntPtr.Zero;
            try
            {
                if (!OpenProcessToken(process, TOKEN_QUERY, out token))
                {
                    return null;
                }

                GetTokenInformation(token, TokenInformationClass.TokenIntegrityLevel, IntPtr.Zero, 0, out uint needed);
                if (needed == 0)
                {
                    return null;
                }

                info = Marshal.AllocHGlobal((int)needed);
                if (!GetTokenInformation(token, TokenInformationClass.TokenIntegrityLevel, info, needed, out _))
                {
                    return null;
                }

                var label = Marshal.PtrToStructure<TOKEN_MANDATORY_LABEL>(info);
                var sid = label.Label.Sid;
                int subAuthorityCount = Marshal.ReadByte(GetSidSubAuthorityCount(sid));
                if (subAuthorityCount == 0)
                {
                    return null;
                }

                IntPtr ridPtr = GetSidSubAuthority(sid, (uint)(subAuthorityCount - 1));
                return Marshal.ReadInt32(ridPtr);
            }
            catch
            {
                return null;
            }
            finally
            {
                if (info != IntPtr.Zero)
                {
                    Marshal.FreeHGlobal(info);
                }
                if (token != IntPtr.Zero)
                {
                    CloseHandle(token);
                }
            }
        }

        private static TokenElevationType GetElevationType()
        {
            IntPtr token = IntPtr.Zero;
            try
            {
                if (!OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, out token))
                {
                    return TokenElevationType.Default;
                }

                uint size = sizeof(int);
                IntPtr buffer = Marshal.AllocHGlobal((int)size);
                try
                {
                    if (!GetTokenInformation(token, TokenInformationClass.TokenElevationType, buffer, size, out _))
                    {
                        return TokenElevationType.Default;
                    }
                    return (TokenElevationType)Marshal.ReadInt32(buffer);
                }
                finally
                {
                    Marshal.FreeHGlobal(buffer);
                }
            }
            catch
            {
                return TokenElevationType.Default;
            }
            finally
            {
                if (token != IntPtr.Zero)
                {
                    CloseHandle(token);
                }
            }
        }

        private enum TokenElevationType
        {
            Default = 1,
            Full = 2,
            Limited = 3,
        }

        private enum TokenInformationClass
        {
            TokenElevationType = 18,
            TokenIntegrityLevel = 25,
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct SID_AND_ATTRIBUTES
        {
            public IntPtr Sid;
            public uint Attributes;
        }

        [StructLayout(LayoutKind.Sequential)]
        private struct TOKEN_MANDATORY_LABEL
        {
            public SID_AND_ATTRIBUTES Label;
        }

        private const uint PROCESS_QUERY_LIMITED_INFORMATION = 0x1000;
        private const uint TOKEN_QUERY = 0x0008;

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern IntPtr OpenProcess(uint desiredAccess, bool inheritHandle, uint processId);

        [DllImport("kernel32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool CloseHandle(IntPtr handle);

        [DllImport("kernel32.dll")]
        private static extern IntPtr GetCurrentProcess();

        [DllImport("kernel32.dll", SetLastError = true, CharSet = CharSet.Unicode)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool QueryFullProcessImageName(IntPtr hProcess, uint flags, char[] exeName, ref uint size);

        [DllImport("advapi32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool OpenProcessToken(IntPtr processHandle, uint desiredAccess, out IntPtr tokenHandle);

        [DllImport("advapi32.dll", SetLastError = true)]
        [return: MarshalAs(UnmanagedType.Bool)]
        private static extern bool GetTokenInformation(IntPtr tokenHandle, TokenInformationClass tokenInformationClass, IntPtr tokenInformation, uint tokenInformationLength, out uint returnLength);

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern IntPtr GetSidSubAuthorityCount(IntPtr sid);

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern IntPtr GetSidSubAuthority(IntPtr sid, uint subAuthorityIndex);
    }
}
