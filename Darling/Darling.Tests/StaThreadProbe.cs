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
using System.Runtime.InteropServices;
using System.Threading;

/// <summary>
/// #7600: what the hang guard can read about the stuck thread and the process around it. Two PR runs on 11-04 hung the shared STA thread
/// for 126 s and 68 s with the thread waiting, 0 ms of CPU and nothing on the dispatcher, and then the bodies returned: slow, not blocked
/// for good. The step names could not say what the thread waited on, so this reads the thread's native stack (the frames name the call it
/// sits in: a kernel wait for a file, a message wait, a runtime lock, a COM call) and the process counters that tell the likely causes
/// apart (file reads, page faults, free memory, GC pauses, a starved thread pool). It only reads: a failure to read is reported as text
/// and never replaces the guard's own message. This file lives in Darling.Tests and is compiled into Lite.Tests by a link, like
/// <c>StaTestThread.cs</c>, and is in the global namespace for the same reason.
/// </summary>
internal static class StaThreadProbe
{
    private const uint ThreadSuspendResume = 0x0002;
    private const uint ThreadGetContext = 0x0008;
    private const uint ThreadQueryInformation = 0x0040;

    /* CONTEXT_AMD64 | CONTROL | INTEGER: the registers an unwind needs. */
    private const int ContextControlInteger = 0x100003;
    private const int ContextSize = 1232;
    private const int ContextFlagsOffset = 0x30;
    private const int ContextRspOffset = 0x98;
    private const int ContextRipOffset = 0xF8;
    private const int MaxFrames = 32;

    [DllImport("kernel32.dll")]
    private static extern IntPtr OpenThread(uint access, [MarshalAs(UnmanagedType.Bool)] bool inheritHandle, uint threadId);

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CloseHandle(IntPtr handle);

    [DllImport("kernel32.dll")]
    private static extern uint SuspendThread(IntPtr thread);

    [DllImport("kernel32.dll")]
    private static extern uint ResumeThread(IntPtr thread);

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetThreadContext(IntPtr thread, IntPtr context);

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool ReadProcessMemory(IntPtr process, ulong address, IntPtr buffer, nuint size, out nuint read);

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentProcess();

    [DllImport("ntdll.dll")]
    private static extern IntPtr RtlLookupFunctionEntry(ulong controlPc, out ulong imageBase, IntPtr historyTable);

    [DllImport("ntdll.dll")]
    private static extern IntPtr RtlVirtualUnwind(uint handlerType, ulong imageBase, ulong controlPc, IntPtr functionEntry, IntPtr context, out IntPtr handlerData, out ulong establisherFrame, IntPtr contextPointers);

    [DllImport("ntdll.dll")]
    private static extern int NtQueryInformationThread(IntPtr thread, int informationClass, IntPtr information, int length, out int returned);

    [DllImport("dbghelp.dll", CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SymInitializeW(IntPtr process, string? searchPath, [MarshalAs(UnmanagedType.Bool)] bool invadeProcess);

    [DllImport("dbghelp.dll")]
    private static extern uint SymSetOptions(uint options);

    [DllImport("dbghelp.dll", CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SymFromAddrW(IntPtr process, ulong address, out ulong displacement, IntPtr symbol);

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetProcessIoCounters(IntPtr process, out IoCounters counters);

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GlobalMemoryStatusEx(ref MemoryStatus status);

    [DllImport("psapi.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetProcessMemoryInfo(IntPtr process, out ProcessMemory counters, uint size);

    [StructLayout(LayoutKind.Sequential)]
    private struct IoCounters
    {
        public ulong ReadOperations;
        public ulong WriteOperations;
        public ulong OtherOperations;
        public ulong ReadBytes;
        public ulong WriteBytes;
        public ulong OtherBytes;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct MemoryStatus
    {
        public uint Length;
        public uint MemoryLoad;
        public ulong TotalPhysical;
        public ulong AvailablePhysical;
        public ulong TotalPageFile;
        public ulong AvailablePageFile;
        public ulong TotalVirtual;
        public ulong AvailableVirtual;
        public ulong AvailableExtendedVirtual;
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct ProcessMemory
    {
        public uint Size;
        public uint PageFaultCount;
        public nuint PeakWorkingSetSize;
        public nuint WorkingSetSize;
        public nuint QuotaPeakPagedPoolUsage;
        public nuint QuotaPagedPoolUsage;
        public nuint QuotaPeakNonPagedPoolUsage;
        public nuint QuotaNonPagedPoolUsage;
        public nuint PagefileUsage;
        public nuint PeakPagefileUsage;
    }

    /// <summary>The process-wide counters at one instant. <see cref="Describe"/> reports the difference between two.</summary>
    internal readonly record struct Counters(
        long CpuMs, ulong Reads, ulong ReadBytes, ulong Writes, ulong WriteBytes, uint PageFaults, long GcPauseMs, int Gen2Collections,
        int PoolThreads, long PoolQueued, long LockContentions, ulong FreePhysicalBytes, ulong TotalPhysicalBytes);

    internal static Counters ReadCounters()
    {
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        _ = GetProcessIoCounters(GetCurrentProcess(), out var io);
        _ = GetProcessMemoryInfo(GetCurrentProcess(), out var memory, (uint)Marshal.SizeOf<ProcessMemory>());
        var status = new MemoryStatus { Length = (uint)Marshal.SizeOf<MemoryStatus>() };
        _ = GlobalMemoryStatusEx(ref status);
        return new Counters(
            (long)process.TotalProcessorTime.TotalMilliseconds, io.ReadOperations, io.ReadBytes, io.WriteOperations, io.WriteBytes, memory.PageFaultCount,
            (long)GC.GetTotalPauseDuration().TotalMilliseconds, GC.CollectionCount(2), ThreadPool.ThreadCount, ThreadPool.PendingWorkItemCount,
            Monitor.LockContentionCount, status.AvailablePhysical, status.TotalPhysical);
    }

    /// <summary>What the process did between two readings, and what the machine had free: the figures that tell cold file reads, memory pressure, a long GC and a starved pool apart.</summary>
    internal static string Describe(Counters before, Counters after, TimeSpan window) =>
        $"Around it, in those {window.TotalMilliseconds:0} ms the whole process used {after.CpuMs - before.CpuMs} ms of CPU, made {after.Reads - before.Reads} file reads "
        + $"({(after.ReadBytes - before.ReadBytes) / 1024} KB) and {after.Writes - before.Writes} writes ({(after.WriteBytes - before.WriteBytes) / 1024} KB), and took "
        + $"{after.PageFaults - before.PageFaults} page faults. GC has paused {after.GcPauseMs} ms in total ({after.Gen2Collections} gen-2 collections, "
        + $"{after.Gen2Collections - before.Gen2Collections} of them in the sample). The thread pool has {after.PoolThreads} threads and {after.PoolQueued} queued items, "
        + $"there have been {after.LockContentions} lock contentions, and Windows has {after.FreePhysicalBytes / 1048576} of {after.TotalPhysicalBytes / 1048576} MB of memory free.";

    /// <summary>
    /// Another thread's native stack, leaf first, as <c>module!export+offset</c>. A frame in code the runtime generated (managed code, a
    /// stub) has no module and shows as <c>(generated code)</c>; the native frames around it still name the call the thread sits in.
    /// </summary>
    internal static string NativeStack(int nativeThreadId)
    {
        if (RuntimeInformation.ProcessArchitecture != Architecture.X64)
        {
            return "(the native stack is only read on x64)";
        }

        var frames = CaptureFrames((uint)nativeThreadId, out var failure);
        if (frames.Count == 0)
        {
            return $"(the native stack could not be read: {failure})";
        }

        return string.Join(" < ", frames.Select(Symbolize)) + (frames.Count >= MaxFrames ? " < ..." : "");
    }

    private static List<ulong> CaptureFrames(uint threadId, out string failure)
    {
        var frames = new List<ulong>();
        failure = "";
        var raw = Marshal.AllocHGlobal(ContextSize + 16);
        var scratch = Marshal.AllocHGlobal(64);
        var thread = IntPtr.Zero;
        try
        {
            var context = new IntPtr((raw.ToInt64() + 15) & ~15L);

            /* Each P/Invoke below is called once on a null handle first: its stub is generated on first use, which takes runtime locks the
               suspended thread could be holding. After this the window between SuspendThread and ResumeThread makes one call. */
            _ = SuspendThread(IntPtr.Zero);
            _ = ResumeThread(IntPtr.Zero);
            _ = GetThreadContext(IntPtr.Zero, context);
            _ = RtlLookupFunctionEntry(0, out _, IntPtr.Zero);

            thread = OpenThread(ThreadSuspendResume | ThreadGetContext | ThreadQueryInformation, false, threadId);
            if (thread == IntPtr.Zero)
            {
                failure = "the thread could not be opened";
                return frames;
            }

            /* The stack's bounds, from the thread's TEB, so an unwind that goes wrong stops instead of reading unmapped memory. */
            if (NtQueryInformationThread(thread, 0, scratch, 48, out _) != 0
                || !ReadMemory((ulong)Marshal.ReadInt64(scratch, 8) + 8, scratch + 32, 16))
            {
                failure = "the thread's stack bounds could not be read";
                return frames;
            }

            var stackHigh = (ulong)Marshal.ReadInt64(scratch, 32);
            var stackLow = (ulong)Marshal.ReadInt64(scratch, 40);

            Marshal.WriteInt32(context, ContextFlagsOffset, ContextControlInteger);
            var suspended = SuspendThread(thread) != uint.MaxValue;
            var gotContext = false;
            try
            {
                gotContext = suspended && GetThreadContext(thread, context);
            }
            finally
            {
                if (suspended)
                {
                    _ = ResumeThread(thread);
                }
            }

            if (!gotContext)
            {
                failure = "the thread's registers could not be read";
                return frames;
            }

            var rip = (ulong)Marshal.ReadInt64(context, ContextRipOffset);
            var rsp = (ulong)Marshal.ReadInt64(context, ContextRspOffset);
            while (rip != 0 && frames.Count < MaxFrames && rsp >= stackLow && rsp < stackHigh)
            {
                frames.Add(rip);
                var entry = RtlLookupFunctionEntry(rip, out var imageBase, IntPtr.Zero);
                if (entry == IntPtr.Zero)
                {
                    /* A leaf with no unwind data: its return address is the top of the stack. */
                    if (!ReadMemory(rsp, scratch, 8))
                    {
                        break;
                    }

                    rip = (ulong)Marshal.ReadInt64(scratch);
                    rsp += 8;
                    Marshal.WriteInt64(context, ContextRipOffset, (long)rip);
                    Marshal.WriteInt64(context, ContextRspOffset, (long)rsp);
                }
                else
                {
                    _ = RtlVirtualUnwind(0, imageBase, rip, entry, context, out _, out _, IntPtr.Zero);
                    var nextRsp = (ulong)Marshal.ReadInt64(context, ContextRspOffset);
                    if (nextRsp <= rsp)
                    {
                        break;
                    }

                    rip = (ulong)Marshal.ReadInt64(context, ContextRipOffset);
                    rsp = nextRsp;
                }
            }

            return frames;
        }
        finally
        {
            if (thread != IntPtr.Zero)
            {
                _ = CloseHandle(thread);
            }

            Marshal.FreeHGlobal(scratch);
            Marshal.FreeHGlobal(raw);
        }
    }

    private static bool ReadMemory(ulong address, IntPtr buffer, int length) =>
        ReadProcessMemory(GetCurrentProcess(), address, buffer, (nuint)length, out var read) && (int)read == length;

    private static bool s_symbolsReady;
    private static System.Diagnostics.ProcessModule[]? s_modules;

    private static string Symbolize(ulong address)
    {
        s_modules ??= ListModules();
        var module = s_modules.FirstOrDefault(m => address >= (ulong)m.BaseAddress && address < (ulong)m.BaseAddress + (ulong)m.ModuleMemorySize);
        if (module is null)
        {
            return "(generated code)";
        }

        var name = System.IO.Path.GetFileNameWithoutExtension(module.ModuleName);
        var symbol = ExportNear(address);
        return symbol is null ? $"{name}+0x{address - (ulong)module.BaseAddress:x}" : $"{name}!{symbol}";
    }

    private static System.Diagnostics.ProcessModule[] ListModules()
    {
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        return process.Modules.Cast<System.Diagnostics.ProcessModule>().ToArray();
    }

    /// <summary>The nearest exported name at or below the address, as <c>name+0xoffset</c>. dbghelp gets no symbol path, so it never goes to a symbol server.</summary>
    private static string? ExportNear(ulong address)
    {
        const int nameOffset = 84;
        const int nameChars = 200;
        const int size = nameOffset + 4 + (nameChars * 2);
        var process = GetCurrentProcess();
        if (!s_symbolsReady)
        {
            _ = SymSetOptions(0x00000002 /* SYMOPT_UNDNAME */ | 0x00000004 /* SYMOPT_DEFERRED_LOADS */);
            s_symbolsReady = SymInitializeW(process, AppContext.BaseDirectory, true);
        }

        var symbol = Marshal.AllocHGlobal(size);
        try
        {
            for (var i = 0; i < size; i += 4)
            {
                Marshal.WriteInt32(symbol, i, 0);
            }

            Marshal.WriteInt32(symbol, 0, 88);
            Marshal.WriteInt32(symbol, 80, nameChars);
            if (!SymFromAddrW(process, address, out var displacement, symbol))
            {
                return null;
            }

            var name = Marshal.PtrToStringUni(symbol + nameOffset);
            return string.IsNullOrEmpty(name) ? null : $"{name}+0x{displacement:x}";
        }
        finally
        {
            Marshal.FreeHGlobal(symbol);
        }
    }
}
