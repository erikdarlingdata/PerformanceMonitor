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

    /* The 16 integer registers, Rax first and R15 last, 8 bytes each from this offset; Rsp and Rbp are among them. */
    private const int ContextIntegerOffset = 0x78;
    private const int ContextR15Offset = 0xF0;

    /* #8190: the walk reads a copy of the thread's stack, from its stack pointer to the stack's base, with zeroed slack on either side
       for a frame pointer that sits a little below the stack pointer. A committed stack larger than the cap is not read. */
    private const int SnapshotSlack = 4096;
    private const int MaxSnapshotBytes = 8 << 20;

    [DllImport("kernel32.dll")]
    private static extern IntPtr OpenThread(uint access, [MarshalAs(UnmanagedType.Bool)] bool inheritHandle, uint threadId);

    [DllImport("kernel32.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool CloseHandle(IntPtr handle);

    [DllImport("kernel32.dll")]
    [SuppressGCTransition]
    private static extern uint SuspendThread(IntPtr thread);

    [DllImport("kernel32.dll")]
    [SuppressGCTransition]
    private static extern uint ResumeThread(IntPtr thread);

    [DllImport("kernel32.dll")]
    [SuppressGCTransition]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool GetThreadContext(IntPtr thread, IntPtr context);

    [DllImport("kernel32.dll")]
    private static extern IntPtr GetCurrentProcess();

    [DllImport("kernel32.dll")]
    private static extern uint GetCurrentThreadId();

    [DllImport("kernel32.dll", CharSet = CharSet.Unicode)]
    private static extern IntPtr GetModuleHandleW(string moduleName);

    [DllImport("dbghelp.dll", CharSet = CharSet.Unicode)]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SymInitializeW(IntPtr process, string? searchPath, [MarshalAs(UnmanagedType.Bool)] bool invadeProcess);

    [DllImport("dbghelp.dll")]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool SymRefreshModuleList(IntPtr process);

    [DllImport("kernel32.dll")]
    [SuppressGCTransition]
    [return: MarshalAs(UnmanagedType.Bool)]
    private static extern bool ReadProcessMemory(IntPtr process, ulong address, IntPtr buffer, nuint size, out nuint read);

    [DllImport("ntdll.dll")]
    private static extern IntPtr RtlLookupFunctionEntry(ulong controlPc, out ulong imageBase, IntPtr historyTable);

    [DllImport("ntdll.dll")]
    private static extern IntPtr RtlVirtualUnwind(uint handlerType, ulong imageBase, ulong controlPc, IntPtr functionEntry, IntPtr context, out IntPtr handlerData, out ulong establisherFrame, IntPtr contextPointers);

    [DllImport("ntdll.dll")]
    private static extern int NtQueryInformationThread(IntPtr thread, int informationClass, IntPtr information, int length, out int returned);

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

    /// <summary>WPF's native render engine, without its extension. Its render thread is the one the UI thread waits for in <c>MilComposition_SyncFlush</c>.</summary>
    internal const string RenderEngine = "wpfgfx_cor3";

    /// <summary>
    /// #7600: whether WPF's render engine is loaded in the process. It loads when the first MediaContext is made (the first visual that invalidates
    /// its layout, or the first window), and that first use starts the render thread and its rendering device. Read when a body begins, so the
    /// guard's message can say whether the stuck body was the one that paid for the start-up.
    /// </summary>
    internal static bool RenderEngineLoaded() => GetModuleHandleW(RenderEngine + ".dll") != IntPtr.Zero;

    /// <summary>
    /// #7600: the native stacks, leaf first, of the threads that are inside WPF's render engine, found by the wpfgfx frames on their stacks (the
    /// thread's start address is not a reliable sign of it). The three stalls of 11-06 had the STA thread waiting in <c>MilComposition_SyncFlush</c>
    /// for this thread, and what the render thread itself waited on is what those reports could not say. At most three threads, twelve frames each.
    /// The STA thread and the calling thread are skipped.
    /// </summary>
    internal static string RenderThreadStacks(int staThreadId)
    {
        if (RuntimeInformation.ProcessArchitecture != Architecture.X64)
        {
            return "(the render thread's stack is only read on x64)";
        }

        if (!RenderEngineLoaded())
        {
            return $"WPF's render engine ({RenderEngine}) is not loaded, so there is no render thread to read.";
        }

        /* Listed again here, for this class and for dbghelp: the engine may have loaded after the module list was first taken. */
        s_modules = ListModules();
        _ = EnsureSymbols();
        var engine = s_modules.Where(m => m.ModuleName.StartsWith("wpfgfx", StringComparison.OrdinalIgnoreCase)).ToArray();
        var self = GetCurrentThreadId();
        var found = new List<string>();
        using var process = System.Diagnostics.Process.GetCurrentProcess();
        foreach (System.Diagnostics.ProcessThread thread in process.Threads)
        {
            if (thread.Id == staThreadId || thread.Id == self)
            {
                continue;
            }

            var frames = CaptureFrames((uint)thread.Id, out _);
            if (!frames.Any(frame => engine.Any(module => Within(frame, module))))
            {
                continue;
            }

            var state = thread.ThreadState == System.Diagnostics.ThreadState.Wait ? $"Wait ({thread.WaitReason})" : thread.ThreadState.ToString();
            found.Add($"WPF's render thread (native id {thread.Id}, {state}) was at: {string.Join(" < ", frames.Take(12).Select(Symbolize))}{(frames.Count > 12 ? " < ..." : "")}.");
            if (found.Count == 3)
            {
                break;
            }
        }

        return found.Count == 0
            ? "No other thread in the process had a wpfgfx frame on its stack at the sample, so the render thread was not inside the engine's own code then."
            : string.Join(" ", found);
    }

    private static bool Within(ulong address, System.Diagnostics.ProcessModule module) =>
        address >= (ulong)module.BaseAddress && address < (ulong)module.BaseAddress + (ulong)module.ModuleMemorySize;

    /// <summary>
    /// #8190: the instruction addresses on another thread's stack, leaf first. The thread is suspended only to read its registers and to
    /// copy the used part of its stack: a managed thread held suspended while a garbage collection starts blocks the collection, and
    /// with it every thread that then polls for one, this one included, so the thread runs again before the walk. The walk reads the
    /// copy, never the thread's stack. Before each unwind step every register that holds a stack address is moved into the copy, and
    /// after it moved back, so the step reads its frame from memory that is this method's own and cannot change or go away. A walk over
    /// the live stack of a thread that has moved on reads garbage where a frame was, and over one that has exited reads freed memory;
    /// either is an access violation, which no managed handler catches and which ends the test host.
    /// </summary>
    private static List<ulong> CaptureFrames(uint threadId, out string failure)
    {
        var frames = new List<ulong>(MaxFrames);
        failure = "";
        var raw = Marshal.AllocHGlobal(ContextSize + 16);
        var scratch = Marshal.AllocHGlobal(64);
        var snapshot = IntPtr.Zero;
        var thread = IntPtr.Zero;
        try
        {
            var context = new IntPtr((raw.ToInt64() + 15) & ~15L);

            /* Each P/Invoke used between SuspendThread and ResumeThread is called once on a null handle first: its stub is generated on
               first use, which takes runtime locks the suspended thread could be holding. Nothing in that window allocates, for the same
               reason: the copy's memory is taken before it. Those four calls also suppress the GC transition, so no return inside the
               window polls for a collection: a collection that starts while a managed thread is held suspended waits for that thread,
               and a poll here would wait for the collection, which is a deadlock of the whole process. */
            _ = SuspendThread(IntPtr.Zero);
            _ = ResumeThread(IntPtr.Zero);
            _ = GetThreadContext(IntPtr.Zero, context);
            _ = ReadMemory(0, scratch, 8);

            thread = OpenThread(ThreadSuspendResume | ThreadGetContext | ThreadQueryInformation, false, threadId);
            if (thread == IntPtr.Zero)
            {
                failure = "the thread could not be opened";
                return frames;
            }

            /* The stack's bounds, from the thread's TEB: the copy is sized by the committed stack, and the walk ends at the stack's base. */
            if (NtQueryInformationThread(thread, 0, scratch, 48, out _) != 0
                || !ReadMemory((ulong)Marshal.ReadInt64(scratch, 8) + 8, scratch + 32, 16))
            {
                failure = "the thread's stack bounds could not be read";
                return frames;
            }

            var stackHigh = (ulong)Marshal.ReadInt64(scratch, 32);
            var stackLow = (ulong)Marshal.ReadInt64(scratch, 40);
            if (stackHigh <= stackLow || stackHigh - stackLow > (ulong)MaxSnapshotBytes)
            {
                failure = "the thread's stack is not a size this reads";
                return frames;
            }

            var capacity = (int)(stackHigh - stackLow) + (2 * SnapshotSlack);
            snapshot = Marshal.AllocHGlobal(capacity);
            for (var i = 0; i < SnapshotSlack; i += 8)
            {
                Marshal.WriteInt64(snapshot, i, 0);
                Marshal.WriteInt64(snapshot, capacity - SnapshotSlack + i, 0);
            }

            Marshal.WriteInt32(context, ContextFlagsOffset, ContextControlInteger);
            var suspended = SuspendThread(thread) != uint.MaxValue;
            var gotContext = false;
            var gotStack = false;
            ulong rsp = 0;
            try
            {
                gotContext = suspended && GetThreadContext(thread, context);
                if (gotContext)
                {
                    rsp = (ulong)Marshal.ReadInt64(context, ContextRspOffset);
                    gotStack = rsp >= stackLow && rsp < stackHigh && ReadMemory(rsp, snapshot + SnapshotSlack, (int)(stackHigh - rsp));
                }
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

            if (!gotStack)
            {
                failure = "the thread's stack could not be read";
                return frames;
            }

            /* A stack address a is at a + delta in the copy. */
            var delta = (long)snapshot + SnapshotSlack - (long)rsp;
            var copiedFrom = rsp;
            var copyEnd = (ulong)snapshot + (ulong)capacity;
            var rip = (ulong)Marshal.ReadInt64(context, ContextRipOffset);
            while (rip != 0 && frames.Count < MaxFrames && rsp >= copiedFrom && rsp + 8 <= stackHigh)
            {
                frames.Add(rip);
                var entry = RtlLookupFunctionEntry(rip, out var imageBase, IntPtr.Zero);
                if (entry == IntPtr.Zero)
                {
                    /* A leaf with no unwind data: its return address is the top of the stack. */
                    rip = (ulong)Marshal.ReadInt64(new IntPtr((long)rsp + delta));
                    rsp += 8;
                    Marshal.WriteInt64(context, ContextRipOffset, (long)rip);
                    Marshal.WriteInt64(context, ContextRspOffset, (long)rsp);
                    continue;
                }

                MoveStackRegisters(context, copiedFrom - SnapshotSlack, stackHigh, delta);
                _ = RtlVirtualUnwind(0, imageBase, rip, entry, context, out _, out _, IntPtr.Zero);
                MoveStackRegisters(context, (ulong)snapshot, copyEnd, -delta);
                var nextRsp = (ulong)Marshal.ReadInt64(context, ContextRspOffset);
                if (nextRsp <= rsp)
                {
                    break;
                }

                rip = (ulong)Marshal.ReadInt64(context, ContextRipOffset);
                rsp = nextRsp;
            }

            return frames;
        }
        finally
        {
            if (thread != IntPtr.Zero)
            {
                _ = CloseHandle(thread);
            }

            if (snapshot != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(snapshot);
            }

            Marshal.FreeHGlobal(scratch);
            Marshal.FreeHGlobal(raw);
        }
    }

    /// <summary>
    /// Moves every integer register whose value lies in [low, high) by delta: before an unwind step from the thread's stack into the
    /// copy, and after it back. The unwinder reads a frame only through the stack pointer and the frame register, and in a context read
    /// together with the copy both hold stack addresses; a register the step restored from the copy holds a stack address again and is
    /// left as it is.
    /// </summary>
    private static void MoveStackRegisters(IntPtr context, ulong low, ulong high, long delta)
    {
        for (var offset = ContextIntegerOffset; offset <= ContextR15Offset; offset += 8)
        {
            var value = (ulong)Marshal.ReadInt64(context, offset);
            if (value >= low && value < high)
            {
                Marshal.WriteInt64(context, offset, (long)value + delta);
            }
        }
    }

    private static bool ReadMemory(ulong address, IntPtr buffer, int length) =>
        ReadProcessMemory(GetCurrentProcess(), address, buffer, (nuint)length, out var read) && (int)read == length;

    /// <summary>dbghelp is single-threaded, so every call into it holds this.</summary>
    private static readonly object s_dbghelp = new();
    private static bool s_symbolsReady;
    private static System.Diagnostics.ProcessModule[]? s_modules;

    /// <summary>
    /// dbghelp initialized once for this process, with no symbol path (it never goes to a symbol server), and its module list brought
    /// up to date on every call: a module loaded since the last one (WPF's render engine loads with the first window) is unknown to the
    /// symbolizer until it is listed.
    /// </summary>
    private static bool EnsureSymbols()
    {
        lock (s_dbghelp)
        {
            var process = GetCurrentProcess();
            if (!s_symbolsReady)
            {
                _ = SymSetOptions(0x00000002 /* SYMOPT_UNDNAME */ | 0x00000004 /* SYMOPT_DEFERRED_LOADS */);
                s_symbolsReady = SymInitializeW(process, AppContext.BaseDirectory, true);
            }

            if (s_symbolsReady)
            {
                _ = SymRefreshModuleList(process);
            }

            return s_symbolsReady;
        }
    }

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
        if (!s_symbolsReady && !EnsureSymbols())
        {
            return null;
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
            ulong displacement;
            bool found;
            lock (s_dbghelp)
            {
                found = SymFromAddrW(GetCurrentProcess(), address, out displacement, symbol);
            }

            if (!found)
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
