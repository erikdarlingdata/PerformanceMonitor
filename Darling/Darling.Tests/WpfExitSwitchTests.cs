/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using System.Threading;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5596: a test process must not spend about 0.3 seconds per WPF STA test thread when it exits. Every STA thread that
/// touches a <c>WeakEventManager</c> gets a <c>WeakEventTable</c> that registers a <c>ProcessExit</c> handler. At exit the
/// handler runs on the main thread, finds the table's own thread gone (its dispatcher never shut down), and asks that
/// dispatcher to purge the table with a 300 ms timeout nobody answers. About fifty test files start STA threads, so the
/// exit stage of the whole-tree run passed the 11 seconds xUnit v3 allows after the run returns, and the runner printed
/// "Foreground threads were left running, forcing process exit" (exit code 1, 0 failed tests) although no thread was
/// left running. The fix is a WPF switch in each test project's runtime configuration; these tests prove it is in
/// effect in this process.
/// <para>
/// This file lives in Darling.Tests and is compiled into Lite.Tests by a link (one rule, two suites), so each test
/// process checks its own runtime configuration.
/// </para>
/// </summary>
public sealed class WpfExitSwitchTests
{
    /// <summary>WindowsBase's name for the switch that makes the weak-event table's exit handler purge directly.</summary>
    internal const string SwitchName = "Switch.MS.Internal.DoNotInvokeInWeakEventTableShutdownListener";

    [Fact]
    public void This_process_runs_with_the_weak_event_table_exit_switch_on()
    {
        Assert.True(
            AppContext.TryGetSwitch(SwitchName, out var enabled) && enabled,
            $"{SwitchName} is not true in this test process, so each WPF STA test thread costs ~0.3 s at exit (#5596). "
            + "Add <RuntimeHostConfigurationOption Include=\"" + SwitchName + "\" Value=\"true\" /> to the test project.");
    }

    [Fact]
    public void The_installed_WPF_still_knows_the_switch()
    {
        /* A switch the runtime no longer reads is a silent no-op: the exit gets slow again and nothing says why. The
           name is a string literal inside WindowsBase, so the assembly the tests run against is searched for it. */
        var windowsBase = typeof(System.Windows.Threading.Dispatcher).Assembly.Location;
        var bytes = File.ReadAllBytes(windowsBase);
        var needle = Encoding.Unicode.GetBytes(SwitchName);
        Assert.True(
            bytes.AsSpan().IndexOf(needle) >= 0,
            $"{Path.GetFileName(windowsBase)} no longer contains \"{SwitchName}\": WPF dropped or renamed the switch, so the ~0.3 s "
            + "per STA thread exit cost (#5596) is back. Find its replacement in dotnet/wpf (BaseAppContextSwitches) and update the test projects.");
    }

    [Fact]
    public void A_weak_event_tables_exit_handler_for_an_ended_STA_thread_returns_at_once_not_after_the_300_ms_dispatcher_wait()
    {
        /* The exit handler's own work, run here on purpose and timed: the table is made on its own STA thread by a ListBox
           over an observable collection (the usual WeakEventManager user), the thread ends without shutting its dispatcher
           down, and that table's ProcessExit handler then runs on this thread, exactly as it does at process exit. Without
           the switch it waits out Dispatcher.Invoke's 300 ms timeout every time; with it, it purges an empty table. The
           least of three runs is asserted, so one scheduling hiccup on a busy runner cannot fail it, while the old shape
           (300 ms or more, every time) always does. Only this test's own table is touched: the handler is picked out by
           the table it holds, so other tests' tables, still in use in parallel, are left alone. */
        var tableType = typeof(System.Windows.Threading.Dispatcher).Assembly.GetType("MS.Internal.WeakEventTable");
        Assert.NotNull(tableType);
        var current = tableType!.GetProperty("CurrentWeakEventTable", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(current);
        var exitField = typeof(AppDomain).GetField("ProcessExit", BindingFlags.NonPublic | BindingFlags.Instance);
        Assert.True(exitField is not null, "AppDomain no longer keeps its ProcessExit handlers in a field named ProcessExit: update this test to read them (#5596).");

        var fastest = TimeSpan.MaxValue;
        for (var attempt = 0; attempt < 3; attempt++)
        {
            object? table = null;
            Exception? error = null;
            using (var staGate = WpfStaGate.Enter())
            {
                var thread = new Thread(() =>
                {
                    try
                    {
                        var list = new System.Collections.ObjectModel.ObservableCollection<int>();
                        var box = new System.Windows.Controls.ListBox { ItemsSource = list };
                        box.Measure(new System.Windows.Size(100, 100));
                        table = current!.GetValue(null);
                    }
                    catch (Exception ex)
                    {
                        error = ex;
                    }
                });
                thread.SetApartmentState(ApartmentState.STA);
                thread.Start();
                thread.Join();
            }

            if (error is not null)
            {
                throw error;
            }

            Assert.NotNull(table);
            Delegate? handler = null;
            foreach (var registered in (exitField!.GetValue(AppDomain.CurrentDomain) as Delegate)?.GetInvocationList() ?? Array.Empty<Delegate>())
            {
                if (registered.Target is WeakReference listener && ReferenceEquals(listener.Target, table))
                {
                    handler = registered;
                }
            }

            Assert.True(handler is not null, "no ProcessExit handler holds the WeakEventTable the STA thread made: WPF changed how a table registers for exit (#5596).");
            var clock = Stopwatch.StartNew();
            ((EventHandler)handler!)(null, EventArgs.Empty);
            clock.Stop();
            if (clock.Elapsed < fastest)
            {
                fastest = clock.Elapsed;
            }
        }

        Assert.True(
            fastest < TimeSpan.FromMilliseconds(250),
            $"A WeakEventTable's exit handler took {fastest.TotalMilliseconds:0} ms at best for a table whose thread had ended: it is waiting out the "
            + "300 ms dispatcher invoke at process exit again, once per STA test thread (#5596). Check " + SwitchName + " in the test project's runtime configuration.");
    }
}
