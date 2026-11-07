/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using Darling.Tests;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Investigations;
using PerformanceMonitorLite.Windows;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8007: the investigations window has a request picked and its detail read still out when a filter change reloads the list. The
/// list's answer clears the grid's pick and the pane. The detail then answers: the pane stays cleared, because the read was for a
/// request the window no longer has picked. The web page's twin is InvestigationsPageTests in Darling.Tests.
/// </summary>
public sealed class InvestigationsWindowDroppedDetailTests
{
    private static readonly DateTime T0 = new(2026, 10, 14, 12, 0, 0, DateTimeKind.Utc);
    private const string Cleared = "Pick a request to see its reports and journal.";

    [Fact]
    public void ADetailReadStillOutWhenAFilterChangeReloadsTheList_DoesNotRedrawThePane()
    {
        var store = new HeldStore();
        var headWhileOut = "";
        var headAfterList = "";
        var headAfterAnswer = "";
        object? pickAfterList = new();
        StaTestThread.Run(async () =>
        {
            var app = Application.Current ?? new Application { ShutdownMode = ShutdownMode.OnExplicitShutdown };
            ResourceDictionary? added = null;
            if (!app.Resources.Contains("NumericCell"))
            {
                added = new ResourceDictionary { Source = new Uri("pack://application:,,,/PerformanceMonitorLite;component/Themes/DarkTheme.xaml") };
                app.Resources.MergedDictionaries.Add(added);
            }

            var window = new InvestigationsWindow(new InvestigationPanelReader(store, () => T0));
            try
            {
                /* A State pick reads the list; the window is not shown, so this is its first read. */
                window.StateCombo.SelectedIndex = 1;
                Assert.True(await PumpUntilAsync(() => window.RequestsGrid.Items.Count == 2), "the list did not reach the grid");

                /* Picking the first request sends its detail read, which the store holds open. */
                window.RequestsGrid.SelectedItem = window.RequestsGrid.Items[0];
                await store.DetailAsked.Task;
                headWhileOut = window.DetailHead.Text;

                /* A Family pick reloads the list while the detail read is still out: the new list clears the pick and the pane. */
                window.FamilyCombo.SelectedIndex = 1;
                Assert.True(await PumpUntilAsync(() => store.ListReads == 2 && window.DetailHead.Text == Cleared), "the list reload did not clear the pane");
                headAfterList = window.DetailHead.Text;
                pickAfterList = window.RequestsGrid.SelectedItem;

                /* The detail read answers. A pane redrawn for the dropped request shows up within the wait; a pane left alone never does. */
                store.DetailGate.SetResult();
                await PumpUntilAsync(() => window.DetailHead.Text != Cleared, polls: 50);
                headAfterAnswer = window.DetailHead.Text;
            }
            finally
            {
                window.Close();
                if (added is not null)
                {
                    app.Resources.MergedDictionaries.Remove(added);
                }
            }
        });

        Assert.Equal("Loading this request...", headWhileOut);
        Assert.Equal(Cleared, headAfterList);
        Assert.Null(pickAfterList);
        Assert.Equal(1, store.DetailReads);
        Assert.Equal(Cleared, headAfterAnswer);
    }

    /// <summary>Lets the dispatcher run until <paramref name="condition"/> holds, looking at most <paramref name="polls"/> times, 20 ms apart.
    /// The window's reads finish on the dispatcher, so the wait yields to it instead of blocking the STA thread.</summary>
    private static async Task<bool> PumpUntilAsync(Func<bool> condition, int polls = 250)
    {
        for (var i = 0; i < polls && !condition(); i++)
        {
            await Task.Delay(20);
        }

        return condition();
    }

    /// <summary>A store that answers the list at once and holds every detail read until <see cref="DetailGate"/> is set. Nothing else is read or written.</summary>
    private sealed class HeldStore : IInvestigationRequestStore
    {
        private static readonly Guid A = new("11111111-1111-1111-1111-111111111111");
        private static readonly Guid B = new("22222222-2222-2222-2222-222222222222");

        private static readonly InvestigationRequestRow RowA = new(A, 1, 7, "srv-7", "blocking", "Blocking", T0, T0, 4, "reported",
            T0, T0.AddHours(4), "agent-a", T0.AddMinutes(20), 1, T0.AddMinutes(12));

        private static readonly InvestigationRequestRow RowB = new(B, 1, 8, "srv-8", "resources", "High CPU", T0.AddHours(-1), T0.AddHours(-1), 1, "open",
            T0.AddHours(-1), T0.AddHours(3), null, null, 0, null);

        private int _listReads;
        private int _detailReads;

        public int ListReads => Volatile.Read(ref _listReads);
        public int DetailReads => Volatile.Read(ref _detailReads);

        /// <summary>Set when a detail read has been asked for and is being held.</summary>
        public TaskCompletionSource DetailAsked { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Set by the test to let the held detail read answer.</summary>
        public TaskCompletionSource DetailGate { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public Task<IReadOnlyList<InvestigationRequestRow>> ListAsync(string? state, int? serverId, string? family, int limit, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _listReads);
            return Task.FromResult<IReadOnlyList<InvestigationRequestRow>>(new[] { RowA, RowB });
        }

        public async Task<InvestigationReportsRead> ReadReportsAsync(Guid? requestId, int? serverId, DateTime sinceUtc, int limit, int journalLimit, CancellationToken cancellationToken = default)
        {
            Interlocked.Increment(ref _detailReads);
            DetailAsked.TrySetResult();
            await DetailGate.Task;
            return new InvestigationReportsRead(new[] { RowA }, Array.Empty<InvestigationReportRow>(), Array.Empty<InvestigationJournalRow>(), null);
        }

        public Task<InvestigationOpenResult> OpenOrJoinAsync(InvestigationAlert alert, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<InvestigationClaimResult> ClaimAsync(Guid requestId, string agentId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> SetBriefAsync(Guid requestId, Guid claimId, short briefFormat, string briefJson, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> ReleaseAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<InvestigationSubmitOutcome> SubmitAsync(InvestigationReportSubmission report, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<InvestigationRenewResult> RenewAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<string?> ClaimHolderAsync(Guid requestId, Guid claimId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IReadOnlyDictionary<int, (int LastHour, int LastDay)>> RouteUseAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task RecordRefusalAsync(Guid requestId, string agentId, string operation, string reason, string? detailJson, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<bool> RequestExistsAsync(Guid requestId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<(InvestigationRequestRow Row, string? Brief)?> ReadRequestAsync(Guid requestId, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<IReadOnlyList<InvestigationOpenedRow>> OpenedSinceAsync(DateTime sinceUtc, IReadOnlyCollection<int>? routeIds, int limit, CancellationToken cancellationToken = default) => throw new NotSupportedException();
        public Task<InvestigationSweepResult> SweepAsync(CancellationToken cancellationToken = default) => throw new NotSupportedException();
    }
}
