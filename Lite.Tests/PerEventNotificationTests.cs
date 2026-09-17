using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Notifications;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Guards <see cref="PerEventNotification.Split"/> (#1141): one message per incident, capped, with a
/// trailing overflow batch so no #1140 fingerprint is ever dropped, and (#3330) each message attaching the
/// forensic file belonging to the incident it describes rather than the first one in the window.
/// </summary>
public class PerEventNotificationTests
{
    private static AlertContext WithIncidents(int n)
    {
        var incidents = new List<AlertIncident>();
        for (int i = 0; i < n; i++)
            incidents.Add(new AlertIncident($"key{i}", new[] { $"db.dbo.T{i}" }, OccurrenceCount: i + 1));
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, incidents);
        return ctx;
    }

    [Fact]
    public void Split_NoIncidents_ReturnsEmpty()
    {
        Assert.Empty(PerEventNotification.Split(new AlertContext(), 10));
    }

    [Fact]
    public void Split_WithinCap_OneMessagePerIncident()
    {
        var messages = PerEventNotification.Split(WithIncidents(3), 10);
        Assert.Equal(3, messages.Count);
        Assert.All(messages, m => Assert.False(m.IsOverflow));
        Assert.All(messages, m => Assert.Single(m.Context.Incidents!));
        Assert.Equal("1", messages[0].CurrentValue); // Current Value = occurrence count (incident 0 => 1), not the object list
    }

    [Fact]
    public void Split_PerEventCard_CarriesIncidentDetailFields()
    {
        var ctx = new AlertContext();
        var incident = new AlertIncident("k", new[] { "db.dbo.Orders" }, OccurrenceCount: 3,
            DetailFields: new[] { new AlertIncidentField("Victim SQL", "UPDATE x"), new AlertIncidentField("Processes", "spids 51,52") });
        AlertIncidentRenderer.Apply(ctx, new[] { incident });

        var msg = Assert.Single(PerEventNotification.Split(ctx, 10));
        var item = Assert.Single(msg.Context.Details);
        Assert.Contains(item.Fields, f => f.Label == "Victim SQL" && f.Value == "UPDATE x");
        Assert.Contains(item.Fields, f => f.Label == "Processes" && f.Value == "spids 51,52");
        Assert.Contains(item.Fields, f => f.Label == "Dedup Key" && f.Value == "k");
    }

    /// <summary>
    /// <b>#3330.</b> Each card attaches ITS OWN incident's graph. Copying the source's put the first graph in
    /// the window on all N messages, so four of five emails attached evidence for a deadlock their own card
    /// did not mention — and every one of them looked correct, because a graph was present.
    /// <para>Asserted as a set of DISTINCT files paired to the right fingerprint, not just "each message has
    /// an attachment": the defect satisfied the second reading perfectly.</para>
    /// </summary>
    [Fact]
    public void Split_EachCardAttachesItsOwnIncidentsGraph()
    {
        var ctx = new AlertContext
        {
            /* The alert-level value the defect copied: deliberately a graph NO incident owns, so a message
               carrying it fails rather than coincidentally matching one of them. */
            AttachmentXml = "<deadlock window-first/>",
            AttachmentFileName = AlertIncidentAttachment.DeadlockGraphFileName
        };
        var incidents = new List<AlertIncident>();
        for (int i = 0; i < 3; i++)
        {
            incidents.Add(new AlertIncident($"key{i}", new[] { $"db.dbo.T{i}" }, OccurrenceCount: i + 1,
                Attachment: new AlertIncidentAttachment(
                    $"<deadlock n=\"{i}\"/>", AlertIncidentAttachment.DeadlockGraphFileName)));
        }
        AlertIncidentRenderer.Apply(ctx, incidents);

        var messages = PerEventNotification.Split(ctx, 10);

        Assert.Equal(3, messages.Count);
        for (int i = 0; i < messages.Count; i++)
        {
            var carried = Assert.Single(messages[i].Context.Incidents!);
            Assert.Equal($"key{i}", carried.DedupKey);
            Assert.Equal($"<deadlock n=\"{i}\"/>", messages[i].Context.AttachmentXml);
            Assert.Equal(AlertIncidentAttachment.DeadlockGraphFileName, messages[i].Context.AttachmentFileName);
        }

        /* Three cards, three DIFFERENT files, and none of them the window's first. */
        Assert.Equal(3, messages.Select(m => m.Context.AttachmentXml).Distinct().Count());
        Assert.All(messages, m => Assert.NotEqual("<deadlock window-first/>", m.Context.AttachmentXml));
    }

    /// <summary>
    /// #3330: the overflow batch resolves against its OWN incidents, not the source's and not the first
    /// individual card's. It is the one per-event payload carrying several fingerprints, so "the first of
    /// the ones it carries" is the only answer that names something on the card.
    /// </summary>
    [Fact]
    public void Split_OverflowBatch_AttachesTheFirstGraphItActuallyCarries()
    {
        var ctx = new AlertContext();
        var incidents = new List<AlertIncident>
        {
            new("capped", new[] { "db.dbo.A" }, Attachment: new AlertIncidentAttachment("<a/>", "deadlock_graph.xml")),
            new("over1", new[] { "db.dbo.B" }, Attachment: new AlertIncidentAttachment("<b/>", "deadlock_graph.xml")),
            new("over2", new[] { "db.dbo.C" }, Attachment: new AlertIncidentAttachment("<c/>", "deadlock_graph.xml"))
        };
        AlertIncidentRenderer.Apply(ctx, incidents);

        var messages = PerEventNotification.Split(ctx, 1);

        Assert.Equal("<a/>", messages[0].Context.AttachmentXml);
        Assert.True(messages[1].IsOverflow);
        Assert.Equal("<b/>", messages[1].Context.AttachmentXml);
    }

    /// <summary>
    /// #3330: an incident with no forensic file of its own gets NO attachment, rather than the alert-level
    /// one. A blocking chain seen only by the DMV-snapshot fallback has no blocked-process report at all
    /// (<c>BlockedProcessAlertRow.HasReportXml</c>), so this is a routine outcome and not a gap — and
    /// attaching a neighbouring incident's report to it is the whole defect.
    /// <para>Both members are asserted null because they are read by consumers with different rules:
    /// <c>EmailSendCore</c> attaches only when both are set, while <c>EmailTemplateBuilder</c> prints
    /// "Attached: &lt;name&gt;" on the filename alone — so a leaked filename is an email that advertises a
    /// file it does not carry.</para>
    /// </summary>
    [Fact]
    public void Split_AnIncidentWithNoGraphOfItsOwn_AttachesNothing()
    {
        var ctx = new AlertContext
        {
            AttachmentXml = "<blocked-process-report other-incident/>",
            AttachmentFileName = AlertIncidentAttachment.BlockedProcessReportFileName
        };
        AlertIncidentRenderer.Apply(ctx, new[] { new AlertIncident("dmv-only", new[] { "db.dbo.T" }) });

        var msg = Assert.Single(PerEventNotification.Split(ctx, 10));

        Assert.Null(msg.Context.AttachmentXml);
        Assert.Null(msg.Context.AttachmentFileName);
    }

    /// <summary>
    /// #3330: a half-populated attachment is treated as absent. It cannot arise from the shared producers,
    /// which build the pair in one expression — this pins the rule at the seam so a future producer that
    /// captures XML without naming it degrades to "no attachment" rather than to an email announcing a file
    /// it does not carry.
    /// </summary>
    [Fact]
    public void Split_AnIncompleteAttachment_IsTreatedAsAbsent()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, new[]
        {
            new AlertIncident("no-name", new[] { "db.dbo.T" }, Attachment: new AlertIncidentAttachment("<x/>", "")),
            new AlertIncident("no-xml", new[] { "db.dbo.U" }, Attachment: new AlertIncidentAttachment("", "deadlock_graph.xml"))
        });

        Assert.All(PerEventNotification.Split(ctx, 10), m =>
        {
            Assert.Null(m.Context.AttachmentXml);
            Assert.Null(m.Context.AttachmentFileName);
        });
    }

    [Fact]
    public void Split_CurrentValueIsOccurrenceCount()
    {
        var ctx = new AlertContext();
        AlertIncidentRenderer.Apply(ctx, new[] { new AlertIncident("k", new[] { "db.dbo.A" }, OccurrenceCount: 7) });
        var msg = Assert.Single(PerEventNotification.Split(ctx, 10));
        Assert.Equal("7", msg.CurrentValue);
    }

    [Fact]
    public void Split_OverCap_CapsIndividualAndBatchesOverflow()
    {
        var messages = PerEventNotification.Split(WithIncidents(5), 2);
        Assert.Equal(3, messages.Count); // 2 individual + 1 overflow
        Assert.False(messages[0].IsOverflow);
        Assert.False(messages[1].IsOverflow);
        Assert.True(messages[2].IsOverflow);
        Assert.Equal(3, messages[2].Context.Incidents!.Count); // overflow carries the remaining 3
        Assert.Contains("+3 more", messages[2].CurrentValue);
    }

    [Fact]
    public void Split_PreservesEveryFingerprint()
    {
        var source = WithIncidents(5);
        var messages = PerEventNotification.Split(source, 2);
        var emitted = messages.SelectMany(m => m.Context.Incidents!).Select(i => i.DedupKey).ToHashSet();
        var expected = source.Incidents!.Select(i => i.DedupKey).ToHashSet();
        Assert.Equal(expected, emitted);
    }

    [Fact]
    public void Split_CapZero_TreatedAsOne()
    {
        var messages = PerEventNotification.Split(WithIncidents(3), 0);
        Assert.Equal(2, messages.Count); // 1 individual + overflow of 2
        Assert.True(messages[1].IsOverflow);
    }
}
