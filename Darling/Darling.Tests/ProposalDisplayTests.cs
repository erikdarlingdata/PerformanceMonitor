/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Windows.Controls;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Investigations;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>The words a proposal is listed in (#7109): the short target line for each tool, the name of whoever pressed, and the
/// Viewer's approval screens that read them.</summary>
public sealed class ProposalDisplayTests
{
    [Theory]
    [InlineData("force_query_store_plan", "{\"database\":\"Sales\",\"query_id\":4411,\"plan_id\":9023}", "Force plan 9023 of query 4411 in Sales")]
    [InlineData("unforce_query_store_plan", "{\"database\":\"Sales\",\"query_id\":4411,\"plan_id\":9023}", "Unforce plan 9023 of query 4411 in Sales")]
    [InlineData("kill_session", "{\"session_id\":53,\"login_time\":\"2026-11-01T08:14:09Z\",\"database\":\"Sales\",\"azure\":false}", "Session 53 in Sales, logged in 2026-11-01 08:14 UTC")]
    [InlineData("kill_session", "{\"pid\":4521,\"backend_start_epoch\":1793578646}", "PostgreSQL backend 4521")]
    [InlineData("recompile_module", "{\"database\":\"Sales\",\"module\":\"[dbo].[GetOrders]\"}", "[dbo].[GetOrders] in Sales")]
    [InlineData("drop_plan_guide", "{\"database\":\"Sales\",\"guide_name\":\"PG_Orders\"}", "Plan guide PG_Orders in Sales")]
    [InlineData("evict_cached_plan", "{\"plan_handle\":\"0x060005005B3D2C1B30D5D1F2A50100000100000000000000\"}", "Cached plan 0x060005005B3D2C1B…")]
    public void TheTargetLine_IsAShortReadableLine_ForEachTool_NotRawKeysAndValues(string tool, string target, string expected)
    {
        Assert.Equal(expected, ProposalDisplay.TargetLine(tool, JsonNode.Parse(target)));
    }

    [Fact]
    public void ThePlanGuideLine_ReadsTheMaskedTarget_SoAStatementClassValueNeverReachesIt()
    {
        const string target = "{\"database\":\"Sales\",\"definition\":{\"name\":\"PG_Orders\",\"statement\":\"SELECT 'alice'\",\"hints\":\"OPTION (RECOMPILE)\"}}";
        foreach (var mode in new[] { McpDataDetail.Full, McpDataDetail.MaskLiterals, McpDataDetail.MetadataOnly })
        {
            var masked = JsonNode.Parse(McpDataDetailMasker.ForAnswer(mode).Json(target));
            var line = ProposalDisplay.TargetLine("create_plan_guide", masked);
            Assert.DoesNotContain("alice", line);
            Assert.DoesNotContain("RECOMPILE", line);
            Assert.EndsWith(" in Sales", line);
        }

        /* Whatever the masker withheld is simply not named: the line still reads. */
        Assert.Equal("New plan guide in Sales", ProposalDisplay.TargetLine("create_plan_guide", JsonNode.Parse("{\"database\":\"Sales\",\"definition\":\"(withheld by mcp.dataDetail)\"}")));
    }

    [Fact]
    public void APress_IsNamedByThePerson_OrByTheAdministratorPathInWords_NeverByTheStoredKey()
    {
        var people = new Dictionary<long, string> { [13] = "Ann Rivera" };
        Assert.Null(ProposalDisplay.PressName(null, people));
        Assert.Equal("Ann Rivera", ProposalDisplay.PressName("person:13", people));
        Assert.DoesNotContain("person:", ProposalDisplay.PressName("person:99", people));
        Assert.Contains("administrator in the web", ProposalDisplay.PressName("admin:web", people));
        Assert.Contains("shared web token", ProposalDisplay.PressName("admin:web", people));
        Assert.Equal("an administrator in the Viewer", ProposalDisplay.PressName("admin:viewer", people));
    }

    [Fact]
    public void TheViewerRow_ShowsTheServiceTargetLine_ACapitalKind_AndTheFirstPressByName()
    {
        var json = "{\"proposals\":[{\"proposal_id\":\"0b9a3f4e-1111-4222-8333-444455556666\",\"kind\":\"kill a session\",\"server_name\":\"pg-7\","
            + "\"target\":{\"pid\":4521,\"backend_start_epoch\":1},\"target_text\":\"PostgreSQL backend 4521\",\"state\":\"pending\","
            + "\"first_press_by\":\"person:13\",\"first_press_name\":\"Ann Rivera\",\"expires_at\":\"2026-11-01T11:00:00Z\","
            + "\"single_person\":false,\"can_approve_here\":false,\"nobody_can_approve\":false,\"link\":\"https://dash.example/#/proposals?proposal=x\"}]}";
        var row = Assert.Single(ProposalViewRow.ReadList(json));
        Assert.Equal("Kill a session", row.Kind);
        Assert.Equal("PostgreSQL backend 4521", row.Target);
        /* The grid cell is short. The status line the Approve button writes carries the person and the link in full. */
        Assert.DoesNotContain("https://", row.ApprovalDisplay);
        Assert.Contains("https://dash.example/#/proposals?proposal=x", row.ApprovalDetail);
        Assert.Contains("First press by Ann Rivera.", row.ApprovalDetail);
        Assert.DoesNotContain("person:13", row.ApprovalDetail);
        Assert.Equal(row.ApprovalDetail, ProposalViewRow.ApproveError(row));
    }

    [Theory]
    [InlineData(null, "required", false, true, false)]
    [InlineData(0, "required", false, true, false)]
    [InlineData(2, "required", false, true, false)]
    [InlineData(3, "required", false, true, true)]
    [InlineData(3, "off", false, true, false)]
    [InlineData(3, "required", true, true, false)]
    [InlineData(3, "required", false, false, false)]
    public void TheNobodyCanApproveSentence_ShowsOnlyOnALevel3Route_WithApprovalRequired_AndNoSinglePerson(int? level, string approval, bool single, bool nobody, bool shown)
    {
        Assert.Equal(shown, NotificationRouteEditDialog.ApprovalNote(level, approval, single, nobody) is not null);
    }

    [Fact]
    public void TheRouteDialogSentence_FollowsTheLevelTheApprovalSettingAndTheCheckBox_AsTheyChange()
    {
        StaTestThread.Run(() =>
        {
            var dialog = new NotificationRouteEditDialog(null, null) { NobodyCanApprove = true };
            var level = (ComboBox)dialog.FindName("AgentLevelCombo");
            var approval = (ComboBox)dialog.FindName("ActionApprovalCombo");
            var single = (CheckBox)dialog.FindName("ApprovalSinglePersonCheck");
            var note = (TextBlock)dialog.FindName("ApprovalNobodyText");

            /* A new route inherits the app-wide level and approval defaults to required: no proposal can come from it, so no sentence. */
            Assert.Equal(System.Windows.Visibility.Collapsed, note.Visibility);
            level.SelectedItem = level.Items.Cast<ComboBoxItem>().First(i => Equals(i.Tag, 3));
            Assert.Equal(System.Windows.Visibility.Visible, note.Visibility);
            Assert.Contains("Nobody can approve this", note.Text);
            approval.SelectedItem = approval.Items.Cast<ComboBoxItem>().First(i => Equals(i.Tag, "off"));
            Assert.Equal(System.Windows.Visibility.Collapsed, note.Visibility);
            approval.SelectedItem = approval.Items.Cast<ComboBoxItem>().First(i => Equals(i.Tag, "required"));
            Assert.Equal(System.Windows.Visibility.Visible, note.Visibility);
            single.IsChecked = true;
            single.RaiseEvent(new System.Windows.RoutedEventArgs(System.Windows.Controls.Primitives.ButtonBase.ClickEvent));
            Assert.Equal(System.Windows.Visibility.Collapsed, note.Visibility);
        });
    }
}
