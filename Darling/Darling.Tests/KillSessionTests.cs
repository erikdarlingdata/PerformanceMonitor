/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Remediation;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>#5632 step 12: the parts of the session kill that need no server and no store. Behaviour only.</summary>
public sealed class KillSessionTests
{
    private const string Name = "kill_session";

    private static KillSessionFacts Facts(long? bytes = 0) =>
        new(57, new DateTime(2026, 10, 19, 8, 0, 0), "app_login", "host-a", "an app", "sleeping", 1, new DateTime(2026, 10, 19, 8, 5, 0), bytes);

    [Theory]
    [InlineData("azure_sql_database", "database_required")]
    [InlineData("own_session", "own_session")]
    [InlineData("session_gone", "session_gone")]
    [InlineData("session_reused", "session_reused")]
    [InlineData("session_changed", "session_changed")]
    [InlineData("not_user_session", "not_user_session")]
    [InlineData("kill_permission_denied", "kill_permission_denied")]
    public void EveryRefusalOfTheBatch_IsARefusalWithItsOwnReason_AndNeverSaysItWrote(string outcome, string reason)
    {
        foreach (var dryRun in new[] { true, false })
        {
            var result = SqlServerRemediationExecutor.KillAnswer(Name, dryRun, 57, outcome, Facts(), null, null);
            Assert.Equal(RemediationOutcome.Refused, result.Outcome);
            Assert.Equal(reason, result.Reason);
            Assert.False(result.Wrote);
            Assert.Null(result.Rollback);
        }
    }

    /// <summary>#7109: a write carries the expected last request end, a NULL as a NULL, and a call with no binding turns the check off.</summary>
    [Fact]
    public void TheKillBatchParameters_CarryTheExpectedLastRequestEnd_AndNullStaysNull()
    {
        static (bool Check, object Value) Read(KillExpectedActivity? expected)
        {
            using var command = new Microsoft.Data.SqlClient.SqlCommand();
            SqlServerRemediationExecutor.AddKillActivityParameters(command, expected);
            return ((bool)command.Parameters["@check_activity"].Value, command.Parameters["@expected_last_end"].Value);
        }

        var ended = new DateTime(2026, 10, 19, 8, 5, 0, 123);
        var withTime = Read(new KillExpectedActivity(ended));
        Assert.True(withTime.Check);
        Assert.Equal(ended, withTime.Value);

        var neverEnded = Read(new KillExpectedActivity(null));
        Assert.True(neverEnded.Check);
        Assert.Equal(DBNull.Value, neverEnded.Value);

        Assert.False(Read(null).Check);
    }

    /// <summary>#7109: the SQL Server batches refuse on the new outcome without wording it as a reused id, and the Azure batch says so too.</summary>
    [Fact]
    public void ASessionThatRanSomethingNew_IsRefusedInPlainWords_OnBothBatches()
    {
        foreach (var database in new string?[] { null, "Sales" })
        {
            var result = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "session_changed", Facts(), null, null, database: database);
            Assert.Equal(RemediationOutcome.Refused, result.Outcome);
            Assert.Equal(RemediationResult.Reasons.SessionChanged, result.Reason);
            Assert.Contains("run something new since the dry run", result.Message);
            Assert.Contains("Nothing was run", result.Message);
            Assert.False(result.Wrote);
        }
    }

    /// <summary>#5632: every refusal of the Azure SQL Database batch is a refusal with its own reason, and never says it wrote.</summary>
    [Theory]
    [InlineData("not_azure_sql_database", "not_azure_sql_database")]
    [InlineData("wrong_database", "wrong_database")]
    [InlineData("not_primary", "not_primary")]
    [InlineData("view_state_permission_denied", "view_state_permission_denied")]
    [InlineData("own_session", "own_session")]
    [InlineData("session_gone", "session_gone")]
    [InlineData("session_reused", "session_reused")]
    [InlineData("session_changed", "session_changed")]
    [InlineData("session_in_other_database", "session_in_other_database")]
    [InlineData("not_user_session", "not_user_session")]
    [InlineData("tool_login_session", "tool_login_session")]
    [InlineData("kill_permission_denied", "kill_permission_denied")]
    public void EveryRefusalOfTheAzureBatch_IsARefusalWithItsOwnReason_AndNeverSaysItWrote(string outcome, string reason)
    {
        foreach (var dryRun in new[] { true, false })
        {
            var result = SqlServerRemediationExecutor.KillAnswer(Name, dryRun, 57, outcome, Facts(), null, null, database: "Sales");
            Assert.Equal(RemediationOutcome.Refused, result.Outcome);
            Assert.Equal(reason, result.Reason);
            Assert.False(result.Wrote);
        }
    }

    /// <summary>#5632: a missing grant on an Azure SQL Database names the database-scoped grant, not the SQL Server one.</summary>
    [Fact]
    public void AnAzureGrantRefusal_NamesTheDatabaseGrant_AndTheDatabase()
    {
        var kill = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "kill_permission_denied", Facts(), null, null, database: "Sales");
        Assert.Contains("GRANT KILL DATABASE CONNECTION", kill.Message, StringComparison.Ordinal);
        Assert.Contains("Sales", kill.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("ALTER ANY CONNECTION", kill.Message, StringComparison.Ordinal);

        var view = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "view_state_permission_denied", null, null, null, database: "Sales");
        Assert.Contains("GRANT VIEW DATABASE STATE", view.Message, StringComparison.Ordinal);

        var box = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "kill_permission_denied", Facts(), null, null);
        Assert.Contains("ALTER ANY CONNECTION", box.Message, StringComparison.Ordinal);
    }

    /// <summary>#5632: on an Azure SQL Database a session the primary does not show is "not found on the primary", never reached elsewhere.</summary>
    [Fact]
    public void AnAzureSessionNotFound_SaysNotFoundOnThePrimary()
    {
        var gone = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "session_gone", null, null, null, database: "Sales");
        Assert.Equal("session_gone", gone.Reason);
        Assert.Contains("not found on the primary of database Sales", gone.Message, StringComparison.Ordinal);
    }

    /// <summary>#5632: the Azure errors that say the service failed, not the statement, are retried on the open and answer "outcome unknown" after the kill was sent.</summary>
    [Theory]
    [InlineData(40613, true)]
    [InlineData(40197, true)]
    [InlineData(40501, true)]
    [InlineData(49918, true)]
    [InlineData(49919, true)]
    [InlineData(49920, true)]
    [InlineData(10928, true)]
    [InlineData(10929, true)]
    [InlineData(-2, true)]
    [InlineData(10054, true)]
    [InlineData(6106, false)]
    [InlineData(229, false)]
    [InlineData(18456, false)]
    public void TheAzureTransientErrors_AreTheServiceFailing_NotTheStatement(int number, bool transient)
    {
        Assert.Equal(transient, SqlServerRemediationExecutor.IsAzureTransientNumber(number));
    }

    /// <summary>#5632: an error that is not a SQL error, after the batch was sent, is outcome unknown on Azure as on SQL Server; a cancel is not.</summary>
    [Fact]
    public void AfterTheAzureBatchWasSent_ALostReplyIsUnknown_AndACancelIsNot()
    {
        Assert.True(SqlServerRemediationExecutor.IsAzureOutcomeUnknown(new System.IO.IOException("reset"), connectionOpen: true, cancelled: false));
        Assert.False(SqlServerRemediationExecutor.IsAzureOutcomeUnknown(new OperationCanceledException(), connectionOpen: true, cancelled: true));
    }

    private static ServerActionPermit KillPermit(string? database) =>
        new(ServerActionNames.KillSession, 1, "srv", "57", database, Guid.NewGuid(), "person:test");

    /// <summary>
    /// #5632: the Azure kill takes its database from the permit the gate issued, so a permit for session 57 in one database never reaches session 57 in
    /// another. A permit that names none, or names master or a name that cannot be a database, is refused before any connection.
    /// </summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("master")]
    [InlineData("MASTER")]
    [InlineData("a\0b")]
    public async System.Threading.Tasks.Task TheAzureKill_RefusesAPermitWithNoUsableDatabase_BeforeAnyConnection(string? database)
    {
        var executor = new SqlServerRemediationExecutor(1, new RemediationServer("127.0.0.1,1"), "remediator", "env:NOT_SET_5632");
        var result = await executor.KillAzureSessionAsync(KillPermit(database), 57, new DateTime(2026, 10, 19), dryRun: true, default);
        Assert.Equal(RemediationOutcome.Refused, result.Outcome);
        Assert.Equal(RemediationResult.Reasons.BadDatabase, result.Reason);
    }

    /// <summary>#5632: a permit for another session id is refused first, whatever database it names.</summary>
    [Fact]
    public async System.Threading.Tasks.Task TheAzureKill_RefusesAPermitForAnotherSession()
    {
        var executor = new SqlServerRemediationExecutor(1, new RemediationServer("127.0.0.1,1"), "remediator", "env:NOT_SET_5632");
        var result = await executor.KillAzureSessionAsync(KillPermit("Sales"), 58, new DateTime(2026, 10, 19), dryRun: true, default);
        Assert.Equal(RemediationResult.Reasons.PermitMismatch, result.Reason);
    }

    /// <summary>#5632: the database name is read before the gate, trimmed, and master or a name that cannot be a database is not one.</summary>
    [Theory]
    [InlineData(" Sales ", "Sales")]
    [InlineData("Sales", "Sales")]
    [InlineData(null, null)]
    [InlineData("", null)]
    [InlineData("  ", null)]
    [InlineData("master", null)]
    [InlineData(" Master ", null)]
    [InlineData("a\0b", null)]
    public void TheAzureDatabaseName_IsReadTrimmed_BeforeTheGate(string? given, string? expected)
    {
        Assert.Equal(expected, DarlingMcpPlanForceTools.AzureDatabaseShape(given));
        Assert.Null(DarlingMcpPlanForceTools.AzureDatabaseShape(new string('x', 129)));
        Assert.NotNull(DarlingMcpPlanForceTools.AzureDatabaseShape(new string('x', 128)));
    }

    /// <summary>#5632: an excluded-database entry with a stray space still excludes its database, whatever the case.</summary>
    [Theory]
    [InlineData("Archive", "archive", true)]
    [InlineData(" Archive ", "Archive", true)]
    [InlineData("Archive\t", "ARCHIVE", true)]
    [InlineData("Archive2", "Archive", false)]
    public void AnExcludedDatabaseEntry_IsComparedTrimmed(string entry, string database, bool excluded)
    {
        Assert.Equal(excluded, DarlingMcpPlanForceTools.IsExcludedDatabase(["Other", entry], database));
        Assert.False(DarlingMcpPlanForceTools.IsExcludedDatabase(null, database));
    }

    /// <summary>
    /// #5632: a refusal about Darling's own session, or a session in another database, answers no facts about that session; one about an
    /// ordinary session still does, so the caller can see why it was refused.
    /// </summary>
    [Theory]
    [InlineData("own_session", false)]
    [InlineData("tool_login_session", false)]
    [InlineData("session_in_other_database", false)]
    [InlineData("session_reused", false)]
    [InlineData("not_user_session", true)]
    [InlineData("kill_permission_denied", true)]
    [InlineData("dry_ok", true)]
    public void ARefusalAboutTheToolsOwnSessionOrAnotherDatabase_ReturnsNoFacts(string outcome, bool facts)
    {
        var result = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, outcome, Facts(), null, null, database: "Sales");
        Assert.Equal(facts, result.Session is not null);
    }

    /// <summary>#5632: Azure's text for a database that is unavailable, a service error or a busy service names the logical server and carries an id; the answer says the number instead.</summary>
    [Theory]
    [InlineData(40613, "Database 'Sales' on server 'contoso-sql' is not currently available. Please retry the connection later. If the problem persists, contact customer support, and provide them the session tracing ID of '{A1B2C3D4-0000-1111-2222-333344445555}'.")]
    [InlineData(40197, "The service has encountered an error processing your request. Please try again. Error code 4060 on server 'contoso-sql'.")]
    [InlineData(40501, "The service is currently busy. Retry the request after 10 seconds. Incident ID: A1B2C3D4-0000-1111-2222-333344445555. Code: 40501. server 'contoso-sql'")]
    public void AnAzureServiceError_IsReportedByNumber_NotByTheServicesText(int number, string message)
    {
        var text = SqlServerRemediationExecutor.ErrorText([number], message, "s3cret");
        Assert.Contains(number.ToString(System.Globalization.CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
        Assert.DoesNotContain("contoso", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("A1B2C3D4", text, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>#5632: any other Azure error text loses the secret, a quoted server name and any id before it is answered or logged.</summary>
    [Fact]
    public void AnyOtherAzureErrorText_LosesTheSecretTheServerNameAndTheId()
    {
        var text = SqlServerRemediationExecutor.ErrorText([4060],
            "Cannot open database \"Sales\" requested by the login on server 'contoso-sql' with s3cret. Activity ID {A1B2C3D4-0000-1111-2222-333344445555}.", "s3cret");
        Assert.DoesNotContain("contoso", text, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("s3cret", text, StringComparison.Ordinal);
        Assert.DoesNotContain("A1B2C3D4", text, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("Sales", text, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5632: the number of a firewall or server-name error is reported with fixed wording, and any other error text loses an IPv4 or IPv6 address
    /// (the client's, or the server's) whichever way the text names it, while a clock time or a version number is left alone.
    /// </summary>
    [Theory]
    [InlineData(40615, "Cannot open server 'contoso-sql' requested by the login. Client with IP address '203.0.113.7' is not allowed to access the server.", "203.0.113.7")]
    [InlineData(40532, "Cannot open server \"contoso-sql\" requested by the login. The login failed.", "contoso")]
    public void AFirewallOrServerNameError_IsReportedByNumber_WithNoAddressAndNoName(int number, string message, string leaked)
    {
        var text = SqlServerRemediationExecutor.ErrorText([number], message, "s3cret");
        Assert.Contains("SQL error " + number.ToString(System.Globalization.CultureInfo.InvariantCulture), text, StringComparison.Ordinal);
        Assert.DoesNotContain(leaked, text, StringComparison.OrdinalIgnoreCase);
    }

    [Theory]
    [InlineData("A network error occurred connecting to 203.0.113.7:1433.", "203.0.113.7")]
    [InlineData("Failed to connect to 10.20.30.40.", "10.20.30.40")]
    [InlineData("no pg_hba.conf entry for host \"2001:db8:85a3::8a2e:370:7334\", user", "2001:db8")]
    [InlineData("could not connect to [fe80::1%12]:5432", "fe80")]
    [InlineData("could not connect to ::1.", "::1")]
    [InlineData("mapped ::ffff:192.0.2.128 refused", "192.0.2.128")]
    public void AnErrorTextWithAnAddress_ComesBackWithoutIt(string message, string address)
    {
        var text = SqlServerRemediationExecutor.ErrorText([], message, "s3cret");
        Assert.DoesNotContain(address, text, StringComparison.Ordinal);
        Assert.Contains("<address>", text, StringComparison.Ordinal);

        /* The exception overload (the text RunAsync and the outcome-unknown path answer) does the same, and also removes the secret. */
        var login = new RemediationLogin("svc", "s3cret");
        var fromException = SqlServerRemediationExecutor.ErrorText(new InvalidOperationException(message + " s3cret"), login);
        Assert.DoesNotContain(address, fromException, StringComparison.Ordinal);
        Assert.DoesNotContain("s3cret", fromException, StringComparison.Ordinal);
    }

    [Fact]
    public void AClockTimeAndAVersionNumber_AreNotTakenForAnAddress()
    {
        const string message = "Timeout at 12:30:45 on build 16.0.4125 for Class::Method.";
        Assert.Equal(message, SqlServerRemediationExecutor.ErrorText([], message, ""));
    }

    /// <summary>
    /// #5632: when the registry has not recorded engine edition 5 yet, the refusal says so (the call went the SQL Server way, which ignores
    /// database_name), and does not send the caller round again with database_name.
    /// </summary>
    [Fact]
    public void WhenTheRegistryHasNotRecordedAnAzureEngineYet_TheRefusalSaysSo_NotToPassDatabaseNameAgain()
    {
        var result = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "azure_sql_database", null, null, null);
        Assert.Equal("database_required", result.Reason);
        Assert.Contains("not recorded its engine edition yet", result.Message, StringComparison.Ordinal);
        Assert.Contains("database_name was not used", result.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("call kill_session again with database_name", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyTheKilledOutcome_SaysWrote_AndADryOkNever_DoesEvenOnARealCall()
    {
        var dry = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "dry_ok", Facts(), null, null);
        Assert.True(dry is { Outcome: RemediationOutcome.Done, Wrote: false, Rollback: null });

        var killed = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(), null, null);
        Assert.True(killed is { Outcome: RemediationOutcome.Done, Wrote: true });

        /* A real call whose batch answered "dry_ok" did not kill: it must not read as a write. */
        var confused = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "dry_ok", Facts(), null, null);
        Assert.False(confused.Wrote);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("something_else")]
    public void AnOutcomeTheBatchDoesNotName_IsAFailure_NeverAKill(string? outcome)
    {
        var result = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, outcome, Facts(), null, null);
        Assert.Equal(RemediationOutcome.Failed, result.Outcome);
        Assert.False(result.Wrote);
    }

    [Fact]
    public void AfterAKill_TheRollbackReadSaysWhetherARollbackRuns_AndItsPercent()
    {
        var running = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(4096), "KILLED/ROLLBACK", 42.5);
        Assert.Equal(new KillRollback(true, 42.5), running.Rollback);

        var none = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(0), null, null);
        Assert.Equal(new KillRollback(false, null), none.Rollback);

        /* An ordinary command's percent is not a rollback's. */
        var other = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(0), "SELECT", 12);
        Assert.Equal(new KillRollback(false, null), other.Rollback);
    }

    [Fact]
    public void AKillWhoseRollbackReadFailed_IsStillAKill_WithItsRollbackNotRead()
    {
        var result = SqlServerRemediationExecutor.KillAnswer(Name, false, 57, "killed", Facts(4096), null, null, rollbackRead: false);
        Assert.Equal(RemediationOutcome.Done, result.Outcome);
        Assert.True(result.Wrote);
        Assert.Null(result.Rollback);
        Assert.Contains("was not read", result.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSessionFacts_RideOnTheResult_SoTheJournalCanKeepThem()
    {
        var facts = Facts(120);
        var result = SqlServerRemediationExecutor.KillAnswer(Name, true, 57, "dry_ok", facts, null, null);
        Assert.Equal(facts, result.Session);
    }

    [Theory]
    [InlineData("2026-10-19T12:34:56.5400000", 2026, 10, 19, 12, 34, 56, 540)]
    [InlineData("2026-10-19T12:34:56.5400000Z", 2026, 10, 19, 12, 34, 56, 540)]
    [InlineData("2026-10-19T12:34:56Z", 2026, 10, 19, 12, 34, 56, 0)]
    [InlineData("2026-10-19T12:34:56.003", 2026, 10, 19, 12, 34, 56, 3)]
    public void TheLoginTimeGetBlockingReturns_IsReadAsUtc_WithItsFractionKept(string text, int y, int mo, int d, int h, int mi, int s, int ms)
    {
        Assert.True(DarlingMcpPlanForceTools.TryReadLoginTime(text, out var utc));
        Assert.Equal(new DateTime(y, mo, d, h, mi, s, ms), utc);
        Assert.Equal(DateTimeKind.Unspecified, utc.Kind);
    }

    [Fact]
    public void ALoginTimeWithAnOffset_IsMovedToUtc_NotReadAsLocal()
    {
        Assert.True(DarlingMcpPlanForceTools.TryReadLoginTime("2026-10-19T12:00:00+02:00", out var utc));
        Assert.Equal(new DateTime(2026, 10, 19, 10, 0, 0), utc);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("yesterday")]
    [InlineData("10/19/2026 12:00")]
    [InlineData("2026-10-19")]
    [InlineData("2026-13-45T00:00:00Z")]
    public void ALoginTimeThatIsNotATime_IsNotRead(string? text)
    {
        Assert.False(DarlingMcpPlanForceTools.TryReadLoginTime(text, out _));
    }

    [Fact]
    public void ALogByteCount_AboveZero_WarnsOfARollback_AndZeroOrNoneDoesNot()
    {
        var warned = Assert.Single(DarlingMcpPlanForceTools.KillWarnings(Facts(120)));
        Assert.Contains("rolls that work back", warned, StringComparison.Ordinal);
        Assert.Contains("120 bytes", warned, StringComparison.Ordinal);
        Assert.Empty(DarlingMcpPlanForceTools.KillWarnings(Facts(0)));
        Assert.Empty(DarlingMcpPlanForceTools.KillWarnings(Facts(null)));
        Assert.Empty(DarlingMcpPlanForceTools.KillWarnings(null));
    }

    [Fact]
    public void ASessionKill_HasNoUndo_WhateverItsResultAndAge()
    {
        var called = $$"""{"kind":"{{AgentChangeKinds.SessionKill}}","tool":"kill_session"}""";
        var change = new AgentChangeRow(1, DateTime.UtcNow, null, 7, "agent-a", "why", called, AgentChangeResult.Done, null, null, null, null);
        Assert.Equal(AgentChangeKinds.SessionKill, change.Kind);
        Assert.False(change.CanTryUndo(DateTime.UtcNow));
    }
}
