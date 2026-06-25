using System;
using PerformanceMonitor.Notifications;
using PerformanceMonitorDashboard.Interfaces;
using PerformanceMonitorDashboard.Models;
using PerformanceMonitorDashboard.Services;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Plan E Stage E1 — verifies the Dashboard <see cref="IAlertSettings"/> adapter mirrors
/// Plan D's invariance discipline: it reads live from <see cref="IUserPreferencesService"/>
/// (no caching), it clamps the analysis-notify bounds at the settings boundary (the inline
/// clamp was removed from AnalysisNotificationService), and the credential-backed getters
/// route to the service statics without throwing when no credential is present.
/// </summary>
public class DashboardAlertSettingsTests
{
    /// <summary>Minimal IUserPreferencesService over one mutable UserPreferences instance.</summary>
    private sealed class FakePreferencesService : IUserPreferencesService
    {
        public UserPreferences Preferences { get; } = new();

        public UserPreferences GetPreferences() => Preferences;
        public void SavePreferences(UserPreferences preferences) { }
        public void UpdateWaitStatsRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateCpuRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateMemoryRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateFileIoRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateExpensiveQueriesRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateBlockingRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
        public void UpdateCollectionHealthRange(int hoursBack, DateTime? fromDate = null, DateTime? toDate = null) { }
    }

    [Fact]
    public void ReadsLiveFromPreferences_NoCaching()
    {
        var fake = new FakePreferencesService();
        var settings = new DashboardAlertSettings(fake);

        // Plain string/bool/int pass-throughs reflect the current value on every access.
        fake.Preferences.SmtpServer = "smtp.example.com";
        Assert.Equal("smtp.example.com", settings.SmtpServer);

        fake.Preferences.SmtpServer = "smtp.changed.com";
        Assert.Equal("smtp.changed.com", settings.SmtpServer);

        fake.Preferences.SmtpEnabled = true;
        Assert.True(settings.SmtpEnabled);

        fake.Preferences.EmailCooldownMinutes = 42;
        Assert.Equal(42, settings.EmailCooldownMinutes);

        fake.Preferences.TeamsWebhookEnabled = true;
        Assert.True(settings.TeamsWebhookEnabled);

        fake.Preferences.TeamsProxyAddress = "http://proxy:8080";
        Assert.Equal("http://proxy:8080", settings.TeamsProxyAddress);
    }

    [Fact]
    public void AnalysisNotifySeverity_ClampsToZeroTwoRange()
    {
        var fake = new FakePreferencesService();
        var settings = new DashboardAlertSettings(fake);

        fake.Preferences.AnalysisNotifySeverity = -3.0;
        Assert.Equal(0.0, settings.AnalysisNotifySeverity);

        fake.Preferences.AnalysisNotifySeverity = 9.9;
        Assert.Equal(2.0, settings.AnalysisNotifySeverity);

        fake.Preferences.AnalysisNotifySeverity = 1.25;
        Assert.Equal(1.25, settings.AnalysisNotifySeverity);
    }

    [Fact]
    public void AnalysisNotifyCooldownMinutes_ClampsTo30To10080Range()
    {
        var fake = new FakePreferencesService();
        var settings = new DashboardAlertSettings(fake);

        fake.Preferences.AnalysisNotifyCooldownMinutes = 5;
        Assert.Equal(30, settings.AnalysisNotifyCooldownMinutes);

        fake.Preferences.AnalysisNotifyCooldownMinutes = 99999;
        Assert.Equal(10080, settings.AnalysisNotifyCooldownMinutes);

        fake.Preferences.AnalysisNotifyCooldownMinutes = 360;
        Assert.Equal(360, settings.AnalysisNotifyCooldownMinutes);
    }

    [Fact]
    public void CredentialBackedGetters_RouteToStatics_WithoutThrowing()
    {
        var fake = new FakePreferencesService();
        var settings = new DashboardAlertSettings(fake);

        // Structural/smoke check: these route to EmailAlertService.GetSmtpPassword() and
        // WebhookAlertService.Get{Teams,Slack}WebhookUrl(). They must not throw; with no
        // credential present the password getter returns null and the URL getters "".
        var ex = Record.Exception(() =>
        {
            _ = settings.GetSmtpPassword();
            _ = settings.TeamsWebhookUrl;
            _ = settings.SlackWebhookUrl;
        });
        Assert.Null(ex);
        Assert.NotNull(settings.TeamsWebhookUrl);
        Assert.NotNull(settings.SlackWebhookUrl);
    }

    [Fact]
    public void TransientAdapter_ReadsFromGivenPreferences_NotLiveService()
    {
        // The test-send adapter (MOD-1) reads the UserPreferences it was handed — the form
        // values the user just typed — never the saved IUserPreferencesService.
        var formValues = new UserPreferences
        {
            SmtpEnabled = true,
            SmtpServer = "typed.example.com",
            SmtpPort = 2525,
            SmtpFromAddress = "from@example.com",
            SmtpRecipients = "to@example.com",
            EmailCooldownMinutes = 7
        };
        var settings = new UserPreferencesAlertSettings(formValues);

        Assert.Equal("typed.example.com", settings.SmtpServer);
        Assert.Equal(2525, settings.SmtpPort);
        Assert.Equal("from@example.com", settings.SmtpFromAddress);
        Assert.Equal("to@example.com", settings.SmtpRecipients);
        Assert.Equal(7, settings.EmailCooldownMinutes);
        Assert.True(settings.SmtpEnabled);
    }
}
