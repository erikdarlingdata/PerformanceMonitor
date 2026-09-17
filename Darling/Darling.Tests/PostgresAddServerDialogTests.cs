/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3499: the viewer's Add Server dialog can author a PostgreSQL target — the engine selector, the
/// PostgreSQL-only port field, the per-engine control hiding, and the auth gate the backend applies — while
/// the SQL Server arm renders byte-identical to the pre-selector dialog.
///
/// <para><b>The pinning split, same rationale as <see cref="ServerIdentitySurvivesAnEditTests"/>.</b> The pure
/// seams (<c>ParsePortText</c>, <c>MonitoredServerRow.IsPostgres</c>, the row defaults) are tested
/// behaviourally. Everything that lives in the WPF dialog — the XAML default state, the edit-path prefill and
/// engine lock, the test-connect threading — is pinned textually, because reproducing it behaviourally means
/// standing up <c>AddServerDialog</c> with a live store, which the suite cannot do; and each of those shapes is
/// invisible to every other test while silently breaking either the regression pin (SQL Server renders as
/// today) or the issue's named arms (engine + port through Test Connection, registration, and an edit
/// round-trip).</para>
/// </summary>
public sealed class PostgresAddServerDialogTests
{
    /* ─────────────────────────────── ParsePortText (pure) ─────────────────────────────── */

    /// <summary>Blank is 0, "the driver's default" — the same omitted-means-default contract as the MCP
    /// add_servers tool's port field, so a default-port dialog Add derives the SAME identity as an MCP
    /// onboarding that omitted port. This is the arm that keeps one real server ONE identity across the two
    /// onboarding doors.</summary>
    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void ParsePortText_Blank_IsDriverDefault(string? text)
    {
        var (port, error) = AddServerDialog.ParsePortText(text);
        Assert.Equal(0, port);
        Assert.Null(error);
    }

    [Theory]
    [InlineData("5432", 5432)]
    [InlineData("6432", 6432)]
    [InlineData(" 5433 ", 5433)]
    [InlineData("0", 0)] /* an explicit 0 is the default spelled out, exactly as the MCP tool accepts it */
    [InlineData("65535", 65535)]
    [InlineData("1", 1)]
    public void ParsePortText_ValidValues_ParseAsTyped(string text, int expected)
    {
        var (port, error) = AddServerDialog.ParsePortText(text);
        Assert.Equal(expected, port);
        Assert.Null(error);
    }

    [Theory]
    [InlineData("sql01")]
    [InlineData("54.32")]
    [InlineData("1e4")]
    public void ParsePortText_NonNumeric_IsRefused_NamingTheBlankDefault(string text)
    {
        var (port, error) = AddServerDialog.ParsePortText(text);
        Assert.Equal(0, port);
        Assert.NotNull(error);
        Assert.Contains("5432", error, StringComparison.Ordinal);
    }

    /// <summary>Range-checked to match the MCP tool's validation (1–65535, 0 allowed as the default) rather
    /// than failing later inside Npgsql with a driver error nothing points back at the box.</summary>
    [Theory]
    [InlineData("65536")]
    [InlineData("70000")]
    [InlineData("-1")]
    public void ParsePortText_OutOfRange_IsRefused(string text)
    {
        var (port, error) = AddServerDialog.ParsePortText(text);
        Assert.Equal(0, port);
        Assert.NotNull(error);
        Assert.Contains("between 1 and 65535", error, StringComparison.Ordinal);
    }

    /* ─────────────────────────────── MonitoredServerRow's engine facts ─────────────────────────────── */

    /// <summary>The SQL Server regression pin at the model level: the new fields default to exactly the values
    /// the store's V70 columns default to, so every pre-#3499 constructor call site — the bulk dialog's row
    /// builder, the viewer-servers.json migrate-in — keeps writing the byte-identical SQL Server row it always
    /// wrote without being edited.</summary>
    [Fact]
    public void RowDefaults_AreTheSqlServerRow_SoEveryExistingWriterIsUnchanged()
    {
        var row = new MonitoredServerRow();
        Assert.Equal("sqlserver", row.Engine);
        Assert.Equal(0, row.Port);
        Assert.False(row.IsPostgres);
    }

    /// <summary>
    /// <see cref="MonitoredServerRow.IsPostgres"/> agrees with the service's <c>MonitoredServer.TargetEngine</c>
    /// for every spelling, checked against the service's OWN parse rather than a copied list — the store holds
    /// whatever string onboarded the row (a darling.json seed persists its raw text), and the Edit dialog must
    /// recognize a PostgreSQL row however it was spelled, or an edit would silently re-render it as SQL Server.
    /// The unrecognized-means-SqlServer arm mirrors the service for the same reason it exists there: a typo
    /// must not flip a row's UI.
    /// </summary>
    [Theory]
    [InlineData("postgres")]
    [InlineData("PostgreSQL")]
    [InlineData("pg")]
    [InlineData("aurora")]
    [InlineData("aurora-postgresql")]
    [InlineData(" postgres ")]
    [InlineData("sqlserver")]
    [InlineData("")]
    [InlineData("postgress")] /* the typo arm: unrecognized folds to SQL Server, both sides */
    public void IsPostgres_AgreesWithTheServiceTargetEngineParse(string engine)
    {
        var service = new MonitoredServer { Engine = engine };
        var viewer = new MonitoredServerRow { Engine = engine };
        Assert.Equal(service.IsPostgres, viewer.IsPostgres);
    }

    /* ─────────────────────────────── XAML default-state pins ─────────────────────────────── */

    /// <summary>
    /// THE regression pin the issue asks for, held where it lives: an untouched dialog IS the SQL Server form.
    /// SQL Server is the checked engine default and every PostgreSQL-only element starts Collapsed, so until
    /// the operator picks PostgreSQL the rendered tree is the pre-#3499 dialog plus one radio row — and the
    /// per-engine swaps are event-driven from the radios, never part of the default state.
    /// </summary>
    [Fact]
    public void TheXamlDefaultState_IsTheSqlServerForm()
    {
        var xaml = RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml");

        Assert.Contains("x:Name=\"SqlServerEngineRadio\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"PostgresEngineRadio\"", xaml, StringComparison.Ordinal);

        /* SQL Server checked by default; the PostgreSQL panel exists but starts hidden. */
        var sqlRadio = xaml[xaml.IndexOf("x:Name=\"SqlServerEngineRadio\"", StringComparison.Ordinal)..];
        Assert.Contains("IsChecked=\"True\"", sqlRadio[..sqlRadio.IndexOf("/>", StringComparison.Ordinal)], StringComparison.Ordinal);
        var pgPanel = xaml[xaml.IndexOf("x:Name=\"PostgresOptionsPanel\"", StringComparison.Ordinal)..];
        Assert.Contains("Visibility=\"Collapsed\"", pgPanel[..pgPanel.IndexOf(">", StringComparison.Ordinal)], StringComparison.Ordinal);

        /* Both radios drive the one engine handler; the port box lives inside the gated panel. */
        Assert.Contains("Checked=\"EngineMode_Changed\"", xaml, StringComparison.Ordinal);
        Assert.Contains("x:Name=\"PortBox\"", xaml, StringComparison.Ordinal);

        /* The dialog's own title stays the SQL Server one — the engine swap retitles at runtime. */
        Assert.Contains("Title=\"Add SQL Server\"", xaml, StringComparison.Ordinal);
        Assert.Contains("Text=\"Add SQL Server Connection\"", xaml, StringComparison.Ordinal);
    }

    /* ─────────────────────────────── code-behind source pins ─────────────────────────────── */

    /// <summary>
    /// The per-engine swap hides rather than resets: the SQL-Server-only controls (read-only intent,
    /// multi-subnet failover — AG-listener/FCI concepts the PostgreSQL connection builder never reads) toggle
    /// Visibility only, so a hidden checkbox keeps its IsChecked and an edited row's stored values survive the
    /// round trip. And the PostgreSQL arm force-checks SQL auth so a hidden radio can never be the checked one.
    /// </summary>
    [Fact]
    public void ThePostgresArm_HidesSqlServerOnlyControls_AndForcesSqlAuth()
    {
        var source = ReadDialogSource();

        Assert.Contains("ReadOnlyIntentCheckBox.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;", source, StringComparison.Ordinal);
        Assert.Contains("MultiSubnetFailoverCheckBox.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;", source, StringComparison.Ordinal);
        Assert.Contains("WindowsAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;", source, StringComparison.Ordinal);
        Assert.Contains("EntraMfaAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;", source, StringComparison.Ordinal);
        Assert.Contains("ServicePrincipalAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;", source, StringComparison.Ordinal);
        Assert.Contains("ManagedIdentityAuthRadio.Visibility = postgres ? Visibility.Collapsed : Visibility.Visible;", source, StringComparison.Ordinal);
        Assert.Contains("if (postgres && SqlAuthRadio.IsChecked != true)", source, StringComparison.Ordinal);

        /* Hiding must never be resetting — an unchecking regression here mangles the edit round trip. */
        Assert.DoesNotContain("ReadOnlyIntentCheckBox.IsChecked = false", source, StringComparison.Ordinal);
        Assert.DoesNotContain("MultiSubnetFailoverCheckBox.IsChecked = false", source, StringComparison.Ordinal);
    }

    /// <summary>
    /// The save-path auth belt: the same rule the backend applies (DarlingConfig.Validate and the MCP
    /// add_servers validation both refuse every non-SQL auth for a PostgreSQL target), enforced before the
    /// write so the two onboarding paths cannot disagree. The hidden radios make the inline modes unreachable;
    /// the live route is a credential profile resolving to integrated / service principal / managed identity,
    /// and the refusal is tailored — it names what IS supported (#3486's discipline), not just what is not.
    /// </summary>
    [Fact]
    public void TheSaveGate_RefusesNonSqlAuthForPostgres_WithATailoredMessage()
    {
        var source = ReadDialogSource();

        Assert.Contains("if (isPostgres && !string.Equals(auth, ServerStoreCredential.Sql, StringComparison.OrdinalIgnoreCase))", source, StringComparison.Ordinal);
        Assert.Contains("error = PostgresRequiresSqlAuthMessage;", source, StringComparison.Ordinal);

        /* The message itself: names the supported mode and the way forward, and says it matches the service. */
        Assert.Contains("username/password (SQL) authentication", AddServerDialog.PostgresRequiresSqlAuthMessage, StringComparison.Ordinal);
        Assert.Contains("not supported for PostgreSQL targets", AddServerDialog.PostgresRequiresSqlAuthMessage, StringComparison.Ordinal);
        Assert.Contains("credential profile", AddServerDialog.PostgresRequiresSqlAuthMessage, StringComparison.Ordinal);
    }

    /// <summary>
    /// The edit round trip the issue names explicitly: a PostgreSQL row loads back with engine and port intact
    /// and saves without mangling either. Engine recognition goes through IsPostgres (the TargetEngine parse,
    /// so a darling.json spelling is recognized), the radios are LOCKED (an engine flip under a preserved
    /// server_id would interleave two engines' histories, the #2158 identity turned against itself), the port
    /// prefills from the row (0 renders blank — the default's spelling in the box), and the save carries the
    /// stored engine string VERBATIM and only reads the port box on the PostgreSQL arm.
    /// </summary>
    [Fact]
    public void TheEditPath_LoadsEngineAndPortBack_LocksTheEngine_AndSavesBothUnmangled()
    {
        var source = ReadDialogSource();

        /* Prefill + lock. */
        Assert.Contains("if (existing.IsPostgres)", source, StringComparison.Ordinal);
        Assert.Contains("PortBox.Text = existing.Port > 0 ? existing.Port.ToString(CultureInfo.InvariantCulture) : \"\";", source, StringComparison.Ordinal);
        Assert.Contains("SqlServerEngineRadio.IsEnabled = false;", source, StringComparison.Ordinal);
        Assert.Contains("PostgresEngineRadio.IsEnabled = false;", source, StringComparison.Ordinal);

        /* Save: the stored engine spelling rides verbatim; the port box is PostgreSQL-only and a SQL Server
           edit preserves the stored port rather than zeroing it. */
        Assert.Contains("var engine = _existing?.Engine", source, StringComparison.Ordinal);
        Assert.Contains("var port = _existing?.Port ?? 0;", source, StringComparison.Ordinal);
    }

    /// <summary>Engine + port ride the probe exactly as they ride the registry write, so Test Connection
    /// vouches for the connection the save will store — the request half of #3244's contract (the reply half,
    /// the engine-aware version label, was already consumed by both dialogs).</summary>
    [Fact]
    public void TestConnection_CarriesEngineAndPort()
    {
        var source = ReadDialogSource();

        Assert.Contains("Engine = row.Engine,", source, StringComparison.Ordinal);
        Assert.Contains("Port = row.Port,", source, StringComparison.Ordinal);
    }

    /* ─────────────────────────────── helpers ─────────────────────────────── */

    /* The shared reader, not a private walk-up: RepoFileAdoptionTests holds the census that exactly one
       file declares the reader and everyone else adopts it — the same helper-proliferation rule the #3489
       review nit asked for, enforced. */
    private static string ReadDialogSource() =>
        RepoFile.ReadRepoFile("Darling", "PerformanceMonitor.Darling.Viewer", "AddServerDialog.xaml.cs");
}
