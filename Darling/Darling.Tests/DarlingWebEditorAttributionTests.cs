/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.AspNetCore.Http;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Hosting;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// <para><b>The property:</b> every custom-view write arriving over the web HTTP surface stamps
/// <c>updated_by</c> with the REQUESTING SEAT's own <see cref="DarlingWebSeat.EditorPrincipal"/> — the
/// authenticated OIDC subject when that seat has one, the <c>web</c> constant when it does not — and no web
/// write site stamps a constant of its own.</para>
///
/// <para>This is #2550's first-named consequence: a dashboard that cannot say who changed it. The pieces are
/// all one seam — the auth middleware resolves a seat and publishes it on <see cref="HttpContext.Items"/>,
/// <see cref="DarlingWebSeat.FromContext"/> reads it back, <see cref="DarlingWebSeat.EditorPrincipal"/> turns it
/// into a stamp — and every piece is individually correct and individually testable while the JOIN between them
/// is not. A resolver with no caller is unit-tested and inert; a comment naming its consumers reads as evidence
/// that they exist. That gap is what a source-derived pin sees and a behavioural one cannot.</para>
///
/// <para><b>The routes this covers, derived from the property rather than from reading the assertions.</b>
/// Reading members finds a WRONG assertion; only enumerating how a property can become false finds a MISSING
/// one.</para>
/// <list type="table">
/// <item><term>R1 — resolved but not passed</term><description>a write site passes a constant or a fresh
/// literal instead of the seat's principal: <see cref="EveryWebCustomViewWrite_StampsTheRequestingSeatsPrincipal"/>,
/// which checks each discovered site SEPARATELY so reverting one of two still reds.</description></item>
/// <item><term>R2 — passed but overwritten downstream</term><description>the store ignores the argument.
/// <c>DarlingCustomViewsLiveTests</c> round-trips it against live Postgres; its fixture principal is
/// deliberately NOT <c>web</c>, since a fixture whose value IS the constant cannot discriminate a hardcoded
/// stamp from a bound one.</description></item>
/// <item><term>R3 — the subjectless path regresses</term><description>the OIDC seat stamps correctly while the
/// shared token stops stamping <c>web</c>: <see cref="EditorPrincipal_IsTheSubject_ElseWebForASubjectlessSeat"/>
/// over real <see cref="HttpContext"/> instances, both directions.</description></item>
/// <item><term>R4 — a NEW write site added later</term><description>the population is DISCOVERED from the
/// source, not enumerated, so a third site is covered without being named — a pin listing today's two is blind
/// to tomorrow's third. Counted a second, differently-failing way by
/// <see cref="WebEditorPrincipal_IsReferencedOnlyByTheSeatsFallback"/>, which sees files the first scan does
/// not read at all.</description></item>
/// <item><term>R5 — the seat is never published</term><description>the route with no symptom: if the
/// middleware stops writing the item, <see cref="DarlingWebSeat.FromContext"/> returns the shared-token seat and
/// every row reverts to <c>web</c> with nothing thrown and nothing logged.
/// <see cref="AuthMiddleware_PublishesTheResolvedSeat_BeforeItForwards"/> is the only thing between that and a
/// green build.</description></item>
/// <item><term>R6 — the fallback itself inverts</term><description><c>Subject ?? web</c> becoming a bare
/// constant: already held by <c>DarlingWebOidcTests.Seat_EditorPrincipal_SubjectOrTheWebConstant</c>, and not
/// duplicated here.</description></item>
/// <item><term>R8 — the principal reaches the WRONG parameter</term><description>every argument around
/// <c>updatedBy</c> is a string, so a positional swap with <c>description</c> compiles and stamps the
/// description. Closed at the compiler by naming the parameter at each write site, and pinned by
/// <see cref="StampsNamed"/> so the naming cannot quietly go away.</description></item>
/// <item><term>R7 — the MCP asymmetry drifts</term><description>MCP keeps stamping <c>mcp</c> unconditionally
/// BY DECISION (no sign-in flow, no subject to prefer), so
/// <see cref="EveryMcpCustomViewWrite_StampsTheMcpConstant"/> pins it as chosen rather than leaving it as
/// residue that a later reader repairs into symmetry.</description></item>
/// </list>
///
/// <para><b>What this cannot see.</b> These are source-derived: they prove each write site passes the seat's
/// principal and that the middleware publishes the seat, NOT that an end-to-end HTTPS request lands a subject
/// in the column — there is no HTTP-level harness for these endpoints, and standing one up needs a live
/// Postgres and a real bind. The behavioural half is <see cref="DarlingWebSeat"/>'s own resolution over real
/// contexts (below) plus the live store round-trip; the join between them is asserted from the source. Said
/// plainly because "it's tested" unscoped is how #2213's wiring gap survived a green suite.</para>
/// </summary>
public sealed class DarlingWebEditorAttributionTests
{
    private const string EndpointsPath = "Darling/PerformanceMonitor.Darling.Service/DarlingWebEndpoints.cs";
    private const string SeatPath = "Darling/PerformanceMonitor.Darling.Service/Hosting/DarlingWebSeat.cs";
    private const string HostPath = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingWebHostService.cs";
    private const string McpToolsPath = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpCustomViewTools.cs";
    private const string LiveTestsPath = "Darling/Darling.Tests/DarlingCustomViewsLiveTests.cs";
    private const string ServiceProject = "Darling/PerformanceMonitor.Darling.Service";

    /// <summary>A custom-view write on the store: the two method names that take an <c>updatedBy</c>.</summary>
    private static readonly Regex CustomViewWrite = new(@"\.(?:Create|Update)Async\s*\(", RegexOptions.Compiled);

    /// <summary>
    /// A READ of the seat's principal — <c>EditorPrincipal</c> as its own identifier, never as the tail of a
    /// longer one. The word boundary is the entire assertion: a plain substring test for
    /// <c>"EditorPrincipal"</c> is satisfied by <c>WebEditorPrincipal</c> and <c>McpEditorPrincipal</c>, which
    /// makes it pass on exactly the constant-stamping code it exists to reject — a check that cannot fail, and
    /// indistinguishable from a working one until a write site is reverted and nothing goes red.
    /// </summary>
    private static readonly Regex SeatPrincipalRead =
        new(@"(?<![A-Za-z0-9_])EditorPrincipal(?![A-Za-z0-9_])", RegexOptions.Compiled);

    /// <summary>The MCP provenance constant, matched whole for the same reason.</summary>
    private static readonly Regex McpPrincipalRead =
        new(@"(?<![A-Za-z0-9_])McpEditorPrincipal(?![A-Za-z0-9_])", RegexOptions.Compiled);

    /* =====================================================================================================
       R3 / R6 — the resolution itself, over real HttpContext instances.
       ===================================================================================================== */

    /// <summary>
    /// The stamp a request resolves to, read the way the endpoints read it. Both directions matter and they
    /// fail independently: an OIDC seat must stamp the PERSON (the whole point of #2550), and a subjectless
    /// seat must still stamp <c>web</c> — the shared token is the default deployment, so a change that named
    /// OIDC users while breaking the constant would regress every install that has not configured an IdP.
    /// </summary>
    [Fact]
    public void EditorPrincipal_IsTheSubject_ElseWebForASubjectlessSeat()
    {
        /* An OIDC admin: the subject IS the stamp. */
        var admin = Carrying(new DarlingWebSeat("placeholder-admin@example.invalid", CanEdit: true));
        Assert.Equal("placeholder-admin@example.invalid", DarlingWebSeat.FromContext(admin).EditorPrincipal);

        /* An OIDC viewer is named too. It cannot reach a write today (the middleware refuses the method), but
           the stamp must not depend on the ROLE — a role-shaped fallback would stamp `web` the moment a
           read-only seat is ever granted a write. */
        var viewer = Carrying(new DarlingWebSeat("placeholder-viewer@example.invalid", CanEdit: false));
        Assert.Equal("placeholder-viewer@example.invalid", DarlingWebSeat.FromContext(viewer).EditorPrincipal);

        /* The shared token, and the loopback pipeline that publishes no seat at all: the honest constant. */
        Assert.Equal(
            DarlingWebEndpoints.WebEditorPrincipal,
            DarlingWebSeat.FromContext(Carrying(DarlingWebSeat.SharedToken)).EditorPrincipal);
        Assert.Equal(
            DarlingWebEndpoints.WebEditorPrincipal,
            DarlingWebSeat.FromContext(new DefaultHttpContext()).EditorPrincipal);
    }

    /// <summary>
    /// The constant is still exactly <c>web</c>. Stored rows carry it, so changing the spelling would split
    /// one provenance marker into two that nothing reconciles — the value is data, not a label.
    /// </summary>
    [Fact]
    public void TheSubjectlessStamp_IsStillTheStoredSpelling()
    {
        Assert.Equal("web", DarlingWebEndpoints.WebEditorPrincipal);
        Assert.Equal("mcp", DarlingWebEndpoints.McpEditorPrincipal);
    }

    /* =====================================================================================================
       R1 / R4 — every web write site, discovered rather than listed.
       ===================================================================================================== */

    /// <summary>
    /// Every custom-view write in the web endpoints passes a seat-derived principal. The sites are DISCOVERED
    /// by walking the file's code stream, which is what makes this cover a write site nobody has written yet:
    /// a pin naming today's create and update is blind to a third route added on any branch.
    ///
    /// <para>The check is per-site and the assertion is POSITIVE — the argument list must READ the seat's
    /// <see cref="DarlingWebSeat.EditorPrincipal"/>, matched as a whole identifier. A negative check ("does
    /// not pass <c>WebEditorPrincipal</c>") would be satisfied by a site passing a bare <c>"web"</c> literal
    /// instead, and literals are blanked from the code stream, so the negative form cannot even see that
    /// spelling. Requiring the seat read means every way of NOT reading the seat fails, including ones not
    /// thought of here — but only with the identifier boundary, since <c>WebEditorPrincipal</c> ENDS in
    /// <c>EditorPrincipal</c> and a substring test therefore passes on the very constant it rejects.</para>
    ///
    /// <para>The floor on the discovered count is the load-bearing half: if the match stops finding call sites
    /// — a rename, a refactor to a different store method — an empty population satisfies a per-site loop
    /// vacuously and this pin would pass while covering nothing.</para>
    /// </summary>
    [Fact]
    public void EveryWebCustomViewWrite_StampsTheRequestingSeatsPrincipal()
    {
        var code = CodeOf(EndpointsPath);
        var sites = WriteSites(code);

        Assert.True(
            sites.Count >= 2,
            $"Discovered {sites.Count} custom-view write site(s) in {EndpointsPath}; expected at least the "
          + "create and the update. A count below the floor means this scan no longer finds the writes it "
          + "exists to check, so every per-site assertion below it passes vacuously.");

        var offenders = sites
            .Where(site => !StampsNamed(ArgumentsOf(code, site), SeatPrincipalRead))
            .Select(site => $"line {LineOf(code, site)}: {Collapse(ArgumentsOf(code, site))}")
            .ToList();

        Assert.True(
            offenders.Count == 0,
            $"These custom-view writes in {EndpointsPath} do not stamp updated_by with the requesting seat's "
          + "EditorPrincipal, so the row cannot say who authored it — thread "
          + "DarlingWebSeat.FromContext(context).EditorPrincipal through instead of a constant:\n  "
          + string.Join("\n  ", offenders));
    }

    /// <summary>
    /// The second count, and it fails differently: <see cref="DarlingWebEndpoints.WebEditorPrincipal"/> is
    /// referenced by <see cref="DarlingWebSeat.EditorPrincipal"/> and by nothing else in the service. That is
    /// the one place the "person, else the constant" choice is made, and a direct reference anywhere else is
    /// code deciding a principal without consulting the seat.
    ///
    /// <para>Scanning the whole project rather than one file is the point of having this alongside the
    /// per-site scan: they overlap on the sites in <c>DarlingWebEndpoints.cs</c> and diverge everywhere else,
    /// so a write added in a file the per-site scan never opens is still caught. Comments are stripped, so the
    /// <c>see cref</c> references in both constants' own documentation are correctly invisible here.</para>
    /// </summary>
    [Fact]
    public void WebEditorPrincipal_IsReferencedOnlyByTheSeatsFallback()
    {
        var declaring = SeatPath.Replace('/', Path.DirectorySeparatorChar);
        var owning = EndpointsPath.Replace('/', Path.DirectorySeparatorChar);

        var referencing = Directory
            .EnumerateFiles(RepoFile.PathTo(ServiceProject), "*.cs", SearchOption.AllDirectories)
            .Where(path => !path.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal)
                        && !path.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            .Where(path => CSharpSourceWalker
                .StripCommentsAndStrings(File.ReadAllText(path))
                .Contains("WebEditorPrincipal", StringComparison.Ordinal))
            .Select(path => Path.GetRelativePath(RepoFile.Root, path))
            .OrderBy(path => path, StringComparer.Ordinal)
            .ToList();

        Assert.True(
            referencing.Count >= 1,
            "No file in the service references WebEditorPrincipal, not even its own declaration — this scan "
          + "is reading the wrong tree and cannot fail for the right reason.");

        var unexpected = referencing
            .Where(path => !string.Equals(path, declaring, StringComparison.Ordinal)
                        && !string.Equals(path, owning, StringComparison.Ordinal))
            .ToList();

        Assert.True(
            unexpected.Count == 0,
            "WebEditorPrincipal is referenced outside DarlingWebSeat.EditorPrincipal (its one legitimate "
          + "consumer) and its own declaration. Code reaching for the constant directly is deciding a "
          + "principal without asking the seat, which is how the stamp lost the subject in the first place:\n  "
          + string.Join("\n  ", unexpected));
    }

    /* =====================================================================================================
       R5 — the seat has to be PUBLISHED, or every consumer silently reads the default.
       ===================================================================================================== */

    /// <summary>
    /// The auth middleware publishes the resolved seat on the request BEFORE it forwards, on the arm that
    /// forwards. This route has no symptom: <see cref="DarlingWebSeat.FromContext"/> defaults to the
    /// shared-token seat on an absent item — deliberately, because loopback-only mode registers no auth
    /// middleware — so a middleware that stopped publishing would send every named operator's edits back to
    /// being stamped <c>web</c> with nothing logged, nothing thrown, and every other pin here still green.
    ///
    /// <para>Bounded to the <c>Allow</c> arm's own region rather than asserted against the whole file, because
    /// "the assignment exists somewhere in this file" would be satisfied by an assignment on an arm that never
    /// forwards a request. Order is asserted too: publishing after the forward is publishing to a request that
    /// has already been served.</para>
    /// </summary>
    [Fact]
    public void AuthMiddleware_PublishesTheResolvedSeat_BeforeItForwards()
    {
        var code = CodeOf(HostPath);

        var arm = code.IndexOf("case WebRequestAction.Allow:", StringComparison.Ordinal);
        Assert.True(arm >= 0, $"The Allow arm is no longer spelled this way in {HostPath}; this pin is reading nothing.");

        var next = code.IndexOf("case WebRequestAction.", arm + 1, StringComparison.Ordinal);
        var region = next > arm ? code[arm..next] : code[arm..];

        var publish = region.IndexOf("context.Items[DarlingWebSeat.HttpContextItemKey]", StringComparison.Ordinal);
        var forward = region.IndexOf("next(context)", StringComparison.Ordinal);

        Assert.True(
            publish >= 0,
            "The web auth middleware's Allow arm does not publish the resolved seat onto HttpContext.Items. "
          + "Every downstream consumer would read FromContext's absent-item default instead — the shared-token "
          + "seat — so updated_by silently reverts to the 'web' constant for named operators and /api/session "
          + "reports edit rights to a read-only seat. Neither failure surfaces anywhere but the data.");

        Assert.True(
            forward >= 0,
            $"The Allow arm in {HostPath} no longer forwards the request; this pin can no longer check ordering.");

        Assert.True(
            publish < forward,
            "The Allow arm publishes the seat AFTER forwarding the request, so the endpoint that needed it ran "
          + "without it and read the shared-token default.");
    }

    /* =====================================================================================================
       The sibling consumer the same middleware comment names: /api/session.
       ===================================================================================================== */

    /// <summary>
    /// <c>/api/session</c> answers from the request's seat, not from a constant. The SPA hides every edit
    /// affordance on <c>can_edit: false</c> and <c>web.network.oidc.viewerRoles</c> is documented as reaching
    /// that rendering — so a hardcoded <c>true</c> makes a documented behaviour unreachable and shows a
    /// read-only seat buttons whose every click the middleware then refuses.
    ///
    /// <para>Same seam as the stamp and the same shape of failure — an endpoint with a resolved seat available
    /// answering from a literal instead — which is why the two are guarded together.</para>
    /// </summary>
    [Fact]
    public void SessionEndpoint_ReportsTheRequestingSeatsEditRight()
    {
        /* The route is a string LITERAL, so it does not exist in the code stream — the walker blanks literal
           text, which is the whole reason a scan for code cannot be fooled by prose. It blanks in PLACE
           though, preserving length and newlines, so an offset found in the raw source indexes the stripped
           stream identically. Anchor in the raw text, span the code: the pin then cannot match a route name
           that only appears in a comment, and cannot miss one because the walker hid it. */
        var source = RepoFile.ReadRepoFileLf(EndpointsPath);
        var code = CSharpSourceWalker.StripCommentsAndStrings(source);
        Assert.Equal(source.Length, code.Length);

        var route = source.IndexOf("app.MapGet(\"/api/session\"", StringComparison.Ordinal);
        Assert.True(route >= 0, $"The /api/session GET route is gone from {EndpointsPath}; this pin is reading nothing.");

        var handler = CSharpSourceWalker.StatementSpanFrom(code, route, 1);

        Assert.Contains("DarlingWebSeat.FromContext", handler, StringComparison.Ordinal);
        Assert.Contains("CanEdit", handler, StringComparison.Ordinal);
    }

    /* =====================================================================================================
       R2 — the live round-trip's fixture has to be able to fail.
       ===================================================================================================== */

    /// <summary>
    /// The live store round-trip's principals are distinct from each other AND from the <c>web</c> constant.
    /// This pins the only property that makes that round-trip a falsifier rather than a formality. Passing
    /// <c>"web"</c> and asserting <c>"web"</c> comes back is satisfied by a store that drops the argument and
    /// writes the constant, so such a fixture cannot tell a bound parameter from a hardcoded one. Distinct
    /// values also separate the create's stamp from the update's, which is what makes the update's <c>$5</c>
    /// binding provable at all.
    ///
    /// <para>Here rather than in the live class because it must run WITHOUT a Postgres: the live test skips
    /// when <c>DARLING_TEST_PG</c> is unset, and a guard that skips alongside the thing it guards protects
    /// nothing on the runs where it is needed most. Reads the fixture's values rather than restating them —
    /// a copy would agree with itself while the fixture drifted.</para>
    /// </summary>
    [Fact]
    public void LiveRoundTripPrincipals_CanDiscriminateAHardcodedStamp()
    {
        var source = RepoFile.ReadRepoFileLf(LiveTestsPath);

        var create = ConstantValue(source, "CreatePrincipal");
        var update = ConstantValue(source, "UpdatePrincipal");

        Assert.NotEqual(DarlingWebEndpoints.WebEditorPrincipal, create);
        Assert.NotEqual(DarlingWebEndpoints.WebEditorPrincipal, update);
        Assert.NotEqual(create, update);
    }

    /* =====================================================================================================
       R7 — the MCP asymmetry, pinned as a decision.
       ===================================================================================================== */

    /// <summary>
    /// Every custom-view write over MCP stamps <see cref="DarlingWebEndpoints.McpEditorPrincipal"/>, and that
    /// is CHOSEN, not left behind. MCP authenticates a client on its own network block with its own shared
    /// token and has no sign-in flow to carry a person through, so there is no subject to prefer and <c>mcp</c>
    /// is the honest answer for a surface where per-user identity does not exist. The web surface stamps a
    /// person because it has an OIDC path to establish one; MCP has none, so there is nothing to derive.
    ///
    /// <para>Pinned so the asymmetry survives contact with someone tidying it: a reader who sees the web sites
    /// stamping a subject and the MCP sites stamping a constant would otherwise reasonably "finish the job",
    /// and there is nothing on the MCP surface to finish it with. Discovered the same way as the web sites, so
    /// a third MCP write is covered too.</para>
    /// </summary>
    [Fact]
    public void EveryMcpCustomViewWrite_StampsTheMcpConstant()
    {
        var code = CodeOf(McpToolsPath);
        var sites = WriteSites(code);

        Assert.True(
            sites.Count >= 2,
            $"Discovered {sites.Count} custom-view write site(s) in {McpToolsPath}; expected at least the "
          + "create and the update, or the per-site loop below passes vacuously.");

        var offenders = sites
            .Where(site => !StampsNamed(ArgumentsOf(code, site), McpPrincipalRead))
            .Select(site => $"line {LineOf(code, site)}: {Collapse(ArgumentsOf(code, site))}")
            .ToList();

        Assert.True(
            offenders.Count == 0,
            $"These custom-view writes in {McpToolsPath} do not stamp the MCP provenance constant. MCP has no "
          + "per-user identity to stamp instead — it is a client credential on its own network block — so a "
          + "site reaching for anything else is either wrong or is claiming an identity MCP cannot "
          + "establish:\n  " + string.Join("\n  ", offenders));
    }

    /* ── helpers ── */

    /// <summary>
    /// Whether a write's argument list stamps <c>updated_by</c> with <paramref name="principal"/> AND does it
    /// through the NAMED parameter. The name is what closes the last route: every argument the store's writes
    /// take around <c>updatedBy</c> is a string, so a positional swap with <c>description</c> compiles, stamps
    /// the description into <c>updated_by</c>, and satisfies any check that only asks whether the principal
    /// appears somewhere in the list. Naming it makes the swap a compile error and makes this pin able to see
    /// the difference.
    /// </summary>
    private static bool StampsNamed(string arguments, Regex principal) =>
        principal.IsMatch(arguments)
        && Regex.IsMatch(arguments, @"updatedBy\s*:\s*[A-Za-z_]");

    private static DefaultHttpContext Carrying(DarlingWebSeat seat)
    {
        var context = new DefaultHttpContext();
        context.Items[DarlingWebSeat.HttpContextItemKey] = seat;
        return context;
    }

    /// <summary>The file's CODE, with comments and literal text blanked: a scan for a call cannot match prose
    /// in a doc comment, and newlines survive so offenders report a real line number.</summary>
    private static string CodeOf(string relative) =>
        CSharpSourceWalker.StripCommentsAndStrings(RepoFile.ReadRepoFileLf(relative));

    /// <summary>Offsets of every custom-view store write in a code stream.</summary>
    private static List<int> WriteSites(string code) =>
        CustomViewWrite.Matches(code).Select(match => match.Index).ToList();

    /// <summary>The balanced argument list attached to the write at <paramref name="site"/>.</summary>
    private static string ArgumentsOf(string code, int site) =>
        CSharpSourceWalker.ConstructionSpanFrom(code, site);

    /// <summary>The literal a named <c>private const string</c> is declared with, read from the source so a
    /// pin over a fixture's VALUE cannot drift apart from the fixture.</summary>
    private static string ConstantValue(string source, string name)
    {
        var match = Regex.Match(source, @"const\s+string\s+" + Regex.Escape(name) + @"\s*=\s*""([^""]*)""");
        Assert.True(match.Success, $"No `const string {name} = \"...\"` declaration found; this pin is reading nothing.");
        return match.Groups[1].Value;
    }

    private static int LineOf(string code, int offset) =>
        code.AsSpan(0, offset).Count('\n') + 1;

    private static string Collapse(string text) =>
        Regex.Replace(text, @"\s+", " ").Trim();
}
