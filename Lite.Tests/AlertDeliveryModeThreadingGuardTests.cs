using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using Darling.Tests;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3430: every Lite call into <c>EmailAlertService.TrySendAlertEmailAsync</c> states its effective
/// <c>AlertNotificationMode</c>, or is named here with a reason.
///
/// <para><b>Why a source guard and not a behavioural test.</b> The mode is an OPTIONAL parameter whose
/// default declines aggregation, so a call site that omits it compiles, delivers, records its row, and is
/// green under every behavioural pin — it simply exempts its own alert family from a bound every other
/// family on the same store obeys. Nothing but the source shows that. It was found on this very change:
/// the connection-edge and AG senders bypass <c>LiteAlertDeliverer</c> entirely and both omitted the
/// argument, so a network blip dropping several monitored servers at once still cost one post per server on
/// Lite while the identical scenario was capped on Darling.</para>
///
/// <para><b>It guards the CATEGORY.</b> Fixing the two call sites fixes today's instance; a fifth sender
/// added later would reintroduce it, and the omission is invisible in review for the same reason it was
/// invisible here — the line reads complete. The optional parameter cannot be made required without
/// breaking the deprecated Dashboard shell, so the requirement lives in a check instead.</para>
///
/// <para>Compiled into <c>Lite.Tests</c> rather than <c>Darling.Tests</c> because the files it reads are
/// Lite's, and <c>Lite.Tests</c> already links <see cref="CSharpSourceWalker"/> for exactly this kind of
/// pass. Literal- and comment-aware through that walker, so a method name inside a doc comment or a string
/// is never counted as a call.</para>
/// </summary>
public sealed class AlertDeliveryModeThreadingGuardTests
{
    private const string Method = "TrySendAlertEmailAsync";
    private const string Argument = "deliveryMode:";

    /// <summary>
    /// The one call that is allowed to state no mode, and why. Keyed on the repo-relative path so a move
    /// re-opens the question rather than carrying the exemption along silently.
    /// </summary>
    private static readonly Dictionary<string, string> Exempt = new(StringComparer.Ordinal)
    {
        [Path.Combine("Lite", "Services", "EmailAlertService.cs")] =
            "The analysis-finding forward. Findings carry their own analysis_notify_cooldown_minutes " +
            "throttle, were not part of what #3430 measured, and deliberately decline the per-metric " +
            "ceiling - the same boundary Darling draws by giving DarlingFindingAlertSender its own send core.",
    };

    /// <summary>
    /// The declaration is not a call. Excluded by its return type rather than by line number, and the count
    /// of exclusions is asserted below so the rule is exercised rather than merely present — a
    /// discriminator that matches nothing would silently turn the whole sweep into a no-op over a set that
    /// includes the declaration and always fails, or worse, be loosened until it does not.
    /// <para>#3916: the declaration returns <c>Task&lt;AlertDelivery?&gt;</c>, the disposition it recorded, so
    /// the analysis page road can tell a delivery from a send that reached no one. The prefix names that return
    /// type exactly; the floor of one declaration below is what catches the next signature change.</para>
    /// </summary>
    private const string DeclarationPrefix = "Task<AlertDelivery?> ";

    [Fact]
    public void EveryLiteAlertSend_StatesItsDeliveryMode()
    {
        var root = RepoRoot();
        var offenders = new List<string>();
        var required = new List<string>();
        var exemptSeen = new List<string>();
        var declarations = 0;

        foreach (var file in LiteSourceFiles(root))
        {
            var text = File.ReadAllText(file);
            var code = CSharpSourceWalker.CodeMask(text);
            var relative = Path.GetRelativePath(root, file);

            for (var at = text.IndexOf(Method, StringComparison.Ordinal);
                 at >= 0;
                 at = text.IndexOf(Method, at + 1, StringComparison.Ordinal))
            {
                if (!code[at])
                {
                    /* A doc comment or a string mentioning the name. */
                    continue;
                }

                if (at >= DeclarationPrefix.Length &&
                    text.AsSpan(at - DeclarationPrefix.Length, DeclarationPrefix.Length)
                        .SequenceEqual(DeclarationPrefix))
                {
                    declarations++;
                    continue;
                }

                var line = text.Take(at).Count(c => c == '\n') + 1;
                var site = $"{relative}:{line}";

                if (Exempt.ContainsKey(relative))
                {
                    exemptSeen.Add(site);
                    continue;
                }

                required.Add(site);

                /* The reference plus its attached argument list, brace-balanced and literal-aware, so a
                   nested call or a string containing a comma cannot truncate the span. */
                var span = CSharpSourceWalker.ConstructionSpanFrom(text, at);
                if (!span.Contains(Argument, StringComparison.Ordinal))
                {
                    offenders.Add(site);
                }
            }
        }

        /* Population floors first, and they are the whole reason this pin is not vacuous. A sweep that
           found nothing to check passes; these three say it found the declaration, the exemption and at
           least the three live senders (the deliverer, the connection edge and the AG alert). */
        Assert.Equal(1, declarations);
        Assert.NotEmpty(exemptSeen);
        Assert.True(required.Count >= 3,
            $"expected at least three Lite call sites to check, found {required.Count}: " +
            string.Join(", ", required));

        Assert.True(offenders.Count == 0,
            $"Lite call(s) into {Method} state no {Argument} argument, so they decline #3430's per-metric " +
            "repeat ceiling while every other alert on the same store obeys it. Resolve the effective mode " +
            "through AlertDeliveryModeResolver — from the ServerConnection's own override where the caller " +
            "holds one, or ServerManager.ResolveAlertDeliveryModeOverride where it only has the hashed id — " +
            "and pass it. If the omission is deliberate, add the file to this test's Exempt map WITH the " +
            "reason, the way the analysis-finding forward is.\n\n" +
            string.Join("\n", offenders));
    }

    /// <summary>
    /// The guard's own self-test. It reports a call written without the argument and stays quiet on one
    /// written with it, over synthetic text rather than the tree — a rule proven only by the tree passing is
    /// a rule that has never been shown to fail, and this one's whole value is failing on a shape that
    /// compiles.
    /// </summary>
    [Fact]
    public void TheGuard_ReportsACallWithNoModeAndAcceptsOneWithIt()
    {
        const string Without =
            "        _ = _emailAlertService.TrySendAlertEmailAsync(\n" +
            "            metricName, serverName, currentValue, \"Online\", serverId,\n" +
            "            context: null, muted: isMuted, detailText: detailText);\n";

        const string With =
            "        _ = _emailAlertService.TrySendAlertEmailAsync(\n" +
            "            metricName, serverName, currentValue, \"Online\", serverId,\n" +
            "            context: null, muted: isMuted, detailText: detailText,\n" +
            "            deliveryMode: AlertDeliveryModeResolver.Resolve(o, App.AlertDeliveryMode));\n";

        Assert.False(StatesItsMode(Without));
        Assert.True(StatesItsMode(With));

        /* A mention inside a string or a comment is not a call, so neither shape below is even reached by
           the sweep — asserted because the masking is what stops the doc comments on this very method from
           being counted as four more offenders. */
        Assert.False(IsCode("        // calls TrySendAlertEmailAsync without a mode\n", Method));
        Assert.False(IsCode("        var s = \"TrySendAlertEmailAsync\";\n", Method));
    }

    private static bool StatesItsMode(string snippet)
    {
        var at = snippet.IndexOf(Method, StringComparison.Ordinal);
        Assert.True(at >= 0, "the fixture does not contain the call it is about");
        return CSharpSourceWalker.ConstructionSpanFrom(snippet, at)
            .Contains(Argument, StringComparison.Ordinal);
    }

    private static bool IsCode(string snippet, string token)
    {
        var at = snippet.IndexOf(token, StringComparison.Ordinal);
        Assert.True(at >= 0, "the fixture does not contain the token it is about");
        return CSharpSourceWalker.CodeMask(snippet)[at];
    }

    /* Every tracked C# file under Lite/, build output excluded by whole path SEGMENT so a directory merely
       starting with "bin" or "obj" is still swept. */
    private static IEnumerable<string> LiteSourceFiles(string root) =>
        Directory.EnumerateFiles(Path.Combine(root, "Lite"), "*.cs", SearchOption.AllDirectories)
            .Where(path => !Path.GetRelativePath(root, path)
                .Split(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar)
                .Any(segment => segment is "bin" or "obj"))
            .OrderBy(path => path, StringComparer.Ordinal);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"could not locate the repo root walking up from {thisFile}");
    }
}
