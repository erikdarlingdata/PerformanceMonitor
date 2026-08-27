/*
   Firewall rule VERIFICATION for the running service (#1771).

   The service runs as the `NT SERVICE\PerformanceMonitor Darling` virtual account, which by design cannot
   modify Windows Firewall. Every surface that opted into LAN exposure used to try anyway on each start —
   New-NetFirewallRule / Remove-NetFirewallRule — and every attempt failed with "Access is denied" on a
   hardened box. On a field instance that was merely log noise (the rules already existed from a manual
   setup); on a genuinely FRESH networked install nothing would create the rule at all, so the endpoint
   would be firewalled off with only a warning to say so. Networked is the normal deployment mode.

   The rules are therefore created by the ELEVATED install path (install-darling.ps1 -> the exe's
   --configure-firewall verb) and removed by uninstall-darling.ps1. What is left here is a READ-ONLY probe:
   Get-NetFirewallRule needs no elevation (verified on Windows 11 26200 under a restricted token: reads
   succeed, writes return PermissionDenied), so the service can still tell the operator the truth without
   holding a privilege it should not have.

   Everything except CheckAsync is pure, so the probe shape, the parse, the verdict table, and the remedy
   command all pin without a live firewall.
*/

using System;
using System.Globalization;
using System.Runtime.Versioning;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace PerformanceMonitor.Darling.Service;

/// <summary>What a firewall probe found, against what the config asked for. Public (where the rest of
/// <see cref="DarlingFirewallCheck"/> is internal) only so the test project can name it in a [Theory]
/// parameter — xUnit needs public test methods, and a public method cannot take an internal type.</summary>
public enum FirewallRuleVerdict
{
    /// <summary>Loopback-only and no rule exists — the healthy default install. Reported nowhere; there is
    /// nothing for an operator to do and a line here would be noise on every start of every default box.</summary>
    LoopbackNoRule,

    /// <summary>Loopback-only but a scoped rule is still open — a stale allow rule left behind by an exposure
    /// that was turned off by hand. Nothing is listening on the network so it grants no access, but it is
    /// drift worth one line naming the removal.</summary>
    LoopbackStaleRule,

    /// <summary>Exposed and the rule is present — the healthy networked install. One INFO line, so an
    /// operator can see the check ran and passed.</summary>
    ExposedRulePresent,

    /// <summary>Exposed but NO rule exists — the case that used to fail silently on a fresh install. One WARN
    /// naming the exact elevated command.</summary>
    ExposedRuleMissing,

    /// <summary>The probe itself could not run or could not be parsed. Reported once, never retried.</summary>
    ProbeFailed,
}

/// <summary>
/// Read-only Windows Firewall verification for the three optionally-exposed Darling surfaces (the bundled
/// store, the MCP endpoint, the web dashboard). See the file header for why this replaced the old
/// attempt-and-fail self-heal.
/// <para>Windows-only in whole: every member is about Windows Firewall, and the pure builders compose
/// <see cref="DarlingManagedPostgres"/>'s own Windows-only command builders. The test project targets
/// <c>net10.0-windows</c>, so the pure members still pin directly.</para>
/// </summary>
[SupportedOSPlatform("windows")]
internal static class DarlingFirewallCheck
{
    /// <summary>
    /// The sentinel the probe prints, so parsing keys off something this code owns rather than off
    /// PowerShell's object formatting (which is culture- and host-dependent).
    /// </summary>
    internal const string ProbeSentinel = "DarlingFirewallProbe=";

    /// <summary>
    /// The shared DisplayName prefix every Darling firewall rule carries. Uninstall removes rules by
    /// <c>"{prefix}*"</c> rather than reconstructing each port-specific name, so this constant is the seam —
    /// <c>DarlingFirewallCheckTests</c> pins it against all three real rule-name builders AND against the
    /// literal in uninstall-darling.ps1, so a rename cannot silently orphan rules on uninstall.
    /// </summary>
    internal const string RuleNamePrefix = "PerformanceMonitor Darling ";

    /// <summary>The separator every scoped rule name puts before its port, and the seam this class derives a
    /// per-surface wildcard from.</summary>
    private const string PortSuffixSeparator = " (port ";

    /// <summary>
    /// PURE: the DisplayName wildcard covering EVERY port of one surface — <c>"PerformanceMonitor Darling MCP
    /// (port *)"</c> for any MCP rule. The port is part of the rule NAME, so changing a port does not update a
    /// rule, it makes a DIFFERENT one and strands the old: an inbound allow rule left open on a port the
    /// service no longer listens on. Reconciling by exact name alone could never clean that up, so the
    /// elevated verb sweeps this wildcard per surface before ensuring the desired rule.
    /// <para>An unrecognized name (no port suffix) returns UNCHANGED rather than gaining a wildcard. That is
    /// the safety property, not a formality: a wildcard derived from a malformed name could match — and
    /// therefore delete — rules this product does not own. The consequence is worth stating exactly, because it
    /// is stronger than "safe" and provable from these two lines: on that path the caller's sweep degrades to
    /// the exact-name removal it already performed, so this change is <b>strictly additive in blast radius</b> —
    /// it can only ever widen a delete to OTHER PORTS OF THE SAME SURFACE, never to anything else.</para>
    /// </summary>
    internal static string SurfaceRuleWildcard(string ruleName)
    {
        var separator = ruleName.IndexOf(PortSuffixSeparator, StringComparison.Ordinal);
        return separator < 0 ? ruleName : ruleName[..separator] + PortSuffixSeparator + "*)";
    }

    /// <summary>
    /// PURE probe command: count the rules with this DisplayName and print the count behind
    /// <see cref="ProbeSentinel"/>. Deliberately shaped to ALWAYS exit 0 and never throw:
    /// <list type="bullet">
    /// <item>An exact DisplayName that matches nothing is an <c>ObjectNotFound</c> ERROR from
    /// Get-NetFirewallRule, not an empty result — "absent" is a normal, expected answer here, so it must not
    /// look like a failure. <c>-ErrorAction SilentlyContinue</c> suppresses the text.</item>
    /// <item>...but a suppressed error still leaves powershell.exe exiting 1 (the same trap
    /// <see cref="DarlingManagedPostgres.BuildFirewallDisableCommand"/> documents), so the explicit
    /// <c>exit 0</c> is load-bearing: without it every absent rule would read as a failed probe.</item>
    /// <item>The result is wrapped in <c>@(...)</c> because one DisplayName can match SEVERAL rules (Windows
    /// stores one per profile) and a single match would otherwise have no <c>.Count</c>.</item>
    /// </list>
    /// The name goes through the same single-quoted-literal escaping as the write builders.
    /// <para>Takes either ONE rule's exact DisplayName — what <see cref="CheckAsync"/> asks, because the service
    /// verifies the specific rule its own config wants — or a <see cref="SurfaceRuleWildcard"/>, which is what
    /// <c>--configure-firewall</c>'s not-elevated branch asks (#2445): there the question is whether the sweep
    /// it is about to hand over would remove anything, so the probe has to cover exactly what the sweep would
    /// delete. Single quoting is literal to the SHELL and not to the cmdlet, so a <c>*</c> reaches
    /// <c>-DisplayName</c> as the wildcard it is — the same way it does for the sweep's
    /// <c>Remove-NetFirewallRule -DisplayName</c>, which is where that behaviour is already load-bearing.</para>
    /// </summary>
    internal static string BuildProbeCommand(string ruleName) =>
        $"$c = @(Get-NetFirewallRule -DisplayName {DarlingManagedPostgres.SingleQuotedPowerShell(ruleName)} " +
        $"-ErrorAction SilentlyContinue).Count; Write-Output ('{ProbeSentinel}' + $c); exit 0";

    /// <summary>
    /// PURE parse of <see cref="BuildProbeCommand"/> output: true/false when the sentinel carried a count,
    /// null when it is absent or unparseable (PowerShell missing, the command died, a truncated pipe). A null
    /// is <see cref="FirewallRuleVerdict.ProbeFailed"/>, never a silent "absent" — guessing "absent" would
    /// manufacture a bogus WARN, and guessing "present" would hide a real one.
    /// </summary>
    internal static bool? TryParseProbeOutput(string? output)
    {
        if (string.IsNullOrEmpty(output))
        {
            return null;
        }

        var start = output.IndexOf(ProbeSentinel, StringComparison.Ordinal);
        if (start < 0)
        {
            return null;
        }

        var digits = start + ProbeSentinel.Length;
        var end = digits;
        while (end < output.Length && char.IsAsciiDigit(output[end]))
        {
            end++;
        }

        return end > digits
            && int.TryParse(output[digits..end], NumberStyles.None, CultureInfo.InvariantCulture, out var count)
            ? count > 0
            : null;
    }

    /// <summary>PURE verdict table: what the config asked for vs. what the probe found.</summary>
    internal static FirewallRuleVerdict Classify(bool exposed, bool? present) =>
        present is null ? FirewallRuleVerdict.ProbeFailed
        : exposed
            ? (present.Value ? FirewallRuleVerdict.ExposedRulePresent : FirewallRuleVerdict.ExposedRuleMissing)
            : (present.Value ? FirewallRuleVerdict.LoopbackStaleRule : FirewallRuleVerdict.LoopbackNoRule);

    /// <summary>
    /// PURE: the elevated command that fixes this verdict, or null when there is nothing to fix. This is what
    /// makes the WARN actionable — the operator gets the exact rule to run, not a description of one. Built by
    /// the SAME builders the elevated <c>--configure-firewall</c> verb runs, so the printed command and the
    /// applied command can never drift.
    /// </summary>
    internal static string? BuildRemedyCommand(FirewallRuleVerdict verdict, string ruleName, int port, string? cidr) =>
        verdict switch
        {
            FirewallRuleVerdict.ExposedRuleMissing when !string.IsNullOrWhiteSpace(cidr) =>
                DarlingManagedPostgres.BuildFirewallEnableCommand(ruleName, port, cidr),
            FirewallRuleVerdict.LoopbackStaleRule =>
                DarlingManagedPostgres.BuildFirewallDisableCommand(ruleName),
            _ => null,
        };

    /// <summary>
    /// PURE dedup gate: report only when the (rule, verdict) pair CHANGES. The MCP and web hosts run under a
    /// supervisor that re-attempts a failed start with backoff, so a check wired where the old reconcile sat
    /// would otherwise repeat its WARN on every attempt — the retry spam #1771 explicitly rules out. A steady
    /// state is stated once; a real transition (rule created by an admin, exposure turned off) reports again.
    /// </summary>
    internal static bool ShouldReport(string? lastRuleName, FirewallRuleVerdict? lastVerdict, string ruleName, FirewallRuleVerdict verdict) =>
        lastVerdict != verdict || !string.Equals(lastRuleName, ruleName, StringComparison.Ordinal);

    /// <summary>
    /// Runs the read-only probe and reports the verdict once per state change (see <see cref="ShouldReport"/>).
    /// NEVER writes a rule and NEVER throws except on cancellation — a monitoring service must not fail to
    /// start because it could not read the firewall. Callers pass their own last-reported state and store the
    /// returned value back.
    /// </summary>
    internal static async Task<(string RuleName, FirewallRuleVerdict Verdict)> CheckAsync(
        string ruleName,
        int port,
        bool exposed,
        string? cidr,
        string? lastRuleName,
        FirewallRuleVerdict? lastVerdict,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        FirewallRuleVerdict verdict;
        try
        {
            var (exitCode, output) = await DarlingManagedPostgres.RunPowerShellAsync(
                BuildProbeCommand(ruleName), cancellationToken);

            /* The probe exits 0 for both answers, so a non-zero exit means the probe itself broke; do not read
               a count out of a run that failed. */
            verdict = Classify(exposed, exitCode == 0 ? TryParseProbeOutput(output) : null);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
        catch (Exception ex)
        {
            logger.LogDebug("Firewall probe for '{Rule}' could not run: {Message}", ruleName, ex.Message);
            verdict = FirewallRuleVerdict.ProbeFailed;
        }

        if (!ShouldReport(lastRuleName, lastVerdict, ruleName, verdict))
        {
            return (ruleName, verdict);
        }

        var remedy = BuildRemedyCommand(verdict, ruleName, port, cidr);
        switch (verdict)
        {
            case FirewallRuleVerdict.ExposedRulePresent:
                logger.LogInformation(
                    "Firewall rule '{Rule}' is present. The service verifies this rule but never modifies it — " +
                    "firewall rules are created by the elevated installer (install-darling.ps1).",
                    ruleName);
                break;

            case FirewallRuleVerdict.ExposedRuleMissing:
                logger.LogWarning(
                    "Firewall rule '{Rule}' is MISSING and this endpoint is configured for LAN access on port {Port}, " +
                    "so remote clients will be blocked. The service account cannot create it by design. Run " +
                    "PerformanceMonitor.Darling.Service.exe --configure-firewall from an ELEVATED PowerShell (it " +
                    "reconciles every Darling rule from darling.json), or run just this one:\n{Command}",
                    ruleName, port, remedy ?? "(no command — allowFrom is missing or not a valid CIDR; run --configure-network)");
                break;

            case FirewallRuleVerdict.LoopbackStaleRule:
                logger.LogWarning(
                    "Firewall rule '{Rule}' is still open but this endpoint now binds LOOPBACK ONLY, so the rule is " +
                    "stale. Nothing is listening on the network, so it grants no access. Remove it with " +
                    "PerformanceMonitor.Darling.Service.exe --configure-firewall from an ELEVATED PowerShell, or " +
                    "run just this one:\n{Command}",
                    ruleName, remedy);
                break;

            case FirewallRuleVerdict.ProbeFailed:
                logger.LogWarning(
                    "Could not verify firewall rule '{Rule}' (the read-only probe did not return a usable answer). " +
                    "This does not affect the endpoint; verify the rule by hand with " +
                    "Get-NetFirewallRule -DisplayName '{Rule}'.",
                    ruleName, ruleName);
                break;

            case FirewallRuleVerdict.LoopbackNoRule:
            default:
                /* Loopback-only with no rule is the healthy default — deliberately silent. */
                break;
        }

        return (ruleName, verdict);
    }
}
