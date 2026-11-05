# Authentication

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

`PerformanceMonitor.Common.AuthenticationTypes` supports seven authentication types, defined once there and named the same way in every edition. **Not every edition connects with all seven**, so the table says which does what: Lite offers all of them. The Darling service and its Viewer connect with four: Windows, SQL Server, Service Principal and Managed Identity (`auth` set to `integrated`, `sql`, `serviceprincipal` or `managedidentity` in `darling.json`). The service runs headless, so it refuses the other three, which need a person at the machine or their own sign-in on it.

| Type | Editions | Interactive? | Credential stored? | Where |
|---|---|---|---|---|
| Windows | Lite, Darling | No | None | — |
| SQL Server | Lite, Darling | No | Password | Lite: Windows Credential Manager. Darling: see below |
| Entra ID (MFA) | Lite | Yes, once per session | None | — |
| Service Principal | Lite, Darling | No | Client secret | Lite: Windows Credential Manager. Darling: see below |
| Managed Identity | Lite, Darling | No | None | — |
| Existing Azure Sign-In | Lite | No | None | — (established outside the app, e.g. `az login`) |
| Device Code | Lite | Yes, once per session | None | — (entered in a browser, on any device) |

**Where Darling keeps a SQL password or a client secret.** A server in `darling.json` holds it in one of three forms. `encryptedPassword` is a LocalMachine DPAPI blob that `--encrypt-password` produces. `password` holds an `env:NAME` or `file:PATH` reference, or the value itself. A server added in the Darling Viewer keeps its password or secret in the store, sealed to the service's RSA-3072 password key. Only the service can read it, and only for that server's connection settings.

**Managed Identity and Service Principal** are non-interactive Azure AD (Entra ID) authentication modes, added for fleet onboarding of Azure SQL Database / Managed Instance without a per-server interactive MFA prompt (see [#1038](https://github.com/erikdarlingdata/PerformanceMonitor/issues/1038)). Both map directly to `Microsoft.Data.SqlClient`'s native `SqlAuthenticationMethod` (`ActiveDirectoryServicePrincipal` / `ActiveDirectoryManagedIdentity`) — PerformanceMonitor never acquires, caches, or stores a token itself; the official Microsoft driver handles that internally.

- **Managed Identity** requires the machine running the app/service to itself be an Azure resource (VM, App Service, etc.) with a system- or user-assigned managed identity. That identity is then provisioned as a user directly on each target database (see the [Azure SQL Database section of Permissions](permissions.md#azure-sql-database-lite--darling)). Nothing is stored locally.
- **Service Principal** uses an Entra app registration's client id + secret. The client id is non-secret and stored in config. In Lite the secret is stored only in Windows Credential Manager, same as a SQL auth password. Darling keeps it the way it keeps a SQL password (above).

In `darling.json`, a Service Principal server sets `"auth": "serviceprincipal"`, the application (client) id in `username`, and the client secret in `encryptedPassword`. That is the same pair of fields as a SQL login. A Managed Identity server sets `"auth": "managedidentity"`. A system-assigned identity needs no other field. A user-assigned identity puts its client id in `username`. The Darling Viewer writes the same values, and the other server fields are in [Configure darling.json](darling/install.md#configure-darlingjson).

### Existing Azure Sign-In (Lite only, `ActiveDirectoryDefault`)

Labelled **Azure — Existing Sign-In (az login)** in Lite's connection dialog, and added because **Entra ID (MFA) cannot be repaired in place** (see [#3196](https://github.com/erikdarlingdata/PerformanceMonitor/issues/3196), [#3214](https://github.com/erikdarlingdata/PerformanceMonitor/issues/3214)). `Microsoft.Data.SqlClient` forces the Windows account broker (WAM) on for any caller using the driver's own Entra application id, and `UseWamBroker` is inert in that configuration — so when the broker refuses a sign-in, the app has no browser fallback to reach for. This mode maps to `SqlAuthenticationMethod.ActiveDirectoryDefault`, whose arm in the driver returns before any MSAL public-client application is constructed, which is the only place a broker is ever attached.

It signs in as **whichever Azure identity is already established on the machine** and **never prompts** — the driver hard-codes `ExcludeInteractiveBrowserCredential = true`, so there is no browser and no account picker. It searches, in order: the `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` environment variables, a workload identity, a managed identity, a Visual Studio or VS Code sign-in, an Azure CLI session (`az login`), an Azure PowerShell session, and an Azure Developer CLI session. Two consequences worth knowing before you pick it:

- **With no Azure sign-in on the machine it fails rather than asking for one.** Run `az login` first. The connection dialog says which of the two failure modes happened — nothing found, or one found and broken — and the log names each source and why it declined.
- **The order is the driver's, not yours.** There is no seam to narrow the list: the driver constructs the credential chain itself and this app cannot pass options into it. On a machine with several Azure identities set up, this connects as whichever comes first, which is not necessarily the one you signed into Windows with. When a specific identity matters, use **Service Principal**, which names it.
- **The log says which one won.** Lite records the credential type `DefaultAzureCredential` actually selected — `AzureCliCredential`, `EnvironmentCredential`, `ManagedIdentityCredential` and so on — so "whichever comes first" is answerable after the fact rather than only a caveat. Expect exactly one such line per run of the app: `Azure.Identity` reports the selection once per credential instance and the driver caches that instance for the process, so later connections have nothing new to report and say so at `Debug`. The type name is the only thing recorded; no tenant id, account or scope.

Darling does not offer this mode. The Darling service connects with Windows, SQL Server, Service Principal and Managed Identity only, so its Viewer refuses this mode when you save a server. To connect to Azure SQL as a named Azure identity from Darling, use Service Principal or Managed Identity.

Not yet confirmed against a live Entra tenant. The mechanism above is read off the pinned `Microsoft.Data.SqlClient` 7.1.1 and `Azure.Identity` 1.18.0 sources. Tests pin the behaviour. Nobody here has a tenant to run it against. Treat it as worth trying rather than as known to work, and please report what happens.

### Device Code (Lite only, `ActiveDirectoryDeviceCodeFlow`)

Labelled **Azure — Device Code (browser on any device)** in Lite's connection dialog. It shows a short code and a URL; you open that URL in a browser — on the same machine or on a phone — enter the code, and sign in normally, MFA included. Lite never sees your password, and the browser does not have to be on the machine running Lite.

It exists because **the other two Entra paths each depend on something an elevated process cannot reach**, and for different reasons (see [#3196](https://github.com/erikdarlingdata/PerformanceMonitor/issues/3196)):

- **Entra ID (MFA)** hands sign-in to the Windows account broker (WAM), which cannot reach the interactive user's logon session from a process launched as a different account. It fails with `0x80070520` / `unknown_broker_error`, and there is no app-side opt-out — `ActiveDirectoryAuthenticationProviderOptions.UseWamBroker` looks like one and is not.
- **Existing Azure Sign-In** reads the credential cached under `%USERPROFILE%`, which under elevation is the *launching* account's profile. An `az login` done in your own session is not there.

Device code has neither dependency. `DeviceCodeRequest` in `Microsoft.Identity.Client` POSTs to the tenant's device-code endpoint, invokes the callback with the code, and polls the token endpoint — no broker, no profile, no cached credential. `WithBroker` governs interactive and silent acquisition and has nothing to say about a device-code request, which is why the mode is broker-free even though the driver attaches a broker to the MSAL application it shares with the other modes.

Two things worth knowing before you pick it:

- **You have about three minutes.** That limit is the SQL driver's, not Lite's: its device-code arm builds its own three-minute cancellation and passes that rather than the `Connect Timeout`-derived one, so raising `Connect Timeout` does not extend it. Closing the code window gives up and hands the dialog back.
- **It is interactive, so it behaves like Entra ID (MFA) does.** The periodic connectivity sweep skips these servers rather than raising a code at nobody, but **data collection does not skip them**: expect one code window per run of the app, including on an unattended collection cycle shortly after Lite starts, after which the driver's cached token serves the rest of the run. Declining a prompt stops it being raised again until you open the server yourself. It is also not offered in **Add Multiple Servers**, where one code per server would be a prompt storm of codes that expire faster than they can be entered.
- **Every code window names the server its code is for.** That name comes from the token acquisition that produced the code, not from whichever sign-in happened to be in progress, so a window cannot name one server beside another's code. Background reads raise named prompts too; what a background prompt cannot do is hand a waiting dialog back when you close it, because nothing is waiting behind it.
- **Only one device-code sign-in can be in flight at a time**, and a second is refused with a message rather than queued. Two sign-ins to the same server describe identically, and there is one slot for the sign-in a window's Cancel belongs to, so the first one's code could be displayed on the second one's window. Finish or close the one on screen and try again.

Darling does not offer this mode. The service is a headless collector with nobody to read a code, so its Viewer refuses this mode when you save a server. Darling connects with Windows, SQL Server, Service Principal and Managed Identity only.

Not yet confirmed against a live Entra tenant. The mechanism above is read off the pinned `Microsoft.Data.SqlClient` 7.1.1 and `Microsoft.Identity.Client` 4.84.2 sources. Tests pin the behaviour. Nobody here has a tenant to run it against. Treat it as worth trying rather than as known to work, and please report what happens.

### Credential Profiles (Lite, fleet onboarding)

For a fleet of servers sharing one identity — one managed identity or one service principal used across many Azure SQL databases — Lite has **Credential Profiles**: a named, reusable credential that any number of server entries can reference instead of each one carrying its own inline auth. Create one under **Manage Servers → Credential Profiles…**, then point server entries at it. Profiles live in `profiles.json` alongside `servers.json`; a Managed Identity profile stores no secret, and Service Principal / SQL Server profile secrets live in Windows Credential Manager, never in the JSON file.
