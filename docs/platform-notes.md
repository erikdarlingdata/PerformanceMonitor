# Platform Notes

Audience: operators.

Part of the [Performance Monitor documentation](../README.md#documentation).

### AWS RDS for SQL Server: the server certificate

Lite checks the server certificate by default. The certificate of an RDS for SQL Server instance chains to an Amazon RDS certificate authority, such as `rds-ca-rsa2048-g1`, and Windows does not trust those authorities by default. A connection test or `--add-server` then fails with `The certificate chain was issued by an authority that is not trusted.` A self-signed certificate gives the same message, but the cause and the fix are different.

Choose one of these:

1. **Trust the RDS certificate authority on the Lite machine.** Download the certificate bundle for the Region of the instance from the [AWS list of certificate bundles](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.SSL.html#UsingWithRDS.SSL.CertificatesAllRegions). Use the PKCS7 file for that Region, `https://truststore.pki.rds.amazonaws.com/<region>/<region>-bundle.p7b`. The bundle for one Region holds the root certificates of that Region. AWS says to register only root certificates, not intermediate ones. Import every certificate in the file into the trusted roots of the machine, from an elevated PowerShell:

   ```powershell
   $certs = [System.Security.Cryptography.X509Certificates.X509Certificate2Collection]::new()
   $certs.Import("$PWD\<region>-bundle.p7b")
   $store = [System.Security.Cryptography.X509Certificates.X509Store]::new('Root', 'LocalMachine')
   $store.Open('ReadWrite')
   $store.AddRange($certs)
   $store.Close()
   ```

   `Import-Certificate` is not a substitute here: it imports only the first certificate of a `.p7b` file. The machine store covers every Windows user, so it also covers the dedicated user of a headless Lite. Lite then verifies the server, and the connection needs no trust setting.
2. **Trust the certificate for that server.** Check **Trust server certificate (skip certificate validation)** in **Edit Server**, or give `--trust-server-certificate`, or `trust_server_certificate` in JSON, to `--add-server`. The connection stays encrypted, but Lite no longer checks who answers. Use it only on a network that you trust. `Strict` encryption ignores this setting.

### AWS RDS Parameter Group Configuration

`sp_configure` is not available on AWS RDS for SQL Server. Features that depend on server-level configuration must be set through **AWS RDS Parameter Groups** instead.

**Blocked process threshold** — Enables blocked-process-report collection (the richer XE-sourced blocking detail). Without it the blocked-process-report XE will not fire on RDS, but blocking is still captured by the always-on `dmv_blocking_snapshot` collector, so the blocking grid and block-chain viewer stay populated regardless.

1. Open the [AWS RDS Console](https://console.aws.amazon.com/rds/) and navigate to **Parameter groups**
2. Create a new parameter group (or modify the one attached to your instance) — Family `sqlserver-ee-16.0` (or your edition/version), Type DB Parameter Group
3. Search for `blocked process threshold (s)` and set it to `5` (seconds)
4. Apply the parameter group to your RDS instance (may require a reboot if the parameter is static)
5. Verify: `SELECT c.name, c.value_in_use FROM sys.configurations AS c WHERE c.name = N'blocked process threshold (s)';`

**Deadlocks** — No parameter group configuration is required. The SQL Server deadlock monitor runs automatically on all platforms, and the `xml_deadlock_report` Extended Event fires without any threshold setting.

**Azure SQL Database** — The blocked process threshold is fixed at 20 seconds and cannot be changed. The `blocked_process_report` event fires automatically when blocking exceeds this duration.

### AWS RDS and Aurora for PostgreSQL (Darling)

A PostgreSQL server entry in `darling.json` can set `awsRoleArn`, and `awsExternalId` when the role's trust policy asks for one. Darling then assumes that role for its AWS calls for that server. These are the host check, CPU and host memory from Performance Insights, and the log reads. A server with no role uses the AWS credentials of the Darling process. Those can come from an `AWS_PROFILE` profile that has a `role_arn`.

The file seeds a new server only. To change the role of a stored server, use the web Admin page, the `edit_server` MCP tool, or the viewer. Those surfaces can set only roles that the `allowedAwsRoles` list in `darling.json` allows.

Darling reads the logs and Performance Insights data of an RDS or Aurora target only after it checks the host. The host must match the endpoint that AWS reports. If the host does not match, Darling reads nothing for that server and records a PERMISSIONS outcome that names the host and the id.
