/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;

namespace Darling.Tests;

/// <summary>
/// The statement corpus shared by the .NET evaluation tests and the PostgreSQL parity test (#4348): what the
/// filter names, what it leaves alone, and the two strings only the .NET engine bounds with a time limit.
/// </summary>
internal static class SensitiveStatementCorpus
{
    /// <summary>Statements the filter must name. Each is also judged lower-cased and with a leading block
    /// comment (<see cref="Variants"/>).</summary>
    public static readonly string[] Named =
    {
        "CREATE LOGIN [app] WITH PASSWORD = N'S3cret'",
        "ALTER LOGIN [app] WITH PASSWORD = 0x0200AB HASHED",
        "ALTER LOGIN [app] DISABLE",
        "CREATE/* x */LOGIN [a] WITH PASSWORD = 'p'",
        "CREATE\r\nLOGIN [a] FROM WINDOWS",
        "EXEC sp_addlogin 'app', 'S3cret'",
        "EXEC master..sp_password NULL, 'S3cret', 'app'",
        "EXEC sp_addlinkedsrvlogin 'SRV', 'false', NULL, 'u', 'S3cret'",
        "EXEC sp_setapprole 'r', 'S3cret'",
        "OPEN SYMMETRIC KEY k DECRYPTION BY PASSWORD = 'S3cret'",
        "OPEN MASTER KEY DECRYPTION BY PASSWORD = N'S3cret'",
        "CREATE MASTER KEY ENCRYPTION BY PASSWORD = N'S3cret'",
        "CREATE CREDENTIAL c WITH IDENTITY = 'u', SECRET = 'S3cret'",
        "CREATE DATABASE SCOPED CREDENTIAL c WITH IDENTITY = 'SHARED ACCESS SIGNATURE', SECRET = 'sv=1&sig=x'",
        "BACKUP CERTIFICATE c TO FILE = 'f' WITH PRIVATE KEY (FILE = 'k', ENCRYPTION BY PASSWORD = 'S3cret')",
        "RESTORE DATABASE d FROM DISK = 'f' WITH MEDIAPASSWORD = 'S3cret'",
        "CREATE SYMMETRIC KEY k WITH KEY_SOURCE = 'phrase', ALGORITHM = AES_256 ENCRYPTION BY CERTIFICATE c",
        "SELECT * FROM OPENROWSET('MSOLEDBSQL', 'Server=h;UID=u;PWD=S3cret;', 'SELECT 1')",
        "SELECT * FROM OPENDATASOURCE('MSOLEDBSQL', 'Data Source=h;User ID=u;Pwd=S3cret').db.dbo.t",
        "EXEC sp_addlinkedserver @server = 'S', @provider = 'MSOLEDBSQL', @provstr = 'UID=u;PWD=S3cret'",
        "EXEC sp_addpushsubscription_agent @publication = N'p', @job_password = N'S3cret'",
        "ALTER SERVICE MASTER KEY WITH OLD_ACCOUNT = 'a', OLD_PASSWORD = 'S3cret'",
        "ALTER APPLICATION ROLE r WITH PASSWORD = N'S3cret'",
        "SELECT DECRYPTBYPASSPHRASE('phrase', @c)",
        "exec sp_executesql N'UPDATE dbo.t SET c = @pwd', N'@pwd nvarchar(50)', @pwd = N'S3cret'",
        "UPDATE dbo.Creds SET [password] = N'S3cret'",
        "UPDATE t SET \"password\" = 'S3cret'",
        "UPDATE dbo.Users SET pwd = N'S3cret' WHERE id = 7",
        "UPDATE dbo.Users SET password = @p",
        "SELECT 1 WHERE client_secret = 'S3cret'",

        // a typed declaration of a password variable (#5320): the name, a type with an optional length, then the literal
        "DECLARE @mypassword nvarchar(20) = N'S3cret'",
        "DECLARE @pwd varchar(max) = 'S3cret'",
        "DECLARE @secret sysname = N'S3cret'",
        "DECLARE @a int = 1, @pwd varchar(10) = 'S3cret'",
        "DECLARE @a int = 1, @b nvarchar(20) = N'x', @db_passwd nvarchar(20) = N'S3cret', @c int",
        "DECLARE @mypassword varbinary(64) = 0x0200AB",
        "DECLARE @mypassword AS nvarchar(20) = N'S3cret'",
        "DECLARE @mypassword [nvarchar](20) = N'S3cret'",
        "DECLARE @mypassword sys.sysname = N'S3cret'",
        "DECLARE @mypassword nvarchar (20) = N'S3cret'",
        "DECLARE @mypassword decimal(10, 2) = N'S3cret'",
        "DECLARE @pwd nvarchar(20) /* x */ = -- y\r\n N'S3cret'",
        "CREATE PROCEDURE dbo.p @password nvarchar(20) = N'S3cret' AS SELECT 1",
        "exec sp_executesql N'SELECT 1', N'@password nvarchar(20) = N''S3cret'''",
        "DECLARE pwd text := 'secret-x'; BEGIN NULL; END",
        "DECLARE db_secret varchar(20) = 'secret-x'",
        "DECLARE api_secret constant text := e'secret-x'",
        "DECLARE pwd text DEFAULT 'secret-x'",
        "DECLARE pwd text := $q$secret-x$q$",

        // the typed declaration names exactly T3's name set, key_source included (#5320 L1)
        "DECLARE @key_source nvarchar(100) = N'S3cret'",
        "DECLARE key_source text := 'secret-x'",
        "CREATE PROCEDURE dbo.p @key_source varchar(50) = 'S3cret' AS SELECT 1",

        // the PostgreSQL forms
        "ALTER ROLE app PASSWORD 'secret-x'",
        "CREATE ROLE r LOGIN PASSWORD 'secret-x'",
        "CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'u', password 'secret-x')",
        "ALTER SYSTEM SET primary_conninfo = 'host=h password=secret-x'",
        "CREATE SUBSCRIPTION sub CONNECTION 'host=h password=secret-x' PUBLICATION p",
        "ALTER ROLE app SET work_mem = '64MB'",
        "CREATE USER MAPPING FOR u SERVER s OPTIONS (user 'u', secret_access_key 'secret-x')",
        "ALTER SERVER s OPTIONS (ADD token 'secret-x')",
        "CREATE SERVER s FOREIGN DATA WRAPPER w OPTIONS (api_key 'secret-x')",

        // a libpq sslpassword in a connection string (bare, spaced, quoted and in a URI), a pgcrypto call that
        // carries a literal, and a client tool's -P option inside xp_cmdshell
        "SELECT dblink_connect('c', 'host=db2 dbname=app user=app sslcert=/k/c.crt sslkey=/k/c.key sslpassword=Hunter2')",
        "SELECT * FROM dblink('host=db2 user=app sslkey=/k/c.key sslpassword = Hunter2', 'select 1') AS t(x int)",
        "SELECT dblink_connect('c', 'host=db2 sslkey=/k/c.key sslpassword=''Hunter 2''')",
        "SELECT dblink_connect('postgresql://db2/app?sslkey=/k/c.key&sslpassword=Hunter2')",
        "SELECT id FROM app.users WHERE pw = crypt('hunter2', pw)",
        "SELECT pgp_sym_decrypt(card, 'K3y-2026') FROM payments",
        "SELECT pgp_sym_decrypt_bytea(decode(card, 'hex'), 'K3y-2026') FROM payments",
        "INSERT INTO t VALUES (pgp_sym_encrypt(note, $k$K3y-2026$k$))",
        "UPDATE app.users SET pw = crypt ('hunter2', gen_salt('bf'))",
        "SELECT hmac(body, 'K3y-2026', 'sha256') FROM msgs",
        "SELECT decrypt_iv(data, 'key12345', 'iv123456', 'aes') FROM t",
        @"EXEC master..xp_cmdshell 'bcp Sales.dbo.Orders out D:\x.dat -S PROD01 -U etl -P S3cret! -c'",
        "EXEC master..xp_cmdshell 'sqlcmd -S PROD01 -U etl -P \"S3cret!\" -Q \"select 1; select 2\"'",
        "EXEC xp_cmdshell 'osql -S PROD01 -U sa -PS3cret! -i x.sql'",
        "EXEC xp_cmdshell 'isql -S PROD01 -U sa -P S3cret!'",
        "EXEC xp_cmdshell 'bcp db.dbo.t in x.dat -P S3cret! -U etl -S PROD01'",

        // a key or passphrase one call deep, an armored private key anywhere, a /P switch, SQLCMDPASSWORD, a $ and a
        // digit at the start of a conninfo value, a login written before the tool name, and a multi-line bcp
        "SELECT pgp_sym_decrypt(card, trim('K3y-2026')) FROM payments",
        "SELECT id FROM app.users WHERE pw = crypt(lower('Hunter2'), pw)",
        "SELECT id FROM app.users WHERE pw = crypt(CAST('hunter2' AS text), pw)",
        "SELECT pgp_sym_decrypt(card, f('K3y-2026')) FROM payments",
        "SELECT pgp_pub_decrypt(msg, dearmor('-----BEGIN PGP PRIVATE KEY BLOCK-----\n\nlQOYBF...\n-----END PGP PRIVATE KEY BLOCK-----')) FROM t",
        "SELECT pgp_pub_decrypt(msg, dearmor(trim('-----BEGIN PGP PRIVATE KEY BLOCK-----\n\nlQOYBF...\n-----END PGP PRIVATE KEY BLOCK-----'))) FROM t",
        "INSERT INTO app.keys (id, armored) VALUES (1, '-----BEGIN PGP PRIVATE KEY BLOCK-----\n\nlQOYBF...\n-----END PGP PRIVATE KEY BLOCK-----')",
        @"EXEC xp_cmdshell 'sqlcmd /S PROD01 /U etl /P S3cret! /i D:\load.sql'",
        @"EXEC xp_cmdshell 'set SQLCMDPASSWORD=S3cret!&& sqlcmd -S PROD01 -U etl -i D:\load.sql'",
        "SELECT dblink_connect('c', 'host=db2 sslkey=/k/c.key sslpassword=$9Kite')",
        @"DECLARE @creds varchar(100) = '-S PROD01 -U etl -P S3cret!'; DECLARE @cmd varchar(4000) = 'bcp Sales.dbo.Orders out D:\x.dat -c ' + @creds; EXEC xp_cmdshell @cmd;",
        "EXEC master..xp_cmdshell 'bcp Sales.dbo.Orders out D:\\x.dat\n    -S PROD01 -U etl\n    -P S3cret! -c'",

        // a setting whose name says key, secret, password, passphrase or token set to a literal (SET ... = / TO, and
        // set_config), and a password-like column compared to a hash of a literal (HASHBYTES, digest)
        "SET app.encryption_key = 'K3y-2026'",
        "SET LOCAL app.encryption_key = 'K3y-2026'",
        "SET SESSION app.api_token TO 'abc123'",
        "SET app.vault_key TO E'K3y-2026'",
        "SET app.master_passphrase = $p$K3y 2026$p$",
        "SELECT set_config('app.encryption_key', 'K3y-2026', false)",
        "SELECT set_config ( 'app.api_token' , 'abc123' , true )",
        "SELECT set_config('app.master_passphrase', $q$K3y 2026$q$, false)",
        "SELECT id FROM app.users WHERE pass_hash = HASHBYTES('SHA2_256', 'hunter2')",
        "SELECT id FROM dbo.Users u WHERE u.PwdHash = HASHBYTES('SHA2_512', N'hunter2')",
        "SELECT id FROM dbo.Users WHERE [PassHash] = HASHBYTES ( 'SHA2_256' , N'hunter2' )",
        "SELECT id FROM app.users WHERE pass_hash = digest('hunter2', 'sha256')",
        "SELECT id FROM app.users WHERE pwd_digest = digest(E'hunter2', 'sha512')",
    };

    /// <summary>Statements the filter must leave alone.</summary>
    public static readonly string[] NotNamed =
    {
        "SELECT name FROM sys.sql_logins",
        "EXECUTE AS LOGIN = N'app'",
        "SELECT * FROM dbo.Users WHERE PasswordHash = @h",
        "UPDATE dbo.Users SET PasswordHash = HASHBYTES('SHA2_256', @p)",
        "SELECT * FROM OPENROWSET(BULK N'f.json', SINGLE_CLOB) AS j",
        "EXEC sp_helplogins",
        "SELECT secret_id FROM dbo.t WHERE secret_id = 5",
        "SELECT * FROM t WHERE pwd = @p",
        "SELECT * FROM t WHERE pwd = $1",

        // typed declarations that carry no literal, and neighbours of the typed-declaration shape (#5320)
        "DECLARE @password nvarchar(20) = @p",
        "DECLARE @mypassword nvarchar(20)",
        "DECLARE pwd text := $1",
        "CREATE PROCEDURE dbo.p @password nvarchar(20) = NULL AS SELECT 1",
        "CREATE TABLE dbo.u (id int, password_hash varbinary(64) NOT NULL)",
        "SELECT * FROM t WHERE pwd_len int",
        "DECLARE @pwd_len int = 8",
        "DECLARE @pwd int = 8",
        "DECLARE @mykey_source nvarchar(100) = N'x'",
        "DECLARE @key_sources nvarchar(100) = N'x'",
        "SELECT password varchar FROM t WHERE x = 'a'",
        "SELECT * FROM t WHERE is_secret AND state = 'x'",
        "SELECT [password] FROM t",
        "SET password_encryption = 'scram-sha-256'",
        "ALTER INDEX ix ON dbo.t REBUILD",
        "BACKUP DATABASE d TO URL = 'https://storage.example/c/d.bak' WITH CREDENTIAL = 'cred'",
        "ALTER AVAILABILITY GROUP ag FAILOVER",

        // the PostgreSQL neighbours
        "SELECT * FROM t WHERE password_changed_at > $1",
        "SET work_mem = '64MB'",
        "SELECT rolname FROM pg_roles",
        "SELECT 1",
        "CREATE TABLE passwords (id int)",

        // neighbours of the sslpassword, pgcrypto and client tool shapes
        "SELECT * FROM t WHERE sslpassword_set = $1",
        "SELECT password_reset_at FROM users",
        "SELECT crypto_id FROM t",
        "SELECT encrypt_status FROM jobs",
        "SELECT encrypted FROM t WHERE state = 'x'",
        "SELECT id FROM app.users WHERE pw = crypt($1, pw)",
        "SELECT id FROM app.users WHERE name = 'bob' AND pw = crypt($1, pw)",
        "SELECT pgp_sym_decrypt(card, $1) FROM payments",
        "SELECT pgp_sym_decrypt(card, f($1)) FROM payments",
        "SELECT pgp_pub_encrypt(note, dearmor($1)) FROM t",
        "SELECT '-----BEGIN PGP PUBLIC KEY BLOCK-----'",
        @"EXEC master..xp_cmdshell 'bcp Sales.dbo.Orders out D:\x.dat -S PROD01 -T -c'",
        "EXEC master..xp_cmdshell 'sqlcmd -S PROD01 -E -Q \"select 1\"'",
        @"EXEC master..xp_cmdshell 'dir D:\'",
        @"EXEC xp_cmdshell 'sqlcmd -S PROD01 -E -i D:\load.sql'",
        "SELECT * FROM t WHERE note = 'isql' AND pw = 5",
        "SET search_path = 'x'",
        "SET work_mem = '64MB'",
        "SELECT set_config('application_name', 'x', false)",
        "SET statement_timeout TO '5s'",
        "SELECT set_config('app.api_token', $1, false)",
        "SELECT HASHBYTES('SHA2_256', 'abc')",
        "SELECT id FROM t WHERE checksum = digest('abc', 'sha256')",
        "SELECT id FROM app.users WHERE pass_hash = digest($1, 'sha256')",
    };

    /// <summary>The strings the plan once expected the .NET judge to time out on: many comment tokens after a
    /// keyword. The linear-time pre-check (#5320) now answers Clean for them, as PostgreSQL does. They are not
    /// part of the parity set, because the full .NET judge alone still backtracks on them.</summary>
    public static readonly string[] Adversarial =
    {
        "create" + string.Concat(Enumerable.Repeat(" --", 40)) + "x",
        "password" + string.Concat(Enumerable.Repeat(" --", 40)) + "x",
    };

    /// <summary>Strings aimed at the typed-declaration alternative (#5320): a password word, then many comment
    /// tokens, a type and a long length part, but no literal. The pre-check answers Clean for each of them. Kept
    /// apart from <see cref="Adversarial"/>, which the output-filter tests prefix with a pre-check hit so the
    /// full judge runs and times out; these do not make the full judge slow.</summary>
    public static readonly string[] TypedDeclarationAdversarial =
    {
        "pwd" + string.Concat(Enumerable.Repeat(" --", 40)) + " nvarchar(20) = x",
        "secret" + string.Concat(Enumerable.Repeat(" /* c */", 40)) + " text :",
        "pwd varchar(" + new string(' ', 100_000) + "x",
        "declare @pwd " + string.Concat(Enumerable.Repeat("nvarchar ", 5_000)) + "= @p",
    };

    /// <summary>A typed secret-named variable followed by a long dash banner, in a value where another part passes
    /// the pre-check (<c>hashed_pwd = HASHBYTES(1, @p)</c> has no word start, so only the pre-check's superset hits
    /// it, and the full judge then runs). The typed-declaration alternative backtracks on the run of dashes (the known
    /// comment-run case, #5320 L2), so the .NET judge answers TimedOut, which counts as named, while PostgreSQL
    /// answers Clean. It errs toward withholding. Not part of the parity set; the tests pin both answers.</summary>
    public static readonly string[] NamedByTimeoutInDotNetOnly =
    {
        "UPDATE u SET hashed_pwd = HASHBYTES(1, @p); DECLARE @pwd nvarchar(20) " + new string('-', 60) + "\nSELECT 1",
    };

    /// <summary>A named statement as written, lower-cased, and with a leading block comment.</summary>
    public static string[] Variants(string text) => new[] { text, text.ToLowerInvariant(), "/* c */ " + text };
}
