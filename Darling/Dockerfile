# The Darling service container (#1804) — builds from source so `docker build` works from any checkout:
#
#   docker build -f Darling/Dockerfile -t performancemonitor-darling .
#
# Build context is the REPO ROOT (the service references PerformanceMonitor.Common / .Collectors /
# .Analysis / Darling.Storage by project). The runtime stage is the official aspnet image (the service
# hosts the web dashboard + MCP endpoint on Kestrel) and sets DOTNET_RUNNING_IN_CONTAINER, which is what
# the bind ladder's container gate keys on — network exposure with the mandatory token/CIDR works here
# under postgres.managed = false, because the compose port mapping is the boundary.
#
# The bundled zero-admin store is Windows-only BY DESIGN and never runs in this image: pair the container
# with the official timescale/timescaledb image (see the compose file) and keep postgres.managed = false.
# Secrets never land in darling.json — use env:/file: references (#1804 stage 1), which are compose
# `secrets:`-friendly.

# Pinned to the SDK global.json asks for, NOT the floating :10.0 tag. global.json requests an exact SDK
# with rollForward "latestPatch", which rolls only inside that SDK's feature band. When it asked for 10.0.302,
# the floating tag advanced to an SDK 10.0.400 image, and every container build died with "A compatible .NET
# SDK was not found ... Requested SDK version: 10.0.302 / Installed SDKs: 10.0.400" and a bare exit code 155.
# It broke dev and both open PRs at once, and it broke on a Microsoft image refresh rather than on any commit here.
#
# The runner-side build never saw it because setup-dotnet resolves its SDK FROM global.json; only the
# container had an independent opinion about which SDK to use. Pinning removes that second opinion: bump
# this line and global.json together, deliberately, instead of being bumped by an upstream tag move.
# Dependabot's dotnet-sdk updates edit only global.json, so DockerfileSdkPinTests fails their pull request
# until this line matches (#3906).
FROM mcr.microsoft.com/dotnet/sdk:10.0.401 AS build

# The msbuild version property for the publish below, threaded in by nightly.yml (#3455) so the
# service compiled into a :nightly image stamps the same 3.7.0-nightly.<date>.<run> string its image
# tag carries. The informational version is what --version prints and what get_collection_health's
# service block serves, so a container answering with the clean release string while running a
# nightly is the defect that issue documents on the zip, one artifact over. Empty by default ON
# PURPOSE: the shell expansion below then adds no flag at all, the publish inherits the version
# Directory.Build.props declares, and every other consumer of this Dockerfile — build.yml's release
# image, a hand-run docker build — produces byte-for-byte the command it produced before the ARG
# existed.
ARG VERSION=
WORKDIR /src
COPY . .
RUN dotnet publish Darling/PerformanceMonitor.Darling.Service/PerformanceMonitor.Darling.Service.csproj \
    -c Release -o /app/publish ${VERSION:+-p:Version=$VERSION}

FROM mcr.microsoft.com/dotnet/aspnet:10.0
# Microsoft.Data.SqlClient probes the Kerberos GSSAPI library at connect time on Linux — even for SQL
# auth — and the aspnet base image does not carry it. Found the hard way in the #1804 container smoke.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /app/publish .

# #3914: where the service keeps the passwords of the least-privilege roles it provisions on the compose store.
# Created owner-only here because Docker seeds a new named volume from the image's directory, mode included, so
# the compose file's darling-credentials volume starts owner-only too. The service creates each file 0600, and
# keeps the store's log-hash key there as well (#4004).
RUN mkdir -p /var/lib/darling/credentials && chmod 700 /var/lib/darling/credentials

# The config mount point the compose file uses; override DARLING_CONFIG to relocate.
ENV DARLING_CONFIG=/config/darling.json

# Web dashboard + MCP (both opt-in via darling.json; exposed here only behind their token gates).
EXPOSE 5153 5152

ENTRYPOINT ["dotnet", "PerformanceMonitor.Darling.Service.dll"]
