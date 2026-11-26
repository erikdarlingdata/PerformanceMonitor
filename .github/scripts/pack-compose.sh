#!/usr/bin/env bash
# Packs Darling/compose/ into PerformanceMonitorDarling-compose-<version>.tar.gz (#6123, #9333).
# build.yml (the release) and nightly.yml (the nightly) both call this, so the two archives cannot drift.
#
# Usage: pack-compose.sh <version> <releases_dir>
#   <version>       the release version, or the nightly's own version. It is also the image tag the packed
#                   docker-compose.yml pins the darling service to (the :<version> tag both workflows push).
#   <releases_dir>  where the archive is written. It must exist.
# Run it from the repository root. It prints the archive's listing and fails if the pack is wrong.
#
# The packed copy names this version's image tag instead of :latest, so the files pull that version's image.
# Only tracked files ship: the checkout has no local secrets. The archive opens into one folder so a user can
# extract it and cd into it. Line endings are made LF, because .gitattributes checks text out as CRLF.
set -euo pipefail

version="${1:?usage: pack-compose.sh <version> <releases_dir>}"
releases_dir="${2:?usage: pack-compose.sh <version> <releases_dir>}"

compose_dir="PerformanceMonitorDarling-compose-${version}"
archive="${releases_dir}/PerformanceMonitorDarling-compose-${version}.tar.gz"
compose_stage="$(mktemp -d)"
mkdir -p "${compose_stage}/${compose_dir}"
cp -R Darling/compose/. "${compose_stage}/${compose_dir}/"
find "${compose_stage}/${compose_dir}" -type f -exec sed -i 's/\r$//' {} +
compose_file="${compose_stage}/${compose_dir}/docker-compose.yml"
sed -i "s|\(image: ghcr\.io/erikdarlingdata/performancemonitor-darling\):latest|\1:${version}|" "${compose_file}"
if [ "$(grep -c "image: ghcr\.io/erikdarlingdata/performancemonitor-darling:${version}$" "${compose_file}")" != "1" ] || grep -q "performancemonitor-darling:latest" "${compose_file}"; then
  echo "::error::The packed compose file does not name the darling image at :${version}."
  exit 1
fi
# #6140: setup.sh is the quickstart's first step, so it must open with its execute bit. The checkout holds it as
# mode 755 already, and chmod makes that certain. The tar listing below must show it executable and the
# script must parse under a POSIX shell, so a bad pack fails the job and not the user's first command.
chmod 755 "${compose_stage}/${compose_dir}/setup.sh"
sh -n "${compose_stage}/${compose_dir}/setup.sh"
# #9701: owned by root, so a root extract does not leave the build machine's uid on the files.
tar --owner=0 --group=0 --numeric-owner -C "${compose_stage}" -czf "${archive}" "${compose_dir}"
tar -tzf "${archive}"
# grep reads the whole listing (no -q): under pipefail, a grep that stops at its first match can make tar fail
# its next write, and the check then fails on an archive that is fine (#9671).
if ! tar -tvzf "${archive}" | grep -E '^-rwxr-xr-x .* '"${compose_dir}"'/setup\.sh$' >/dev/null; then
  echo "::error::setup.sh is missing from the compose archive or lacks its execute bit (#6140)."
  exit 1
fi
rm -rf "${compose_stage}"
