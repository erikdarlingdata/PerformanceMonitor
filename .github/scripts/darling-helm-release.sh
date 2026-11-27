#!/usr/bin/env bash
# Stamps, checks and packages the Darling Helm chart for a release (#8017). Called by
# .github/workflows/darling-helm-release.yml, tested by Darling/helm/performancemonitor-darling/tests/release-tests.sh.
#
#   darling-helm-release.sh package --chart <dir> --tag <vX.Y.Z> --version <X.Y.Z> --owner <owner> \
#                                   --digest <sha256:...> --out <dir>
#     Copies the chart into <out>/chart (the chart directory itself is never written), strips CR line endings from the
#     copy, stamps Chart.yaml version/appVersion and values.yaml image.tag/image.digest, checks that nothing else changed,
#     runs helm lint and helm template, checks the rendered image, then writes
#     <out>/performancemonitor-darling-<version>.tgz and <out>/SHA256SUMS-helm.txt. The last stdout line is
#     package=<path of the .tgz>.
#
#   darling-helm-release.sh verify-stamp <original chart dir> <stamped chart dir> <version> <digest>
#     Exits 0 only when the stamped chart differs from the original in exactly the four stamped values, holding the
#     expected ones.
#
#   darling-helm-release.sh decide --version <X.Y.Z> --exists <true|false> --signed <true|false> \
#                                  --assets "<release asset names>" --overwrite <true|false>
#     What a publish run does, from what the registry and the release already hold. Prints three lines:
#       push=<true|false>     push the chart packaged by this run;
#       sign=<true|false>     sign the version's digest (the pushed one, or the one already in the registry);
#       upload=<none|packaged|pulled>  attach the .tgz and SHA256SUMS-helm.txt: this run's package, or the chart
#                             already published (so the release assets always match the registry), or nothing.
#     A published version is never replaced unless --overwrite true: a re-run only adds what is missing. Release assets
#     with no chart in the registry are refused unless --overwrite true, because they would not match a new push.
#
#   darling-helm-release.sh same-chart <published .tgz> <packaged .tgz> <digest>
#     Exits 0 only when the chart already in the registry is this release's chart: Chart.yaml and values.yaml equal this
#     run's package once parsed (so formatting cannot differ them), the published values pin <digest>, and every other
#     file is byte-identical. Run before a published chart is signed or attached to the release, so a chart pushed
#     under X.Y.Z by anything else is never vouched for.
#
# Every refusal is one stderr line starting "darling-helm-release: " and exit 1. Needs helm, yq (mikefarah, v4),
# sha256sum and tar. The version is read from Directory.Build.props by the workflow, not here: the version-read tests
# scan the workflows.
set -euo pipefail

readonly CHART_NAME=performancemonitor-darling
readonly TAG_PATTERN='^v[0-9]+\.[0-9]+\.[0-9]+$'
readonly DIGEST_PATTERN='^sha256:[0-9a-f]{64}$'

die() {
  echo "darling-helm-release: $*" >&2
  exit 1
}

for tool in helm yq sha256sum tar; do
  command -v "${tool}" > /dev/null || die "${tool} is not on PATH"
done

# helm show <chart|values> of a chart directory as JSON with sorted keys, so two charts compare by content.
show_json() {
  helm show "$1" "$2" | tr -d '\r' | yq -o=json -I=2 'sort_keys(..)'
}

# Every leaf of a JSON document as path=value, one per line: names the first difference between two documents.
leaves() {
  yq -p=json -o=json -I=0 '.. | select(tag != "!!map" and tag != "!!seq") | (path | join(".")) + "=" + (. | tostring)' \
    | sed 's/^"//; s/"$//'
}

# compare <label> <expected json> <actual json>: dies naming the first leaf path that differs.
compare() {
  local label="$1" expected="$2" actual="$3" first
  if [ "${expected}" = "${actual}" ]; then
    return 0
  fi
  first="$(diff <(leaves <<<"${expected}") <(leaves <<<"${actual}") | grep -m1 -E '^[<>] ' | sed -E 's/^[<>] //; s/=.*$//' || true)"
  die "the stamped ${label} differs from the release's in more than the stamped values (first difference: ${first:-a structural change})"
}

verify_stamp() {
  [ "$#" -eq 4 ] || die "usage: verify-stamp <original chart dir> <stamped chart dir> <version> <digest>"
  local original="$1" stamped="$2" version="$3" digest="$4" orig_chart new_chart orig_values new_values

  orig_chart="$(show_json chart "${original}")"
  new_chart="$(show_json chart "${stamped}")"
  orig_values="$(show_json values "${original}")"
  new_values="$(show_json values "${stamped}")"

  compare "Chart.yaml" \
    "$(yq -p=json -o=json 'del(.version, .appVersion)' <<<"${orig_chart}")" \
    "$(yq -p=json -o=json 'del(.version, .appVersion)' <<<"${new_chart}")"
  compare "values.yaml" \
    "$(yq -p=json -o=json 'del(.image.tag, .image.digest)' <<<"${orig_values}")" \
    "$(yq -p=json -o=json 'del(.image.tag, .image.digest)' <<<"${new_values}")"

  [ "$(yq -p=json '.version' <<<"${new_chart}")" = "${version}" ] || die "the stamped Chart.yaml version is not ${version}"
  [ "$(yq -p=json '.appVersion' <<<"${new_chart}")" = "${version}" ] || die "the stamped Chart.yaml appVersion is not ${version}"
  [ "$(yq -p=json '.image.tag' <<<"${new_values}")" = "${version}" ] || die "the stamped values.yaml image.tag is not ${version}"
  [ "$(yq -p=json '.image.digest' <<<"${new_values}")" = "${digest}" ] || die "the stamped values.yaml image.digest is not ${digest}"
}

package() {
  local chart="" tag="" version="" owner="" digest="" out=""
  while [ "$#" -gt 0 ]; do
    case "$1" in
      --chart) chart="$2" ;;
      --tag) tag="$2" ;;
      --version) version="$2" ;;
      --owner) owner="$2" ;;
      --digest) digest="$2" ;;
      --out) out="$2" ;;
      *) die "unknown argument: $1" ;;
    esac
    shift 2 || die "missing a value after $1"
  done
  for name in chart tag version owner digest out; do
    [ -n "${!name}" ] || die "--${name} is required"
  done

  [[ "${tag}" =~ ${TAG_PATTERN} ]] || die "${tag} is not a release tag (expected vMAJOR.MINOR.PATCH)"
  [ "v${version}" = "${tag}" ] || die "tag ${tag} does not match version ${version} from Directory.Build.props"
  [[ "${digest}" =~ ${DIGEST_PATTERN} ]] || die "image digest '${digest}' is not sha256:<64 lowercase hex>"
  [ -f "${chart}/Chart.yaml" ] || die "${chart} is not a chart directory"

  local image repository
  image="ghcr.io/${owner,,}/${CHART_NAME}"
  repository="$(yq '.image.repository' "${chart}/values.yaml" | tr -d '\r')"
  [ "${repository}" = "${image}" ] \
    || die "image.repository in values.yaml is ${repository}, not ${image}: the chart would pin an image this repository's release did not push"

  mkdir -p "${out}"
  out="$(cd "${out}" && pwd)"
  [ ! -e "${out}/chart" ] || die "${out}/chart already exists"

  local work original stamped
  work="$(mktemp -d)"
  trap 'rm -rf "${work}"' RETURN
  original="${work}/${CHART_NAME}"
  stamped="${out}/chart"

  # The checkout is CRLF on the runner (.gitattributes: * text=auto eol=crlf); the published chart is LF only.
  cp -R "${chart}" "${original}"
  find "${original}" -type f -exec sed -i 's/\r$//' {} +
  cp -R "${original}" "${stamped}"

  VERSION="${version}" yq -i '.version = strenv(VERSION) | .appVersion = strenv(VERSION)' "${stamped}/Chart.yaml"
  VERSION="${version}" DIGEST="${digest}" yq -i '.image.tag = strenv(VERSION) | .image.digest = strenv(DIGEST)' \
    "${stamped}/values.yaml"
  verify_stamp "${original}" "${stamped}" "${version}" "${digest}"

  local lint
  lint="$(helm lint "${stamped}" 2>&1)" || die "helm lint failed: ${lint}"

  local rendered expected
  rendered="$(helm template release "${stamped}" 2>&1)" || die "helm template failed: ${rendered}"
  rendered="$(yq 'select(.kind == "Deployment") | .spec.template.spec.containers[] | select(.name == "darling") | .image' <<<"${rendered}")"
  expected="${image}:${version}@${digest}"
  [ "${rendered}" = "${expected}" ] || die "the packaged chart renders image ${rendered}, expected ${expected}"

  local packaged
  packaged="$(helm package "${stamped}" --destination "${out}" 2>&1)" || die "helm package failed: ${packaged}"
  local tgz="${CHART_NAME}-${version}.tgz"
  [ -f "${out}/${tgz}" ] || die "helm package did not write ${tgz}: ${packaged}"
  (cd "${out}" && sha256sum "${tgz}" > SHA256SUMS-helm.txt)

  echo "package=${out}/${tgz}"
}

decide() {
  local version="" exists="" signed="" assets="" overwrite="" have_assets=0 complete_assets=0
  local assets_set=0
  while [ "$#" -gt 0 ]; do
    [ "$#" -ge 2 ] || die "missing a value after $1"
    case "$1" in
      --version) version="$2" ;;
      --exists) exists="$2" ;;
      --signed) signed="$2" ;;
      --assets) assets="$2"; assets_set=1 ;;
      --overwrite) overwrite="$2" ;;
      *) die "unknown argument: $1" ;;
    esac
    shift 2
  done
  [ -n "${version}" ] || die "--version is required"
  [ "${assets_set}" = 1 ] || die "--assets is required (an empty string for none)"
  for flag in exists signed overwrite; do
    case "${!flag}" in
      true | false) ;;
      *) die "--${flag} must be true or false, not '${!flag}'" ;;
    esac
  done

  local tgz="${CHART_NAME}-${version}.tgz" name
  for name in ${assets}; do
    if [ "${name}" = "${tgz}" ] || [ "${name}" = "SHA256SUMS-helm.txt" ]; then
      have_assets=$((have_assets + 1))
    fi
  done
  [ "${have_assets}" -eq 2 ] && complete_assets=1

  if [ "${overwrite}" = true ]; then
    printf 'push=true\nsign=true\nupload=packaged\n'
    return 0
  fi
  if [ "${exists}" = false ]; then
    [ "${have_assets}" -eq 0 ] \
      || die "the release already carries chart assets but the registry has no ${version}: run with overwrite to replace them"
    printf 'push=true\nsign=true\nupload=packaged\n'
    return 0
  fi
  printf 'push=false\nsign=%s\nupload=%s\n' \
    "$([ "${signed}" = true ] && echo false || echo true)" \
    "$([ "${complete_assets}" = 1 ] && echo none || echo pulled)"
}

same_chart() {
  [ "$#" -eq 3 ] || die "usage: same-chart <published .tgz> <packaged .tgz> <digest>"
  local published="$1" packaged="$2" digest="$3" work pub ours file
  local replace="the registry holds a different ${CHART_NAME} under this version; a hand run with overwrite: true replaces it"
  [ -f "${published}" ] || die "${published} does not exist"
  [ -f "${packaged}" ] || die "${packaged} does not exist"
  work="$(mktemp -d)"
  trap 'rm -rf "${work}"' RETURN
  mkdir -p "${work}/published" "${work}/packaged"
  tar -m -xzf "${published}" -C "${work}/published"
  tar -m -xzf "${packaged}" -C "${work}/packaged"
  pub="${work}/published/${CHART_NAME}"
  ours="${work}/packaged/${CHART_NAME}"
  [ -d "${pub}" ] && [ -d "${ours}" ] || die "a package does not hold a ${CHART_NAME} chart directory"

  [ "$(show_json values "${pub}" | yq -p=json '.image.digest')" = "${digest}" ] \
    || die "the published chart does not pin image digest ${digest}: ${replace}"
  [ "$(show_json chart "${pub}")" = "$(show_json chart "${ours}")" ] || die "Chart.yaml differs: ${replace}"
  [ "$(show_json values "${pub}")" = "$(show_json values "${ours}")" ] || die "values.yaml differs: ${replace}"

  # Every other file, both ways, byte for byte.
  while IFS= read -r file; do
    [ "${file}" = "Chart.yaml" ] || [ "${file}" = "values.yaml" ] && continue
    cmp -s "${pub}/${file}" "${ours}/${file}" || die "${file} differs: ${replace}"
  done < <(cd "${pub}" && find . -type f | sed 's|^\./||' | sort)
  while IFS= read -r file; do
    [ -f "${pub}/${file}" ] || die "${file} is missing from the published chart: ${replace}"
  done < <(cd "${ours}" && find . -type f | sed 's|^\./||' | sort)
}

case "${1:-}" in
  package) shift; package "$@" ;;
  verify-stamp) shift; verify_stamp "$@" ;;
  decide) shift; decide "$@" ;;
  same-chart) shift; same_chart "$@" ;;
  *) die "usage: darling-helm-release.sh package ... | verify-stamp ... | decide ... | same-chart ... (see the header of this script)" ;;
esac
