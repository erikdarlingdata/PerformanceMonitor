#!/bin/sh
# Writes the Darling configuration file from the chart's template, replacing placeholders with secret values.
#
#   render-config.sh <template> <output>
#
# For each pair DARLING_SUBST_<n>_PLACEHOLDER / DARLING_SUBST_<n>_VALUE (n = 0, 1, ... until a gap) it
#   1. refuses a value outside [A-Za-z0-9:/._~%?=&+#-] or one with a newline (exit 2, naming the placeholder, never the value),
#   2. refuses a placeholder that does not appear in the template,
#   3. replaces every occurrence literally (no regex, no & expansion),
#   4. refuses a placeholder that is still present afterwards.
# POSIX sh and awk only: it runs in the Darling image, which has no jq.
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: render-config.sh <template> <output>" >&2
  exit 64
fi

template="$1"
output="$2"
work="${output}.tmp"
rm -f "${work}"
cp "${template}" "${work}"
trap 'rm -f "${work}" "${work}.next"' EXIT

nl='
'
n=0
while eval "[ -n \"\${DARLING_SUBST_${n}_PLACEHOLDER+set}\" ]"; do
  eval "placeholder=\${DARLING_SUBST_${n}_PLACEHOLDER}"
  if [ -z "${placeholder}" ]; then
    echo "render-config: substitution ${n} has an empty placeholder" >&2
    exit 2
  fi
  if ! eval "[ -n \"\${DARLING_SUBST_${n}_VALUE+set}\" ]"; then
    echo "render-config: no value for placeholder ${placeholder}" >&2
    exit 2
  fi
  eval "value=\${DARLING_SUBST_${n}_VALUE}"

  case "${value}" in
    *"${nl}"*)
      echo "render-config: the value for placeholder ${placeholder} contains a newline" >&2
      exit 2
      ;;
  esac
  if ! printf '%s\n' "${value}" | grep -Eq '^[A-Za-z0-9:/._~%?=&+#-]+$'; then
    echo "render-config: the value for placeholder ${placeholder} is empty or has a character outside [A-Za-z0-9:/._~%?=&+#-]" >&2
    exit 2
  fi
  if ! grep -qF -- "${placeholder}" "${work}"; then
    echo "render-config: placeholder ${placeholder} does not appear in the template" >&2
    exit 2
  fi

  # Literal replacement: ENVIRON is read without escape processing, index/substr does no pattern matching.
  PLACEHOLDER="${placeholder}" VALUE="${value}" awk '
    BEGIN { ph = ENVIRON["PLACEHOLDER"]; val = ENVIRON["VALUE"]; len = length(ph) }
    {
      line = $0
      out = ""
      while ((i = index(line, ph)) > 0) {
        out = out substr(line, 1, i - 1) val
        line = substr(line, i + len)
      }
      print out line
    }
  ' "${work}" > "${work}.next"
  mv "${work}.next" "${work}"

  if grep -qF -- "${placeholder}" "${work}"; then
    echo "render-config: placeholder ${placeholder} is still present after substitution" >&2
    exit 2
  fi
  n=$((n + 1))
done

mv "${work}" "${output}"
