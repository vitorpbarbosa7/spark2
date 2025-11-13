#!/usr/bin/env bash
set -euo pipefail

TS="$(date +%Y%m%d_%H%M%S)"
OUT="${HOME}/spark_env_${TS}.md"

h1() { echo -e "\n# $1\n" >> "$OUT"; }
h2() { echo -e "\n## $1\n" >> "$OUT"; }
code() { echo -e "\n\`\`\`$1" >> "$OUT"; cat >> "$OUT"; echo -e "\`\`\`\n" >> "$OUT"; }

# Append a command's output if it exists
run() {
  local title="$1"; shift
  h2 "$title"
  {
    echo '$' "$@"
    "$@" 2>&1 || true
  } | sed $'s/\r$//' | sed 's/[[:space:]]$//' | sed 's/^\$/\$/' | sed '1i```bash' -e '$a```' >> "$OUT"
}

# Append a file's contents if it exists
filedump() {
  local label="$1" path="$2"
  if [[ -f "$path" ]]; then
    h2 "$label ($path)"
    echo '```'bash >> "$OUT"
    cat "$path" >> "$OUT"
    echo '```' >> "$OUT"
  fi
}

# Resolve real path (portable; macOS readlink -f not default)
realpath_py() {
  python3 - <<'PY' "$1"
import os, sys
print(os.path.realpath(sys.argv[1]))
PY
}

echo "# Spark Environment Snapshot" > "$OUT"
echo "_Generated on $(date)_" >> "$OUT"

# System
h2 "System"
{
  echo "Shell: $SHELL"
  echo
  echo '```bash'
  sw_vers 2>/dev/null || true
  uname -a
  echo '```'
} >> "$OUT"

# Homebrew
if command -v brew >/dev/null 2>&1; then
  run "Homebrew Version" brew --version
  run "brew info apache-spark" brew info apache-spark
  run "brew list apache-spark (files)" brew list apache-spark
fi

# Java
run "JAVA Version" java --version
h2 "JAVA_HOME"
echo -e "\n\`\`\`bash\n${JAVA_HOME-<unset>}\n\`\`\`\n" >> "$OUT"

# Spark binaries
SPARK_BIN="$(command -v spark-submit || true)"
h2 "spark-submit on PATH"
echo -e "\n\`\`\`bash\n${SPARK_BIN:-<not found>}\n\`\`\`\n" >> "$OUT"

if [[ -n "${SPARK_BIN}" ]]; then
  # Resolve symlinks
  REAL_SPARK_BIN="$(python3 - <<'PY' "$SPARK_BIN"
import os, sys; print(os.path.realpath(sys.argv[1]))
PY
)"
  h2 "Resolved spark-submit path"
  echo -e "\n\`\`\`bash\n${REAL_SPARK_BIN}\n\`\`\`\n" >> "$OUT"

  # Version (with launch command printed)
  SPARK_PRINT_LAUNCH_COMMAND=1 run "spark-submit --version" "$SPARK_BIN" --version
fi

# SPARK_HOME & related
h2 "SPARK_HOME"
echo -e "\n\`\`\`bash\n${SPARK_HOME-<unset>}\n\`\`\`\n" >> "$OUT"

if [[ -n "${SPARK_HOME-}" && -d "$SPARK_HOME" ]]; then
  RESOLVED_SHOME="$(realpath_py "$SPARK_HOME")"
  h2 "Resolved SPARK_HOME"
  echo -e "\n\`\`\`bash\n$RESOLVED_SHOME\n\`\`\`\n" >> "$OUT"
  run "ls \$SPARK_HOME/bin" ls -la "$SPARK_HOME/bin"
  run "ls \$SPARK_HOME/libexec/bin" ls -la "$SPARK_HOME/libexec/bin"
fi

# Env vars commonly used with Spark on macOS
h2 "Relevant Environment Variables"
{
  echo '```bash'
  echo "SPARK_HOME=${SPARK_HOME-<unset>}"
  echo "SPARK_LOCAL_IP=${SPARK_LOCAL_IP-<unset>}"
  echo "PYARROW_IGNORE_TIMEZONE=${PYARROW_IGNORE_TIMEZONE-<unset>}"
  echo "OBJC_DISABLE_INITIALIZE_FORK_SAFETY=${OBJC_DISABLE_INITIALIZE_FORK_SAFETY-<unset>}"
  echo "JAVA_HOME=${JAVA_HOME-<unset>}"
  echo '```'
} >> "$OUT"

# PATH entries related to spark/java/python/scala
h2 "PATH (entries containing spark/java/python/scala)"
{
  echo '```bash'
  echo "$PATH" | tr ':' '\n' | grep -Ei '(spark|java|python|scala)' || true
  echo '```'
} >> "$OUT"

# Python / PySpark
run "Python (which)" which python3
run "Python version" python3 --version
if command -v pip >/dev/null 2>&1; then
  run "pip show pyspark" pip show pyspark
  run "pip freeze | grep -E 'pyspark|pyarrow|findspark'" bash -lc "pip freeze | grep -E 'pyspark|pyarrow|findspark' || true"
fi
# Programmatic check for pyspark version & SparkSession version
python3 - <<'PY' 2>/dev/null | sed '1i```bash' -e '$a```' >> "$OUT" || true
import sys
try:
    import pyspark
    from pyspark.sql import SparkSession
    print("pyspark.__version__ =", pyspark.__version__)
    spark = SparkSession.builder.getOrCreate()
    print("SparkSession.version =", spark.version)
    spark.stop()
except Exception as e:
    print("Python check error:", e)
PY

# pyenv (if used)
if command -v pyenv >/dev/null 2>&1; then
  run "pyenv versions" pyenv versions
  run "pyenv which spark-submit" pyenv which spark-submit
fi

# Scala
run "Scala which" which scala
run "Scala version" bash -lc "scala -version 2>&1"

# Spark conf files (common locations)
CANDIDATE_CONFS=()
[[ -n "${SPARK_HOME-}" ]] && CANDIDATE_CONFS+=("$SPARK_HOME/conf")
CANDIDATE_CONFS+=("/etc/spark/conf" "/usr/local/etc/spark/conf" "/opt/homebrew/etc/spark/conf")

h2 "Spark conf directories found"
{
  echo '```bash'
  for d in "${CANDIDATE_CONFS[@]}"; do
    [[ -d "$d" ]] && echo "$d"
  done
  echo '```'
} >> "$OUT"

for d in "${CANDIDATE_CONFS[@]}"; do
  [[ -d "$d" ]] || continue
  run "ls ${d}" ls -la "$d"
  filedump "spark-defaults.conf" "$d/spark-defaults.conf"
  filedump "spark-env.sh" "$d/spark-env.sh"
  filedump "log4j2.properties" "$d/log4j2.properties"
done

# JARs overview (count + a few examples)
if [[ -n "${SPARK_HOME-}" && -d "$SPARK_HOME/jars" ]]; then
  h2 "Spark JARs"
  {
    echo '```bash'
    echo "Total jars: $(ls "$SPARK_HOME/jars" | wc -l | tr -d ' ')"
    echo
    echo "Some jars:"
    ls "$SPARK_HOME/jars" | head -n 30
    echo '```'
  } >> "$OUT"
fi

# Shell rc files (may contain tokens — review before sharing)
for f in "$HOME/.zshrc" "$HOME/.zprofile" "$HOME/.zshenv" "$HOME/.bashrc" "$HOME/.bash_profile"; do
  filedump "$(basename "$f")" "$f"
done

# Oh My Zsh specifics (if present)
if [[ -n "${ZSH-}" && -d "${ZSH}" ]]; then
  h2 "Oh My Zsh"
  {
    echo '```bash'
    echo "ZSH=${ZSH}"
    echo '```'
  } >> "$OUT"
  # Extract theme and plugins from .zshrc
  if [[ -f "$HOME/.zshrc" ]]; then
    h2 "Oh My Zsh theme/plugins (from .zshrc)"
    {
      echo '```bash'
      grep -E '^(ZSH_THEME|plugins=)' "$HOME/.zshrc" || true
      echo '```'
    } >> "$OUT"
  fi
fi

echo -e "\n---\nSaved to: $OUT\n"

