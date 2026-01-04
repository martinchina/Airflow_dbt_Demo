
#!/usr/bin/env bash
## 
#  Description: Create local MacOS env for Airflow & dbt Demo Project
#  Author: Martin Zang
#  Date: 2025-12-29
#  Version: 1.0.0
#  Usage: ./create_env.sh
#  Example: ./create_env.sh
#  Note: To be added..
## 

set -Eeuo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_ROOT/.venv"
AIRFLOW_HOME="${AIRFLOW_HOME:-$PROJECT_ROOT/airflow}"
AIRFLOW_PORT="${AIRFLOW_PORT:-8080}"
AIRFLOW_VERSION="${AIRFLOW_VERSION:-2.9.2}"
DBT_CORE_VERSION="${DBT_CORE_VERSION:-1.8.6}"
DBT_DUCKDB_VERSION="${DBT_DUCKDB_VERSION:-1.8.2}"
DBT_PROJECT_DIR="$PROJECT_ROOT/dbt_demo"
DBT_PROFILES_DIR="$DBT_PROJECT_DIR/profiles"
PID_FILE="$PROJECT_ROOT/.airflow_pids"

START_SERVICES="false"
STOP_SERVICES="false"
FORCE_OVERWRITE="false"

log() { printf "\033[1;32m[OK]\033[0m %s\n" "$*"; }
info() { printf "\033[1;34m[INFO]\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[WARN]\033[0m %s\n" "$*"; }
err() { printf "\033[1;31m[ERR]\033[0m %s\n" "$*" >&2; }
die() { err "$*"; exit 1; }

command_exists() { command -v "$1" >/dev/null 2>&1; }

ensure_macos() {
if [ "$(uname -s)" != "Darwin" ]; then
die "本脚本仅适用于 MacOS。检测到: $(uname -s)"
fi
log "MacOS 检测通过"
}

ensure_homebrew() {
if command_exists brew; then
log "Homebrew 已存在"
else
warn "未检测到 Homebrew，将尝试自动安装（可能需要输入密码）"
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)" || die "Homebrew 安装失败，请手动安装后重试： https://brew.sh"
if [ -d /opt/homebrew/bin ]; then
eval "$(/opt/homebrew/bin/brew shellenv)"
elif [ -d /usr/local/bin ]; then
eval "$(/usr/local/bin/brew shellenv)"
fi
command_exists brew || die "未能正确安装 Homebrew，请手动安装后再运行脚本"
log "Homebrew 安装完成"
fi
}

ensure_python() {
if command_exists python3; then
PY_VER="$(python3 -V 2>&1 | awk '{print $2}')"
info "检测到 Python3: $PY_VER"
else
warn "未检测到 python3，将使用 Homebrew 安装 python@3.11"
brew install python@3.11 || die "安装 python@3.11 失败"
fi
}

create_venv() {
if [ -d "$VENV_DIR" ]; then
info "Python 虚拟环境已存在：$VENV_DIR"
else
python3 -m venv "$VENV_DIR" || die "创建虚拟环境失败"
log "创建虚拟环境成功：$VENV_DIR"
fi
. "$VENV_DIR/bin/activate"
python -m pip install --upgrade pip wheel setuptools
log "升级 pip/setuptools/wheel 完成"
}

install_airflow_dbt() {
PY_MINOR="$(python -c 'import sys;print(f"{sys.version_info[0]}.{sys.version_info[1]}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PY_MINOR}.txt"
info "安装 Airflow ${AIRFLOW_VERSION}（constraints: ${CONSTRAINT_URL}）"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"
log "Airflow 安装完成"
info "安装 dbt-core==${DBT_CORE_VERSION} 与 dbt-duckdb==${DBT_DUCKDB_VERSION}"
pip install "dbt-core==${DBT_CORE_VERSION}" "dbt-duckdb==${DBT_DUCKDB_VERSION}"
log "dbt 相关依赖安装完成"
}

init_airflow() {
export AIRFLOW_HOME="$AIRFLOW_HOME"
mkdir -p "$AIRFLOW_HOME/dags" "$AIRFLOW_HOME/logs" "$AIRFLOW_HOME/plugins"
info "初始化 Airflow 元数据库: AIRFLOW_HOME=${AIRFLOW_HOME}"
"$VENV_DIR/bin/airflow" db init
if ! "$VENV_DIR/bin/airflow" users list 2>/dev/null | grep -qE '[[:space:]]admin[[:space:]]'; then
info "创建 Airflow 管理员账户 admin/admin"
"$VENV_DIR/bin/airflow" users create --role Admin --username admin --password admin --firstname Admin --lastname User --email admin@example.com
else
info "已存在 Airflow 用户，无需创建"
fi
log "Airflow 初始化完成"
}

scaffold_dbt_demo() {
if [ -d "$DBT_PROJECT_DIR" ] && [ "$FORCE_OVERWRITE" != "true" ]; then
info "检测到已有 dbt_demo 工程，跳过生成（如需覆盖，请加 --force）"
else
mkdir -p "$DBT_PROJECT_DIR/models/example" "$DBT_PROFILES_DIR"
cat > "$DBT_PROJECT_DIR/dbt_project.yml" <<EOF
name: 'dbt_demo'
version: '1.0'
config-version: 2
profile: 'dbt_demo'
model-paths: ['models']
models:
  dbt_demo:
    +materialized: view
EOF
cat > "$DBT_PROJECT_DIR/models/example/hello.sql" <<'EOF'
select 1 as id, 'hello_dbt' as message
EOF
cat > "$DBT_PROJECT_DIR/models/example/schema.yml" <<'EOF'
version: 2
models:
  - name: hello
    description: "Simple hello model"
    columns:
      - name: id
        tests: [not_null, unique]
EOF
cat > "$DBT_PROFILES_DIR/profiles.yml" <<EOF
dbt_demo:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: $DBT_PROJECT_DIR/dbt_demo.duckdb
      threads: 4
EOF
log "dbt_demo 工程与 DuckDB 配置已生成：$DBT_PROJECT_DIR"
fi
}

generate_airflow_dag() {
local dag_path="$AIRFLOW_HOME/dags/dbt_duckdb_demo.py"
local dbt_bin="$VENV_DIR/bin/dbt"
cat > "$dag_path" <<EOF
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

DBT_BIN = "$dbt_bin"
DBT_PROJECT_DIR = "$DBT_PROJECT_DIR"
DBT_PROFILES_DIR = "$DBT_PROFILES_DIR"

default_args = {
    "owner": "airflow",
}

with DAG(
    dag_id="dbt_duckdb_demo",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f'cd "{DBT_PROJECT_DIR}" && "{DBT_BIN}" debug --profiles-dir "{DBT_PROFILES_DIR}"',
    )
    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f'cd "{DBT_PROJECT_DIR}" && "{DBT_BIN}" build --profiles-dir "{DBT_PROFILES_DIR}"',
    )
    dbt_debug >> dbt_build
EOF
log "示例 DAG 已生成：$dag_path"
}

write_env_file() {
local env_file="$PROJECT_ROOT/.env"
cat > "$env_file" <<EOF
export AIRFLOW_HOME="$AIRFLOW_HOME"
export DBT_PROFILES_DIR="$DBT_PROFILES_DIR"
export PATH="$VENV_DIR/bin:\$PATH"
EOF
log "写入环境变量文件：$env_file"
}

start_services() {
export AIRFLOW_HOME="$AIRFLOW_HOME"
mkdir -p "$AIRFLOW_HOME/logs/services"
nohup "$VENV_DIR/bin/airflow" webserver --port "$AIRFLOW_PORT" > "$AIRFLOW_HOME/logs/services/webserver.out" 2>&1 &
WS_PID=$!
sleep 2
nohup "$VENV_DIR/bin/airflow" scheduler > "$AIRFLOW_HOME/logs/services/scheduler.out" 2>&1 &
SCH_PID=$!
printf "%s\n%s\n" "$WS_PID" "$SCH_PID" > "$PID_FILE"
log "Airflow 已启动：webserver(pid=$WS_PID), scheduler(pid=$SCH_PID)"
info "控制台: http://localhost:$AIRFLOW_PORT  用户名: admin  密码: admin"
}

stop_services() {
if [ -f "$PID_FILE" ]; then
mapfile -t pids < "$PID_FILE" || true
for p in "${pids[@]}"; do
if ps -p "$p" >/dev/null 2>&1; then
kill "$p" || true
fi
done
rm -f "$PID_FILE"
log "Airflow 服务已停止"
else
warn "未发现运行中的 Airflow（PID 文件不存在：$PID_FILE）"
fi
}

print_usage() {
cat <<'EOF'
用法:
  ./create_env.sh [--start] [--stop] [--force]

说明:
  直接执行不带参数：创建 Python 虚拟环境，安装 Airflow 与 dbt-duckdb，
  初始化 Airflow，脚手架 dbt_demo 工程，并生成可运行的 DAG。

可选参数:
  --start   启动 Airflow webserver(默认端口 8080) 与 scheduler（后台运行）
  --stop    停止通过本脚本启动的 webserver 与 scheduler
  --force   覆盖已存在的 dbt_demo 脚手架文件

完成后:
  1) source ./.env
  2) 手动启动（可选）:
     $AIRFLOW_HOME 环境下：
       airflow webserver --port 8080
       airflow scheduler
  3) 浏览器打开 http://localhost:8080  使用 admin/admin 登录
  4) 在 UI 中触发 DAG: dbt_duckdb_demo
EOF
}

parse_args() {
while [ $# -gt 0 ]; do
case "$1" in
--start) START_SERVICES="true" ;;
--stop) STOP_SERVICES="true" ;;
--force) FORCE_OVERWRITE="true" ;;
-h|--help) print_usage; exit 0 ;;
*) err "未知参数: $1"; print_usage; exit 1 ;;
esac
shift
done
}

main() {
parse_args "$@"
ensure_macos
ensure_homebrew
ensure_python
create_venv
install_airflow_dbt
init_airflow
scaffold_dbt_demo
generate_airflow_dag
write_env_file
if [ "$STOP_SERVICES" = "true" ]; then
stop_services
fi
if [ "$START_SERVICES" = "true" ]; then
start_services
fi
log "全部完成！下一步建议："
printf "1) 运行: source \"%s/.env\"\n" "$PROJECT_ROOT"
printf "2) 启动: \"%s/bin/airflow\" webserver --port %s & \"%s/bin/airflow\" scheduler &\n" "$VENV_DIR" "$AIRFLOW_PORT" "$VENV_DIR"
printf "3) 打开: http://localhost:%s 并用 admin/admin 登录；触发 DAG: dbt_duckdb_demo\n" "$AIRFLOW_PORT"
}

main "$@"

