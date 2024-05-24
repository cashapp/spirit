#!/bin/bash
#
# source this file to get the following functions:
# - setup: install required packages
# - activate <version>: download and extract the specified version of MySQL
# - initialize_mysql <name>: initialize a MySQL data directory
# - start_mysql <name> <port> <server_id>: start a MySQL server
# - check_mysql <name>: check if a MySQL server is running
# - deploy_replication <num_nodes> <base_port>: deploy a replication setup
# - exec_mysql_sock <name> <query>: execute a query on a MySQL server

setup() {
	local cmd=()
	if (( UID != 0 ))
	then
		cmd=(sudo)
	fi
	cmd+=(apt)

	"${cmd[@]}" update

	packages=(
		ca-certificates
		libaio1
		libnuma1
		libncurses5
		libncurses6
		netcat
		wget
		sudo
		xz-utils
		)

	"${cmd[@]}" install -y  "${packages[@]}"
}

mysql_cmd=(
	./bin/mysqld --no-defaults
	--socket="mysql.sock"
	--log-error="mysql.err"
	--pid-file="mysql.pid"
	--user="$(id -u -n)"
	--gtid-mode=ON
	--enforce-gtid-consistency=ON
)
if (( UID == 0 ))
then
	mysql_cmd+=(--user=root)
fi

activate() {
	if [[ $1 ]]; then
		version=$1
	else
		echo "Usage: activate <version>" >&2
		return 1
	fi

	if [[ $version = http* ]]; then
		url=$version
		filename="${url##*/}"
		dirname="${filename%.tar.xz}"
	else
		minor_version="${version%.*}"
		printf -v filename "mysql-%s-linux-glibc2.28-x86_64.tar.xz" "$version"
		printf -v url "https://dev.mysql.com/get/Downloads/MySQL-%s/%s" "$minor_version" "$filename"
		dirname="${filename%.tar.xz}"
	fi

	if [[ -d $dirname ]]; then
		pushd "$dirname"
		return
	fi
	if [[ ! -f $filename ]]; then
		wget "$url" || return
	fi
	tar -xf "$filename"
	pushd "$dirname"
}

initialize_mysql() {
	local datadir=data
	if [[ $1 ]]; then
		datadir="data-$1"
	fi
	"${mysql_cmd[@]}" --initialize-insecure --datadir="$datadir"
}

start_mysql() {
	local datadir=data
	local name
	local port=3306
	local server_id=1

	if [[ $1 ]]; then
		name=$1
		datadir="data-$1"
	fi
	if [[ $2 ]]; then
		port=$2
	fi
	if [[ $3 ]]; then
		server_id=$3
	fi

	check_port "$port" || return

	"${mysql_cmd[@]}" --datadir="$datadir" --port="$port" --server-id="$server_id" &

	pid=$!

	sleep 1
	if ! kill -0 "$pid"; then
		cat "$datadir/mysql.err"
		return 1
	fi

	check_mysql "$name"
}

check_mysql() {
	local datadir=$1
	local i
	for ((i=0;i<5;i++))
	do
		sleep 1
		exec_mysql_sock "$datadir" 'SELECT 1' > /dev/null && return
	done
	printf %s\\n "MySQL check of $datadir failed"
	return 1
}


deploy_replication() {
	num_nodes=2
	base_port=3306

	if [[ $1 ]]; then
		num_nodes=$1
	fi
	if [[ $2 ]]; then
		base_port=$2
	fi

	initialize_mysql primary
	start_mysql primary "$base_port" || return

	exec_mysql_sock primary "CREATE USER 'repl'@'%' IDENTIFIED BY 'replica'"
	exec_mysql_sock primary "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'"

	# SOURCE_AUTO_POSITION=1"

	local i
	for ((i=1;i<num_nodes;i++))
	do
		name="replica$i"
		port=$((base_port + i))
		initialize_mysql "$name" || continue
		start_mysql "$name" "$port" "$(( 100 + i ))" || continue
		exec_mysql_sock "$name" "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=$base_port, GET_SOURCE_PUBLIC_KEY=1"
		exec_mysql_sock "$name" "START REPLICA USER='repl' PASSWORD='replica'"
	done
}

exec_mysql_sock() {
	local datadir=data
	if [[ $2 ]]; then
		datadir="data-$1"
		shift
	fi

	./bin/mysql -u root -S "$datadir/mysql.sock" -e "$1"
}

check_port() {
	local port=$1
	if nc -w0 -z localhost "$port" </dev/null >/dev/null; then
		printf %s\\n "Port $port is in use" >&2
		return 1
	fi
}
