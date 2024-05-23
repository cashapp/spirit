#!/bin/bash
set -xe

version=8.4.0

# careful, this ubuntu:full-latest image is VERY large ... like 50GB!!!
# docker run -ti --platform=linux/amd64 ghcr.io/catthehacker/ubuntu:full-latest

#sudo apt install -y libncurses6 #???
#sudo apt install -y libncurses5 libaio1

pushd /tmp 

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

	minor_version="${version%.*}"

	printf -v filename "mysql-%s-linux-glibc2.28-x86_64.tar.xz" "$version"
	printf -v url "https://dev.mysql.com/get/Downloads/MySQL-%s/%s" "$minor_version" "$filename"
	dirname="${filename%.tar.xz}"

	if [[ -d $dirname ]]; then
		pushd "$dirname"
		return
	fi
	if [[ ! -f $filename ]]; then
		wget "$url"
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
	start_mysql primary "$base_port"

	exec_mysql_sock primary "CREATE USER 'repl'@'%' IDENTIFIED BY 'replica'"
	exec_mysql_sock primary "GRANT REPLICATION SLAVE ON *.* TO 'repl'@'%'"

	# SOURCE_AUTO_POSITION=1"

	local i
	for ((i=1;i<num_nodes;i++))
	do
		name="replica$i"
		port=$((base_port + i))
		initialize_mysql "$name"
		start_mysql "$name" "$port" "$(( 100 + i ))"
		exec_mysql_sock "$name" "CHANGE REPLICATION SOURCE TO SOURCE_HOST='127.0.0.1', SOURCE_PORT=$base_port, GET_SOURCE_PUBLIC_KEY=1"
		exec_mysql_sock "$name" "START REPLICA USER='repl' PASSWORD='replica'"
	done
}

exec_mysql_sock() {
	local datadir=data
	if [[ $1 ]]; then
		datadir="data-$1"
	fi
	./bin/mysql -u root -S "$datadir/mysql.sock" -e "$2"
}

setup
activate "$version"
deploy_replication
