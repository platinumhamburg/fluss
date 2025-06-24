#!/bin/bash
set -ex
# Fonebox script to manage Fluss local cluster

# Function to find FLUSS_ROOT
function find_fluss_root() {
  local dir="$PWD"
  while [ "$dir" != "/" ]; do
    if [ -f "$dir/pom.xml" ] && grep -q '<modules>' "$dir/pom.xml" && grep -q '<name>Fluss :</name>' "$dir/pom.xml" && [ -f "$dir/mvnw" ]; then
      echo "$dir"
      return
    fi
    dir="$(dirname "$dir")"
  done
  echo ""
}

# Set default values
FLUSS_ROOT=$(find_fluss_root)
FONEBOX_HOME=${FONEBOX_HOME:-${FLUSS_ROOT}/.fluss}
FONEBOX_CONF=$FONEBOX_HOME/fonebox.conf
FLINK_VERSION=${FLINK_VERSION:-1.20.0}
LOCAL_MODE=false
DELETE_DATA=false
FLUSS_VERSION=0.7.0
FONEBOX_REMOTE_DATA_DIR=$FONEBOX_HOME/remote-data
FONEBOX_LAKEHOUSE_DIR=$FONEBOX_HOME/lakehouse
FONEBOX_DATA_DIR=$FONEBOX_HOME/data
FONEBOX_LOG_DIR=$FONEBOX_HOME/logs
FONEBOX_ASSETS_DIR=$FONEBOX_HOME/assets

# Initialize CONTAINER_RUNTIME and CONTAINER_COMPOSE_CMD from fonebox.conf if it exists
if [ -f "$FONEBOX_CONF" ]; then
  source "$FONEBOX_CONF"
  CONTAINER_RUNTIME=${CONTAINER_RUNTIME:-docker}
  CONTAINER_CMD=${CONTAINER_CMD:-$CONTAINER_RUNTIME}
  CONTAINER_COMPOSE_CMD=${CONTAINER_COMPOSE_CMD:-docker-compose}
else
  CONTAINER_RUNTIME=docker
  CONTAINER_CMD=docker
  CONTAINER_COMPOSE_CMD=docker-compose
fi

# Function to display help message
function show_help() {
  echo "Usage: fonebox subcommand [options]"
  echo "Subcommands:"
  echo "  install     Install Fluss and Flink"
  echo "  reinstall   Reinstall Fluss and Flink"
  echo "  uninstall   Uninstall Fluss and Flink"
  echo "  start       Start a Fluss cluster"
  echo "  stop        Stop a Fluss cluster"
  echo "  restart     Restart a Fluss cluster"
  echo "  status      Show the status of a Fluss cluster"
  echo "  sql         Open a Flink SQL client"
  echo "  help        Show this help message and exit"
}

# Function to perform local installation
function local_install() {
  echo "Performing local installation..."
  
  # Build Fluss from local code
  echo "Building Fluss from local code..."
  cd $FLUSS_ROOT
  
  # Build Fluss from local code
#  if [ "${CLEAN_BUILD:-false}" = "true" ]; then
#    echo "Executing clean package build..."
#    ./mvnw clean package -DskipTests
#  else
#    echo "Executing package build..."
#    ./mvnw package -DskipTests
#  fi

  cat <<EOF > $FONEBOX_HOME/assets/fonebox-fluss.Dockerfile
FROM eclipse-temurin:17-jre-jammy AS builder

# Install dependencies
RUN set -ex; \
  apt-get update; \
  apt-get -y install gpg libsnappy1v5 gettext-base libjemalloc-dev; \
  rm -rf /var/lib/apt/lists/*

 # Prepare environment
ENV FLUSS_HOME=/opt/fluss
ENV PATH=\$FLUSS_HOME/bin:\$PATH
RUN groupadd --system --gid=9999 fluss && \
     useradd --system --home-dir \$FLUSS_HOME --uid=9999 --gid=fluss fluss
WORKDIR \$FLUSS_HOME

# Please copy build-target to the docker dictory first, then copy to the image.
COPY --chown=fluss:fluss fluss-dist/target/fluss-0.7-SNAPSHOT-bin/fluss-0.7-SNAPSHOT /opt/fluss/

RUN ["chown", "-R", "fluss:fluss", "."]
COPY docker/docker-entrypoint.sh /
RUN ["chmod", "+x", "/docker-entrypoint.sh"]
ENTRYPOINT ["/docker-entrypoint.sh"]
CMD ["help"]

EOF

  cat <<EOF > $FONEBOX_HOME/assets/fonebox-flink.Dockerfile
FROM fluss/quickstart-flink:1.20-0.7.0

RUN rm -rf /opt/flink/lib/fluss-flink-1.20-0.7.0.jar
COPY --chown=flink:flink fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-0.7-SNAPSHOT.jar /opt/flink/lib
RUN rm -rf /opt/sql-client/lib/fluss-flink-1.20-0.7.0.jar
COPY --chown=flink:flink fluss-flink/fluss-flink-1.20/target/fluss-flink-1.20-0.7-SNAPSHOT.jar /opt/sql-client/lib

RUN ["chmod", "+x", "/opt/sql-client/sql-client"]
EOF
  
  $CONTAINER_CMD build -f $FONEBOX_HOME/assets/fonebox-fluss.Dockerfile -t fluss/fluss:local .

  $CONTAINER_CMD build -f $FONEBOX_HOME/assets/fonebox-flink.Dockerfile -t fluss/flink:local .

  echo "Local installation complete."
}

# Function to install Fluss and Flink
function install() {
  if [ -f "$FONEBOX_CONF" ]; then
    echo "Error: Fonebox is already installed. Use 'reinstall' to reinstall."
    exit 1
  fi

  # Create necessary directories
  mkdir -p $FONEBOX_ASSETS_DIR/observability

  # Extract fluss-quickstart-observability.zip to $FONEBOX_ASSETS_DIR/observability
  tmpdir=$(mktemp -d)
  unzip -o $FLUSS_ROOT/website/docs/assets/fluss-quickstart-observability.zip -d $tmpdir
  rm -rf $FONEBOX_ASSETS_DIR/observability
  mv $tmpdir/fluss-quickstart-observability $FONEBOX_ASSETS_DIR/observability

  # Define Flink image based on version
  if [ "$FLINK_VERSION" = "1.20.0" ]; then
    FLINK_IMAGE="fluss/quickstart-flink:1.20-0.7.0"
  else
    echo "Error: Unsupported Flink version '$FLINK_VERSION'. Only version 1.20.0 is currently supported."
    exit 1
  fi

  # Perform local installation if LOCAL_MODE is true
  if [ "$LOCAL_MODE" = true ]; then
    local_install
    FLUSS_VERSION="local"
    FLINK_IMAGE="fluss/flink:local"
  else
    # Download released Fluss and Flink binaries
    echo "Downloading released Fluss and Flink binaries..."
    # Add download commands here
  fi

  # Create configuration files
  echo "Creating configuration files..."
  
  # Generate $CONTAINER_CMD-compose.yaml
  echo "Generating docker-compose.yaml..."
  cat <<EOF > $FONEBOX_HOME/docker-compose.yaml
services:
  jobmanager:
    image: $FLINK_IMAGE
    ports:
      - "8083:8081"
    command: jobmanager
    container_name: fonebox-jobmanager
    environment:
      - |
        FLINK_PROPERTIES=jobmanager.rpc.address: jobmanager
        metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prom.port: 9250
    volumes:
      - $FONEBOX_LAKEHOUSE_DIR/paimon:/tmp/paimon
      - $FONEBOX_LOG_DIR/jobmanager:/opt/flink/log
  taskmanager:
    image: $FLINK_IMAGE
    depends_on:
      - jobmanager
    command: taskmanager
    container_name: fonebox-taskmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 10
        taskmanager.memory.process.size: 2048m
        taskmanager.memory.framework.off-heap.size: 256m
        metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory
        metrics.reporter.prom.port: 9250
    volumes:
      - $FONEBOX_LAKEHOUSE_DIR/paimon:/tmp/paimon
      - $FONEBOX_LOG_DIR/taskmanager:/opt/flink/log

  zookeeper:
    restart: always
    image: zookeeper:3.9.2
    container_name: fonebox-zookeeper
    environment:
      - |
        ZOO_LOG4J_PROP=log4j.rootLogger=INFO,CONSOLE,FILE
    volumes:
      - $FONEBOX_DATA_DIR/zookeeper/data:/data
      - $FONEBOX_DATA_DIR/zookeeper/datalog:/datalog
      - $FONEBOX_LOG_DIR/zookeeper:/logs

  coordinator-server:
    image: fluss/fluss:$FLUSS_VERSION
    command: coordinatorServer
    container_name: fonebox-fluss-coordinator-server
    depends_on:
      - zookeeper
    environment:
      - |
        FLUSS_PROPERTIES=zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://coordinator-server:9123
        remote.data.dir: $FONEBOX_REMOTE_DATA_DIR
        datalake.format: paimon
        datalake.paimon.metastore: filesystem
        datalake.paimon.warehouse: /tmp/paimon
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
        logback.configurationFile: logback-loki-console.xml
      - APP_NAME=coordinator-server
    volumes:
      - $FONEBOX_LAKEHOUSE_DIR/paimon:/tmp/paimon
      - $FONEBOX_DATA_DIR/coordinator-server:/tmp/fluss/data
      - $FONEBOX_REMOTE_DATA_DIR:/tmp/fluss/remote-data
      - $FONEBOX_LOG_DIR/coordinator-server:/opt/fluss/log

  tablet-server-0:
    image: fluss/fluss:$FLUSS_VERSION
    command: tabletServer
    container_name: fonebox-fluss-tablet-server-0
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server-0:9123
        tablet-server.id: 0
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
        logback.configurationFile: logback-loki-console.xml
      - APP_NAME=tablet-server
    volumes:
      - $FONEBOX_LAKEHOUSE_DIR/paimon:/tmp/paimon
      - $FONEBOX_DATA_DIR/tablet-server-0:/tmp/fluss/data
      - $FONEBOX_REMOTE_DATA_DIR:/tmp/fluss/remote-data
      - $FONEBOX_LOG_DIR/tablet-server-0:/opt/fluss/log

  tablet-server-1:
    image: fluss/fluss:$FLUSS_VERSION
    command: tabletServer
    container_name: fonebox-fluss-tablet-server-1
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server-1:9123
        tablet-server.id: 1
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
        logback.configurationFile: logback-loki-console.xml
      - APP_NAME=tablet-server
    volumes:
      - $FONEBOX_LAKEHOUSE_DIR/paimon:/tmp/paimon
      - $FONEBOX_DATA_DIR/tablet-server-1:/tmp/fluss/data
      - $FONEBOX_REMOTE_DATA_DIR:/tmp/fluss/remote-data
      - $FONEBOX_LOG_DIR/tablet-server-1:/opt/fluss/log

  tablet-server-2:
    image: fluss/fluss:$FLUSS_VERSION
    command: tabletServer
    container_name: fonebox-fluss-tablet-server-2
    depends_on:
      - coordinator-server
    environment:
      - |
        FLUSS_PROPERTIES=
        zookeeper.address: zookeeper:2181
        bind.listeners: FLUSS://tablet-server-2:9123
        tablet-server.id: 2
        kv.snapshot.interval: 0s
        data.dir: /tmp/fluss/data
        remote.data.dir: /tmp/fluss/remote-data
        metrics.reporters: prometheus
        metrics.reporter.prometheus.port: 9250
        logback.configurationFile: logback-loki-console.xml
      - APP_NAME=tablet-server
    volumes:
      - $FONEBOX_LAKEHOUSE_DIR/paimon:/tmp/paimon
      - $FONEBOX_DATA_DIR/tablet-server-2:/tmp/fluss/data
      - $FONEBOX_REMOTE_DATA_DIR:/tmp/fluss/remote-data
      - $FONEBOX_LOG_DIR/tablet-server-2:/opt/fluss/log

  prometheus:
    image: bitnami/prometheus:2.55.1-debian-12-r0
    container_name: fonebox-prometheus
    ports:
      - "9092:9090"
    volumes:
      - $FONEBOX_ASSETS_DIR/observability/fluss-quickstart-observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro
  loki:
    image: grafana/loki:3.3.2
    container_name: fonebox-loki
    ports:
      - "3102:3100"
  grafana:
    image:
      grafana/grafana:11.4.0
    container_name: fonebox-grafana
    ports:
      - "3002:3000"
    depends_on:
      - prometheus
      - loki
    volumes:
      - $FONEBOX_ASSETS_DIR/observability/fluss-quickstart-observability/grafana:/etc/grafana:ro

EOF

  # Save installation parameters to fonebox.conf
  echo "Saving installation parameters to $FONEBOX_CONF..."
  echo "FLINK_IMAGE=$FLINK_IMAGE" > $FONEBOX_CONF
  echo "LOCAL_MODE=$LOCAL_MODE" >> $FONEBOX_CONF
  echo "FLUSS_VERSION=$FLUSS_VERSION" >> $FONEBOX_CONF
  echo "CONTAINER_RUNTIME=$CONTAINER_RUNTIME" >> $FONEBOX_CONF
  echo "CONTAINER_CMD=$CONTAINER_CMD" >> $FONEBOX_CONF
  echo "CONTAINER_COMPOSE_CMD=$CONTAINER_COMPOSE_CMD" >> $FONEBOX_CONF

  echo "Installation complete."
}

# Function to reinstall Fluss and Flink
function reinstall() {
  # Delete existing data if requested
  if [ "$DELETE_DATA" = true ]; then
    echo "Deleting existing data..."
    rm -rf $FONEBOX_HOME/{data,logs}
  fi

  # Uninstall existing installation
  if [ -f "$FONEBOX_CONF" ]; then
    uninstall
  fi

  # Install again
  install
}

# Function to uninstall Fluss and Flink
function uninstall() {
  if [ ! -f "$FONEBOX_CONF" ]; then
    echo "Error: Fonebox is not installed."
    exit 1
  fi
  if [ -f "$FONEBOX_HOME"/docker-compose.yaml ]; then
    stop
  fi
  echo "Uninstalling Fonebox..."
  rm -rf $FONEBOX_HOME
  echo "Uninstallation complete."
}

# Function to start Fluss cluster
function start() {
  if [ ! -f "$FONEBOX_CONF" ]; then
    echo "Error: Fonebox is not installed. Use 'install' to install."
    exit 1
  fi

  # Check if $CONTAINER_CMD-compose.yaml exists
  if [ ! -f "$FONEBOX_HOME/docker-compose.yaml" ]; then
    echo "Error: docker-compose.yaml not found. Please run 'install' first."
    exit 1
  fi

  # Delete existing data if requested
  if [ "$DELETE_DATA" = true ]; then
    echo "Deleting existing data..."
    $CONTAINER_CMD rm -f $FONEBOX_HOME/data
  fi

  # Start containers using $CONTAINER_CMD-compose
  echo "Starting Fluss cluster..."
  COMPOSE_PROJECT_NAME=fonebox $CONTAINER_COMPOSE_CMD -f $FONEBOX_HOME/docker-compose.yaml up -d
  
  # Check if containers are running
  status
}

# Function to stop Fluss cluster
function stop() {
  if [ ! -f "$FONEBOX_CONF" ]; then
    echo "Error: Fonebox is not installed. Use 'install' to install."
    exit 1
  fi

  echo "Stopping Fluss cluster..."
  
  COMPOSE_PROJECT_NAME=fonebox $CONTAINER_COMPOSE_CMD -f $FONEBOX_HOME/docker-compose.yaml down
  
  # Optionally delete data directory if requested
  if [ "$DELETE_DATA" = true ]; then
    echo "Deleting data directory..."
    $CONTAINER_CMD rm -rf $FONEBOX_HOME/data
  fi

  echo "Fluss cluster stopped."
}

# Function to restart Fluss cluster
function restart() {
  stop
  start
}

# Function to show status of Fluss cluster
function status() {
  if [ ! -f "$FONEBOX_CONF" ]; then
    echo "Error: Fonebox is not installed. Use 'install' to install."
    exit 1
  fi

  echo "Showing status of Fluss cluster..."

  # Define color variables
  GREEN='\033[0;32m'
  RED='\033[0;31m'
  NC='\033[0m' # No Color

  # Print all fonebox containers' status, skipping the first line
  echo "Container Status:"
  $CONTAINER_CMD ps --filter "name=fonebox" --format "table {{.Names}}\t{{.Status}}" | awk 'NR>1 {print}' | while read -r line; do
    if [[ $line == *"Up"* ]]; then
      echo -e "$line\t${GREEN}OK${NC}"
    else
      echo -e "$line\t${RED}ERROR${NC}"
    fi
  done

  # Check if all containers are running
  local all_running=true
  local container_status
  container_status=$($CONTAINER_CMD ps --filter "name=fonebox" --format '{{.Status}}')

  # Iterate through each container status
  while IFS= read -r status; do
    if [[ ! $status =~ "Up" ]]; then
      all_running=false
      break
    fi
  done <<< "$container_status"

  echo ""
  # Output cluster status summary
  if [ "$all_running" = true ]; then
    echo "Cluster Status: All containers are running."
  else
    echo "Cluster Status: Some containers are not running. Please check the logs for details."
  fi

  # Display Cluster Info
  echo ""
  echo "Cluster Info:"
  echo "'bootstrap.servers' = 'coordinator-server:9123'"

  # Display service links
  echo ""
  echo "Service Links:"
  echo "Flink WebUI: http://localhost:8083/"
  echo "Grafana Dashboard: http://localhost:3002/dashboards"
  echo "Loki Explorer: http://localhost:3002/a/grafana-lokiexplore-app/"
  echo "Prometheus WebUI: http://localhost:9092/"
  echo ""

}

# Function to open Flink SQL client
function sql() {
  if [ ! -f "$FONEBOX_CONF" ]; then
    echo "Error: Fonebox is not installed. Use 'install' to install."
    exit 1
  fi

  # Set default catalog name
  CATALOG_NAME=${CATALOG_NAME:-fluss_catalog}

  echo "Opening Flink SQL client with catalog: $CATALOG_NAME..."
  
  # Start Flink SQL Client with specified catalog using $CONTAINER_CMD-compose exec
  COMPOSE_PROJECT_NAME=fonebox $CONTAINER_COMPOSE_CMD -f $FONEBOX_HOME/docker-compose.yaml exec jobmanager /opt/flink/bin/sql-client.sh
}

# Parse command line arguments
case $1 in
  install|reinstall)
    shift
    while getopts "f:lh:r:" opt; do
      case $opt in
        f) FLINK_VERSION=$OPTARG ;;
        l) LOCAL_MODE=true ;;
        r) 
          CONTAINER_RUNTIME=$OPTARG
          if [ "$CONTAINER_RUNTIME" = "docker" ]; then
            CONTAINER_CMD="docker"
            CONTAINER_COMPOSE_CMD="docker-compose"
          elif [ "$CONTAINER_RUNTIME" = "podman" ]; then
            CONTAINER_CMD="podman"
            CONTAINER_COMPOSE_CMD="docker-compose"
          else
            echo "Invalid container runtime: $CONTAINER_RUNTIME. Supported values are '$CONTAINER_CMD' and 'podman'."
            exit 1
          fi
          ;;
        h) show_help; exit 0 ;;
        \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
      esac
    done
    if [ "$1" = "install" ]; then
      install
    else
      reinstall
    fi
    ;;
  uninstall)
    uninstall
    ;;
  start|stop|restart|status|sql)
    while getopts "Dh" opt; do
      case $opt in
        D) DELETE_DATA=true ;;
        h) show_help; exit 0 ;;
        \?) echo "Invalid option: -$OPTARG" >&2; exit 1 ;;
      esac
    done
    $1
    ;;
  help)
    show_help
    ;;
  *)
    echo "Invalid subcommand: $1"
    show_help
    exit 1
    ;;
esac