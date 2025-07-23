#!/bin/bash
#
# Copyright (c) 2024 Alibaba Group Holding Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CONF_FILE="${FLUSS_HOME}/conf/server.yaml"

prepare_configuration() {
    if [ -n "${FLUSS_PROPERTIES}" ]; then
        echo "${FLUSS_PROPERTIES}" >> "${CONF_FILE}"
    fi
    envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"
}

prepare_configuration

args=("$@")

if [ "$1" = "help" ]; then
  printf "Usage: $(basename "$0") (coordinatorServer|tabletServer)\n"
  printf "    Or $(basename "$0") help\n\n"
  exit 0
elif [ "$1" = "coordinatorServer" ]; then
  args=("${args[@]:1}")
  echo "Starting Coordinator Server"
  exec "$FLUSS_HOME/bin/coordinator-server.sh" start-foreground "${args[@]}"
elif [ "$1" = "tabletServer" ]; then
  args=("${args[@]:1}")
  echo "Starting Tablet Server"
  exec "$FLUSS_HOME/bin/tablet-server.sh" start-foreground "${args[@]}"
fi

args=("${args[@]}")

## Running command in pass-through mode
exec "${args[@]}"