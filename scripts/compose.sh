#!/bin/sh
# Keep archive paths identical inside and outside Docker.
set -eu
project_dir=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd -P)
WEATHER_DATA_DIR="${WEATHER_DATA_DIR:-$project_dir/data}"
case "$WEATHER_DATA_DIR" in
  /*) ;;
  *) WEATHER_DATA_DIR="$project_dir/$WEATHER_DATA_DIR" ;;
esac
mkdir -p "$WEATHER_DATA_DIR"
WEATHER_DATA_DIR=$(CDPATH= cd -- "$WEATHER_DATA_DIR" && pwd -P)
export WEATHER_DATA_DIR
exec docker compose --project-directory "$project_dir" "$@"
