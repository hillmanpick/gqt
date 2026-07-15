#!/bin/sh
set -eu

SOURCE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/nginx" && pwd)
CONF_DIR=${1:-/root/openclaw-stack/nginx/conf.d}
STAMP=$(date +%Y%m%d-%H%M%S)

install_include() {
    target_file=$1
    include_file=$2
    include_path=$3

    install -m 644 "$SOURCE_DIR/$include_file" "$CONF_DIR/$include_file"
    if grep -Fq "include $include_path;" "$target_file"; then
        return
    fi

    cp -a "$target_file" "$target_file.bak-gqt-$STAMP"
    temp_file=$(mktemp "$target_file.tmp.XXXXXX")
    awk -v include_path="$include_path" '
        !inserted && $0 == "    location / {" {
            print "    include " include_path ";"
            print ""
            inserted = 1
        }
        { print }
        END { if (!inserted) exit 42 }
    ' "$target_file" > "$temp_file"
    chmod --reference="$target_file" "$temp_file"
    chown --reference="$target_file" "$temp_file"
    mv "$temp_file" "$target_file"
}

install_include \
    "$CONF_DIR/app.http.conf" \
    "gqt.http.inc" \
    "/etc/nginx/conf.d/gqt.http.inc"

install_include \
    "$CONF_DIR/app.https.conf" \
    "gqt.https.inc" \
    "/etc/nginx/conf.d/gqt.https.inc"
