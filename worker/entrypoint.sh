#!/bin/bash
set -e

if [ -z "$HEADLESS" ] || [ "$HEADLESS" = "false" ]; then

    if [ -n "$DEBUG_DISPLAY" ]; then
        export DISPLAY="$DEBUG_DISPLAY"
        echo "Debugging mode: forwarding display to $DISPLAY (Xvfb skipped)."
    else
        echo "Production mode: emulating virtual display for the browser using Xvfb."
        Xvfb :99 -screen 0 1280x1024x24 -nolisten tcp &
        export DISPLAY=:99
        sleep 0.5
    fi

else
    echo "Headless mode is enabled. Skipping display setup."
fi

exec "$@"
