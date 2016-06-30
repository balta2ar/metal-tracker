metal-tracker
=============

# About

This is a set of scripts to automatically download and seed new entries from
metal-tracker.com site.

# Running

Run transmission daemon as follows:
transmission-daemon --foreground --config-dir ~/.config/transmission-metal-tracker

Check current status:
transmission-remote 9092 -st

Show daemon configuration:
transmission-remote 9092 -si

List current torrents:
transmission-remote 9092 -l
