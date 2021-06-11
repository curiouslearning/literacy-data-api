#!/bin/bash
memcached -u memcache -d start
node src/index.js
