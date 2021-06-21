#!/bin/bash

cd /yaxt/web/gis-server
pm2 restart app.json
pm2 restart app5.json