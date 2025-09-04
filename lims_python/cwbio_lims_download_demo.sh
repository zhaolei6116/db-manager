#!/bin/bash
python3 cwbio_lims_downloader.py  --startTime "$(date +"%Y-%m-%d %H:%M:%S")" --endTime "$(date -d "24 hours ago" +"%Y-%m-%d %H:%M:%S")" --lab S --path ./LimsData