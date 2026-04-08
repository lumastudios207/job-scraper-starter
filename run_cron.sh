#!/bin/bash
cd /Users/iannikov/Documents/job-scraper-starter
.venv/bin/python runner.py >> logs/cron.log 2>&1
