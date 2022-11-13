#! /bin/bash
  apt update
  cd /home/rishabh_rustogi/discord-crawler/
  python3 discordMessageExtractor.py --mode extractAll
  poweroff