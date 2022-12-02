#! /bin/bash
  apt update
  cd
  python3 discordMessageExtractor_NO_GCP.py --mode extractAll --size 9000000
  poweroff