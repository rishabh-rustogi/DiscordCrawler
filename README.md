# DiscordCrawler
Dowload messages from Discord and uploads to GCP cloud storage. User only need to deal with the discord application and provide their token.

# To Run
Comment out any GCP code to run the code locally (or use discordMessageExtractor_NO_GCP.py)

### Modes (--mode)
- update : updates the config files
- extractOld: extract messages from the previous seen message
- extractNew: extract messages from the very start
- extractAll: run all update, extractOld, and extractNew together

# To Do
- Transfer config files to firestore (Two config files)
-- User info like name, password and token
-- Server, channel and other info (DO NOT EDIT FILE)
- Read from config file to run extractOld and extractNew
