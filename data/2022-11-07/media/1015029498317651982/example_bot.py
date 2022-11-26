import os
from dotenv import load_dotenv
import discord

# Read environment variables for discord.py from .enf file
load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')

# Set up discord Client
intents = discord.Intents.default()
intents.message_content = True
client = discord.Client(intents=intents)

# Event this runs when the bot is connected to Discord
@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')

    # Print the name of bot to console
    for guild in client.guilds:
        if guild.name == GUILD:
            break
    print(
        f'{client.user} is connected to the following guild:\n'
        f'{guild.name}(id: {guild.id})'
    )

    # Print guild(server) members to console
    members = '\n - '.join([member.name for member in guild.members])
    print(f'Guild Members:\n - {members}')


# Event that runs when the bot detect a new message
@client.event
async def on_message(message):
    # ignores messages from the bot itself
    if message.author == client.user:
        return

    # respond to particular message
    if message.content == 'Hi, Ryo\'s bot':
        await message.channel.send("Hi there")

    # respond if a message starts from particular word
    # if message.content.startswith('Hi'):
    #     await message.channel.send('Hi')


# Start the bot
client.run(TOKEN)