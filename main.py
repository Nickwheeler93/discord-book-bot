import discord
from discord.ext import commands
import logging
from dotenv import load_dotenv
import os
import sqlite3
from database import DB_PATH

load_dotenv()
token = os.getenv('DISCORD_TOKEN')

handler = logging.FileHandler(filename="discord.log" , encoding='utf-8' , mode='w')
intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix='!', intents=intents)

def ensure_profile(user):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    INSERT OR IGNORE INTO user_profiles (user_id, username)
    VALUES (?, ?)
    """, (user.id, user.name))

    cur.execute("""
    UPDATE user_profiles
    SET username = ?, last_active = CURRENT_TIMESTAMP
    WHERE user_id = ?
    """, (user.name, user.id))

    conn.commit()
    conn.close()



#bot events

@bot.event
async def on_ready():
    print("we are ready to go!")

@bot.event
async def on_member_join(member):
    channel_id = 1457524496406806675  # replace with your channel ID
    channel = bot.get_channel(channel_id)

    if channel:
        await channel.send(
            f"Welcome to the Book Club, {member.mention}! 👋"
        )





#bot commands


@bot.command()
async def startbook(ctx, *, title: str):
    # Make sure the user has a profile row
    ensure_profile(ctx.author)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Write to reading_progress (one active book per user)
    cur.execute("""
        INSERT OR REPLACE INTO reading_progress (user_id, book_title, total_pages, current_page)
        VALUES (?, ?, NULL, 0)
    """, (ctx.author.id, title))

    # Update profile summary fields
    cur.execute("""
        UPDATE user_profiles
        SET current_book = ?, current_page = 0, last_active = CURRENT_TIMESTAMP
        WHERE user_id = ?
    """, (title, ctx.author.id))

    conn.commit()
    conn.close()

    await ctx.send(f"📖 Started: **{title}**")

@bot.command()
async def progress(ctx, page: int):
    # Ensure profile exists / update username + last_active
    ensure_profile(ctx.author)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Update current book progress
    cur.execute("""
        UPDATE reading_progress
        SET current_page = ?
        WHERE user_id = ?
    """, (page, ctx.author.id))

    if cur.rowcount == 0:
        await ctx.send("You don’t have an active book yet. Use `!startbook <title>` first.")
        conn.close()
        return

    # Mirror progress into profile
    cur.execute("""
        UPDATE user_profiles
        SET current_page = ?, last_active = CURRENT_TIMESTAMP
        WHERE user_id = ?
    """, (page, ctx.author.id))

    conn.commit()
    conn.close()

    await ctx.send(f"✅ Updated progress to page **{page}**")

@bot.command()
async def mybook(ctx):
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT book_title, current_page, total_pages
        FROM reading_progress
        WHERE user_id = ?
    """, (ctx.author.id,))
    row = cur.fetchone()
    conn.close()

    if not row:
        await ctx.send("You’re not tracking a book yet. Use `!startbook <title>`.")
        return

    title, current_page, total_pages = row
    total_part = f"/{total_pages}" if total_pages else ""
    await ctx.send(f"📚 **{title}** — {current_page}{total_part} pages")


@bot.command()
async def finish(ctx):
    # Ensure profile exists / update username + last_active
    ensure_profile(ctx.author)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # Get active book
    cur.execute("""
        SELECT book_title, total_pages
        FROM reading_progress
        WHERE user_id = ?
    """, (ctx.author.id,))
    row = cur.fetchone()

    if not row:
        conn.close()
        await ctx.send("You don’t have an active book to finish.")
        return

    title, total_pages = row

    # Archive into finished_books
    cur.execute("""
        INSERT INTO finished_books (user_id, book_title, total_pages)
        VALUES (?, ?, ?)
    """, (ctx.author.id, title, total_pages))

    # Remove active book
    cur.execute("""
        DELETE FROM reading_progress
        WHERE user_id = ?
    """, (ctx.author.id,))

    # Update profile stats
    cur.execute("""
        UPDATE user_profiles
        SET books_finished = books_finished + 1,
            current_book = NULL,
            current_page = 0,
            last_active = CURRENT_TIMESTAMP
        WHERE user_id = ?
    """, (ctx.author.id,))

    conn.commit()
    conn.close()

    await ctx.send(f"🎉 Finished: **{title}**")

@bot.command()
async def profile(ctx):
    ensure_profile(ctx.author)

    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("""
    SELECT username, current_book, current_page, books_finished, last_active
    FROM user_profiles
    WHERE user_id = ?
    """, (ctx.author.id,))
    row = cur.fetchone()
    conn.close()

    username, current_book, current_page, books_finished, last_active = row
    await ctx.send(
        f"👤 **{ctx.author.display_name}**\n"
        f"📖 Current: **{current_book or 'None'}** (page {current_page})\n"
        f"✅ Finished: **{books_finished}**\n"
        f"🕒 Last active: {last_active}"
    )





bot.run(token, log_handler=handler, log_level=logging.DEBUG)
