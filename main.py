import os
import logging
import re
import discord
from discord.ext import commands
from dotenv import load_dotenv
import aiohttp

from database import (
    init_db,
    DEFAULT_DB_PATH,
    upsert_user,
    set_goodreads_url,
    get_user_profile_summary,
    add_book_to_user,
    list_user_books,
    update_user_book_progress,
    update_user_book_status,
    get_user_book_link,
    set_last_milestone,
    get_last_finished,
    get_recent_reading_updates,
    get_recent_finishes,
    STATUS_READING,
    STATUS_FINISHED,
    MILESTONES,
)

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

# Channels
WELCOME_CHANNEL_ID = 1457524496406806675          # welcome channel (also used for "I'm alive")
MILESTONE_CHANNEL_ID = 1455707887321088132        # milestones congrats channel

# Logging to mounted logs directory
os.makedirs("./logs", exist_ok=True)
handler = logging.FileHandler(filename="./logs/discord.log", encoding="utf-8", mode="a")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True
bot = commands.Bot(command_prefix="!", intents=intents)

# Per-user last search cache for Google Books
# {discord_user_id: [ {title, author, volume_id, published_year, isbn13, page_count}, ... ]}
LAST_SEARCH: dict[str, list[dict]] = {}


def ensure_user(ctx: commands.Context) -> None:
    upsert_user(str(ctx.author.id), display_name=ctx.author.display_name, db_path=DEFAULT_DB_PATH)


def format_reading_list(reading: list[dict]) -> str:
    lines = []
    for i, b in enumerate(reading, start=1):
        title = b.get("title") or "Untitled"
        author = b.get("author") or "Unknown author"
        pct = b.get("progress_pct", 0)

        cp = b.get("current_page")
        tp = b.get("total_pages")
        if cp is not None and tp:
            extra = f"{cp}/{tp} ({pct}%)"
        elif tp and pct is not None:
            extra = f"{pct}% (total {tp})"
        elif cp is not None:
            extra = f"page {cp}"
        else:
            extra = f"{pct}%"

        lines.append(f"{i}. **{title}** — {author} • {extra}")
    return "\n".join(lines)


def parse_progress_value(value: str) -> dict:
    """
    Accepts:
      - '120' (page)
      - '45%' (percent)
      - '120/500' (page/total)
    Returns dict with keys among: current_page, total_pages, progress_pct
    """
    v = value.strip()

    # percent
    if v.endswith("%"):
        pct = int(v[:-1].strip())
        return {"progress_pct": max(0, min(100, pct))}

    # fraction pages
    if "/" in v:
        a, b = v.split("/", 1)
        current_page = int(a.strip())
        total_pages = int(b.strip())
        if total_pages <= 0:
            raise ValueError("total pages must be > 0")
        pct = int(round((current_page / total_pages) * 100))
        return {
            "current_page": current_page,
            "total_pages": total_pages,
            "progress_pct": max(0, min(100, pct)),
        }

    # raw page number
    return {"current_page": int(v)}


def resolve_reading_book_id(discord_user_id: str, which: str | None) -> int | None:
    """
    Resolution rules:
    - if user has 1 reading book -> caller shouldn't use this, just pick it
    - if multiple:
        - which is digit -> index into reading list (1-based)
        - else -> substring match on title (case-insensitive)
    """
    reading = list_user_books(discord_user_id, status=STATUS_READING, db_path=DEFAULT_DB_PATH, limit=200)
    if not reading:
        return None

    if len(reading) == 1:
        return int(reading[0]["book_id"])

    if not which:
        return None

    w = which.strip()

    if w.isdigit():
        idx = int(w)
        if 1 <= idx <= len(reading):
            return int(reading[idx - 1]["book_id"])
        return None

    q = w.lower()
    # exact match
    for b in reading:
        if (b.get("title") or "").strip().lower() == q:
            return int(b["book_id"])
    # substring match
    for b in reading:
        if q in ((b.get("title") or "").lower()):
            return int(b["book_id"])

    return None


async def google_books_search(query: str, limit: int = 3) -> list[dict]:
    """
    Uses public Google Books volumes API.
    """
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {"q": query.strip(), "maxResults": str(max(1, min(10, limit)))}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()

    results: list[dict] = []
    for item in (data.get("items") or [])[:limit]:
        volume_id = item.get("id")
        info = item.get("volumeInfo") or {}

        title = info.get("title") or "Untitled"
        authors = info.get("authors") or []
        author = ", ".join(authors) if authors else None

        published = info.get("publishedDate") or ""
        year = int(published[:4]) if len(published) >= 4 and published[:4].isdigit() else None

        isbn13 = None
        for ident in (info.get("industryIdentifiers") or []):
            if ident.get("type") == "ISBN_13":
                isbn13 = ident.get("identifier")
                break

        page_count = info.get("pageCount")  # may be None

        results.append(
            {
                "title": title,
                "author": author,
                "volume_id": volume_id,
                "published_year": year,
                "isbn13": isbn13,
                "page_count": page_count,
            }
        )

    return results


async def announce_milestone_if_crossed(discord_user_id: str, book_id: int) -> None:
    link = get_user_book_link(discord_user_id, book_id, db_path=DEFAULT_DB_PATH)
    if not link:
        return

    pct = int(link.get("progress_pct") or 0)
    last = int(link.get("last_milestone") or 0)

    crossed = [m for m in MILESTONES if last < m <= pct]
    if not crossed:
        return

    new_last = max(crossed)
    set_last_milestone(discord_user_id, book_id, new_last, db_path=DEFAULT_DB_PATH)

    channel = bot.get_channel(MILESTONE_CHANNEL_ID)
    if not channel:
        return

    title = link.get("title") or "your book"
    await channel.send(f"🎉 <@{discord_user_id}> just hit **{new_last}%** on **{title}**! Keep going 💪📚")


@bot.event
async def on_ready():
    init_db(DEFAULT_DB_PATH)
    print("im ready!")

    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if channel:
        await channel.send("✅ I’m alive and online!")


@bot.event
async def on_member_join(member: discord.Member):
    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if channel:
        await channel.send(f"Welcome to the Book Club, {member.mention}! 👋")


# -----------------------
# Commands
# -----------------------

@bot.command()
async def setgoodreads(ctx, *, url: str):
    ensure_user(ctx)
    set_goodreads_url(str(ctx.author.id), url.strip(), db_path=DEFAULT_DB_PATH)
    await ctx.send("🔗 Saved! Your Goodreads URL will show in `!profile`.")


@bot.command()
async def profile(ctx):
    ensure_user(ctx)

    summary = get_user_profile_summary(str(ctx.author.id), db_path=DEFAULT_DB_PATH)
    goodreads = summary.get("goodreads_url") or "Not set"

    reading = list_user_books(str(ctx.author.id), status=STATUS_READING, db_path=DEFAULT_DB_PATH, limit=10)
    finished = get_last_finished(str(ctx.author.id), limit=3, db_path=DEFAULT_DB_PATH)

    reading_part = "None"
    if reading:
        # show up to 3 current reading
        lines = []
        for b in reading[:3]:
            title = b.get("title") or "Untitled"
            pct = b.get("progress_pct", 0)
            lines.append(f"- **{title}** ({pct}%)")
        reading_part = "\n".join(lines)

    finished_part = "None"
    if finished:
        lines = []
        for b in finished:
            t = b.get("title") or "Untitled"
            a = b.get("author") or "Unknown author"
            lines.append(f"- **{t}** — {a}")
        finished_part = "\n".join(lines)

    await ctx.send(
        f"👤 **{ctx.author.display_name}**\n"
        f"🔗 Goodreads: {goodreads}\n\n"
        f"📖 **Currently reading:**\n{reading_part}\n\n"
        f"✅ **Last 3 finished:**\n{finished_part}"
    )


@bot.command()
async def mybooks(ctx):
    ensure_user(ctx)
    reading = list_user_books(str(ctx.author.id), status=STATUS_READING, db_path=DEFAULT_DB_PATH, limit=50)
    if not reading:
        await ctx.send("You’re not tracking any books right now. Use `!startbook <title>`.")
        return
    await ctx.send("📚 **Currently reading:**\n" + format_reading_list(reading))


@bot.command()
async def currentlyreading(ctx):
    rows = get_recent_reading_updates(limit=5, db_path=DEFAULT_DB_PATH)
    if not rows:
        await ctx.send("No recent reading updates yet.")
        return

    lines = []
    for r in rows:
        name = r.get("display_name") or "Unknown"
        title = r.get("title") or "Untitled"
        author = r.get("author") or "Unknown author"
        pct = r.get("progress_pct", 0)
        lines.append(f"- **{name}** → **{title}** — {author} ({pct}%)")
    await ctx.send("🕒 **Recent reading updates:**\n" + "\n".join(lines))


@bot.command()
async def finishedbooks(ctx):
    rows = get_recent_finishes(limit=5, db_path=DEFAULT_DB_PATH)
    if not rows:
        await ctx.send("No recent finishes yet.")
        return

    lines = []
    for r in rows:
        name = r.get("display_name") or "Unknown"
        title = r.get("title") or "Untitled"
        author = r.get("author") or "Unknown author"
        lines.append(f"- **{name}** finished **{title}** — {author}")
    await ctx.send("🏁 **Recent finishes:**\n" + "\n".join(lines))


@bot.command()
async def searchbook(ctx, *, query: str):
    ensure_user(ctx)

    results = await google_books_search(query, limit=3)
    if not results:
        await ctx.send("No results found.")
        return

    LAST_SEARCH[str(ctx.author.id)] = results

    lines = []
    for i, r in enumerate(results, start=1):
        t = r["title"]
        a = r.get("author") or "Unknown author"
        y = f" ({r['published_year']})" if r.get("published_year") else ""
        pages = f" — {r['page_count']} pages" if r.get("page_count") else ""
        lines.append(f"{i}. **{t}** — {a}{y}{pages}")

    await ctx.send(
        "🔎 **Top results:**\n"
        + "\n".join(lines)
        + "\n\nUse `!addbook <index>` to add it to your currently reading."
    )


@bot.command()
async def addbook(ctx, index: int):
    ensure_user(ctx)

    results = LAST_SEARCH.get(str(ctx.author.id)) or []
    if not results:
        await ctx.send("Run `!searchbook <query>` or `!startbook <title>` first.")
        return

    if not (1 <= index <= len(results)):
        await ctx.send(f"Index out of range. Choose 1-{len(results)}.")
        return

    r = results[index - 1]
    title = r["title"]
    author = r.get("author")
    volume_id = r.get("volume_id")
    year = r.get("published_year")
    isbn13 = r.get("isbn13")
    page_count = r.get("page_count")

    _, _, created = add_book_to_user(
        discord_user_id=str(ctx.author.id),
        title=title,
        author=author,
        status=STATUS_READING,
        progress_pct=0,
        total_pages=page_count if isinstance(page_count, int) else None,
        google_volume_id=volume_id,
        isbn13=isbn13,
        published_year=year,
        db_path=DEFAULT_DB_PATH,
    )

    if created:
        extra = f" (total pages: {page_count})" if page_count else ""
        await ctx.send(f"✅ Added **{title}** to currently reading{extra}.")
    else:
        await ctx.send(f"♻️ **{title}** was already on your list — set to currently reading.")


@bot.command()
async def startbook(ctx, *, query: str):
    """
    Searches Google Books and shows top 3 for user to choose.
    Manual fallback supported by using a delimiter:
      !startbook Title Here | 500
    If Google finds nothing but you provided '| totalpages', it creates the book manually.
    """
    ensure_user(ctx)

    # Manual fallback: "title | pages"
    title = query.strip()
    manual_pages = None
    if "|" in title:
        left, right = title.split("|", 1)
        title = left.strip()
        right = right.strip()
        if right.isdigit():
            manual_pages = int(right)

    results = await google_books_search(title, limit=3)
    if results:
        LAST_SEARCH[str(ctx.author.id)] = results

        lines = []
        for i, r in enumerate(results, start=1):
            t = r["title"]
            a = r.get("author") or "Unknown author"
            y = f" ({r['published_year']})" if r.get("published_year") else ""
            pages = f" — {r['page_count']} pages" if r.get("page_count") else ""
            lines.append(f"{i}. **{t}** — {a}{y}{pages}")

        await ctx.send(
            "📖 **Pick one:**\n"
            + "\n".join(lines)
            + "\n\nUse `!addbook <index>` to add it to your currently reading."
        )
        return

    # No results: if they provided manual pages, create manually
    if manual_pages is not None:
        _, _, created = add_book_to_user(
            discord_user_id=str(ctx.author.id),
            title=title,
            author=None,
            status=STATUS_READING,
            progress_pct=0,
            total_pages=manual_pages,
            db_path=DEFAULT_DB_PATH,
        )
        if created:
            await ctx.send(f"✅ Couldn’t find it on Google Books — created **{title}** ({manual_pages} pages) and started it.")
        else:
            await ctx.send(f"♻️ **{title}** already existed — set to currently reading and total pages updated if needed.")
        return

    await ctx.send("No Google Books results. If you want to add manually, use: `!startbook Title | 500`")


@bot.command()
async def progress(ctx, value: str, *, which: str = None):
    """
    Update progress for currently reading book(s).

    If multiple reading books and no 'which' provided -> returns indexed list.
    Usage:
      !progress 120
      !progress 45%
      !progress 120/500
      !progress 120 2
      !progress 45% dune
    """
    ensure_user(ctx)

    reading = list_user_books(str(ctx.author.id), status=STATUS_READING, db_path=DEFAULT_DB_PATH, limit=200)
    if not reading:
        await ctx.send("You don’t have any active reading books. Use `!startbook <title>`.")
        return

    if len(reading) > 1 and not which:
        await ctx.send(
            "You have multiple books marked **currently reading**. Reply with an index or title:\n"
            + format_reading_list(reading)
            + "\n\nExample: `!progress 120 2` or `!progress 45% dune`"
        )
        return

    book_id = resolve_reading_book_id(str(ctx.author.id), which)
    if not book_id:
        await ctx.send("Couldn’t resolve that book. Try an index from the list or a clearer title.")
        return

    # Parse and compute missing fields where possible
    try:
        payload = parse_progress_value(value)
    except Exception as e:
        await ctx.send(f"Bad progress value. Try `120`, `45%`, or `120/500`. ({e})")
        return

    link = get_user_book_link(str(ctx.author.id), book_id, db_path=DEFAULT_DB_PATH)
    if not link:
        await ctx.send("Internal error: couldn’t load book link.")
        return

    # If user gives percent but we have total_pages and no current_page -> derive current_page
    if "progress_pct" in payload and "current_page" not in payload:
        tp = payload.get("total_pages") or link.get("total_pages")
        if tp:
            payload["current_page"] = int(round((payload["progress_pct"] / 100) * tp))
        else:
            # can't infer page without total pages
            pass

    # If user gives current_page and we have total_pages and percent not provided -> derive percent
    if "current_page" in payload and "progress_pct" not in payload:
        tp = payload.get("total_pages") or link.get("total_pages")
        if tp:
            payload["progress_pct"] = max(0, min(100, int(round((payload["current_page"] / tp) * 100))))

    # If they gave percent but we still have no total_pages and no current_page derivation possible
    if "progress_pct" in payload and link.get("total_pages") is None and payload.get("total_pages") is None and "current_page" not in payload:
        await ctx.send("I need total pages to translate percent into pages. Set it with: `!startbook Title | 500` (or use `120/500`).")
        # still allow storing percent alone
        update_user_book_progress(str(ctx.author.id), book_id, progress_pct=payload["progress_pct"], db_path=DEFAULT_DB_PATH)
        await announce_milestone_if_crossed(str(ctx.author.id), book_id)
        return

    update_user_book_progress(
        str(ctx.author.id),
        book_id,
        progress_pct=payload.get("progress_pct"),
        current_page=payload.get("current_page"),
        total_pages=payload.get("total_pages"),
        db_path=DEFAULT_DB_PATH,
    )

    await ctx.send("✅ Progress updated.")
    await announce_milestone_if_crossed(str(ctx.author.id), book_id)


@bot.command(name="finish", aliases=["finishbook"])
async def finish_book(ctx, *, which: str = None):
    """
    Mark a currently reading book as finished.
    If multiple reading books and no identifier -> returns indexed list.
    """
    ensure_user(ctx)

    reading = list_user_books(str(ctx.author.id), status=STATUS_READING, db_path=DEFAULT_DB_PATH, limit=200)
    if not reading:
        await ctx.send("You don’t have any active reading books to finish.")
        return

    if len(reading) > 1 and not which:
        await ctx.send(
            "You have multiple books marked **currently reading**. Reply with an index or title:\n"
            + format_reading_list(reading)
            + "\n\nExample: `!finish 2` or `!finish dune`"
        )
        return

    book_id = resolve_reading_book_id(str(ctx.author.id), which)
    if not book_id:
        await ctx.send("Couldn’t resolve that book. Try an index from the list or a clearer title.")
        return

    # Snap to 100% if we can
    link = get_user_book_link(str(ctx.author.id), book_id, db_path=DEFAULT_DB_PATH)
    tp = link.get("total_pages") if link else None
    cp = link.get("current_page") if link else None
    if tp and (cp is None or cp < tp):
        update_user_book_progress(str(ctx.author.id), book_id, current_page=tp, progress_pct=100, db_path=DEFAULT_DB_PATH)
    else:
        update_user_book_progress(str(ctx.author.id), book_id, progress_pct=100, db_path=DEFAULT_DB_PATH)

    update_user_book_status(str(ctx.author.id), book_id, status=STATUS_FINISHED, db_path=DEFAULT_DB_PATH)

    await ctx.send("🎉 Marked as **finished**!")
    await announce_milestone_if_crossed(str(ctx.author.id), book_id)


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN missing. Put it in .env as DISCORD_TOKEN=...")

    bot.run(TOKEN, log_handler=handler, log_level=logging.INFO)
