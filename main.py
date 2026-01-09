import os
import logging
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
    MILESTONES,
)

load_dotenv()
TOKEN = os.getenv("DISCORD_TOKEN")

os.makedirs("./logs", exist_ok=True)
handler = logging.FileHandler(filename="./logs/discord.log", encoding="utf-8", mode="a")

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

WELCOME_CHANNEL_ID = 1457524496406806675 # set to where you waelcome messages
MILESTONE_CHANNEL_ID = 1455707887321088132 # set to where you want congratulations messages


# per-user cache of last Google Books search results
# { discord_user_id(str): [ {title, authors, volume_id, published_year, isbn13}, ... ] }
LAST_SEARCH: dict[str, list[dict]] = {}


def ensure_user(ctx: commands.Context) -> None:
    upsert_user(
        discord_user_id=str(ctx.author.id),
        display_name=ctx.author.display_name,
        db_path=DEFAULT_DB_PATH,
    )


def format_reading_index_list(books: list[dict]) -> str:
    lines = []
    for i, b in enumerate(books, start=1):
        title = b.get("title") or "Untitled"
        author = b.get("author") or "Unknown author"
        pct = b.get("progress_pct", 0)
        lines.append(f"{i}. **{title}** — {author} ({pct}%)")
    return "\n".join(lines)


def resolve_book_from_arg(discord_user_id: str, arg: str | None, status_scope: str = "reading") -> int | None:
    """
    Resolve a book_id from either:
    - numeric index (1-based) into user's books with given status_scope
    - title substring match (best-effort) within that scope, fallback to all statuses
    If arg is None, returns None.
    """
    if not arg:
        return None

    arg = arg.strip()
    scoped = list_user_books(discord_user_id, status=status_scope, db_path=DEFAULT_DB_PATH, limit=200)

    # index
    if arg.isdigit():
        idx = int(arg)
        if 1 <= idx <= len(scoped):
            return int(scoped[idx - 1]["book_id"])
        return None

    q = arg.lower()

    # exact-ish match within scope
    for b in scoped:
        if (b.get("title") or "").strip().lower() == q:
            return int(b["book_id"])

    # contains match within scope
    for b in scoped:
        if q in ((b.get("title") or "").lower()):
            return int(b["book_id"])

    # fallback: search all user books
    allb = list_user_books(discord_user_id, db_path=DEFAULT_DB_PATH, limit=200)
    for b in allb:
        if q in ((b.get("title") or "").lower()):
            return int(b["book_id"])

    return None


async def google_books_search(query: str, limit: int = 5) -> list[dict]:
    """
    Uses Google Books public volumes endpoint (no key required for basic usage).
    Returns list of dicts with title/authors/volume_id/year/isbn13.
    """
    q = query.strip()
    url = "https://www.googleapis.com/books/v1/volumes"
    params = {"q": q, "maxResults": str(max(1, min(10, limit)))}

    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=15) as resp:
            resp.raise_for_status()
            data = await resp.json()

    out = []
    for item in data.get("items", [])[:limit]:
        volume_id = item.get("id")
        info = item.get("volumeInfo", {}) or {}
        title = info.get("title") or "Untitled"
        authors = info.get("authors") or []
        published = info.get("publishedDate") or ""
        year = None
        if len(published) >= 4 and published[:4].isdigit():
            year = int(published[:4])

        # try to pull isbn13
        isbn13 = None
        for ident in info.get("industryIdentifiers", []) or []:
            if ident.get("type") == "ISBN_13":
                isbn13 = ident.get("identifier")
                break

        out.append(
            {
                "title": title,
                "authors": ", ".join(authors) if authors else None,
                "volume_id": volume_id,
                "published_year": year,
                "isbn13": isbn13,
            }
        )
    return out


async def maybe_announce_milestone(ctx: commands.Context, book_id: int) -> None:
    """
    After a progress update, check if milestone crossed; if so, announce and persist last_milestone.
    """
    link = get_user_book_link(str(ctx.author.id), book_id, db_path=DEFAULT_DB_PATH)
    if not link:
        return

    pct = int(link.get("progress_pct") or 0)
    last = int(link.get("last_milestone") or 0)

    crossed = [m for m in MILESTONES if last < m <= pct]
    if not crossed:
        return

    new_last = max(crossed)
    set_last_milestone(str(ctx.author.id), book_id, new_last, db_path=DEFAULT_DB_PATH)

    title = link.get("title") or "that book"
    await ctx.send(f"🏁 Milestone! **{title}** just hit **{new_last}%** 🎉")


@bot.event
async def on_ready():
    init_db(DEFAULT_DB_PATH)
    print("we are ready to go!")


@bot.event
async def on_member_join(member: discord.Member):
    channel = bot.get_channel(WELCOME_CHANNEL_ID)
    if channel:
        await channel.send(f"Welcome to the Book Club, {member.mention}! 👋")


# ----------------
# Commands
# ----------------

@bot.command()
async def setgoodreads(ctx, *, url: str):
    ensure_user(ctx)
    set_goodreads_url(str(ctx.author.id), url.strip(), db_path=DEFAULT_DB_PATH)
    await ctx.send("🔗 Saved your Goodreads URL! It’ll show up in `!profile`.")


@bot.command()
async def profile(ctx):
    ensure_user(ctx)
    summary = get_user_profile_summary(str(ctx.author.id), db_path=DEFAULT_DB_PATH)
    counts = summary["counts"]
    goodreads = summary.get("goodreads_url") or "Not set"

    await ctx.send(
        f"👤 **{ctx.author.display_name}**\n"
        f"🔗 Goodreads: {goodreads}\n"
        f"📚 Plan: **{counts.get('plan_to_read', 0)}** | "
        f"Reading: **{counts.get('reading', 0)}** | "
        f"Finished: **{counts.get('finished', 0)}** | "
        f"DNF: **{counts.get('dnf', 0)}** | "
        f"Paused: **{counts.get('paused', 0)}**"
    )


@bot.command()
async def list(ctx, status: str = None):
    ensure_user(ctx)
    books = list_user_books(str(ctx.author.id), status=status, db_path=DEFAULT_DB_PATH, limit=50)
    if not books:
        await ctx.send("No books found for that filter.")
        return

    lines = []
    for i, b in enumerate(books, start=1):
        title = b.get("title") or "Untitled"
        author = b.get("author") or "Unknown author"
        st = b.get("status")
        pct = b.get("progress_pct", 0)
        lines.append(f"{i}. **{title}** — {author} [{st}] ({pct}%)")

    await ctx.send("📚 Your books:\n" + "\n".join(lines[:25]))


@bot.command()
async def search(ctx, *, query: str):
    ensure_user(ctx)

    results = await google_books_search(query, limit=5)
    if not results:
        await ctx.send("No results found.")
        return

    LAST_SEARCH[str(ctx.author.id)] = results

    lines = []
    for i, r in enumerate(results, start=1):
        t = r["title"]
        a = r["authors"] or "Unknown author"
        y = f" ({r['published_year']})" if r.get("published_year") else ""
        lines.append(f"{i}. **{t}** — {a}{y}")

    await ctx.send(
        "🔎 Google Books results:\n"
        + "\n".join(lines)
        + "\n\nUse: `!addbook <index> [plan|reading|finished|dnf|paused]`"
    )


@bot.command()
async def addbook(ctx, index: int, status: str = "plan_to_read"):
    """
    Add a book from your last !search results.
    Usage: !addbook 2 reading
    """
    ensure_user(ctx)

    status = status.strip().lower()
    # normalize common short forms
    if status == "plan":
        status = "plan_to_read"

    results = LAST_SEARCH.get(str(ctx.author.id)) or []
    if not results:
        await ctx.send("Run `!search <query>` first.")
        return

    if not (1 <= index <= len(results)):
        await ctx.send(f"Index out of range. Choose 1-{len(results)}.")
        return

    r = results[index - 1]
    title = r["title"]
    author = r.get("authors")
    volume_id = r.get("volume_id")
    year = r.get("published_year")
    isbn13 = r.get("isbn13")

    _, book_id, created = add_book_to_user(
        discord_user_id=str(ctx.author.id),
        title=title,
        author=author,
        status=status,
        progress_pct=0,
        google_volume_id=volume_id,
        isbn13=isbn13,
        published_year=year,
        db_path=DEFAULT_DB_PATH,
    )

    if created:
        await ctx.send(f"✅ Added **{title}** ({status})")
    else:
        await ctx.send(f"♻️ You already had **{title}** — I updated it to **{status}**.")

    # if they add as reading, it becomes the "active reading" list; progress commands will disambiguate if multiple.


@bot.command()
async def startbook(ctx, *, title: str):
    """
    Convenience: start reading a book by title (creates a local book record if needed).
    """
    ensure_user(ctx)
    _, _, created = add_book_to_user(
        discord_user_id=str(ctx.author.id),
        title=title,
        author=None,
        status="reading",
        progress_pct=0,
        db_path=DEFAULT_DB_PATH,
    )
    if created:
        await ctx.send(f"📖 Started **{title}** (reading).")
    else:
        await ctx.send(f"📖 Set **{title}** to **reading**.")


@bot.command()
async def totalpages(ctx, total_pages: int, *, which: str = None):
    """
    Set total pages for a reading book.

    If multiple reading books and you don't specify which, bot replies with an index list.
    Usage:
      !totalpages 500
      !totalpages 500 2
      !totalpages 500 dune
    """
    ensure_user(ctx)

    reading = list_user_books(str(ctx.author.id), status="reading", db_path=DEFAULT_DB_PATH, limit=200)
    if not reading:
        await ctx.send("No active reading books. Use `!startbook <title>` or `!addbook <index> reading`.")
        return

    if len(reading) > 1 and not which:
        await ctx.send(
            "You have multiple books marked **reading**. Reply with an index:\n"
            + format_reading_index_list(reading)
            + "\n\nExample: `!totalpages 500 2`"
        )
        return

    book_id = resolve_book_from_arg(str(ctx.author.id), which or "1", status_scope="reading") if len(reading) > 1 else int(reading[0]["book_id"])
    if not book_id:
        await ctx.send("Couldn't resolve that book. Try an index from the list or a clearer title.")
        return

    update_user_book_progress(
        discord_user_id=str(ctx.author.id),
        book_id=book_id,
        total_pages=total_pages,
        db_path=DEFAULT_DB_PATH,
    )
    await ctx.send(f"📏 Set total pages to **{total_pages}**")


@bot.command()
async def progress(ctx, page: int, *, which: str = None):
    """
    Update current page for a reading book.
    Usage:
      !progress 120
      !progress 120 2
      !progress 120 dune
    """
    ensure_user(ctx)

    reading = list_user_books(str(ctx.author.id), status="reading", db_path=DEFAULT_DB_PATH, limit=200)
    if not reading:
        await ctx.send("No active reading books. Use `!startbook <title>` first.")
        return

    if len(reading) > 1 and not which:
        await ctx.send(
            "You have multiple books marked **reading**. Reply with an index or title:\n"
            + format_reading_index_list(reading)
            + "\n\nExample: `!progress 120 2` or `!progress 120 dune`"
        )
        return

    book_id = resolve_book_from_arg(str(ctx.author.id), which or "1", status_scope="reading") if len(reading) > 1 else int(reading[0]["book_id"])
    if not book_id:
        await ctx.send("Couldn't resolve that book. Try an index from the list or a clearer title.")
        return

    update_user_book_progress(
        discord_user_id=str(ctx.author.id),
        book_id=book_id,
        current_page=page,
        db_path=DEFAULT_DB_PATH,
    )
    await ctx.send(f"✅ Updated progress to page **{page}**")
    await maybe_announce_milestone(ctx, book_id)


@bot.command()
async def progresspct(ctx, percent: int, *, which: str = None):
    """
    Update percent progress for a reading book.
    Usage:
      !progresspct 45
      !progresspct 45 2
      !progresspct 45 dune
    """
    ensure_user(ctx)

    reading = list_user_books(str(ctx.author.id), status="reading", db_path=DEFAULT_DB_PATH, limit=200)
    if not reading:
        await ctx.send("No active reading books. Use `!startbook <title>` first.")
        return

    if len(reading) > 1 and not which:
        await ctx.send(
            "You have multiple books marked **reading**. Reply with an index or title:\n"
            + format_reading_index_list(reading)
            + "\n\nExample: `!progresspct 45 2` or `!progresspct 45 dune`"
        )
        return

    book_id = resolve_book_from_arg(str(ctx.author.id), which or "1", status_scope="reading") if len(reading) > 1 else int(reading[0]["book_id"])
    if not book_id:
        await ctx.send("Couldn't resolve that book. Try an index from the list or a clearer title.")
        return

    pct = max(0, min(100, int(percent)))
    update_user_book_progress(
        discord_user_id=str(ctx.author.id),
        book_id=book_id,
        progress_pct=pct,
        db_path=DEFAULT_DB_PATH,
    )
    await ctx.send(f"✅ Updated progress to **{pct}%**")
    await maybe_announce_milestone(ctx, book_id)


@bot.command()
async def finish(ctx, *, which: str = None):
    """
    Mark a book finished.
    If multiple reading books and no 'which', reply with index list.
    Usage:
      !finish
      !finish 2
      !finish dune
    """
    ensure_user(ctx)

    reading = list_user_books(str(ctx.author.id), status="reading", db_path=DEFAULT_DB_PATH, limit=200)
    if not reading:
        await ctx.send("No active reading books to finish.")
        return

    if len(reading) > 1 and not which:
        await ctx.send(
            "You have multiple books marked **reading**. Reply with an index or title:\n"
            + format_reading_index_list(reading)
            + "\n\nExample: `!finish 2` or `!finish dune`"
        )
        return

    book_id = resolve_book_from_arg(str(ctx.author.id), which or "1", status_scope="reading") if len(reading) > 1 else int(reading[0]["book_id"])
    if not book_id:
        await ctx.send("Couldn't resolve that book. Try an index from the list or a clearer title.")
        return

    update_user_book_progress(str(ctx.author.id), book_id, progress_pct=100, db_path=DEFAULT_DB_PATH)
    update_user_book_status(str(ctx.author.id), book_id, status="finished", db_path=DEFAULT_DB_PATH)

    await ctx.send("🎉 Marked as **finished**!")
    await maybe_announce_milestone(ctx, book_id)


if __name__ == "__main__":
    if not TOKEN:
        raise RuntimeError("DISCORD_TOKEN missing. Put it in .env as DISCORD_TOKEN=...")

    bot.run(TOKEN, log_handler=handler, log_level=logging.INFO)
