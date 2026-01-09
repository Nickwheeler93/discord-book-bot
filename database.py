from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

DEFAULT_DB_PATH = "./data/bookbot.db"

# Statuses
STATUS_PLAN = "plan_to_read"
STATUS_READING = "reading"
STATUS_FINISHED = "finished"
STATUS_DNF = "dnf"
STATUS_PAUSED = "paused"

ALLOWED_STATUSES = {STATUS_PLAN, STATUS_READING, STATUS_FINISHED, STATUS_DNF, STATUS_PAUSED}

# Milestones for congrats
MILESTONES = [25, 50, 75, 100]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


@contextmanager
def get_conn(db_path: str = DEFAULT_DB_PATH):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    finally:
        conn.close()


def _table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;",
        (table_name,),
    )
    return cur.fetchone() is not None


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.execute(f"PRAGMA table_info({table});")
    cols = {row["name"] for row in cur.fetchall()}
    return column in cols


def init_db(db_path: str = DEFAULT_DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute("PRAGMA foreign_keys = ON;")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                discord_user_id  TEXT NOT NULL UNIQUE,
                display_name     TEXT,
                goodreads_url    TEXT,
                created_at       TEXT NOT NULL
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS books (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                google_volume_id TEXT,
                title            TEXT NOT NULL,
                author           TEXT,
                isbn13           TEXT,
                published_year   INTEGER,
                created_at       TEXT NOT NULL,
                UNIQUE(google_volume_id),
                UNIQUE(isbn13)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_books (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL,
                book_id         INTEGER NOT NULL,
                status          TEXT NOT NULL,
                progress_pct    INTEGER NOT NULL DEFAULT 0,
                current_page    INTEGER,
                total_pages     INTEGER,
                started_at      TEXT,
                finished_at     TEXT,
                last_milestone  INTEGER NOT NULL DEFAULT 0,
                created_at      TEXT NOT NULL,
                updated_at      TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(book_id) REFERENCES books(id) ON DELETE CASCADE,
                UNIQUE(user_id, book_id)
            );
            """
        )

        conn.execute("CREATE INDEX IF NOT EXISTS idx_users_discord_id ON users(discord_user_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_books_user ON user_books(user_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_books_status ON user_books(status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_user_books_updated ON user_books(updated_at);")

        # Light migrations
        if _table_exists(conn, "users") and not _column_exists(conn, "users", "goodreads_url"):
            conn.execute("ALTER TABLE users ADD COLUMN goodreads_url TEXT;")

        if _table_exists(conn, "user_books") and not _column_exists(conn, "user_books", "last_milestone"):
            conn.execute("ALTER TABLE user_books ADD COLUMN last_milestone INTEGER NOT NULL DEFAULT 0;")

        conn.execute("PRAGMA foreign_keys = ON;")


# --------------------------
# Users
# --------------------------

def upsert_user(
    discord_user_id: str,
    display_name: Optional[str] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    now = utc_now_iso()
    with get_conn(db_path) as conn:
        conn.execute(
            """
            INSERT INTO users (discord_user_id, display_name, created_at)
            VALUES (?, ?, ?)
            ON CONFLICT(discord_user_id) DO UPDATE SET
                display_name = COALESCE(excluded.display_name, users.display_name);
            """,
            (discord_user_id, display_name, now),
        )
        row = conn.execute("SELECT id FROM users WHERE discord_user_id=?;", (discord_user_id,)).fetchone()
        return int(row["id"])


def set_goodreads_url(discord_user_id: str, url: Optional[str], db_path: str = DEFAULT_DB_PATH) -> None:
    with get_conn(db_path) as conn:
        conn.execute("UPDATE users SET goodreads_url=? WHERE discord_user_id=?;", (url, discord_user_id))


def get_user(discord_user_id: str, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    with get_conn(db_path) as conn:
        row = conn.execute("SELECT * FROM users WHERE discord_user_id=?;", (discord_user_id,)).fetchone()
        return dict(row) if row else None


# --------------------------
# Books
# --------------------------

def add_or_get_book(
    title: str,
    author: Optional[str] = None,
    google_volume_id: Optional[str] = None,
    isbn13: Optional[str] = None,
    published_year: Optional[int] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> int:
    title = title.strip()
    now = utc_now_iso()

    with get_conn(db_path) as conn:
        if google_volume_id:
            row = conn.execute("SELECT id FROM books WHERE google_volume_id=?;", (google_volume_id,)).fetchone()
            if row:
                return int(row["id"])

        if isbn13:
            row = conn.execute("SELECT id FROM books WHERE isbn13=?;", (isbn13,)).fetchone()
            if row:
                return int(row["id"])

        # soft dedupe by title+author
        row = conn.execute(
            """
            SELECT id FROM books
            WHERE lower(title)=lower(?) AND lower(COALESCE(author,''))=lower(COALESCE(?, ''));
            """,
            (title, (author or "").strip()),
        ).fetchone()
        if row:
            return int(row["id"])

        conn.execute(
            """
            INSERT INTO books (google_volume_id, title, author, isbn13, published_year, created_at)
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            (google_volume_id, title, author.strip() if author else None, isbn13, published_year, now),
        )
        return int(conn.execute("SELECT last_insert_rowid();").fetchone()[0])


def search_books_local(query: str, limit: int = 10, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    q = f"%{query.strip()}%"
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            SELECT * FROM books
            WHERE title LIKE ? OR author LIKE ?
            ORDER BY title ASC
            LIMIT ?;
            """,
            (q, q, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]


# --------------------------
# User-books
# --------------------------

def add_book_to_user(
    discord_user_id: str,
    title: str,
    author: Optional[str] = None,
    status: str = STATUS_READING,
    progress_pct: int = 0,
    current_page: Optional[int] = None,
    total_pages: Optional[int] = None,
    google_volume_id: Optional[str] = None,
    isbn13: Optional[str] = None,
    published_year: Optional[int] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> Tuple[int, int, bool]:
    status = status.strip().lower()
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"Invalid status '{status}'")

    progress_pct = max(0, min(100, int(progress_pct)))
    now = utc_now_iso()

    user_id = upsert_user(discord_user_id, db_path=db_path)
    book_id = add_or_get_book(
        title=title,
        author=author,
        google_volume_id=google_volume_id,
        isbn13=isbn13,
        published_year=published_year,
        db_path=db_path,
    )

    started_at = now if status == STATUS_READING else None
    finished_at = now if status == STATUS_FINISHED else None

    with get_conn(db_path) as conn:
        existing = conn.execute(
            "SELECT id FROM user_books WHERE user_id=? AND book_id=?;",
            (user_id, book_id),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE user_books
                SET status=?,
                    progress_pct=?,
                    current_page=COALESCE(?, current_page),
                    total_pages=COALESCE(?, total_pages),
                    started_at=COALESCE(?, started_at),
                    finished_at=COALESCE(?, finished_at),
                    updated_at=?
                WHERE user_id=? AND book_id=?;
                """,
                (
                    status,
                    progress_pct,
                    current_page,
                    total_pages,
                    started_at,
                    finished_at,
                    now,
                    user_id,
                    book_id,
                ),
            )
            return user_id, book_id, False

        conn.execute(
            """
            INSERT INTO user_books
              (user_id, book_id, status, progress_pct, current_page, total_pages, started_at, finished_at, last_milestone, created_at, updated_at)
            VALUES
              (?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?);
            """,
            (
                user_id,
                book_id,
                status,
                progress_pct,
                current_page,
                total_pages,
                started_at,
                finished_at,
                now,
                now,
            ),
        )
        return user_id, book_id, True


def list_user_books(
    discord_user_id: str,
    status: Optional[str] = None,
    limit: int = 50,
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict[str, Any]]:
    user = get_user(discord_user_id, db_path=db_path)
    if not user:
        return []
    user_id = int(user["id"])

    sql = """
        SELECT
            ub.book_id, ub.status, ub.progress_pct, ub.current_page, ub.total_pages,
            ub.started_at, ub.finished_at, ub.last_milestone, ub.updated_at,
            b.title, b.author, b.published_year
        FROM user_books ub
        JOIN books b ON b.id = ub.book_id
        WHERE ub.user_id = ?
    """
    params: List[Any] = [user_id]

    if status:
        st = status.strip().lower()
        if st not in ALLOWED_STATUSES:
            raise ValueError(f"Invalid status '{st}'")
        sql += " AND ub.status = ?"
        params.append(st)

    sql += " ORDER BY ub.updated_at DESC LIMIT ?"
    params.append(int(limit))

    with get_conn(db_path) as conn:
        cur = conn.execute(sql, tuple(params))
        return [dict(r) for r in cur.fetchall()]


def get_user_book_link(discord_user_id: str, book_id: int, db_path: str = DEFAULT_DB_PATH) -> Optional[Dict[str, Any]]:
    user = get_user(discord_user_id, db_path=db_path)
    if not user:
        return None
    user_id = int(user["id"])
    with get_conn(db_path) as conn:
        row = conn.execute(
            """
            SELECT
              ub.book_id, ub.status, ub.progress_pct, ub.current_page, ub.total_pages,
              ub.started_at, ub.finished_at, ub.last_milestone, ub.updated_at,
              b.title, b.author
            FROM user_books ub
            JOIN books b ON b.id = ub.book_id
            WHERE ub.user_id=? AND ub.book_id=?;
            """,
            (user_id, int(book_id)),
        ).fetchone()
        return dict(row) if row else None


def update_user_book_progress(
    discord_user_id: str,
    book_id: int,
    progress_pct: Optional[int] = None,
    current_page: Optional[int] = None,
    total_pages: Optional[int] = None,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    user = get_user(discord_user_id, db_path=db_path)
    if not user:
        raise ValueError("User not found.")
    user_id = int(user["id"])
    now = utc_now_iso()

    # derive percent if possible
    if progress_pct is None and current_page is not None and total_pages:
        if total_pages <= 0:
            raise ValueError("total_pages must be > 0")
        progress_pct = int(round((current_page / total_pages) * 100))

    if progress_pct is not None:
        progress_pct = max(0, min(100, int(progress_pct)))

    with get_conn(db_path) as conn:
        exists = conn.execute(
            "SELECT id FROM user_books WHERE user_id=? AND book_id=?;",
            (user_id, int(book_id)),
        ).fetchone()
        if not exists:
            raise ValueError("This book is not linked to the user.")

        conn.execute(
            """
            UPDATE user_books
            SET progress_pct = COALESCE(?, progress_pct),
                current_page = COALESCE(?, current_page),
                total_pages  = COALESCE(?, total_pages),
                updated_at = ?
            WHERE user_id = ? AND book_id = ?;
            """,
            (progress_pct, current_page, total_pages, now, user_id, int(book_id)),
        )


def update_user_book_status(
    discord_user_id: str,
    book_id: int,
    status: str,
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    status = status.strip().lower()
    if status not in ALLOWED_STATUSES:
        raise ValueError(f"Invalid status '{status}'")

    user = get_user(discord_user_id, db_path=db_path)
    if not user:
        raise ValueError("User not found.")
    user_id = int(user["id"])
    now = utc_now_iso()

    started_at = now if status == STATUS_READING else None
    finished_at = now if status == STATUS_FINISHED else None

    with get_conn(db_path) as conn:
        row = conn.execute(
            "SELECT started_at, finished_at FROM user_books WHERE user_id=? AND book_id=?;",
            (user_id, int(book_id)),
        ).fetchone()
        if not row:
            raise ValueError("This book is not linked to the user.")

        # preserve existing timestamps if already set
        if row["started_at"] and started_at:
            started_at = row["started_at"]
        if row["finished_at"] and finished_at:
            finished_at = row["finished_at"]

        conn.execute(
            """
            UPDATE user_books
            SET status=?,
                started_at=COALESCE(?, started_at),
                finished_at=COALESCE(?, finished_at),
                updated_at=?
            WHERE user_id=? AND book_id=?;
            """,
            (status, started_at, finished_at, now, user_id, int(book_id)),
        )


def set_last_milestone(discord_user_id: str, book_id: int, milestone: int, db_path: str = DEFAULT_DB_PATH) -> None:
    user = get_user(discord_user_id, db_path=db_path)
    if not user:
        return
    user_id = int(user["id"])
    now = utc_now_iso()
    with get_conn(db_path) as conn:
        conn.execute(
            """
            UPDATE user_books
            SET last_milestone=?, updated_at=?
            WHERE user_id=? AND book_id=?;
            """,
            (int(milestone), now, user_id, int(book_id)),
        )


def get_user_profile_summary(discord_user_id: str, db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    user = get_user(discord_user_id, db_path=db_path)
    if not user:
        return {"exists": False, "goodreads_url": None}

    counts = {s: 0 for s in ALLOWED_STATUSES}
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            SELECT status, COUNT(*) AS c
            FROM user_books ub
            JOIN users u ON u.id = ub.user_id
            WHERE u.discord_user_id = ?
            GROUP BY status;
            """,
            (discord_user_id,),
        )
        for r in cur.fetchall():
            if r["status"] in counts:
                counts[r["status"]] = int(r["c"])

    return {
        "exists": True,
        "goodreads_url": user.get("goodreads_url"),
        "display_name": user.get("display_name"),
        "counts": counts,
    }


def get_last_finished(discord_user_id: str, limit: int = 3, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    user = get_user(discord_user_id, db_path=db_path)
    if not user:
        return []
    user_id = int(user["id"])

    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            SELECT b.title, b.author, ub.finished_at
            FROM user_books ub
            JOIN books b ON b.id = ub.book_id
            WHERE ub.user_id=? AND ub.status=?
            ORDER BY ub.finished_at DESC, ub.updated_at DESC
            LIMIT ?;
            """,
            (user_id, STATUS_FINISHED, int(limit)),
        )
        return [dict(r) for r in cur.fetchall()]


def get_recent_reading_updates(limit: int = 5, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    """
    Returns up to N most-recent (unique) users who updated a reading book.
    """
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            SELECT
              u.discord_user_id,
              u.display_name,
              b.title,
              b.author,
              ub.progress_pct,
              ub.updated_at
            FROM user_books ub
            JOIN users u ON u.id = ub.user_id
            JOIN books b ON b.id = ub.book_id
            WHERE ub.status = ?
            ORDER BY ub.updated_at DESC
            LIMIT 50;
            """,
            (STATUS_READING,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        uid = r["discord_user_id"]
        if uid in seen:
            continue
        seen.add(uid)
        out.append(r)
        if len(out) >= limit:
            break
    return out


def get_recent_finishes(limit: int = 5, db_path: str = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    """
    Returns up to N most-recent (unique) users who finished a book.
    """
    with get_conn(db_path) as conn:
        cur = conn.execute(
            """
            SELECT
              u.discord_user_id,
              u.display_name,
              b.title,
              b.author,
              ub.finished_at
            FROM user_books ub
            JOIN users u ON u.id = ub.user_id
            JOIN books b ON b.id = ub.book_id
            WHERE ub.status = ? AND ub.finished_at IS NOT NULL
            ORDER BY ub.finished_at DESC
            LIMIT 50;
            """,
            (STATUS_FINISHED,),
        )
        rows = [dict(r) for r in cur.fetchall()]

    seen: set[str] = set()
    out: List[Dict[str, Any]] = []
    for r in rows:
        uid = r["discord_user_id"]
        if uid in seen:
            continue
        seen.add(uid)
        out.append(r)
        if len(out) >= limit:
            break
    return out
