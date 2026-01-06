import sqlite3

DB_PATH = "data/books.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS reading_progress (
        user_id INTEGER PRIMARY KEY,
        book_title TEXT NOT NULL,
        total_pages INTEGER,
        current_page INTEGER DEFAULT 0,
        started_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS finished_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        book_title TEXT NOT NULL,
        total_pages INTEGER,
        finished_at TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_profiles (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        current_book TEXT,
        current_page INTEGER DEFAULT 0,
        books_finished INTEGER DEFAULT 0,
        last_active TEXT DEFAULT CURRENT_TIMESTAMP
    )
    """)

    conn.commit()
    conn.close()




if __name__ == "__main__":
    init_db()
    print("Database initialized")