from pathlib import Path
import sqlite3

DB_PATH = Path("/dbfs/Workspace/Shared") / "dbx_tester.db"
def db_conn():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    return conn, cursor

class InitError(Exception):
    pass

class init:
    def __init__(self):

        self.conn, self.cursor = db_conn()
        try:
            self.create_global_config()
            self.create_notebook_test()
            self.create_job_test()
        except Exception as e:
            raise InitError(f"Error initializing database: {e}")
        finally:
            if self.conn:
                self.conn.close()
        pass

    def create_global_config(self):
        query = """
        CREATE TABLE IF NOT EXISTS global_config (
            test_dir TEXT,
            cluster TEXT,
            repo_dir TEXT,
            test_cache_dir TEXT,
            log_dir TEXT,
        )
        """
        self.cursor.execute(query)
        self.conn.commit()
        pass

    def create_notebook_test(self):
        query = """
        CREATE TABLE IF NOT EXISTS notebook_test (
            test_id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_dir TEXT,
            test_path TEXT,
            test_name TEXT,
            test_dag TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
    )
        """
        self.cursor.execute(query)
        self.conn.commit()
        pass

    def create_job_test(self):
        query = """
        CREATE TABLE IF NOT EXISTS job_test (
            test_id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_dir TEXT,
            test_path TEXT,
            test_name TEXT,
            test_dag TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
        """
        self.cursor.execute(query)
        self.conn.commit()
        pass

    def create_notebook_test_logs(self):
        query = """
        CREATE TABLE IF NOT EXISTS notebook_test_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_id INTEGER,
            runs TEXT,
            status TEXT,
            errorlogs TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ends_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.cursor.execute(query)
        self.conn.commit()
        pass


    def create_job_test_logs(self):
        query = """
        CREATE TABLE IF NOT EXISTS job_test_logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            test_id INTEGER,
            runs TEXT,
            status TEXT,
            errorlogs TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ends_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        self.cursor.execute(query)
        self.conn.commit()
        pass
