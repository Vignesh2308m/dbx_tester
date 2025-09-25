import sqlite3
import json

from dbx_tester.db.init import db_conn


class JobError(Exception):
    pass

def add_notebook_test(test_dir, test_path, test_name, test_dag):
    conn, cursor = db_conn()
    try:
        query = """
        INSERT INTO notebook_test (test_dir,test_path, test_name, test_dag)
        VALUES (?, ?, ?, ?) ON CONFLICT(test_dir,test_path, test_name) DO UPDATE SET
            test_dag=excluded.test_dag,
            updated_at=CURRENT_TIMESTAMP"""
        
        cursor.execute(query, (test_dir, test_path, test_name, json.dumps(test_dag)))
        conn.commit()
    except Exception as e:
        raise JobError(f"Error adding job: {e}")
    finally:
        if conn:
            conn.close()


def get_notebook_test(test_dir, test_path, test_name):
    conn, cursor = db_conn()
    try:
        query = """
        SELECT * FROM notebook_test WHERE test_dir=? AND test_path=? AND test_name=? """
        cursor.execute(query, (test_dir, test_path, test_name))
        result = cursor.fetchone()
        if result:
            return {
                "test_path": result[0],
                "test_name": result[1],
                "test_dag": json.loads(result[2]),
                "created_at": result[3],
                "updated_at": result[4],
            }
        else:
            return None
    except Exception as e:
        raise JobError(f"Error fetching job: {e}")
    finally:
        if conn:
            conn.close()

def list_notebook_tests(test_dir):
    conn, cursor = db_conn()
    try:
        query = """
        SELECT * FROM notebook_test WHERE test_dir=? """
        cursor.execute(query, (test_dir,))
        results = cursor.fetchall()
        jobs = []
        for result in results:
            jobs.append({
                "test_path": result[0],
                "test_name": result[1],
                "test_dag": json.loads(result[2]),
                "created_at": result[3],
                "updated_at": result[4],
            })
        return jobs
    except Exception as e:
        raise JobError(f"Error listing jobs: {e}")
    finally:
        if conn:
            conn.close()

def log_notebook_test(self,test_id, runs, status, errorlogs):
    try:
        query = """
        INSERT INTO notebook_test_logs (test_id, runs, status, errorlogs)
        VALUES (?, ?, ?, ?)"""
        self.cursor.execute(query, (test_id, json.dumps(runs), status, errorlogs))
        self.conn.commit()
    except Exception as e:
        raise JobError(f"Error logging job run: {e}")
    finally:
        if self.conn:
            self.conn.close()