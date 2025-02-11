import psycopg2
from psycopg2.extras import DictCursor
import threading
import time

DB_CONFIG = {
    "dbname": "db",
    "user": "ol",
    "password": "123",
    "host": "localhost",
    "port": 5432,
}


def create_tasks_table():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id SERIAL PRIMARY KEY,
            task_name VARCHAR(255) NOT NULL,
            status VARCHAR(50) NOT NULL DEFAULT 'pending',
            worker_id INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()


def add_task(task_name):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tasks (task_name) VALUES (%s);
    """,
        (task_name,),
    )
    conn.commit()
    cur.close()
    conn.close()
    print(f"Task '{task_name}' added to the queue.")


def fetch_task(worker_id):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=DictCursor)

    try:
        conn.autocommit = False

        cur.execute("""
            SELECT * FROM tasks
            WHERE status = 'pending'
            ORDER BY created_at
            FOR UPDATE SKIP LOCKED
            LIMIT 1
        """)

        task = cur.fetchone()

        if task:
            cur.execute(
                """
                UPDATE tasks
                SET status = 'processing', worker_id = %s, updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (worker_id, task["id"]),
            )

            conn.commit()
            return task
        else:
            conn.commit()
            return None

    except Exception as e:
        conn.rollback()
        raise e
    finally:
        cur.close()
        conn.close()


def execute_task(worker_id):
    task = fetch_task(worker_id)

    if task:
        try:
            print(
                f"Worker {worker_id} is processing task {task['id']}: {task['task_name']}"
            )

            time.sleep(2)

            conn = psycopg2.connect(**DB_CONFIG)
            cur = conn.cursor()

            cur.execute(
                """
                UPDATE tasks
                SET status = 'completed', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (task["id"],),
            )

            conn.commit()
            print(f"Worker {worker_id} completed task {task['id']}")

        except Exception as e:
            cur.execute(
                """
                UPDATE tasks
                SET status = 'failed', updated_at = CURRENT_TIMESTAMP
                WHERE id = %s
            """,
                (task["id"],),
            )

            conn.commit()
            print(f"Worker {worker_id} failed to process task {task['id']}: {e}")

        finally:
            cur.close()
            conn.close()
    else:
        print(f"No tasks available for worker {worker_id}")


def worker(worker_id):
    while True:
        execute_task(worker_id)
        time.sleep(1)


if __name__ == "__main__":
    create_tasks_table()

    add_task("Task 1")
    add_task("Task 2")
    add_task("Task 3")
    add_task("Task 4")
    add_task("Task 5")

    workers = []
    for i in range(3):  # 3 воркера
        t = threading.Thread(target=worker, args=(i + 1,))
        workers.append(t)
        t.start()

    for t in workers:
        t.join()
