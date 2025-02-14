import psycopg2
from psycopg2.extras import DictCursor
import threading
import time
import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

DB_CONFIG = {
    "dbname": "my_new_db",
    "user": "postgres",
    "password": "123",
    "host": "localhost",
    "port": 5432,
}


def create_tasks_table():
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    task_name VARCHAR(255) NOT NULL,
                    status VARCHAR(50) NOT NULL DEFAULT 'pending',
                    worker_id INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
                CREATE INDEX IF NOT EXISTS idx_status ON tasks(status);
            """)
            conn.commit()
            logging.info("Tasks table created or already exists.")


def add_task(task_name):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO tasks (task_name) VALUES (%s);
            """,
                (task_name,),
            )
            conn.commit()
            logging.info(f"Task '{task_name}' added to the queue.")


def fetch_task(worker_id):
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=DictCursor) as cur:
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
                logging.error(f"Error fetching task: {e}")
                raise e


def execute_task(worker_id):
    task = fetch_task(worker_id)

    if task:
        try:
            logging.info(
                f"Worker {worker_id} is processing task {task['id']}: {task['task_name']}"
            )

            # Имитация обработки задачи
            time.sleep(2)

            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE tasks
                        SET status = 'completed', updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """,
                        (task["id"],),
                    )

                    conn.commit()
                    logging.info(f"Worker {worker_id} completed task {task['id']}")

        except Exception as e:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE tasks
                        SET status = 'failed', updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                    """,
                        (task["id"],),
                    )

                    conn.commit()
                    logging.error(
                        f"Worker {worker_id} failed to process task {task['id']}: {e}"
                    )

    else:
        logging.info(f"No tasks available for worker {worker_id}")


def worker(worker_id):
    while True:
        execute_task(worker_id)
        time.sleep(1)


if __name__ == "__main__":
    create_tasks_table()

    # Добавление тестовых задач
    for i in range(1, 6):
        add_task(f"Task {i}")

    # Запуск воркеров
    workers = []
    for i in range(3):  # 3 воркера
        t = threading.Thread(target=worker, args=(i + 1,))
        workers.append(t)
        t.start()

    for t in workers:
        t.join()
