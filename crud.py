import os
from flask import Flask, request, jsonify
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor

app = Flask(__name__)

# --- Database Connection ---
DATABASE_URL = os.environ.get('DATABASE_URL', 'postgresql://postgres:080902@localhost:5432/InternDB')

def get_db_connection():
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None

def create_table_if_not_exists():
    """Creates the 'students' table if it does not already exist."""
    conn = get_db_connection()
    if conn is None:
        print("Could not connect to database to create table. Please check your DATABASE_URL.")
        return
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS students (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE NOT NULL,
                    age INTEGER,
                    course VARCHAR(255),
                    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
                );
            """)
        conn.commit()
        print("Student table checked/created successfully.")
    except Exception as e:
        print(f"Error creating table: {e}")
        conn.rollback()
    finally:
        conn.close()

# Call the function to ensure the table exists when the app starts
create_table_if_not_exists()

# --- API Endpoints ---

@app.route('/students', methods=['POST'])
def add_student():
    """
    Endpoint to add a new student.
    Expected JSON format: {"name": "...", "email": "...", "age": ..., "course": "..."}
    """
    data = request.get_json()
    if not data or 'name' not in data or 'email' not in data:
        return jsonify({"error": "Name and email are required fields."}), 400

    name = data.get('name')
    email = data.get('email')
    age = data.get('age')
    course = data.get('course')

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO students (name, email, age, course) VALUES (%s, %s, %s, %s) RETURNING id;",
                (name, email, age, course)
            )
            student_id = cur.fetchone()[0]
            conn.commit()
            return jsonify({
                "message": "Student added successfully",
                "id": student_id,
                "name": name,
                "email": email
            }), 201
    except psycopg2.IntegrityError:
        # This will catch cases where the email is not unique
        conn.rollback()
        return jsonify({"error": "A student with this email already exists."}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/students', methods=['GET'])
def get_all_students():
    """
    Endpoint to retrieve all students from the database.
    """
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id, name, email, age, course, created_at FROM students ORDER BY id;")
            students = cur.fetchall()
            student_list = [dict(row) for row in students]
            # Convert datetime objects to string for JSON serialization
            for student in student_list:
                if 'created_at' in student and student['created_at']:
                    student['created_at'] = student['created_at'].isoformat()
            return jsonify(student_list)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/students/<int:student_id>', methods=['GET'])
def get_student(student_id):
    """
    Endpoint to get a single student by their ID.
    """
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        with conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("SELECT id, name, email, age, course, created_at FROM students WHERE id = %s;", (student_id,))
            student = cur.fetchone()
            if student:
                student_dict = dict(student)
                if student_dict['created_at']:
                    student_dict['created_at'] = student_dict['created_at'].isoformat()
                return jsonify(student_dict)
            else:
                return jsonify({"error": f"Student with ID {student_id} not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/students/<int:student_id>', methods=['PUT'])
def update_student(student_id):
    """
    Endpoint to update an existing student by ID.
    Expected JSON format: {"name": "...", "email": "...", "age": ..., "course": "..."}
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No data provided for update."}), 400

    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        with conn.cursor() as cur:
            query_parts = []
            params = []
            if 'name' in data:
                query_parts.append("name = %s")
                params.append(data['name'])
            if 'email' in data:
                query_parts.append("email = %s")
                params.append(data['email'])
            if 'age' in data:
                query_parts.append("age = %s")
                params.append(data['age'])
            if 'course' in data:
                query_parts.append("course = %s")
                params.append(data['course'])

            if not query_parts:
                return jsonify({"error": "No valid fields to update."}), 400

            params.append(student_id)
            update_query = sql.SQL("UPDATE students SET {} WHERE id = %s").format(
                sql.SQL(', ').join(map(sql.SQL, query_parts))
            )

            cur.execute(update_query, params)
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": f"Student with ID {student_id} not found."}), 404
            
            conn.commit()
            return jsonify({"message": f"Student with ID {student_id} updated successfully."})
    except psycopg2.IntegrityError:
        conn.rollback()
        return jsonify({"error": "A student with this email already exists."}), 409
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route('/students/<int:student_id>', methods=['DELETE'])
def delete_student(student_id):
    """
    Endpoint to delete a student by their ID.
    """
    conn = get_db_connection()
    if conn is None:
        return jsonify({"error": "Database connection failed"}), 500

    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM students WHERE id = %s;", (student_id,))
            if cur.rowcount == 0:
                conn.rollback()
                return jsonify({"error": f"Student with ID {student_id} not found."}), 404

            conn.commit()
            return jsonify({"message": f"Student with ID {student_id} deleted successfully."})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# Main entry point to run the app
if __name__ == '__main__':
    app.run(debug=True)

