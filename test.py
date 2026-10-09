import time
import pickle
import tracemalloc
import random


# --------------------------------------------------
# 1. Generate 1,000 rows × 2 columns
# --------------------------------------------------

def generate_2_column_data(rows=1000):
    data = []

    for i in range(rows):
        row = {
            "id": i,
            "value": random.randint(1, 1000)
        }

        data.append(row)

    return data


# --------------------------------------------------
# 2. Generate 1,000 rows × 15 columns
# --------------------------------------------------

def generate_15_column_data(rows=1000):
    data = []

    for i in range(rows):
        row = {
            "id": i,
            "name": f"user_{i}",
            "age": random.randint(18, 70),
            "city": random.choice(["Toronto", "Mississauga", "Brampton"]),
            "salary": random.randint(30000, 100000),
            "department": random.choice(["IT", "HR", "Finance", "Sales"]),
            "experience": random.randint(0, 20),
            "score": random.random(),
            "active": random.choice([True, False]),
            "orders": random.randint(0, 100),
            "rating": random.randint(1, 5),
            "country": "Canada",
            "bonus": random.randint(0, 10000),
            "remote": random.choice([True, False]),
            "join_year": random.randint(2015, 2026)
        }

        data.append(row)

    return data


# --------------------------------------------------
# 3. Serialize the data and measure time + memory
# --------------------------------------------------

def serialize_data(data):
    # Start measuring memory
    tracemalloc.start()

    # Start measuring time
    start_time = time.perf_counter()

    # Serialize
    serialized_data = pickle.dumps(data)

    # Stop measuring time
    end_time = time.perf_counter()

    # Get memory information
    current_memory, peak_memory = tracemalloc.get_traced_memory()

    # Stop memory tracking
    tracemalloc.stop()

    elapsed_time = end_time - start_time

    return {
        "serialized_data": serialized_data,
        "time_seconds": elapsed_time,
        "peak_memory_MB": peak_memory / (1024 * 1024),
        "serialized_size_MB": len(serialized_data) / (1024 * 1024)
    }


# --------------------------------------------------
# 4. Generate both datasets
# --------------------------------------------------

data_2_columns = generate_2_column_data()

data_15_columns = generate_15_column_data()

# --------------------------------------------------
# 5. Serialize both using the SAME function
# --------------------------------------------------

result_2_columns = serialize_data(data_2_columns)

result_15_columns = serialize_data(data_15_columns)

# --------------------------------------------------
# 6. Display results
# --------------------------------------------------

print("===== 2 COLUMN DATASET =====")

print("Rows:", len(data_2_columns))
print("Columns:", len(data_2_columns[0]))

print("Serialization time:",
      result_2_columns["time_seconds"], "seconds")

print("Peak memory:",
      result_2_columns["peak_memory_MB"], "MB")

print("Serialized size:",
      result_2_columns["serialized_size_MB"], "MB")

print("\n===== 15 COLUMN DATASET =====")

print("Rows:", len(data_15_columns))
print("Columns:", len(data_15_columns[0]))

print("Serialization time:",
      result_15_columns["time_seconds"], "seconds")

print("Peak memory:",
      result_15_columns["peak_memory_MB"], "MB")

print("Serialized size:",
      result_15_columns["serialized_size_MB"], "MB")