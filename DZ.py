from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse
import asyncpg
import json
from datetime import datetime

'''CREATE TABLE cars (
  id INT PRIMARY KEY,
  model TEXT NOT NULL,
  year DATE NOT NULL,
  color TEXT NOT NULL,
  plate_number TEXT NOT NULL,
  car_type TEXT NOT NULL,
  accident_ids INT[]
);

CREATE TABLE accidents (
  id INT PRIMARY KEY,
  car_ids INT[] NOT NULL,
  accident_date DATE NOT NULL,
  damage_description TEXT
);
'''


app = FastAPI()

#--- ЧТЕНИЕ ---

async def select_cars(conn):
    cars = await conn.fetch(f"SELECT * FROM semyshev_subachev.cars")
    return cars
async def select_accidents(conn):
    accidents = await conn.fetch(f"SELECT * FROM semyshev_subachev.accidents")
    return accidents
async def select_car(conn, car_id):
    car = await conn.fetch(f"SELECT * FROM semyshev_subachev.cars WHERE id = $1", int(car_id))
    return car
async def select_car_accidents(conn, car_id):
    accidents = await conn.fetch(f"SELECT * FROM semyshev_subachev.accidents WHERE $1 = ANY(car_ids)", int(car_id))
    return accidents

#--- ОБНОВЛЕНИЕ ---

async def update_accidents(conn, car_id, accident_id):
    query = 'UPDATE semyshev_subachev.cars SET accident_ids = array_append(accident_ids, $2) WHERE id = $1'
    return await conn.fetchval(query, car_id, accident_id)

#--- СОЗДАНИЕ ---

async def insert_car(conn, model, year, color, plate_number, car_type, accident_ids):
    query = "INSERT INTO semyshev_subachev.cars (model, year, color, plate_number, car_type, accident_ids) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id;"
    car_id = await conn.fetchval(query, model, datetime.strptime(year, "%d.%m.%Y").date(), color, plate_number, car_type, json.loads(accident_ids))
    return car_id
async def insert_accident(conn, car_ids, accident_date, damage_description):
    query = "INSERT INTO semyshev_subachev.accidents (car_ids, accident_date, damage_description) VALUES ($1, $2, $3) RETURNING id;"
    accident_id = await conn.fetchval(query, car_ids, accident_date, damage_description)
    for car_id in car_ids:
        await update_accidents(conn, car_id, accident_id)
    return accident_id

#--- УДАЛЕНИЕ ---

async def remove_car(conn, car_id):
    await conn.fetchval(f'DELETE FROM semyshev_subachev.cars WHERE id = $1', int(car_id))
    await conn.fetchval(f'DELETE FROM semyshev_subachev.accidents WHERE $1 = ANY(car_ids)', int(car_id))
    return "Removed car!"


@app.get("/cars")
async def get_cars():
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await select_cars(conn)
@app.get("/accidents")
async def get_accidents():
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await select_accidents(conn)
@app.get("/get_car/{car_id}")
async def get_car(car_id):
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await select_car(conn, car_id)
@app.get("/get_car_accidents/{car_id}")
async def get_car_accidents(car_id):
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await select_car_accidents(conn, car_id)
@app.post("/upload_car/")
async def upload_car(model, year, color, plate_number, car_type, accident_ids):
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await insert_car(conn, model, year, color, plate_number, car_type, accident_ids)
@app.post("/upload_accident/")
async def upload_accident(car_ids, accident_date, damage_description):
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await insert_accident(conn, car_ids, accident_date, damage_description)
@app.post("/update_accidents/")
async def update_accident(car_id, accident_id):
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await update_accidents(conn, car_id, accident_id)
@app.delete("/delete_car/")
async def delete_car(car_id):
    conn = await asyncpg.connect(user='school',
                                 password='School1234*',
                                 database='school_db',
                                 host='79.174.88.238',
                                 port=15221)
    return await remove_car(conn, car_id)