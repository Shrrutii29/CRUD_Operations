from fastapi import FastAPI, File, UploadFile, HTTPException, Depends
from pydantic import BaseModel
import sqlite3, csv

app = FastAPI()

# database setup
def init_db():
    with sqlite3.connect("data.db") as db:
        cursor = db.cursor()
        cursor.execute('''create table if not exists items (
            item_id integer primary key autoincrement,
            name text,
            description text
        )''')
        db.commit()

init_db()

# dependency
def get_db():
    db = sqlite3.connect("data.db")
    try:
        yield db
    finally:
        db.close()

# pydantic schema
class item(BaseModel):
    name: str
    description: str

# root
@app.get("/")
def read_root():
    return {"message": "welcome to CRUD API"}

# upload csv
@app.post("/upload_csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="please upload a csv file")

    content = await file.read()
    decoded = content.decode("utf-8").splitlines()
    reader = csv.DictReader(decoded)

    if "name" not in reader.fieldnames or "description" not in reader.fieldnames:
        raise HTTPException(status_code=400, detail="csv must contain 'name' and 'description' columns")

    con = sqlite3.connect("data.db")
    cursor = con.cursor()

    count = 0
    for row in reader:
        name = row.get("name", "").strip()
        description = row.get("description", "").strip()

        if name and description:
            cursor.execute(
                "INSERT INTO items (name, description) VALUES (?, ?)",
                (name, description)
            )
            count += 1

    con.commit()
    con.close()

    return {"message": f"{count} items inserted successfully"}


# get one item
@app.get("/get/{item_id}")
def get_item(item_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("select * from items where item_id = ?", (item_id,))
    row = cursor.fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="item not found")
    return {"item_id": row[0], "name": row[1], "description": row[2]}

# get all items
@app.get("/items")
def get_all_items(db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("select * from items")
    rows = cursor.fetchall()
    return [{"item_id": row[0], "name": row[1], "description": row[2]} for row in rows]

# create item
@app.post("/items")
def create_item(item: item, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("insert into items (name, description) values (?, ?)", (item.name, item.description))
    db.commit()
    return {"message": "item created successfully", "item_id": cursor.lastrowid}

# update item
@app.put("/items/{item_id}")
def update_item(item_id: int, item: item, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("select * from items where item_id = ?", (item_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="item not found")

    cursor.execute(
        "update items set name = ?, description = ? where item_id = ?",
        (item.name, item.description, item_id)
    )
    db.commit()
    return {"message": "item updated successfully"}

# delete item
@app.delete("/items/{item_id}")
def delete_item(item_id: int, db: sqlite3.Connection = Depends(get_db)):
    cursor = db.cursor()
    cursor.execute("select * from items where item_id = ?", (item_id,))
    if not cursor.fetchone():
        raise HTTPException(status_code=404, detail="item not found")

    cursor.execute("delete from items where item_id = ?", (item_id,))
    db.commit()
    return {"message": "item deleted successfully"}
