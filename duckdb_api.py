from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import duckdb

app = FastAPI()


class Duck(BaseModel):
    id: int
    name: str
    age: int


con = duckdb.connect(database='ducks.db')


@app.post("/ducks/")
async def create_duck(duck: Duck):
    try:
        con.execute(
            f"INSERT INTO ducks VALUES ({duck.id}, '{duck.name}', {duck.age})")
        return {"message": "Duck created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/ducks/{duck_id}")
async def read_duck(duck_id: int):
    try:
        result = con.execute(f"SELECT * FROM ducks WHERE id={duck_id}")
        duck = result.fetchone()
        if duck:
            return {"id": duck[0], "name": duck[1], "age": duck[2]}
        else:
            raise HTTPException(status_code=404, detail="Duck not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.put("/ducks/{duck_id}")
async def update_duck(duck_id: int, duck: Duck):
    try:
        result = con.execute(
            f"UPDATE ducks SET name='{duck.name}', age={duck.age} WHERE id={duck_id}")
        if result.rowcount == 1:
            return {"message": "Duck updated successfully"}
        else:
            raise HTTPException(status_code=404, detail="Duck not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/ducks/{duck_id}")
async def delete_duck(duck_id: int):
    try:
        result = con.execute(f"DELETE FROM ducks WHERE id={duck_id}")
        if result.rowcount == 1:
            return {"message": "Duck deleted successfully"}
        else:
            raise HTTPException(status_code=404, detail="Duck not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
