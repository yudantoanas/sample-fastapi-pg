from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from datetime import datetime
import psycopg2
import pandas as pd

# create connection
conn = psycopg2.connect(
    dbname="neondb", user="neondb_owner", password="npg_sLfVg8iW4EwO",
    host="ep-steep-water-a102fmjl-pooler.ap-southeast-1.aws.neon.tech",
)

# create FastAPI object
app = FastAPI()


class Profile(BaseModel):
    name: str
    age: int
    location: str


@app.get('/')
async def getWelcome():
    return {
        "msg": "sample-fastapi-pg"
    }


@app.get('/profiles')
async def getProfiles():
    df = pd.read_sql("select * from profiles;", conn)

    return {
        "data": df.to_dict(orient="records")
    }


@app.get('/profiles/{id}')
async def getProfileById(id: int):
    query = f"""
    select * from profiles
    where id = {id}
    """
    df = pd.read_sql(query, conn)

    if len(df) == 0:
        raise HTTPException(status_code=404, detail="Data not found!")

    return {
        "data": df.to_dict(orient="records")
    }


@app.post("/profiles")
async def createProfile(profile: Profile):
    # get existing records
    query = """
    select * from profiles
    """
    df = pd.read_sql(query, conn)

    # insert new
    cursor = conn.cursor()
    sql = "INSERT INTO profiles (id, name, age, location, created_at) VALUES (%s, %s, %s, %s, %s)"
    cursor.execute(
        sql,
        (
            int(df.id.count() + 1), profile.name,
            profile.age, profile.location, datetime.now().date()
        )
    )

    conn.commit()

    return {
        "msg": "Successfully created new profile!"
    }


@app.patch("/profiles/{id}")
async def updateProfile(id: int, profile: Profile):
    cursor = conn.cursor()

    # update data
    query = f"""
    update profiles 
    set name = '{profile.name}',
    age = {profile.age},
    location = '{profile.location}'
    where id = {id}
    """
    cursor.execute(query)

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Data not found!")

    conn.commit()

    return {
        "msg": "Successfully updated profile!"
    }


@app.delete("/profiles/{id}")
async def updateProfile(id: int):
    cursor = conn.cursor()

    # delete data
    query = f"""
    delete from profiles
    where id = {id}
    """
    cursor.execute(query)

    if cursor.rowcount == 0:
        raise HTTPException(status_code=404, detail="Data not found!")

    conn.commit()

    return {
        "msg": "Successfully deleted profile!"
    }
