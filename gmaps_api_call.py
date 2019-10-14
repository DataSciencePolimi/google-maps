import googlemaps
import requests
import json
import sqlite3
import requests
import json
import time

APIKEY = 'YOUR_GMAPS_API_KEY'


def create_connection(db_file):
   """ create a database connection to the SQLite database
       specified by the db_file
   :param db_file: database file
   :return: Connection object or None
   """
   conn = None
   try:
      conn = sqlite3.connect(db_file)
      cursor = conn.cursor()
      cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
      print(cursor.fetchall())
   except Exception as e:
      print(e)

   return conn


def place_exists_in_db(conn, place_id):
   res = False
   try:
      cur = conn.cursor()
      cur.execute("SELECT * FROM poi where place_id=?",(place_id,))

      rows = cur.fetchall()
      if (len(rows)==0):
         print("No record found for given place id: " + str(place_id))
         return False
      if (len(rows)>1):
         print("Inconsistency, place Id field should be UNIQUE but found more than 1 records for place id: " + str(place_id) )
         return False

      row = rows[0]
      print("Record found: " + str(row))
      res = True
   except Exception as ex:
      print("exception" + ex)

   return res


def insert_place_to_db(conn, place_id, address, lat, lng, google_id, name, rating, user_ratings_total, types):
   id = None
   try:
      cur = conn.cursor()

      cur.execute("INSERT INTO poi(place_id, address, lat, lng, google_id, poi_name, rating, user_ratings_total, types) VALUES(?,?,?,?,?,?,?,?,?)",(place_id, address, lat, lng, google_id, name, rating, user_ratings_total, types) )
      id = cur.lastrowid
      conn.commit()
   except Exception as ex:
      print("exception" + ex)
   return id


def apiCall(conn, url, token=None):
   try:
      print(url)
      if not token is None:
         token = "&pagetoken="+token
         url += token

      response = requests.get(url)
      res = json.loads(response.text)
      print("here results ---->>> ", len(res["results"]))

      if ("results" in res):
         results_paginated = res["results"]
         for result in results_paginated:
            print(result)

            if not ("place_id" in result):
               print("place_id should exist in returning results, skipping this POI record")
               continue;

            place_id = str(result["place_id"])

            if(place_exists_in_db(conn, place_id)):
               print("This place already exists in DB: " + str(result))
               continue;

            else:
               print("This place does not exist in DB, a new record will be added")

               if ("formatted_address" in result):
                  address = str(result["formatted_address"])

               if ("geometry" in result):
                  if ("location" in result["geometry"]):
                     if ("lat" in result["geometry"]["location"]):
                        lat = str(result["geometry"]["location"]["lat"])
                     if ("lng" in result["geometry"]["location"]):
                        lng = str(result["geometry"]["location"]["lng"])

               if ("id" in result):
                  google_id = str(result["id"])

               if ("name" in result):
                  name = str(result["name"])

               if ("rating" in result):
                  rating = str(result["rating"])

               if ("user_ratings_total" in result):
                  user_ratings_total = str(result["user_ratings_total"])

               if ("types" in result):
                  types = str(result["types"])

               print("address:" + address)
               print("lat:" + lat)
               print("lng:" + lng)
               print("google_id:" + google_id)
               print("place_id:" + place_id)
               print("name:" + name)
               print("rating:" + rating)
               print("user_ratings_total:" + user_ratings_total)
               print("types:" + types)

               inserted_record_id = insert_place_to_db(conn, place_id, address, lat, lng, google_id, name,
                                                       rating, user_ratings_total, types)

               print("insertion to sqlite is success! id:" + str(inserted_record_id))

      pagetoken = res.get("next_page_token",None)

      print("here -->> ", pagetoken)
   except Exception as ex:
      print(ex)
   return pagetoken


def findPlacesFromText(loc=("32.5732","25.9692"),radius=4000, pagetoken = None):
   lat, lng = loc
   type = "hospital"
   url = "https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={lat},{lng}&radius={radius}&type={type}&key={APIKEY}{pagetoken}".format(lat = lat, lng = lng, radius = radius, type = type,APIKEY = APIKEY, pagetoken = "&pagetoken="+pagetoken if pagetoken else "")
   url2 = "https://maps.googleapis.com/maps/api/place/textsearch/xml?query=hospitals+in+Maputo&key="+APIKEY

   print(url2)
   response = requests.get(url2)
   res = json.loads(response.text)
   # print(res)
   print("here results ---->>> ", len(res["results"]))

   for result in res["results"]:
      info = ";".join(map(str,[result["name"],result["geometry"]["location"]["lat"],result["geometry"]["location"]["lng"],result.get("rating",0),result["place_id"]]))
      print(info)
   pagetoken = res.get("next_page_token",None)

   print("here -->> ", pagetoken)

   return pagetoken


if __name__ == "__main__" :
   conn = create_connection("/Users/emrecalisir/anaconda3/safari.db")

   if not conn:
      print("connection to sqlite could not be initialized, exiting")
      exit(-1)

   # TEST
   exists = place_exists_in_db(conn, "1")
   if not exists:
      inserted_record_id = insert_place_to_db(conn, "1", "address", "lat", "lng", "google_id", "name", "rating", "user_ratings_total", "types")
      print("insertion to sqlite is success! id:" + str(inserted_record_id))

   pagetoken = None

   #fields = 'address_component,adr_address,formatted_address,geometry,icon,name,permanently_closed,photo,place_id,plus_code,type,url,utc_offset,vicinity,formatted_phone_number,international_phone_number,opening_hours,website,price_level,rating,review,user_ratings_total'
   #url = "https://maps.googleapis.com/maps/api/place/details/json?place_id=37b1d83c7abcf938ffd87f3422a7eeab12773403&fields=address_component,adr_address,formatted_address,geometry,icon,name,permanently_closed,photo,place_id,plus_code,type,url,utc_offset,vicinity,formatted_phone_number,international_phone_number,opening_hours,website,price_level,rating,review,user_ratings_total&key="+APIKEY
   #https://maps.googleapis.com/maps/api/place/search/JSON?location=Enter latitude,Enter Longitude&radius=10000&types=store&hasNextPage=true&nextPage()=true&sensor=false&key=Enter Google_Map_key
   url = "https://maps.googleapis.com/maps/api/place/textsearch/json?query=hospitals+in+Maputo&key=" + APIKEY

   print("started")
   cnt = 0
   MAX_LIMIT = 10

   next_page_token = apiCall(conn, url)
   cnt += 1

   while next_page_token!="":
      if(cnt >= MAX_LIMIT):
         print("limit is used enough for today, breaking the operation")
         break
      time.sleep(5)

      pagetoken = apiCall(conn, url, token = next_page_token)
      cnt += 1

   print("completed after " + str(cnt) + " api calls")
