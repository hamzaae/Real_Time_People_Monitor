import firebase_admin
from firebase_admin import db, credentials
import os

cred = credentials.Certificate("dbs/firebase/credentials.json")
firebaseURL = "https://rtdbpc-default-rtdb.firebaseio.com/"

firebase_admin.initialize_app(cred, {"databaseURL": firebaseURL})

# ref = db.reference("/zone_1")
print(db.reference("/").get())
# db.reference("/zone_1").set(8)
# print(db.reference("/zone_1").get())
