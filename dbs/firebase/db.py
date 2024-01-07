import firebase_admin
from firebase_admin import db, credentials
import os

cred = credentials.Certificate("dbs/firebase/credentials.json")
firebaseURL = "https://rtdbpc-default-rtdb.firebaseio.com/"

firebase_admin.initialize_app(cred, {"databaseURL": firebaseURL})

db.reference("/Y").set(55)
db.reference("/X").set(8)
print(db.reference("/Y").get())

db.reference("/").child('users').push({
    'name': 'John Doe',
    'age': 25,
    'email': 'john@example.com'
})



# Test 1 -------------------------------

# for i in range(15):
#     db.reference("/X").set(i)
#     print(db.reference("/zone_1").get())
    # os.system("cls")
# db.reference("/Y").set(55)
# db.reference("/X").set(8)
# print(db.reference("/Y").get())

# db.reference("/").child('users').push({
#     'name': 'John Doe',
#     'age': 25,
#     'email': 'john@example.com'
# })

# Test 2 -------------------------------    

# import pyautogui
# import time

# def track_cursor():
#     try:
#         while True:
#             x, y = pyautogui.position()
#             db.reference("/X").set(x)
#             db.reference("/Y").set(y)
#             print(f"Cursor position: x={x}, y={y}")
#             # time.sleep(1)  

#     except KeyboardInterrupt:
#         print("Tracking stopped.")

# if __name__ == "__main__":
#     track_cursor()

