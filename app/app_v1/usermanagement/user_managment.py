from app.app_v1.database import get_db
def checkNumber(number):
    sql = f"""SELECT * from user_managment WHERE phone = '{number}'"""
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cur.execute(sql)
            result = cur.fetchone()
            if result:
                return {'response': True, "user": result}
            else:
                return  {'response': False}
        except: 
            return {'response': False}


# Authenticate
def authenticate(name, passcode):
    with get_db() as conn:
        cur = conn.cursor()

        sql = f"""SELECT * from user_managment WHERE email = '{name}' and passcode = '{passcode}'"""
        sql2 = f"""SELECT * from user_managment WHERE phone = '{name}' and passcode = '{passcode}'"""
        sql3 = f"""SELECT * from user_managment WHERE name = '{name}' and passcode = '{passcode}'"""

        result = None
        try:
            cur.execute(sql)
            result = cur.fetchone()
        except Exception as e:
            try:
                cur.execute(sql2)
                result = cur.fetchone()
            except Exception as e:
                print(e)
                try:
                    cur.execute(sql3)
                    result = cur.fetchone()
                except Exception as e:
                    print("User Not Found")
                    
        if result:
            return result
        else:
            return False
            

# Get User
def getUser(user_id = None, state=None, lga=None, ward=None, email=None):
    with get_db() as conn:
        cur = conn.cursor()
        query = ""
        if state and state != "undefined":
            query = f"place like '{state},%'"
        if lga and lga != "undefined":
            query = f"place like '{state},{lga},%'"
        if ward and ward != "undefined":
            query = f"place like '{state},{lga},{ward},%'"

        if user_id:
            sql = f"""SELECT * from user_managment WHERE id = {user_id}"""
        elif query:
            sql = f"""SELECT * from user_managment where {query} AND Not email = '{email}'"""
        else:
            sql = f"""SELECT * from user_managment where Not email = '{email}'"""

        user_levels = {
            1:"Polling Level",
            2:"Ward Level",
            3:"LGA Level",
            4:"State Level",
            5:"Admin",
        }
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetchall()
            for result in results:
                result['user_level'] = user_levels[result['user_type']]
            json_data = results
            return json_data
        except Exception as e:
            print(e)
            return str(e)


def getUserByEmail(email):
    with get_db() as conn:
        cur = conn.cursor()

        sql = f"""SELECT * from user_managment WHERE email = '{email}'"""
        try:
            cur.execute(sql)
            json_data = cur.fetchone()
            return json_data
        except Exception as e:
            print(e)
            return str(e)
        

# Add User
def addUser(name, email, phone, passcode, user_type, place):
    with get_db() as conn:
        cur = conn.cursor()

        sql = f"""INSERT INTO `user_managment` (`name`, `email`, `phone`, `passcode`, `user_type`, `place`) VALUES ('{name}', '{email}', '{phone}', '{passcode}', {user_type}, '{place}')"""
        try:
            cur.execute(sql)
            conn.commit()
            return {'code':200, 'message': 'User created successfully'}
        except Exception as e:
            print(e)
            return str(e)


# Update User
def editUser(user_id, name, email, phone, passcode, user_type):
    with get_db() as conn:
        cur = conn.cursor()

        sql = f"""Update `user_managment` SET `name`=`{name}` `email`=`{email}`, `phone`=`{phone}`, `passcode`=`{[passcode]}`, `user_type`={user_type} Where id = {user_id})"""
        try:
            cur.execute(sql)
            conn.commit()
            return {'message': 'User updated successfully'}
        except Exception as e:
            print(e)
            return str(e)


def deleteUser(id):
    with get_db() as conn:
        cur = conn.cursor()

        sql = f"""DELETE FROM `user_managment` Where id = {id}"""
        try:
            cur.execute(sql)
            conn.commit()
            return {'message': 'User Deleted successfully'}
        except Exception as e:
            print(e)
            return str(e)