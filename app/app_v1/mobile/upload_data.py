
from app.app_v1.database import get_db,get_db2
import json


# def addData_all(role,country,state, lga, ward, pu, file, remark, type, lat, long, phone, email):



def addData_pu(state, lga, ward, pu, file, remark, type, lat, long, phone, email ):
    with get_db2() as conn:
        cur = conn.cursor()
        try:
            sql = '''INSERT INTO userdata_pu
                    (
                    state_name,
                    lga_name,
                   ward_name,
                   pu_name,
                   remark,
                   
                   file,
                    file_type,
                    lat,
                    long,
                    phone,
                     email

                    )
                    VALUES(% s, % s, % s, % s, % s, % s, % s, % s, %s,%s, %s)'''
                    
            cur.execute(
                sql, (state, lga, ward, pu,  remark, file, type, lat, long, phone, email))
            conn.commit()
        # app.conn.close()
            return '1'
        except:
            return '0'

def getData_pu(country,state, lga, ward, pu):

    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT * FROM userdata_pu WHERE country_id= {country} AND state_id = {state} AND lga_id= {lga} AND ward_id = {ward} AND pu_id = {pu}"
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetch_pandas_all()
            res = results.to_json(orient="records")
            parsed = json.loads(res)
            # cur.close()
            json_data = parsed
            return json_data
        except Exception as e:
            print(e)
            return str(e)



def addData_ward(state, lga, ward,file, remark, type, lat, long, phone, email ):
    with get_db2() as conn:
        cur = conn.cursor()
        try:
            sql = '''INSERT INTO userdata_ward
                    (
                    state_name,
                    lga_name,
                   ward_name,
                   remark,               
                   file,
                    file_type,
                    lat,
                    long,
                    phone,
                     email

                    )
                    VALUES(% s, % s, % s, % s, % s, % s, % s, %s,%s,%s)'''
                    
            cur.execute(
                sql, (state, lga, ward,  remark, file, type, lat, long, phone, email))
            conn.commit()
        # app.conn.close()
            return '1'
        except:
            return '0'


def getData_ward(country,state, lga, ward):

    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT * FROM userdata_ward WHERE country_id= {country} AND state_id = {state} AND lga_id= {lga} AND ward_id = {ward}"
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetch_pandas_all()
            res = results.to_json(orient="records")
            parsed = json.loads(res)
            # cur.close()
            json_data = parsed
            return json_data
        except Exception as e:
            print(e)
            return str(e)


def addData_lga(state, lga,file, remark, type, lat, long, phone, email ):
    with get_db2() as conn:
        cur = conn.cursor()
        try:
            sql = '''INSERT INTO userdata_lga
                    (
                    state_name,
                    lga_name,
                   remark,
                   
                   file,
                    file_type,
                    lat,
                    long,
                    phone,
                     email

                    )
                    VALUES(% s, % s, % s, % s, % s, %s,%s, %s, %s)'''
                    
            cur.execute(
                sql, (state, lga, remark, file, type, lat, long, phone, email))
            conn.commit()
        # app.conn.close()
            return '1'
        except:
            return '0'


def getData_lga(country,state, lga):

    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT * FROM userdata_lga WHERE country_id = {country} AND state_id = {state} AND lga_id= {lga}"
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetch_pandas_all()
            res = results.to_json(orient="records")
            parsed = json.loads(res)
            # cur.close()
            json_data = parsed
            return json_data
        except Exception as e:
            print(e)
            return str(e)

def addData_district(state, district, file, remark, type, lat, long, phone, email ):
    with get_db2() as conn:
        cur = conn.cursor()
        try:
            sql = '''INSERT INTO userdata_district
                    (
                    state_name,
                    district_name,
                   remark,
                   
                   file,
                    file_type,
                    lat,
                    long,
                    phone,
                     email

                    )
                    VALUES(% s, % s, % s, % s, % s, % s, %s,%s, %s)'''
                    
            cur.execute(
                sql, (state, district, remark, file, type, lat, long, phone, email))
            conn.commit()
        # app.conn.close()
            return '1'
        except:
            return '0'


def getData_district(country,state, constituency):

    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT * FROM userdata_district WHERE country_id= {country} AND state_id = {state} AND district_id= {constituency}"
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetch_pandas_all()
            res = results.to_json(orient="records")
            parsed = json.loads(res)
            # cur.close()
            json_data = parsed
            return json_data
        except Exception as e:
            print(e)
            return str(e)

def addData_constituency(state, constituency, file, remark, type, lat, long, phone, email ):
    with get_db2() as conn:
        cur = conn.cursor()
        try:
            sql = '''INSERT INTO userdata_constituency
                    (
                    state_name,
                    constituency_name,
                   remark,
                   file,
                    file_type,
                    lat,
                    long,
                    phone,
                     email

                    )
                    VALUES(% s, % s, % s, % s, % s, % s, %s,%s, %s)'''
                    
            cur.execute(
                sql, (state, constituency, remark, file, type, lat, long, phone, email))
            conn.commit()
        # app.conn.close()
            return '1'
        except:
            return '0'


def getData_constituency(country,state, constituency):

    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT * FROM userdata_constituency WHERE country_id= {country} AND state_id = {state} AND const_id = {constituency}"
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetch_pandas_all()
            res = results.to_json(orient="records")
            parsed = json.loads(res)
            # cur.close()
            json_data = parsed
            return json_data
        except Exception as e:
            print(e)
            return str(e)


def addData_state(state,file, remark, type, lat, long, phone, email ):
    with get_db2() as conn:
        cur = conn.cursor()
        try:
            sql = '''INSERT INTO userdata_state
                    (
                    state_name,
                   remark,
                   
                   file,
                    file_type,
                    lat,
                    long,
                    phone,
                     email

                    )
                    VALUES(% s, % s, % s, % s, % s, %s,%s, %s)'''
                    
            cur.execute(
                sql, (state, remark,file, type, lat, long, phone, email))
            conn.commit()
        # app.conn.close()
            return '1'
        except:
            return '0'


def getData_state(country,state):

    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT * FROM userdata_state WHERE country_id = {country} AND state_id ={state}"
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetch_pandas_all()
            res = results.to_json(orient="records")
            parsed = json.loads(res)
            # cur.close()
            json_data = parsed
            return json_data
        except Exception as e:
            print(e)
            return str(e)


def addData_country(country,file, remark,type, lat, long, phone, email ):
    with get_db2() as conn:
        cur = conn.cursor()
        try:
            sql = '''INSERT INTO userdata_country
                    (country,
                   remark,
                   
                   file,
                    file_type,
                    lat,
                    long,
                    phone,
                     email

                    )
                    VALUES(% s, % s, % s, % s, % s, %s,%s, %s)'''
                    
            cur.execute(
                sql, (country,remark, file, type, lat, long, phone, email))
            conn.commit()
        # app.conn.close()
            return '1'
        except:
            return '0'


#needs to be updated

def getData_country(country):

    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT * FROM userdata_country"
        try:
            cur.execute(sql)
            row_headers = [x[0] for x in cur.description]
            results = cur.fetch_pandas_all()
            res = results.to_json(orient="records")
            parsed = json.loads(res)
            # cur.close()
            json_data = parsed
            return json_data
        except Exception as e:
            print(e)
            return str(e)
