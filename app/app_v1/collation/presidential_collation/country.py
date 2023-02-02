
from app.app_v1.database import get_db,get_db2
from datetime import datetime
import json
# Get states
def getCountrybadge():
    with get_db2() as conn:
        cur = conn.cursor()

        sql = "SELECT COUNTRY_ID, COUNTRY_NAME FROM country_result_table;"
        
        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_country WHERE file_type=0"""

                cur.execute(sql)
                row['images'] = cur.fetchone()
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_country WHERE file_type=1"""
                cur.execute(sql)
                row['videos'] = cur.fetchone()
                json_data1.append(row['COUNTRY_ID'])
                json_data2.append(row['COUNTRY_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in country", e)
            return str(e)




def getCountry():
    with get_db2() as conn:
        cur = conn.cursor()

        sql = "SELECT COUNTRY_ID, COUNTRY_NAME FROM country_result_table;"
        
        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                json_data1.append(row['COUNTRY_ID'])
                json_data2.append(row['COUNTRY_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in state", e)
            return str(e)






# Get country
def getCountryResult(COUNTRY_NAME):
    with get_db2() as conn:
        cur = conn.cursor()
        sql = f"""SELECT * FROM country_result_table where COUNTRY_ID = {COUNTRY_NAME}"""
        final ={}
        try:
            cur.execute(sql)

            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
            total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
            data = ['DATE_TIME', 'PERSON_COLLATED']
            parties_results = {}
            total_results={}
            other_data_results={}
            for key in parties:
                parties_results.update( {key:results[0][key]})
               
            for key in total:
                total_results.update( {key:results[0][key]})

            for key in data:
                other_data_results.update( {key:results[0][key]})

            final['results'] = parties_results
            final['total'] = total_results
            final['other_data'] = other_data_results
            return final
        except Exception as e:
            print(e)
            return str(e)


def updateCountryResult(COUNTRY_NAME, data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
      

        timer = now.strftime("%m/%d/%Y, %H:%M:%S")
        query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in data.items()]
        query = query[:-1]
        query = ", ".join(query)
        sql = f"""Update country_result_table SET {query} , date_time ='{timer}',status='collated' where COUNTRY_ID= {COUNTRY_NAME}"""
        print(sql)
        sql2 = f"""Select * from country_result_table Where COUNTRY_ID = '{COUNTRY_NAME}'"""
        try:
            cur.execute(sql)
            conn.commit()
            cur.execute(sql2)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            res= {}
            for row in results:
                res.update({'person_collated':row['PERSON_COLLATED']})
                res.update({"time":row['DATE_TIME']})
            return res
        except Exception as e:
            print(e)
            return str(e)


   
def cancelCountryResult(COUNTRY_NAME, data={}):
    now = datetime.now() 
    data2 = data.copy()
    with get_db2() as conn:
        cur = conn.cursor()
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        sql = f"""Update country_result_table SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}' WHERE COUNTRY_ID={COUNTRY_NAME}"""
        sql2 = f"""Select * FROM country_result_table"""
        try:
            cur.execute(sql)
            conn.commit()
            cur.execute(sql2)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            res= {}
            for row in results:
                res.update({'person_collated':row['PERSON_COLLATED']})
                res.update({"time":row['DATE_TIME']})
            return res
        except Exception as e:
            print(e)
            return str(e)




