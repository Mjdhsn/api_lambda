from app.app_v1.database import get_db,get_db2
import json
from datetime import datetime
# Get wards
def getWardbadge(state,lga):
    with get_db2() as conn:
        cur = conn.cursor()

        if state and lga:
            sql = f"""SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table WHERE 
            state_id = {state} AND 
            lga_id = {lga}"""
        else:
            sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_ward WHERE file_type=0 AND state_id = {state} AND lga_id = {lga} AND ward_id = {row['WARD_ID']}"""
                cur.execute(sql)
                row['images'] = cur.fetchone()
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_ward WHERE file_type=1 AND state_id = {state} AND lga_id = {lga} AND ward_id=  {row['WARD_ID']}"""
                cur.execute(sql)
                row['videos'] = cur.fetchone()
                json_data1.append(row['WARD_ID'])
                json_data2.append(row['WARD_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in ward", state,lga, e)
            return str(e)



# Get wards
def getWard(state_id,lga_id):
    with get_db2() as conn:
        cur = conn.cursor()

        if state_id and lga_id:
            sql = f"""SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table WHERE 
            state_id = {state_id} AND 
            lga_id = {lga_id}"""
        else:
            sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                json_data1.append(row['WARD_ID'])
                json_data2.append(row['WARD_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in ward", state_id,lga_id, e)
            return str(e)
    
# Get wards
def getdistrict(state_id,lga_id):
    with get_db2() as conn:
        cur = conn.cursor()

        if state_id and lga_id:
            sql = f"""SELECT DISTINCT state_id,state_name, district_id,district_name FROM sen_pu_table WHERE 
            state_id = {state_id} AND 
            lga_id = {lga_id}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                json_data1.append(row['DISTRICT_ID'])
                json_data2.append(row['DISTRICT_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in ward", state_id,lga_id, e)
            return str(e)

# Get wards
def getconstituency(state_id,lga_id):
    with get_db2() as conn:
        cur = conn.cursor()

        if state_id and lga_id:
            sql = f"""SELECT DISTINCT state_id, state_name,const_id,constituency_name FROM rep_pu_table WHERE 
            state_id = {state_id} AND 
            lga_id = {lga_id}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                json_data1.append(row['CONST_ID'])
                json_data2.append(row['CONSTITUENCY_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in ward", state_id,lga_id, e)
            return str(e)

# Get wards
def getWardsenate(state_name,senate_district,lga_name):
    with get_db2() as conn:
        cur = conn.cursor()

  
        sql = f"SELECT DISTINCT state_id, state_name, district_id, district_name,lga_id, lga_name, ward_id ,ward_name FROM sen_pu_table WHERE  state_id={state_name} and district_id={senate_district} and lga_id= {lga_name}"

        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            json_data1 = []
            json_data2 = []
            res= {}
            for row in results:
                json_data1.append(row['WARD_ID'])
                json_data2.append(row['WARD_NAME'])
            res['WARD_ID'] = json_data1
            res['WARD_NAME'] = json_data2
            return [json_data1] +[json_data2] + [results]
        except Exception as e:
            print("error in ward", state_name,senate_district, e)
            return str(e)

# Get wards
def getWardrep(state_name,constituency_name,lga_name):
    with get_db2() as conn:
        cur = conn.cursor()

  
        sql = f"SELECT DISTINCT  state_id, state_name,const_id,constituency_name, lga_id, lga_name,WARD_ID ,WARD_NAME FROM rep_pu_table WHERE  state_id={state_name} and const_id={constituency_name} and lga_id= {lga_name}"

        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            json_data1 = []
            json_data2 = []
            res= {}
            for row in results:
                json_data1.append(row['WARD_ID'])
                json_data2.append(row['WARD_NAME'])
            res['WARD_ID'] = json_data1
            res['WARD_NAME'] = json_data2
            return [json_data1] +[json_data2] +[results]
        except Exception as e:
            print("error in ward", state_name,constituency_name, e)
            return str(e)



# Get ward
def getWardResult(country_name,state_name,lga_name,WARD_NAME):
    with get_db2() as conn:
        cur = conn.cursor()
        sql = f"""SELECT * FROM ward_result_table where country_id = {country_name} AND state_id = {state_name} and lga_id = {lga_name} and ward_id = {WARD_NAME}"""
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
        




def getWardResultsenate(country_name,state_name,senate_district,lga_name,WARD_NAME):
    with get_db2() as conn:
        cur = conn.cursor()
        sql = f"""SELECT * FROM sen_ward_table where country_id = {country_name} AND state_id= {state_name} and district_id={senate_district} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
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

def getWardResultrep(country_name,state_name,constituency_name,lga_name,WARD_NAME):
    with get_db2() as conn:
        cur = conn.cursor()
        sql = f"""SELECT * FROM rep_ward_table where country_id = {country_name} AND state_id= {state_name} and const_id={constituency_name} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
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



def updateWardResult(country_name,state_name,lga_name,WARD_NAME, data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
      

        timer = now.strftime("%m/%d/%Y, %H:%M:%S")
        query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in data.items()]
        query = query[:-1]
        query = ", ".join(query)
        sql = f"""Update ward_result_table SET {query} , date_time ='{timer}',status='collated' where country_id = {country_name} AND state_id= {state_name} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
        sql2 = f"""Select * from ward_result_table Where country_id = {country_name} AND  state_id= {state_name} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
        try:
            cur.execute(sql)
            # results = cur.fetch_pandas_all()
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

def updateWardResultsenate(country_name,state_name,senate_district,lga_name,WARD_NAME,data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
      

        timer = now.strftime("%m/%d/%Y, %H:%M:%S")
        query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in data.items()]
        query = query[:-1]
        query = ", ".join(query)
        sql = f"""Update sen_ward_table SET {query} , date_time ='{timer}',status='collated' where  state_id= {state_name} and district_id ={senate_district} and lga_id = {lga_name} and WARD_ID = {WARD_NAME}"""
        sql2 = f"""Select * from sen_ward_table Where  state_id= {state_name} and district_id ={senate_district} and lga_id = {lga_name} and WARD_ID = {WARD_NAME}"""
        try:
            cur.execute(sql)
            # results = cur.fetch_pandas_all()
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

def updateWardResultrep(country_name,state_name,constituency_name,lga_name,WARD_NAME, data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
      

        timer = now.strftime("%m/%d/%Y, %H:%M:%S")
        query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in data.items()]
        query = query[:-1]
        query = ", ".join(query)
        sql = f"""Update rep_ward_table SET {query} , date_time ='{timer}',status='collated' where  state_id= {state_name} and const_id ={constituency_name} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
        sql2 = f"""Select * from rep_ward_table Where  state_id= {state_name} and const_id ={constituency_name} and lga_id = {lga_name} and WARD_ID = {WARD_NAME}"""
        try:
            cur.execute(sql)
            # results = cur.fetch_pandas_all()
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


def cancelWardResult(country_name,state_name,lga_name,WARD_NAME, data={}):
    now = datetime.now() 

    with get_db2() as conn:
        cur = conn.cursor()
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        sql = f"""Update ward_result_table SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'  where country_id = {country_name} AND state_id= {state_name} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
        sql2 = f"""Select * FROM ward_result_table  where country_id = {country_name} AND state_id= {state_name} and lga_id = {lga_name} and WARD_ID = {WARD_NAME}"""
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


def cancelWardResultsenate(country_name,state_name,senate_district,lga_name,WARD_NAME,data={}):
    now = datetime.now() 

    with get_db2() as conn:
        cur = conn.cursor()
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        sql = f"""Update sen_ward_table SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}' where state_id= {state_name}  and district_id={senate_district} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
        sql2 = f"""Select * FROM sen_ward_table  Where  state_id= {state_name} and district_id={senate_district} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
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


def cancelWardResultsrep(country_name,state_name,constituency_name,lga_name,WARD_NAME,data={}):
    now = datetime.now() 

    with get_db2() as conn:
        cur = conn.cursor()
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        sql = f"""Update rep_ward_table SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}' where  state_id= {state_name}  and const_id ={constituency_name} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME}"""
        sql2 = f"""Select * FROM rep_ward_table  Where  state_id= {state_name} and const_id ={constituency_name} and lga_id = {lga_name}  and WARD_ID = {WARD_NAME} """
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
