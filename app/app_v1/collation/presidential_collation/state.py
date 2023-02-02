
from app.app_v1.database import get_db,get_db2
from datetime import datetime
import json
# Get states
def getStatebadge():
    with get_db2() as conn:
        cur = conn.cursor()

        sql = "SELECT DISTINCT state_id, state_name FROM pu_result_table"
        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_state WHERE file_type=0 AND state_id  = {row['STATE_ID']}"""
                cur.execute(sql)
                row['images'] = cur.fetchone()
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_state WHERE file_type=1 AND state_id = {row['STATE_ID']}"""
                cur.execute(sql)
                row['videos'] = cur.fetchone()
                json_data1.append(row['STATE_ID'])
                json_data2.append(row['STATE_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in state", e)
            return str(e)


def getSenateebadge(state_id):
    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT DISTINCT state_id, state_name, district_id, district_name FROM sen_pu_table where state_id ={state_id}"
        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_district WHERE file_type=0 AND state_id  = {row['STATE_ID']} and district_id= {row['DISTRICT_ID']}"""
                cur.execute(sql)
                row['images'] = cur.fetchone()
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_district WHERE file_type=1 AND state_id = {row['STATE_ID']} and district_id= {row['DISTRICT_ID']}"""
                cur.execute(sql)
                row['videos'] = cur.fetchone()
                json_data1.append(row['DISTRICT_ID'])
                json_data2.append(row['DISTRICT_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in state", e)
            return str(e)

def getRepebadge(state_id):
    with get_db2() as conn:
        cur = conn.cursor()

        sql = f"SELECT DISTINCT state_id, state_name, const_id, constituency_name FROM rep_pu_table where state_id={state_id}"
        try:
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)
            #cur.close()
            json_data1 = []
            json_data2 = []
            for row in results:
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_constituency WHERE file_type=0 AND state_id  = {row['STATE_ID']} and const_id= {row['CONST_ID']}"""
                cur.execute(sql)
                row['images'] = cur.fetchone()
                sql = f"""SELECT COUNT(*) as  count1 FROM userdata_constituency WHERE file_type=1 AND state_id = {row['STATE_ID']} and const_id= {row['CONST_ID']}"""
                cur.execute(sql)
                row['videos'] = cur.fetchone()
                json_data1.append(row['CONST_ID'])
                json_data2.append(row['CONSTITUENCY_NAME'])
            return [json_data1]+[json_data2]+[results]
        except Exception as e:
            print("error in state", e)
            return str(e)


def getState():
        with get_db2() as conn:
            cur = conn.cursor()

            sql = "SELECT DISTINCT state_id, state_name FROM pu_result_table"
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                #cur.close()
                json_data1 = []
                json_data2 = []
                for row in results:
                    json_data1.append(row['STATE_ID'])
                    json_data2.append(row['STATE_NAME'])
                return [json_data1]+[json_data2]+[results]
            except Exception as e:
                print("error in state", e)
                return str(e)

def getStateSenate(state_name):
        with get_db2() as conn:
            cur = conn.cursor()

            sql = f"SELECT DISTINCT state_id, state_name ,DISTRICT_ID, DISTRICT_NAME FROM sen_pu_table where state_id={state_name}"
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                #cur.close()
                json_data1 = []
                json_data2 = []
                res= {}
                for row in results:
                    json_data1.append(row['DISTRICT_ID'])
                    json_data2.append(row['DISTRICT_NAME'])
                res['DISTRICT_ID'] = json_data1
                res['DISTRICT_NAME'] =json_data2
                return [json_data1] + [json_data2] +[results]
            except Exception as e:
                print("error in state", e)
                return str(e)

def getStaterep(state_name):
        with get_db2() as conn:
            cur = conn.cursor()

            sql = f"SELECT DISTINCT state_id, state_name , const_id, CONSTITUENCY_NAME FROM rep_pu_table where state_id={state_name}"
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                #cur.close()
                json_data1 = []
                json_data2 = []
                res= {}
                for row in results:
                    json_data1.append(row['CONST_ID'])
                    json_data2.append(row['CONSTITUENCY_NAME'])
                res['CONSTITUENCY_NAME'] =json_data2
                return [json_data1] +[json_data2] + [results]
            except Exception as e:
                print("error in state", e)
                return str(e)



# Get state


# Get state
def getStateResult(country_name,state_name):
    with get_db2() as conn:
        cur = conn.cursor()
        sql = f"""SELECT * FROM state_result_table where country_id = {country_name} AND state_id = {state_name}"""
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


def getStateResultsenate(country_name,state_name,senate_district):
    with get_db2() as conn:
        cur = conn.cursor()
        sql = f"""SELECT * FROM sen_district_table where country_id = {country_name} AND state_id = {state_name}  and  district_id={senate_district}"""
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

def getStateResultrep(country_name,state_name,CONSTITUENCY_NAME):
    with get_db2() as conn:
        cur = conn.cursor()
        sql = f"""SELECT * FROM rep_constituency_table where country_id = {country_name} AND state_id = {state_name}  and CONST_ID={CONSTITUENCY_NAME}"""
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



def updateStateResult(country_name,state_name, data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
     
      
        
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in data.items()]
        query = query[:-1]
        query = ", ".join(query)
        sql = f"""Update state_result_table SET {query} , date_time ='{timer}', status='collated' where country_id = {country_name} and state_id = {state_name}"""
        sql2 = f"""Select * from state_result_table where country_id = {country_name} AND state_id = {state_name}"""
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





def updateStateResultsenate(country_name,state_name,senate_district,data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
      

        timer = now.strftime("%m/%d/%Y, %H:%M:%S")
        query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in data.items()]
        query = query[:-1]
        query = ", ".join(query)
        sql = f"""Update sen_district_table SET {query} , date_time ='{timer}',status='collated' where  state_id = {state_name}  and DISTRICT_ID ={senate_district}"""
        sql2 = f"""Select * from sen_district_table Where  state_id = {state_name}  and DISTRICT_ID ={senate_district} """
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


def updateStateResultrep(country_name,state_name,CONSTITUENCY_NAME,data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
      

        timer = now.strftime("%m/%d/%Y, %H:%M:%S")
        query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in data.items()]
        query = query[:-1]
        query = ", ".join(query)
        sql = f"""Update rep_constituency_table SET {query} , date_time ='{timer}',status='collated' where  state_id = {state_name}  and CONST_ID ={CONSTITUENCY_NAME}"""
        sql2 = f"""Select * from rep_constituency_table Where  state_id = {state_name}  and CONST_ID ={CONSTITUENCY_NAME}"""
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



















def cancelStateResult(country_name,state_name, data={}):
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        sql = f"""Update state_result_table SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}' where country_id = {country_name} AND state_id = {state_name}"""
        sql2 = f"""Select * FROM state_result_table  where country_id = {country_name} AND state_id = {state_name}"""
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


def cancelStateResultsenate(country_name,state_name,senate_district,data={}):
    now = datetime.now() 

    with get_db2() as conn:
        cur = conn.cursor()
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        sql = f"""Update sen_district_table SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}' where state_id = {state_name}  and DISTRICT_ID ={senate_district}"""
        sql2 = f"""Select * FROM sen_district_table  Where  state_id = {state_name}  and DISTRICT_ID ={senate_district} """
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




def cancelStateResultsrep(country_name,state_name,CONSTITUENCY_NAME,data={}):
    now = datetime.now() 

    with get_db2() as conn:
        cur = conn.cursor()
        timer = now.strftime("%m/%d/%Y, %H:%M:%S")

        sql = f"""Update rep_constituency_table SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}' where  state_id = {state_name}  and  CONST_ID ={CONSTITUENCY_NAME}"""
        sql2 = f"""Select * FROM rep_constituency_table  Where  state_id = {state_name}  and CONST_ID ={CONSTITUENCY_NAME}"""
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






