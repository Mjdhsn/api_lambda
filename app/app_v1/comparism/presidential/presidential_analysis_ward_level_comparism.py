from app.app_v1.comparism.presidential.party_comparism  import comparism_table
from app.app_v1.database import get_db2
import time
import json

def get_ward_ward_all_results(country_id,state_id,lga_id,ward_id):
       with get_db2() as conn:
        cur = conn.cursor()
        
        conditions_country = {

    "polling_unit": {

        "total": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and state_id={state_id} and lga_id={lga_id} and ward_id={ward_id}""" ,
        "collated": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and (status='collated' OR status='canceled') and state_id={state_id} and lga_id={lga_id} and ward_id={ward_id}""",
        "non_collated": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and status='non collated' and state_id={state_id} and lga_id={lga_id} and ward_id={ward_id}""",
        "canceled": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and status='canceled' and state_id={state_id} and lga_id={lga_id} and ward_id={ward_id}""",
        
    },

 
    "others":{

        "WARD STATUS": f"""{comparism_table['query']}   select case when status='collated' then 'collated'  when status='non collated' then 'non collated' WHEN status='canceled' THEN 'canceled'
 ELSE 'check manually' end as message from ward_result_table where 1=1 and state_id={state_id} and lga_id={lga_id} and ward_id={ward_id}  """,

    "comparism_table": f"""{comparism_table['query']}
        
   select cc.row_num, cc.party,cc.Scores AS pu_result ,ccw.Scores AS ward_result from compare_ward cc 
   	left join compare_wardw ccw on ccw.party=cc.party and ccw.state_id=cc.state_id and ccw.lga_id=cc.lga_id and ccw.ward_id=cc.ward_id
         		 where 1=1 and ccw.state_id={state_id} and ccw.lga_id={lga_id} and ccw.ward_id={ward_id}  ORDER BY cc.party
         """
    }

}

     
        final_results = {}

        key_values = []
        execute_queries = []
        query =[]
        polling_unit = {}
        pol_final = {}
        ward_final ={}
        lga_final = {}
        state_final= {}
        ward={}
        lga={}
        others = {}
        for key, val in conditions_country['polling_unit'].items():
                execute_queries.append(val)
                key_values.append(key)
            
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        while True:

            for result in query:
                status = conn.get_query_status(result)
                if str(status) == 'QueryStatus.SUCCESS':
                    index = query.index(result)
                    key = key_values[index]
                    cur.get_results_from_sfqid(result)
                    val_results = cur.fetch_pandas_all()
                    val_results = val_results.to_json(orient="records")
                    val_results = json.loads(val_results)
                    # res = ret.to_json(orient="records")
                    # parsed = json.loads(res)
                    polling_unit[key] = val_results                      
                else :
                    time.sleep(0.005)
            if len(polling_unit) ==len(execute_queries):
                break
         
        pol_final['TOTAL PU'] = polling_unit['total']
        pol_final['COLLATED PU'] = polling_unit['collated']
        pol_final['NON-COLLATED PU'] = polling_unit['non_collated']
        pol_final['CANCELLED PU'] = polling_unit['canceled']

        key_values = []
        execute_queries = []
        query =[]
        for key, val in conditions_country['others'].items():
                execute_queries.append(val)
                key_values.append(key)
            
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        while True:

            for result in query:
                status = conn.get_query_status(result)
                if str(status) == 'QueryStatus.SUCCESS':
                    
                    index = query.index(result)
                    key = key_values[index]
                    cur.get_results_from_sfqid(result)
                    val_results = cur.fetch_pandas_all()
                    val_results = val_results.to_json(orient="records")
                    val_results = json.loads(val_results)
                    # res = ret.to_json(orient="records")
                    # parsed = json.loads(res)
                    others[key] = val_results                      
                else :
                    time.sleep(0.005)
            if len(others) ==len(execute_queries):
                break
       
        
        final_results['Polling Unit'] = pol_final
        final_results['others'] = others

        return final_results


          

          
    