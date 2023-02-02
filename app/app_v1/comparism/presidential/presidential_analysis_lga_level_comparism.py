from app.app_v1.comparism.presidential.party_comparism  import comparism_table
from app.app_v1.database import get_db2
import json
import time

def get_lga_lga_all_results(country_id,state_id,lga_id):
       with get_db2() as conn:
        cur = conn.cursor()
        conditions_country = {

    "polling_unit": {

        "total": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and state_id={state_id} and lga_id={lga_id}""" ,
        "collated": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and (status='collated' OR status='canceled') and state_id={state_id} and lga_id={lga_id}""",
        "non_collated": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and status='non collated' and state_id={state_id} and lga_id={lga_id}""",
        "canceled": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and status='canceled' and state_id={state_id} and lga_id={lga_id}""",
        
    },

    "ward":{

        "total": f"""{comparism_table['query']} select count(*) as count1 from wtw where 1=1 and state_id={state_id} and lga_id={lga_id}""" ,
        "collated": f"""{comparism_table['query']} select count(*) as count1 from wtw where 1=1 and (status='collated' OR status='canceled') and state_id={state_id} and lga_id={lga_id}""",
        "non_collated": f"""{comparism_table['query']} select count(*) as count1 from wtw where 1=1 and status='non collated' and state_id={state_id} and lga_id={lga_id}""",
        "canceled": f"""{comparism_table['query']} select count(*) as count1 from wtw where 1=1 and status='canceled' and state_id={state_id} and lga_id={lga_id}""",
    },



    "others":{

        "LGA STATUS": f"""{comparism_table['query']}   select case when status='collated' then 'collated'  when status='non collated' then 'non collated' WHEN status='canceled' THEN 'canceled'
 ELSE 'check manually' end  as message from lga_result_table where 1=1 and state_id={state_id} and lga_id={lga_id}   """,

    "comparism_table": f"""{comparism_table['query']}
        
   select cc.row_num, cc.party,cc.Scores AS pu_result,ccw.Scores AS ward_result,ccl.Scores AS lga_result from compare_lga cc 
   	left join compare_lgaw ccw on ccw.party=cc.party and ccw.state_id=cc.state_id and ccw.lga_id=cc.lga_id
   		left join compare_lgal ccl on ccl.party=ccw.party and ccl.state_id=ccw.state_id and ccl.lga_id=ccw.lga_id
         		 where 1=1 and ccl.state_id={state_id} and ccl.lga_id={lga_id} ORDER BY cc.party
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
        for key, val in conditions_country['ward'].items():
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
                    ward[key] = val_results                      
                else :
                    time.sleep(0.005)
            if len(ward) ==len(execute_queries):
                break

        ward_final['TOTAL WARD'] = ward['total']
        ward_final['COLLATED WARD'] = ward['collated']
        ward_final['NON-COLLATED WARD'] = ward['non_collated']
        ward_final['CANCELLED WARD'] = ward['canceled']  
      
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
        final_results['Ward'] = ward_final
        final_results['others'] = others

        return final_results


          