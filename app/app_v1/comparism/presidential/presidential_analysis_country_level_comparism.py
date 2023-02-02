from app.app_v1.comparism.presidential.party_comparism  import comparism_table
from app.app_v1.database import get_db2
import json
import time
def get_country_country_all_results(state_id):
       with get_db2() as conn:
        cur = conn.cursor()
        conditions_country = {






    "polling_unit": {

        "total": f"""{comparism_table['query']} select count(*) as count1 from pu """ ,
        "collated": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and (status='collated' OR status='canceled')""",
        "non_collated": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and status='non collated'""",
        "canceled": f"""{comparism_table['query']} select count(*) as count1 from pu where 1=1 and status='canceled'""",
        
    },

    "ward":{

        "total": f"""{comparism_table['query']} select count(*) as count1 from wtw """ ,
        "collated": f"""{comparism_table['query']} select count(*) as count1 from wtw where 1=1 and (status='collated' OR status='canceled')""",
        "non_collated": f"""{comparism_table['query']} select count(*) as count1 from wtw where 1=1 and status='non collated'""",
        "canceled": f"""{comparism_table['query']} select count(*) as count1 from wtw where 1=1 and status='canceled'""",
    },

    "lga": {

        "total": f"""{comparism_table['query']} select count(*) as count1 from lgatl """ ,
        "collated": f"""{comparism_table['query']} select count(*) as count1 from lgatl where 1=1 and (status='collated' OR status='canceled')""",
        "non_collated": f"""{comparism_table['query']} select count(*) as count1 from lgatl where 1=1 and status='non collated'""",
        "canceled": f"""{comparism_table['query']} select count(*) as count1 from lgatl where 1=1 and status='canceled'""",
        
    },

    "state": {

        "total": f"""{comparism_table['query']} select count(*) as count1 from sts """ ,
        "collated": f"""{comparism_table['query']} select count(*) as count1 from sts where 1=1 and (status='collated' OR status='canceled')""",
        "non_collated": f"""{comparism_table['query']} select count(*) as count1 from sts where 1=1 and status='non collated'""",
        "canceled": f"""{comparism_table['query']} select count(*) as count1 from sts where 1=1 and status='canceled'""",
        
    },

    "others":{

        "COUNTRY STATUS": f"""{comparism_table['query']}  select case when status='collated' then 'collated'  when status='non collated' then 'non collated' WHEN status='canceled' THEN 'canceled'
        ELSE 'check manually' end as message from country_result_table crt  """,

    "comparism_table": f"""{comparism_table['query']}
        
    select cc.row_num, cc.party,cc.Scores AS pu_result,ccw.Scores AS ward_result,ccl.Scores AS lga_result,ccs.Scores AS state_result,ccc.Scores AS country_result  from compare_country cc
   	    left join compare_countryw ccw on ccw.party=cc.party
        left join compare_countryl ccl on ccl.party=cc.party
        		left join compare_countrys ccs on ccs.party=cc.party
       			left join compare_countryc ccc on ccc.party=cc.party ORDER BY cc.party
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
        state={}
        others = {}
        desired_order_list = ['total','collated', 'non_collated', 'canceled']
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
        for key, val in conditions_country['lga'].items():
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
                    lga[key] = val_results                      
                else :
                    time.sleep(0.005)
            if len(lga) ==len(execute_queries):
                break
        lga_final['TOTAL LGA'] = lga['total']
        lga_final['COLLATED LGA'] = lga['collated']
        lga_final['NON-COLLATED LGA'] = lga['non_collated']
        lga_final['CANCELLED LGA'] = lga['canceled']
        print('lga')
        key_values = []
        execute_queries = []
        query =[]
        for key, val in conditions_country['state'].items():
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
                    state[key] = val_results                      
                else :
                    time.sleep(0.005)
            if len(state) ==len(execute_queries):
                break
        state_final['TOTAL STATE'] = state['total']
        state_final['COLLATED STATE'] = state['collated']
        state_final['NON-COLLATED STATE'] = state['non_collated']
        state_final['CANCELLED STATE'] = state['canceled']   
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
        final_results['Lga'] = lga_final
        final_results['State'] = state_final
        final_results['others'] = others

        return final_results


          