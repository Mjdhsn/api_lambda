from app.app_v1.database import get_db,get_db2
from app.app_v1.results.senate_results.partytable import presidential_table_state
import json


parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')


# QUERIES
conditions_state = {
    "total": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1 FROM dist""",
    "total_registered_votes_table": f"""{presidential_table_state['query']} select state_name, district_name,Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM sen_district_table""",
    "total_registered_votes": f"""{presidential_table_state['query']} SELECT COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM dist""",
    'canceled': f"""{presidential_table_state['query']} SELECT count(*) as  count1 FROM dist where status ='canceled'""",  
    "canceled_table": f"""{presidential_table_state['query']} SELECT state_name,district_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM dist where status ='canceled' """,
    "total_registered_canceled_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM dist where status ='canceled' """ ,   
    "collated": f"""{presidential_table_state['query']}  SELECT COUNT(*) as  count1 FROM dist WHERE  (status = 'collated' OR status='canceled')""",
    "collated_table": f"""{presidential_table_state['query']} SELECT state_name,district_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM dist WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM dist where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1   FROM dist WHERE status='non collated'""",
    "un_collated_table":f"""{presidential_table_state['query']} SELECT state_name,district_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM dist WHERE status='non collated'""",
    "total_registered_uncollated_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM dist WHERE status='non collated'""",
    
}



where_list = ["total","total_registered_votes_table","total_registered_votes"]

table_list = ["total_registered_votes_table","canceled_table","collated_table", "un_collated_table"]




# state results

def get_state_state_all_results(country_name="undefined",state_name="undefined",district_name="undefined",party_data={}):
       with get_db2() as conn:
        cur = conn.cursor()

         
        country_query = ""
        state_query = ""
        district_query =""

      

     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id ={country_name}"
        if state_name and state_name != "undefined":
            state_query = f" AND state_id={state_name}"
        if district_name and district_name != "undefined":
            district_query = f" AND district_id={district_name}"
       
        final_parties = []
        input_parties = []
        for key, value in party_data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)

        state_result = f"""{presidential_table_state['query']} select state_name,  {parties},  Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from st where 1=1 {state_query} {district_query} """

        key_values = []
        execute_queries = []
        if country_name or state_name:
            for key, val in conditions_state.items():
                
                if key in where_list:
                    val += f" where 1=1 {state_query} {district_query}"
                else:
                    val += f" and 1=1 {state_query} {district_query}"

                execute_queries.append(val)
                key_values.append(key)
        execute_queries.append(state_result)
        key_values.append("result")

        map1 = ['STATE_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        result = ['result']
        mapres =['STATE_NAME']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
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
                    ress[key] = val_results                      
                else :
                    time.sleep(0.03)
            if len(ress) ==len(execute_queries):
                break
        return ress

        

       