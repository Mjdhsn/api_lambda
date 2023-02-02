from app.app_v1.database import get_db,get_db2
from app.app_v1.results.presidential_results.partytable import presidential_table_state
import json


parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')


# QUERIES
conditions_state = {
    "total": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1 FROM st""",
    "total_registered_votes_table": f"""{presidential_table_state['query']} select state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM st""",
    "total_registered_votes": f"""{presidential_table_state['query']} SELECT COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st""",
    'canceled': f"""{presidential_table_state['query']} SELECT count(*) as  count1 FROM st where status ='canceled'""",  
    "canceled_table": f"""{presidential_table_state['query']} SELECT state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM st where status ='canceled' """,
    "total_registered_canceled_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st where status ='canceled' """ ,   
    "collated": f"""{presidential_table_state['query']}  SELECT COUNT(*) as  count1 FROM st WHERE  (status = 'collated' OR status='canceled')""",
    "collated_table": f"""{presidential_table_state['query']} SELECT state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM st WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1   FROM st WHERE status='non collated'""",
    "un_collated_table":f"""{presidential_table_state['query']} SELECT state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM st WHERE status='non collated'""",
    "total_registered_uncollated_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st WHERE status='non collated'""",
    
}


# QUERIES
conditions_country = {
    "total": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1 FROM st""",
    "total_registered_votes_table": f"""{presidential_table_state['query']} select country_name,state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM st""",
    "total_registered_votes": f"""{presidential_table_state['query']} SELECT COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st """,
    'canceled': f"""{presidential_table_state['query']} SELECT count(*) as  count1 FROM st where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_state['query']} SELECT country_name,state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM st where status ='canceled' """,
    "total_registered_canceled_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st where status ='canceled' """ ,   
    "collated": f"""{presidential_table_state['query']}  SELECT COUNT(*) as  count1 FROM st WHERE  (status = 'collated' OR status='canceled')""",
    "collated_table": f"""{presidential_table_state['query']} SELECT country_name, state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM st WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1   FROM st WHERE status='non collated'""",
    "un_collated_table":f"""{presidential_table_state['query']} SELECT country_name,state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM st WHERE status='non collated'""",
    "total_registered_uncollated_votes": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0)  as  count1 FROM st WHERE status='non collated'""",
    
}

where_list = ["total","total_registered_votes_table","total_registered_votes"]

table_list = ["total_registered_votes_table","canceled_table","collated_table", "un_collated_table"]




# state results

def get_state_state_all_results(country_name="undefined",state_name="undefined",party_data={}):
       with get_db2() as conn:
        cur = conn.cursor()

         
        country_query = ""
        state_query = ""
      

     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id ={country_name}"
        if state_name and state_name != "undefined":
            state_query = f" AND state_id={state_name}"
       
        final_parties = []
        input_parties = []
        for key, value in party_data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)

        state_result = f"""{presidential_table_state['query']} select state_name,  {parties},  Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from st where 1=1 {state_query} """

        key_values = []
        execute_queries = []
        if country_name or state_name:
            for key, val in conditions_state.items():
                
                if key in where_list:
                    val += f" where 1=1 {state_query}"
                else:
                    val += f" and 1=1 {state_query}"

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

        

        try:
            
            for index, val in enumerate(execute_queries):
                

                name_val = []
                total_val = []
                other_val = []

                cur.execute(val)
                # val_results = cur.fetchall()
                val_results = cur.fetch_pandas_all()
                val_results = val_results.to_json(orient="records")
                val_results = json.loads(val_results)

                if key_values[index] in table_list:
                    res = {}
                    if val_results:
                        for val in val_results:
                            filterByKey = lambda keys: {x: val[x] for x in keys}
                            names = filterByKey(map1)
                            total =  filterByKey(map2)
                            other =  filterByKey(map3)
                            name_val.append(names)
                            total_val.append(total)
                            other_val.append(other)
                        res['names'] = name_val
                        res['total'] = total_val
                        res['other'] = other_val
                        val = [res]
                        final_results[key_values[index]] = val
                    
                    else:
                        res['names'] = {}
                        res['total'] = {}
                        res['other'] = {}
                        val = [res]
                        final_results[key_values[index]] = val


                elif key_values[index] in result:
                    res = {}

                    filterByKey = lambda keys: {x: val_results[0][x] for x in keys}
                    names = filterByKey(mapres)
                    total =  filterByKey(map2)
                    party = filterByKey(final_parties)
                    res['names'] = names
                    res['parties'] = party
                    res['total'] = total
                   
                    val = [res]
                    final_results[key_values[index]] = val

                
                    # 
                else:
                    final_results[key_values[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)

#  country result table


def get_state_country_all_results(country_name,party_data={}):
      with get_db2() as conn:
        cur = conn.cursor()
           
        country_query = ""     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id ={country_name}"
      
        final_parties = []
        input_parties = []
        for key, value in party_data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)

        country_result = f"""{presidential_table_state['query']} select country_name,  {parties},  Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from ct where 1=1 """
        key_values = []
        execute_queries = []
        if country_name:
            for key, val in conditions_country.items():
                
                if key in where_list:
                    val += f" where 1=1"
                else:
                    val += f" and 1=1"

                execute_queries.append(val)
                key_values.append(key)
        execute_queries.append(country_result)
        key_values.append("result")

        map1 = ['COUNTRY_NAME','STATE_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        result = ['result']
        mapres = ['COUNTRY_NAME']
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
        

        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

                cur.execute(val)
                # val_results = cur.fetchall()
                val_results = cur.fetch_pandas_all()
                val_results = val_results.to_json(orient="records")
                val_results = json.loads(val_results)

                if key_values[index] in table_list:
                    res = {}
                    if val_results:
                        for val in val_results:
                            filterByKey = lambda keys: {x: val[x] for x in keys}
                            names = filterByKey(map1)
                            total =  filterByKey(map2)
                            other =  filterByKey(map3)
                            name_val.append(names)
                            total_val.append(total)
                            other_val.append(other)
                        res['names'] = name_val
                        res['total'] = total_val
                        res['other'] = other_val
                        val = [res]
                        final_results[key_values[index]] = val
                    
                    else:
                        res['names'] = {}
                        res['total'] = {}
                        res['other'] = {}
                        val = [res]
                        final_results[key_values[index]] = val



                elif key_values[index] in result:
                    res = {}
                    filterByKey = lambda keys: {x: val_results[0][x] for x in keys}
                    names = filterByKey(mapres)
                    total =  filterByKey(map2)
                    party = filterByKey(final_parties)
                    res['names'] = names
                    res['parties'] = party
                    res['total'] = total
                   
                    val = [res]
                    final_results[key_values[index]] = val

                
                    # 
                else:
                    final_results[key_values[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)


        




