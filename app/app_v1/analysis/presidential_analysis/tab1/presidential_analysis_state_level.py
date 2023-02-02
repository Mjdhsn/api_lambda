

from app.app_v1.database import get_db,get_db2
from app.app_v1.analysis.presidential_analysis.tab1.party_table import presidential_table_state
import json





# QUERIES




# QUERIES
conditions_state = {
    "total": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1 FROM  st""",
    "total_registered_votes_table": f"""{presidential_table_state['query']} select  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st""",
    "total_registered_votes": f"""{presidential_table_state['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st""",
    'canceled': f"""{presidential_table_state['query']} SELECT count(*) as  count1 FROM  st where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_state['query']} SELECT state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status ='canceled' """,
    "total_registered_canceled_voters": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status ='canceled' """ ,   
    'collated': f"""{presidential_table_state['query']}   SELECT sum(case when status = 'collated' OR status = 'canceled' then 1 else 0 end) as  count1 FROM  st""",
    "collated_table": f"""{presidential_table_state['query']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM  st WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1   FROM  st where status='non collated'""",
    "un_collated_table":f"""{presidential_table_state['query']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status='non collated'""",
    "total_registered_uncollated_voters": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status='non collated'""",
    "total_registered_voters": f"""{presidential_table_state['query']} SELECT Total_Registered_voters  FROM st""",
    "total_accredited_voters": f"""{presidential_table_state['query']} SELECT Total_Accredited_voters  from st""",
    "total_rejected_votes": f"""{presidential_table_state['query']} SELECT Total_Rejected_votes   from st """,
    "total_valid_votes": f"""{presidential_table_state['query']} SELECT total_valid_votes  from st """,
    "total_vote_casted": f"""{presidential_table_state['query']} SELECT total_vote_casted  from st""",
    "percentage_voters_turnout": f"""{presidential_table_state['query']} SELECT percentage_voters_turnout  from st""",
    "over_voting": f"""{presidential_table_state['query']} SELECT count(*) as count1 FROM  st WHERE over_vote_values>0""",
    "over_voting_table":f"""{presidential_table_state['query']} SELECT state_name,over_vote_values,remarks,percentage_voters_turnout  FROM  st WHERE over_vote_values>0""",
    "total_over_voting": f"""{presidential_table_state['query']} select sum(over_vote_values) as over_votes_figuers FROM  st WHERE over_vote_values>0""",
    "party_graph":f"""{presidential_table_state['query']} SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num,party,votes,	
         IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),IFF(remarks ='canceled', 'Canceled' ,'Collation has not started...'))  as percentage_votes_casted FROM win_s """
}

# QUERIES
conditions_country = {
    "total": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1 FROM  st""",
    "total_registered_votes_table": f"""{presidential_table_state['query']} select  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st""",
    "total_registered_votes": f"""{presidential_table_state['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st""",
    'canceled': f"""{presidential_table_state['query']} SELECT count(*) as  count1 FROM  st where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_state['query']} SELECT state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status ='canceled' """,
    "total_registered_canceled_voters": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status ='canceled' """ ,   
    'collated': f"""{presidential_table_state['query']}   SELECT sum(case when status = 'collated' OR status = 'canceled' then 1 else 0 end) as  count1 FROM  st""",
    "collated_table": f"""{presidential_table_state['query']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM  st WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_state['query']} SELECT COUNT(*) as  count1   FROM  st where status='non collated'""",
    "un_collated_table":f"""{presidential_table_state['query']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status='non collated'""",
    "total_registered_uncollated_voters": f"""{presidential_table_state['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status='non collated'""",
    "total_registered_voters": f"""{presidential_table_state['query']} SELECT Total_Registered_voters  FROM ct""",
    "total_accredited_voters": f"""{presidential_table_state['query']} SELECT Total_Accredited_voters  from ct""",
    "total_rejected_votes": f"""{presidential_table_state['query']} SELECT Total_Rejected_votes   from ct """,
    "total_valid_votes": f"""{presidential_table_state['query']} SELECT total_valid_votes  from ct """,
    "total_vote_casted": f"""{presidential_table_state['query']} SELECT total_vote_casted  from ct""",
    "percentage_voters_turnout": f"""{presidential_table_state['query']} SELECT percentage_voters_turnout  from ct""",
    "over_voting": f"""{presidential_table_state['query']} SELECT count(*) as count1 FROM  st WHERE over_vote_values>0""",
    "over_voting_table":f"""{presidential_table_state['query']} SELECT state_name,over_vote_values,remarks,percentage_voters_turnout  FROM  st WHERE over_vote_values>0""",
    "total_over_voting": f"""{presidential_table_state['query']} select sum(over_vote_values) as over_votes_figuers FROM  st WHERE over_vote_values>0""",
    "party_graph":f"""{presidential_table_state['query']} SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes,	
         IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),IFF(remarks ='canceled', 'Canceled' ,'Collation has not started...'))  as percentage_votes_casted FROM win_c """
}






where_list = ['canceled',"canceled_table","total_registered_canceled_voters","collated_table","total_registered_collated_voters","un_collated","un_collated_table","total_registered_uncollated_voters","over_voting","over_voting_table","total_over_voting"]



table_list = ["total_registered_votes_table","canceled_table","collated_table", "un_collated_table","over_voting_table"]






# state results
def get_state_state_all_results(country_name="undefined",state_name="undefined"):
    with get_db2() as conn:
        cur = conn.cursor()
        country_query = ""
        state_query = ""
  

     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id ={country_name}"
        if state_name and state_name != "undefined":
            state_query = f" AND state_id={state_name}"
    
        
        key_values = []
        execute_queries = []
        if country_name or state_name :
            for key, val in conditions_state.items():
                if key in where_list:
                    val += f" AND 1=1 {state_query}"
                else:
                    val += f" WHERE 1=1 {state_query}"

                execute_queries.append(val)
                key_values.append(key)
            
                
                
            
            
        map1 = ['STATE_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        map4 =['OVER_VOTE_VALUES','PERCENTAGE_VOTERS_TURNOUT']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
        try:
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
        except Exception as e:
            print(e)
            return str(e)

              
                 
        # try:
            
        #     for index, val in enumerate(execute_queries):
        #         name_val = []
        #         total_val = []
        #         other_val = []

          
        #         cur.execute(val)
        #         val_results = cur.fetch_pandas_all()
        #         val_results = val_results.to_json(orient="records")
        #         val_results = json.loads(val_results)
        #         if key_values[index] in table_list:
        #             res = {}

        #             if val_results:

        #                 if key_values[index] =='over_voting_table':
        #                     for val in val_results:
        #                         filterByKey = lambda keys: {x: val[x] for x in keys}
        #                         names = filterByKey(map1)
        #                         total =  filterByKey(map4)
        #                         other =  filterByKey(map3)
        #                         name_val.append(names)
        #                         total_val.append(total)
        #                         other_val.append(other)
        #                     res['names'] = name_val
        #                     res['total'] = total_val
        #                     res['other'] = other_val
        #                     val = [res]
        #                     final_results[key_values[index]] = val
        #                 else:

        #                     for val in val_results:
        #                         filterByKey = lambda keys: {x: val[x] for x in keys}
        #                         names = filterByKey(map1)
        #                         total =  filterByKey(map2)
        #                         other =  filterByKey(map3)
        #                         name_val.append(names)
        #                         total_val.append(total)
        #                         other_val.append(other)

        #                     res['names'] = name_val
        #                     res['total'] = total_val
        #                     res['other'] = other_val
        #                     val = [res]
        #                     final_results[key_values[index]] = val

        #             else:
        #                 res['names'] = {}
        #                 res['total'] = {}
        #                 res['other'] = {}
        #                 val = [res]
        #                 final_results[key_values[index]] = val
                
        #         else:
        #             final_results[key_values[index]] = val_results
          
        #     return final_results
        # except Exception as e:
        #     print(e)
        #     return str(e)

#  country result table
def get_state_country_all_results(country_name="undefined"):
    with get_db2() as conn:
        cur = conn.cursor()
        country_query = ""
        
     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id ={country_name}"
       
        key_values = []
        execute_queries = []
        if country_name:
            for key, val in conditions_country.items():
                if key in where_list:
                    val += f" AND 1=1"
                else:
                    val += f" WHERE 1=1 "

                execute_queries.append(val)
                key_values.append(key)
            
            
        map1 = ['STATE_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        map4 =['OVER_VOTE_VALUES','PERCENTAGE_VOTERS_TURNOUT']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
        try:
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
        except Exception as e:
            print(e)
            return str(e)

              
                 
        # try:
            
        #     for index, val in enumerate(execute_queries):
        #         name_val = []
        #         total_val = []
        #         other_val = []

          
        #         cur.execute(val)
        #         val_results = cur.fetch_pandas_all()
        #         val_results = val_results.to_json(orient="records")
        #         val_results = json.loads(val_results)
        #         if key_values[index] in table_list:
        #             res = {}

        #             if val_results:

        #                 if key_values[index] =='over_voting_table':
        #                     for val in val_results:
        #                         filterByKey = lambda keys: {x: val[x] for x in keys}
        #                         names = filterByKey(map1)
        #                         total =  filterByKey(map4)
        #                         other =  filterByKey(map3)
        #                         name_val.append(names)
        #                         total_val.append(total)
        #                         other_val.append(other)
        #                     res['names'] = name_val
        #                     res['total'] = total_val
        #                     res['other'] = other_val
        #                     val = [res]
        #                     final_results[key_values[index]] = val
        #                 else:

        #                     for val in val_results:
        #                         filterByKey = lambda keys: {x: val[x] for x in keys}
        #                         names = filterByKey(map1)
        #                         total =  filterByKey(map2)
        #                         other =  filterByKey(map3)
        #                         name_val.append(names)
        #                         total_val.append(total)
        #                         other_val.append(other)

        #                     res['names'] = name_val
        #                     res['total'] = total_val
        #                     res['other'] = other_val
        #                     val = [res]
        #                     final_results[key_values[index]] = val

        #             else:
        #                 res['names'] = {}
        #                 res['total'] = {}
        #                 res['other'] = {}
        #                 val = [res]
        #                 final_results[key_values[index]] = val
                
        #         else:
        #             final_results[key_values[index]] = val_results
          
        #     return final_results
        # except Exception as e:
        #     print(e)
        #     return str(e)