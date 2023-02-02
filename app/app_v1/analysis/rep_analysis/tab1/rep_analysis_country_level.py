


from app.app_v1.database import get_db,get_db2
from app.app_v1.analysis.rep_analysis.tab1.party_table import presidential_table_state
import json

parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')



# QUERIES
conditions_country = {
    "collation_status": f"""{presidential_table_state['query']} select case when status='collated' then 'collated'  when status='non collated' then 'non collated' when status='canceled'  then 'canceled' 
 else 'check manually' end AS count1 from REP_CONSTITUENCY_TABLE""",
    "over_voting_status": f"""{presidential_table_state['query']}select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  count1 from rept""",
    "over_voting_figure": f"""{presidential_table_state['query']} select  over_vote_values as count1 from rept""",
   
    "total_registered_voters": f"""{presidential_table_state['query']} SELECT Total_Registered_voters as count1  from rept""",
    "total_accredited_voters": f"""{presidential_table_state['query']} SELECT Total_Accredited_voters as count1  from rept""",
    "total_rejected_votes": f"""{presidential_table_state['query']} SELECT Total_Rejected_votes  as count1 from rept """,
    "total_valid_votes": f"""{presidential_table_state['query']} SELECT total_valid_votes as count1  from rept """,
    "total_vote_casted": f"""{presidential_table_state['query']} SELECT total_vote_casted as count1 from rept""",
    "percentage_voters_turnout": f"""{presidential_table_state['query']} SELECT percentage_voters_turnout as count1  from rept""",
    "party_graph":f"""{presidential_table_state['query']}  SELECT ROW_NUMBER() OVER(PARTITION BY constituency_name ORDER BY votes DESC) AS row_num,party,votes,	
         IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),IFF(remarks ='canceled', 'Canceled' ,'Collation has not started...'))   as percentage_votes_casted FROM win_d """
}







#  country result table
def get_country_country_all_results(state_name="undefined",constituency_name="undefined"):
    with get_db2() as conn:
        cur = conn.cursor()
        state_query = ""
        district_query = ""
        
     
        final_results = {}
        if state_name and state_name != "undefined":
            state_query = f" AND state_id ={state_name}"
        if constituency_name and constituency_name != "undefined":
            district_query = f" AND const_id ={constituency_name}" 
       
        key_values = []
        execute_queries = []
        if state_name:
            for key, val in conditions_country.items():
                val += f" WHERE 1=1 {state_query} {district_query} "

                execute_queries.append(val)
                key_values.append(key)

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
          
        #         cur.execute(val)
        #         val_results = cur.fetch_pandas_all()
        #         val_results = val_results.to_json(orient="records")
        #         val_results = json.loads(val_results)
        #         # if val_results:
        #         #     print(key_values[index])
        #         final_results[key_values[index]] = val_results
        #     return final_results
        # except Exception as e:
        #     print(e)
        #     return str(e)
