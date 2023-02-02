


from app.app_v1.database import get_db,get_db2
from app.app_v1.analysis.presidential_analysis.tab1.party_table import presidential_table_country
import json

parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')



# QUERIES
conditions_country = {
    "collation_status": f"""{presidential_table_country['query']} select case when status='collated' then 'collated'  when status='non collated' then 'non coalated' else 'canceled' end as 'coallation status' from country_result_table""",
    "over_voting_status": f"""{presidential_table_country['query']} select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  over_voting_status from ct""",
    "over_voting_figure": f"""{presidential_table_country['query']} select  over_vote_values as Total_over_vote_figures from ct""",
   
    "total_registered_voters": f"""{presidential_table_country['query']} SELECT Total_Registered_voters as count1  FROM ct""",
    "total_accredited_voters": f"""{presidential_table_country['query']} SELECT Total_Accredited_voters as count1  from ct""",
    "total_rejected_votes": f"""{presidential_table_country['query']} SELECT Total_Rejected_votes  as count1 from ct """,
    "total_valid_votes": f"""{presidential_table_country['query']} SELECT total_valid_votes as count1  from ct """,
    "total_vote_casted": f"""{presidential_table_country['query']} SELECT total_vote_casted as count1 from ct""",
    "percentage_voters_turnout": f"""{presidential_table_country['query']} SELECT percentage_voters_turnout as count1  from ct""",
    "party_graph":f"""{presidential_table_country['query']}  SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes,	
         IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),IFF(remarks ='canceled', 'Canceled' ,'Collation has not started...'))  as percentage_votes_casted FROM win_c """
}







#  country result table
def get_country_country_all_results(country_name="undefined"):
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
                val += f" WHERE 1=1 "

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
