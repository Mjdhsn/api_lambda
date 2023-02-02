
from app.app_v1.database import get_db2
from app.app_v1.analysis.presidential_analysis.tab2_copy.party_table import presidential_table_country


import pandas as pd
import json







#  country result table
def get_country_country_all_results(country_name="undefined",data={}):
    with get_db2() as conn:
        cur = conn.cursor()
        party_name = data['party_name']
        # typo = data['level']
        conditions_country_country = {

    "country": {

        "values": {

            "collation_status": f"""{presidential_table_country['query']}  select (case when status='collated' then 'collated'  when status='non collated' then 'non coalated' when status='canceled' then 'canceled' else 'check manually' end ) AS collation_status from country_result_table  """,
            "over_voting_status": f"""{presidential_table_country['query']} select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  over_voting_status FROM ct   """,
            "total_over_vote_figure": f"""{presidential_table_country['query']}  select  over_vote_values as Total_over_vote_figures FROM ct	 """,
            "total_registered_votes": f"""{presidential_table_country['query']} SELECT Total_Registered_voters  FROM ct  """,
            "total_accredited_votes": f"""{presidential_table_country['query']}  SELECT Total_Accredited_voters  FROM ct  """,
            "total_rejected_votes": f"""{presidential_table_country['query']} select Total_Rejected_votes   FROM ct """,
            "total_valid_votes": f"""{presidential_table_country['query']}  SELECT total_valid_votes  FROM ct  """,
            "total_votes_casted": f"""{presidential_table_country['query']} SELECT total_vote_casted  FROM ct """,
            "percentage_voters_turnout": f"""{presidential_table_country['query']}  SELECT percentage_voters_turnout  FROM ct  """,
            "general_party_performance": f"""{presidential_table_country['query']}

           select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 			IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_country
 			order by current_update desc limit 1 ; """

        },

        "tables": {

        "general_party_performance": f"""{presidential_table_country['query']}
    	SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
        iff(total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Voting in progress...') as percentage_score FROM win_country 
 		 """

        }
    },

}       
        final_results = {}
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []


        for key1, val in conditions_country_country.items():
            if key1 == 'values':
                for key2,value in val.items():
                    execute_queries_values.append(value)
                    key_values.append(key2)
            elif key1 =='tables':
                for key2,value in val.items():
                    execute_queries_tables.append(value)
                    key_values_table.append(key2)
            else:
                pass

    
        ress = {}
        ress_table ={}
        valu = {}
        tab = {}
        query = []
        complete = []
        query2 = []
        complete2 = []
        queries = len(conditions_country_country['values'])
        queries2 = len(conditions_country_country['tables'])
        import time
        for item in range(queries):
            query.append(item)
        for item in range(queries2):
            query2.append(item)
       

    
        for qry in query:
            sql = execute_queries_values[qry]
            try:
                cur.execute_async(sql)
                query[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
        for qry in query2:
            sql = execute_queries_tables[qry]
            # print(sql)
            try:
                cur.execute_async(sql)
                query2[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')

        while True:

            for result in query:
                status = conn.get_query_status(result)
                if str(status) == 'QueryStatus.SUCCESS':

                    
                    index = query.index(result)
                    key = key_values[index]
                    cur.get_results_from_sfqid(result)
                    recs = cur.fetchone()
                    ret = {"count1":recs}
                    # res = ret.to_json(orient="records")
                    # parsed = json.loads(res)
                    ress[key] = recs                      
                else :
                    time.sleep(0.03)
            if len(ress) ==queries:
                break
            
        while True:
            for result in query2:
                status = conn.get_query_status(result)
                if str(status) == 'QueryStatus.SUCCESS':
                    # print(key_values_table)
                    index = query2.index(result)
                    key = key_values_table[index]
                    cur.get_results_from_sfqid(result)
                    recs = cur.fetch_pandas_all()
                    res = recs.to_json(orient="records")
                    parsed = json.loads(res)
                    ress_table[key] = parsed                        
                else :
                    time.sleep(0.03)
            
            if len(ress_table) ==queries2 :
                break
            


        
        
        # print(ress.keys(),len(ress))
        valu['values'] = ress
    




        # print(ress_table.keys(),len(ress_table))
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)

        conn.close()
        return final_results

