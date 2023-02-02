from app.app_v1.database import get_db2

from app.app_v1.analysis.presidential_analysis.tab2_copy.party_table import presidential_table_pu


import pandas as pd
import json
from app.app_v1.analysis.presidential_analysis.tab2_copy.party_table import presidential_table_ward


#  ward results

def get_ward_ward_all_results(country_name="undefined",state_name="undefined", lga_name="undefined",ward_name="undefined",data={}):
        with get_db2() as conn:
            # global state_name,lga_name,ward_name,pu_name
            cur = conn.cursor()
            party_name = data['party_name']
            
            conditions_ward_ward = {

    "ward": {

        "values": {

            "collation_status": f"""{presidential_table_ward['query']}  select (case when status='collated' then 'collated'  when status='non collated' then 'non coalated' when status='canceled' then 'canceled' else 'check manually' end ) AS collation_status from ward_result_table where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and ward_id={ward_name} """,
            "over_voting_status": f"""{presidential_table_ward['query']} select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  over_voting_status FROM  wt  where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and ward_id={ward_name} """,
            "total_over_vote_figure": f"""{presidential_table_ward['query']}  select  over_vote_values as Total_over_vote_figures FROM  wt	where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and ward_id={ward_name} """,
            "total_registered_votes": f"""{presidential_table_ward['query']} SELECT Total_Registered_voters  FROM  wt where  1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and ward_id={ward_name}""",
            "total_accredited_votes": f"""{presidential_table_ward['query']}  SELECT Total_Accredited_voters  FROM  wt where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and ward_id={ward_name}""",
            "total_rejected_votes": f"""{presidential_table_ward['query']} select Total_Rejected_votes   FROM  wt where  1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name}and ward_id={ward_name}""",
            "total_valid_votes": f"""{presidential_table_ward['query']}  SELECT total_valid_votes  FROM  wt where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and ward_id={ward_name}""",
            "total_votes_casted": f"""{presidential_table_ward['query']} SELECT total_vote_casted  FROM  wt where  1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name}and ward_id={ward_name}""",
            "percentage_voters_turnout": f"""{presidential_table_ward['query']}  SELECT percentage_voters_turnout  FROM  wt where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and ward_id={ward_name}""",
            "general_party_performance": f"""{presidential_table_ward['query']}

           select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 			IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_ward where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name}
 			order by current_update desc limit 1 ; """

        },

        "tables": {

        "general_party_performance": f"""{presidential_table_ward['query']}
    	SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
        iff(total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Voting in progress...') as percentage_score FROM win_ward 
 		where 1=1 and state_id={state_name} and lga_id={lga_name} and ward_id={ward_name}"""

        }
    },

}
            final_results = {}

        
            key_values = []
            key_values_table = []
            execute_queries_values = []
            execute_queries_tables = []

    
            for key1, val in conditions_ward_ward.items():
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
            queries = len(conditions_ward_ward['values'])
            queries2 = len(conditions_ward_ward['tables'])
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

                
        

    # lga results
def get_ward_lga_all_results(country_name="undefined",state_name="undefined",lga_name="undefined",data={}):
        with get_db2() as conn:
            # global state_name,lga_name,ward_name,pu_name
            cur = conn.cursor()
            party_name = data['party_name']
    
            conditions_lga_ward = {

    "ward": {

        "values": {

            "total": f"""{presidential_table_ward['query']}  select  count(*) as count1 from ward_result_table where  1=1 and state_id={state_name} and lga_id={lga_name}""",
            "total_collated": f"""{presidential_table_ward['query']} select COALESCE(sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) ,0) as count1  FROM wt where  1=1 and state_id={state_name} and lga_id={lga_name}""",
            "total_non_collated": f"""{presidential_table_ward['query']}  select COALESCE(sum(case when status = 'non collated'  then 1 else  0 end) ,0) as count1  FROM wt where  1=1 and state_id={state_name} and lga_id={lga_name}""",
            "total_canceled": f"""{presidential_table_ward['query']}  select COALESCE(sum(case when status = 'canceled'  then 1 else  0 end) ,0) as count1  FROM wt where  1=1 and state_id={state_name} and lga_id={lga_name}""",
            "total_over_voting": f"""{presidential_table_ward['query']}  select count(*) as count1 FROM wt where 1=1 and over_vote_values>0 and state_id={state_name} and lga_id={lga_name}""",

            "number_clear_win": f"""{presidential_table_ward['query']}   select count(*) as count1 from win_ward where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",
            "number_win_with_doubt": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_ward where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",
            "number_of_clear_loss": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_ward where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",
            "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_ward where 1=1 and party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",
            "above_clearly_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 FROM wt where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name} and lga_id={lga_name}""",
            "above_with_doubt_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 FROM wt where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name} and lga_id={lga_name}""",
            "general_party_performance": f"""{presidential_table_ward['query']}

           select IFF(row_num < 2 and total_valid_votes > 0 and remarks='OK' AND party='{party_name}', 'clear leading',
 		IFF(row_num < 2 and total_valid_votes > 0 and over_vote_values > 0 AND party='{party_name}', 'leading with doubt',
 			IFF(row_num > 1 and total_valid_votes > 0 and over_vote_values > 0 AND party='{party_name}', 'lagging with doubt',
 			IFF(row_num > 1 and total_valid_votes > 0 and remarks='OK' AND party='{party_name}', 'clear lagging', ''))))
 			as current_update from win_lga order by current_update desc limit 1 """

        },

        "tables": {

            "total": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name, Total_Registered_voters FROM wt where  1=1 and state_id={state_name} and lga_id={lga_name}""",
            "total_collated": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name, {party_name} AS scores,total_vote_casted, remarks   FROM wt where 1=1 and  status = 'collated' OR status='canceled' and state_id={state_name} and lga_id={lga_name}""",
            "total_non_collated": f"""{presidential_table_ward['query']}   select state_name,lga_name, ward_name, Total_Registered_voters, remarks   FROM wt where 1=1 and status='non collated' and state_id={state_name} and lga_id={lga_name}""",
            "total_canceled": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,Total_Registered_voters, remarks   FROM wt where 1=1 and status = 'canceled' and state_id={state_name} and lga_id={lga_name}""",
            "total_over_voting": f"""{presidential_table_ward['query']} select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  FROM wt where  1=1 and state_id={state_name} and lga_id={lga_name}""",

            "number_clear_win": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",

            "number_win_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",

            "number_of_clear_loss": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",

            "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} and lga_id={lga_name}""",

            "above_clearly_25": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
FROM wt where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name} and lga_id={lga_name}""",

            "above_with_doubt_25": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
FROM wt where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name} and lga_id={lga_name}""",

            "general_party_performance": f"""{presidential_table_ward['query']}
    	 select ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num, party, votes as Scores,
       IFF(total_vote_casted > 0, concat(round(votes/total_vote_casted*100, 2), '%'), 'Collation has not started') as percentage_score FROM win_lga
          where 1 = 1 and state_id={state_name} and lga_id={lga_name}"""

        }
    },

}

        final_results = {}
        polling_results= {}

        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []


        for key1, val in conditions_lga_ward['ward'].items():
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
        queries = len(conditions_lga_ward['ward']['values'])
        queries2 = len(conditions_lga_ward['ward']['tables'])
        for item in range(queries):
            query.append(item)
            complete.append(0)
        for item in range(queries2):
            query2.append(item)
            complete2.append(0)
        import time
        
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
                    time.sleep(0.02)
            

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
                    time.sleep(0.02)
            if len(ress) ==queries and len(ress_table) ==queries2 :
                break
            

        
        
        print(ress.keys(),len(ress))
        valu['values'] = ress
       



        allresults ={}
        # print(ress_table.keys(),len(ress_table))
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)

        conn.close()
        allresults['ward'] = final_results
    
        return allresults
        
# state results
def get_ward_state_all_results(country_name="undefined",state_name="undefined",data={}):
    with get_db2() as conn:
        # global state_name,lga_name,ward_name,pu_name
        cur = conn.cursor()
        party_name = data['party_name']
        typo = data['level']
  
        conditions_state = {

    "ward": {

        "values": {

            "total": f"""{presidential_table_ward['query']}  select  count(*) as count1 from ward_result_table where  1=1 and state_id={state_name} """,
            "total_collated": f"""{presidential_table_ward['query']} select COALESCE(sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) ,0) as count1  FROM wt where  1=1 and state_id={state_name} """,
            "total_non_collated": f"""{presidential_table_ward['query']}  select COALESCE(sum(case when status = 'non collated'  then 1 else  0 end) ,0) as count1  FROM wt where  1=1 and state_id={state_name} """,
            "total_canceled": f"""{presidential_table_ward['query']}  select COALESCE(sum(case when status = 'canceled'  then 1 else  0 end) ,0) as count1  FROM wt where  1=1 and state_id={state_name} """,
            "total_over_voting": f"""{presidential_table_ward['query']}  select count(*) as count1 FROM wt where 1=1 and over_vote_values>0 and state_id={state_name} """,

            "number_clear_win": f"""{presidential_table_ward['query']}   select count(*) as count1 from win_ward where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,
            "number_win_with_doubt": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_ward where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} """,
            "number_of_clear_loss": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_ward where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,
            "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_ward where 1=1 and party='{party_name}' and state_id={state_name} """,
            "above_clearly_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 FROM wt where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """,
            "above_with_doubt_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 FROM wt where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """,


        },

        "tables": {

            "total": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name, Total_Registered_voters FROM wt where  1=1 and state_id={state_name} """,
            "total_collated": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name, {party_name} AS scores,total_vote_casted, remarks   FROM wt where 1=1 and  status = 'collated' OR status='canceled' and state_id={state_name} """,
            "total_non_collated": f"""{presidential_table_ward['query']}   select state_name,lga_name, ward_name, Total_Registered_voters, remarks   FROM wt where 1=1 and status='non collated' and state_id={state_name} """,
            "total_canceled": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,Total_Registered_voters, remarks   FROM wt where 1=1 and status = 'canceled' and state_id={state_name} """,
            "total_over_voting": f"""{presidential_table_ward['query']} select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  FROM wt where  1=1 and state_id={state_name} """,

            "number_clear_win": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,

            "number_win_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} """,

            "number_of_clear_loss": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,

            "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} """,

            "above_clearly_25": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
FROM wt where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """,

            "above_with_doubt_25": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
FROM wt where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """



        }
    },

        "lga": {

            "values": {
                "total": f"""{presidential_table_ward['query']}  select  count(*) as count1 from lgat where 1=1 and state_id={state_name} """,
                "total_collated": f"""{presidential_table_ward['query']}   select count(*) as collated from collated_lga where 1=1 and deef=0 and state_id={state_name} """,
                "total_non_collated": f"""{presidential_table_ward['query']}  select count(*)  from non_collated_lga  where 1=1 and total=0 and state_id={state_name} """,
                "total_canceled": f"""{presidential_table_ward['query']}  select count(*) as in_progress from collated_lga where 1=1 and (deef>0 and deef<total) and state_id={state_name} """,
                "total_over_voting": f"""{presidential_table_ward['query']}  select count(*) as count1 from lgat where 1=1 and over_vote_values>0 and state_id={state_name} """,
                "number_clear_win": f"""{presidential_table_ward['query']}   select count(*) as count1 from win_lga where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,
                "number_win_with_doubt": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_lga where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} """,
                "number_of_clear_loss": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_lga where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,
                "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_lga where 1=1 and party='{party_name}' and state_id={state_name} """,
                "above_clearly_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 from lgat where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """,
                "above_with_doubt_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 from lgat where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """,
                "general_party_performance": f"""{presidential_table_ward['query']}

           select IFF(row_num < 2 and total_valid_votes > 0 and remarks='OK' AND party='{party_name}', 'clear leading',
 		IFF(row_num < 2 and total_valid_votes > 0 and over_vote_values > 0 AND party='{party_name}', 'leading with doubt',
 			IFF(row_num > 1 and total_valid_votes > 0 and over_vote_values > 0 AND party='{party_name}', 'lagging with doubt',
 			IFF(row_num > 1 and total_valid_votes > 0 and remarks='OK' AND party='{party_name}', 'clear lagging', ''))))
 			as current_update from win_state order by current_update desc limit 1 """
            },

            "tables": {

                "total": f"""{presidential_table_ward['query']}  select state_name,lga_name,Total_Registered_voters from lgat and deef<total) and state_id={state_name} """,
                "total_collated": f"""{presidential_table_ward['query']}  select state_name,lga_name,Total_Registered_voters from collated_lga where 1=1 and deef=0 and state_id={state_name} """,
                "total_non_collated": f"""{presidential_table_ward['query']}  select state_name,lga_name,Total_Registered_voters  from non_collated_lga  where 1=1 and total=0 and state_id={state_name} """,
                "total_canceled": f"""{presidential_table_ward['query']}   select state_name,lga_name,Total_Registered_voters from collated_lga where 1=1 and (deef>0 and deef<total) and state_id={state_name} """,
                "total_over_voting": f"""{presidential_table_ward['query']} select state_name,lga_name,   {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from lgat where 1=1 and state_id={state_name} """,
                "number_clear_win": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,

                "number_win_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} """,

                "number_of_clear_loss": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_id={state_name} """,

                "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_id={state_name} """,

                "above_clearly_25": f"""{presidential_table_ward['query']}  select state_name,lga_name,   {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from lgat where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """,

                "above_with_doubt_25": f"""{presidential_table_ward['query']}  select state_name,lga_name,   {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from lgat where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name} """,

                "general_party_performance": f"""{presidential_table_ward['query']}
    	 select ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num, party, votes as Scores,
       IFF(total_vote_casted > 0, concat(round(votes/total_vote_casted*100, 2), '%'), 'Collation has not started') as percentage_score FROM win_state
          where 1 = 1 and state_id={state_name} """

            }
        },

    }



        final_results = {}

        allresults= {}
        
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []

        
        if typo =='ward':
            queries = len(conditions_state['ward']['values'])
            queries2 = len(conditions_state['ward']['tables'])

            for key1, val in conditions_state['ward'].items():
                

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
        elif typo =='lga':
            queries = len(conditions_state['lga']['values'])
            queries2 = len(conditions_state['lga']['tables'])

            for key1, val in conditions_state['lga'].items():
                

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
        
        for item in range(queries):
            query.append(item)
            complete.append(0)
        for item in range(queries2):
            query2.append(item)
            complete2.append(0)
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
                    time.sleep(0.02)

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
                    time.sleep(0.02)
            if len(ress) ==queries and len(ress_table) ==queries2 :
                break
            

        
    
        valu['values'] = ress
       



        allresults ={}
        # print(ress_table.keys(),len(ress_table))
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)

        conn.close()
        # allresults['polling_unit'] = final_results
        if typo == 'ward':
            allresults['ward'] = final_results
            return allresults
        elif typo == 'lga':
            allresults['lga'] = final_results
            return allresults


   
#  country result table
def get_ward_country_all_results(country_name="undefined",data={}):
    with get_db2() as conn:
        cur = conn.cursor()
        party_name = data['party_name']
        typo = data['level']


        conditions_country = {

    "ward": {

        "values": {

            "total": f"""{presidential_table_ward['query']}  select  count(*) as count1 from ward_result_table """,
            "total_collated": f"""{presidential_table_ward['query']} select COALESCE(sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) ,0) as count1  FROM wt """,
            "total_non_collated": f"""{presidential_table_ward['query']}  select COALESCE(sum(case when status = 'non collated'  then 1 else  0 end) ,0) as count1  FROM wt """,
            "total_canceled": f"""{presidential_table_ward['query']}  select COALESCE(sum(case when status = 'canceled'  then 1 else  0 end) ,0) as count1  FROM wt """,
            "total_over_voting": f"""{presidential_table_ward['query']}  select count(*) as count1 FROM wt where 1=1 and over_vote_values>0 """,

            "number_clear_win": f"""{presidential_table_ward['query']}   select count(*) as count1 FROM win_ward where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,
            "number_win_with_doubt": f"""{presidential_table_ward['query']}  select count(*) as count1 FROM win_ward where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' """,
            "number_of_clear_loss": f"""{presidential_table_ward['query']}  select count(*) as count1 FROM win_ward where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,
            "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 FROM win_ward where 1=1 and party='{party_name}' """,
            "above_clearly_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 FROM wt where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,
            "above_with_doubt_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 FROM wt where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,


        },

        "tables": {

            "total": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name, Total_Registered_voters FROM wt """,
            "total_collated": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name, {party_name} AS scores,total_vote_casted, remarks   FROM wt where 1=1 and  status = 'collated' OR status='canceled' """,
            "total_non_collated": f"""{presidential_table_ward['query']}   select state_name,lga_name, ward_name, Total_Registered_voters, remarks   FROM wt where 1=1 and status='non collated'  """,
            "total_canceled": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,Total_Registered_voters, remarks   FROM wt where 1=1 and status = 'canceled'  """,
            "total_over_voting": f"""{presidential_table_ward['query']} select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  FROM wt """,

            "number_clear_win": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes FROM win_ward 
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,

            "number_win_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks FROM win_ward 
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' """,

            "number_of_clear_loss": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes FROM win_ward 
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,

            "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks FROM win_ward 
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' """,

            "above_clearly_25": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
FROM wt where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,

            "above_with_doubt_25": f"""{presidential_table_ward['query']}  select state_name,lga_name, ward_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
FROM wt where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """



        }
    },

  
        "lga": {

            "values": {
                "total": f"""{presidential_table_ward['query']}  select  count(*) as count1 from lgat """,
                "total_collated": f"""{presidential_table_ward['query']}   select count(*) as collated from collated_lga where 1=1 and deef=0 """,
                "total_non_collated": f"""{presidential_table_ward['query']}  select count(*)  from non_collated_lga  where 1=1 and total=0 """,
                "total_canceled": f"""{presidential_table_ward['query']}  select count(*) as in_progress from collated_lga where 1=1 and (deef>0 and deef<total) """,
                "total_over_voting": f"""{presidential_table_ward['query']}  select count(*) as count1 from lgat where 1=1 and over_vote_values>0 and  """,
                "number_clear_win": f"""{presidential_table_ward['query']}   select count(*) as count1 from win_lga where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,
                "number_win_with_doubt": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_lga where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' """,
                "number_of_clear_loss": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_lga where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,
                "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_lga where 1=1 and party='{party_name}' """,
                "above_clearly_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 from lgat where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,
                "above_with_doubt_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 from lgat where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,

            },

            "tables": {

                "total": f"""{presidential_table_ward['query']}  select state_name,lga_name,Total_Registered_voters from lgat  """,
                "total_collated": f"""{presidential_table_ward['query']}  select state_name,lga_name,Total_Registered_voters from collated_lga where 1=1 and deef=0 """,
                "total_non_collated": f"""{presidential_table_ward['query']}  select state_name,lga_name,Total_Registered_voters  from non_collated_lga  where 1=1 and total=0 """,
                "total_canceled": f"""{presidential_table_ward['query']}   select state_name,lga_name,Total_Registered_voters from collated_lga where 1=1 and (deef>0 and deef<total) """,
                "total_over_voting": f"""{presidential_table_ward['query']} select state_name,lga_name,   {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from lgat """,

                "number_clear_win": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,

                "number_win_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga 
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' """,

                "number_of_clear_loss": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' """,

                "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}  select state_name,lga_name,   votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga 
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' """,

                "above_clearly_25": f"""{presidential_table_ward['query']}  select state_name,lga_name,   {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from lgat where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,

                "above_with_doubt_25": f"""{presidential_table_ward['query']}  select state_name,lga_name,   {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from lgat where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,

            }
        },

        "state": {

            "values": {

                "total": f"""{presidential_table_ward['query']}  select  count(*) as count1 from st """,
                "total_collated": f"""{presidential_table_ward['query']}   select count(*) as collated from collated_st where 1=1 and deef=0  """,
                "total_non_collated": f"""{presidential_table_ward['query']}  select count(*)  from non_collated_st  where 1=1 and total=0   """,
                "total_canceled": f"""{presidential_table_ward['query']}  select count(*) as in_progress from collated_st where 1=1 and (deef>0 and deef<total)   """,
                "total_over_voting": f"""{presidential_table_ward['query']}  select count(*) as count1 from st where 1=1 and over_vote_values>0  """,
                "number_clear_win": f"""{presidential_table_ward['query']}   select count(*) as count1 from win_state where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
                "number_win_with_doubt": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_state where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
                "number_of_clear_loss": f"""{presidential_table_ward['query']}  select count(*) as count1 from win_state where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
                "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_state where 1=1 and party='{party_name}'   """,
                "above_clearly_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 from st where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25   """,
                "above_with_doubt_25": f"""{presidential_table_ward['query']}  select COUNT(*) AS count1 from lgat where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,
                "general_party_performance": f"""{presidential_table_ward['query']}  
      	   
           select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 		IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_country order by current_update desc limit 1 """

            },

            "tables": {

                "total": f"""{presidential_table_ward['query']}  select state_name,Total_Registered_voters from st  """,
                "total_collated": f"""{presidential_table_ward['query']}  select state_name,Total_Registered_voters from collated_state where 1=1 and deef=0  """,
                "total_non_collated": f"""{presidential_table_ward['query']}  select state_name,Total_Registered_voters  from non_collated_state  where 1=1 and total=0    """,
                "total_canceled": f"""{presidential_table_ward['query']}   select state_name,Total_Registered_voters from collated_state where 1=1 and (deef>0 and deef<total)   """,
                "total_over_voting": f"""{presidential_table_ward['query']} select state_name,  {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from st where 1=1  """,
                "number_clear_win": f"""{presidential_table_ward['query']}  select state_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,

                "number_win_with_doubt": f"""{presidential_table_ward['query']}  select state_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_state 
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'  """,

                "number_of_clear_loss": f"""{presidential_table_ward['query']}  select state_name,  votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'  """,

                "number_of_loss_with_doubt": f"""{presidential_table_ward['query']}  select state_name,  votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_state 
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'  """,

                "above_clearly_25": f"""{presidential_table_ward['query']}  select state_name,  = AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND('NNPP'/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from st where 1=1 and remarks='OK' and {party_name}/total_vote_casted*100>=25 and state_id={state_name}  """,

                "above_with_doubt_25": f"""{presidential_table_ward['query']}  select state_name,  F AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from st where 1=1 and over_vote_values>0 and {party_name}/total_vote_casted*100>=25   """,

                "general_party_performance": f"""{presidential_table_ward['query']}  
    	 select ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		
       IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Collation has not started') as percentage_score FROM win_country 
          """

            }
        }
    }


        final_results = {}

        allresults= {}
        
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []

    
        if typo =='ward':
            queries = len(conditions_country['ward']['values'])
            queries2 = len(conditions_country['ward']['tables'])

            for key1, val in conditions_country['ward'].items():
                

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
        elif typo =='lga':
            queries = len(conditions_country['lga']['values'])
            queries2 = len(conditions_country['lga']['tables'])

            for key1, val in conditions_country['lga'].items():
                

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
        elif typo =='state':
            queries = len(conditions_country['state']['values'])
            queries2 = len(conditions_country['state']['tables'])

            for key1, val in conditions_country['state'].items():
                

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
        
        for item in range(queries):
            query.append(item)
            complete.append(0)
        for item in range(queries2):
            query2.append(item)
            complete2.append(0)
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
                    time.sleep(0.02)

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
                    time.sleep(0.02)
      
            if len(ress) ==queries and len(ress_table) ==queries2 :

                break
            

        
        
        valu['values'] = ress
       



        allresults ={}
        # print(ress_table.keys(),len(ress_table))
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)

        conn.close()
    

        if typo == 'ward':
            allresults['ward'] = final_results
            return allresults
        elif typo == 'lga':
            allresults['lga'] = final_results
            return allresults
        elif typo == 'state':
            allresults['state'] = final_results
            return allresults
