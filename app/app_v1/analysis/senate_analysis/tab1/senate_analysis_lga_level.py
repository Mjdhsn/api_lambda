from app.app_v1.database import get_db,get_db2
from app.app_v1.analysis.senate_analysis.tab1.party_table import presidential_table_lga
import json





parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')

where_list = ['canceled',"canceled_table","total_registered_canceled_voters","collated_table","total_registered_collated_voters","un_collated","un_collated_table","total_registered_uncollated_voters","Over_voting","Over_voting_table","total_over_voting_table"]

# QUERIES
conditions_lga = {
    "total": f"""{presidential_table_lga['query']} SELECT COUNT(*) as  count1 FROM lgat""",
    "total_registered_votes_table": f"""{presidential_table_lga['query']} select  lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat""",
    "total_registered_votes": f"""{presidential_table_lga['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat""",
    'canceled': f"""{presidential_table_lga['query']} SELECT count(*) as  count1 FROM lgat where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_lga['query']} SELECT lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status ='canceled' """,
    "total_registered_canceled_voters": f"""{presidential_table_lga['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status ='canceled' """ ,   
    'collated': f"""{presidential_table_lga['query']}   SELECT sum(case when status = 'collated' OR status = 'canceled' then 1 else 0 end) as  count1 FROM lgat""",
    "collated_table": f"""{presidential_table_lga['query']} SELECT  lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM lgat WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{presidential_table_lga['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_lga['query']} SELECT COUNT(*) as  count1   FROM lgat where status='non collated'""",
    "un_collated_table":f"""{presidential_table_lga['query']} SELECT  lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status='non collated'""",
    "total_registered_uncollated_voters": f"""{presidential_table_lga['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status='non collated'""",
    "total_registered_voters": f"""{presidential_table_lga['query']} SELECT Total_Registered_voters  FROM lgat""",
    "total_accredited_voters": f"""{presidential_table_lga['query']} SELECT Total_Accredited_voters  from lgat""",
    "total_rejected_votes": f"""{presidential_table_lga['query']} SELECT Total_Rejected_votes   from lgat """,
    "total_valid_votes": f"""{presidential_table_lga['query']} SELECT total_valid_votes  from lgat """,
    "total_vote_casted": f"""{presidential_table_lga['query']} SELECT total_vote_casted  from lgat""",
    "percentage_voters_turnout": f"""{presidential_table_lga['query']} SELECT percentage_voters_turnout  from lgat""",
    "over_voting": f"""{presidential_table_lga['query']} SELECT count(*) as count1 FROM lgat WHERE over_vote_values>0""",
    "over_voting_table":f"""{presidential_table_lga['query']} SELECT lga_name,over_vote_values,remarks,percentage_voters_turnout  FROM lgat WHERE over_vote_values>0""",
    "total_over_voting": f"""{presidential_table_lga['query']} select sum(over_vote_values) as over_votes_figuers FROM lgat WHERE over_vote_values>0""",
    "party_graph":f"""{presidential_table_lga['query']} SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num,party,votes,	
         IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),IFF(remarks ='canceled', 'Canceled' ,'Collation has not started...'))   as percentage_votes_casted FROM win_l """
}



# QUERIES
conditions_state = {
    "total": f"""{presidential_table_lga['query']} SELECT COUNT(*) as  count1 FROM lgat""",
    "total_registered_votes_table": f"""{presidential_table_lga['query']} select district_name, lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat""",
    "total_registered_votes": f"""{presidential_table_lga['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat""",
    'canceled': f"""{presidential_table_lga['query']} SELECT count(*) as  count1 FROM lgat where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_lga['query']} SELECT district_name,lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status ='canceled' """,
    "total_registered_canceled_voters": f"""{presidential_table_lga['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status ='canceled' """ ,   
    'collated': f"""{presidential_table_lga['query']}   SELECT sum(case when status = 'collated' OR status = 'canceled' then 1 else 0 end) as  count1 FROM lgat""",
    "collated_table": f"""{presidential_table_lga['query']} SELECT  district_name, lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM lgat WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{presidential_table_lga['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_lga['query']} SELECT COUNT(*) as  count1   FROM lgat where status='non collated'""",
    "un_collated_table":f"""{presidential_table_lga['query']} SELECT  district_name,lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status='non collated'""",
    "total_registered_uncollated_voters": f"""{presidential_table_lga['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status='non collated'""",
    "total_registered_voters": f"""{presidential_table_lga['query']} SELECT Total_Registered_voters  from dist""",
    "total_accredited_voters": f"""{presidential_table_lga['query']} SELECT Total_Accredited_voters  from dist""",
    "total_rejected_votes": f"""{presidential_table_lga['query']} SELECT Total_Rejected_votes   from dist """,
    "total_valid_votes": f"""{presidential_table_lga['query']} SELECT total_valid_votes  from dist """,
    "total_vote_casted": f"""{presidential_table_lga['query']} SELECT total_vote_casted  from dist""",
    "percentage_voters_turnout": f"""{presidential_table_lga['query']} SELECT percentage_voters_turnout  from dist""",
    "over_voting": f"""{presidential_table_lga['query']} SELECT count(*) as count1 FROM lgat WHERE over_vote_values>0""",
    "over_voting_table":f"""{presidential_table_lga['query']} SELECT district_name,lga_name, over_vote_values,remarks,percentage_voters_turnout  FROM lgat WHERE over_vote_values>0""",
    "total_over_voting": f"""{presidential_table_lga['query']} select sum(over_vote_values) as over_votes_figuers FROM lgat WHERE over_vote_values>0""",
    "party_graph":f"""{presidential_table_lga['query']} SELECT ROW_NUMBER() OVER(PARTITION BY district_name ORDER BY votes DESC) AS row_num,party,votes,	
         IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),IFF(remarks ='canceled', 'Canceled' ,'Collation has not started...'))   as percentage_votes_casted FROM win_d """
}




parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')


where_list = ['canceled',"canceled_table","total_registered_canceled_voters","collated_table","total_registered_collated_voters","un_collated","un_collated_table","total_registered_uncollated_voters","over_voting","over_voting_table","total_over_voting"]

table_list = ["total_registered_votes_table","canceled_table","collated_table", "un_collated_table","over_voting_table"]




# lga results

def get_lga_lga_all_results(state_name="undefined",district_name="undefined",lga_name="undefined"):
    with get_db2() as conn:
        cur = conn.cursor()
        state_query = ""
        district_query = ""
        lga_query = ""
      
     
        final_results = {}
        if state_name and state_name != "undefined":
            state_query = f" AND state_id ={state_name}"
        if district_name and district_name != "undefined":
            district_query = f" AND district_id={district_name}"
        if lga_name and lga_name != "undefined":
            lga_query = f" AND lga_id='{lga_name}'"
 
        key_values = []
        execute_queries = []
        if state_name or district_name or lga_name:
            for key, val in conditions_lga.items():
                if key in where_list:
                    val += f" AND 1=1 {state_query} {district_query} {lga_query}"
                else:
                    val += f" WHERE 1=1 {state_query} {district_query} {lga_query}"

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

 

# state results
def get_lga_state_all_results(state_name="undefined",district_name="undefined"):
    with get_db2() as conn:
        cur = conn.cursor()
        state_query = ""
        district_query = ""
  

     
        final_results = {}
        if state_name and state_name != "undefined":
            state_query = f" AND state_id ={state_name}"
        if district_name and district_name != "undefined":
            district_query = f" AND district_id={district_name}"
    
        
        key_values = []
        execute_queries = []
        if state_name or district_name :
            for key, val in conditions_state.items():
                if key in where_list:
                    val += f" AND 1=1 {state_query} {district_query}"
                else:
                    val += f" WHERE 1=1 {state_query} {district_query}"

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

              
     