from app.app_v1.database import get_db

from app.app_v1.analysis.presidential_analysis.tab2.schema import *





state_name= None
lga_name = None 
ward_name= None 
pu_name= None
party_name =None



# QUERIES
conditions_pu = {
    "values":{
    "registered_voters": f"""{ward_query['query']} SELECT Total_Registered_voters as count1 from  wt""",
     "accredited_voters": f"""{ward_query['query']}  SELECT Total_Accredited_voters as count1 from wt """,
     "rejected_votes": f"""{ward_query['query']} SELECT Total_Rejected_votes as count1  from wt """,
     "valid_votes": f"""{ward_query['query']}  SELECT total_valid_votes as count1 from wt""",
     "vote_casted": f"""{ward_query['query']} SELECT total_vote_casted as count1 from wt """,
     "percentage_voters_turnout": f"""{ward_query['query']} SELECT percentage_voters_turnout as count1 from wt """,
    
    "collation_status": f"""{ward_query['query']} 
     select case when status='collated' then 'collated'  when status='non collated' then 'non coalated' else 'canceled' end 
 as count1 from ward_result_table""",
    
     "over_voting_status":f"""{ward_query['query']} select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  count1 from wt """,
     "over_voting_values": f"""{ward_query['query']} select  over_vote_values as count1 from wt""",
     "general_party_performance": f"""{ward_query['query']} 
       select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_pu
 			 order by current_update desc limit 1
    
    
    # """
    },

    "table":{

    "general_party_performance_table": f"""{ward_query['query']}         
    SELECT ROW_NUMBER() OVER(PARTITION BY pu_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_pu """,


    }
    

}

# print(conditions_pu['values']['registered_voters'])
# QUERIES
conditions_lga = {

    "ward":{

        "values": {
        
    "total": f"""{ward_query['query']} SELECT  count(*) from ward_result_table where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    "total_collated": f"""{ward_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
    "total_non_collated": f"""{ward_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
    "total_canceled": f"""{ward_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",  
    "total_over_voting": f"""{ward_query['query']} select count(*) from wt where over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""" ,   
    "number_clear_win": f"""{ward_query['query']}  select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name} and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    "number_win_with_doubt": f"""{ward_query['query']} select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    "number_of_clear_loss": f"""{ward_query['query']} select count(*) from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    "number_of_loss_with_doubt": f"""{ward_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu WHERE party="{party_name}" and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    "above_clearly_25":f"""{ward_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    "above_with_doubt_25": f"""{ward_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    "general_party_performance": f"""{ward_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party={party_name},'lagging with doubt',
			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
			as current_update from win_ward where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'
 			order by current_update desc limit 1"""
    
    },

    "tables": {

        "total": f"""{ward_query['query']} select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from wt where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        "total_collated": f"""{ward_query['query']} SELECT pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from wt WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        "total_non_collated": f"""{ward_query['query']}  SELECT pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status='non collated' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        "total_canceled": f"""{ward_query['query']} SELECT pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,  
        "canceled_table": f"""{ward_query['query']} SELECT pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        "total_over_voting": f"""{ward_query['query']}select pu_code, pu_name, ZLP as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
  from wt where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""" ,   
        "number_clear_win": f"""{ward_query['query']} select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,

        "number_win_with_doubt": f"""{ward_query['query']} select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
      
        "number_of_clear_loss": f"""{ward_query['query']} select pu_code, pu_name, votes as Scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",

        "number_of_loss_with_doubt": f"""{ward_query['query']} select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        
        "above_clearly_25":f"""{ward_query['query']} SELECT pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
from wt where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        
        "above_with_doubt_25": f"""{ward_query['query']} SELECT pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
from wt where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
        "general_party_performance": f"""{ward_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward 
where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """

    }
    }
}

# QUERIES
conditions_lga = {
    "total": f"""{values['wt']} SELECT COUNT(*) as  count1 FROM wt""",
    "total_registered_votes_table": f"""{values['wt']} select lga_name, ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt""",
    "total_registered_votes": f"""{values['wt']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt""",
    "canceled": f"""{values['wt']} SELECT count(*) as  count1 FROM wt where status ="canceled" """,  
    "canceled_table": f"""{values['wt']} SELECT lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where status ='canceled' """ ,   
    "collated": f"""{values['wt']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM wt""",
    "collated_table": f"""{values['wt']} SELECT  lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM wt WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['wt']} SELECT COUNT(*) as  count1   FROM wt where status='non collated'""",
    "un_collated_table":f"""{values['wt']} SELECT  lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where status='non collated'""",
    "total_registered_voters": f"""{values['lgat']} SELECT Total_Registered_voters  FROM lgat""",
    "total_accredited_voters": f"""{values['lgat']} SELECT Total_Accredited_voters  from lgat""",
    "total_rejected_votes": f"""{values['lgat']} SELECT Total_Rejected_votes   from lgat """,
    "total_valid_votes": f"""{values['lgat']} SELECT total_valid_votes  from lgat """,
    "total_vote_casted": f"""{values['lgat']} SELECT total_vote_casted  from lgat""",
    "percentage_voters_turnout": f"""{values['lgat']} SELECT percentage_voters_turnout  from lgat""",
    "over_voting": f"""{values['wt']} SELECT count(*)  as count1 FROM wt WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['wt']} SELECT lga_name,ward_name,over_vote_values,remarks,percentage_voters_turnout  FROM wt WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['wt']} select sum(over_vote_values) as over_votes_figuers FROM wt WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_l']} SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_l """
}



# QUERIES
conditions_state = {
    "total": f"""{values['wt']} SELECT COUNT(*) as  count1 FROM wt""",
    "total_registered_votes_table": f"""{values['wt']} select state_name,lga_name, ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt""",
    "total_registered_votes": f"""{values['wt']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt""",
    "canceled": f"""{values['wt']} SELECT count(*) as  count1 FROM wt where status ="canceled" """,  
    "canceled_table": f"""{values['wt']} SELECT state_name,lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where status ='canceled' """ ,   
    "collated": f"""{values['wt']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM wt""",
    "collated_table": f"""{values['wt']} SELECT  state_name,lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM wt WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['wt']} SELECT COUNT(*) as  count1   FROM wt where status='non collated'""",
    "un_collated_table":f"""{values['wt']} SELECT  state_name,lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where status='non collated'""",
    "total_registered_voters": f"""{values['st']} SELECT Total_Registered_voters  FROM st""",
    "total_accredited_voters": f"""{values['st']} SELECT Total_Accredited_voters  from st""",
    "total_rejected_votes": f"""{values['st']} SELECT Total_Rejected_votes   from st """,
    "total_valid_votes": f"""{values['st']} SELECT total_valid_votes  from st """,
    "total_vote_casted": f"""{values['st']} SELECT total_vote_casted  from st""",
    "percentage_voters_turnout": f"""{values['st']} SELECT percentage_voters_turnout  from st""",
    "over_voting": f"""{values['wt']} SELECT count(*)  as count1 FROM wt WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['wt']} SELECT state_name,lga_name,ward_name,over_vote_values,remarks,percentage_voters_turnout  FROM wt WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['wt']} select sum(over_vote_values) as over_votes_figuers FROM wt WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_s']} SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_s """
}

# QUERIES
conditions_country = {
    "total": f"""{values['wt']} SELECT COUNT(*) as  count1 FROM wt""",
    "total_registered_votes_table": f"""{values['wt']} select state_name,lga_name, ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt""",
    "total_registered_votes": f"""{values['wt']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt""",
    "canceled": f"""{values['wt']} SELECT count(*) as  count1 FROM wt where status ="canceled" """,  
    "canceled_table": f"""{values['wt']} SELECT state_name,lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where status ='canceled' """ ,   
    "collated": f"""{values['wt']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM wt""",
    "collated_table": f"""{values['wt']} SELECT  state_name,lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM wt WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['wt']} SELECT COUNT(*) as  count1   FROM wt where status='non collated'""",
    "un_collated_table":f"""{values['wt']} SELECT  state_name,lga_name,ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM wt where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['wt']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM wt where status='non collated'""",
    "total_registered_voters": f"""{values['ct']} SELECT Total_Registered_voters  FROM ct""",
    "total_accredited_voters": f"""{values['ct']} SELECT Total_Accredited_voters  from ct""",
    "total_rejected_votes": f"""{values['ct']} SELECT Total_Rejected_votes   from ct """,
    "total_valid_votes": f"""{values['ct']} SELECT total_valid_votes  from ct """,
    "total_vote_casted": f"""{values['ct']} SELECT total_vote_casted  from ct""",
  "percentage_voters_turnout": f"""{values['ct']} SELECT percentage_voters_turnout  from ct""",
    "over_voting": f"""{values['wt']} SELECT count(*)  as count1 FROM wt WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['wt']} SELECT state_name,lga_name,ward_name,over_vote_values,remarks,percentage_voters_turnout  FROM wt WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['wt']} select sum(over_vote_values) as over_votes_figuers FROM wt WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_c']} SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_c """
}


parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')






#  ward results

def get_ward_ward_all_results(country_name="undefined",state_name="undefined", lga_name="undefined",ward_name="undefined"):
    with get_db() as conn:
        cur = conn.cursor()

        country_query = ""
        state_query = ""
        lga_query = ""
        ward_query = ""

     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_name ='{country_name}'"
        if state_name and state_name != "undefined":
            state_query = f" AND state_name='{state_name}'"
        if lga_name and lga_name != "undefined":
            lga_query = f" AND lga_name='{lga_name}'"
        if ward_name and ward_name != "undefined":
            ward_query = f" AND ward_name='{ward_name}'"
       
        
        key_values = []
        execute_queries = []
        if country_name or state_name or lga_name or ward_name :
            for key, val in conditions_ward.items():
                if key in where_list:
                    val += f" AND 1=1 {state_query} {lga_query} {ward_query}"
                else:
                    val += f" WHERE 1=1 {state_query} {lga_query} {ward_query}"

                execute_queries.append(val)
                key_values.append(key)

        map1 = ['ward_name']
        map2 = ['Total_Registered_voters','Total_Accredited_voters','Total_Rejected_votes']
        map3 = ['remarks']
        map4 =['over_vote_values','percentage_voters_turnout']
                 
        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

          
                cur.execute(val)
                val_results = cur.fetchall()
                if key_values[index] in table_list:
                    res = {}

                    if val_results:

                        if key_values[index] =='over_voting_table':
                            for val in val_results:
                                filterByKey = lambda keys: {x: val[x] for x in keys}
                                names = filterByKey(map1)
                                total =  filterByKey(map4)
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
                
                else:
                    final_results[key_values[index]] = val_results
          
            return final_results
        except Exception as e:
            print(e)
            return str(e)
            
      



# lga results

def get_ward_lga_all_results(country_name="undefined",state_name="undefined",lga_name="undefined"):
    with get_db() as conn:
        cur = conn.cursor()
        country_query = ""
        state_query = ""
        lga_query = ""
      
     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_name ='{country_name}'"
        if state_name and state_name != "undefined":
            state_query = f" AND state_name='{state_name}'"
        if lga_name and lga_name != "undefined":
            lga_query = f" AND lga_name='{lga_name}'"
 
        key_values = []
        execute_queries = []
        if country_name or state_name or lga_name:
            for key, val in conditions_lga.items():
                if key in where_list:
                    val += f" AND 1=1 {state_query} {lga_query}"
                else:
                    val += f" WHERE 1=1 {state_query} {lga_query}"

                execute_queries.append(val)
                key_values.append(key)
            
           
            
        
        map1 = ['lga_name','ward_name']
        map2 = ['Total_Registered_voters','Total_Accredited_voters','Total_Rejected_votes']
        map3 = ['remarks']
        map4 =['over_vote_values','percentage_voters_turnout']
              
                  
        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

          
                cur.execute(val)
                val_results = cur.fetchall()
                if key_values[index] in table_list:
                    res = {}

                    if val_results:

                        if key_values[index] =='over_voting_table':
                            for val in val_results:
                                filterByKey = lambda keys: {x: val[x] for x in keys}
                                names = filterByKey(map1)
                                total =  filterByKey(map4)
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
                
                else:
                    final_results[key_values[index]] = val_results
          
            return final_results
        except Exception as e:
            print(e)
            return str(e)


# state results
def get_ward_state_all_results(country_name="undefined",state_name="undefined"):
    with get_db() as conn:
        cur = conn.cursor()
        country_query = ""
        state_query = ""
  

     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_name ='{country_name}'"
        if state_name and state_name != "undefined":
            state_query = f" AND state_name='{state_name}'"
    
        
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
            
                
                
            
        map1 = ['state_name','lga_name','ward_name']
        map2 = ['Total_Registered_voters','Total_Accredited_voters','Total_Rejected_votes']
        map3 = ['remarks']
        map4 =['over_vote_values','percentage_voters_turnout']
              
                  
        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

          
                cur.execute(val)
                val_results = cur.fetchall()
                if key_values[index] in table_list:
                    res = {}

                    if val_results:

                        if key_values[index] =='over_voting_table':
                            for val in val_results:
                                filterByKey = lambda keys: {x: val[x] for x in keys}
                                names = filterByKey(map1)
                                total =  filterByKey(map4)
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
                
                else:
                    final_results[key_values[index]] = val_results
          
            return final_results
        except Exception as e:
            print(e)
            return str(e)


#  country result table
def get_ward_country_all_results(country_name="undefined"):
    with get_db() as conn:
        cur = conn.cursor()
        country_query = ""
        
     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_name ='{country_name}'"
       
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
            
            
          
        map1 = ['state_name','lga_name','ward_name']
        map2 = ['Total_Registered_voters','Total_Accredited_voters','Total_Rejected_votes']
        map3 = ['remarks']
        map4 =['over_vote_values','percentage_voters_turnout']
              
                  
        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

          
                cur.execute(val)
                val_results = cur.fetchall()
                if key_values[index] in table_list:
                    res = {}

                    if val_results:

                        if key_values[index] =='over_voting_table':
                            for val in val_results:
                                filterByKey = lambda keys: {x: val[x] for x in keys}
                                names = filterByKey(map1)
                                total =  filterByKey(map4)
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
                
                else:
                    final_results[key_values[index]] = val_results
          
            return final_results
        except Exception as e:
            print(e)
            return str(e)

    
