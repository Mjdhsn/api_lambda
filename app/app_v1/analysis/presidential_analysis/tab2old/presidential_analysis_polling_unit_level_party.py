
from app.app_v1.database import get_db,get_db2
from app.app_v1.analysis.presidential_analysis.tab2.schema import ward_query,lga_query,state_query,country_query
from app.app_v1.analysis.presidential_analysis.tab2.schema2 import pu_query

state_name= None
lga_name = None 
ward_name= None 
pu_name= None
party_name =None
import multiprocessing
from multiprocessing import Pool
from multiprocessing import Pool, cpu_count



# conditions_lga = {

#      "polling_unit":{

#                "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from pu where over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name} and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu WHERE party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_ward where state_name= '{state_name}' and lga_name =  '{lga_name}' 
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_collated": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_non_collated": f"""{pu_query['query']}  SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_canceled": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,  
#         "canceled_table": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_over_voting": f"""{pu_query['query']}select ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
#         "number_clear_win": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
# WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}' and lga_name =  '{lga_name}'  """,

#         "number_win_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
# WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
# WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
# WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        
#         "above_clearly_25":f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from pu where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from pu where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		-- 11
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward 
# where state_name= '{state_name}' and lga_name =  '{lga_name}'  """

#     }
#     },

#     "wards":{

   
#                "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from wt where over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name} and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_ward WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_ward WHERE party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_ward where state_name= '{state_name}' and lga_name =  '{lga_name}' 
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from wt where state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_collated": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from wt WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_non_collated": f"""{pu_query['query']}  SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status='non collated' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_canceled": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,  
#         "canceled_table": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
#         "total_over_voting": f"""{pu_query['query']}select ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from wt where state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
#         "number_clear_win": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
# WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}' and lga_name =  '{lga_name}'  """,

#         "number_win_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
# WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
# WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
# WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        
#         "above_clearly_25":f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from wt where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from wt where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		-- 11
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward 
# where state_name= '{state_name}' and lga_name =  '{lga_name}'  """

#     }
#     }
# }
# conditions_state = {

#     "polling_unit":{

#           "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where state_name= '{state_name}'   """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  FROM pu WHERE state_name= '{state_name}'  """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  FROM pu WHERE state_name= '{state_name}'  """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  FROM pu WHERE state_name= '{state_name}'  """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from pu where over_vote_values>0 and state_name= '{state_name}'  """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name} and state_name= '{state_name}'   """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'   """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'   """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu WHERE party='{party_name}' and state_name= '{state_name}'   """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}'   """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}'   """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_ward where state_name= '{state_name}'  
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from pu where state_name= '{state_name}'  """,
#         "total_collated": f"""{pu_query['query']} SELECT lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}'  """,
#         "total_non_collated": f"""{pu_query['query']}  SELECT lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_name= '{state_name}'  """,
#         "total_canceled": f"""{pu_query['query']} SELECT lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_name= '{state_name}'   """,  
#         "canceled_table": f"""{pu_query['query']} SELECT lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_name= '{state_name}'  """,
#         "total_over_voting": f"""{pu_query['query']}select lga_name,ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from pu where state_name= '{state_name}'  """ ,   
#         "number_clear_win": f"""{pu_query['query']} select lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
# WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}'   """,

#         "number_win_with_doubt": f"""{pu_query['query']} select lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
# WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
# WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
# WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
        
#         "above_clearly_25":f"""{pu_query['query']} SELECT lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from pu where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}'  """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} SELECT lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from pu where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}'   """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		-- 11
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward 
# where state_name= '{state_name}'   """

    
#     }
#     },

#      "wards":{

#        "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where state_name= '{state_name}'   """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}'  """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}'  """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from wt WHERE state_name= '{state_name}'  """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from wt where over_vote_values>0 and state_name= '{state_name}'  """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name} and state_name= '{state_name}'   """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'   """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_ward WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'   """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_ward WHERE party='{party_name}' and state_name= '{state_name}'   """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}'   """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}'   """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_ward where state_name= '{state_name}'  
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from wt where state_name= '{state_name}'  """,
#         "total_collated": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from wt WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}'  """,
#         "total_non_collated": f"""{pu_query['query']}  select  lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status='non collated' and state_name= '{state_name}'  """,
#         "total_canceled": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status = 'canceled'  and state_name= '{state_name}'   """,  
#         "canceled_table": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt WHERE status = 'canceled'  and state_name= '{state_name}'  """,
#         "total_over_voting": f"""{pu_query['query']}select  lga_name,ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from wt where state_name= '{state_name}'  """ ,   
#         "number_clear_win": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
# WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}'   """,

#         "number_win_with_doubt": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
# WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
# WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
# WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
        
#         "above_clearly_25":f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from wt where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}'  """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from wt where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}'   """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		-- 11
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward 
# where state_name= '{state_name}'   """

#     }
#     },

#    "lga":{

#               "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where state_name= '{state_name}'   """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from lgat WHERE state_name= '{state_name}'  """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from lgat WHERE state_name= '{state_name}'  """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from lgat WHERE state_name= '{state_name}'  """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from lgat where over_vote_values>0 and state_name= '{state_name}'  """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_lga WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name} and state_name= '{state_name}'   """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_lga WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'   """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_lga WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'   """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_lga WHERE party='{party_name}' and state_name= '{state_name}'   """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from lgat WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}'   """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from lgat WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}'   """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_lga where state_name= '{state_name}'  
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from lgat where state_name= '{state_name}'  """,
#         "total_collated": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from lgat WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}'  """,
#         "total_non_collated": f"""{pu_query['query']}  select  lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from lgat WHERE status='non collated' and state_name= '{state_name}'  """,
#         "total_canceled": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from lgat WHERE status = 'canceled'  and state_name= '{state_name}'   """,  
#         "canceled_table": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from lgat WHERE status = 'canceled'  and state_name= '{state_name}'  """,
#         "total_over_voting": f"""{pu_query['query']}select  lga_name,ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from lgat where state_name= '{state_name}'  """ ,   
#         "number_clear_win": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
# WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}'   """,

#         "number_win_with_doubt": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga 
# WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
# WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga 
# WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
        
#         "above_clearly_25":f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from lgat where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}'  """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} select  lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from lgat where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}'   """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		-- 11
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_lga 
# where state_name= '{state_name}'   """

#     }
#     }


# }

# conditions_country = {

#     "polling_unit":{
   
#            "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where 1=1    """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  FROM pu where 1=1   """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  FROM pu where 1=1   """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  FROM pu where 1=1   """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from pu where 1=1 over_vote_values>0   """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_pu where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}   """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_pu where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_pu where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu where 1=1 party='{party_name}'    """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'   """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0    """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_ward where 1=1   
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from pu where 1=1   """,
#         "total_collated": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   FROM pu where 1=1  status = 'collated' OR status='canceled'   """,
#         "total_non_collated": f"""{pu_query['query']}  SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu where 1=1 status='non collated'   """,
#         "total_canceled": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu where 1=1 status = 'canceled'    """,  
#         "canceled_table": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu where 1=1 status = 'canceled'    """,
#         "total_over_voting": f"""{pu_query['query']}select state_name,lga_name,ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from pu where 1=1   """ ,   
#         "number_clear_win": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
# where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'     """,

#         "number_win_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
# where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
# where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
# where 1=1 row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
        
#         "above_clearly_25":f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from pu where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'  """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from pu where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0     """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_pu 
# where 1=1    """

#     }

# },

#     "ward":{
  
#     "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where 1=1    """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from wt where 1=1   """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from wt where 1=1   """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from wt where 1=1   """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from wt where 1=1 over_vote_values>0   """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_ward where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}   """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_ward where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_ward where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_ward where 1=1 party='{party_name}'    """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'   """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from wt where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0    """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_ward where 1=1   
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from wt where 1=1   """,
#         "total_collated": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from wt where 1=1  status = 'collated' OR status='canceled'   """,
#         "total_non_collated": f"""{pu_query['query']}  SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt where 1=1 status='non collated'   """,
#         "total_canceled": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt where 1=1 status = 'canceled'    """,  
#         "canceled_table": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from wt where 1=1 status = 'canceled'    """,
#         "total_over_voting": f"""{pu_query['query']}select state_name,lga_name,ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from wt where 1=1   """ ,   
#         "number_clear_win": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
# where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'  """,

#         "number_win_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
# where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
# where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
# where 1=1 row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
        
#         "above_clearly_25":f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from wt where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'  """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from wt where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0     """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward 
# where 1=1    """

# }
#     }
# ,

#     "lga":{
   
   
#            "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where 1=1    """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from lgat where 1=1   """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from lgat where 1=1   """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from lgat where 1=1   """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from lgat where 1=1 over_vote_values>0   """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_lga where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}   """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_lga where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_lga where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_lga where 1=1 party='{party_name}'    """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from lgat where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'   """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from lgat where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0    """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_lga where 1=1   
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from lgat where 1=1   """,
#         "total_collated": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from lgat where 1=1  status = 'collated' OR status='canceled'   """,
#         "total_non_collated": f"""{pu_query['query']}  SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from lgat where 1=1 status='non collated'   """,
#         "total_canceled": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from lgat where 1=1 status = 'canceled'    """,  
#         "canceled_table": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from lgat where 1=1 status = 'canceled'    """,
#         "total_over_voting": f"""{pu_query['query']}select state_name,lga_name,ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from lgat where 1=1   """ ,   
#         "number_clear_win": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
# where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'     """,

#         "number_win_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga 
# where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
# where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga 
# where 1=1 row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
        
#         "above_clearly_25":f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from lgat where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'  """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from lgat where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0     """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_lga 
# where 1=1    """
# }

#     }
# ,

#     "state":{

   
#            "values": {
        
#     "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where 1=1    """,
#     "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from st where 1=1   """,
#     "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from st where 1=1   """,
#     "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from st where 1=1   """,  
#     "total_over_voting": f"""{pu_query['query']} select count(*) from st where 1=1 over_vote_values>0   """ ,   
#     "number_clear_win": f"""{pu_query['query']}  select count(*) from win_state where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}   """,
#     "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_state where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
#     "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_state where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
#     "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_state where 1=1 party='{party_name}'    """,
#     "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from st where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'   """,
#     "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from st where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0    """,
#     "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
# 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
# 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
# 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
# 			as current_update from win_state where 1=1   
#  			order by current_update desc limit 1"""
    
#     },

#     "tables": {

#         "total": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from st where 1=1   """,
#         "total_collated": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from st where 1=1  status = 'collated' OR status='canceled'   """,
#         "total_non_collated": f"""{pu_query['query']}  SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from st where 1=1 status='non collated'   """,
#         "total_canceled": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from st where 1=1 status = 'canceled'    """,  
#         "canceled_table": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from st where 1=1 status = 'canceled'    """,
#         "total_over_voting": f"""{pu_query['query']}select state_name,lga_name,ward_name,pu_code, pu_name, ZLP as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
#   from st where 1=1   """ ,   
#         "number_clear_win": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
# where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'     """,

#         "number_win_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_state 
# where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
      
#         "number_of_clear_loss": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
# where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,

#         "number_of_loss_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_state 
# where 1=1 row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
        
#         "above_clearly_25":f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
# from st where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'  """,
        
#         "above_with_doubt_25": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
# from st where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0     """,
#         "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as scores,		
#         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_state 
# where 1=1    """

#     }

# }

# }
import pandas as pd
import json
def work_log_values(query):
     with get_db2() as conn:
        # global state_name,lga_name,ward_name,pu_name
        cur = conn.cursor()
        cur.execute(query)
        val_results = cur.fetch_pandas_all()
        return {'count1':val_results[0]}
        # else:
        #     val_results = cur.fetchall()
        #     return val_results 
def work_log_tables(query):
     with get_db2() as conn:
        # global state_name,lga_name,ward_name,pu_name
        cur = conn.cursor()
        cur.execute(query)
        val_results = cur.fetch_pandas_all() 
        res = val_results.to_json(orient="records")
        parsed = json.loads(res)
        return parsed    
        # return val_results 


def get__polling_pu_all_results(country_name="undefined",state_name="undefined", lga_name="undefined", ward_name="undefined",pu_name="undefined",data={}):
    
    with get_db2() as conn:
        # global state_name,lga_name,ward_name,pu_name
        cur = conn.cursor()
        for key, value in data.items():
            party_name = value
        # QUERIES
        conditions_pu = {
            "values":{
            "registered_voters": f"""{pu_query['query']} SELECT Total_Registered_voters as count1 FROM  pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
              "accredited_voters": f"""{pu_query['query']}  SELECT Total_Accredited_voters as count1 from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
              "rejected_votes": f"""{pu_query['query']} SELECT Total_Rejected_votes as count1  from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
              "valid_votes": f"""{pu_query['query']}  SELECT total_valid_votes as count1 from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
              "vote_casted": f"""{pu_query['query']} SELECT total_vote_casted as count1 from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
              "percentage_voters_turnout": f"""{pu_query['query']} SELECT percentage_voters_turnout as count1 from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
            
             "collation_status": f"""{pu_query['query']} 
              select case when status='collated' then 'collated'  when status='non collated' then 'non coalated' else 'canceled' end 
          as count1 from pu_result_table where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
            
              "over_voting_status":f"""{pu_query['query']} select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  count1 from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
              "over_voting_values": f"""{pu_query['query']} select  over_vote_values as count1 from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'""",
              "general_party_performance": f"""{pu_query['query']} 
                 select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party = '{party_name}','clear leading',
 			IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party = '{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party = '{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party = '{party_name}','clear lagging',''))) )
 			as current_update from win_pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' and pu_name='{pu_name}'
 			order by current_update desc limit 1 """

            
            
            
              
            },

            "tables":{

            "general_party_performance_table": f"""{pu_query['query']}     

               SELECT ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
       iff(total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Voting in progress...') as percentage_score FROM win_pu 
 		   where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'  and pu_name='{pu_name}'	"""


            }
            

        }
     
        final_results = {}

     
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []

   
        for key1, val in conditions_pu.items():
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
        queries = len(conditions_pu['values'])
        queries2 = len(conditions_pu['tables'])
        for item in range(queries):
            query.append(item)
            complete.append(0)
        for item in range(queries2):
            query2.append(item)
            complete2.append(0)
        import time
        process_complete = 0
        process_pass = 0
        process_complete2 = 0
        process_pass2 = 0
        for qry in query:
            sql = execute_queries_values[qry]
            try:
                cur.execute_async(sql)
                query[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
   
        while process_complete == 0:
            item = -1
            process_pass += 1
            if sum(complete) == queries or process_pass == 10:
                process_complete = 1
            for result in query:
                item += 1
                if complete[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete[item] = 1
                        index = query.index(result)
                        key = key_values[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetchone()
                        ret = {"count1":recs}
                        # res = ret.to_json(orient="records")
                        # parsed = json.loads(res)
                        ress[key] = recs                      
                    else :
                        time.sleep(0.10)
        valu['values'] = ress
        for qry in query2:
            sql = execute_queries_tables[qry]
            # print(sql)
            try:
                cur.execute_async(sql)
                query2[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
        while process_complete2 == 0:
            item = -1
            process_pass2 += 1
            if sum(complete2) == queries2 or process_pass2 == 10:
                process_complete2 = 1
            for result in query2:
                item += 1
                if complete2[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        print(key_values_table)
                        complete2[item] = 1
                        index = query2.index(result)
                        key = key_values_table[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetch_pandas_all()
                        res = recs.to_json(orient="records")
                        parsed = json.loads(res)
                        ress_table[key] = parsed                        
                    else :
                        time.sleep(0.10)
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)


        return final_results


    
       

#  ward results

def get__polling_ward_all_results(country_name="undefined",state_name="undefined", lga_name="undefined",ward_name="undefined",data={}):
    with get_db2() as conn:
        cur = conn.cursor()
        for key, value in data.items():
            party_name = value
        conditions_ward = {

    "polling_unit":{

        "values": {
        
       "total": f"""{pu_query['query']} SELECT  count(*) as count1 from pu_result_table where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
       "total_collated": f"""{pu_query['query']}SELECT COALESCE(sum(case when status = 'collated' OR status='canceled' then 1 else  0 end),0) as count1  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
       "total_non_collated": f"""{pu_query['query']} SELECT COALESCE(sum(case when status = 'non collated'  then 1 else  0 end),0) as count1  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
       "total_canceled": f"""{pu_query['query']} SELECT COALESCE(sum(case when status = 'canceled'  then 1 else  0 end),0) as count1  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",  
       "total_over_voting": f"""{pu_query['query']} select count(*) as count1 from pu where over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""" ,   
       "number_clear_win": f"""{pu_query['query']}  select count(*) as count1 from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
        "number_win_with_doubt": f"""{pu_query['query']} select count(*) as count1 from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
      "number_of_clear_loss": f"""{pu_query['query']} select count(*) as count1 from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}  select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end),0) as count1 from win_pu WHERE party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
       "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS count1 FROM pu WHERE {party_name}/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
     
      "above_with_doubt_25": f"""{pu_query['query']} 
       SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE total_vote_casted>0 and {party_name}/total_vote_casted*100>=25 AND state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
    #   SELECT COUNT(*) AS count1 FROM pu WHERE total_vote_casted>0 and {party_name}/total_vote_casted*100>=25 and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,

      
      "general_party_performance":
      

             f"""{pu_query['query']}     select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party = '{party_name}','clear leading',
 			IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party = '{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party = '{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party = '{party_name}','clear lagging',''))) )
 			as current_update from win_ward where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'
 			order by current_update desc limit 1 """
    
    },

    "tables": {

        "total": f"""{pu_query['query']} select pu_code, pu_name,Total_Registered_voters from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        "total_collated": f"""{pu_query['query']} SELECT pu_code, pu_name,{party_name} AS scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        "total_non_collated": f"""{pu_query['query']}  SELECT pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        "total_canceled": f"""{pu_query['query']} SELECT pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,  
        "total_over_voting": f"""{pu_query['query']}select pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""" ,   
        "number_clear_win": f"""{pu_query['query']} select pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,

        "number_win_with_doubt": f"""{pu_query['query']} select pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
      
         "number_of_clear_loss": f"""{pu_query['query']} select pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
  WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",

         "number_of_loss_with_doubt": f"""{pu_query['query']} select pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
  WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        
        "above_clearly_25":f"""{pu_query['query']} SELECT pu_code, pu_name, {party_name} AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
 from pu where {party_name}/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}'""",
        
        "above_with_doubt_25": f"""{pu_query['query']} SELECT pu_code, pu_name, {party_name} AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
 from pu where total_vote_casted>0 and {party_name}/total_vote_casted*100>=25 and state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """,
        
        "general_party_performance": f"""{pu_query['query']}  
    	SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
       IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Collation has not started') as percentage_score FROM win_ward 
          where state_name= '{state_name}' and lga_name =  '{lga_name}' and ward_name= '{ward_name}' """

    }
    }
}

        final_results = {}
        polling_results= {}
     
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []

   
        for key1, val in conditions_ward['polling_unit'].items():
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
        queries = len(conditions_ward['polling_unit']['values'])
        queries2 = len(conditions_ward['polling_unit']['tables'])
        for item in range(queries):
            query.append(item)
            complete.append(0)
        for item in range(queries2):
            query2.append(item)
            complete2.append(0)
        import time
        process_complete = 0
        process_pass = 0
        process_complete2 = 0
        process_pass2 = 0
        for qry in query:
            sql = execute_queries_values[qry]
            try:
                cur.execute_async(sql)
                query[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
   
        while process_complete == 0:
            item = -1
            process_pass += 1
            if sum(complete) == queries or process_pass == 10:
                process_complete = 1
            for result in query:
                item += 1
                if complete[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete[item] = 1
                        index = query.index(result)
                        key = key_values[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetchone()
                        ret = {"count1":recs}
                        # res = ret.to_json(orient="records")
                        # parsed = json.loads(res)
                        ress[key] = recs                      
                    else :
                        time.sleep(0.05)
        valu['values'] = ress
        for qry in query2:
            sql = execute_queries_tables[qry]
            
            try:
                cur.execute_async(sql)
                query2[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
        while process_complete2 == 0:
            item = -1
            process_pass2 += 1
            if sum(complete2) == queries2 or process_pass2 == 10:
                process_complete2 = 1
            for result in query2:
                item += 1
                if complete2[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete2[item] = 1
                        index = query2.index(result)
                        key = key_values_table[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetch_pandas_all()
                        res = recs.to_json(orient="records")
                        parsed = json.loads(res)
                        ress_table[key] = parsed                        
                    else :
                        time.sleep(0.05)
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)
        polling_results['polling_unit'] = final_results

        conn.close()
        return polling_results


# lga results

def get_polling_lga_all_results(country_name="undefined",state_name="undefined",lga_name="undefined",data={}):
    with get_db2() as conn:
        cur = conn.cursor()
        party_name = data['party_name']
        typo = data['level']
        # for key, value in data.items():
        #     party_name = value
        
        # for key, value in level.items():
        #     typo  = value
        

        conditions_lga = {

    "polling_unit":{

        "values": {
        
       "total": f"""{pu_query['query']} SELECT  count(*) as count1 from pu_result_table where state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
       "total_collated": f"""{pu_query['query']}SELECT COALESCE(sum(case when status = 'collated' OR status='canceled' then 1 else  0 end),0) as count1  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,
       "total_non_collated": f"""{pu_query['query']} SELECT COALESCE(sum(case when status = 'non collated'  then 1 else  0 end),0) as count1  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,
       "total_canceled": f"""{pu_query['query']} SELECT COALESCE(sum(case when status = 'canceled'  then 1 else  0 end),0) as count1  FROM pu WHERE state_name= '{state_name}' and lga_name =  '{lga_name}' """,  
       "total_over_voting": f"""{pu_query['query']} select count(*) as count1 from pu where over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
       "number_clear_win": f"""{pu_query['query']}  select count(*) as count1 from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
        "number_win_with_doubt": f"""{pu_query['query']} select count(*) as count1 from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
      "number_of_clear_loss": f"""{pu_query['query']} select count(*) as count1 from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}
       select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end),0) from win_pu WHERE party='ZLP' and state_id=19 AND lga_id=4 """,
        #  select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end),0) as count1 from win_pu WHERE party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
       "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS count1 FROM pu WHERE {party_name}/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
      "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS count1 FROM pu WHERE {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
    
    
    },

    "tables": {

        "total": f"""{pu_query['query']} select ward_name,pu_code, pu_name,Total_Registered_voters from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        "total_collated": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,{party_name} AS scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        "total_non_collated": f"""{pu_query['query']}  SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        "total_canceled": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,  
        "total_over_voting": f"""{pu_query['query']}select ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from pu where state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
        "number_clear_win": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}' and lga_name =  '{lga_name}'  """,

        "number_win_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
      
         "number_of_clear_loss": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
  WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,

         "number_of_loss_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
  WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        
            "above_clearly_25":f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, {party_name} AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks 
 from pu where {party_name}/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' and lga_name =  '{lga_name}'""",
        
        "above_with_doubt_25": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, {party_name} AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks AS Remarks  
 from pu where total_vote_casted>0 and {party_name}/total_vote_casted*100>=25 and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        

    }
    },

      "ward":{

        "values": {
        
       "total": f"""{pu_query['query']} SELECT  count(*) as count1 from wt where state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
       "total_collated": f"""{pu_query['query']}  select count(*) as collated from collated_ward where diff=0 and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
       "total_non_collated": f"""{pu_query['query']} select count(*) as in_progress from collated_ward where (diff>0 and diff<total) and  state_name= '{state_name}' and lga_name =  '{lga_name}' """,
       "total_canceled": f"""{pu_query['query']} select count(*)  from non_collated_ward  where total=0 and  state_name= '{state_name}' and lga_name =  '{lga_name}' """,  
       "total_over_voting": f"""{pu_query['query']} select count(*) as count1 from wt where over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
       "number_clear_win": f"""{pu_query['query']}  select count(*) as count1 from win_ward WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
        "number_win_with_doubt": f"""{pu_query['query']} select count(*) as count1 from win_ward WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
      "number_of_clear_loss": f"""{pu_query['query']} select count(*) as count1 from win_ward WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}  select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end),0) as count1 from win_ward WHERE party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
       "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS count1 from wt WHERE {party_name}/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
      "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS count1 from wt WHERE {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
      "general_party_performance": f"""{pu_query['query']}  

      	  select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 		IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_lga where state_name= '{state_name}' and lga_name =  '{lga_name}' '
 			order by current_update desc limit 1 """
    
    },

    "tables": {

         "total": f"""{pu_query['query']} select ward_name,Total_Registered_voters from wt state_name= '{state_name}' and lga_name =  '{lga_name}' """,
         "total_collated": f"""{pu_query['query']} select ward_name,Total_Registered_voters from collated_ward where diff=0 and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
         "total_non_collated": f"""{pu_query['query']}  select ward_name,Total_Registered_voters from collated_ward where (diff>0 and diff<total) and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
         "total_canceled": f"""{pu_query['query']} select ward_name,Total_Registered_voters  from non_collated_ward  where total=0 and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,  
        "total_over_voting": f"""{pu_query['query']} select ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from wt where state_name= '{state_name}' and lga_name =  '{lga_name}' """ ,   
        "number_clear_win": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}' and lga_name =  '{lga_name}'  """,

        "number_win_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
      
         "number_of_clear_loss": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
  WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,

         "number_of_loss_with_doubt": f"""{pu_query['query']} select ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
  WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        
        "above_clearly_25":f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from wt where {party_name}/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' and lga_name =  '{lga_name}' """,
        
        "above_with_doubt_25": f"""{pu_query['query']} SELECT ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from wt where {party_name}/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}' and lga_name =  '{lga_name}'  """,
        
        "general_party_performance": f"""{pu_query['query']}  
    	SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
       IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Collation has not started') as percentage_score FROM win_lga 
          where state_name= '{state_name}' and lga_name =  '{lga_name}'  """

    }
    }


}

         
        final_results = {}
    
        allresults= {}
     
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []

        if typo == 'pu':

            for key1, val in conditions_lga['polling_unit'].items():
                queries = len(conditions_lga['polling_unit']['values'])
                queries2 = len(conditions_lga['polling_unit']['tables'])


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
        elif typo =='ward':
            queries = len(conditions_lga['ward']['values'])
            queries2 = len(conditions_lga['ward']['tables'])

            for key1, val in conditions_lga['ward'].items():
                

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

        print(len(execute_queries_values))
                    
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
        process_complete = 0
        process_pass = 0
        process_complete2 = 0
        process_pass2 = 0
        for qry in query:
            sql = execute_queries_values[qry]
            try:
                cur.execute_async(sql)
                query[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
   
        while process_complete == 0:
            item = -1
            process_pass += 1
            if sum(complete) == queries or process_pass == 10:
                process_complete = 1
            for result in query:
                item += 1
                if complete[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete[item] = 1
                        index = query.index(result)
                        key = key_values[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetchone()
                        ret = {"count1":recs}
                        # res = ret.to_json(orient="records")
                        # parsed = json.loads(res)
                        ress[key] = recs                      
                    else :
                        time.sleep(0.05)
        valu['values'] = ress
        for qry in query2:
            sql = execute_queries_tables[qry]
            try:
                cur.execute_async(sql)
                query2[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
        while process_complete2 == 0:
            item = -1
            process_pass2 += 1
            if sum(complete2) == queries2 or process_pass2 == 10:
                process_complete2 = 1
            for result in query2:
                item += 1
                if complete2[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete2[item] = 1
                        index = query2.index(result)
                        key = key_values_table[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetch_pandas_all()
                        res = recs.to_json(orient="records")
                        parsed = json.loads(res)
                        ress_table[key] = parsed                        
                    else :
                        time.sleep(0.05)
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)
        if typo == 'pu':
            allresults['polling_unit'] = final_results
            return allresults
        elif typo == 'ward':
            allresults['ward'] = final_results
            return allresults


# state results
def get_polling_state_all_results(country_name="undefined",state_name="undefined", data={},level={}):
     with get_db2() as conn:
        cur = conn.cursor()
        for key, value in data.items():
            party_name = value
        
        for key, value in level.items():
            typo  = value
        
      

        conditions_state = {

    "polling_unit":{

        "values": {
        
       "total": f"""{pu_query['query']}  select  count(*) as count1 from pu_result_table where state_name= '{state_name}'  """,
       "total_collated": f"""{pu_query['query']} select COALESCE(sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) ,0) as count1  FROM pu WHERE state_name= '{state_name}' """,
       "total_non_collated": f"""{pu_query['query']}  select COALESCE(sum(case when status = 'non collated'  then 1 else  0 end) ,0) as count1  FROM pu WHERE state_name= '{state_name}' """,
       "total_canceled": f"""{pu_query['query']}  select COALESCE(sum(case when status = 'canceled'  then 1 else  0 end) ,0) as count1  FROM pu WHERE state_name= '{state_name}' """,  
       "total_over_voting": f"""{pu_query['query']}  select count(*) as count1 from pu where over_vote_values>0 and state_name= '{state_name}' """ ,   
       "number_clear_win": f"""{pu_query['query']}   select count(*) as count1 from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,
        "number_win_with_doubt": f"""{pu_query['query']}  select count(*) as count1 from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
      "number_of_clear_loss": f"""{pu_query['query']}  select count(*) as count1 from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_pu WHERE party='{party_name}' and state_name= '{state_name}'  """,
       "above_clearly_25":f"""{pu_query['query']}  select COUNT(*) AS count1 FROM pu WHERE {party_name}/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}'  """,
      "above_with_doubt_25": f"""{pu_query['query']}  select COUNT(*) AS count1 FROM pu WHERE {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}'  """,
   
    
    },

    "tables": {

        "total": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name,Total_Registered_voters from pu where state_name= '{state_name}' """,
        "total_collated": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name,{party_name} AS scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_name= '{state_name}' """,
        "total_non_collated": f"""{pu_query['query']}   select lga_name, ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_name= '{state_name}' """,
        "total_canceled": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_name= '{state_name}'  """,  
        "total_over_voting": f"""{pu_query['query']} select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from pu where state_name= '{state_name}' """ ,   
        "number_clear_win": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}'  """,

        "number_win_with_doubt": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' """,
      
         "number_of_clear_loss": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
  WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' """,

         "number_of_loss_with_doubt": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
  WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' """,
        
        "above_clearly_25":f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from pu where {party_name}/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' """,
        
        "above_with_doubt_25": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from pu where {party_name}/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}'  """
        
    

    }
    },

      "ward":{

        "values": {
        
       "total": f"""{pu_query['query']}  select  count(*) as count1 from wt where state_name= '{state_name}'  """,
       "total_collated": f"""{pu_query['query']}   select count(*) as collated from collated_ward where deef=0 and state_name= '{state_name}' """,
       "total_non_collated": f"""{pu_query['query']}  select count(*) as in_progress from collated_ward where (deef>0 and deef<total) and  state_name= '{state_name}' """,
       "total_canceled": f"""{pu_query['query']}  select count(*)  from non_collated_ward  where total=0 and  state_name= '{state_name}' """,  
       "total_over_voting": f"""{pu_query['query']}  select count(*) as count1 from wt where over_vote_values>0 and state_name= '{state_name}' """ ,   
       "number_clear_win": f"""{pu_query['query']}   select count(*) as count1 from win_ward WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,
        "number_win_with_doubt": f"""{pu_query['query']}  select count(*) as count1 from win_ward WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
      "number_of_clear_loss": f"""{pu_query['query']}  select count(*) as count1 from win_ward WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_ward WHERE party='{party_name}' and state_name= '{state_name}'  """,
       "above_clearly_25":f"""{pu_query['query']}  select COUNT(*) AS count1 from wt WHERE {party_name}/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}'  """,
      "above_with_doubt_25": f"""{pu_query['query']}  select COUNT(*) AS count1 from wt WHERE {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}'  """,

    
    },

    "tables": {

         "total": f"""{pu_query['query']}  select lga_name,ward_name,Total_Registered_voters from wt state_name= '{state_name}' """,
         "total_collated": f"""{pu_query['query']}  select lga_name,ward_name,Total_Registered_voters from collated_ward where deef=0 and state_name= '{state_name}' """,
         "total_non_collated": f"""{pu_query['query']}   select lga_name,ward_name,Total_Registered_voters from collated_ward where (deef>0 and deef<total) and state_name= '{state_name}' """,
         "total_canceled": f"""{pu_query['query']}  select lga_name,ward_name,Total_Registered_voters  from non_collated_ward  where total=0 and state_name= '{state_name}'  """,  
        "total_over_voting": f"""{pu_query['query']} select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from wt where state_name= '{state_name}' """ ,   
        "number_clear_win": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}'  """,

        "number_win_with_doubt": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' """,
      
         "number_of_clear_loss": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
  WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' """,

         "number_of_loss_with_doubt": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
  WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' """,
        
        "above_clearly_25":f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from wt where {party_name}/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' """,
        
        "above_with_doubt_25": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from wt where {party_name}/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}'  """,
        

    },


      "lga":{

        "values": {
        
       "total": f"""{pu_query['query']}  select  count(*) as count1 from lgat where state_name= '{state_name}'  """,
       "total_collated": f"""{pu_query['query']}   select count(*) as collated from collated_lga where deef=0 and state_name= '{state_name}' """,
       "total_non_collated": f"""{pu_query['query']}  select count(*) as in_progress from collated_lga where (deef>0 and deef<total) and  state_name= '{state_name}' """,
       "total_canceled": f"""{pu_query['query']}  select count(*)  from non_collated_lga  where total=0 and  state_name= '{state_name}' """,  
       "total_over_voting": f"""{pu_query['query']}  select count(*) as count1 from lgat where over_vote_values>0 and state_name= '{state_name}' """ ,   
       "number_clear_win": f"""{pu_query['query']}   select count(*) as count1 from win_lga WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,
        "number_win_with_doubt": f"""{pu_query['query']}  select count(*) as count1 from win_lga WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}'  """,
      "number_of_clear_loss": f"""{pu_query['query']}  select count(*) as count1 from win_lga WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}'  """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_lga WHERE party='{party_name}' and state_name= '{state_name}'  """,
       "above_clearly_25":f"""{pu_query['query']}  select COUNT(*) AS count1 from lgat WHERE {party_name}/total_vote_casted*100>=25 and remarks='OK' AND state_name= '{state_name}'  """,
      "above_with_doubt_25": f"""{pu_query['query']}  select COUNT(*) AS count1 from lgat WHERE {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND state_name= '{state_name}'  """,
      "general_party_performance": f"""{pu_query['query']}  

      	   select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 		IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_state where state_name= '{state_name}' '
 			order by current_update desc limit 1 """
    
    },

    "tables": {

         "total": f"""{pu_query['query']}  select lga_name,ward_name,Total_Registered_voters from lgat state_name= '{state_name}' """,
         "total_collated": f"""{pu_query['query']}  select lga_name,ward_name,Total_Registered_voters from collated_lga where deef=0 and state_name= '{state_name}' """,
         "total_non_collated": f"""{pu_query['query']}   select lga_name,ward_name,Total_Registered_voters from collated_lga where (deef>0 and deef<total) and state_name= '{state_name}' """,
         "total_canceled": f"""{pu_query['query']}  select lga_name,ward_name,Total_Registered_voters  from non_collated_lga  where total=0 and state_name= '{state_name}'  """,  
        "total_over_voting": f"""{pu_query['query']} select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from lgat where state_name= '{state_name}' """ ,   
        "number_clear_win": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  state_name= '{state_name}'  """,

        "number_win_with_doubt": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga 
WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' """,
      
         "number_of_clear_loss": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
  WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and state_name= '{state_name}' """,

         "number_of_loss_with_doubt": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga 
  WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and state_name= '{state_name}' """,
        
        "above_clearly_25":f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from lgat where {party_name}/total_vote_casted*100>=25 and remarks='OK' and state_name= '{state_name}' """,
        
        "above_with_doubt_25": f"""{pu_query['query']}  select lga_name, ward_name,pu_code, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from lgat where {party_name}/total_vote_casted*100>=25 and over_vote_values>0 and state_name= '{state_name}'  """,
        
        "general_party_performance": f"""{pu_query['query']}  
    	 select ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		
       IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Collation has not started') as percentage_score FROM win_state 
          where state_name= '{state_name}'  """

    }
    }
    }


}




         
        final_results = {}
    
        allresults= {}
     
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []

        if typo == 'pu':

            for key1, val in conditions_state['polling_unit'].items():
                queries = len(conditions_state['polling_unit']['values'])
                queries2 = len(conditions_state['polling_unit']['tables'])


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
        elif typo =='ward':
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
        process_complete = 0
        process_pass = 0
        process_complete2 = 0
        process_pass2 = 0
        for qry in query:
            sql = execute_queries_values[qry]
            try:
                cur.execute_async(sql)
                query[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
   
        while process_complete == 0:
            item = -1
            process_pass += 1
            if sum(complete) == queries or process_pass == 10:
                process_complete = 1
            for result in query:
                item += 1
                if complete[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete[item] = 1
                        index = query.index(result)
                        key = key_values[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetchone()
                        ret = {"count1":recs}
                        # res = ret.to_json(orient="records")
                        # parsed = json.loads(res)
                        ress[key] = recs                      
                    else :
                        time.sleep(0.05)
        valu['values'] = ress
        for qry in query2:
            sql = execute_queries_tables[qry]
            try:
                cur.execute_async(sql)
                query2[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
        while process_complete2 == 0:
            item = -1
            process_pass2 += 1
            if sum(complete2) == queries2 or process_pass2 == 10:
                process_complete2 = 1
            for result in query2:
                item += 1
                if complete2[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete2[item] = 1
                        index = query2.index(result)
                        key = key_values_table[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetch_pandas_all()
                        res = recs.to_json(orient="records")
                        parsed = json.loads(res)
                        ress_table[key] = parsed                        
                    else :
                        time.sleep(0.05)
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)
        if typo == 'pu':
            allresults['polling_unit'] = final_results
            return allresults
        elif typo == 'ward':
            allresults['ward'] = final_results
            return allresults
        elif typo == 'lga':
            allresults['lga'] = final_results
            return allresults
    






#  country result table
def get_polling_country_all_results(country_name="undefined",data={},level={}):
    with get_db2() as conn:
        cur = conn.cursor()
        for key, value in data.items():
            party_name = value
        
        for key, value in level.items():
            typo  = value
        
      






        conditions_country = {

    "polling_unit":{

        "values": {
        
       "total": f"""{pu_query['query']}  select  count(*) as count1 from pu_result_table where 1=1 and   """,
       "total_collated": f"""{pu_query['query']} select COALESCE(sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) ,0) as count1  FROM pu where 1=1 and  """,
       "total_non_collated": f"""{pu_query['query']}  select COALESCE(sum(case when status = 'non collated'  then 1 else  0 end) ,0) as count1  FROM pu where 1=1 and  """,
       "total_canceled": f"""{pu_query['query']}  select COALESCE(sum(case when status = 'canceled'  then 1 else  0 end) ,0) as count1  FROM pu where 1=1 and  """,  
       "total_over_voting": f"""{pu_query['query']}  select count(*) as count1 from pu where 1=1 and over_vote_values>0 and  """ ,   
       "number_clear_win": f"""{pu_query['query']}   select count(*) as count1 from win_pu where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and   """,
        "number_win_with_doubt": f"""{pu_query['query']}  select count(*) as count1 from win_pu where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and   """,
      "number_of_clear_loss": f"""{pu_query['query']}  select count(*) as count1 from win_pu where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and   """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_pu where 1=1 and party='{party_name}' and   """,
       "above_clearly_25":f"""{pu_query['query']}  select COUNT(*) AS count1 FROM pu where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK' AND   """,
      "above_with_doubt_25": f"""{pu_query['query']}  select COUNT(*) AS count1 FROM pu where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND   """,
      
    
    },

    "tables": {

        "total": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name,Total_Registered_voters from pu where 1=1 and  """,
        "total_collated": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name,{party_name} AS scores,total_vote_casted, remarks   FROM pu where 1=1 and  status = 'collated' OR status='canceled' and  """,
        "total_non_collated": f"""{pu_query['query']}   select state_name,lga_name, ward_name,pu_code,, pu_name,Total_Registered_voters, remarks   FROM pu where 1=1 and status='non collated' and  """,
        "total_canceled": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name,Total_Registered_voters, remarks   FROM pu where 1=1 and status = 'canceled'  and   """,  
        "total_over_voting": f"""{pu_query['query']} select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from pu where 1=1 and  """ ,   
        "number_clear_win": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and    """,

        "number_win_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and  """,
      
         "number_of_clear_loss": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  """,

         "number_of_loss_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and  """,
        
        "above_clearly_25":f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from pu where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK' and  """,
        
        "above_with_doubt_25": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from pu where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0 and   """
        
    

    }
    },

      "ward":{

        "values": {
        
       "total": f"""{pu_query['query']}  select  count(*) as count1 from wt where 1=1 and   """,
       "total_collated": f"""{pu_query['query']}   select count(*) as collated from collated_ward where 1=1 and deef=0 and  """,
       "total_non_collated": f"""{pu_query['query']}  select count(*) as in_progress from collated_ward where 1=1 and (deef>0 and deef<total) and   """,
       "total_canceled": f"""{pu_query['query']}  select count(*)  from non_collated_ward  where 1=1 and total=0 and   """,  
       "total_over_voting": f"""{pu_query['query']}  select count(*) as count1 from wt where 1=1 and over_vote_values>0 and  """ ,   
       "number_clear_win": f"""{pu_query['query']}   select count(*) as count1 from win_ward where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and   """,
        "number_win_with_doubt": f"""{pu_query['query']}  select count(*) as count1 from win_ward where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and   """,
      "number_of_clear_loss": f"""{pu_query['query']}  select count(*) as count1 from win_ward where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and   """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_ward where 1=1 and party='{party_name}' and   """,
       "above_clearly_25":f"""{pu_query['query']}  select COUNT(*) AS count1 from wt where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK' AND   """,
      "above_with_doubt_25": f"""{pu_query['query']}  select COUNT(*) AS count1 from wt where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND   """,

    
    },

    "tables": {

         "total": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters from wt  """,
         "total_collated": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters from collated_ward where 1=1 and deef=0 and  """,
         "total_non_collated": f"""{pu_query['query']}   select state_name,lga_name,ward_name,Total_Registered_voters from collated_ward where 1=1 and (deef>0 and deef<total) and  """,
         "total_canceled": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters  from non_collated_ward  where 1=1 and total=0 and   """,  
        "total_over_voting": f"""{pu_query['query']} select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from wt where 1=1 and  """ ,   
        "number_clear_win": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and    """,

        "number_win_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and  """,
      
         "number_of_clear_loss": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  """,

         "number_of_loss_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and  """,
        
        "above_clearly_25":f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from wt where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK' and  """,
        
        "above_with_doubt_25": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from wt where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0 and   """,
        

    },


      "lga":{

        "values": {
        
       "total": f"""{pu_query['query']}  select  count(*) as count1 from lgat where 1=1 and   """,
       "total_collated": f"""{pu_query['query']}   select count(*) as collated from collated_lga where 1=1 and deef=0 and  """,
       "total_non_collated": f"""{pu_query['query']}  select count(*) as in_progress from collated_lga where 1=1 and (deef>0 and deef<total) and   """,
       "total_canceled": f"""{pu_query['query']}  select count(*)  from non_collated_lga  where 1=1 and total=0 and   """,  
       "total_over_voting": f"""{pu_query['query']}  select count(*) as count1 from lgat where 1=1 and over_vote_values>0 and  """ ,   
       "number_clear_win": f"""{pu_query['query']}   select count(*) as count1 from win_lga where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and   """,
        "number_win_with_doubt": f"""{pu_query['query']}  select count(*) as count1 from win_lga where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and   """,
      "number_of_clear_loss": f"""{pu_query['query']}  select count(*) as count1 from win_lga where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and   """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_lga where 1=1 and party='{party_name}' and   """,
       "above_clearly_25":f"""{pu_query['query']}  select COUNT(*) AS count1 from lgat where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK' AND   """,
      "above_with_doubt_25": f"""{pu_query['query']}  select COUNT(*) AS count1 from lgat where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND   """,
      "general_party_performance": f"""{pu_query['query']}  

      	   select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 		IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_state where 1=1 and  '
 			order by current_update desc limit 1 """
    
    },

    "tables": {

         "total": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters from lgat  """,
         "total_collated": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters from collated_lga where 1=1 and deef=0 and  """,
         "total_non_collated": f"""{pu_query['query']}   select state_name,lga_name,ward_name,Total_Registered_voters from collated_lga where 1=1 and (deef>0 and deef<total) and  """,
         "total_canceled": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters  from non_collated_lga  where 1=1 and total=0 and   """,  
        "total_over_voting": f"""{pu_query['query']} select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from lgat where 1=1 and  """ ,   
        "number_clear_win": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}' and    """,

        "number_win_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga 
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and  """,
      
         "number_of_clear_loss": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga 
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}' and  """,

         "number_of_loss_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga 
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}' and  """,
        
        "above_clearly_25":f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from lgat where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK' and  """,
        
        "above_with_doubt_25": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from lgat where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0 and   """,
        
        "general_party_performance": f"""{pu_query['query']}  
    	 select ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		
       IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Collation has not started') as percentage_score FROM win_state 
          where 1=1 and   """

    }
    },

       "state":{

        "values": {
        
       "total": f"""{pu_query['query']}  select  count(*) as count1 from st where 1=1 and   """,
       "total_collated": f"""{pu_query['query']}   select count(*) as collated from collated_st where 1=1 and deef=0  """,
       "total_non_collated": f"""{pu_query['query']}  select count(*) as in_progress from collated_st where 1=1 and (deef>0 and deef<total)   """,
       "total_canceled": f"""{pu_query['query']}  select count(*)  from non_collated_st  where 1=1 and total=0   """,  
       "total_over_voting": f"""{pu_query['query']}  select count(*) as count1 from st where 1=1 and over_vote_values>0  """ ,   
       "number_clear_win": f"""{pu_query['query']}   select count(*) as count1 from win_state where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
        "number_win_with_doubt": f"""{pu_query['query']}  select count(*) as count1 from win_state where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
      "number_of_clear_loss": f"""{pu_query['query']}  select count(*) as count1 from win_state where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
       "number_of_loss_with_doubt": f"""{pu_query['query']}   select COALESCE(sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) ,0) as count1 from win_state where 1=1 and party='{party_name}'   """,
       "above_clearly_25":f"""{pu_query['query']}  select COUNT(*) AS count1 from st where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK'   """,
      "above_with_doubt_25": f"""{pu_query['query']}  select COUNT(*) AS count1 from lgat where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0 AND   """,
      "general_party_performance": f"""{pu_query['query']}  

      	   select IFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
 		IFF(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
 			IFF( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','lagging with doubt',
 			IFF(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
 			as current_update from win_country where 1=1 and  '
 			order by current_update desc limit 1 """
    
    },

    "tables": {

         "total": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters from st  """,
         "total_collated": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters from collated_state where 1=1 and deef=0  """,
         "total_non_collated": f"""{pu_query['query']}   select state_name,lga_name,ward_name,Total_Registered_voters from collated_state where 1=1 and (deef>0 and deef<total)   """,
         "total_canceled": f"""{pu_query['query']}  select state_name,lga_name,ward_name,Total_Registered_voters  from non_collated_state  where 1=1 and total=0    """,  
        "total_over_voting": f"""{pu_query['query']} select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks
  from st where 1=1  """ ,   
        "number_clear_win": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
where 1=1 and row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,

        "number_win_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_state 
where 1=1 and row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'  """,
      
         "number_of_clear_loss": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
  where 1=1 and row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'  """,

         "number_of_loss_with_doubt": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, votes as scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_state 
  where 1=1 and row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'  """,
        
        "above_clearly_25":f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks 
from st where 1=1 and {party_name}/total_vote_casted*100>=25 and remarks='OK' """,
        
        "above_with_doubt_25": f"""{pu_query['query']}  select state_name,lga_name, ward_name,pu_code,, pu_name, {party_name} AS scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND({party_name}/total_vote_casted*100,2),'%') AS percentage_votes,remarks
from st where 1=1 and {party_name}/total_vote_casted*100>=25 and over_vote_values>0   """,
        
        "general_party_performance": f"""{pu_query['query']}  
    	 select ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		
       IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Collation has not started') as percentage_score FROM win_country 
          where 1=1 """

    }
    }
    }


}
  

          
        final_results = {}

        allresults= {}
        
        key_values = []
        key_values_table = []
        execute_queries_values = []
        execute_queries_tables = []

        if typo == 'pu':

            for key1, val in conditions_country['polling_unit'].items():
                queries = len(conditions_country['polling_unit']['values'])
                queries2 = len(conditions_country['polling_unit']['tables'])


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
        elif typo =='ward':
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
        process_complete = 0
        process_pass = 0
        process_complete2 = 0
        process_pass2 = 0
        for qry in query:
            sql = execute_queries_values[qry]
            try:
                cur.execute_async(sql)
                query[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')

        while process_complete == 0:
            item = -1
            process_pass += 1
            if sum(complete) == queries or process_pass == 10:
                process_complete = 1
            for result in query:
                item += 1
                if complete[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete[item] = 1
                        index = query.index(result)
                        key = key_values[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetchone()
                        ret = {"count1":recs}
                        # res = ret.to_json(orient="records")
                        # parsed = json.loads(res)
                        ress[key] = recs                      
                    else :
                        time.sleep(0.05)
        valu['values'] = ress
        for qry in query2:
            sql = execute_queries_tables[qry]
            try:
                cur.execute_async(sql)
                query2[qry]=cur.sfqid
            except:
                print('Skipped a sceanrio')
        while process_complete2 == 0:
            item = -1
            process_pass2 += 1
            if sum(complete2) == queries2 or process_pass2 == 10:
                process_complete2 = 1
            for result in query2:
                item += 1
                if complete2[item] == 0:
                    # print('result for:' +str(result))
                    status = conn.get_query_status(result)
                    if str(status) == 'QueryStatus.SUCCESS':
                        
                        complete2[item] = 1
                        index = query2.index(result)
                        key = key_values_table[index]
                        cur.get_results_from_sfqid(result)
                        recs = cur.fetch_pandas_all()
                        res = recs.to_json(orient="records")
                        parsed = json.loads(res)
                        ress_table[key] = parsed                        
                    else :
                        time.sleep(0.05)
        tab['tables'] = ress_table
        final_results.update(valu)
        final_results.update(tab)
        if typo == 'pu':
            allresults['polling_unit'] = final_results
            return allresults
        elif typo == 'ward':
            allresults['ward'] = final_results
            return allresults
        elif typo == 'lga':
            allresults['lga'] = final_results
            return allresults
        elif typo == 'state':
            allresults['state'] = final_results
            return allresults