 "polling_unit":{


           "values": {
        
    "total": f"""{pu_query['query']} SELECT  count(*) from pu_result_table where 1=1    """,
    "total_collated": f"""{pu_query['query']}SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  from st where 1=1   """,
    "total_non_collated": f"""{pu_query['query']} SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  from st where 1=1   """,
    "total_canceled": f"""{pu_query['query']} SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  from st where 1=1   """,  
    "total_over_voting": f"""{pu_query['query']} select count(*) from st where 1=1 over_vote_values>0   """ ,   
    "number_clear_win": f"""{pu_query['query']}  select count(*) from win_state where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}   """,
    "number_win_with_doubt": f"""{pu_query['query']} select count(*) from win_state where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
    "number_of_clear_loss": f"""{pu_query['query']} select count(*) from win_state where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,
    "number_of_loss_with_doubt": f"""{pu_query['query']}  select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_state where 1=1 party="{party_name}"    """,
    "above_clearly_25":f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from st where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'   """,
    "above_with_doubt_25": f"""{pu_query['query']} SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove from st where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0    """,
    "general_party_performance": f"""{pu_query['query']}  select if (row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear leading',
		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}','leading with doubt',
			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party={party_name},'lagging with doubt',
			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party='{party_name}','clear lagging',''))) )
			as current_update from win_state where 1=1   
 			order by current_update desc limit 1"""
    
    },

    "tables": {

        "total": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from st where 1=1   """,
        "total_collated": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   from st where 1=1  status = 'collated' OR status='canceled'   """,
        "total_non_collated": f"""{pu_query['query']}  SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from st where 1=1 status='non collated'   """,
        "total_canceled": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from st where 1=1 status = 'canceled'    """,  
        "canceled_table": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   from st where 1=1 status = 'canceled'    """,
        "total_over_voting": f"""{pu_query['query']}select state_name,lga_name,ward_name,pu_code, pu_name, ZLP as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
  from st where 1=1   """ ,   
        "number_clear_win": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
where 1=1 row_num<2 and total_valid_votes>0 and remarks='OK' AND party='{party_name}'     """,

        "number_win_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_state 
where 1=1 row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
      
        "number_of_clear_loss": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
where 1=1 row_num>1 AND total_valid_votes>0 and remarks='OK' AND party='{party_name}'   """,

        "number_of_loss_with_doubt": f"""{pu_query['query']} select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_state 
where 1=1 row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party='{party_name}'   """,
        
        "above_clearly_25":f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
from st where 1=1 ZLP/total_vote_casted*100>=25 and remarks='OK'  """,
        
        "above_with_doubt_25": f"""{pu_query['query']} SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
from st where 1=1 ZLP/total_vote_casted*100>=25 and over_vote_values>0     """,
        "general_party_performance": f"""{pu_query['query']}         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_state 
where 1=1    """


    }
}