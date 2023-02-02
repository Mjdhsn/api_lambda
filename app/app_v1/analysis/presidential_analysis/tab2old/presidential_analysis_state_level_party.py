
from app.app_v1.database import get_db


values = {

"st":"""
WITH st AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + srt.Total_Rejected_votes)
          AS total_vote_casted, 
          
          IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,


         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + srt.Total_Rejected_votes > srt.Total_Accredited_voters and 
                   srt.Total_Accredited_voters  > srt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + srt.Total_Rejected_votes > srt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( srt.Total_Accredited_voters  > srt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non coalated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (srt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + srt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 srt.Total_Rejected_votes)/srt.Total_Registered_voters *100,2),'%'), 
                 if (srt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM state_result_table srt)
""",

"ct":"""
WITH st AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + srt.Total_Rejected_votes)
          AS total_vote_casted, 
          
        IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,

         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + srt.Total_Rejected_votes > srt.Total_Accredited_voters and 
                   srt.Total_Accredited_voters  > srt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + srt.Total_Rejected_votes > srt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( srt.Total_Accredited_voters  > srt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non coalated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (srt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + srt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 srt.Total_Rejected_votes)/srt.Total_Registered_voters *100,2),'%'), 
                 if (srt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM state_result_table srt),


ct AS
		(select country_id,country_name, sum(srt.A) AS A, sum(srt.AA) AS AA, sum(srt.AAC) AS AAC, 
			sum(srt.ADC) AS ADC, sum(srt.ADP) AS ADP, sum(srt.APC) AS APC, sum(srt.APGA) AS APGA,
			sum(srt.APM) AS APM, sum(srt.APP) AS APP, sum(srt.BP) AS BP, sum(srt.LP) AS LP,
			sum(srt.NRM) AS NRM, sum(srt.NNPP) as NNPP, sum(srt.PDP) AS PDP, sum(srt.PRP) AS PRP, 
			sum(srt.SDP) AS SDP, sum(srt.YPP) AS YPP, sum(srt.ZLP) AS ZLP, 
			sum(srt.Total_Rejected_votes) AS Total_Rejected_votes, sum(srt.Total_Registered_voters) AS Total_Registered_voters,
			sum(srt.Total_Accredited_voters) AS Total_Accredited_voters,
			
			(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
          	sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)) AS total_valid_votes,

          (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
           sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))
          AS total_vote_casted, 
		
 		IF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
               sum(Total_Accredited_voters) - sum(Total_Registered_voters),
           IF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > Total_Accredited_voters ,
            (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) - sum(Total_Accredited_voters),0)
                 ) AS over_vote_values,
                 
		IF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) and 
            sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) ,
            'Over Votting! Because total votes casted are greater than total accredited voters',  
        IF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
           'Over Votting! Because total accredited voters are greater than total registered voters', 
        IF(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)>0,'OK','non collated')
                   ))) AS remarks, 
             
 		IF (sum(Total_Registered_voters)>0 and
           sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)>0,             
           CONCAT(ROUND((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))/sum(Total_Registered_voters) *100,2),'%'), 
       IF (sum(Total_Registered_voters)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
       FROM state_result_table srt)

"""

}

values_win = {
"win_s":f"""{values['st']},win_s as

  (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, A AS votes, "A" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, AA AS votes, "AA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, ADP AS votes, "ADP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APP AS votes, "APP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, AAC AS votes, "AAC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, ADC AS votes, "ADC" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APC AS votes, "APC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APGA AS votes, "APGA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APM AS votes, "APM" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, BP AS votes, "BP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, LP AS votes, "LP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, NRM AS votes, "NRM" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, NNPP AS votes, "NNPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, PDP AS votes, "PDP" AS party FROM st   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, PRP AS votes, "PRP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, SDP AS votes, "SDP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, YPP AS votes, "YPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, ZLP AS votes, "ZLP" AS party FROM st   )

""",
"win_c":f"""{values['ct']}, win_c as
 (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, A AS votes, "A" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, AA AS votes, "AA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, ADP AS votes, "ADP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, APP AS votes, "APP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, AAC AS votes, "AAC" AS party FROM ct
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, ADC AS votes, "ADC" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, APC AS votes, "APC" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, APGA AS votes, "APGA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, APM AS votes, "APM" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, BP AS votes, "BP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, LP AS votes, "LP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, NRM AS votes, "NRM" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, NNPP AS votes, "NNPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, PDP AS votes, "PDP" AS party FROM ct  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, PRP AS votes, "PRP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, SDP AS votes, "SDP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, YPP AS votes, "YPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,total_vote_casted, ZLP AS votes, "ZLP" AS party FROM ct  )
         


""",


"non_collated_state": f"""{values['st']}, non_collated_state as 

(SELECT state_id,state_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM st group by pu.state_id )


""",


"collated_state": f"""{values['st']},collated_state as

( select count(*) as total,state_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from st  group by pu.state_id)

"""

}


win_tab2 = {



"win_state":f"""{values_win['win_s']} 

 (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_s) """

,

"win_country":f"""{values_win['win_c']}

 (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_c) """



}







# QUERIES




# QUERIES
conditions_state = {
    "total": f"""{values['st']} SELECT COUNT(*) as  count1 FROM  st""",
    "total_registered_votes_table": f"""{values['st']} select  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st""",
    "total_registered_votes": f"""{values['st']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st""",
    "canceled": f"""{values['st']} SELECT count(*) as  count1 FROM  st where status ="canceled" """,  
    "canceled_table": f"""{values['st']} SELECT state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['st']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status ='canceled' """ ,   
    "collated": f"""{values['st']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM  st""",
    "collated_table": f"""{values['st']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM  st WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['st']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['st']} SELECT COUNT(*) as  count1   FROM  st where status='non collated'""",
    "un_collated_table":f"""{values['st']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['st']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status='non collated'""",
    "total_registered_voters": f"""{values['st']} SELECT Total_Registered_voters  FROM st""",
    "total_accredited_voters": f"""{values['st']} SELECT Total_Accredited_voters  from st""",
    "total_rejected_votes": f"""{values['st']} SELECT Total_Rejected_votes   from st """,
    "total_valid_votes": f"""{values['st']} SELECT total_valid_votes  from st """,
    "total_vote_casted": f"""{values['st']} SELECT total_vote_casted  from st""",
    "percentage_voters_turnout": f"""{values['st']} SELECT percentage_voters_turnout  from st""",
    "over_voting": f"""{values['st']} SELECT count(*) as count1 FROM  st WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['st']} SELECT state_name,over_vote_values,remarks,percentage_voters_turnout  FROM  st WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['st']} select sum(over_vote_values) as over_votes_figuers FROM  st WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_s']} SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_s """
}

# QUERIES
conditions_country = {
    "total": f"""{values['st']} SELECT COUNT(*) as  count1 FROM  st""",
    "total_registered_votes_table": f"""{values['st']} select  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st""",
    "total_registered_votes": f"""{values['st']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st""",
    "canceled": f"""{values['st']} SELECT count(*) as  count1 FROM  st where status ="canceled" """,  
    "canceled_table": f"""{values['st']} SELECT state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['st']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status ='canceled' """ ,   
    "collated": f"""{values['st']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM  st""",
    "collated_table": f"""{values['st']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM  st WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['st']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['st']} SELECT COUNT(*) as  count1   FROM  st where status='non collated'""",
    "un_collated_table":f"""{values['st']} SELECT  state_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM  st where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['st']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM  st where status='non collated'""",
    "total_registered_voters": f"""{values['ct']} SELECT Total_Registered_voters  FROM ct""",
    "total_accredited_voters": f"""{values['ct']} SELECT Total_Accredited_voters  from ct""",
    "total_rejected_votes": f"""{values['ct']} SELECT Total_Rejected_votes   from ct """,
    "total_valid_votes": f"""{values['ct']} SELECT total_valid_votes  from ct """,
    "total_vote_casted": f"""{values['ct']} SELECT total_vote_casted  from ct""",
    "percentage_voters_turnout": f"""{values['ct']} SELECT percentage_voters_turnout  from ct""",
    "over_voting": f"""{values['st']} SELECT count(*) as count1 FROM  st WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['st']} SELECT state_name,over_vote_values,remarks,percentage_voters_turnout  FROM  st WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['st']} select sum(over_vote_values) as over_votes_figuers FROM  st WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_c']} SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_c """
}




where_list = ["canceled","canceled_table","total_registered_canceled_voters","collated_table","total_registered_collated_voters","un_collated","un_collated_table","total_registered_uncollated_voters","over_voting","over_voting_table","total_over_voting"]



table_list = ["total_registered_votes_table","canceled_table","collated_table", "un_collated_table","over_voting_table"]






# state results
def get_state_state_all_results(country_name="undefined",state_name="undefined"):
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
            
                
                
            
            
        map1 = ['state_name']
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
def get_state_country_all_results(country_name="undefined"):
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
            
            
        map1 = ['state_name']
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