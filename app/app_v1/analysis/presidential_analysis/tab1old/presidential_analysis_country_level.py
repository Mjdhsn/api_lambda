
from app.app_v1.database import get_db


values = {

"ct":"""
WITH ct AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + crt.Total_Rejected_votes)
          AS total_vote_casted, 
          
         IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,


         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + crt.Total_Rejected_votes > crt.Total_Accredited_voters and 
                   crt.Total_Accredited_voters  > crt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + crt.Total_Rejected_votes > crt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( crt.Total_Accredited_voters  > crt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non coalated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (crt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + crt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 crt.Total_Rejected_votes)/crt.Total_Registered_voters *100,2),'%'), 
                 if (crt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM country_result_table crt)

"""

}

values_win = {
"win_c":f"""{presidential_table['query']}, win_c as 
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

"""

}


parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')



# QUERIES
conditions_country = {
    "collation_status": f"""{presidential_table['query']} select case when status='collated' then 'collated'  when status='non collated' then 'non coalated' else 'canceled' end as 'coallation status' from country_result_table""",
    "over_voting_status": f"""{presidential_table['query']} select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  over_voting_status from ct""",
    "over_voting_figure": f"""{presidential_table['query']} select  over_vote_values as Total_over_vote_figures from ct""",
   
    "total_registered_voters": f"""{presidential_table['query']} SELECT Total_Registered_voters as count1  FROM ct""",
    "total_accredited_voters": f"""{presidential_table['query']} SELECT Total_Accredited_voters as count1  from ct""",
    "total_rejected_votes": f"""{presidential_table['query']} SELECT Total_Rejected_votes  as count1 from ct """,
    "total_valid_votes": f"""{presidential_table['query']} SELECT total_valid_votes as count1  from ct """,
    "total_vote_casted": f"""{presidential_table['query']} SELECT total_vote_casted as count1 from ct""",
    "percentage_voters_turnout": f"""{presidential_table['query']} SELECT percentage_voters_turnout as count1  from ct""",
    "party_graph":f"""{values_win['win_c']} SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_c """
}





#  country result table
def get_country_country_all_results(country_name="undefined"):
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
                val += f" WHERE 1=1 "

                execute_queries.append(val)
                key_values.append(key)
            
            
        try:
            
            for index, val in enumerate(execute_queries):
          
                cur.execute(val)
                val_results = cur.fetchall()
                # if val_results:
                #     print(key_values[index])
                final_results[key_values[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)
