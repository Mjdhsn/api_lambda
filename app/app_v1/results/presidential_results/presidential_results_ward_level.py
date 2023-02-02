from app.app_v1.database import get_db,get_db2
from app.app_v1.results.presidential_results.partytable import presidential_table_ward
import json

# values ={
#  "wt":"""
# WITH wt AS

#     (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

#           (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes)
#           AS total_vote_casted, 
          
#           IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
# 	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#               YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
#               (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                Total_Rejected_votes) - Total_Accredited_voters,0)
#                  ) AS over_vote_values,

                 

#          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters and 
#                    wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
#           			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters ,
#                    'Over Votting! Because total votes casted are greater than total accredited voters',  
#                    IF( wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total accredited voters are greater than total registered voters', 
#                    IF (status='canceled','canceled',
#                    IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
#                    )))) AS remarks, 
                 
                 
                 
#              IF (status='canceled','canceled',
#              IF (wrt.Total_Registered_voters>0 and
#             	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes>0,             
#                  CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                  wrt.Total_Rejected_votes)/wrt.Total_Registered_voters *100,2),'%'), 
#                  if (wrt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
#                  ) AS percentage_voters_turnout


#             FROM ward_result_table wrt)
 
#  """,

#   "lgat":"""
# WITH wt AS

#     (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

#           (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes)
#           AS total_vote_casted, 
          
#           IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
# 	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#               YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
#               (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                Total_Rejected_votes) - Total_Accredited_voters,0)
#                  ) AS over_vote_values,


#          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters and 
#                    wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
#           			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters ,
#                    'Over Votting! Because total votes casted are greater than total accredited voters',  
#                    IF( wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total accredited voters are greater than total registered voters', 
#                    IF (status='canceled','canceled',
#                    IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
#                    )))) AS remarks, 
                 
                 
                 
#              IF (status='canceled','canceled',
#              IF (wrt.Total_Registered_voters>0 and
#             	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes>0,             
#                  CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                  wrt.Total_Rejected_votes)/wrt.Total_Registered_voters *100,2),'%'), 
#                  if (wrt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
#                  ) AS percentage_voters_turnout


#             FROM ward_result_table wrt),

# lgat as
# 		(SELECT country_id,country_name, wt.state_id, wt.state_name, wt.lga_id, wt.lga_name,
# 			sum(wt.A) AS A, sum(wt.AA) AS AA, sum(wt.AAC) AS AAC, 
# 			sum(wt.ADC) AS ADC, sum(wt.ADP) AS ADP, sum(wt.APC) AS APC, sum(wt.APGA) AS APGA,
# 			sum(wt.APM) AS APM, sum(wt.APP) AS APP, sum(wt.BP) AS BP, sum(wt.LP) AS LP,
# 			sum(wt.NRM) AS NRM, sum(wt.NNPP) as NNPP, sum(wt.PDP) AS PDP, sum(wt.PRP) AS PRP, 
# 			sum(wt.SDP) AS SDP, sum(wt.YPP) AS YPP, sum(wt.ZLP) AS ZLP, 
# 			sum(wt.Total_Rejected_votes) AS Total_Rejected_votes, sum(wt.Total_Registered_voters) AS Total_Registered_voters,
# 			sum(wt.Total_Accredited_voters) AS Total_Accredited_voters
#         	FROM wt group by wt.state_id, wt.lga_id) 
 
#  """,
# "st":"""
 
#  WITH wt AS

#     (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

#           (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes)
#           AS total_vote_casted, 
          
#           IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
# 	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#               YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
#               (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                Total_Rejected_votes) - Total_Accredited_voters,0)
#                  ) AS over_vote_values,


#          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters and 
#                    wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
#           			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters ,
#                    'Over Votting! Because total votes casted are greater than total accredited voters',  
#                    IF( wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total accredited voters are greater than total registered voters', 
#                    IF (status='canceled','canceled',
#                    IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
#                    )))) AS remarks, 
                 
                 
                 
#              IF (status='canceled','canceled',
#              IF (wrt.Total_Registered_voters>0 and
#             	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes>0,             
#                  CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                  wrt.Total_Rejected_votes)/wrt.Total_Registered_voters *100,2),'%'), 
#                  if (wrt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
#                  ) AS percentage_voters_turnout


#             FROM ward_result_table wrt),

# lgat as
# 		(SELECT country_id,country_name, wt.state_id, wt.state_name, wt.lga_id, wt.lga_name,
# 			sum(wt.A) AS A, sum(wt.AA) AS AA, sum(wt.AAC) AS AAC, 
# 			sum(wt.ADC) AS ADC, sum(wt.ADP) AS ADP, sum(wt.APC) AS APC, sum(wt.APGA) AS APGA,
# 			sum(wt.APM) AS APM, sum(wt.APP) AS APP, sum(wt.BP) AS BP, sum(wt.LP) AS LP,
# 			sum(wt.NRM) AS NRM, sum(wt.NNPP) as NNPP, sum(wt.PDP) AS PDP, sum(wt.PRP) AS PRP, 
# 			sum(wt.SDP) AS SDP, sum(wt.YPP) AS YPP, sum(wt.ZLP) AS ZLP, 
# 			sum(wt.Total_Rejected_votes) AS Total_Rejected_votes, sum(wt.Total_Registered_voters) AS Total_Registered_voters,
# 			sum(wt.Total_Accredited_voters) AS Total_Accredited_voters
#         	FROM wt group by wt.state_id, wt.lga_id) ,
# st AS
# 		(select country_id,country_name,state_id,state_name, sum(lgat.A) AS A, sum(lgat.AA) AS AA, sum(lgat.AAC) AS AAC, 
# 			sum(lgat.ADC) AS ADC, sum(lgat.ADP) AS ADP, sum(lgat.APC) AS APC, sum(lgat.APGA) AS APGA,
# 			sum(lgat.APM) AS APM, sum(lgat.APP) AS APP, sum(lgat.BP) AS BP, sum(lgat.LP) AS LP,
# 			sum(lgat.NRM) AS NRM, sum(lgat.NNPP) as NNPP, sum(lgat.PDP) AS PDP, sum(lgat.PRP) AS PRP, 
# 			sum(lgat.SDP) AS SDP, sum(lgat.YPP) AS YPP, sum(lgat.ZLP) AS ZLP, 
# 			sum(lgat.Total_Rejected_votes) AS Total_Rejected_votes, sum(lgat.Total_Registered_voters) AS Total_Registered_voters,
# 			sum(lgat.Total_Accredited_voters) AS Total_Accredited_voters			
#         FROM lgat GROUP BY lgat.state_id)
 
 
#  """,

#   "ct":"""
 
#  WITH wt AS

#     (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

#           (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes)
#           AS total_vote_casted, 
          
#          IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
# 	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#               YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
#               (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                Total_Rejected_votes) - Total_Accredited_voters,0)
#                  ) AS over_vote_values,


#          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters and 
#                    wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
#           			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + wrt.Total_Rejected_votes > wrt.Total_Accredited_voters ,
#                    'Over Votting! Because total votes casted are greater than total accredited voters',  
#                    IF( wrt.Total_Accredited_voters  > wrt.Total_Registered_voters,
#                    'Over Votting! Because total accredited voters are greater than total registered voters', 
#                    IF (status='canceled','canceled',
#                    IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
#                    )))) AS remarks, 
                 
                 
                 
#              IF (status='canceled','canceled',
#              IF (wrt.Total_Registered_voters>0 and
#             	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + wrt.Total_Rejected_votes>0,             
#                  CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                  wrt.Total_Rejected_votes)/wrt.Total_Registered_voters *100,2),'%'), 
#                  if (wrt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
#                  ) AS percentage_voters_turnout


#             FROM ward_result_table wrt),

# lgat as
# 		(SELECT country_id,country_name, wt.state_id, wt.state_name, wt.lga_id, wt.lga_name,
# 			sum(wt.A) AS A, sum(wt.AA) AS AA, sum(wt.AAC) AS AAC, 
# 			sum(wt.ADC) AS ADC, sum(wt.ADP) AS ADP, sum(wt.APC) AS APC, sum(wt.APGA) AS APGA,
# 			sum(wt.APM) AS APM, sum(wt.APP) AS APP, sum(wt.BP) AS BP, sum(wt.LP) AS LP,
# 			sum(wt.NRM) AS NRM, sum(wt.NNPP) as NNPP, sum(wt.PDP) AS PDP, sum(wt.PRP) AS PRP, 
# 			sum(wt.SDP) AS SDP, sum(wt.YPP) AS YPP, sum(wt.ZLP) AS ZLP, 
# 			sum(wt.Total_Rejected_votes) AS Total_Rejected_votes, sum(wt.Total_Registered_voters) AS Total_Registered_voters,
# 			sum(wt.Total_Accredited_voters) AS Total_Accredited_voters
#         	FROM wt group by wt.state_id, wt.lga_id) ,
# st AS
# 		(select country_id,country_name,state_id,state_name, sum(lgat.A) AS A, sum(lgat.AA) AS AA, sum(lgat.AAC) AS AAC, 
# 			sum(lgat.ADC) AS ADC, sum(lgat.ADP) AS ADP, sum(lgat.APC) AS APC, sum(lgat.APGA) AS APGA,
# 			sum(lgat.APM) AS APM, sum(lgat.APP) AS APP, sum(lgat.BP) AS BP, sum(lgat.LP) AS LP,
# 			sum(lgat.NRM) AS NRM, sum(lgat.NNPP) as NNPP, sum(lgat.PDP) AS PDP, sum(lgat.PRP) AS PRP, 
# 			sum(lgat.SDP) AS SDP, sum(lgat.YPP) AS YPP, sum(lgat.ZLP) AS ZLP, 
# 			sum(lgat.Total_Rejected_votes) AS Total_Rejected_votes, sum(lgat.Total_Registered_voters) AS Total_Registered_voters,
# 			sum(lgat.Total_Accredited_voters) AS Total_Accredited_voters			
#         FROM lgat GROUP BY lgat.state_id),
        
# ct AS
# 		(select country_name, sum(st.A) AS A, sum(st.AA) AS AA, sum(st.AAC) AS AAC, 
# 			sum(st.ADC) AS ADC, sum(st.ADP) AS ADP, sum(st.APC) AS APC, sum(st.APGA) AS APGA,
# 			sum(st.APM) AS APM, sum(st.APP) AS APP, sum(st.BP) AS BP, sum(st.LP) AS LP,
# 			sum(st.NRM) AS NRM, sum(st.NNPP) as NNPP, sum(st.PDP) AS PDP, sum(st.PRP) AS PRP, 
# 			sum(st.SDP) AS SDP, sum(st.YPP) AS YPP, sum(st.ZLP) AS ZLP, 
# 			sum(st.Total_Rejected_votes) AS Total_Rejected_votes, sum(st.Total_Registered_voters) AS Total_Registered_voters,
# 			sum(st.Total_Accredited_voters) AS Total_Accredited_voters			
#                 FROM st)
 
 
#  """

# }

parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')



# QUERIES
conditions_ward = {
    "total": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1 from wt""",
    "total_registered_votes_table": f"""{presidential_table_ward['query']} select ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt""",
    "total_registered_votes": f"""{presidential_table_ward['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 from wt""",
    'canceled': f"""{presidential_table_ward['query']} SELECT count(*) as  count1 from wt where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_ward['query']} SELECT ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status ='canceled' """,
    "total_registered_canceled_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status ='canceled' """ ,   
    "collated": f"""{presidential_table_ward['query']}  SELECT COUNT(*) as  count1 from wt where   (status = 'collated' OR status='canceled')""",
    "collated_table": f"""{presidential_table_ward['query']} SELECT ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt where  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1   from wt where status='non collated'""",
    "un_collated_table":f"""{presidential_table_ward['query']} SELECT ward_name,   Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status='non collated'""",
    "total_registered_uncollated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status='non collated'""",
    
}


# QUERIES
conditions_lga = {
    "total": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1 from wt""",
    "total_registered_votes_table": f"""{presidential_table_ward['query']} select lga_name,ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt""",
    "total_registered_votes": f"""{presidential_table_ward['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 from wt """,
    'canceled': f"""{presidential_table_ward['query']} SELECT count(*) as  count1 from wt where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_ward['query']} SELECT lga_name, ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status ='canceled' """,
    "total_registered_canceled_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status ='canceled' """ ,   
    "collated": f"""{presidential_table_ward['query']}  SELECT COUNT(*) as  count1 from wt where   (status = 'collated' OR status='canceled')""",
    "collated_table": f"""{presidential_table_ward['query']} SELECT lga_name, ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt where  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1   from wt where status='non collated'""",
    "un_collated_table":f"""{presidential_table_ward['query']} SELECT lga_name,ward_name,   Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status='non collated'""",
    "total_registered_uncollated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status='non collated'""",
    
}

# QUERIES
conditions_state = {
    "total": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1 from wt""",
    "total_registered_votes_table": f"""{presidential_table_ward['query']} select state_name,lga_name,ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt""",
    "total_registered_votes": f"""{presidential_table_ward['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 from wt""",
    'canceled': f"""{presidential_table_ward['query']} SELECT count(*) as  count1 from wt where status ='canceled'""",  
    "canceled_table": f"""{presidential_table_ward['query']} SELECT state_name,lga_name, ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status ='canceled' """,
    "total_registered_canceled_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status ='canceled' """ ,   
    "collated": f"""{presidential_table_ward['query']}  SELECT COUNT(*) as  count1 from wt where   (status = 'collated' OR status='canceled')""",
    "collated_table": f"""{presidential_table_ward['query']} SELECT state_name,lga_name, ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt where  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1   from wt where status='non collated'""",
    "un_collated_table":f"""{presidential_table_ward['query']} SELECT state_name,lga_name,ward_name,   Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status='non collated'""",
    "total_registered_uncollated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status='non collated'""",
    
}


# QUERIES
conditions_country = {
    "total": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1 from wt""",
    "total_registered_votes_table": f"""{presidential_table_ward['query']} select country_name,state_name,lga_name,ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt""",
    "total_registered_votes": f"""{presidential_table_ward['query']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 from wt """,
    'canceled': f"""{presidential_table_ward['query']} SELECT count(*) as  count1 from wt where status ='canceled' """,  
    "canceled_table": f"""{presidential_table_ward['query']} SELECT country_name,state_name,lga_name, ward_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status ='canceled' """,
    "total_registered_canceled_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status ='canceled' """ ,   
    "collated": f"""{presidential_table_ward['query']}  SELECT COUNT(*) as  count1 from wt where   (status = 'collated' OR status='canceled')""",
    "collated_table": f"""{presidential_table_ward['query']} SELECT country_name, state_name,lga_name, ward_name,  Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  from wt where  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{presidential_table_ward['query']} SELECT COUNT(*) as  count1   from wt where status='non collated'""",
    "un_collated_table":f"""{presidential_table_ward['query']} SELECT country_name,state_name,lga_name,ward_name,   Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks from wt where status='non collated'""",
    "total_registered_uncollated_votes": f"""{presidential_table_ward['query']} select COALESCE(sum(Total_Registered_voters),0) as  count1 from wt where status='non collated'""",
    
}

where_list = ["total","total_registered_votes_table","total_registered_votes"]

table_list = ["total_registered_votes_table","canceled_table","collated_table", "un_collated_table"]





#  ward results

def get_ward_ward_all_results(country_name="undefined",state_name="undefined", lga_name="undefined", ward_name="undefined",party_data={}):
    with get_db2() as conn:
        cur = conn.cursor()

          
        country_query = ""
        state_query = ""
        lga_query = ""
        ward_query = ""

     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id={country_name}"
        if state_name and state_name != "undefined":
            state_query = f" AND state_id={state_name}"
        if lga_name and lga_name != "undefined":
            lga_query = f" AND lga_id={lga_name}"
        if ward_name and ward_name != "undefined":
            ward_query = f" AND ward_id={ward_name}"
      
        final_parties = []
        input_parties = []
        for key, value in party_data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)

        ward_result = f"""{presidential_table_ward['query']} select ward_name,  {parties} , Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from wt where 1=1 {state_query} {lga_query} {ward_query} """

        key_values = []
        execute_queries = []
        if country_name or state_name or lga_name or ward_name:
            for key, val in conditions_ward.items():
                
                if key in where_list:
                    val += f" where 1=1 {state_query} {lga_query} {ward_query}"
                else:
                    val += f" and 1=1 {state_query} {lga_query} {ward_query}"

                execute_queries.append(val)
                key_values.append(key)
        execute_queries.append(ward_result)
        key_values.append("result")

        map1 = ['WARD_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        result = ['result']
        mapres =['WARD_NAME']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
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
        

        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

                cur.execute(val)
                # val_results = cur.fetchall()
                val_results = cur.fetch_pandas_all()
                val_results = val_results.to_json(orient="records")
                val_results = json.loads(val_results)

                if key_values[index] in table_list:
                    res = {}
                    if val_results:
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



                elif key_values[index] in result:
                    res = {}
                    filterByKey = lambda keys: {x: val_results[0][x] for x in keys}
                    names = filterByKey(mapres)
                    total =  filterByKey(map2)
                    party = filterByKey(final_parties)
                    res['names'] = names
                    res['parties'] = party
                    res['total'] = total
                   
                    val = [res]
                    final_results[key_values[index]] = val

                
                    # 
                else:
                    final_results[key_values[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)



# lga results

def get_ward_lga_all_results(country_name="undefined",state_name="undefined", lga_name="undefined",party_data={}):
    with get_db2() as conn:
        cur = conn.cursor()
    
        country_query = ""
        state_query = ""
        lga_query = ""
   
     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id={country_name}"
        if state_name and state_name != "undefined":
            state_query = f" AND state_id={state_name}"
        if lga_name and lga_name != "undefined":
            lga_query = f" AND lga_id={lga_name}"
   
        final_parties = []
        input_parties = []
        for key, value in party_data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)

        lga_result = f"""{presidential_table_ward['query']} select  lga_name, {parties},  Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from lgat where 1=1 {state_query} {lga_query} """

        key_values = []
        execute_queries = []
        if country_name or state_name or lga_name:
            for key, val in conditions_lga.items():
                
                if key in where_list:
                    val += f" where 1=1 {state_query} {lga_query}"
                else:
                    val += f" and 1=1 {state_query} {lga_query}"

                execute_queries.append(val)
                key_values.append(key)
        execute_queries.append(lga_result)
        key_values.append("result")

        map1 = ['LGA_NAME','WARD_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        result = ['result']
        mapres =['LGA_NAME']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
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


        

        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

                cur.execute(val)
                # val_results = cur.fetchall()
                val_results = cur.fetch_pandas_all()
                val_results = val_results.to_json(orient="records")
                val_results = json.loads(val_results)

                if key_values[index] in table_list:
                    res = {}
                    if val_results:
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



                elif key_values[index] in result:
                    res = {}

                    filterByKey = lambda keys: {x: val_results[0][x] for x in keys}
                    names = filterByKey(mapres)
                    total =  filterByKey(map2)
                    party = filterByKey(final_parties)
                    res['names'] = names
                    res['parties'] = party
                    res['total'] = total
                   
                    val = [res]
                    final_results[key_values[index]] = val

                
                    # 
                else:
                    final_results[key_values[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)

# state results

def get_ward_state_all_results(country_name="undefined",state_name="undefined",party_data={}):
    with get_db2() as conn:
        cur = conn.cursor()

         
        country_query = ""
        state_query = ""
      

     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id={country_name}"
        if state_name and state_name != "undefined":
            state_query = f" AND state_id={state_name}"
       
        final_parties = []
        input_parties = []
        for key, value in party_data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)

        state_result = f"""{presidential_table_ward['query']} select state_name,  {parties},  Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from st where 1=1 {state_query} """

        key_values = []
        execute_queries = []
        if country_name or state_name:
            for key, val in conditions_state.items():
                
                if key in where_list:
                    val += f" where 1=1 {state_query}"
                else:
                    val += f" and 1=1 {state_query}"

                execute_queries.append(val)
                key_values.append(key)
        execute_queries.append(state_result)
        key_values.append("result")

        map1 = ['STATE_NAME','LGA_NAME','WARD_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        result = ['result']
        mapres =['STATE_NAME']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
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


        

        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

                cur.execute(val)
                # val_results = cur.fetchall()
                val_results = cur.fetch_pandas_all()
                val_results = val_results.to_json(orient="records")
                val_results = json.loads(val_results)

                if key_values[index] in table_list:
                    res = {}
                    if val_results:
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



                elif key_values[index] in result:
                    res = {}

                    filterByKey = lambda keys: {x: val_results[0][x] for x in keys}
                    names = filterByKey(mapres)
                    total =  filterByKey(map2)
                    party = filterByKey(final_parties)
                    res['names'] = names
                    res['parties'] = party
                    res['total'] = total
                   
                    val = [res]
                    final_results[key_values[index]] = val

                
                    # 
                else:
                    final_results[key_values[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)

#  country result table


def get_ward_country_all_results(country_name,party_data={}):
    with get_db2() as conn:
        cur = conn.cursor()
           
        country_query = ""     
        final_results = {}
        if country_name and country_name != "undefined":
            country_query = f" AND country_id={country_name}"
      
        final_parties = []
        input_parties = []
        for key, value in party_data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)

        country_result = f"""{presidential_table_ward['query']} select country_name,  {parties},  Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from ct where 1=1 """
        key_values = []
        execute_queries = []
        if country_name:
            for key, val in conditions_country.items():
                
                if key in where_list:
                    val += f" where 1=1"
                else:
                    val += f" and 1=1"

                execute_queries.append(val)
                key_values.append(key)
        execute_queries.append(country_result)
        key_values.append("result")

        map1 = ['STATE_NAME','LGA_NAME','WARD_NAME']
        map2 = ['TOTAL_REGISTERED_VOTERS','TOTAL_ACCREDITED_VOTERS','TOTAL_REJECTED_VOTES']
        map3 = ['REMARKS']
        result = ['result']
        mapres = ['COUNTRY_NAME']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
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

        

        try:
            
            for index, val in enumerate(execute_queries):
                name_val = []
                total_val = []
                other_val = []

                cur.execute(val)
                # val_results = cur.fetchall()
                val_results = cur.fetch_pandas_all()
                val_results = val_results.to_json(orient="records")
                val_results = json.loads(val_results)

                if key_values[index] in table_list:
                    res = {}
                    if val_results:
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



                elif key_values[index] in result:
                    res = {}
                    filterByKey = lambda keys: {x: val_results[0][x] for x in keys}
                    names = filterByKey(mapres)
                    total =  filterByKey(map2)
                    party = filterByKey(final_parties)
                    res['names'] = names
                    res['parties'] = party
                    res['total'] = total
                   
                    val = [res]
                    final_results[key_values[index]] = val

                
                    # 
                else:
                    final_results[key_values[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)
