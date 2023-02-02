from app.app_v1.database import get_db,get_db2
from app.app_v1.results.presidential_results.partytable import presidential_table_country
import json



# values_country ={
  
#   "ct":"""
 
#  WITH ct AS

#     (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

#           (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + crt.Total_Rejected_votes)
#           AS total_vote_casted, 
          
#           IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
# 	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#               YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
#               (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                Total_Rejected_votes) - Total_Accredited_voters,0)
#                  ) AS over_vote_values,


#          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + crt.Total_Rejected_votes > crt.Total_Accredited_voters and 
#                    crt.Total_Accredited_voters  > crt.Total_Registered_voters,
#                    'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
#           			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
#                    YPP + ZLP + crt.Total_Rejected_votes > crt.Total_Accredited_voters ,
#                    'Over Votting! Because total votes casted are greater than total accredited voters',  
#                    IF( crt.Total_Accredited_voters  > crt.Total_Registered_voters,
#                    'Over Votting! Because total accredited voters are greater than total registered voters', 
#                    IF (status='canceled','canceled',
#                    IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
#                    )))) AS remarks, 
                 
                 
                 
#              IF (status='canceled','canceled',
#              IF (crt.Total_Registered_voters>0 and
#             	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + crt.Total_Rejected_votes>0,             
#                  CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
#                  crt.Total_Rejected_votes)/crt.Total_Registered_voters *100,2),'%'), 
#                  if (crt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
#                  ) AS percentage_voters_turnout


#             FROM country_result_table crt)
 
#  """

# }




# QUERIES
conditions_country = {
 
    "country_result": f"""{presidential_table_country['query']} select country_name, A,  AA,  AAC, ADC,  ADP,  APC,  APGA,  APM,  APP,  BP,  LP,  NRM, NNPP,  PDP,  PRP,  SDP,  YPP,  ZLP, Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from ct""",
    
}






#  country result table
parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')


def get_country_country_all_results(data={}):
    with get_db2() as conn:
        cur = conn.cursor()
      

        final_parties = []
        input_parties = []
        for key, value in data.items():
            input_parties.append(str(value))
        for party in parties_values:
            if party in input_parties:
                final_parties.append(party)
        parties = ','.join(final_parties)
        
        country_result = f"""{presidential_table_country['query']} select country_name , {parties} , Total_Registered_voters,  Total_Accredited_voters, Total_Rejected_votes from ct """

        final_results = {}

   

        keys = ['result']


        all_lists = [country_result]
            
    
        try:
            for index, val in enumerate(all_lists):
                cur.execute(val)
                # val_results = cur.fetchall()
                val_results = cur.fetch_pandas_all()
                val_results = val_results.to_json(orient="records")
                val_results = json.loads(val_results)
                final_results[keys[index]] = val_results
            return final_results
        except Exception as e:
            print(e)
            return str(e)






