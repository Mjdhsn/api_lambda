
presidential_table_pu = {
"query": f"""

WITH pu AS

    (SELECT *, A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP AS total_valid_votes,
    (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP+ Total_Rejected_votes) AS total_vote_casted, 
          
          IFF( Total_Accredited_voters  > Total_Registered_voters, Total_Accredited_voters - Total_Registered_voters,
          IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,

         IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters and 
                   Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
                   'Over Votting!! Because total votes casted are greater than total accredited voters',  
                   IFF( Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total accredited voters are greater than total registered voters', 
                   IFF (status='canceled','canceled',
                   IFF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IFF (status='canceled','canceled',
             IFF (Total_Registered_voters>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 Total_Rejected_votes)/Total_Registered_voters *100,2),'%'), 
                 IFF (Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM SEN_PU_TABLE),

 wt as
		(SELECT distinct ward_id, country_id, country_name,state_id,district_id, state_name,district_name,district_code, lga_id, lga_name, ward_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id,ward_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM SEN_PU_TABLE),
       
 lgat as
		(SELECT distinct lga_id, country_id, country_name,state_id,district_id, state_name,district_name,district_code, lga_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM wt),
       	
dist AS
		(SELECT distinct district_id,state_id, country_id, country_name, state_name,district_name,district_code,
			sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,district_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM lgat),
       	
win AS
         (SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM pu 
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM pu 
          UNION
       	  SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM pu  
          UNION
		  SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM pu   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM pu  
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM pu   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM pu   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM pu 
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM pu 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name,pu_name,pu_id,pu_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM pu  ),
       	
       	
win_w AS
         (SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM wt 
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM wt 
          UNION
       	  SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM wt  
          UNION
		  SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM wt   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM wt  
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM wt   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM wt   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM wt 
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM wt  ),

win_l AS
         (SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM lgat  
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM lgat   
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM lgat  ),
          
win_d AS
         (SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM dist 
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM dist  
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM dist  ),
          
  
win_pu AS
           (SELECT state_id,district_id, lga_id, ward_id,state_name,district_name,district_code,lga_name, ward_name,pu_id,pu_code,pu_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num FROM win),
                
 win_ward AS
           (SELECT state_id,district_id, lga_id, ward_id,state_name,district_name,district_code,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id,district_id, lga_id,state_name,district_name,district_code,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_district AS
           (SELECT state_id,district_id,state_name,district_name,district_code, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name,district_name,district_code ORDER BY votes DESC) AS row_num FROM win_d),
                


non_collated_ward AS 
			(SELECT distinct ward_id,state_id,district_id,lga_id,state_name,district_name,district_code,lga_name, ward_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS total FROM pu),   -- 7. non collated wards
			
non_collated_lga AS 
			(SELECT DISTINCT  lga_id, state_id,district_id,state_name,district_name,district_code,lga_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) AS total FROM pu),

non_collated_district AS 
			(SELECT distinct district_id,state_id,state_name,district_name,district_code,sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total FROM pu),		
			
collated_ward AS 
			(SELECT distinct ward_id,state_id,district_id,lga_id,state_name,district_name,district_code,lga_name, ward_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) - 
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id)) AS deef FROM pu),
			
collated_lga AS
			(SELECT distinct lga_id,state_id,district_id,state_name,district_name,district_code,lga_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) - 
			 sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id)) AS deef FROM pu),
			 
collated_district AS
			(SELECT distinct district_id,state_id,state_name,district_name,district_code, sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) - 
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id)) AS deef FROM pu)


"""

}

presidential_table_ward ={
	
	"query": f"""
WITH wt AS

    (SELECT *, A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP AS total_valid_votes,
    (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP+ Total_Rejected_votes) AS total_vote_casted, 
          
          IFF( Total_Accredited_voters  > Total_Registered_voters, Total_Accredited_voters - Total_Registered_voters,
          IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,

         IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters and 
                   Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
                   'Over Votting!! Because total votes casted are greater than total accredited voters',  
                   IFF( Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total accredited voters are greater than total registered voters', 
                   IFF (status='canceled','canceled',
                   IFF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IFF (status='canceled','canceled',
             IFF (Total_Registered_voters>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 Total_Rejected_votes)/Total_Registered_voters *100,2),'%'), 
                 IFF (Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM SEN_WARD_TABLE),

 
 lgat as
		(SELECT distinct lga_id, country_id, country_name,state_id,district_id, state_name,district_name,district_code, lga_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS Total_Rejected_votes, select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  > select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) - select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  > select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  > select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id))/select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id) *100,2),'%'), 
       IFF (select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id,lga_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM SEN_WARD_TABLE),
       	
dist AS
		(SELECT distinct district_id,state_id, country_id, country_name, state_name,district_name,district_code,
			sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Rejected_votes, select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) - select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,district_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id))/select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id) *100,2),'%'), 
       IFF (select COALESCE(sum(Total_Registered_voters),0)  OVER (PARTITION BY  country_id,state_id,district_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM lgat),
       	
win_w AS
         (SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM wt 
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM wt 
          UNION
       	  SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM wt  
          UNION
		  SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM wt   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM wt  
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM wt   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM wt   
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM wt 
          UNION
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM wt 
          UNION 
          SELECT state_id,district_id, lga_id, ward_id, state_name,district_name,district_code,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM wt  ),

win_l AS
         (SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM lgat  
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM lgat   
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM lgat  ),
          
win_d AS
         (SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM dist 
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM dist  
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM dist  ),
          
  
 win_ward AS
           (SELECT state_id,district_id, lga_id, ward_id,state_name,district_name,district_code,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id,district_id, lga_id,state_name,district_name,district_code,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_district AS
           (SELECT state_id,district_id,state_name,district_name,district_code, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name,district_name,district_code ORDER BY votes DESC) AS row_num FROM win_d),
                


non_collated_ward AS 
			(SELECT distinct ward_id,state_id,district_id,lga_id,state_name,district_name,district_code,lga_name, ward_name,select COALESCE(sum(Total_Registered_voters),0) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS total FROM wt),   -- 7. non collated wards
			
non_collated_lga AS 
			(SELECT DISTINCT  lga_id, state_id,district_id,state_name,district_name,district_code,lga_name,select COALESCE(sum(Total_Registered_voters),0) OVER(PARTITION BY state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) AS total FROM wt),

non_collated_district AS 
			(SELECT distinct district_id,state_id,state_name,district_name,district_code,select COALESCE(sum(Total_Registered_voters),0) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total FROM wt),		
			
collated_ward AS 
			(SELECT distinct ward_id,state_id,district_id,lga_id,state_name,district_name,district_code,lga_name, ward_name,select COALESCE(sum(Total_Registered_voters),0) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id) - 
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id, ward_id)) AS deef FROM wt),
			
collated_lga AS
			(SELECT distinct lga_id,state_id,district_id,state_name,district_name,district_code,lga_name, select COALESCE(sum(Total_Registered_voters),0) OVER(PARTITION BY state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) - 
			 sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id)) AS deef FROM wt),
			 
collated_district AS
			(SELECT distinct district_id,state_id,state_name,district_name,district_code, select COALESCE(sum(Total_Registered_voters),0) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) - 
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id)) AS deef FROM wt)
	"""

}

presidential_table_lga ={
	
	"query": f"""
WITH lgat AS

    (SELECT *, A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP AS total_valid_votes,
    (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP+ Total_Rejected_votes) AS total_vote_casted, 
          
          IFF( Total_Accredited_voters  > Total_Registered_voters, Total_Accredited_voters - Total_Registered_voters,
          IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,

         IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters and 
                   Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
                   'Over Votting!! Because total votes casted are greater than total accredited voters',  
                   IFF( Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total accredited voters are greater than total registered voters', 
                   IFF (status='canceled','canceled',
                   IFF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IFF (status='canceled','canceled',
             IFF (Total_Registered_voters>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 Total_Rejected_votes)/Total_Registered_voters *100,2),'%'), 
                 IFF (Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM SEN_LGA_TABLE),

 dist AS
		(SELECT distinct district_id,state_id, country_id, country_name, state_name,district_name,district_code,
			sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,district_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,district_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,district_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,district_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,district_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,district_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,district_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,district_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM lgat),

win_l AS
         (SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM lgat  
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM lgat 
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM lgat   
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM lgat
          UNION
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM lgat 
          UNION 
          SELECT state_id,district_id, lga_id, state_name,district_name,district_code,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM lgat  ),
          
win_d AS
         (SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM dist 
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM dist  
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM dist  ),

win_lga AS
           (SELECT state_id,district_id, lga_id,state_name,district_name,district_code,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_district AS
           (SELECT state_id,district_id,state_name,district_name,district_code, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name,district_name,district_code ORDER BY votes DESC) AS row_num FROM win_d),
                

non_collated_lga AS 
			(SELECT DISTINCT  lga_id, state_id,district_id,state_name,district_name,district_code,lga_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) AS total FROM lgat),

non_collated_district AS 
			(SELECT distinct district_id,state_id,state_name,district_name,district_code,sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total FROM lgat),		
			

collated_lga AS
			(SELECT distinct lga_id,state_id,district_id,state_name,district_name,district_code,lga_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id) - 
			 sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id,lga_id)) AS deef FROM lgat),
			 
collated_district AS
			(SELECT distinct district_id,state_id,state_name,district_name,district_code, sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) - 
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id)) AS deef FROM lgat)
	"""
	
}

presidential_table_state ={
	
	"query": f"""
WITH dist AS

    (SELECT *, A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP AS total_valid_votes,
    (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP+ Total_Rejected_votes) AS total_vote_casted, 
          
          IFF( Total_Accredited_voters  > Total_Registered_voters, Total_Accredited_voters - Total_Registered_voters,
          IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,

         IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters and 
                   Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
                   'Over Votting!! Because total votes casted are greater than total accredited voters',  
                   IFF( Total_Accredited_voters  > Total_Registered_voters,
                   'Over Votting!! Because total accredited voters are greater than total registered voters', 
                   IFF (status='canceled','canceled',
                   IFF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IFF (status='canceled','canceled',
             IFF (Total_Registered_voters>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 Total_Rejected_votes)/Total_Registered_voters *100,2),'%'), 
                 IFF (Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM SEN_DISTRICT_TABLE),

win_d AS
         (SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM dist 
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM dist  
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM dist
          UNION
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM dist
          UNION 
          SELECT state_id,district_id, state_name,district_name,district_code, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM dist  ),

win_district AS
           (SELECT state_id,district_id,state_name,district_name,district_code, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name,district_name,district_code ORDER BY votes DESC) AS row_num FROM win_d),
                

non_collated_district AS 
			(SELECT distinct district_id,state_id,state_name,district_name,district_code,sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total FROM dist),		
			

collated_district AS
			(SELECT distinct district_id,state_id,state_name,district_name,district_code, sum(Total_Registered_voters) OVER(PARTITION BY state_id,district_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) AS total,
			(sum(case when status = 'collated' OR status = 'canceled' OR status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,district_id) - 
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,district_id)) AS deef FROM dist)

	"""
	
}