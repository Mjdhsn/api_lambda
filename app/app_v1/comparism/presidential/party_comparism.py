comparism_table ={
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


            FROM pu_result_table),

 wt as
		(SELECT distinct ward_id, country_id, country_name,state_id, state_name, lga_id, lga_name, ward_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id,ward_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM pu),
       
 lgat as
		(SELECT distinct lga_id, country_id, country_name,state_id, state_name, lga_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,lga_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM wt),
       	
st AS
		(SELECT distinct state_id, country_id, country_name, state_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id) + sum(APP) OVER (PARTITION BY  country_id,state_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM lgat),
       	
ct AS
		(SELECT distinct country_id, country_name,
			sum(A)  OVER (PARTITION BY  country_id) AS A, sum(AA)  OVER (PARTITION BY  country_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id)  + sum(APM) OVER (PARTITION BY  country_id) + sum(APP) OVER (PARTITION BY  country_id) +
			+sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM st),
       	
win AS
         (SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM pu 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM pu
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM pu
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM pu
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM pu  
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM pu ),

win_w AS
         (SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM wt 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM wt 
          UNION
       	  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM wt  
          UNION
		  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM wt  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM wt 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM wt  ),

win_l AS
         (SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM lgat  
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM lgat   
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM lgat  ),
          
win_s AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM st   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM st   ),
          
  win_c AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, 'A' AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, 'AA' AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, 'ADP' AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, 'APP' AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, 'AAC' AS party FROM ct
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, 'ADC' AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, 'APC' AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, 'APGA' AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, 'APM' AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, 'BP' AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, 'LP' AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, 'NRM' AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, 'NNPP' AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, 'PDP' AS party FROM ct  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, 'PRP' AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, 'SDP' AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, 'YPP' AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, 'ZLP' AS party FROM ct  ),

-- ===================for ward_result_table=============================================
wtw AS

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


            FROM WARD_RESULT_TABLE),

 lgatw as
		(SELECT distinct lga_id, country_id, country_name,state_id, state_name, lga_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP) OVER (PARTITION BY  country_id,state_id,lga_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id,lga_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id,lga_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id,lga_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id,lga_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id,lga_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id,lga_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM WARD_RESULT_TABLE),
       	
stw AS
		(SELECT distinct state_id, country_id, country_name, state_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id) + sum(APP) OVER (PARTITION BY  country_id,state_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM lgatw),
       	
ctw AS
		(SELECT distinct country_id, country_name,
			sum(A)  OVER (PARTITION BY  country_id) AS A, sum(AA)  OVER (PARTITION BY  country_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id)  + sum(APM) OVER (PARTITION BY  country_id) + sum(APP) OVER (PARTITION BY  country_id) +
			+sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM stw),
       	
win_ww AS
         (SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM wtw 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM wtw 
          UNION
       	  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM wtw  
          UNION
		  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM wtw   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM wtw  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM wtw   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM wtw   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM wtw 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM wtw 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM wtw  ),

win_lw AS
         (SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM lgatw
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM lgatw
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM lgatw 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM lgatw  
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM lgatw 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM lgatw 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM lgatw   
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM lgatw
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM lgatw 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM lgatw  ),
          
win_sw AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM stw 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM stw 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM stw 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM stw 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM stw 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM stw 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM stw   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM stw 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM stw 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM stw   ),
          
  win_cw AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, 'A' AS party FROM ctw
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, 'AA' AS party FROM ctw
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, 'ADP' AS party FROM ctw
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, 'APP' AS party FROM ctw
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, 'AAC' AS party FROM ctw
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, 'ADC' AS party FROM ctw
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, 'APC' AS party FROM ctw
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, 'APGA' AS party FROM ctw
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, 'APM' AS party FROM ctw
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, 'BP' AS party FROM ctw
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, 'LP' AS party FROM ctw
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, 'NRM' AS party FROM ctw
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, 'NNPP' AS party FROM ctw
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, 'PDP' AS party FROM ctw  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, 'PRP' AS party FROM ctw
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, 'SDP' AS party FROM ctw
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, 'YPP' AS party FROM ctw
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, 'ZLP' AS party FROM ctw  ),          

-- =======================================for lga_result_table===========================================
lgatl AS

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


            FROM LGA_RESULT_TABLE),

       	
stl AS
		(SELECT distinct state_id, country_id, country_name, state_name,
			sum(A)  OVER (PARTITION BY  country_id,state_id) AS A, sum(AA)  OVER (PARTITION BY  country_id,state_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id,state_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id,state_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id,state_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id,state_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id,state_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id,state_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id,state_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id,state_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id,state_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id,state_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id,state_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id,state_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id,state_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id)  + sum(APM) OVER (PARTITION BY  country_id,state_id) + sum(APP) OVER (PARTITION BY  country_id,state_id) +
			+sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id,state_id) + sum(AA)  OVER (PARTITION BY  country_id,state_id) + sum(AAC)  OVER (PARTITION BY  country_id,state_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id,state_id) + sum(ADP)  OVER (PARTITION BY  country_id,state_id) + sum(APC)  OVER (PARTITION BY  country_id,state_id) +
			sum(APGA)  OVER (PARTITION BY  country_id,state_id) + sum(APM)  OVER (PARTITION BY  country_id,state_id) + sum(APP)  OVER (PARTITION BY  country_id,state_id) +
			sum(BP)  OVER (PARTITION BY  country_id,state_id) + sum(LP)  OVER (PARTITION BY  country_id,state_id) + sum(NRM)  OVER (PARTITION BY  country_id,state_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id,state_id) + sum(PDP)  OVER (PARTITION BY  country_id,state_id) + sum(PRP)  OVER (PARTITION BY  country_id,state_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id,state_id) + sum(YPP)  OVER (PARTITION BY  country_id,state_id) + sum(ZLP)  OVER (PARTITION BY  country_id,state_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id,state_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id,state_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM LGA_RESULT_TABLE),
       	
ctl AS
		(SELECT distinct country_id, country_name,
			sum(A)  OVER (PARTITION BY  country_id) AS A, sum(AA)  OVER (PARTITION BY  country_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id)  + sum(APM) OVER (PARTITION BY  country_id) + sum(APP) OVER (PARTITION BY  country_id) +
			+sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM stl),
       	
win_ll AS
         (SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM lgatl
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM lgatl
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM lgatl 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM lgatl  
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM lgatl 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM lgatl 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM lgatl   
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM lgatl
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM lgatl 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM lgatl  ),
          
win_sl AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM stl 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM stl 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM stl 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM stl 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM stl 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM stl 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM stl   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM stl 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM stl 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM stl   ),
          
  win_cl AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, 'A' AS party FROM ctl
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, 'AA' AS party FROM ctl
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, 'ADP' AS party FROM ctl
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, 'APP' AS party FROM ctl
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, 'AAC' AS party FROM ctl
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, 'ADC' AS party FROM ctl
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, 'APC' AS party FROM ctl
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, 'APGA' AS party FROM ctl
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, 'APM' AS party FROM ctl
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, 'BP' AS party FROM ctl
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, 'LP' AS party FROM ctl
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, 'NRM' AS party FROM ctl
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, 'NNPP' AS party FROM ctl
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, 'PDP' AS party FROM ctl  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, 'PRP' AS party FROM ctl
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, 'SDP' AS party FROM ctl
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, 'YPP' AS party FROM ctl
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, 'ZLP' AS party FROM ctl  ),

-- ==========================================for state_result_table=================================
sts AS

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


            FROM STATE_RESULT_TABLE),

   
cts AS
		(SELECT distinct country_id, country_name,
			sum(A)  OVER (PARTITION BY  country_id) AS A, sum(AA)  OVER (PARTITION BY  country_id) AS AA, sum(AAC)  OVER (PARTITION BY  country_id) AS AAC, 
			sum(ADC)  OVER (PARTITION BY  country_id) AS ADC, sum(ADP)  OVER (PARTITION BY  country_id) AS ADP, sum(APC)  OVER (PARTITION BY  country_id) AS APC,
			sum(APGA)  OVER (PARTITION BY  country_id) AS APGA, sum(APM)  OVER (PARTITION BY  country_id) AS APM, sum(APP)  OVER (PARTITION BY  country_id) AS APP,
			sum(BP)  OVER (PARTITION BY  country_id) AS BP, sum(LP)  OVER (PARTITION BY  country_id) AS LP,sum(NRM)  OVER (PARTITION BY  country_id) AS NRM, 
			sum(NNPP)  OVER (PARTITION BY  country_id) as NNPP, sum(PDP)  OVER (PARTITION BY  country_id) AS PDP, sum(PRP)  OVER (PARTITION BY  country_id) AS PRP, 
			sum(SDP)  OVER (PARTITION BY  country_id) AS SDP, sum(YPP)  OVER (PARTITION BY  country_id) AS YPP, sum(ZLP)  OVER (PARTITION BY  country_id) AS ZLP, 
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) AS Total_Rejected_votes, sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) AS Total_Registered_voters,
			sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) AS Total_Accredited_voters, 
			
		   (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)
			) AS total_valid_votes,

          (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))
          AS total_vote_casted,
           
          IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
               sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) - sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
          IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),
          	
          	sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) - sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id),0
			)) AS over_vote_values,
                  
		IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) and 
            sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
            'Over Votting!! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
         IFF (sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id) > sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id) ,
            'Over Votting!! Because total votes casted are greater than total accredited voters',  
        IFF( sum(Total_Accredited_voters)  OVER (PARTITION BY  country_id)  > sum(Total_Registered_voters)  OVER (PARTITION BY  country_id),
           'Over Votting!! Because total accredited voters are greater than total registered voters', 
        IFF(sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id)  + sum(APM) OVER (PARTITION BY  country_id) + sum(APP) OVER (PARTITION BY  country_id) +
			+sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)>0,'OK','non collated')
            ))) AS remarks,
            
            
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)>0,             
           CONCAT(ROUND((sum(A)  OVER (PARTITION BY  country_id) + sum(AA)  OVER (PARTITION BY  country_id) + sum(AAC)  OVER (PARTITION BY  country_id) + 
			sum(ADC)  OVER (PARTITION BY  country_id) + sum(ADP)  OVER (PARTITION BY  country_id) + sum(APC)  OVER (PARTITION BY  country_id) +
			sum(APGA)  OVER (PARTITION BY  country_id) + sum(APM)  OVER (PARTITION BY  country_id) + sum(APP)  OVER (PARTITION BY  country_id) +
			sum(BP)  OVER (PARTITION BY  country_id) + sum(LP)  OVER (PARTITION BY  country_id) + sum(NRM)  OVER (PARTITION BY  country_id) + 
			sum(NNPP)  OVER (PARTITION BY  country_id) + sum(PDP)  OVER (PARTITION BY  country_id) + sum(PRP)  OVER (PARTITION BY  country_id) + 
			sum(SDP)  OVER (PARTITION BY  country_id) + sum(YPP)  OVER (PARTITION BY  country_id) + sum(ZLP)  OVER (PARTITION BY  country_id)+
			sum(Total_Rejected_votes)  OVER (PARTITION BY  country_id))/sum(Total_Registered_voters)  OVER (PARTITION BY  country_id) *100,2),'%'), 
       IFF (sum(Total_Registered_voters)  OVER (PARTITION BY  country_id)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
       AS percentage_voters_turnout 
          
       	FROM sts),
       	
win_ss AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, 'A' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, 'AA' AS party FROM sts 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, 'ADP' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, 'APP' AS party FROM sts 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, 'AAC' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, 'ADC' AS party FROM sts 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, 'APC' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, 'APGA' AS party FROM sts 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, 'APM' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, 'BP' AS party FROM sts 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, 'LP' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, 'NRM' AS party FROM sts 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, 'NNPP' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, 'PDP' AS party FROM sts   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, 'PRP' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, 'SDP' AS party FROM sts 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, 'YPP' AS party FROM sts 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, 'ZLP' AS party FROM sts   ),
          
  win_cs AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, 'A' AS party FROM cts
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, 'AA' AS party FROM cts
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, 'ADP' AS party FROM cts
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, 'APP' AS party FROM cts
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, 'AAC' AS party FROM cts
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, 'ADC' AS party FROM cts
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, 'APC' AS party FROM cts
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, 'APGA' AS party FROM cts
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, 'APM' AS party FROM cts
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, 'BP' AS party FROM cts
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, 'LP' AS party FROM cts
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, 'NRM' AS party FROM cts
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, 'NNPP' AS party FROM cts
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, 'PDP' AS party FROM cts  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, 'PRP' AS party FROM cts
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, 'SDP' AS party FROM cts
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, 'YPP' AS party FROM cts
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, 'ZLP' AS party FROM cts  ),

-- ====================================for country_result_table=========================================       
ctc AS

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


            FROM COUNTRY_RESULT_TABLE),


  win_cc AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, 'A' AS party FROM ctc
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, 'AA' AS party FROM ctc
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, 'ADP' AS party FROM ctc
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, 'APP' AS party FROM ctc
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, 'AAC' AS party FROM ctc
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, 'ADC' AS party FROM ctc
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, 'APC' AS party FROM ctc
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, 'APGA' AS party FROM ctc
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, 'APM' AS party FROM ctc
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, 'BP' AS party FROM ctc
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, 'LP' AS party FROM ctc
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, 'NRM' AS party FROM ctc
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, 'NNPP' AS party FROM ctc
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, 'PDP' AS party FROM ctc  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, 'PRP' AS party FROM ctc
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, 'SDP' AS party FROM ctc
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, 'YPP' AS party FROM ctc
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, 'ZLP' AS party FROM ctc  ),
          
-- ======================= for country_result_table=======================================
win_countryc AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_cc),

-- =====================================================for state_result_table=============================================================================			
win_states AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_ss),
                
win_countrys AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_cs),


non_collated_states AS 
			(SELECT DISTINCT state_id,state_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM sts),		
			
collated_states AS
			(SELECT distinct state_id,state_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM sts),
-- =======================================for lga_result_table==========================================================================
win_lgal AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_ll),
                
win_statel AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_sl),
                
win_countryl AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_cl),

non_collated_lgal AS 
			(SELECT DISTINCT  lga_id, state_id,state_name,lga_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id) AS total FROM lgatl),

non_collated_statel AS 
			(SELECT DISTINCT state_id,state_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM lgatl),		
			

collated_lgal AS
			(SELECT distinct lga_id,state_id,state_name,lga_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id) AS total FROM lgatl),

collated_statel AS
			(SELECT distinct state_id,state_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM lgatl),

-- =============================================for ward_result_table===============================================================================
 win_wardw AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_ww),
                
win_lgaw AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_lw),
                
win_statew AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_sw),
                
win_countryw AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_cw),

non_collated_wardw AS 
			(SELECT distinct ward_id,state_id,lga_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id, ward_id) AS total FROM wtw),   -- 7. non collated wards
			
non_collated_lgaw AS 
			(SELECT DISTINCT  lga_id, state_id,state_name,lga_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id) AS total FROM wtw),

non_collated_statew AS 
			(SELECT DISTINCT state_id,state_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM wtw),		
			
collated_wardw AS 
			(SELECT distinct ward_id,state_id,lga_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id, ward_id) AS total FROM wtw),

collated_lgaw AS
			(SELECT distinct lga_id,state_id,state_name,lga_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id) AS total FROM wtw),

collated_statew AS
			(SELECT distinct state_id,state_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM wtw),
-- ===========================================for pu_result_table=========================================================================
                
win_pu AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name,pu_code,pu_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num FROM win),
                
 win_ward AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_state AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_s),
                
win_country AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IFF (total_vote_casted>0,CONCAT(ROUND(votes/total_vote_casted*100,2),'%'),'Collation has not started') AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_c),

non_collated_ward AS 
			(SELECT distinct ward_id,state_id,lga_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id, ward_id) AS total FROM pu),   -- 7. non collated wards
			
non_collated_lga AS 
			(SELECT DISTINCT  lga_id, state_id,state_name,lga_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id) AS total FROM pu),

non_collated_state AS 
			(SELECT DISTINCT state_id,state_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters, 
			sum(case when status = 'non collated' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM pu),		
			
collated_ward AS 
			(SELECT distinct ward_id,state_id,lga_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id, ward_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id, ward_id) AS total FROM pu),

collated_lga AS
			(SELECT distinct lga_id,state_id,state_name,lga_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id,lga_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id,lga_id) AS total FROM pu),

collated_state AS
			(SELECT distinct state_id,state_name, sum(Total_Registered_voters) OVER(PARTITION BY state_id) AS Total_Registered_voters,
			sum(case when status = 'collated' OR status = 'canceled' then 1 else  0 end) OVER(PARTITION BY state_id) AS total FROM pu),
			

-- ************************************from PU collation table***********************************************************
compare_pu AS
        (SELECT ROW_NUMBER() OVER(PARTITION BY pu_name ORDER BY party ASC) AS row_num,party,votes as Scores,		
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_pu), 

compare_ward AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY party ASC) AS row_num,party,votes as Scores,	 state_id,lga_id,ward_id,
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward), 
        
compare_lga AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY party ASC) AS row_num,party,votes as Scores,state_id,lga_id,		
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_lga),  
        
compare_state AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY party ASC) AS row_num,party,votes as Scores,state_id,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_state), 
        
compare_country AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY party ASC) AS row_num,party,votes as Scores,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_country), 

-- ************************************from ward collation table***********************************************************************
compare_wardw AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY party ASC) AS row_num,party,votes as Scores,	 state_id,lga_id,ward_id,
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_wardw), 
        
compare_lgaw AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY party ASC) AS row_num,party,votes as Scores,state_id,lga_id,		
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_lgaw),  
        
compare_statew AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY party ASC) AS row_num,party,votes as Scores,state_id,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_statew), 
        
compare_countryw AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY party ASC) AS row_num,party,votes as Scores,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_countryw), 
        
-- ===============================================from LGA collation table*=======================================================================
        
compare_lgal AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY party ASC) AS row_num,party,votes as Scores,state_id,lga_id,		
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_lgal),  
        
compare_statel AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY party ASC) AS row_num,party,votes as Scores,state_id,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_statel), 
        
compare_countryl AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY party ASC) AS row_num,party,votes as Scores,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_countryl),        

-- =======================================================from STATE collation table*======================================================================

compare_states AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY party ASC) AS row_num,party,votes as Scores,state_id,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_states), 
        
compare_countrys AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY party ASC) AS row_num,party,votes as Scores,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_countrys),        

-- =============================================================================================================================
compare_countryc AS
 		(SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY party ASC) AS row_num,party,votes as Scores,	
        concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_countryc)        


"""

}