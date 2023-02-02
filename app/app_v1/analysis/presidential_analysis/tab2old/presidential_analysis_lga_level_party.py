from app.app_v1.database import get_db



values = {


"lgat":"""
WITH lgat AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + lrt.Total_Rejected_votes)
          AS total_vote_casted, 
          
           IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,


         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + lrt.Total_Rejected_votes > lrt.Total_Accredited_voters and 
                   lrt.Total_Accredited_voters  > lrt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + lrt.Total_Rejected_votes > lrt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( lrt.Total_Accredited_voters  > lrt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non coalated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (lrt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + lrt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 lrt.Total_Rejected_votes)/lrt.Total_Registered_voters *100,2),'%'), 
                 if (lrt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM lga_result_table lrt)


""",

"st":"""
WITH lgat AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + lrt.Total_Rejected_votes)
          AS total_vote_casted, 
          
          IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,


         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + lrt.Total_Rejected_votes > lrt.Total_Accredited_voters and 
                   lrt.Total_Accredited_voters  > lrt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + lrt.Total_Rejected_votes > lrt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( lrt.Total_Accredited_voters  > lrt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non coalated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (lrt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + lrt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 lrt.Total_Rejected_votes)/lrt.Total_Registered_voters *100,2),'%'), 
                 if (lrt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM lga_result_table lrt),

st AS
		(select country_id,country_name,lrt.state_id,lrt.state_name, sum(lrt.A) AS A, sum(lrt.AA) AS AA, sum(lrt.AAC) AS AAC, 
			sum(lrt.ADC) AS ADC, sum(lrt.ADP) AS ADP, sum(lrt.APC) AS APC, sum(lrt.APGA) AS APGA,
			sum(lrt.APM) AS APM, sum(lrt.APP) AS APP, sum(lrt.BP) AS BP, sum(lrt.LP) AS LP,
			sum(lrt.NRM) AS NRM, sum(lrt.NNPP) as NNPP, sum(lrt.PDP) AS PDP, sum(lrt.PRP) AS PRP, 
			sum(lrt.SDP) AS SDP, sum(lrt.YPP) AS YPP, sum(lrt.ZLP) AS ZLP, 
			sum(lrt.Total_Rejected_votes) AS Total_Rejected_votes, sum(lrt.Total_Registered_voters) AS Total_Registered_voters,
			sum(lrt.Total_Accredited_voters) AS Total_Accredited_voters,

			(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
          	sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)) AS total_valid_votes,

          (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
           sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))
          AS total_vote_casted, 
		
 		 IF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > Total_Accredited_voters ,
            (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) - sum(Total_Accredited_voters),
        IF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            sum(Total_Accredited_voters) - sum(Total_Registered_voters), 0)
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
       FROM lga_result_table lrt  GROUP BY lrt.state_id)
""",

"ct":"""
WITH lgat AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + lrt.Total_Rejected_votes)
          AS total_vote_casted, 
          
         IF ( Total_Accredited_voters  > Total_Registered_voters,Total_Accredited_voters - Total_Registered_voters, 
	   if (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + Total_Rejected_votes > Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               Total_Rejected_votes) - Total_Accredited_voters,0)
                 ) AS over_vote_values,


         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + lrt.Total_Rejected_votes > lrt.Total_Accredited_voters and 
                   lrt.Total_Accredited_voters  > lrt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + lrt.Total_Rejected_votes > lrt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( lrt.Total_Accredited_voters  > lrt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non coalated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (lrt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + lrt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 lrt.Total_Rejected_votes)/lrt.Total_Registered_voters *100,2),'%'), 
                 if (lrt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM lga_result_table lrt),

st AS
		(select country_id,country_name,lrt.state_id,lrt.state_name, sum(lrt.A) AS A, sum(lrt.AA) AS AA, sum(lrt.AAC) AS AAC, 
			sum(lrt.ADC) AS ADC, sum(lrt.ADP) AS ADP, sum(lrt.APC) AS APC, sum(lrt.APGA) AS APGA,
			sum(lrt.APM) AS APM, sum(lrt.APP) AS APP, sum(lrt.BP) AS BP, sum(lrt.LP) AS LP,
			sum(lrt.NRM) AS NRM, sum(lrt.NNPP) as NNPP, sum(lrt.PDP) AS PDP, sum(lrt.PRP) AS PRP, 
			sum(lrt.SDP) AS SDP, sum(lrt.YPP) AS YPP, sum(lrt.ZLP) AS ZLP, 
			sum(lrt.Total_Rejected_votes) AS Total_Rejected_votes, sum(lrt.Total_Registered_voters) AS Total_Registered_voters,
			sum(lrt.Total_Accredited_voters) AS Total_Accredited_voters,

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
       FROM lga_result_table lrt  GROUP BY lrt.state_id),
        
ct AS
		(select country_id,country_name, sum(st.A) AS A, sum(st.AA) AS AA, sum(st.AAC) AS AAC, 
			sum(st.ADC) AS ADC, sum(st.ADP) AS ADP, sum(st.APC) AS APC, sum(st.APGA) AS APGA,
			sum(st.APM) AS APM, sum(st.APP) AS APP, sum(st.BP) AS BP, sum(st.LP) AS LP,
			sum(st.NRM) AS NRM, sum(st.NNPP) as NNPP, sum(st.PDP) AS PDP, sum(st.PRP) AS PRP, 
			sum(st.SDP) AS SDP, sum(st.YPP) AS YPP, sum(st.ZLP) AS ZLP, 
			sum(st.Total_Rejected_votes) AS Total_Rejected_votes, sum(st.Total_Registered_voters) AS Total_Registered_voters,
			sum(st.Total_Accredited_voters) AS Total_Accredited_voters,
			
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
       FROM st)

"""

}

values_win = {
"win_l":f"""{values['lgat']} , win_l as 
 (SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, A AS votes, "A" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, AA AS votes, "AA" AS party FROM lgat
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, ADP AS votes, "ADP" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APP AS votes, "APP" AS party FROM lgat
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, AAC AS votes, "AAC" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, ADC AS votes, "ADC" AS party FROM lgat 
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APC AS votes, "APC" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APGA AS votes, "APGA" AS party FROM lgat  
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, APM AS votes, "APM" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, BP AS votes, "BP" AS party FROM lgat 
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, LP AS votes, "LP" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, NRM AS votes, "NRM" AS party FROM lgat 
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, NNPP AS votes, "NNPP" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, PDP AS votes, "PDP" AS party FROM lgat   
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, PRP AS votes, "PRP" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, SDP AS votes, "SDP" AS party FROM lgat
          UNION
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, YPP AS votes, "YPP" AS party FROM lgat 
          UNION 
          SELECT state_name,state_id, lga_id, lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters, ZLP AS votes, "ZLP" AS party FROM lgat  )

""",
"win_s":f"""{values['st']} ,win_s as
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



"non_collated_lga":f"""{values['lgat']}, non_collated_lga as


(SELECT state_id, lga_id,state_name,lga_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total from  lgatgroup by pu.state_id,pu.lga_id )



""",

"non_collated_state": f"""{values['lgat']}, non_collated_state as 

(SELECT state_id,state_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total from  lgatgroup by pu.state_id )


""",


"collated_lga": f"""{values['lgat']},collated_lga as
( select count(*) as total,state_id, lga_id,state_name, lga_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from  lgat group by pu.state_id,pu.lga_id)

""",

"collated_state": f"""{values['lga']},collated_state as

( select count(*) as total,state_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from  lgat group by pu.state_id)

"""

}

win_tab2 = {


"win_lga":f"""{values_win['win_l']}

  (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l) """

,

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



parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')

where_list = ["canceled","canceled_table","total_registered_canceled_voters","collated_table","total_registered_collated_voters","un_collated","un_collated_table","total_registered_uncollated_voters","Over_voting","Over_voting_table","total_over_voting_table"]

# QUERIES
conditions_lga = {
    "total": f"""{values['lgat']} SELECT COUNT(*) as  count1 FROM lgat""",
    "total_registered_votes_table": f"""{values['lgat']} select  lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat""",
    "total_registered_votes": f"""{values['lgat']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat""",
    "canceled": f"""{values['lgat']} SELECT count(*) as  count1 FROM lgat where status ="canceled" """,  
    "canceled_table": f"""{values['lgat']} SELECT lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status ='canceled' """ ,   
    "collated": f"""{values['lgat']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM lgat""",
    "collated_table": f"""{values['lgat']} SELECT  lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM lgat WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['lgat']} SELECT COUNT(*) as  count1   FROM lgat where status='non collated'""",
    "un_collated_table":f"""{values['lgat']} SELECT  lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status='non collated'""",
    "total_registered_voters": f"""{values['lgat']} SELECT Total_Registered_voters  FROM lgat""",
    "total_accredited_voters": f"""{values['lgat']} SELECT Total_Accredited_voters  from lgat""",
    "total_rejected_votes": f"""{values['lgat']} SELECT Total_Rejected_votes   from lgat """,
    "total_valid_votes": f"""{values['lgat']} SELECT total_valid_votes  from lgat """,
    "total_vote_casted": f"""{values['lgat']} SELECT total_vote_casted  from lgat""",
    "percentage_voters_turnout": f"""{values['lgat']} SELECT percentage_voters_turnout  from lgat""",
    "over_voting": f"""{values['lgat']} SELECT count(*) as count1 FROM lgat WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['lgat']} SELECT lga_name,over_vote_values,remarks,percentage_voters_turnout  FROM lgat WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['lgat']} select sum(over_vote_values) as over_votes_figuers FROM lgat WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_l']} SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_l """
}



# QUERIES
conditions_state = {
    "total": f"""{values['lgat']} SELECT COUNT(*) as  count1 FROM lgat""",
    "total_registered_votes_table": f"""{values['lgat']} select state_name, lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat""",
    "total_registered_votes": f"""{values['lgat']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat""",
    "canceled": f"""{values['lgat']} SELECT count(*) as  count1 FROM lgat where status ="canceled" """,  
    "canceled_table": f"""{values['lgat']} SELECT state_name,lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status ='canceled' """ ,   
    "collated": f"""{values['lgat']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM lgat""",
    "collated_table": f"""{values['lgat']} SELECT  state_name, lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM lgat WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['lgat']} SELECT COUNT(*) as  count1   FROM lgat where status='non collated'""",
    "un_collated_table":f"""{values['lgat']} SELECT  state_name,lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status='non collated'""",
    "total_registered_voters": f"""{values['st']} SELECT Total_Registered_voters  FROM st""",
    "total_accredited_voters": f"""{values['st']} SELECT Total_Accredited_voters  from st""",
    "total_rejected_votes": f"""{values['st']} SELECT Total_Rejected_votes   from st """,
    "total_valid_votes": f"""{values['st']} SELECT total_valid_votes  from st """,
    "total_vote_casted": f"""{values['st']} SELECT total_vote_casted  from st""",
    "percentage_voters_turnout": f"""{values['st']} SELECT percentage_voters_turnout  from st""",
    "over_voting": f"""{values['lgat']} SELECT count(*) as count1 FROM lgat WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['lgat']} SELECT state_name,lga_name, over_vote_values,remarks,percentage_voters_turnout  FROM lgat WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['lgat']} select sum(over_vote_values) as over_votes_figuers FROM lgat WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_s']} SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_s """
}

# QUERIES
conditions_country = {
    "total": f"""{values['lgat']} SELECT COUNT(*) as  count1 FROM lgat""",
    "total_registered_votes_table": f"""{values['lgat']} select  state_name, lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat""",
    "total_registered_votes": f"""{values['lgat']} SELECT COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat""",
    "canceled": f"""{values['lgat']} SELECT count(*) as  count1 FROM lgat where status ="canceled" """,  
    "canceled_table": f"""{values['lgat']} SELECT state_name,lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status ="canceled" """,
    "total_registered_canceled_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status ='canceled' """ ,   
    "collated": f"""{values['lgat']}   SELECT sum(case when status = "collated" OR status = "canceled" then 1 else 0 end) as  count1 FROM lgat""",
    "collated_table": f"""{values['lgat']} SELECT  state_name,lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks  FROM lgat WHERE  (status = 'collated' OR status='canceled')""",
    "total_registered_collated_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where (status = 'collated' OR status='canceled')""",
    "un_collated": f"""{values['lgat']} SELECT COUNT(*) as  count1   FROM lgat where status='non collated'""",
    "un_collated_table":f"""{values['lgat']} SELECT state_name, lga_name, Total_Registered_voters,Total_Accredited_voters,Total_Rejected_votes,remarks FROM lgat where status='non collated'""",
    "total_registered_uncollated_voters": f"""{values['lgat']} select COALESCE(sum(Total_Registered_voters),0) as  count1 FROM lgat where status='non collated'""",
    "total_registered_voters": f"""{values['ct']} SELECT Total_Registered_voters  FROM ct""",
    "total_accredited_voters": f"""{values['ct']} SELECT Total_Accredited_voters  from ct""",
    "total_rejected_votes": f"""{values['ct']} SELECT Total_Rejected_votes   from ct """,
    "total_valid_votes": f"""{values['ct']} SELECT total_valid_votes  from ct """,
    "total_vote_casted": f"""{values['ct']} SELECT total_vote_casted  from ct""",
    "percentage_voters_turnout": f"""{values['ct']} SELECT percentage_voters_turnout  from ct""",
    "over_voting": f"""{values['lgat']} SELECT count(*) as count1  FROM lgat WHERE over_vote_values>0""",
    "over_voting_table":f"""{values['lgat']} SELECT state_name,lga_name,over_vote_values,remarks,percentage_voters_turnout  FROM lgat WHERE over_vote_values>0""",
    "total_over_voting": f"""{values['lgat']} select sum(over_vote_values) as over_votes_figuers FROM lgat WHERE over_vote_values>0""",
    "party_graph":f"""{values_win['win_c']} SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes,	
         concat(COALESCE(round(votes/total_vote_casted*100,2),0),'%')  as percentage_votes_casted FROM win_c """
}


parties_values =  "A, AA, ADP, APP, AAC, ADC, APC, APGA, APM, BP, LP, NRM, NNPP, PDP, PRP, SDP, YPP, ZLP".replace(" ", "").split(',')


where_list = ["canceled","canceled_table","total_registered_canceled_voters","collated_table","total_registered_collated_voters","un_collated","un_collated_table","total_registered_uncollated_voters","over_voting","over_voting_table","total_over_voting"]

table_list = ["total_registered_votes_table","canceled_table","collated_table", "un_collated_table","over_voting_table"]




# lga results

def get_lga_lga_all_results(country_name="undefined",state_name="undefined",lga_name="undefined"):
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
            
           
        
        map1 = ['lga_name']
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
def get_lga_state_all_results(country_name="undefined",state_name="undefined"):
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
            
                
                
            
        map1 = ['state_name','lga_name']
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
def get_lga_country_all_results(country_name="undefined"):
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
            
            
        map1 = ['state_name','lga_name']
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