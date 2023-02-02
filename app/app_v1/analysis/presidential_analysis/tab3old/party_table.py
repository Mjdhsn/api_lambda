pu_query = {
"query": f"""

WITH pu AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes)
          AS total_vote_casted, 
          
          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               prt.Total_Rejected_votes) - prt.Total_Accredited_voters,
               IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
               prt.Total_Accredited_voters - prt.Total_Registered_voters, 0)
                 ) AS over_vote_values,

         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters and 
                   prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (prt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 prt.Total_Rejected_votes)/Total_Registered_voters *100,2),'%'), 
                 if (prt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM pu_result_table prt),

 wt as
		(SELECT country_id, country_name, prt.state_id, prt.state_name, prt.lga_id, prt.lga_name, prt.ward_id, prt.ward_name,
			sum(prt.A) AS A, sum(prt.AA) AS AA, sum(prt.AAC) AS AAC, 
			sum(prt.ADC) AS ADC, sum(prt.ADP) AS ADP, sum(prt.APC) AS APC, sum(prt.APGA) AS APGA,
			sum(prt.APM) AS APM, sum(prt.APP) AS APP, sum(prt.BP) AS BP, sum(prt.LP) AS LP,
			sum(prt.NRM) AS NRM, sum(prt.NNPP) as NNPP, sum(prt.PDP) AS PDP, sum(prt.PRP) AS PRP, 
			sum(prt.SDP) AS SDP, sum(prt.YPP) AS YPP, sum(prt.ZLP) AS ZLP, 
			sum(prt.Total_Rejected_votes) AS Total_Rejected_votes, sum(prt.Total_Registered_voters) AS Total_Registered_voters,
			sum(prt.Total_Accredited_voters) AS Total_Accredited_voters, 
			
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
                 
        	FROM pu_result_table prt group by prt.state_id, prt.lga_id, prt.ward_id) ,

lgat AS
		(select country_id, country_name, wt.state_id, wt.state_name, wt.lga_id, wt.lga_name, sum(wt.A) AS A, sum(wt.AA) AS AA, sum(wt.AAC) AS AAC, 
			sum(wt.ADC) AS ADC, sum(wt.ADP) AS ADP, sum(wt.APC) AS APC, sum(wt.APGA) AS APGA,
			sum(wt.APM) AS APM, sum(wt.APP) AS APP, sum(wt.BP) AS BP, sum(wt.LP) AS LP,
			sum(wt.NRM) AS NRM, sum(wt.NNPP) as NNPP, sum(wt.PDP) AS PDP, sum(wt.PRP) AS PRP, 
			sum(wt.SDP) AS SDP, sum(wt.YPP) AS YPP, sum(wt.ZLP) AS ZLP, 
			sum(wt.Total_Rejected_votes) AS Total_Rejected_votes, sum(wt.Total_Registered_voters) AS Total_Registered_voters,
			sum(wt.Total_Accredited_voters) AS Total_Accredited_voters,
			
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
       FROM wt GROUP BY wt.state_id, wt.lga_id),
        
st AS
		(select country_id,country_name,lgat.state_id,lgat.state_name, sum(lgat.A) AS A, sum(lgat.AA) AS AA, sum(lgat.AAC) AS AAC, 
			sum(lgat.ADC) AS ADC, sum(lgat.ADP) AS ADP, sum(lgat.APC) AS APC, sum(lgat.APGA) AS APGA,
			sum(lgat.APM) AS APM, sum(lgat.APP) AS APP, sum(lgat.BP) AS BP, sum(lgat.LP) AS LP,
			sum(lgat.NRM) AS NRM, sum(lgat.NNPP) as NNPP, sum(lgat.PDP) AS PDP, sum(lgat.PRP) AS PRP, 
			sum(lgat.SDP) AS SDP, sum(lgat.YPP) AS YPP, sum(lgat.ZLP) AS ZLP, 
			sum(lgat.Total_Rejected_votes) AS Total_Rejected_votes, sum(lgat.Total_Registered_voters) AS Total_Registered_voters,
			sum(lgat.Total_Accredited_voters) AS Total_Accredited_voters,

			(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
          	sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)) AS total_valid_votes,

          (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
           sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))
          AS total_vote_casted, 
		
 		 IF (((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) > Total_Accredited_voters) ,
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
       FROM lgat GROUP BY lgat.state_id),
        
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
       FROM st),
       
win AS
         (SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM pu 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM pu
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM pu
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM pu
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM pu  
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM pu  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM pu ),

win_w AS
         (SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM wt 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM wt 
          UNION
       	  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM wt  
          UNION
		  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM wt  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM wt 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM wt  ),

win_l AS
         (SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM lgat  
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM lgat   
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM lgat  ),
          
win_s AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM st   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM st   ),
          
  win_c AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, "A" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, "AA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, "ADP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, "APP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, "AAC" AS party FROM ct
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, "ADC" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, "APC" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, "APGA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, "APM" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, "BP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, "LP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, "NRM" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, "NNPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, "PDP" AS party FROM ct  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, "PRP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, "SDP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, "YPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, "ZLP" AS party FROM ct  ),

total_pu AS
         (SELECT * FROM pu_result_table pu),

win_pu AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name,pu_code,pu_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num FROM win),
                
 win_ward AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_state AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_s),
                
win_country AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_c),

non_collated_ward AS 
			(SELECT state_id,lga_id,ward_id,state_name,lga_name, ward_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id, pu.ward_id ),   -- 7. non collated wards
			
non_collated_lga AS 
			(SELECT state_id, lga_id,state_name,lga_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id ) ,

non_collated_state AS 
			(SELECT state_id,state_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id ),			
			
collated_ward AS
			( select count(*) as total,state_id, lga_id, ward_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) AS Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id, pu.ward_id), --  lga level;
			
collated_lga AS
			( select count(*) as total,state_id, lga_id,state_name, lga_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id), -- state

collated_state AS
			( select count(*) as total,state_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id),  -- country    
state_25_above AS 
			(select party, count(state_name) count1  from win_s where votes/total_vote_casted*100>25 
			group by party order by count1 desc),
			
       
diff_bw_1st_2nd AS
			(select sa.party,sa.count1,wc.votes from state_25_above sa left join win_c wc on wc.party=sa.party order by wc.votes desc),

			
exp_winner  AS
			(select party,votes,count1,abs(votes-lead(votes,1) over ()) as diff FROM diff_bw_1st_2nd) 				
			

"""


}


ward_query = {

"query": f"""
WITH wt AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes)
          AS total_vote_casted, 
          
          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               prt.Total_Rejected_votes) - prt.Total_Accredited_voters,
               IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
               prt.Total_Accredited_voters - prt.Total_Registered_voters, 0)
                 ) AS over_vote_values,

         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters and 
                   prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (prt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 prt.Total_Rejected_votes)/prt.Total_Registered_voters *100,2),'%'), 
                 if (prt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM ward_result_table prt),

lgat AS
		(select country_id, country_name, wt.state_id, wt.state_name, wt.lga_id, wt.lga_name, sum(wt.A) AS A, sum(wt.AA) AS AA, sum(wt.AAC) AS AAC, 
			sum(wt.ADC) AS ADC, sum(wt.ADP) AS ADP, sum(wt.APC) AS APC, sum(wt.APGA) AS APGA,
			sum(wt.APM) AS APM, sum(wt.APP) AS APP, sum(wt.BP) AS BP, sum(wt.LP) AS LP,
			sum(wt.NRM) AS NRM, sum(wt.NNPP) as NNPP, sum(wt.PDP) AS PDP, sum(wt.PRP) AS PRP, 
			sum(wt.SDP) AS SDP, sum(wt.YPP) AS YPP, sum(wt.ZLP) AS ZLP, 
			sum(wt.Total_Rejected_votes) AS Total_Rejected_votes, sum(wt.Total_Registered_voters) AS Total_Registered_voters,
			sum(wt.Total_Accredited_voters) AS Total_Accredited_voters,
			
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
       FROM ward_result_table wt GROUP BY wt.state_id, wt.lga_id),
        
st AS
		(select country_id,country_name,lgat.state_id,lgat.state_name, sum(lgat.A) AS A, sum(lgat.AA) AS AA, sum(lgat.AAC) AS AAC, 
			sum(lgat.ADC) AS ADC, sum(lgat.ADP) AS ADP, sum(lgat.APC) AS APC, sum(lgat.APGA) AS APGA,
			sum(lgat.APM) AS APM, sum(lgat.APP) AS APP, sum(lgat.BP) AS BP, sum(lgat.LP) AS LP,
			sum(lgat.NRM) AS NRM, sum(lgat.NNPP) as NNPP, sum(lgat.PDP) AS PDP, sum(lgat.PRP) AS PRP, 
			sum(lgat.SDP) AS SDP, sum(lgat.YPP) AS YPP, sum(lgat.ZLP) AS ZLP, 
			sum(lgat.Total_Rejected_votes) AS Total_Rejected_votes, sum(lgat.Total_Registered_voters) AS Total_Registered_voters,
			sum(lgat.Total_Accredited_voters) AS Total_Accredited_voters,

			(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
          	sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)) AS total_valid_votes,

          (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
           sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))
          AS total_vote_casted, 
		
 		 IF (((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) > Total_Accredited_voters) ,
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
       FROM lgat GROUP BY lgat.state_id),
        
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
       FROM st),
       
win_w AS
         (SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM wt 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM wt 
          UNION
       	  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM wt  
          UNION
		  SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM wt  
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM wt   
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM wt 
          UNION
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM wt 
          UNION 
          SELECT state_id, lga_id, ward_id, state_name,lga_name,ward_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM wt  ),

win_l AS
         (SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM lgat  
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM lgat   
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM lgat  ),
          
win_s AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM st   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM st   ),
          
  win_c AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, "A" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, "AA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, "ADP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, "APP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, "AAC" AS party FROM ct
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, "ADC" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, "APC" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, "APGA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, "APM" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, "BP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, "LP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, "NRM" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, "NNPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, "PDP" AS party FROM ct  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, "PRP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, "SDP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, "YPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, "ZLP" AS party FROM ct  ),

total_pu AS
         (SELECT * FROM pu_result_table pu),

win_pu AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name,pu_code,pu_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num FROM win),
                
 win_ward AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_state AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_s),
                
win_country AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_c),

non_collated_ward AS 
			(SELECT state_id,lga_id,ward_id,state_name,lga_name, ward_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id, pu.ward_id ),   -- 7. non collated wards
			
non_collated_lga AS 
			(SELECT state_id, lga_id,state_name,lga_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id ) ,

non_collated_state AS 
			(SELECT state_id,state_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id ),			
			
collated_ward AS
			( select count(*) as total,state_id, lga_id, ward_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) AS Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id, pu.ward_id), --  lga level;
			
collated_lga AS
			( select count(*) as total,state_id, lga_id,state_name, lga_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id), -- state

collated_state AS
			( select count(*) as total,state_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id),  -- country    
state_25_above AS 
			(select party, count(state_name) count1  from win_s where votes/total_vote_casted*100>25 
			group by party order by count1 desc),
			
       
diff_bw_1st_2nd AS
			(select sa.party,sa.count1,wc.votes from state_25_above sa left join win_c wc on wc.party=sa.party order by wc.votes desc),

			
exp_winner  AS
			(select party,votes,count1,COALESCE(abs(votes-lead(votes,1) over ()),0) as diff FROM diff_bw_1st_2nd) 				
			
"""
}


lga_query = {

"query": f"""
WITH lgat AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes)
          AS total_vote_casted, 
          
          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               prt.Total_Rejected_votes) - prt.Total_Accredited_voters,
               IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
               prt.Total_Accredited_voters - prt.Total_Registered_voters, 0)
                 ) AS over_vote_values,

         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters and 
                   prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (prt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 prt.Total_Rejected_votes)/prt.Total_Registered_voters *100,2),'%'), 
                 if (prt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM lga_result_table prt),


st AS
		(select country_id,country_name,lgat.state_id,lgat.state_name, sum(lgat.A) AS A, sum(lgat.AA) AS AA, sum(lgat.AAC) AS AAC, 
			sum(lgat.ADC) AS ADC, sum(lgat.ADP) AS ADP, sum(lgat.APC) AS APC, sum(lgat.APGA) AS APGA,
			sum(lgat.APM) AS APM, sum(lgat.APP) AS APP, sum(lgat.BP) AS BP, sum(lgat.LP) AS LP,
			sum(lgat.NRM) AS NRM, sum(lgat.NNPP) as NNPP, sum(lgat.PDP) AS PDP, sum(lgat.PRP) AS PRP, 
			sum(lgat.SDP) AS SDP, sum(lgat.YPP) AS YPP, sum(lgat.ZLP) AS ZLP, 
			sum(lgat.Total_Rejected_votes) AS Total_Rejected_votes, sum(lgat.Total_Registered_voters) AS Total_Registered_voters,
			sum(lgat.Total_Accredited_voters) AS Total_Accredited_voters,

			(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
          	sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)) AS total_valid_votes,

          (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
           sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))
          AS total_vote_casted, 
		
 		 IF (((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) > Total_Accredited_voters) ,
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
       FROM lga_result_table lgat GROUP BY lgat.state_id),
        
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
       FROM st),
       
win_l AS
         (SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM lgat  
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM lgat 
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM lgat   
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM lgat
          UNION
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM lgat 
          UNION 
          SELECT state_id, lga_id, state_name,lga_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM lgat  ),
          
win_s AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM st   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM st   ),
          
  win_c AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, "A" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, "AA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, "ADP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, "APP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, "AAC" AS party FROM ct
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, "ADC" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, "APC" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, "APGA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, "APM" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, "BP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, "LP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, "NRM" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, "NNPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, "PDP" AS party FROM ct  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, "PRP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, "SDP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, "YPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, "ZLP" AS party FROM ct  ),

total_pu AS
         (SELECT * FROM pu_result_table pu),

win_pu AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name,pu_code,pu_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num FROM win),
                
 win_ward AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_state AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_s),
                
win_country AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_c),

non_collated_ward AS 
			(SELECT state_id,lga_id,ward_id,state_name,lga_name, ward_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id, pu.ward_id ),   -- 7. non collated wards
			
non_collated_lga AS 
			(SELECT state_id, lga_id,state_name,lga_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id ) ,

non_collated_state AS 
			(SELECT state_id,state_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id ),			
			
collated_ward AS
			( select count(*) as total,state_id, lga_id, ward_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) AS Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id, pu.ward_id), --  lga level;
			
collated_lga AS
			( select count(*) as total,state_id, lga_id,state_name, lga_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id), -- state

collated_state AS
			( select count(*) as total,state_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id),  -- country    
state_25_above AS 
			(select party, count(state_name) count1  from win_s where votes/total_vote_casted*100>25 
			group by party order by count1 desc),
			
       
diff_bw_1st_2nd AS
			(select sa.party,sa.count1,wc.votes from state_25_above sa left join win_c wc on wc.party=sa.party order by wc.votes desc),

			
exp_winner  AS
			(select party,votes,count1,abs(votes-lead(votes,1) over ()) as diff FROM diff_bw_1st_2nd) 				
			
"""

}


state_query = {

"query": f"""

WITH st AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes)
          AS total_vote_casted, 
          
          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               prt.Total_Rejected_votes) - prt.Total_Accredited_voters,
               IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
               prt.Total_Accredited_voters - prt.Total_Registered_voters, 0)
                 ) AS over_vote_values,

         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters and 
                   prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (prt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 prt.Total_Rejected_votes)/prt.Total_Registered_voters *100,2),'%'), 
                 if (prt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout


            FROM state_result_table prt),


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
       FROM state_result_table st),
       

win_s AS
         (SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, A AS votes, "A" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AA AS votes, "AA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADP AS votes, "ADP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APP AS votes, "APP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, AAC AS votes, "AAC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ADC AS votes, "ADC" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APC AS votes, "APC" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APGA AS votes, "APGA" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, APM AS votes, "APM" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, BP AS votes, "BP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, LP AS votes, "LP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NRM AS votes, "NRM" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, NNPP AS votes, "NNPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PDP AS votes, "PDP" AS party FROM st   
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, PRP AS votes, "PRP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, SDP AS votes, "SDP" AS party FROM st 
          UNION
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, YPP AS votes, "YPP" AS party FROM st 
          UNION 
          SELECT state_id, state_name, Total_Registered_voters,total_vote_casted,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes, ZLP AS votes, "ZLP" AS party FROM st   ),
          
  win_c AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, "A" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, "AA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, "ADP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, "APP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, "AAC" AS party FROM ct
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, "ADC" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, "APC" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, "APGA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, "APM" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, "BP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, "LP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, "NRM" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, "NNPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, "PDP" AS party FROM ct  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, "PRP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, "SDP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, "YPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, "ZLP" AS party FROM ct  ),

total_pu AS
         (SELECT * FROM pu_result_table pu),

win_pu AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name,pu_code,pu_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num FROM win),
                
 win_ward AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_state AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_s),
                
win_country AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_c),

non_collated_ward AS 
			(SELECT state_id,lga_id,ward_id,state_name,lga_name, ward_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id, pu.ward_id ),   -- 7. non collated wards
			
non_collated_lga AS 
			(SELECT state_id, lga_id,state_name,lga_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id,pu.lga_id ) ,

non_collated_state AS 
			(SELECT state_id,state_name,Total_Registered_voters, sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) 
			as total FROM pu group by pu.state_id ),			
			
collated_ward AS
			( select count(*) as total,state_id, lga_id, ward_id,state_name,lga_name, ward_name,sum(Total_Registered_voters) AS Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id, pu.ward_id), --  lga level;
			
collated_lga AS
			( select count(*) as total,state_id, lga_id,state_name, lga_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id,pu.lga_id), -- state

collated_state AS
			( select count(*) as total,state_name,sum(Total_Registered_voters) as Total_Registered_voters, 
			(count(*) - sum(status='collated'or status='canceled')) as diff 
			from pu  group by pu.state_id),  -- country    
state_25_above AS 
			(select party, count(state_name) count1  from win_s where votes/total_vote_casted*100>25 
			group by party order by count1 desc),
			
       
diff_bw_1st_2nd AS
			(select sa.party,sa.count1,wc.votes from state_25_above sa left join win_c wc on wc.party=sa.party order by wc.votes desc),

			
exp_winner  AS
			(select party,votes,count1,abs(votes-lead(votes,1) over ()) as diff FROM diff_bw_1st_2nd) 			

"""
}


country_query = {

"query": f"""

WITH ct AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes)
          AS total_vote_casted, 
          
          IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               prt.Total_Rejected_votes) - prt.Total_Accredited_voters,
               IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
               prt.Total_Accredited_voters - prt.Total_Registered_voters, 0)
                 ) AS over_vote_values,

         IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters and 
                   prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                   IF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                   IF (status='canceled','canceled',
                   IF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
             IF (status='canceled','canceled',
             IF (prt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 prt.Total_Rejected_votes)/prt.Total_Registered_voters *100,2),'%'), 
                 if (prt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
                 
                 ) AS percentage_voters_turnout
                 FROM country_result_table prt),

  win_c AS
         (SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, A AS votes, "A" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AA AS votes, "AA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADP AS votes, "ADP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APP AS votes, "APP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, AAC AS votes, "AAC" AS party FROM ct
          UNION 
          SELECT   country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ADC AS votes, "ADC" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APC AS votes, "APC" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APGA AS votes, "APGA" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, APM AS votes, "APM" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, BP AS votes, "BP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, LP AS votes, "LP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NRM AS votes, "NRM" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, NNPP AS votes, "NNPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PDP AS votes, "PDP" AS party FROM ct  
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, PRP AS votes, "PRP" AS party FROM ct
          UNION 
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, SDP AS votes, "SDP" AS party FROM ct
          UNION
          SELECT  country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, YPP AS votes, "YPP" AS party FROM ct
          UNION 
          SELECT country_name,Total_Registered_voters,Total_Accredited_voters,over_vote_values,remarks,total_valid_votes,total_vote_casted, ZLP AS votes, "ZLP" AS party FROM ct  ),


win_country AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
                IF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num FROM win_c)


"""

}
