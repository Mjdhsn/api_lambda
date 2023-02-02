WITH pu AS

    (SELECT *, (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP) AS total_valid_votes,

          (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes)
          AS total_vote_casted, 
          
         IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
              YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
              (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
               prt.Total_Rejected_votes) - prt.Total_Accredited_voters,
              IFF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
               prt.Total_Accredited_voters - prt.Total_Registered_voters, 0)
                 ) AS over_vote_values,

        IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters and 
                   prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
          			IFF (A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + 
                   YPP + ZLP + prt.Total_Rejected_votes > prt.Total_Accredited_voters ,
                   'Over Votting! Because total votes casted are greater than total accredited voters',  
                  IFF( prt.Total_Accredited_voters  > prt.Total_Registered_voters,
                   'Over Votting! Because total accredited voters are greater than total registered voters', 
                  IFF (status='canceled','canceled',
                  IFF(A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP>0,'OK','non collated')
                   )))) AS remarks, 
                 
                 
                 
            IFF (status='canceled','canceled',
            IFF (prt.Total_Registered_voters>0 and
            	 A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP + prt.Total_Rejected_votes>0,             
                 CONCAT(ROUND((A + AA + ADP + APP + AAC + ADC + APC + APGA + APM + BP + LP + NRM + NNPP + PDP + PRP + SDP + YPP + ZLP +
                 prt.Total_Rejected_votes)/prt.Total_Registered_voters *100,2),'%'), 
                IFF (prt.Total_Registered_voters<=0,'Warning!! total registered voters = 0','Warning!! validate the result'))
                 
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
		
 		IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > Total_Accredited_voters ,
            (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) - sum(Total_Accredited_voters),
              IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
               sum(Total_Accredited_voters) - sum(Total_Registered_voters), 0)
                 ) AS over_vote_values,
                 
IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) and 
            sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
        IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) ,
            'Over Votting! Because total votes casted are greater than total accredited voters',  
       IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
           'Over Votting! Because total accredited voters are greater than total registered voters', 
       IFF(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)>0,'OK','non collated')
                   ))) AS remarks, 
             
IFF (sum(Total_Registered_voters)>0 and
           sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)>0,             
           CONCAT(ROUND((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))/sum(Total_Registered_voters) *100,2),'%'), 
      IFF (sum(Total_Registered_voters)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
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
		
 		IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > Total_Accredited_voters ,
            (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) - sum(Total_Accredited_voters),
       IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            sum(Total_Accredited_voters) - sum(Total_Registered_voters), 0)
                 ) AS over_vote_values,
                 
		IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) and 
            sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
        IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) ,
            'Over Votting! Because total votes casted are greater than total accredited voters',  
       IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
           'Over Votting! Because total accredited voters are greater than total registered voters', 
       IFF(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)>0,'OK','non collated')
                   ))) AS remarks, 
             
 		IFF (sum(Total_Registered_voters)>0 and
           sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)>0,             
           CONCAT(ROUND((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))/sum(Total_Registered_voters) *100,2),'%'), 
      IFF (sum(Total_Registered_voters)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
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
		
 		IFF (((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) > Total_Accredited_voters) ,
            (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) - sum(Total_Accredited_voters),
       IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            sum(Total_Accredited_voters) - sum(Total_Registered_voters), 0)
                 ) AS over_vote_values,
                 
		IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) and 
            sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
        IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) ,
            'Over Votting! Because total votes casted are greater than total accredited voters',  
       IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
           'Over Votting! Because total accredited voters are greater than total registered voters', 
       IFF(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)>0,'OK','non collated')
                   ))) AS remarks, 
             
 		IFF (sum(Total_Registered_voters)>0 and
           sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)>0,             
           CONCAT(ROUND((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))/sum(Total_Registered_voters) *100,2),'%'), 
      IFF (sum(Total_Registered_voters)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
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
		
 		IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > Total_Accredited_voters ,
            (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)) - sum(Total_Accredited_voters),
       IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            sum(Total_Accredited_voters) - sum(Total_Registered_voters), 0)
                 ) AS over_vote_values,
                 
		IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) and 
            sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
            'Over Votting! Because total votes casted are greater than total accredited voters and also total accredited voters are greater than total registered voters', 
        IFF (sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes) > sum(Total_Accredited_voters) ,
            'Over Votting! Because total votes casted are greater than total accredited voters',  
       IFF( sum(Total_Accredited_voters)  > sum(Total_Registered_voters),
           'Over Votting! Because total accredited voters are greater than total registered voters', 
       IFF(sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
            sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
			sum(SDP)+ sum(YPP) +sum(ZLP)>0,'OK','non collated')
                   ))) AS remarks, 
             
 		IFF (sum(Total_Registered_voters)>0 and
           sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes)>0,             
           CONCAT(ROUND((sum(A) + sum(AA) +sum(AAC) + sum(ADC)+ sum(ADP) + sum(APC) + sum(APGA)+sum(APM)+ 
           sum(APP)+ sum(BP)+ sum(LP) +sum(NRM) +sum(NNPP)+ sum(PDP)+sum(PRP) +
		   sum(SDP)+ sum(YPP) +sum(ZLP) +sum(Total_Rejected_votes))/sum(Total_Registered_voters) *100,2),'%'), 
      IFF (sum(Total_Registered_voters)<=0,'Warning!! total registered voters = 0','Warning!! validate the result')) 
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
               IFF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY pu_code ORDER BY votes DESC) AS row_num FROM win),
                
 win_ward AS
           (SELECT state_id, lga_id, ward_id,state_name,lga_name, ward_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
               IFF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num FROM win_w),
                
win_lga AS
           (SELECT state_id, lga_id,state_name,lga_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
               IFF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num FROM win_l),
                
win_state AS
           (SELECT state_id,state_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
               IFF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
                percentage_votes,party,ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num FROM win_s),
                
win_country AS
           (SELECT country_name, votes, Total_Registered_voters,Total_Accredited_voters,
           		over_vote_values,total_vote_casted,total_valid_votes,remarks,
               IFF (Total_Registered_voters>0,CONCAT(ROUND(votes/total_vote_casted*100,2),"%"),"Error: check Total Registerd voters") AS 
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
			from pu  group by pu.state_id)  -- country                
-- *************************************************When polling Unit checked (page 1)***********************************
-- select case when status='collated' then 'collated'  when status='non collated' then 'non coalated' else 'canceled' end 
-- as 'coallation status' from pu_result_table where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001'; -- 1
-- select (case when over_vote_values >0 then remarks else 'NO Over Voting!!' end) as  over_voting_status from pu 
-- where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001' -- 2
-- select  over_vote_values as Total_over_vote_figures from pu	where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001' -- 3
          
-- SELECT Total_Registered_voters  FROM  pu where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001'; -- 4          
--  SELECT Total_Accredited_voters  from pu where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001' ; -- 5 
--  SELECT Total_Rejected_votes   from pu   where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001'; -- 6 
--  SELECT total_valid_votes  from pu where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001';    -- 7
--   SELECT total_vote_casted  from pu where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001';    -- 8
--  SELECT percentage_voters_turnout  from pu where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001' ;    -- 9        
-- 
--         selectIFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear leading',
-- 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'leading with doubt',
-- 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'lagging with doubt',
-- 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear lagging',''))) )
-- 			as current_update from win_pu where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001'
-- 			 order by current_update desc limit 1 ;   -- 10               
  
--         SELECT ROW_NUMBER() OVER(PARTITION BY pu_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
--         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_pu 
-- where state_id=19 and lga_id=4 and ward_id=7 and pu_code='19/04/07/001'        
-- **********************************************************************************************************************
-- *************************************************When ward checked (page 2 column_II polling unit)***********************************
                
--   SELECT  count(*) from pu_result_table where state_id=19 AND lga_id=4 AND ward_id=7 -- 1. total pus in the ward
--   select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from pu where state_id=19 AND lga_id=4 AND ward_id=8 -- 2. total pus names in the ward
--   SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  FROM pu WHERE state_id=19 AND lga_id=4 AND ward_id=7  ; -- 3. collated_pus
--   SELECT pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_id=19 AND lga_id=4 AND ward_id=7  ; -- 4. names of collated_pus
--   SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  FROM pu WHERE state_id=19 AND lga_id=4 AND ward_id=8 ; -- 5. non_collated_pu  
--   SELECT pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_id=19 AND lga_id=4 AND ward_id=8 ; -- 6. names of non_collated_pu  
--   SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  FROM pu WHERE state_id=19 AND lga_id=4 AND ward_id=7  ; -- 7. canceled_pus
--   SELECT pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_id=19 AND lga_id=4 AND ward_id=7  ; -- 8. names of canceled_pus
-- 	 select count(*) from pu where over_vote_values>0 and state_id=19 AND lga_id=4 AND ward_id=7; -- 8. total pus with over-voting
--   select pu_code, pu_name, ZLP as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
--   from pu where over_vote_values>0 and state_id=19 AND lga_id=4 AND ward_id=7; -- 9. details of pus with over-voting
                
-- clear win
--   select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19 AND lga_id=4 AND ward_id=11
-- select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19 AND lga_id=4 AND ward_id=11;  

-- win with doubt pus
-- select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4 AND ward_id=7 
-- select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4 AND ward_id=7 
 
 -- clear loss pus
-- select count(*) from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4 AND ward_id=7
-- select pu_code, pu_name, votes as Scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4 AND ward_id=7
 
       
-- loss with doubt pus
-- select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu WHERE party="ZLP" and state_id=19 AND lga_id=4 AND ward_id=7
-- select pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4 AND ward_id=7

-- clear pus got 25% and above
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_id=19 AND lga_id=4 AND ward_id=7;
-- SELECT pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from pu where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_id=19 AND lga_id=4 AND ward_id=7;
     
-- pus got 25% and above with doubt
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_id=19 AND lga_id=4 AND ward_id=7;
-- SELECT pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from pu where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_id=19 AND lga_id=4 AND ward_id=7;

-- General party performance
--         selectIFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear leading',
-- 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'leading with doubt',
-- 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'lagging with doubt',
-- 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear lagging',''))) )
-- 			as current_update from win_ward where state_id=19 AND lga_id=4 AND ward_id=7
--  			order by current_update desc limit 1 ;               
-- click to see of general party performance                
--         SELECT ROW_NUMBER() OVER(PARTITION BY ward_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
--         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_ward 
-- where state_id=19 and lga_id=4 and ward_id=7;
-- **********************************************************************************************************************
-- *************************************************When lga checked (page 3, column_II polling unit)***********************************                
                
  --   SELECT  count(*) from pu_result_table where state_id=19 AND lga_id=4-- 1. total pus in the lga
--   select ward_name, pu_code, pu_name,Total_Registered_voters from pu where state_id=19 AND lga_id=4  -- 2. total pus names in the lga
--   SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  FROM pu WHERE state_id=19 AND lga_id=4  ; -- 3. collated_pus
--   SELECT ward_name, pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_id=19 AND lga_id=4  ; -- 4. names of collated_pus
--   SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  FROM pu WHERE state_id=19 AND lga_id=4 ; -- 5. non_collated_pu  
--   SELECT ward_name, pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_id=19 AND lga_id=4 ; -- 6. names of non_collated_pu  
--   SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  FROM pu WHERE state_id=19 AND lga_id=4  ; -- 7. canceled_pus
--   SELECT ward_name, pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_id=19 AND lga_id=4  ; -- 8. names of canceled_pus
-- 	 select count(*) from pu where over_vote_values>0 and state_id=19 AND lga_id=4; -- 8. total pus with over-voting
--   select ward_name, pu_code, pu_name, ZLP as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
--   from pu where over_vote_values>0 and state_id=19 AND lga_id=4; -- 9. details of pus with over-voting

-- clear win
--   select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4
-- select ward_name, pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4   

-- win with doubt pus
-- select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4
-- select ward_name, pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4 
 
 -- clear loss pus
-- select count(*) from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4
-- select ward_name, pu_code, pu_name, votes as Scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4
 
       
-- loss with doubt pus
-- select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu WHERE party="ZLP" and state_id=19 AND lga_id=4
-- select ward_name, pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4

-- clear pus got 25% and above
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_id=19 AND lga_id=4;
-- SELECT ward_name, pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from pu where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_id=19 AND lga_id=4
     
-- pus got 25% and above with doubt
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_id=19 AND lga_id=4;
-- SELECT ward_name, pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from pu where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_id=19 AND lga_id=4
                
-- *************************************************When lga checked (page 3, column_III wards)******************* 

--  SELECT  count(*) from wt where state_id=19 AND lga_id=4 -- 1. total wards in the lga
-- 	select ward_name,Total_Registered_voters from wt where state_id=19 AND lga_id=4 -- 2. total ward names in the lga
-- select count(*) as collated from collated_ward where diff=0 -- 3. nos. of collated wards
-- select ward_name,Total_Registered_voters from collated_ward where diff=0	-- 4. names of collated wards	
-- select count(*) as in_progress from collated_ward where (diff>0 and diff<total) and state_id=19 and lga_id=4	-- 5. no. of collation in progress ward
-- select ward_name,Total_Registered_voters from collated_ward where (diff>0 and diff<total) and state_id=19 and lga_id=4  -- 6. names of collation in progress ward
-- select count(*)  from non_collated_ward  where total=0 and state_id=19 AND lga_id=4  -- 7. non collated wards
-- select ward_name,Total_Registered_voters  from non_collated_ward  where total=0 and state_id=19 AND lga_id=4

-- clear win
--    select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19 and lga_id=4
-- select ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
--  WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19 AND lga_id=4   

 -- clear loss wts
--  select count(*) from win_ward WHERE row_num>1 and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4
-- select ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
-- WHERE row_num>1  and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 AND lga_id=4
 
-- win with doubt wts
-- select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP" and state_id=19 AND lga_id=4
-- select ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP" and state_id=19 AND lga_id=4  
        
-- loss with doubt wts
-- select sum(case when row_num>1  and total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) as count from win_ward WHERE party="ZLP" and state_id=19 AND lga_id=4 
-- select ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
-- WHERE row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4 

-- clear wards got 25% and above
--  SELECT COUNT(*)  FROM wt WHERE NNPP/total_vote_casted*100>=25 and remarks='OK' AND state_id=19 AND lga_id=4;
-- SELECT ward_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from wt where NNPP/total_vote_casted*100>=25 and remarks='OK' and state_id=19 AND lga_id=4;
     
-- wts got 25% and above with doubt
--  SELECT COUNT(*) FROM wt WHERE NNPP/total_vote_casted*100>=25 and over_vote_values>0 AND state_id=19 AND lga_id=4 ;
-- SELECT ward_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from wt where NNPP/total_vote_casted*100>=25 and over_vote_values>0 and state_id=19 AND lga_id=4 ;
    
-- General party performance
--         selectIFF (row_num<2 AND total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear leading',
-- 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'leading with doubt',
-- 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'lagging with doubt',
-- 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear lagging',''))) )
-- 			as current_update from win_lga where state_id=19 AND lga_id=4
--  			order by current_update desc limit 1 ;               
-- click to see of general party performance                
--         SELECT ROW_NUMBER() OVER(PARTITION BY lga_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
--         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_lga where state_id=19 and lga_id=4  
			
-- **********************************************************************************************************************
-- *************************************************When state checked (page 4, column_II polling unit)***********************************                
                
  --   SELECT  count(*) from pu_result_table where state_id=19 -- 1. total pus in the lga
--   select lga_name,ward_name,pu_code, pu_name,Total_Registered_voters from pu where state_id=19   -- 2. total pus names in the lga
--   SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  FROM pu WHERE state_id=19   ; -- 3. collated_pus
--   SELECT lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_id=19   ; -- 4. names of collated_pus
--   SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  FROM pu WHERE state_id=19  ; -- 5. non_collated_pu  
--   SELECT lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_id=19  ; -- 6. names of non_collated_pu  
--   SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  FROM pu WHERE state_id=19   ; -- 7. canceled_pus
--   SELECT lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  and state_id=19   ; -- 8. names of canceled_pus
-- 	 select count(*) from pu where over_vote_values>0 and state_id=19 ; -- 8. total pus with over-voting
--   select lga_name,ward_name,pu_code, pu_name, ZLP as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
--   from pu where over_vote_values>0 and state_id=19 ; -- 9. details of pus with over-voting

	
-- clear win
--   select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19
-- select lga_name,ward_name, pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19    

-- win with doubt pus
-- select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19
-- select lga_name,ward_name, pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19  
 
 -- clear loss pus
-- select count(*) from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 
-- select lga_name,ward_name, pu_code, pu_name, votes as Scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 
 
       
-- loss with doubt pus
-- select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu WHERE party="ZLP" and state_id=19
-- select lga_name,ward_name, pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19 AND lga_id=4

-- clear pus got 25% and above
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' AND state_id=19;
-- SELECT lga_name,ward_name, pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from pu where ZLP/total_vote_casted*100>=25 and remarks='OK' and state_id=19 
     
-- pus got 25% and above with doubt
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 AND state_id=19;
-- SELECT lga_name,ward_name, pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from pu where ZLP/total_vote_casted*100>=25 and over_vote_values>0 and state_id=19 
                
-- *************************************************When state checked (page 4, column_III wards)******************* 
 
--  SELECT  count(*) wt where state_id=19  -- 1. total wards in the state
-- 	select lga_name,ward_name,Total_Registered_voters from wt where state_id=19  -- 2. total ward names in the state
-- select count(*) as collated from collated_ward where diff=0 and state_id=19 -- 3. nos. of collated wards
-- select ward_name,Total_Registered_voters from collated_ward where diff=0 and state_id=19	-- 4. names of collated wards	
-- select count(*) as in_progress from collated_ward where (diff>0 and diff<total) and state_id=19	-- 5. no. of collation in progress ward
-- select ward_name,Total_Registered_voters from collated_ward where (diff>0 and diff<total) and state_id=19 -- 6. names of collation in progress ward
-- select count(*)  from non_collated_ward  where total=0 and state_id=19  -- 7. non collated wards
-- select ward_name,Total_Registered_voters  from non_collated_ward  where total=0 and state_id=19 

-- clear win
--    select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19 
-- select lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
--  WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19    

 -- clear loss wts
--  select count(*) from win_ward WHERE row_num>1 and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 
-- select lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
-- WHERE row_num>1  and total_valid_votes>0 and remarks='OK' AND party="ZLP" and state_id=19 
 
-- win with doubt wts
-- select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP" and state_id=19 
-- select lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP" and state_id=19   
        
-- loss with doubt wts
-- select sum(case when row_num>1  and total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) as count from win_ward WHERE party="ZLP" and state_id=19  
-- select lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
-- WHERE row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19  

-- clear wards got 25% and above
--  SELECT COUNT(*)  FROM wt WHERE NNPP/total_vote_casted*100>=25 and remarks='OK' AND state_id=19 ;
-- SELECT lga_name,ward_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from wt where NNPP/total_vote_casted*100>=25 and remarks='OK' and state_id=19 ;
     
-- wts got 25% and above with doubt
--  SELECT COUNT(*) FROM wt WHERE NNPP/total_vote_casted*100>=25 and over_vote_values>0 AND state_id=19  ;
-- SELECT lga_name,ward_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from wt where NNPP/total_vote_casted*100>=25 and over_vote_values>0 and state_id=19  ;
    

-- *************************************************When state checked (page 4, column_IV lga)******************* 
	
--  select count(*) from lgat where state_id=19 -- 1. total lgas in the state
-- 	select lga_name,Total_Registered_voters from lgat where state_id=19;  -- 2. lga names in the state
-- select count(*) as collated from collated_ward where diff=0 and state_id=19; -- 3. nos. of collated wards
-- select ward_name,Total_Registered_voters from collated_ward where diff=0 and state_id=19; -- 4. names of collated wards	
-- select count(*) as in_progress from collated_ward where (diff>0 and diff<total) and state_id=19-- 5. no. of collation in progress ward
-- select ward_name,Total_Registered_voters from collated_ward where (diff>0 and diff<total) and state_id=19;  -- 6. names of collation in progress ward
-- select count(*)  from non_collated_ward  where total=0 and state_id=19 AND lga_id=4  -- 7. non collated wards
-- select ward_name,Total_Registered_voters  from non_collated_ward  where total=0 and state_id=19;
			
-- clear win lgas
--    select count(*) from win_lga WHERE row_num<2 AND total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19; 
-- select lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga
--  WHERE row_num<2 AND total_valid_votes>0 and remarks='OK' AND party="NNPP" and state_id=19; 

 -- clear loss lgas
--  select count(*) from win_lga WHERE row_num>1 and total_valid_votes>0 and remarks='OK' AND party="ZLP"  and state_id=19; 
-- select lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga
-- WHERE row_num>1  and total_valid_votes>0 and remarks='OK' AND party="ZLP"  and state_id=19; 
 
-- win with thought lgas
-- select count(*) from win_lga WHERE row_num<2 AND total_valid_votes>0 and over_vote_values>0 AND party="NNPP"  
-- select lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP" and state_id=19; 

-- loss with thought lgas
-- select sum(case when row_num>1  and total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) as count from win_lga WHERE party="ZLP"  
-- select lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga
-- WHERE row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" and state_id=19;  

-- clear lgas got 25% and above
--  SELECT COUNT(*)  from lgat where state_id=19 and NNPP/total_vote_casted*100>=25 and remarks='OK' ;
-- SELECT lga_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from lgat where state_id=19 and NNPP/total_vote_casted*100>=25 and remarks='OK' and state_id=19; 
--      
-- lgas got 25% and above with thought
--  SELECT COUNT(*) from lgat where state_id=19 and NNPP/total_vote_casted*100>=25 and over_vote_values>0  ;
-- SELECT lga_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from lgat where state_id=19 and NNPP/total_vote_casted*100>=25 and over_vote_values>0  and state_id=19;

			
-- General party performance in the state
--         selectIFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear leading',
-- 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'leading with doubt',
-- 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'lagging with doubt',
-- 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear lagging',''))) )
-- 			as current_update from win_state where state_id=19 
--  			order by current_update desc limit 1 ;  
             
-- click to see of general party performance in the state             
--         SELECT ROW_NUMBER() OVER(PARTITION BY state_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
--         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_state where state_id=19   
			
-- **********************************************************************************************************************
-- *************************************************When country checked (page 5, column_II polling unit)***********************************                
                
--     SELECT  count(*) from pu_result_table  -- 1. total pus in the country
--   select state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters from pu    -- 2. total pus names in the country
--   SELECT sum(case when status = 'collated' OR status='canceled' then 1 else  0 end) as count  FROM pu    ; -- 3. collated_pus
--   SELECT state_name,lga_name,ward_name,pu_code, pu_name,ZLP as scores,total_vote_casted, remarks   FROM pu WHERE  status = 'collated' OR status='canceled' and state_id=19   ; -- 4. names of collated_pus
--   SELECT sum(case when status = 'non collated'  then 1 else  0 end) as count  FROM pu   ; -- 5. non_collated_pu  
--   SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status='non collated' and state_id=19  ; -- 6. names of non_collated_pu  
--   SELECT sum(case when status = 'canceled'  then 1 else  0 end) as count  FROM pu    ; -- 7. canceled_pus
--   SELECT state_name,lga_name,ward_name,pu_code, pu_name,Total_Registered_voters, remarks   FROM pu WHERE status = 'canceled'  ; -- 8. names of canceled_pus
-- 	 select count(*) from pu where over_vote_values>0 and state_id=19 ; -- 8. total pus with over-voting
--   select state_name,lga_name,ward_name,pu_code, pu_name, ZLP as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values,remarks as Remarks
--   from pu where over_vote_values>0 and state_id=19 ; -- 9. details of pus with over-voting

-- clear win
--   select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="ZLP"
-- select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="ZLP"  

-- win with doubt pus
-- select count(*) from win_pu WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP" 
-- select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="ZLP";
 
 -- clear loss pus
-- select count(*) from win_pu WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP"; 
-- select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote _casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and remarks='OK' AND party="ZLP"; 
 
       
-- loss with doubt pus
-- select sum(case when row_num>1 AND total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) from win_pu WHERE party="ZLP"
-- select state_name,lga_name,ward_name,pu_code, pu_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_pu 
-- WHERE row_num>1 AND total_valid_votes>0 and over_vote_values>0 AND party="ZLP";

-- clear pus got 25% and above
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and remarks='OK' ;
-- SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from pu where ZLP/total_vote_casted*100>=25 and remarks='OK'; 
     
-- pus got 25% and above with doubt
--  SELECT COUNT(*) AS PUs_got_25_percent_and_aAbove FROM pu WHERE ZLP/total_vote_casted*100>=25 and over_vote_values>0 ;
-- SELECT state_name,lga_name,ward_name,pu_code, pu_name, ZLP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(ZLP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from pu where ZLP/total_vote_casted*100>=25 and over_vote_values>0; 
                
-- *************************************************When country checked (page 5, column_III wards)******************* 
 
--  SELECT  count(*) wt   -- 1. total wards in the state
-- 	select state_name,lga_name,ward_name,Total_Registered_voters from wt   -- 2. total ward names in the state
-- select count(*) as collated from collated_ward where diff=0 -- 3. nos. of collated wards
-- select state_name,lga_name,ward_name,Total_Registered_voters from collated_ward where diff=0	-- 4. names of collated wards	
-- select count(*) as in_progress from collated_ward where (diff>0 and diff<total); 	-- 5. no. of collation in progress ward
-- select state_name,lga_name,ward_name,Total_Registered_voters from collated_ward where (diff>0 and diff<total); -- 6. names of collation in progress ward
-- select count(*)  from non_collated_ward  where total=0   -- 7. non collated wards
-- select state_name,lga_name,ward_name,Total_Registered_voters  from non_collated_ward  where total=0;

-- clear win
--    select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP" 
-- select state_name,lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
--  WHERE row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP"    

 -- clear loss wts
--  select count(*) from win_ward WHERE row_num>1 and total_valid_votes>0 and remarks='OK' AND party="ZLP" 
-- selectstate_name,lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_ward 
-- WHERE row_num>1  and total_valid_votes>0 and remarks='OK' AND party="ZLP"
 
-- win with doubt wts
-- select count(*) from win_ward WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP";
-- selectstate_name,lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_ward 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP"    
        
-- loss with doubt wts
-- select sum(case when row_num>1  and total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) as count from win_ward WHERE party="ZLP"
-- selectstate_name,lga_name,ward_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_ward 
-- WHERE row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="ZLP"  

-- clear wards got 25% and above
--  SELECT COUNT(*)  FROM wt WHERE NNPP/total_vote_casted*100>=25 and remarks='OK' ;
-- SELECTstate_name,lga_name,ward_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from wt where NNPP/total_vote_casted*100>=25 and remarks='OK' ;
     
-- wts got 25% and above with doubt
--  SELECT COUNT(*) FROM wt WHERE NNPP/total_vote_casted*100>=25 and over_vote_values>0 AND state_id=19  ;
-- SELECTstate_name,lga_name,ward_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from wt where NNPP/total_vote_casted*100>=25 and over_vote_values>0   ;
    

-- *************************************************When country checked (page 5, column_IV lga)******************* 
	
--  select count(*) from lgat  -- 1. total lgas in the state
-- 	select state_name,lga_name,Total_Registered_voters from lgat   -- 2. lga names in the state
-- select count(*) as collated from collated_lga where diff=0 -- 3. nos. of collated wards
-- select state_name,lga_name,Total_Registered_voters from collated_lga where diff=0	-- 4. names of collated wards	
-- select count(*) as in_progress from collated_lga where (diff>0 and diff<total) and state_id=19 and lga_id=4	-- 5. no. of collation in progress ward
-- select state_name,lga_name,Total_Registered_voters from collated_lga where (diff>0 and diff<total)  -- 6. names of collation in progress ward
-- select count(*)  from non_collated_lga  where total=0   -- 7. non collated wards
-- select state_name,lga_name,Total_Registered_voters  from non_collated_lga where total=0 
		
-- clear win lgas
--    select count(*) from win_lga WHERE row_num<2 AND total_valid_votes>0 and remarks='OK' AND party="NNPP" 
-- select state_name,lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga
--  WHERE row_num<2 AND total_valid_votes>0 and remarks='OK' AND party="NNPP"     

 -- clear loss lgas
--  select count(*) from win_lga WHERE row_num>1 and total_valid_votes>0 and remarks='OK' AND party="ZLP"  
-- select state_name,lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_lga
-- WHERE row_num>1  and total_valid_votes>0 and remarks='OK' AND party="ZLP"  
 
-- win with thought lgas
-- select count(*) from win_lga WHERE row_num<2 AND total_valid_votes>0 and over_vote_values>0 AND party="NNPP"  
-- select state_name,lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_lga 
-- WHERE row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP"   

-- loss with thought lgas
-- select sum(case when row_num>1  and total_valid_votes>0 and over_vote_values>0 then 1 else 0 end) as count from win_lga WHERE party="ZLP"  
-- select state_name,lga_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_lga
-- WHERE row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="ZLP"  

-- clear lgas got 25% and above
--  SELECT COUNT(*)  from lgat  and NNPP/total_vote_casted*100>=25 and remarks='OK' ;
-- SELECT state_name,lga_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from lgat  and NNPP/total_vote_casted*100>=25 and remarks='OK' ;
--      
-- lgas got 25% and above with thought
--  SELECT COUNT(*) from lgat  and NNPP/total_vote_casted*100>=25 and over_vote_values>0  ;
-- SELECT state_name,lga_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from lgat  and NNPP/total_vote_casted*100>=25 and over_vote_values>0  ;

-- *************************************************When country checked (page 5, column_V states)******************* 			
--  select count(*) from st -- 1. total lgas in the state
-- 	select state_name,Total_Registered_voters from st  -- 2. lga names in the state
-- select count(*) as collated from collated_state where diff=0 -- 3. nos. of collated wards
-- select state_name,Total_Registered_voters from collated_state where diff=0	-- 4. names of collated wards	
-- select count(*) as in_progress from collated_state where (diff>0 and diff<total) -- 5. no. of collation in progress ward
-- select state_name,Total_Registered_voters from collated_state where (diff>0 and diff<total);  -- 6. names of collation in progress ward
-- select count(*)  from non_collated_state  where total=0;  -- 7. non collated wards
-- select state_name,Total_Registered_voters  from non_collated_state  where total=0;
 
-- clear win
--    select count(*) from win_state WHERE row_num<2 AND total_valid_votes>0 and remarks='OK' AND party="NNPP" 
-- select state_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
--  WHERE row_num<2 AND total_valid_votes>0 and remarks='OK' AND party="NNPP"     

 -- clear loss sts
--  select count(*) from win_state WHERE row_num>1 and total_valid_votes>0 and remarks='OK' AND party="ZLP"  
-- select state_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, percentage_votes from win_state 
-- WHERE row_num>1  and total_valid_votes and remarks='OK' AND party="ZLP"  
 
-- win with thought sts
-- select count(*) from win_state WHERE row_num<2 AND total_valid_votes>0 and over_vote_values >0 AND party="NNPP"  
-- select state_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, percentage_votes, remarks from win_state 
-- WHERE row_num<2 AND total_valid_votes>0 and over_vote_values >0 AND party="NNPP"   
        
-- loss with thought sts
-- select sum(case when row_num>1  and total_valid_votes and over_vote_values >0 then 1 else 0 end) as count from win_state WHERE party="ZLP"  
-- select state_name, votes as Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, over_vote_values, percentage_votes, remarks from win_state 
-- WHERE row_num>1  and total_valid_votes and over_vote_values >0 AND party="ZLP"  

-- clear sts got 25% and above
--  SELECT COUNT(*)  FROM st WHERE NNPP/total_vote_casted*100>=25 and remarks='OK' ;
-- SELECT state_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks 
-- from st where NNPP/total_vote_casted*100>=25 and remarks='OK' ;
--      
-- sts got 25% and above with thought
--  SELECT COUNT(*) FROM st WHERE NNPP/total_vote_casted*100>=25 and over_vote_values >0  ;
-- SELECT state_name, NNPP AS Scores, total_vote_casted,Total_Registered_voters,Total_Accredited_voters,over_vote_values, CONCAT(ROUND(NNPP/total_vote_casted*100,2),"%") AS percentage_votes,remarks AS Remarks  
-- from st where NNPP/total_vote_casted*100>=25 and over_vote_values >0  ;
		
-- General party performance in the state
--         selectIFF (row_num<2 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear leading',
-- 		if(row_num<2 and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'leading with doubt',
-- 			if( row_num>1  and total_valid_votes>0 and over_vote_values>0 AND party="NNPP",'lagging with doubt',
-- 			if(row_num>1 and total_valid_votes>0 and remarks='OK' AND party="NNPP",'clear lagging',''))) )
-- 			as current_update from win_country  
--  			order by current_update desc limit 1 ;  
             
-- click to see of general party performance in the state             
--         SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes as Scores,		-- 11
--         concat(round(votes/total_vote_casted*100,2),'%') as percentage_score FROM win_country    			