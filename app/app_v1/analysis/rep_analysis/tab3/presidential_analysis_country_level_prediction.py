
from app.app_v1.database import get_db,get_db2
from app.app_v1.analysis.presidential_analysis.tab3_copy.party_table import presidential_table_country
import json



conditions_country = {
      "total_registered_voters": f"""{presidential_table_country['query']} SELECT  Total_Registered_voters as count1 from ct""",
      "total_accredited_voters": f"""{presidential_table_country['query']} SELECT  Total_Accredited_voters as count1 from ct""",
      "total_rejected_votes": f"""{presidential_table_country['query']} SELECT  Total_Rejected_votes as count1  from ct """,
      "total_valid_votes": f"""{presidential_table_country['query']} SELECT total_valid_votes as count1 from ct """,
       "total_vote_casted": f"""{presidential_table_country['query']} SELECT total_vote_casted as count1 from ct""",
       "percentage_voters_turnout": f"""{presidential_table_country['query']} SELECT percentage_voters_turnout as count1  from ct""",
      "total_over_voting": f"""{presidential_table_country['query']} SELECT over_vote_values as count1  from ct """,
      "general_remarks": f"""{presidential_table_country['query']} Select remarks AS REMARKS from ct""",

       "party_table":f"""
       {presidential_table_country['query']} 


	SELECT ROW_NUMBER() OVER(PARTITION BY country_name ORDER BY votes DESC) AS row_num,party,votes as Scores,	
       IFF (total_vote_casted>0, concat(round(votes/total_vote_casted*100,2),'%'),'Collation has not started') as percentage_score FROM win_country
			

       """
}



where_list = []

table_list = ["total_over_voting_table"]


#  country result table
def get_country_country_all_results():
    with get_db2() as conn:
        cur = conn.cursor()
     
        final_results = {}

        key_values = []
        execute_queries = []
        for key, val in conditions_country.items():
            execute_queries.append(val)
            key_values.append(key)
        
        map1 = ['state_name','lga_name','ward_name','pu_code','pu_name']
        map2 = ['Total_Registered_voters','Total_Accredited_voters','Total_Rejected_votes']
        map3 = ['remarks']
        map4 =['over_vote_values','percentage_voters_turnout','Total_Registered_voters']
        query =[]
        for index,sql in enumerate(execute_queries):
                try:
                    cur.execute_async(sql)
                    query.append(cur.sfqid)
                except:
                    print('Skipped a sceanrio')
        ress = {}
        import time
        try:
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
        except Exception as e:
            print(e)
            return str(e)            
       

            
        try:

            for index, val in enumerate(execute_queries):

                name_val = []
                total_val = []
                other_val = []
                
                try:
                    cur.execute(val)
                    val_results = cur.fetch_pandas_all()
                    val_results = val_results.to_json(orient="records")
                    val_results = json.loads(val_results)
                except:
                    print("error")
                
                if key_values[index] in table_list:
                        res = {}

                        if val_results:
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
                            res['names'] = {}
                            res['total'] = {}
                            res['other'] = {}
                            val = [res]
                            final_results[key_values[index]] = val

                else:   
                    if val_results:
                        final_results[key_values[index]] = val_results
                # final_results['message'] = [{"message":"Waiting !!!"}]
            
            return final_results
        except Exception as e:
            print(e)
            return str(e)
            
        # try:

        #     for index, val in enumerate(execute_queries):

        #         name_val = []
        #         total_val = []
        #         other_val = []
                
        #         try:

        #             cur.execute(val)
        #             val_results = cur.fetchall()
        #         except:
        #             print("error")

        #         if key_values[index] in table_list:
        #                 res = {}

        #                 if val_results:
        #                     for val in val_results:
        #                         filterByKey = lambda keys: {x: val[x] for x in keys}
        #                         names = filterByKey(map1)
        #                         total =  filterByKey(map4)
        #                         other =  filterByKey(map3)
        #                         name_val.append(names)
        #                         total_val.append(total)
        #                         other_val.append(other)

        #                     res['names'] = name_val
        #                     res['total'] = total_val
        #                     res['other'] = other_val
        #                     val = [res]
        #                     final_results[key_values[index]] = val

        #                 else:
        #                     res['names'] = {}
        #                     res['total'] = {}
        #                     res['other'] = {}
        #                     val = [res]
        #                     final_results[key_values[index]] = val
        #         else:
        #                 final_results[key_values[index]] = val_results
            
        #     return final_results
        # except Exception as e:
        #     print(e)
        #     return str(e)
