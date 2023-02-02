from app.app_v1.database import get_db,get_db2
from datetime import datetime
from datetime import datetime
import json

def submit_data(user,userdata_collate):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()

        if role_input == "pns":
            country_name = level_input['country']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            query = ", ".join(query)
            sql = f"""Update country_result_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} """
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "pss":
            country_name = level_input['country']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
            f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]

            query = ", ".join(query)
            sql = f"""Update state_result_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} """
            print(sql)
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
                
        elif role_input == "pls":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update lga_result_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name}"""
            print(sql)
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "pws":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update ward_result_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update pu_result_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "sds":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update sen_district_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name}  and state_id= {state_name} and district_id = {district_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "sls":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update sen_lga_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and district_id = {district_name} and lga_id= {lga_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "sws":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update sen_ward_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and district_id = {district_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "spa":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update sen_pu_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and  state_id= {state_name}  and district_id = {district_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "rcs":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update rep_constituency_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name}  and state_id= {state_name} and const_id= {constituency_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "rls":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update rep_lga_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and const_id= {constituency_name} and lga_id= {lga_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "rws":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update rep_ward_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and const_id= {constituency_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "rpa":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update rep_pu_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and  state_id= {state_name}  and const_id= {constituency_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

def cancel_data(user,userdata_collate):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
        if role_input == "pns":
            country_name = level_input['country']
            timer = now.strftime("%m/%d/%Y %H:%M")
  
            sql = f"""Update country_result_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
  Where country_id = {country_name} """
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "pss":
            country_name = level_input['country']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
       
            sql = f"""Update state_result_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} """
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
                
        elif role_input == "pls":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")

            sql = f"""Update lga_result_table SET  status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "pws":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")

            sql = f"""Update ward_result_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
  
            sql = f"""Update pu_result_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "sds":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")

            sql = f"""Update sen_district_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name}  and state_id= {state_name} and district_id = {district_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "sls":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")

            sql = f"""Update sen_lga_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and district_id = {district_name} and lga_id= {lga_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "sws":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
      
            sql = f"""Update sen_ward_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and district_id = {district_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "spa":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
  
            sql = f"""Update sen_pu_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and  state_id= {state_name}  and district_id = {district_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "rcs":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")

            sql = f"""Update rep_constituency_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name}  and state_id= {state_name} and const_id= {constituency_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "rls":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            sql = f"""Update rep_lga_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and const_id= {constituency_name} and lga_id= {lga_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "rws":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
    
            sql = f"""Update rep_ward_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and const_id= {constituency_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "rpa":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
          
            sql = f"""Update rep_pu_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0,Total_Registered_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and  state_id= {state_name}  and const_id= {constituency_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
    
        
def upload_data(user,userdata_postmedia):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    phone = user_data['phone']
    email = user_data['email']

    now = datetime.now() 
    remark = userdata_postmedia['remark']
        #ml = userdata_postmedia['ml']

    file = userdata_postmedia['file']
    type = userdata_postmedia['type']
    lat = userdata_postmedia['lat']
    long = userdata_postmedia['long']
    
    with get_db2() as conn:
        cur = conn.cursor()
        if role_input == "pns":
            country_name = level_input['country']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_country
                            (country_id,
                            remark,
                        
                            file,
                            file_type,
                            lat,
                            long,
                            phone,
                            email,
                            date

                            )
                            VALUES(% s, % s, % s, % s, % s, %s,%s, %s,%s)'''
                            
                cur.execute(
                        sql, (country_name,remark, file, type, lat, long, phone, email,timer))
                conn.commit()
                return '1'
            except:
                return '0'
        
        elif role_input == "pss":
            country_name = level_input['country']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")

       
            try:
                sql = '''INSERT INTO userdata_state
                        (
                        country_id,
                        state_id,
                        remark,
                    
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, %s,%s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, remark,file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'
        
                
        elif role_input == "pls":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_lga
                        (
                        country_id,
                        state_id,
                        lga_id,
                        remark,                    
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s,% s, % s, % s, % s, % s, %s,%s,%s,%s)'''
                cur.execute(
                    sql, (country_name,state_name, lga_name, remark, file, type, lat, long, phone, email,timer))
                
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'

        elif role_input == "pws":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")


            try:
                sql = '''INSERT INTO userdata_ward
                        (
                        country_id,
                        state_id,
                        lga_id,
                        ward_id,
                        remark,               
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, % s, % s, %s,%s,%s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, ward_name,  remark, file, type, lat, long, phone, email,timer))
                
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_pu
                        (
                        country_id,
                        state_id,
                        lga_id,
                        ward_id,
                        pu_id,
                        remark,
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s, % s,% s, % s, % s, % s, % s, % s, % s, %s,%s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, ward_name, pu_name,  remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'

           
        
        elif role_input == "sds":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            try:
                sql = '''INSERT INTO userdata_district
                        (
                        country_id,
                        state_id,
                        district_id,
                        remark,
                    
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, %s,%s, %s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, district_name, remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'
            


        elif role_input == "sls":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            try:
                sql = '''INSERT INTO userdata_lga
                        (
                        country_id,
                        state_id,
                        lga_id,
                        remark,
                    
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, %s,%s, %s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'

        elif role_input == "sws":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_ward
                        (
                        country_id,
                        state_id,
                        lga_id,
                        ward_id,
                        remark,               
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, % s, % s, %s,%s,%s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, ward_name,  remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'


        elif role_input == "spa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_pu
                        (
                        country_id,
                        state_id,
                        lga_id,
                        ward_id,
                        pu_id,
                        remark,
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s, % s,% s, % s, % s, % s, % s, % s, % s, %s,%s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, ward_name, pu_name,  remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'

        elif role_input == "rcs":
            country_name = level_input['country']
            constituency_name = level_input['constituency_name']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_constituency
                        (
                        country_id,
                        state_id,
                        const_id,
                        remark,
                    
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, %s,%s, %s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, constituency_name, remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'

        elif role_input == "rls":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_lga
                        (
                        country_id,
                        state_id,
                        const_id,
                        lga_id,
                        remark,
                    
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, %s,%s, %s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'

        elif role_input == "rws":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")

            

            try:
                sql = '''INSERT INTO userdata_ward
                        (
                        country_id,
                        state_id,
                        const_id,
                        lga_id,
                        ward_id,
                        remark,               
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s,% s, % s, % s, % s, % s, % s, % s, %s,%s,%s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, ward_name,  remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'


        elif role_input == "rpa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = '''INSERT INTO userdata_pu
                        (
                        country_id,
                        state_id,
                        lga_id,
                        const_id,
                        ward_id,
                        pu_id,
                        remark,
                        file,
                        file_type,
                        lat,
                        long,
                        phone,
                        email,
                        date

                        )
                        VALUES(% s, % s,% s, % s, % s, % s, % s, % s, % s, %s,%s, %s,%s)'''
                        
                cur.execute(
                    sql, (country_name,state_name, lga_name, ward_name, pu_name,  remark, file, type, lat, long, phone, email,timer))
                conn.commit()
            # app.conn.close()
                return '1'
            except:
                return '0'
        
        
        



        
def message(user):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    name = user_data['name']
    aspirant_photo = user_data['aspirant_avatar']
    typo =user_data['type_of_election'] 

    now = datetime.now() 
  
    with get_db2() as conn:
        cur = conn.cursor()
        if role_input == "pns":
            country_name = level_input['country']
            timer = now.strftime("%m/%d/%Y %H:%M")
            try:
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at National"]
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'
        
        elif role_input == "pss":
            country_name = level_input['country']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
       
            try:
                sql = f"""select distinct state_name from pu_result_table where state_id={state_name}"""
                print()
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}"]
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            # app.conn.close()
            except:
                return '0'
        
                
        elif role_input == "pls":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = f"""select distinct state_name,lga_name from pu_result_table where state_id={state_name} and lga_id={lga_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}"]
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            # app.conn.close()
            except:
                return '0'

        elif role_input == "pws":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = f"""select distinct state_name,lga_name,ward_name from pu_result_table where state_id={state_name} and lga_id={lga_name} and ward_id={ward_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}"]
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            # app.conn.close()
                return '1'
            except:
                return '0'

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = f"""select distinct state_name,lga_name,ward_name,pu_name from pu_result_table where state_id={state_name} and lga_id={lga_name} and ward_id={ward_name} and pu_id={pu_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Agent" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}/{result[3]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            # app.conn.close()
                return '1'
            except:
                return '0'

           
        
        elif role_input == "sds":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            try:
                sql = f"""select distinct state_name,district_name from sen_pu_table where state_id={state_name} and district_id={district_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'
            


        elif role_input == "sls":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            try:
                sql = f"""select distinct state_name,district_name,lga_name from sen_pu_table where state_id={state_name} and district_id={district_name} and lga_id={lga_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'

        elif role_input == "sws":
            country_name = level_input['country']
            state_name = level_input['state']
            district_name = level_input['district']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = f"""select distinct state_name,district_name,lga_name,ward_name from sen_pu_table where state_id={state_name} and district_id={district_name} and lga_id={lga_name} and ward_id={ward_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}/{result[3]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                
                return out
            except:
                return '0'


        elif role_input == "spa":
            country_name = level_input['country']
            state_name = level_input['state']
            district_name = level_input['district']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = f"""select distinct state_name,district_name,lga_name,ward_name,pu_name from sen_pu_table where state_id={state_name} and district_id={district_name} and lga_id={lga_name} and ward_id={ward_name} and pu_id={pu_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Agent" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}/{result[3]}/{result[4]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'

        elif role_input == "rcs":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            try:
                sql = f"""select distinct state_name,constituency_name from rep_pu_table where state_id={state_name} and const_id={constituency_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'

        elif role_input == "rls":
            country_name = level_input['country']
            constituency_name = level_input['constituency']

            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            try:
                sql = f"""select distinct state_name,constituency_name,lga_name from rep_pu_table where state_id={state_name} and const_id={constituency_name} and lga_id={lga_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'

        elif role_input == "rws":
            country_name = level_input['country']
            state_name = level_input['state']
            constituency_name = level_input['constituency']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = f"""select distinct state_name,constituency_name,lga_name,ward_name from rep_pu_table where state_id={state_name} and const_id={constituency_name} and lga_id={lga_name} and ward_id={ward_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Supervisor" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}/{result[3]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'


        elif role_input == "rpa":
            country_name = level_input['country']
            state_name = level_input['state']
            constituency_name = level_input['constituency']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")

            try:
                sql = f"""select distinct state_name,constituency_name,lga_name,ward_name,pu_name from rep_pu_table where state_id={state_name} and const_id={constituency_name} and lga_id={lga_name} and ward_id={ward_name} and pu_id={pu_name}"""
                cur.execute(sql)
                result = cur.fetchone()
                message1 = ["Welcome"]
                message2 = [name]
                message3 = [typo + " " + "elections as Agent" ]
                message4 = ["at " +f"{result[0]}/{result[1]}/{result[2]}/{result[3]}/{result[4]}"]
                
                out = {"message":message1+message2+message3+message4,"aspirant_avatar":aspirant_photo}
                return out
            except:
                return '0'
        
        

def get_data(user):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()

        if role_input == "pns":
            country_name = level_input['country']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from country_result_table Where country_id = {country_name} """
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)

        elif role_input == "pss":
            country_name = level_input['country']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
           
            sql = f"""select * from state_result_table  Where country_id = {country_name} and state_id= {state_name} """
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)

        
                
        elif role_input == "pls":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")

            sql = f"""select * from lga_result_table  Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "pws":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from ward_result_table  Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from pu_result_table  Where country_id = {country_name} and state_id= {state_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)

        
        elif role_input == "sds":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from sen_district_table  Where country_id = {country_name}  and state_id= {state_name} and district_id = {district_name}"""
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)



        elif role_input == "sls":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from sen_lga_table  Where country_id = {country_name} and state_id= {state_name} and district_id = {district_name} and lga_id= {lga_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "sws":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from sen_ward_table  Where country_id = {country_name} and state_id= {state_name} and district_id = {district_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)



        elif role_input == "spa":
            country_name = level_input['country']
            district_name = level_input['district']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from sen_pu_table  Where country_id = {country_name} and  state_id= {state_name}  and district_id = {district_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "rcs":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from rep_constituency_table  Where country_id = {country_name}  and state_id= {state_name} and const_id= {constituency_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)



        elif role_input == "rls":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from rep_lga_table  Where country_id = {country_name} and state_id= {state_name} and const_id= {constituency_name} and lga_id= {lga_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)


        elif role_input == "rws":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from rep_ward_table  Where country_id = {country_name} and state_id= {state_name} and const_id= {constituency_name} and lga_id= {lga_name} and ward_id= {ward_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)



        elif role_input == "rpa":
            country_name = level_input['country']
            constituency_name = level_input['constituency']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from rep_pu_table  Where country_id = {country_name} and  state_id= {state_name}  and const_id= {constituency_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)



def get_data_senate(user):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    final = {
            "results": {
                "A": 0,
                "AA": 0,
                "AAC": 0,
                "ADC": 0,
                "ADP": 0,
                "APC": 0,
                "APGA": 0,
                "APM": 0,
                "APP": 0,
                "BP": 0,
                "LP": 0,
                "NNPP": 0,
                "NRM": 0,
                "PDP": 0,
                "PRP": 0,
                "SDP": 0,
                "YPP": 0,
                "ZLP": 0
            },
            "total": {
                "TOTAL_ACCREDITED_VOTERS": 0,
                "TOTAL_REGISTERED_VOTERS": 0,
                "TOTAL_REJECTED_VOTES": 0
            },
            "other_data": {
                "DATE_TIME": "0",
                "PERSON_COLLATED": 0
            }
            }
    with get_db2() as conn:
        cur = conn.cursor()

        if role_input == "pns":
           return final

        elif role_input == "pss":
           return final
        
                
        elif role_input == "pls":
           return final


        elif role_input == "pws":
            return final


        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            sql = f"""SELECT DISTINCT state_id,state_name, district_id,district_name FROM sen_pu_table WHERE 
            state_id = {state_name} AND 
            lga_id = {lga_name}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            district_name = results[0]['DISTRICT_ID']

            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from sen_pu_table  Where country_id = {country_name} and state_id= {state_name} and district_id ={district_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)

        
        elif role_input == "sds":
           return final 



        elif role_input == "sls":
            return final


        elif role_input == "sws":
            return final



        elif role_input == "spa":
            return final


        elif role_input == "rcs":
            return final


        elif role_input == "rls":
            return final


        elif role_input == "rws":
            return final



        elif role_input == "rpa":
            return final


def get_data_rep(user):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    final = {
            "results": {
                "A": 0,
                "AA": 0,
                "AAC": 0,
                "ADC": 0,
                "ADP": 0,
                "APC": 0,
                "APGA": 0,
                "APM": 0,
                "APP": 0,
                "BP": 0,
                "LP": 0,
                "NNPP": 0,
                "NRM": 0,
                "PDP": 0,
                "PRP": 0,
                "SDP": 0,
                "YPP": 0,
                "ZLP": 0
            },
            "total": {
                "TOTAL_ACCREDITED_VOTERS": 0,
                "TOTAL_REGISTERED_VOTERS": 0,
                "TOTAL_REJECTED_VOTES": 0
            },
            "other_data": {
                "DATE_TIME": "0",
                "PERSON_COLLATED": 0
            }
            }
    with get_db2() as conn:
        cur = conn.cursor()

        if role_input == "pns":
        
           return final

        elif role_input == "pss":
            return final

        
                
        elif role_input == "pls":
            return final



        elif role_input == "pws":
            return final


        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']

            sql = f"""SELECT DISTINCT state_id,state_name, const_id,constituency_name FROM rep_pu_table WHERE 
            state_id = {state_name} AND 
            lga_id = {lga_name}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            constituency_name = results[0]['CONST_ID']
            

            timer = now.strftime("%m/%d/%Y %H:%M")
            
            sql = f"""select * from rep_pu_table  Where country_id = {country_name} and state_id= {state_name} and const_id ={constituency_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            final={}
            try:
                cur.execute(sql)
                results = cur.fetch_pandas_all()
                results = results.to_json(orient="records")
                results = json.loads(results)
                parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
                total =["TOTAL_ACCREDITED_VOTERS","TOTAL_REGISTERED_VOTERS","TOTAL_REJECTED_VOTES"]
    
                data = ['DATE_TIME', 'PERSON_COLLATED']
                parties_results = {}
                total_results={}
                other_data_results={}
                for key in parties:
                    parties_results.update( {key:results[0][key]})
               
                for key in total:
                    total_results.update( {key:results[0][key]})
                
                other_data_results.update({'person_collated':user_data['name']})
                other_data_results.update({"time":timer})

                
                final['results'] = parties_results
                final['total'] = total_results
                final['other_data'] = other_data_results
                return final
            except Exception as e:
                print(e)
                return str(e)

        
        elif role_input == "sds":
            return final
 



        elif role_input == "sls":
            return final



        elif role_input == "sws":
           return final


        elif role_input == "spa":
            return final


        elif role_input == "rcs":
            return final

        elif role_input == "rls":
            return final



        elif role_input == "rws":
            return final


        elif role_input == "rpa":
            return final




from fastapi import status, HTTPException

def submit_data_senate(user,userdata_collate):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()

        if role_input == "pns":
            
            raise HTTPException(
        status_code=status.HTTP_415_UNSUPPORTED_MEDIA_TYPE,
        detail=f'You are not Authorized to collate Senate Elections'
    )
        
        elif role_input == "pss":

            raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
        
                
        elif role_input == "pls":
             
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
            # return {"You are not Authorized to collate Senate Elections"}
            

        elif role_input == "pws":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
            

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            sql = f"""SELECT DISTINCT state_id,state_name, district_id,district_name FROM sen_pu_table WHERE 
            state_id = {state_name} AND 
            lga_id = {lga_name}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            district_name = results[0]['DISTRICT_ID']
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update sen_pu_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and district_id ={district_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "sds":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
            


        elif role_input == "sls":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
            

        elif role_input == "sws":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
            


            

        elif role_input == "rcs":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
            


        elif role_input == "rls":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
           

        elif role_input == "rws":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Senate Elections"
        )
            
           

           

def cancel_data_senate(user,userdata_collate):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
        if role_input == "pns":
               raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
            
            
        
        elif role_input == "pss":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
            
        
                
        elif role_input == "pls":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
            

        elif role_input == "pws":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
            

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            sql = f"""SELECT DISTINCT state_id,state_name, district_id,district_name FROM sen_pu_table WHERE 
            state_id = {state_name} AND 
            lga_id = {lga_name}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            district_name = results[0]['DISTRICT_ID']
            timer = now.strftime("%m/%d/%Y %H:%M")
  
            sql = f"""Update sen_pu_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and district_id={district_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "sds":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
            


        elif role_input == "sls":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
            

        elif role_input == "sws":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )

        elif role_input == "rcs":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
           


        elif role_input == "rls":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
           

        elif role_input == "rws":
             raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Senate Elections"
        )
           



def submit_data_rep(user,userdata_collate):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()

        if role_input == "pns":
            
            raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
        
        elif role_input == "pss":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
            
        
                
        elif role_input == "pls":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
            

        elif role_input == "pws":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
            

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            sql = f"""SELECT DISTINCT state_id,state_name, const_id,constituency_name FROM rep_pu_table WHERE 
            state_id = {state_name} AND 
            lga_id = {lga_name}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            constituency_name = results[0]['CONST_ID']
            
            timer = now.strftime("%m/%d/%Y %H:%M")
            query = [
             f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
            
            query = ", ".join(query)
            sql = f"""Update rep_pu_table SET {query} , date_time ='{timer}',status='collated' Where country_id = {country_name} and state_id= {state_name} and const_id={constituency_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                # results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "sds":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
            


        elif role_input == "sls":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
            

        elif role_input == "sws":
                raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
            


            

        elif role_input == "rcs":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
            


        elif role_input == "rls":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
           

        elif role_input == "rws":
                 raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to collate Rep Elections"
        )
           
        
           

def cancel_data_rep(user,userdata_collate):
    user_data = user['user']
    role_input = user_data['role']
    level_input = user_data['level_childs']
    now = datetime.now() 
    with get_db2() as conn:
        cur = conn.cursor()
        if role_input == "pns":
                  raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
            
        
        elif role_input == "pss":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
            
        
                
        elif role_input == "pls":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
            

        elif role_input == "pws":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
            

        elif role_input == "ppa":
            country_name = level_input['country']
            state_name = level_input['state']
            lga_name = level_input['lga']
            ward_name = level_input['ward']
            pu_name = level_input['pollingUnit']
            sql = f"""SELECT DISTINCT state_id,state_name, const_id,constituency_name FROM rep_pu_table WHERE 
            state_id = {state_name} AND 
            lga_id = {lga_name}"""
        # else:
        #     sql = "SELECT DISTINCT state_id, lga_id, WARD_ID ,WARD_NAME FROM pu_result_table"

        
            cur.execute(sql)
            results = cur.fetch_pandas_all()
            results = results.to_json(orient="records")
            results = json.loads(results)            #cur.close()
            constituency_name = results[0]['CONST_ID']
            timer = now.strftime("%m/%d/%Y %H:%M")
  
            sql = f"""Update rep_pu_table  SET status='canceled', A=0, AA=0, AAC=0, ADC=0, ADP=0, APC=0, APGA=0, APM=0, APP=0, BP=0, LP=0, NNPP=0, NRM=0, PDP=0, PRP=0, SDP=0, Total_Accredited_voters=0, Total_Rejected_votes=0, YPP=0, ZLP=0 , date_time ='{timer}'
 Where country_id = {country_name} and state_id= {state_name} and const_id={constituency_name} and lga_id= {lga_name} and ward_id= {ward_name} and pu_id= {pu_name}"""
            
            try:
                cur.execute(sql)
                results = cur.fetchall()
                conn.commit()
                res= {}
                res.update({'person_collated':user_data['name']})
                res.update({"time":timer})
                return res
            except Exception as e:
                print(e)
                return str(e)
        
        elif role_input == "sds":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
            


        elif role_input == "sls":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
            

        elif role_input == "sws":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
            

        elif role_input == "rcs":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
           


        elif role_input == "rls":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
           

        elif role_input == "rws":
              raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="You are not Authorized to Cancel Rep Elections"
        )
           














# def submit(place,user_type,userdata_collate):

#     now = datetime.now() 
#     with get_db() as conn:
#         cur = conn.cursor()

#         if user_type == 1:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             query = [
#              f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
#             
#             query = ", ".join(query)
#             sql = f"""Update `pu_result_table` SET {query} , `date_time` ='{timer}',`status`='collated' Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}' and `pu_id` = '{placelist[3]}'"""
#             sql2 = f"""Select * from `pu_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}' and `pu_id` = '{placelist[3]}'"""
            
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)

#         if user_type == 2:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             query = [
#              f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
#             
#             query = ", ".join(query)
#             sql = f"""Update `ward_result_table` SET {query} , `date_time` ='{timer}',`status`='collated' Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}'"""
#             sql2 = f"""Select * from `ward_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)


#         if user_type == 3:
#             placelist  = place.split(',')
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             query = [
#              f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
#             
#             query = ", ".join(query)
#             sql = f"""Update `lga_result_table` SET {query} , `date_time` ='{timer}',`status`='collated' Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}'"""
#             sql2 = f"""Select * from `lga_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)
 

#         if user_type == 4:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             query = [
#              f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
#             
#             query = ", ".join(query)
#             sql = f"""Update `state_result_table` SET {query} , `date_time` ='{timer}',`status`='collated' Where `state_id` = '{placelist[0]}'"""
#             sql2 = f"""Select * from `state_result_table` Where `state_id` = '{placelist[0]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)

#         if user_type == 5:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             query = [
#              f"{key}={value[0] if isinstance(value, list) else value}" for key, value in userdata_collate.items()]
#             
#             query = ", ".join(query)
#             sql = f"""Update `country_result_table` SET {query} , `date_time` ='{timer}',`status`='collated' Where `country_name` = 'NIGERIA'"""
#             sql2 = f"""Select * from `country_result_table`'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)


# def cancel(place,user_type,userdata_collate):

#     now = datetime.now() 
#     with get_db() as conn:
#         cur = conn.cursor()

#         if user_type == 1:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Update `pu_result_table` SET `status`='canceled', `A`=0, `AA`=0, `AAC`=0, `ADC`=0, `ADP`=0, `APC`=0, `APGA`=0, `APM`=0, `APP`=0, `BP`=0, `LP`=0, `NNPP`=0, `NRM`=0, `PDP`=0, `PRP`=0, `SDP`=0, `Total_Accredited_voters`=0, `Total_Rejected_votes`=0, `YPP`=0, `ZLP`=0 , `date_time` ='{timer}' Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}' and `pu_id` = '{placelist[3]}'"""

#             sql2 = f"""Select * from `pu_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}' and `pu_id` = '{placelist[3]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)

#         if user_type == 2:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Update `ward_result_table` SET `status`='canceled', `A`=0, `AA`=0, `AAC`=0, `ADC`=0, `ADP`=0, `APC`=0, `APGA`=0, `APM`=0, `APP`=0, `BP`=0, `LP`=0, `NNPP`=0, `NRM`=0, `PDP`=0, `PRP`=0, `SDP`=0, `Total_Accredited_voters`=0, `Total_Rejected_votes`=0, `YPP`=0, `ZLP`=0 , `date_time` ='{timer}' Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}'"""

#             sql2 = f"""Select * from `ward_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)


#         if user_type == 3:
#             placelist  = place.split(',')
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Update `lga_result_table` SET `status`='canceled', `A`=0, `AA`=0, `AAC`=0, `ADC`=0, `ADP`=0, `APC`=0, `APGA`=0, `APM`=0, `APP`=0, `BP`=0, `LP`=0, `NNPP`=0, `NRM`=0, `PDP`=0, `PRP`=0, `SDP`=0, `Total_Accredited_voters`=0, `Total_Rejected_votes`=0, `YPP`=0, `ZLP`=0 , `date_time` ='{timer}' Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}'"""

#             sql2 = f"""Select * from `lga_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)
 

#         if user_type == 4:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Update `state_result_table` SET `status`='canceled', `A`=0, `AA`=0, `AAC`=0, `ADC`=0, `ADP`=0, `APC`=0, `APGA`=0, `APM`=0, `APP`=0, `BP`=0, `LP`=0, `NNPP`=0, `NRM`=0, `PDP`=0, `PRP`=0, `SDP`=0, `Total_Accredited_voters`=0, `Total_Rejected_votes`=0, `YPP`=0, `ZLP`=0 , `date_time` ='{timer}' Where `state_id` = '{placelist[0]}'"""
#             sql2 = f"""Select * from `state_result_table` Where `state_id` = '{placelist[0]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)

#         if user_type == 5:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Update `country_result_table` SET `status`='canceled', `A`=0, `AA`=0, `AAC`=0, `ADC`=0, `ADP`=0, `APC`=0, `APGA`=0, `APM`=0, `APP`=0, `BP`=0, `LP`=0, `NNPP`=0, `NRM`=0, `PDP`=0, `PRP`=0, `SDP`=0, `Total_Accredited_voters`=0, `Total_Rejected_votes`=0, `YPP`=0, `ZLP`=0 , `date_time` ='{timer}' Where `country_name` = 'NIGERIA''"""
#             sql2 = f"""Select * from `country_result_table`'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall()
#                 conn.commit()
#                 cur.execute(sql2)
#                 ress = cur.fetchall()
#                 res= {}
#                 for row in ress:
#                     res.update({'person_collated':row['person_collated']})
#                     res.update({"time":row['date_time']})
#                 return res
#             except Exception as e:
#                 print(e)
#                 return str(e)


# def getvalue(place,user_type):

#     now = datetime.now() 
#     with get_db() as conn:
#         cur = conn.cursor()

#         if user_type == 1:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             final ={}


#             sql = f"""Select * from `pu_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}' and `pu_id` = '{placelist[3]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchone()
#                 parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
#                 total =["Total_Accredited_voters","Total_Registered_voters","Total_Rejected_votes"]
        
#                 data = ['date_time', 'person_collated']
#                 parties_results = {}
#                 total_results={}
#                 other_data_results={}
#                 for key in parties:
#                     parties_results.update( {key:results[key]})
                
#                 for key in total:
#                     total_results.update( {key:results[key]})

#                 for key in data:
#                     other_data_results.update( {key:results[key]})

#                 final['results'] = parties_results
#                 final['total'] = total_results
#                 final['other_data'] = other_data_results
#                 return final
#             except Exception as e:
#                 print(e)
#             return str(e)

#         if user_type == 2:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             final ={}

#             sql = f"""Select * from `ward_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchone()
#                 parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
#                 total =["Total_Accredited_voters","Total_Registered_voters","Total_Rejected_votes"]
        
#                 data = ['date_time', 'person_collated']
#                 parties_results = {}
#                 total_results={}
#                 other_data_results={}
#                 for key in parties:
#                     parties_results.update( {key:results[key]})
                
#                 for key in total:
#                     total_results.update( {key:results[key]})

#                 for key in data:
#                     other_data_results.update( {key:results[key]})

#                 final['results'] = parties_results
#                 final['total'] = total_results
#                 final['other_data'] = other_data_results
#                 return final
#             except Exception as e:
#                 print(e)
#             return str(e)
               


#         if user_type == 3:
#             placelist  = place.split(',')
#             timer = now.strftime("%m/%d/%Y %H:%M")

#             final ={}

#             sql = f"""Select * from `lga_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchone()
#                 parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
#                 total =["Total_Accredited_voters","Total_Registered_voters","Total_Rejected_votes"]
        
#                 data = ['date_time', 'person_collated']
#                 parties_results = {}
#                 total_results={}
#                 other_data_results={}
#                 for key in parties:
#                     parties_results.update( {key:results[key]})
                
#                 for key in total:
#                     total_results.update( {key:results[key]})

#                 for key in data:
#                     other_data_results.update( {key:results[key]})

#                 final['results'] = parties_results
#                 final['total'] = total_results
#                 final['other_data'] = other_data_results
#                 return final
#             except Exception as e:
#                 print(e)
#             return str(e)
               
 

#         if user_type == 4:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             final ={}

#             sql = f"""Select * from `state_result_table` Where `state_id` = '{placelist[0]}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchone()
#                 parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
#                 total =["Total_Accredited_voters","Total_Registered_voters","Total_Rejected_votes"]
        
#                 data = ['date_time', 'person_collated']
#                 parties_results = {}
#                 total_results={}
#                 other_data_results={}
#                 for key in parties:
#                     parties_results.update( {key:results[key]})
                
#                 for key in total:
#                     total_results.update( {key:results[key]})

#                 for key in data:
#                     other_data_results.update( {key:results[key]})

#                 final['results'] = parties_results
#                 final['total'] = total_results
#                 final['other_data'] = other_data_results
#                 return final
#             except Exception as e:
#                 print(e)
#             return str(e)

#         if user_type == 5:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             final ={}

#             sql = f"""Select * from `country_result_table`'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchone()
#                 parties = ["A","AA","AAC","ADC","ADP","APC","APGA","APM","APP","BP","LP","NNPP","NRM","PDP","PRP","SDP","YPP","ZLP"]
#                 total =["Total_Accredited_voters","Total_Registered_voters","Total_Rejected_votes"]
        
#                 data = ['date_time', 'person_collated']
#                 parties_results = {}
#                 total_results={}
#                 other_data_results={}
#                 for key in parties:
#                     parties_results.update( {key:results[key]})
                
#                 for key in total:
#                     total_results.update( {key:results[key]})

#                 for key in data:
#                     other_data_results.update( {key:results[key]})

#                 final['results'] = parties_results
#                 final['total'] = total_results
#                 final['other_data'] = other_data_results
#                 return final
#             except Exception as e:
#                 print(e)
#             return str(e)
              


# def postmedia(place,user_type,userdata_postmedia):

#     with get_db() as conn:
#         cur = conn.cursor()
#         if user_type == 1:
#             placelist  = place.split(',') 
#             sql1 = f"""SELECT state_name,lga_name,ward_name,pu_name FROM `pu_result_table` Where `state_id` = {placelist[0]} and `lga_id` = {placelist[1]} and `ward_id` = {placelist[2]} and `pu_id` = {placelist[3]}"""
#             cur.execute(sql1)
#             results = cur.fetchall()
#             state =  results[0]['state']
#             lga = results[0]['lga_name']
#             ward = results[0]['ward_name']
#             pu = results[0]['pu_name']
            
            
#             remark = userdata_postmedia['remark']
#             #ml = userdata_postmedia['ml']

#             file = userdata_postmedia['file']
#             type = userdata_postmedia['type']
#             lat = userdata_postmedia['lat']
#             long = userdata_postmedia['long']
#             phone = userdata_postmedia['phone']
#             email = userdata_postmedia['email']

#             try:
#                 sql = '''INSERT INTO `userdata_pu`
#                         (
#                         `state`,
#                         `lga`,
#                         `ward`,
#                         `pu`,
#                         `remark`,
                        
#                         `file`,
#                         `file_type`,
#                         `lat`,
#                         `long`,
#                         `phone`,
#                         `email`

#                         )
#                         VALUES( % s, % s, % s, % s, % s, % s, % s, %s,%s, %s, %s)'''
                        
#                 cur.execute(
#                     sql, (state, lga, ward, pu,  remark, file, type, lat, long, phone, email))
#                 conn.commit()
#             # app.conn.close()
#                 return '1'
#             except:
#                 return '0'

#         if user_type == 2:
#             placelist  = place.split(',') 
#             sql1 = f"""SELECT state_name,lga_name,ward_name FROM `ward_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}'"""

#             cur.execute(sql1)
#             results = cur.fetchall()
#             state =  results[0]['state']
#             lga = results[0]['lga_name']
#             ward = results[0]['ward_name']
            
            
#             remark = userdata_postmedia['remarks']
#             #ml = userdata_postmedia['ml']

#             file = userdata_postmedia['file']
#             type = userdata_postmedia['type']
#             lat = userdata_postmedia['lat']
#             long = userdata_postmedia['long']
#             phone = userdata_postmedia['phone']
#             email = userdata_postmedia['email']

#             try:
#                 sql = '''INSERT INTO `userdata_ward`
#                         (
#                         `state`,
#                         `lga`,
#                         `ward`,
#                         `remark`,            
#                         `file`,
#                         `file_type`,
#                         `lat`,
#                         `long`,
#                         `phone`,
#                         `email`

#                         )
#                         VALUES( % s, % s, % s, % s, % s, % s, %s,%s, %s, %s)'''
                        
#                 cur.execute(
#                     sql, (state, lga, ward,  remark, file, type, lat, long, phone, email))
#                 conn.commit()
#             # app.conn.close()
#                 return '1'
#             except:
#                 return '0'

        
#         if user_type == 3:
#             placelist  = place.split(',') 
#             sql1 = f"""SELECT state_name,lga_name, FROM `lga_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}''"""

#             cur.execute(sql1)
#             results = cur.fetchall()
#             state =  results[0]['state']
#             lga = results[0]['lga_name']
            
            
#             remark = userdata_postmedia['remarks']
#             #ml = userdata_postmedia['ml']

#             file = userdata_postmedia['file']
#             type = userdata_postmedia['type']
#             lat = userdata_postmedia['lat']
#             long = userdata_postmedia['long']
#             phone = userdata_postmedia['phone']
#             email = userdata_postmedia['email']

#             try:
#                 sql = '''INSERT INTO `userdata_lga`
#                         (
#                         `state`,
#                         `lga`,
#                         `remark`,     
#                         `file`,
#                         `file_type`,
#                         `lat`,
#                         `long`,
#                         `phone`,
#                         `email`

#                         )
#                         VALUES( % s, % s, % s, % s, % s, % s, %s,%s, %s)'''
                        
#                 cur.execute(
#                     sql, (state, lga,remark, file, type, lat, long, phone, email))
#                 conn.commit()
#             # app.conn.close()
#                 return '1'
#             except:
#                 return '0'

#         if user_type == 4:
#             placelist  = place.split(',') 
#             sql1 = f"""SELECT state_name  FROM `state_result_table` Where `state_id` = '{placelist[0]}'"""

#             cur.execute(sql1)
#             results = cur.fetchall()
#             state =  results[0]['state']
     
            
#             remark = userdata_postmedia['remarks']
#             #ml = userdata_postmedia['ml']

#             file = userdata_postmedia['file']
#             type = userdata_postmedia['type']
#             lat = userdata_postmedia['lat']
#             long = userdata_postmedia['long']
#             phone = userdata_postmedia['phone']
#             email = userdata_postmedia['email']

#             try:
#                 sql = '''INSERT INTO `userdata_state`
#                         (
#                         `state`,
#                         `remark`,
#                         `file`,
#                         `file_type`,
#                         `lat`,
#                         `long`,
#                         `phone`,
#                         `email`

#                         )
#                         VALUES( % s, % s, % s, % s, %s,%s, %s, %s)'''
                        
#                 cur.execute(
#                     sql, (state, remark, file, type, lat, long, phone, email))
#                 conn.commit()
#             # app.conn.close()
#                 return '1'
#             except:
#                 return '0'
#         if user_type == 5:
#             placelist  = place.split(',') 

#             results = cur.fetchall()
#             country =  "NIGERIA"
     
            
#             remark = userdata_postmedia['remarks']
#             #ml = userdata_postmedia['ml']

#             file = userdata_postmedia['file']
#             type = userdata_postmedia['type']
#             lat = userdata_postmedia['lat']
#             long = userdata_postmedia['long']
#             phone = userdata_postmedia['phone']
#             email = userdata_postmedia['email']

#             try:
#                 sql = '''INSERT INTO `userdata_country`
#                         (
#                         `country`,
#                         `remark`,               
#                         `file`,
#                         `file_type`,
#                         `lat`,
#                         `long`,
#                         `phone`,
#                         `email`

#                         )
#                         VALUES( % s, % s, % s, % s, %s,%s, %s, %s)'''
                        
#                 cur.execute(
#                     sql, (country, remark, file, type, lat, long, phone, email))
#                 conn.commit()
#             # app.conn.close()
#                 return '1'
#             except:
#                 return '0'

# def getdata(place,user_type):

#     now = datetime.now() 
#     with get_db() as conn:
#         cur = conn.cursor()

#         if user_type == 1:
#             placelist  = place.split(',') 

#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Select state_name,lga_name, ward_name,pu_name,pu_code from `pu_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}' and `pu_id` = '{placelist[3]}'"""
#             sql2 = f"""select name from user_managment where `place` = '{place}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall() 
#                 cur.execute(sql2)
#                 results2 = cur.fetchall()
#                 state_name = results[0]['state']
#                 lga_name = results[0]['lga_name']
#                 ward_name = results[0]['ward_name']
#                 pu_name = results[0]['pu_name']
#                 pu_code = results[0]['pu_code']
#                 name = results2[0]['name']
#                 message1 = "Welcome"
#                 message2 = f"{name}"
#                 message3 = "to"
#                 message4 = f"{state_name}/{lga_name}/{ward_name}/{pu_name} - {pu_code}"
#                 message = [message1,message2,message3,message4]
#                 return message
#             except Exception as e:
#                 print(e)
#                 return str(e)

#         if user_type == 2:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
          
#             sql = f"""Select state_name,lga_name, ward_name from `pu_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}' and `ward_id` = '{placelist[2]}'"""
#             sql2 = f"""select name from user_managment where `place` = '{place}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall() 
#                 cur.execute(sql2)
#                 results2 = cur.fetchall()
#                 state_name = results[0]['state']
#                 lga_name = results[0]['lga_name']
#                 ward_name = results[0]['ward_name']
#                 name = results2[0]['name']
#                 message1 = "Welcome "
#                 message2 = f"{name}"
#                 message3 = "to"
#                 message4 = f"{state_name}/{lga_name}/{ward_name}"
#                 message = [message1,message2,message3,message4]


                    
#                 return message
#             except Exception as e:
#                 print(e)
#                 return str(e)

#         if user_type == 3:
#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
          
#             sql = f"""Select state_name,lga_name from `pu_result_table` Where `state_id` = '{placelist[0]}' and `lga_id` = '{placelist[1]}'"""
#             sql2 = f"""select name from user_managment where `place` = '{place}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall() 
#                 cur.execute(sql2)
#                 results2 = cur.fetchall()
#                 state_name = results[0]['state']
#                 lga_name = results[0]['lga_name']
#                 name = results2[0]['name']
#                 message1 = "Welcome "
#                 message2 = f"{name}"
#                 message3 = "to"
#                 message4 = f"{state_name}/{lga_name}"
#                 message = [message1,message2,message3,message4]
                         
#                 return message
#             except Exception as e:
#                 print(e)
#                 return str(e)
            

#         if user_type == 4:

#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Select state_name from `pu_result_table` Where `state_id` = '{placelist[0]}'"""
#             sql2 = f"""select name from user_managment where `place` = '{place}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall() 
#                 cur.execute(sql2)
#                 results2 = cur.fetchall()
#                 state_name = results[0]['state']
#                 name = results2[0]['name']
#                 message1 = "Welcome "
#                 message2 = f"{name} "
#                 message3 =  "to"
#                 message4 = f"{state_name}"
#                 message = [message1,message2,message3,message4]
                        
#                 return message
#             except Exception as e:
#                 print(e)
#                 return str(e)
            
#         if user_type == 5:

#             placelist  = place.split(',') 
#             timer = now.strftime("%m/%d/%Y %H:%M")
#             sql = f"""Select country_name from `pu_result_table`'"""
#             sql2 = f"""select name from user_managment where `place` = '{place}'"""
#             try:
#                 cur.execute(sql)
#                 results = cur.fetchall() 
#                 cur.execute(sql2)
#                 results2 = cur.fetchall()
#                 country_name = results[0]['country_name']
#                 name = results2[0]['name']
#                 message1 = "Welcome"
#                 message2 = f"{name}"
#                 message3 =  "to"
#                 message4 = "National Collation Centre"
#                 message = [message1,message2,message3,message4]
                        
#                 return message
#             except Exception as e:
#                 print(e)
#                 return str(e)
           
