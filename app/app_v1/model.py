from pydantic import BaseModel, Field, EmailStr
from fastapi import Query
from typing import Optional
from fastapi import FastAPI, Body, Depends,HTTPException, Security



class AuthModel(BaseModel):
    username: str
    password: str


class InputSchema(BaseModel):
    country_name: str = None
    state_name: str = None
    lga_name: str = None
    ward_name: str = None
    pu_name: str = None

    class Config:
        schema_extra = {
            "example": {
                "country_name": "NIGERIA",
                "state_name": "ABIA",
                "lga_name": "ABA NORTH",
                "ward_name": "ARIARIA MARKET",
                "pu_name": "B.T.C-SCHOOL PREMISES I", 

            }
        }

class requestdata(BaseModel):
    data: dict = None

    class Config:
        schema_extra = {
            "example": {
                    "A": "11",
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
                    "ZLP": 0,
                    "Total_Accredited_voters": 0,
                    "Total_Registered_voters": 259929,
                    "Total_Rejected_votes": 0
                    }
        }

class LgalevelSchema2(BaseModel):
    name: str = None
 

    class Config:
        schema_extra = {
            "example": {
                "name": "ABIA",

            }
        }


class WardlevelSchema2(BaseModel):
    state: str = None
    lga: str = None

 

    class Config:
        schema_extra = {
            "example": {
                "state": "ABIA",
                "lga": "ABA NORTH",


            }
        }

class PollevelSchema2(BaseModel):
    key : int = None
    state:  str  = None
    lga:  str  = None
    wardID:  int  = None

 

    class Config:
        schema_extra = {
            "example": {
                "state": "ABIA",
                "lga": "ABA NORTH",
                "wardID": 1


            }
        }

class StatelevelSchema(BaseModel):
    country_name: str = Query(...)
    state_name: str = Query(...)

    class Config:
        schema_extra = {
            "example": {
                "country_name": "NIGERIA",
                "state_name": "ABIA",


            }
        }

class CountrylevelSchema(BaseModel):
    country_name: str = Query(...)

    class Config:
        schema_extra = {
            "example": {
                "country_name": "NIGERIA",


            }
        }

class UserLoginSchema(BaseModel):
    email: EmailStr = Field(...)
    password: str = Field(...)

    class Config:
        schema_extra = {
            "example": {
                "email": "t@gmail.com",
                "password": "111111"
            }
        }


# class Parties(BaseModel):
#     party:  dict = Query(...)

#     class Config:
#         schema_extra = {
#             "example": {
#                 "1": "A",
#                 "2": "AA"
#             }
#         }

# class mobileSchema(BaseModel):
#     place: str = Field(...)
#     user_type: int = Field(...)
#     userdata_collate: dict == Query(...)

#     class Config:
#         schema_extra = {
#             "example": {
#                 "place": "1,1,1,1",
#                 "user_type": 1,
#                 "userdata_collate": {
#     "A": "6",
#     "AA": 0,
#     "AAC": 0,
#     "ADC": 0,
#     "ADP": 0,
#     "APC": 0,
#     "APGA": "11",
#     "APM": "6",
#     "APP": 0,
#     "BP": 0,
#     "LP": 0,
#     "NNPP": 0,
#     "NRM": 0,
#     "PDP": 0,
#     "PRP": 0,
#     "SDP": 0,
#     "YPP": 0,
#     "ZLP": 0,
#     "Total_Accredited_voters": 0,
#     "Total_Registered_voters": 14,
#     "Total_Rejected_votes": 0
# }

#             }
#         }




# class mobilepostSchema(BaseModel):
#     place: str = Field(...)
#     user_type: int = Field(...)
#     userdata_postmedia: dict == Query(...)

#     class Config:
#         schema_extra = {
#             "example": {
#                 "place": "1,1,1,1",
#                 "user_type": 1,
#                 "userdata_postmedia": {
 


#             }
#         }}