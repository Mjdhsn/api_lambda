from app.app_v1.analysis.presidential_analysis.tab2.schema import *


pu_query_tables = {
"total": f"""""",
"total_collated": f"""{pu_query['query']} """,
    "total_non_collated": f"""{pu_query['query']} """,
    "total_canceled": f"""{pu_query['query']}  """,  
    "canceled_table": f"""{pu_query['query']} """,
    "total_over_voting": f"""{pu_query['query']} """ ,   
    "number_clear_win": f"""{pu_query['query']}   """,
    "number_win_with_doubt": f"""{pu_query['query']} """,
    "number_of_clear_loss": f"""{pu_query['query']} """,
    "number_of_loss_with_doubt": f"""{pu_query['query']} """,
    "above_clearly_25":f"""{pu_query['query']} """,
    "general_performance": f"""{pu_query['query']} """,

}