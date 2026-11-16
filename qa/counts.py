from pyspark.sql import Observation, functions as F

stage_counter = {}

# We define a counter with a permanent set in memory that we can draw from to get stage counts
#   stackoverflow.com/questions/68125593/pyspark-count-number-of-rows-written
#   observe counts within each parallel spark task then merges the results
#   pass in the dataframe of the current stage and a name for it
def count(df, stage_name):
    stage = Observation(stage_name)
    stage_counter[stage_name] = stage
    curr = df.observe(stage, F.count(F.lit(1)).alias("rows")) # count all rows
    return curr

# Spark is lazy and will only count after an action, so this should be executed after an
#   action to get the row counts per stage
def final_counts():
    totals = []
    for stage_name, stage in stage_counter.items():
        totals.append((stage_name, stage.get("rows")))
    return totals



#### Need a function to count df rows per stage in and out