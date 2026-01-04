src_array = [

        {"src" : "bookings"},
        {"src" : "flights"},
        {"src" : "airports"},
        {"src" : "customers"}
]

dbutils.jobs.taskValues.set(key = "output_key", value = src_array)