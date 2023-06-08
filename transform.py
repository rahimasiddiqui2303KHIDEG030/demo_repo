from pyspark.sql import SparkSession
from pyspark.sql.functions import col
spark = SparkSession.builder.appName("FileReader").getOrCreate()


rating = spark.read.json("rating.json", multiLine=True)
rating.show(3)

appointment = spark.read.json("appointment.json", multiLine=True)
appointment.show(3)

councillor = spark.read.json("councillor.json", multiLine=True)
councillor.show(3)

patient_councillor = spark.read.json("patient_councillor.json", multiLine=True)
patient_councillor.show(3)

# Perform the joins
join1 = rating.join(appointment, rating.appointment_id == appointment.id, "inner")
join1.show(3)
join2 = join1.join(councillor, councillor.id == councillor.id, "inner")
join2.show(3)
join3 = join2.join(patient_councillor, join2.patient_id == patient_councillor.patient_id, "inner")
join3.show(3)

# result = join3.select(
#     join3["patient_id"].alias("Patient_id"),
#     join3["councillor_id"].alias("Councillor_id"),
#     join3["specialization"].alias("Specialization"),
#     join3["value"].alias("Value")
# )

result_table = join3.select(appointment["patient_id"], councillor["id"].alias("councillor_id"), councillor["specialization"], rating["value"])

result_table.show(100)