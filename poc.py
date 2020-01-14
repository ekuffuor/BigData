"""
The main components of this code includes:

1. Reading data from File System into a Spark RDD

2. Applying transformations to clean and mold the data

3. Computing summary statistics for each user and checking the distribution of data

The code is broken into two main phases; 
Phase One: Calculating the patient's age and age group from his date of birth 

Phase Two: Finding the distribution and summary statistics each patient's attributes
"""

# PHASE ONE


# load the data from patients.csv

patient_rdd = sc.textFile(‘hdfs:///...patients.csv’)


# create features from the existing patient attributes

patient_demo = patient_rdd.filter(lambda line: ‘patient_id’ not in line ).map(lambda line: patient_attributes(line))

patient_demo.persist()


def patient_attributes(str):

l = str.split(“,”)

return [l[0],l[1], l[2], l[3], l[4], l[5],int(prepare_date(l[1])),age_group(int(prepare_date(l[1])))]


def prepare_date(date_form):

year,month,day = [int(x) for x in date_form.split(“-“)]

try :

born = date(year, month, day)

except ValueError: 

born = date(year, month, day-1) 

return calculate_age(born)


def calculate_age(born):

today = date.today()

return today.year – born.year – ((today.month, today.day) < (born.month, born.day))


def age_group(age):

if age < 10 :

return ‘0-10’

elif age < 20:

return ’10-20′

elif age < 30:

return ’20-30′

elif age < 40:

return ’30-40′

elif age < 50:

return ’40-50′

elif age < 60:

return ’50-60′

elif age < 70:

return ’60-70′

elif age < 80:

return ’70-80′

else :

return ’80+’



# PHASE TWO


# find the distribution of male and female patients

gender_distribution = patient_rdd.filter(lambda line: ‘patient_id’ not in line ).map(lambda line: (line.split(‘,’)
[2].strip(),1)).reduceByKey(lambda a,b:a+b).map(lambda line:(line[1],line[0])).sortByKey(False).collect()


# find distribution for the patients' marital status

marital_status_distribution = patient_rdd.filter(lambda line: ‘patient_id’ not in line ).map(lambda line: (line.split(‘,’)
[3].strip(),1)).reduceByKey(lambda a,b:a+b).map(lambda line:(line[1],line[0])).sortByKey(False).collect()


# find distribution for different age groups

age_group_distribution = patient_demo.map(lambda line : (line[7],1)).reduceByKey(lambda a,b:a+b).map(lambda

line:(line[1],line[0])).sortByKey(False).collect()


# find top 5 cities from where we have most number of patients 

top_city = patient_demo.map(lambda line : (line[5],1)).reduceByKey(lambda a,b:a+b).map(lambda line:(line[1],line[0])).sortByKey(False).take(5)


# find distribution for patient's smoking habit

patient_smoking_distribution = patient_demo.map(lambda line : (line[4],1)).reduceByKey(lambda a,b:a+b).map(lambda line:(line[1],line[0])).sortByKey(False).collect()