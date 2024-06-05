from neo4j import GraphDatabase
import pandas as pd
import time
import random


class Neo4jConnection:

    def __init__(self, uri, user, password):
        self.__uri = uri
        self.__user = user
        self.__password = password
        self.__driver = None
        try:
            self.__driver = GraphDatabase.driver(
                self.__uri, auth=(self.__user, self.__password)
            )
        except Exception as e:
            print("Failed to create the driver:", e)

    def close(self):
        if self.__driver is not None:
            self.__driver.close()

    def query(self, query, parameters=None, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        session = None
        response = None
        try:
            session = (
                self.__driver.session(database=db)
                if db is not None
                else self.__driver.session()
            )
            response = list(session.run(query, parameters))
        except Exception as e:
            print("Query failed:", e)
        finally:
            if session is not None:
                session.close()
        return response

    def run_query(self, query, parameters=None):
        with self.driver.session() as session:
            result = session.run(query, parameters)
            return result


uri = "bolt://localhost:7687"
user = "neo4j"
password = "123456789"
conn = Neo4jConnection(uri=uri, user=user, password=password)
driver = GraphDatabase.driver(uri, auth=(user, password))
remove = conn.query(
    "CALL apoc.periodic.iterate('MATCH (n) RETURN n', 'DETACH DELETE n', {batchSize:1000, iterateList:true})"
)

time.sleep(5)

while True:
    try:
        j = int(
            input(
                "How many students node would you like to be created? Please enter an integer number greater than 10 less than 10000: "
            )
        )
        if 10000 > j > 10:
            break
        else:
            print("The number must be in the range. Please try again.")
    except ValueError:
        print("Invalid input. Please enter a valid integer.")

while True:
    try:
        i = int(
            input(
                "How many lecturer node would you like to be created? Please enter an integer number greater than 5 and less than 1000: "
            )
        )
        if 1000 > i > 5:
            break
        else:
            print("The number must be in the range. Please try again.")
    except ValueError:
        print("Invalid input. Please enter a valid integer.")

while True:
    try:
        k = int(
            input(
                "How many tutor node would you like to be created? Please enter an integer number greater than 2 and less than 100: "
            )
        )
        if 100 > k > 2:
            break
        else:
            print("The number must be in the range. Please try again.")
    except ValueError:
        print("Invalid input. Please enter a valid integer.")


xls = pd.ExcelFile("D:\\VSCode\\Neo4j project\\raw_data.xlsx")
sheet_names = xls.sheet_names

for sheet_name in sheet_names:
    df = pd.read_excel(xls, sheet_name=sheet_name, usecols=[0], header=None)
    globals()[sheet_name.replace(" ", "_").lower() + "_list"] = df.iloc[:, 0].tolist()

with driver.session() as session:
    result = session.run(
        "CREATE (nl:NameList) SET nl.institute = $instituteList , nl.Dept= $department , nl.coursename3= $Course_Politics ,nl.coursename2 = $Course_Math , nl.coursename = $Course_Computer ,nl.semester = $semester , nl.firstNames = $First_Name , nl.lastNames = $Last_Name , nl.email = $Email, nl.city = $City , nl.street= $Street , nl.housn= $House_no , nl.gender = $Gender , nl.specialization = $Specialization, nl.assignment = $Assignment ",
        instituteList=institute_list,
        department=department_list,
        Course_Politics=course_politics_list,
        Course_Math=course_math_list,
        Course_Computer=course_computer_list,
        semester=semester_list,
        First_Name=first_name_list,
        Last_Name=last_name_list,
        Email=email_list,
        City=city_list,
        Street=street_list,
        House_no=house_no_list,
        Gender=gender_list,
        Specialization=specialization_list,
        Assignment=assignment_list,
    )

try:
    with open("D:\\VSCode\\Neo4j project\\cyphers.txt", "r") as file:
        line_count = 0
        for line in file:
            line = line.strip()
            if line:
                if line_count == 0:
                    line = line.replace("=x", f"{j}")
                    print(line)
                elif line_count == 1:
                    line = line.replace("=y", f"{i}")
                    print(line)
                elif line_count == 2:
                    line = line.replace("=z", f"{k}")
                    print(line)
                line_count += 1
                result = conn.query(line)
finally:
    print("Created successfully")

with driver.session() as session:
    while True:
        result = session.run(
            "MATCH ()-[s:ENROLLED_IN]->() RETURN count(s) AS NumberOfStudents"
        )
        number_of_enrolled_rel = result.single().value()
        print(number_of_enrolled_rel)
        if (number_of_enrolled_rel) == 450:
            print(f"You entered: {number_of_enrolled_rel}")
            break
        result = session.run(
            "MATCH (s:Student {studentID: $student_id})  MATCH (n:Course {courseID: $course_id }) MERGE (s)-[:ENROLLED_IN]->(n)",
            student_id=random.randint(10010001, 10010000 + j),
            course_id=random.randint(4001001, 4001200),
        )

    while True:
        result = session.run(
            "MATCH ()-[s:LECTURER_OF]->() RETURN count(s) AS NumberOfStudents"
        )
        number_of_lecturer_of = result.single().value()
        print(number_of_lecturer_of)
        if (number_of_lecturer_of) == 20:
            print(f"You entered: {number_of_lecturer_of}")
            break
        result = session.run(
            "MATCH (l:Lecturer {lecturerID: $lecturer_id})  MATCH (n:Course {courseID: $course_id }) MERGE (l)-[:LECTURER_OF]->(n)",
            lecturer_id=random.randint(2001001, 2001000 + i),
            course_id=random.randint(4001001, 4001200),
        )

    while True:
        result = session.run(
            "MATCH ()-[s:TUTOR_OF]->() RETURN count(s) AS NumberOfStudents"
        )
        number_of_tutor_of = result.single().value()
        print(number_of_tutor_of)
        if (number_of_tutor_of) == 8:
            print(f"You entered: {number_of_tutor_of}")
            break
        result = session.run(
            "MATCH (t:Tutor {tutorID: $tutor_id})  MATCH (n:Course {courseID: $course_id }) MERGE (t)-[:TUTOR_OF]->(n)",
            tutor_id=random.randint(300101, 300100 + k),
            course_id=random.randint(4001001, 4001200),
        )

    while True:
        result = session.run(
            "MATCH ()-[s:ASSIGNMENT_OF]->() RETURN count(s) AS NumberOfStudents"
        )
        number_of_assignment_of = result.single().value()
        print(number_of_assignment_of)
        if (number_of_assignment_of) == 45:
            print(f"You entered: {number_of_assignment_of}")
            break
        result = session.run(
            "MATCH (a:Assignment {assignmentID: $assignment_id})  MATCH (n:Course {courseID: $course_id }) where a.semester=n.semester MERGE (a)-[:ASSIGNMENT_OF]->(n)",
            assignment_id=random.randint(7001001, 7001050),
            course_id=random.randint(4001001, 4001200),
        )

    result = session.run(
        "match (s:Student)-[r:ENROLLED_IN]-(c:Course), (d:Department) where c.department=d.departmentname merge (c)-[:PUBLISHED_BY]-(d)"
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Sociology"})  MATCH (i:Institute {institutename: "Institute of Advanced Sciences and Humanities" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Political Science"})  MATCH (i:Institute {institutename: "Institute of Biological and Environmental Sciences" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Biology"})  MATCH (i:Institute {institutename: "Institute of Economic and Business Analysis" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Economics"})  MATCH (i:Institute {institutename: "Institute of Psychological Sciences" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Psychology"})  MATCH (i:Institute {institutename: "Institute of Literature and Cultural Studies" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of English Literature"})  MATCH (i:Institute {institutename: "Institute of Historical and Anthropological Research" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of History"})  MATCH (i:Institute {institutename: "Institute of Technology and Informatics" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Computer Science"})  MATCH (i:Institute {institutename: "Institute of Advanced Sciences and Humanities" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Mathematics"})  MATCH (i:Institute {institutename: "Institute of Theoretical and Applied Mathematics" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        'MATCH (d:Department {departmentname: "Department of Physics"})  MATCH (i:Institute {institutename: "Institute of Global Studies and Governance" }) MERGE (d)-[:BELONGS_TO]->(i)'
    )
    result = session.run(
        "match(l:Lecturer)-[r:LECTURER_OF]-(c:Course)-[p:PUBLISHED_BY]-(d:Department)-[q:BELONGS_TO]-(i:Institute) MERGE (l)-[:HIRED_BY]->(i) MERGE (l)-[:WORK_FOR]-(d)"
    )
    result = session.run(
        "match(t:Tutor)-[r:TUTOR_OF]-(c:Course)-[p:PUBLISHED_BY]-(d:Department) MERGE (t)-[:HIWI_OF]->(d)"
    )

driver.close()
conn.close()
