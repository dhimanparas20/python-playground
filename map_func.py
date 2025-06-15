# a simple python project that converts marks out of 100 to out of 50 using map() function and lambda expressions and also filter function

student_score:list[float] = [4.5,7.7,2.3,7.8,8.9,2.3]

marks_out_of_100 = list(map(lambda x: (x/10)*100, student_score))
print(marks_out_of_100)

pass_or_fail = list(map(lambda x: "Pass" if x>=33 else "Fail", marks_out_of_100))
print(pass_or_fail)

mapping:dict = dict(zip(student_score, pass_or_fail))
print(mapping)


t1 = [1,2,3,4,5,3,7]
t2 = [7,8,9,67,45,8,67]
print(dict(zip(t1,t2,strict=False)))