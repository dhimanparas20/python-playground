import threading
from time import sleep, perf_counter
from threading import Thread, enumerate, current_thread, active_count
from os import system

system("clear")

def counter_function(name,lmt):
    print("---------------------------------------")
    print(f"thread {name} is Counting till: {lmt}")
    for i in range(lmt):
        print(f"{name} : {i=}")
        sleep(0.1)

start = perf_counter()
counter_function("A",50)
counter_function("B",25)
counter_function("C",30)
counter_function("D",10)
print(f"Time taken: {perf_counter() - start}")

t1 = Thread(target=counter_function, args=("a",50,))
t2 = Thread(target=counter_function, args=("b",25,))
t3 = Thread(target=counter_function, args=("c",30,))
t4 = Thread(target=counter_function, args=("d",10,))

start = perf_counter()
t1.start()
t2.start()
t3.start()
t4.start()

print(f"Active threads: {active_count()}")

for thread in enumerate():
    print("-------------------------------------")
    print("Thread Name:",thread.name)
    print("Is alive: ",thread.is_alive())
    print("Ident: ",thread.ident)
    print("compare ident: ",thread.ident == current_thread().ident)
    print("IDF wtf:",thread.ident is current_thread().ident)



t1.join()
t2.join()
t3.join()
t4.join()
print(f"Time taken: {perf_counter() - start}")